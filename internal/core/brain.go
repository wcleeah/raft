package core

import (
	"log/slog"
	"sync"

	"com.lwc.raft/internal/rpc"
)

type Fellow struct {
	Id   string
	Addr string
}

type BrainDeps struct {
	// Timer for candidate to restart an election after no one is elected
	ElectionTimer Timer
	// Timer for candidate to wait before starting an election
	WaitForElectionTimer Timer
	// Timer for follower to determine leader is down and not sending AE heartbeat
	ElectionTimeoutTimer Timer
	// Timer for leader to send periodic AE heartbeat
	HeartbeatTimer Timer

	EntriesStore Store
	Transport    Transport
}

type BrainConfig struct {
	SelfId  string
	Fellows []*Fellow
}

type Brain struct {
	entryMu sync.Mutex

	entries        *AppendEntriesStore
	l              *slog.Logger
	raftState      *RaftState
	stateMachine   *StateMachine
	deps           *BrainDeps
	// majority is larger than this threshold
	quromThreshold uint32
	id             string
	fellows        []*Fellow
}

func NewBrain(l *slog.Logger, deps *BrainDeps, cfg *BrainConfig) *Brain {
	return &Brain{
		entries:        NewAppendEntriesStore(deps.EntriesStore),
		l:              l,
		raftState:      NewRaftState(),
		stateMachine:   &StateMachine{},
		deps:           deps,
		id:             cfg.SelfId,
		fellows:        cfg.Fellows,
		quromThreshold: uint32(len(cfg.Fellows) / 2),
	}
}

func (b *Brain) Start() {
	go b.HandleStateEvent()

	b.entries.Restore()
	b.raftState.UpdateRole(RAFT_ROLE_FOLLOWER)
}

func (b *Brain) HandleRPC(id string, frame rpc.Frame, relatedReqFrame rpc.Frame) (rpc.RpcPayload, error) {
	switch frame.RPCType {
	case rpc.RPC_TYPE_REQUEST_VOTE_REQ:
		b.HandleVoteRequest(id, rpc.DecodeRequestVoteReq(frame.Payload))
	case rpc.RPC_TYPE_REQUEST_VOTE_RES:
		b.HandleVoteResult(id, rpc.DecodeRequestVoteRes(frame.Payload))
	case rpc.RPC_TYPE_APPEND_ENTRIES_REQ:
		b.HandleAppendEntriesRequest(id, rpc.DecodeAppendEntriesReq(frame.Payload))
	case rpc.RPC_TYPE_APPEND_ENTRIES_RES:
		b.HandleAppendEntriesResult(id, rpc.DecodeAppendEntriesRes(frame.Payload), rpc.DecodeAppendEntriesReq(relatedReqFrame.Payload))
	}

	return nil, nil
}

func (b *Brain) HandleVoteRequest(id string, req *rpc.RequestVoteReq) *rpc.RequestVoteRes {
	idx, log := b.entries.LatestLog()
	if req.LastLogTerm < log.Term {
		return &rpc.RequestVoteRes{
			VoteGranted: false,
			Term:        b.raftState.Term(),
		}
	}
	if req.LastLogIndex < idx {
		return &rpc.RequestVoteRes{
			VoteGranted: false,
			Term:        b.raftState.Term(),
		}
	}
	term, err := b.raftState.Vote(req.CandidateId, req.Term)
	if err != nil {
		return &rpc.RequestVoteRes{
			VoteGranted: false,
			Term:        term,
		}
	}

	return &rpc.RequestVoteRes{
		VoteGranted: true,
		Term:        term,
	}
}

func (b *Brain) HandleVoteResult(id string, res *rpc.RequestVoteRes) {
	b.raftState.GotVote(res.VoteGranted, res.Term, b.quromThreshold)
}

func (b *Brain) HandleAppendEntriesRequest(id string, req *rpc.AppendEntriesReq) *rpc.AppendEntriesRes {
	if req.Term < b.raftState.Term() {
		return &rpc.AppendEntriesRes{
			Term:    b.raftState.Term(),
			Success: false,
		}
	}

	latestLogIdx, err := b.entries.Replicate(req.Entries, req.PrevLogIndex, req.PrevLogTerm)
	if err != nil {
		return &rpc.AppendEntriesRes{
			Term:    b.raftState.Term(),
			Success: false,
		}
	}

	b.raftState.GotAEReq(req.LeaderId, req.Term, req.LeaderCommit, latestLogIdx)

	return &rpc.AppendEntriesRes{
		Term:    b.raftState.Term(),
		Success: true,
	}
}

func (b *Brain) HandleAppendEntriesResult(id string, res *rpc.AppendEntriesRes, relatedReq *rpc.AppendEntriesReq) {
	entries := DecodeAppendEntries(relatedReq.Entries)

	b.raftState.GotAERes(id, res.Success, res.Term, relatedReq.PrevLogIndex+entries.Len(), b.quromThreshold)
}

func (b *Brain) HandleStateEvent() {
	for event := range b.raftState.eventCh {
		switch event {
		case RAFT_STATE_EVENT_ROLE_CHANGE:
			switch b.raftState.Role() {
			case RAFT_ROLE_FOLLOWER:
				b.switchToFollower()
			case RAFT_ROLE_CANDIDATE:
				b.switchToCandidate()
			case RAFT_ROLE_LEADER:
				b.switchToLeader()
			}
		case RAFT_STATE_EVENT_COMMIT_IDX_CHANGE:
			b.applyState()
		}
	}
}

func (b *Brain) switchToFollower() {
	b.deps.HeartbeatTimer.Stop()
	b.deps.ElectionTimer.Stop()
	b.deps.WaitForElectionTimer.Stop()

	go b.startElectionTimeoutCountdown()
}

func (b *Brain) switchToCandidate() {
	b.deps.ElectionTimeoutTimer.Stop()
	go b.ElectionLoop()
}

func (b *Brain) switchToLeader() {
	b.deps.ElectionTimer.Stop()
	b.deps.WaitForElectionTimer.Stop()
	b.deps.ElectionTimeoutTimer.Stop()

	latestLogIdx, _ := b.entries.LatestLog()
	err := b.raftState.InitAsLeader(latestLogIdx)
	if err != nil {
		return
	}

	go b.sendHeartbeat()
}

func (b *Brain) applyState() {
	b.entryMu.Lock()
	defer b.entryMu.Unlock()

	commitIdx := b.raftState.CommitIdx()
	aes := b.entries.ApplyAll(commitIdx)

	for _, ae := range aes {
		b.stateMachine.Act(ae.Action, ae.CounterDelta)
	}
}

func (b *Brain) sendHeartbeat() {
Outer:
	for {
		commitIdx := b.raftState.CommitIdx()
		term := b.raftState.Term()
		for _, fellow := range b.fellows {
			nextIndex := b.raftState.NextIndex(fellow.Id)
			prevLogIdx := nextIndex
			if prevLogIdx != 0 {
				prevLogIdx--
			}
			prevLog, err := b.entries.Get(prevLogIdx)
			if err != nil {
				continue
			}

			b.deps.Transport.Send(fellow.Id, &rpc.AppendEntriesReq{
				Term:         term,
				LeaderCommit: commitIdx,
				PrevLogIndex: prevLogIdx,
				PrevLogTerm:  prevLog.Term,
				LeaderId:     b.id,
				Entries:      b.entries.GetHeartbeatEntries(nextIndex).Encode(),
			})
		}

		select {
		case <-b.deps.HeartbeatTimer.C():
			b.deps.HeartbeatTimer.Reset()
		case <-b.deps.HeartbeatTimer.S():
			break Outer
		}
	}
}

func (b *Brain) startElectionTimeoutCountdown() {
	for {
		b.deps.ElectionTimeoutTimer.Reset()
		select {
		case <-b.deps.ElectionTimeoutTimer.C():
			if !b.raftState.IsVoted() {
				continue
			}
			b.raftState.UpdateRole(RAFT_ROLE_CANDIDATE)
			return
		case <-b.deps.ElectionTimeoutTimer.S():
			return
		}
	}

}

func (b *Brain) ElectionLoop() {
	for {
		b.deps.WaitForElectionTimer.Reset()
		select {
		case <-b.deps.WaitForElectionTimer.C():
		case <-b.deps.WaitForElectionTimer.S():
			return
		}

		err := b.raftState.StartElection(b.id)
		if err != nil {
			return
		}

		lastLogIndex, lastLog := b.entries.LatestLog()
		b.deps.Transport.Boardcast(&rpc.RequestVoteReq{
			Term:         b.raftState.Term(),
			CandidateId:  b.id,
			LastLogIndex: lastLogIndex,
			LastLogTerm:  lastLog.Term,
		})

		b.deps.ElectionTimer.Reset()
		select {
		case <-b.deps.ElectionTimer.C():
		case <-b.deps.ElectionTimer.S():
			return
		}
	}
}
