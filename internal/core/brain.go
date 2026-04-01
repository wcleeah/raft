package core

import (
	"log/slog"

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
	entries      AppendEntries
	l            *slog.Logger
	raftState    *RaftState
	stateMachine *StateMachine
	deps         *BrainDeps
	quromCout    int
	id           string
	fellows      []*Fellow
}

func NewBrain(l *slog.Logger, deps *BrainDeps, cfg *BrainConfig) *Brain {
	return &Brain{
		entries:      *NewAppendEntries(deps.EntriesStore),
		l:            l,
		raftState:    NewRaftState(),
		stateMachine: &StateMachine{},
		deps:         deps,
		id:           cfg.SelfId,
		fellows:      cfg.Fellows,
	}
}

func (b *Brain) Start() {
	go b.HandleStateEvent()

	b.entries.Restore()
	b.raftState.UpdateRole(RAFT_ROLE_FOLLOWER)
}

func (b *Brain) HandleRPC(id string, frame *rpc.Frame) (rpc.RpcPayload, error) {
	switch frame.RPCType {
	case rpc.RPC_TYPE_REQUEST_VOTE_REQ:
		b.HandleVoteRequest(id, rpc.DecodeRequestVoteReq(frame.Payload))
	case rpc.RPC_TYPE_REQUEST_VOTE_RES:
		b.HandleVoteResult(id, rpc.DecodeRequestVoteRes(frame.Payload))
	}

	return nil, nil
}

func (b *Brain) HandleVoteRequest(id string, req *rpc.RequestVoteReq) *rpc.RequestVoteRes {
	idx, log := b.entries.PrevLog()
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
	b.raftState.GotVote(res.VoteGranted, res.Term, uint32(len(b.fellows)/2))
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
	go b.sendHeartbeat()
}

func (b *Brain) sendHeartbeat() {
Outer:
	for {
		lastLogIndex, lastLog := b.entries.PrevLog()
		commitIdx := b.entries.CommitIdx()
		term := b.raftState.Term()
		for _, fellow := range b.fellows {
			entries := b.entries.GetBSAfter(int(b.raftState.NextIndex(fellow.Id)))
			b.deps.Transport.Send(fellow.Id, &rpc.AppendEntriesReq{
				Term:         term,
				LeaderCommit: commitIdx,
				PrevLogIndex: uint32(lastLogIndex),
				PrevLogTerm:  lastLog.Term,
				LeaderId:     b.id,
				Entries:      entries,
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

		lastLogIndex, lastLog := b.entries.PrevLog()
		b.deps.Transport.Boardcast(&rpc.RequestVoteReq{
			Term:         b.raftState.Term(),
			CandidateId:  b.id,
			LastLogIndex: uint32(lastLogIndex),
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
