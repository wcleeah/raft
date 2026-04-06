package runtime

import (
	"log/slog"
	"sync"
	"sync/atomic"

	"com.lwc.raft/internal/core"
	"com.lwc.raft/internal/rpc"
)

type Fellow struct {
	mapMu sync.Mutex

	Id         string
	Addr       string
	RelationId atomic.Uint32
	ReqMap     map[uint32]rpc.RpcPayload
}

type BrainDeps struct {
	// Timer for candidate to restart an election after no one is elected
	ElectionTimer core.Timer
	// Timer for candidate to wait before starting an election
	WaitForElectionTimer core.Timer
	// Timer for follower to determine leader is down and not sending AE heartbeat
	ElectionTimeoutTimer core.Timer
	// Timer for leader to send periodic AE heartbeat
	HeartbeatTimer core.Timer

	EntriesStore core.Store
	Transport    core.Transport
}

type BrainConfig struct {
	Id      string
	Addr    string
	Fellows map[string]*Fellow
}

type Brain struct {
	entryMu sync.Mutex

	entries      *core.AppendEntriesStore
	l            *slog.Logger
	raftState    *core.RaftState
	stateMachine *core.StateMachine
	deps         *BrainDeps
	id           string
	addr         string
	fellows      map[string]*Fellow
}

func NewBrain(l *slog.Logger, deps *BrainDeps, cfg *BrainConfig) *Brain {
	return &Brain{
		entries:      core.NewAppendEntriesStore(deps.EntriesStore),
		l:            l,
		raftState:    &core.RaftState{},
		stateMachine: &core.StateMachine{},
		deps:         deps,
		id:           cfg.Id,
		addr:         cfg.Addr,
		fellows:      cfg.Fellows,
	}
}

func (b *Brain) Start(cfg core.TransportCfg) {
	go b.handleStateEvent()

	// initiate connection for rpc request FROM this node and response for that request
	go b.deps.Transport.Listen(b.addr, b.handleRPC, cfg)

	// initiate connection for rpc request TO this node and response for that request
	for _, fellow := range b.fellows {
		go b.deps.Transport.RegisterPeer(fellow.Id, fellow.Addr, b.handleRPC, cfg)
		b.raftState.RegisterNode(fellow.Id)
	}

	b.entries.Restore()
	b.raftState.UpdateRole(core.RAFT_ROLE_FOLLOWER)
}

func (b *Brain) handleRPC(id string, bs []byte) {
	frame := rpc.DecodeRPCFrame(bs)
	fellow, ok := b.fellows[id]
	if !ok {
		return
	}

	switch frame.RPCType {
	case rpc.RPC_TYPE_REQUEST_VOTE_REQ:
		res := b.handleVoteRequest(rpc.DecodeRequestVoteReq(frame.Payload))

		resFrame := rpc.Frame{
			RPCType:    rpc.RPC_TYPE_REQUEST_VOTE_RES,
			RelationId: frame.RelationId,
			Payload:    res.Encode(),
		}
		b.deps.Transport.Send(id, resFrame.Encode())
	case rpc.RPC_TYPE_APPEND_ENTRIES_REQ:
		res := b.handleAppendEntriesRequest(id, rpc.DecodeAppendEntriesReq(frame.Payload))

		resFrame := rpc.Frame{
			RPCType:    rpc.RPC_TYPE_APPEND_ENTRIES_RES,
			RelationId: frame.RelationId,
			Payload:    res.Encode(),
		}
		b.deps.Transport.Send(id, resFrame.Encode())
	case rpc.RPC_TYPE_STATE_ACTION_REQ:
		res := b.handleStateActionReq(rpc.DecodeStateActionReq(frame.Payload))

		resFrame := rpc.Frame{
			RPCType:    rpc.RPC_TYPE_APPEND_ENTRIES_RES,
			RelationId: frame.RelationId,
			Payload:    res.Encode(),
		}
		b.deps.Transport.Send(id, resFrame.Encode())
	case rpc.RPC_TYPE_REQUEST_VOTE_RES:
		b.handleVoteResult(id, rpc.DecodeRequestVoteRes(frame.Payload))
	case rpc.RPC_TYPE_APPEND_ENTRIES_RES:
		fellow.mapMu.Lock()
		relatedPayload, ok := fellow.ReqMap[frame.RelationId]
		fellow.mapMu.Unlock()

		if relatedPayload == nil {
			return
		}
		aer, ok := relatedPayload.(*rpc.AppendEntriesReq)
		if !ok {
			return
		}
		b.handleAppendEntriesResult(id, rpc.DecodeAppendEntriesRes(frame.Payload), *aer)
	}
}

func (b *Brain) handleVoteRequest(req rpc.RequestVoteReq) rpc.RequestVoteRes {
	idx, log := b.entries.LatestLog()
	if req.LastLogTerm < log.Term {
		return rpc.RequestVoteRes{
			VoteGranted: false,
			Term:        b.raftState.Term(),
		}
	}
	if req.LastLogIndex < idx {
		return rpc.RequestVoteRes{
			VoteGranted: false,
			Term:        b.raftState.Term(),
		}
	}
	term, err := b.raftState.Vote(req.CandidateId, req.Term)
	if err != nil {
		return rpc.RequestVoteRes{
			VoteGranted: false,
			Term:        term,
		}
	}

	return rpc.RequestVoteRes{
		VoteGranted: true,
		Term:        term,
	}
}

func (b *Brain) handleVoteResult(id string, res rpc.RequestVoteRes) {
	b.raftState.GotVote(res.VoteGranted, res.Term)
}

func (b *Brain) handleAppendEntriesRequest(id string, req rpc.AppendEntriesReq) rpc.AppendEntriesRes {
	if req.Term < b.raftState.Term() {
		return rpc.AppendEntriesRes{
			Term:    b.raftState.Term(),
			Success: false,
		}
	}

	latestLogIdx, err := b.entries.Replicate(req.Entries, req.PrevLogIndex, req.PrevLogTerm)
	if err != nil {
		return rpc.AppendEntriesRes{
			Term:    b.raftState.Term(),
			Success: false,
		}
	}

	b.raftState.GotAEReq(req.LeaderId, req.Term, req.LeaderCommit, latestLogIdx)

	return rpc.AppendEntriesRes{
		Term:    b.raftState.Term(),
		Success: true,
	}
}

func (b *Brain) handleAppendEntriesResult(id string, res rpc.AppendEntriesRes, relatedReq rpc.AppendEntriesReq) {
	entries := core.DecodeAppendEntries(relatedReq.Entries)

	b.raftState.GotAERes(id, res.Success, res.Term, relatedReq.PrevLogIndex+entries.Len())
}

func (b *Brain) handleStateActionReq(req rpc.StateActionReq) rpc.StateActionRes {
	if b.raftState.Role() != core.RAFT_ROLE_LEADER {
		return rpc.StateActionRes{
			Success:      false,
			RedirectAddr: b.raftState.LeaderId(),
		}
	}

	b.entries.Append(core.AppendEntry{
		Term:         b.raftState.Term(),
		CounterDelta: req.CounterDelta,
		Action:       req.Action,
	})

	b.raftState.IncLeaderIndexes(b.id)

	return rpc.StateActionRes{
		Success: true,
	}
}

func (b *Brain) handleStateEvent() {
	for event := range b.raftState.EventCh() {
		switch event {
		case core.RAFT_STATE_EVENT_ROLE_CHANGE:
			switch b.raftState.Role() {
			case core.RAFT_ROLE_FOLLOWER:
				b.switchToFollower()
			case core.RAFT_ROLE_CANDIDATE:
				b.switchToCandidate()
			case core.RAFT_ROLE_LEADER:
				b.switchToLeader()
			}
		case core.RAFT_STATE_EVENT_COMMIT_IDX_CHANGE:
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
	go b.electionLoop()
}

func (b *Brain) switchToLeader() {
	b.deps.ElectionTimer.Stop()
	b.deps.WaitForElectionTimer.Stop()
	b.deps.ElectionTimeoutTimer.Stop()

	latestLogIdx, _ := b.entries.LatestLog()
	err := b.raftState.InitAsLeader(b.id, latestLogIdx)
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
			payload := rpc.AppendEntriesReq{
				Term:         term,
				LeaderCommit: commitIdx,
				PrevLogIndex: prevLogIdx,
				PrevLogTerm:  prevLog.Term,
				LeaderId:     b.id,
				Entries:      b.entries.GetHeartbeatEntries(nextIndex).Encode(),
			}
			rId := fellow.RelationId.Add(1)
			frame := rpc.Frame{
				RPCType:    rpc.RPC_TYPE_APPEND_ENTRIES_REQ,
				RelationId: rId,
				Payload:    payload.Encode(),
			}

			fellow.mapMu.Lock()
			fellow.ReqMap[rId] = &payload
			fellow.mapMu.Unlock()

			b.deps.Transport.Send(fellow.Id, frame.Encode())
		}

		select {
		case <-b.deps.HeartbeatTimer.C():
		case <-b.deps.HeartbeatTimer.S():
			break Outer
		}
	}
}

func (b *Brain) startElectionTimeoutCountdown() {
	for {
		select {
		case <-b.deps.ElectionTimeoutTimer.C():
			if !b.raftState.IsVoted() {
				continue
			}
			b.raftState.UpdateRole(core.RAFT_ROLE_CANDIDATE)
			return
		case <-b.deps.ElectionTimeoutTimer.S():
			return
		}
	}

}

func (b *Brain) electionLoop() {
	for {
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

		for _, fellow := range b.fellows {
			payload := rpc.RequestVoteReq{
				Term:         b.raftState.Term(),
				CandidateId:  b.id,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLog.Term,
			}
			rId := fellow.RelationId.Add(1)
			frame := rpc.Frame{
				RPCType:    rpc.RPC_TYPE_REQUEST_VOTE_REQ,
				RelationId: rId,
				Payload:    payload.Encode(),
			}
			fellow.mapMu.Lock()
			fellow.ReqMap[rId] = &payload
			fellow.mapMu.Unlock()

			b.deps.Transport.Send(fellow.Id, frame.Encode())
		}

		select {
		case <-b.deps.ElectionTimer.C():
		case <-b.deps.ElectionTimer.S():
			return
		}
	}
}
