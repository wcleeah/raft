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
	b.l.Info("brain starting", "node_id", b.id, "addr", b.addr)

	_, latestLog := b.entries.Restore()
	b.raftState.RestoreTerm(latestLog.Term)
	b.l.Info("restored term from log store", "node_id", b.id, "restored_term", latestLog.Term)

	go b.handleStateEvent()

	// initiate connection for rpc request FROM this node and response for that request
	b.deps.Transport.RegisterSelf(b.addr, b.handleRPC, cfg)
	b.l.Debug("registered self transport", "node_id", b.id, "addr", b.addr)

	// initiate connection for rpc request TO this node and response for that request
	for _, fellow := range b.fellows {
		b.deps.Transport.RegisterPeer(fellow.Id, fellow.Addr, b.handleRPC, cfg)
		b.raftState.RegisterNode(fellow.Id)
		b.l.Debug("registered peer", "node_id", b.id, "peer_id", fellow.Id, "peer_addr", fellow.Addr)

		fellow.mapMu.Lock()
		if fellow.ReqMap == nil {
			fellow.ReqMap = make(map[uint32]rpc.RpcPayload, 0)
		}
		fellow.mapMu.Unlock()
	}

	// default raft state role value is follower
	b.switchToFollower()
	b.l.Info("initial role set to follower", "node_id", b.id)
}

func (b *Brain) handleRPC(id string, bs []byte) {
	frame := rpc.DecodeRPCFrame(bs)
	fellow, ok := b.fellows[id]
	if !ok {
		b.l.Warn("received RPC from unknown peer, ignoring", "node_id", b.id, "peer_id", id)
		return
	}

	switch frame.RPCType {
	case rpc.RPC_TYPE_REQUEST_VOTE_REQ:
		b.l.Debug("received RequestVote request", "node_id", b.id, "peer_id", id, "relation_id", frame.RelationId)
		res := b.handleVoteRequest(rpc.DecodeRequestVoteReq(frame.Payload))

		resFrame := rpc.Frame{
			RPCType:    rpc.RPC_TYPE_REQUEST_VOTE_RES,
			RelationId: frame.RelationId,
			Payload:    res.Encode(),
		}
		b.deps.Transport.Send(id, resFrame.Encode())
	case rpc.RPC_TYPE_APPEND_ENTRIES_REQ:
		b.l.Debug("received AppendEntries request", "node_id", b.id, "peer_id", id, "relation_id", frame.RelationId)
		res := b.handleAppendEntriesRequest(id, rpc.DecodeAppendEntriesReq(frame.Payload))

		resFrame := rpc.Frame{
			RPCType:    rpc.RPC_TYPE_APPEND_ENTRIES_RES,
			RelationId: frame.RelationId,
			Payload:    res.Encode(),
		}
		b.deps.Transport.Send(id, resFrame.Encode())
	case rpc.RPC_TYPE_STATE_ACTION_REQ:
		b.l.Debug("received StateAction request", "node_id", b.id, "peer_id", id, "relation_id", frame.RelationId)
		res := b.handleStateActionReq(rpc.DecodeStateActionReq(frame.Payload))

		resFrame := rpc.Frame{
			RPCType:    rpc.RPC_TYPE_APPEND_ENTRIES_RES,
			RelationId: frame.RelationId,
			Payload:    res.Encode(),
		}
		b.deps.Transport.Send(id, resFrame.Encode())
	case rpc.RPC_TYPE_REQUEST_VOTE_RES:
		b.l.Debug("received RequestVote response", "node_id", b.id, "peer_id", id, "relation_id", frame.RelationId)
		b.handleVoteResult(id, rpc.DecodeRequestVoteRes(frame.Payload))
	case rpc.RPC_TYPE_APPEND_ENTRIES_RES:
		b.l.Debug("received AppendEntries response", "node_id", b.id, "peer_id", id, "relation_id", frame.RelationId)
		fellow.mapMu.Lock()
		relatedPayload, ok := fellow.ReqMap[frame.RelationId]
		fellow.mapMu.Unlock()

		if relatedPayload == nil {
			b.l.Warn("no related payload for AE response, ignoring", "node_id", b.id, "peer_id", id, "relation_id", frame.RelationId)
			return
		}
		aer, ok := relatedPayload.(*rpc.AppendEntriesReq)
		if !ok {
			b.l.Warn("related payload is not AppendEntriesReq, ignoring", "node_id", b.id, "peer_id", id, "relation_id", frame.RelationId)
			return
		}
		b.handleAppendEntriesResult(id, rpc.DecodeAppendEntriesRes(frame.Payload), *aer)
	}
}

func (b *Brain) handleVoteRequest(req rpc.RequestVoteReq) rpc.RequestVoteRes {
	b.l.Debug("handling vote request", "node_id", b.id, "candidate_id", req.CandidateId, "candidate_term", req.Term, "last_log_index", req.LastLogIndex, "last_log_term", req.LastLogTerm)

	idx, log := b.entries.LatestLog()
	if req.LastLogTerm < log.Term {
		b.l.Debug("rejecting vote: candidate log term behind", "node_id", b.id, "candidate_id", req.CandidateId, "candidate_last_log_term", req.LastLogTerm, "our_last_log_term", log.Term)
		return rpc.RequestVoteRes{
			VoteGranted: false,
			Term:        b.raftState.Term(),
		}
	}
	if req.LastLogIndex < idx {
		b.l.Debug("rejecting vote: candidate log index behind", "node_id", b.id, "candidate_id", req.CandidateId, "candidate_last_log_index", req.LastLogIndex, "our_last_log_index", idx)
		return rpc.RequestVoteRes{
			VoteGranted: false,
			Term:        b.raftState.Term(),
		}
	}
	term, err := b.raftState.Vote(req.CandidateId, req.Term)
	if err != nil {
		b.l.Warn("vote denied by raft state", "node_id", b.id, "candidate_id", req.CandidateId, "term", term, "error", err)
		return rpc.RequestVoteRes{
			VoteGranted: false,
			Term:        term,
		}
	}

	b.l.Info("vote granted", "node_id", b.id, "candidate_id", req.CandidateId, "term", term)
	return rpc.RequestVoteRes{
		VoteGranted: true,
		Term:        term,
	}
}

func (b *Brain) handleVoteResult(id string, res rpc.RequestVoteRes) {
	b.l.Debug("processing vote result", "node_id", b.id, "peer_id", id, "vote_granted", res.VoteGranted, "peer_term", res.Term)
	b.raftState.GotVote(res.VoteGranted, res.Term)
}

func (b *Brain) handleAppendEntriesRequest(id string, req rpc.AppendEntriesReq) rpc.AppendEntriesRes {
	b.l.Debug("handling AE request", "node_id", b.id, "leader_id", req.LeaderId, "term", req.Term, "prev_log_index", req.PrevLogIndex, "prev_log_term", req.PrevLogTerm, "leader_commit", req.LeaderCommit)

	if req.Term < b.raftState.Term() {
		b.l.Warn("rejecting AE: stale term", "node_id", b.id, "leader_id", req.LeaderId, "req_term", req.Term, "current_term", b.raftState.Term())
		return rpc.AppendEntriesRes{
			Term:    b.raftState.Term(),
			Success: false,
		}
	}

	latestLogIdx, err := b.entries.Replicate(req.Entries, req.PrevLogIndex, req.PrevLogTerm)
	if err != nil {
		b.l.Warn("AE replication failed", "node_id", b.id, "leader_id", req.LeaderId, "prev_log_index", req.PrevLogIndex, "prev_log_term", req.PrevLogTerm, "error", err)
		return rpc.AppendEntriesRes{
			Term:    b.raftState.Term(),
			Success: false,
		}
	}

	b.raftState.GotAEReq(req.LeaderId, req.Term, req.LeaderCommit, latestLogIdx)
	b.l.Debug("AE request accepted", "node_id", b.id, "leader_id", req.LeaderId, "term", req.Term, "latest_log_index", latestLogIdx)

	return rpc.AppendEntriesRes{
		Term:    b.raftState.Term(),
		Success: true,
	}
}

func (b *Brain) handleAppendEntriesResult(id string, res rpc.AppendEntriesRes, relatedReq rpc.AppendEntriesReq) {
	entries := core.DecodeAppendEntries(relatedReq.Entries)
	matchIdx := relatedReq.PrevLogIndex + entries.Len()

	b.l.Debug("processing AE result", "node_id", b.id, "peer_id", id, "success", res.Success, "peer_term", res.Term, "match_index", matchIdx)
	b.raftState.GotAERes(id, res.Success, res.Term, matchIdx, b.entries.Copy())
}

func (b *Brain) handleStateActionReq(req rpc.StateActionReq) rpc.StateActionRes {
	b.l.Debug("handling state action request", "node_id", b.id, "action", req.Action, "counter_delta", req.CounterDelta)

	if b.raftState.Role() != core.RAFT_ROLE_LEADER {
		leaderId := b.raftState.LeaderId()
		b.l.Warn("rejecting state action: not leader", "node_id", b.id, "redirect_to", leaderId)
		return rpc.StateActionRes{
			Success:      false,
			RedirectAddr: leaderId,
		}
	}

	b.entries.Append(core.AppendEntry{
		Term:         b.raftState.Term(),
		CounterDelta: req.CounterDelta,
		Action:       req.Action,
	})

	b.raftState.IncLeaderIndexes(b.id)
	b.l.Info("state action appended", "node_id", b.id, "term", b.raftState.Term(), "action", req.Action, "counter_delta", req.CounterDelta)

	return rpc.StateActionRes{
		Success: true,
	}
}

func (b *Brain) handleStateEvent() {
	b.l.Debug("state event handler started", "node_id", b.id)
	for event := range b.raftState.EventCh() {
		switch event {
		case core.RAFT_STATE_EVENT_ROLE_CHANGE:
			role := b.raftState.Role()
			b.l.Info("role change event", "node_id", b.id, "new_role", role, "term", b.raftState.Term())
			switch role {
			case core.RAFT_ROLE_FOLLOWER:
				b.switchToFollower()
			case core.RAFT_ROLE_CANDIDATE:
				b.switchToCandidate()
			case core.RAFT_ROLE_LEADER:
				b.switchToLeader()
			}
		case core.RAFT_STATE_EVENT_COMMIT_IDX_CHANGE:
			b.l.Debug("commit index changed", "node_id", b.id, "commit_idx", b.raftState.CommitIdx())
			b.applyState()
		}
	}
}

func (b *Brain) switchToFollower() {
	b.l.Info("switching to follower", "node_id", b.id, "term", b.raftState.Term())
	b.deps.HeartbeatTimer.Stop()
	b.deps.ElectionTimer.Stop()
	b.deps.WaitForElectionTimer.Stop()

	go b.startElectionTimeoutCountdown()
}

func (b *Brain) switchToCandidate() {
	b.l.Info("switching to candidate", "node_id", b.id, "term", b.raftState.Term())
	b.deps.ElectionTimeoutTimer.Stop()
	go b.electionLoop()
}

func (b *Brain) switchToLeader() {
	b.l.Info("switching to leader", "node_id", b.id, "term", b.raftState.Term())
	b.deps.ElectionTimer.Stop()
	b.deps.WaitForElectionTimer.Stop()
	b.deps.ElectionTimeoutTimer.Stop()

	latestLogIdx, _ := b.entries.LatestLog()
	err := b.raftState.InitAsLeader(b.id, latestLogIdx)
	if err != nil {
		b.l.Error("failed to init as leader", "node_id", b.id, "error", err)
		return
	}

	b.l.Info("leader initialized", "node_id", b.id, "term", b.raftState.Term(), "latest_log_index", latestLogIdx)
	go b.sendHeartbeat()
}

func (b *Brain) applyState() {
	b.entryMu.Lock()
	defer b.entryMu.Unlock()

	commitIdx := b.raftState.CommitIdx()
	aes := b.entries.ApplyAll(commitIdx)

	b.l.Debug("applying entries to state machine", "node_id", b.id, "commit_idx", commitIdx, "entries_count", len(aes))
	for i, ae := range aes {
		b.stateMachine.Act(ae.Action, ae.CounterDelta)
		b.l.Debug("applied entry", "node_id", b.id, "entry_index", i, "action", ae.Action, "counter_delta", ae.CounterDelta)
	}
}

func (b *Brain) sendHeartbeat() {
	b.l.Debug("heartbeat loop started", "node_id", b.id)
Outer:
	for {
		commitIdx := b.raftState.CommitIdx()
		term := b.raftState.Term()
		b.l.Debug("sending heartbeat round", "node_id", b.id, "term", term, "commit_idx", commitIdx)

		for _, fellow := range b.fellows {
			nextIndex := b.raftState.NextIndex(fellow.Id)
			prevLogIdx := nextIndex
			if prevLogIdx != 0 {
				prevLogIdx--
			}
			prevLog, err := b.entries.Get(prevLogIdx)
			if err != nil {
				b.l.Error("failed to get prev log for heartbeat", "node_id", b.id, "peer_id", fellow.Id, "prev_log_index", prevLogIdx, "error", err)
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
			b.l.Debug("heartbeat sent", "node_id", b.id, "peer_id", fellow.Id, "relation_id", rId, "next_index", nextIndex, "prev_log_index", prevLogIdx)
		}

		select {
		case <-b.deps.HeartbeatTimer.C():
			b.l.Debug("heartbeat timer tick", "node_id", b.id)
		case <-b.deps.HeartbeatTimer.S():
			b.l.Debug("heartbeat timer stopped, exiting heartbeat loop", "node_id", b.id)
			break Outer
		}
	}
}

func (b *Brain) startElectionTimeoutCountdown() {
	b.l.Debug("starting election timeout countdown", "node_id", b.id)
	for {
		select {
		case <-b.deps.ElectionTimeoutTimer.C():
			if b.raftState.IsVoted() {
				b.l.Debug("election timeout fired but already voted, resetting", "node_id", b.id)
				continue
			}
			b.l.Info("election timeout expired, promoting to candidate", "node_id", b.id, "term", b.raftState.Term())
			b.raftState.UpdateRole(core.RAFT_ROLE_CANDIDATE)
			return
		case <-b.deps.ElectionTimeoutTimer.S():
			b.l.Debug("election timeout timer stopped, exiting countdown", "node_id", b.id)
			return
		}
	}

}

func (b *Brain) electionLoop() {
	b.l.Debug("entering election loop", "node_id", b.id)
	for {
		select {
		case <-b.deps.WaitForElectionTimer.C():
			b.l.Debug("wait-for-election timer fired", "node_id", b.id)
		case <-b.deps.WaitForElectionTimer.S():
			b.l.Debug("wait-for-election timer stopped, exiting election loop", "node_id", b.id)
			return
		}

		err := b.raftState.StartElection(b.id)
		if err != nil {
			b.l.Error("failed to start election", "node_id", b.id, "error", err)
			return
		}
		b.l.Info("election started", "node_id", b.id, "term", b.raftState.Term())

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
			b.l.Debug("RequestVote sent", "node_id", b.id, "peer_id", fellow.Id, "relation_id", rId, "term", b.raftState.Term(), "last_log_index", lastLogIndex, "last_log_term", lastLog.Term)
		}

		select {
		case <-b.deps.ElectionTimer.C():
			b.l.Debug("election timer expired, no winner yet", "node_id", b.id, "term", b.raftState.Term())
		case <-b.deps.ElectionTimer.S():
			b.l.Debug("election timer stopped, exiting election loop", "node_id", b.id)
			return
		}
	}
}
