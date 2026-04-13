package runtime

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"os"
	"strconv"
	"testing"
	"time"

	"com.lwc.raft/internal/core"
	"com.lwc.raft/internal/rpc"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
)

// THIS SERVE AS A PART OF DETERMINISTIC TESTING FOR THE RUNTIME
// THE GOAL IS TO CHECK IF EACH "EVENT": STATE EVENT / TIMER TICKS, ARE HANDLED CORRECTLY.
// THIS WILL TEST:
// - Start
// - Each RaftState events' logic
// - Each timer ticks' logic
// - Timer cancellation logic

func TestStart(t *testing.T) {
	ass := assert.New(t)
	b := giveMeATestBrain(5)
	ctx, cancelFunc := context.WithCancel(context.Background())

	b.Brain.Start(ctx, b.TransportCfg)

	ass.Equal(b.FakeStore.Saved.LatestLog().Term, b.Brain.raftState.Term(), "Term not restored")
	ass.Equal(b.Brain.raftState.Role(), core.RAFT_ROLE_FOLLOWER, "Role is not follower")
	ass.Equal(b.Brain.raftState.Threshold(), uint32(3), "Node registration incorrect")

	latestIdx, latestLog := b.Brain.entries.LatestLog()
	// Restore should add one dummy item at the head, so idx will eq len
	ass.Equal(b.FakeStore.Saved.Len(), latestIdx, "Restore failure: idx mismatch")
	if diff := cmp.Diff(b.FakeStore.Saved.LatestLog(), latestLog); diff != "" {
		ass.FailNowf("Restore failure: latest log mismatch", "mismatch (-want +got):\n%s", diff)
	}

	ass.NotNil(b.FakeTransport.ConnMap[b.BrainCfg.Id], "Self should be registered to transport")
	for _, fellow := range b.Fellows {
		ass.NotNil(fellow.ReqMap, "Request map should be initialized")
		ass.NotNil(b.FakeTransport.ConnMap[fellow.Id], "Fellow should be registered to transport")
	}

	cancelFunc()
	<-b.Brain.CloseCh()
	ass.ErrorIs(context.Canceled, b.Brain.CloseReason(), "Close reason should be cancelled")
}

func TestHandleStateEvent_RoleChange(t *testing.T) {
	ass := assert.New(t)
	b := giveMeATestBrain(5)

	b.Brain.handleStateEvent(core.RAFT_ROLE_FOLLOWER)
	ass.NotNil(b.Brain.roleCtxCancel, "roleCtxCancel should be updated to a cancel func")
	// default val 0
	ass.Equal(1, b.Brain.roleGen, "roleGen should be incremented to 1")

	called := false
	var fakeCancelFunc context.CancelFunc
	fakeCancelFunc = func() {
		called = true
	}

	b.Brain.roleLoopMu.Lock()
	b.Brain.roleCtxCancel = fakeCancelFunc
	b.Brain.roleLoopMu.Unlock()

	b.Brain.handleStateEvent(core.RAFT_ROLE_FOLLOWER)
	ass.True(called, "old cancel func is not called")
	ass.NotNil(b.Brain.roleCtxCancel, "roleCtxCancel should still be not nil")
	ass.Equal(2, b.Brain.roleGen, "roleGen should be incremented to 2")
}

func TestEventFunc_ElectionTimeoutLoop(t *testing.T) {
	loop := func(ch chan error, brain *Brain, ctx context.Context, rg int) {
		ch <- brain.electionTimeoutLoop(ctx, rg)
	}

	t.Run("Role Gen Incremented", func(t *testing.T) {
		ass := assert.New(t)
		b := giveMeATestBrain(5)

		errCh := make(chan error)
		b.Brain.roleGen++
		go loop(errCh, b.Brain, context.Background(), 0)
		err := <-errCh

		ass.ErrorIs(err, BrainRoleGenExpErr, "Role Gen checking is not implemented")
	})

	t.Run("Timer fired", func(t *testing.T) {
		ass := assert.New(t)
		b := giveMeATestBrain(5)

		errCh := make(chan error)
		go loop(errCh, b.Brain, context.Background(), 0)

		b.Deps.ElectionTimeoutTimer.(*fakeTimer).PassTime()
		err := <-errCh

		ass.EqualError(err, "election timeout expired", "Loop should break if timer passed and not voted")
	})

	t.Run("Context Done", func(t *testing.T) {
		ass := assert.New(t)
		b := giveMeATestBrain(5)

		errCh := make(chan error)
		ctx, cancelFunc := context.WithCancel(context.Background())
		go loop(errCh, b.Brain, ctx, 0)
		cancelFunc()
		err := <-errCh

		ass.ErrorIs(context.Canceled, err, "Ctx cancelling should break the loop")
	})

	t.Run("Brain Done", func(t *testing.T) {
		ass := assert.New(t)
		b := giveMeATestBrain(5)

		errCh := make(chan error)
		go loop(errCh, b.Brain, context.Background(), 0)
		newErrErr := errors.New("404 not found in the system 404 the new era era")
		b.Brain.Close(newErrErr)
		err := <-errCh

		ass.ErrorIs(newErrErr, err, "Brain closed should break the loop")
	})
}

func TestEventFunc_ElectionLoop(t *testing.T) {
	loop := func(ch chan error, brain *Brain, ctx context.Context, rg int) {
		ch <- brain.electionLoop(ctx, rg)
	}

	t.Run("Role Gen Incremented", func(t *testing.T) {
		ass := assert.New(t)
		b := giveMeATestBrain(5)

		errCh := make(chan error)
		b.Brain.roleGen++
		go loop(errCh, b.Brain, context.Background(), 0)
		err := <-errCh

		ass.ErrorIs(err, BrainRoleGenExpErr, "Role Gen checking is not implemented")
	})

	t.Run("Wait For Election Timer fired", func(t *testing.T) {
		ass := assert.New(t)
		b := giveMeATestBrain(5)

		errCh := make(chan error)
		go loop(errCh, b.Brain, context.Background(), 0)

		b.Deps.WaitForElectionTimer.(*fakeTimer).PassTime()
		err := <-errCh

		ass.ErrorIs(core.RaftStateNotACandidateErr, err, "Loop should break if timer passed and not voted")
	})

	t.Run("Context Done", func(t *testing.T) {
		ass := assert.New(t)
		b := giveMeATestBrain(5)

		errCh := make(chan error)
		ctx, cancelFunc := context.WithCancel(context.Background())
		go loop(errCh, b.Brain, ctx, 0)
		cancelFunc()
		err := <-errCh

		ass.ErrorIs(context.Canceled, err, "Ctx cancelling should break the loop")
	})

	t.Run("Brain Done", func(t *testing.T) {
		ass := assert.New(t)
		b := giveMeATestBrain(5)

		errCh := make(chan error)
		go loop(errCh, b.Brain, context.Background(), 0)
		newErrErr := errors.New("404 not found in the system 404 the new era era")
		b.Brain.Close(newErrErr)
		err := <-errCh

		ass.ErrorIs(newErrErr, err, "Brain closed should break the loop")
	})
}

func TestEventFunc_HeartbeatLoop(t *testing.T) {
	loop := func(ch chan error, brain *Brain, ctx context.Context, rg int) {
		ch <- brain.heartbeatLoop(ctx, rg)
	}

	t.Run("Not a Leader", func(t *testing.T) {
		ass := assert.New(t)
		b := giveMeATestBrain(5)

		errCh := make(chan error)
		go loop(errCh, b.Brain, context.Background(), 0)

		err := <-errCh

		ass.ErrorIs(err, core.RaftStateNotALeaderErr, "Loop should break if timer passed and not voted")
	})

	t.Run("Role Gen Incremented", func(t *testing.T) {
		ass := assert.New(t)
		b := giveMeATestBrain(5)
		b.Brain.raftState.UpdateRole(core.RAFT_ROLE_CANDIDATE)
		b.Brain.raftState.UpdateRole(core.RAFT_ROLE_LEADER)

		errCh := make(chan error)
		b.Brain.roleGen++
		go loop(errCh, b.Brain, context.Background(), 0)
		err := <-errCh

		ass.ErrorIs(err, BrainRoleGenExpErr, "Role Gen checking is not implemented")
	})

	t.Run("Context Done", func(t *testing.T) {
		ass := assert.New(t)
		b := giveMeATestBrain(5)
		b.Brain.raftState.UpdateRole(core.RAFT_ROLE_CANDIDATE)
		b.Brain.raftState.UpdateRole(core.RAFT_ROLE_LEADER)

		errCh := make(chan error)
		ctx, cancelFunc := context.WithCancel(context.Background())
		go loop(errCh, b.Brain, ctx, 0)
		cancelFunc()
		err := <-errCh

		ass.ErrorIs(context.Canceled, err, "Ctx cancelling should break the loop")
	})

	t.Run("Brain Done", func(t *testing.T) {
		ass := assert.New(t)
		b := giveMeATestBrain(5)
		b.Brain.raftState.UpdateRole(core.RAFT_ROLE_CANDIDATE)
		b.Brain.raftState.UpdateRole(core.RAFT_ROLE_LEADER)

		errCh := make(chan error)
		go loop(errCh, b.Brain, context.Background(), 0)
		newErrErr := errors.New("404 not found in the system 404 the new era era")
		b.Brain.Close(newErrErr)
		err := <-errCh

		ass.ErrorIs(newErrErr, err, "Brain closed should break the loop")
	})
}

func TestEventFunc_ApplyState(t *testing.T) {
	ass := assert.New(t)
	b := giveMeATestBrain(5)

	latestIdx, latestLog := b.Brain.entries.Restore()

	// Restore should add one dummy item at the head, so idx will eq len
	ass.Equal(b.FakeStore.Saved.Len(), latestIdx, "Unexpected restore failure: idx mismatch")
	if diff := cmp.Diff(b.FakeStore.Saved.LatestLog(), latestLog); diff != "" {
		ass.FailNowf("Unexpected restore failure: latest log mismatch", "mismatch (-want +got):\n%s", diff)
	}

	b.Brain.raftState.RegisterNode(b.Brain.id)

	for _, fellow := range b.Brain.fellows {
		b.Brain.raftState.RegisterNode(fellow.Id)

		if fellow.ReqMap == nil {
			fellow.ReqMap = make(map[uint32]rpc.RpcPayload, 0)
		}
	}

	b.Brain.raftState.UpdateCommitIdx(b.FakeStore.Saved.Len())
	b.Brain.applyState()

	// Sanity check
	ass.Equal(b.FakeStore.Saved.Len(), b.Brain.entries.LastAppliedIdx(), "Last Applied Idx is not updated")
	ass.Equal(int32(3), b.Brain.stateMachine.Counter(), "Counter mismatched")
}

func TestHandleElectionTimeoutTimerTick(t *testing.T) {
	ass := assert.New(t)
	b := giveMeATestBrain(5)

	ass.EqualError(b.Brain.handleElectionTimeoutTimerTick(), "election timeout expired", "Should return error when not voted")
	ass.Equal(core.RAFT_ROLE_CANDIDATE, b.Brain.raftState.Role(), "Should promote to Candidate")

	b.Brain.raftState.UpdateRole(core.RAFT_ROLE_FOLLOWER)
	latestIdx, latestLog := b.Brain.entries.Restore()
	_, err := b.Brain.raftState.Vote(b.Fellows[0].Id, b.Brain.raftState.Term()+1, latestIdx, latestLog.Term, b.Brain.entries.Copy())
	ass.NoError(err, "Unexpected error when voting")
	ass.NoError(b.Brain.handleElectionTimeoutTimerTick(), "Should not return error after voting")
}

func TestHandleWaitForElectionTimerTick(t *testing.T) {
	ass := assert.New(t)
	b := giveMeATestBrain(5)
	latestIdx, latestLog := b.Brain.entries.Restore()

	// Restore should add one dummy item at the head, so idx will eq len
	ass.Equal(b.FakeStore.Saved.Len(), latestIdx, "Unexpected restore failure: idx mismatch")
	if diff := cmp.Diff(b.FakeStore.Saved.LatestLog(), latestLog); diff != "" {
		ass.FailNowf("Unexpected restore failure: latest log mismatch", "mismatch (-want +got):\n%s", diff)
	}

	b.Brain.deps.Transport.RegisterSelf(b.Brain.id, b.Brain.addr, b.Brain.handleRPC, b.TransportCfg)
	b.Brain.raftState.RegisterNode(b.Brain.id)

	for _, fellow := range b.Brain.fellows {
		b.Brain.deps.Transport.RegisterPeer(fellow.Id, fellow.Addr, b.Brain.handleRPC, b.TransportCfg)
		b.Brain.raftState.RegisterNode(fellow.Id)

		if fellow.ReqMap == nil {
			fellow.ReqMap = make(map[uint32]rpc.RpcPayload, 0)
		}
	}

	ass.ErrorIs(core.RaftStateNotACandidateErr, b.Brain.handleWaitForElectionTimerTick(), "Election should not start if role is still follower")
	b.Brain.raftState.UpdateRole(core.RAFT_ROLE_CANDIDATE)

	ass.NoError(b.Brain.handleWaitForElectionTimerTick(), "Election should not have error after promoted to candidate")

	lastLogIdx, lastLog := b.Brain.entries.LatestLog()
	payload := &rpc.RequestVoteReq{
		Term:         b.Brain.raftState.Term(),
		CandidateId:  b.Brain.id,
		LastLogIndex: lastLogIdx,
		LastLogTerm:  lastLog.Term,
	}
	payloadBs := payload.Encode()

	for _, fellow := range b.Brain.fellows {
		if diff := cmp.Diff(payload, fellow.ReqMap[fellow.RelationId.Load()]); diff != "" {
			ass.Failf("Related Payload", "mismatch for %s (-want +got):\n%s", fellow.Id, diff)
		}
		expectedFrame := rpc.Frame{
			RPCType:    rpc.RPC_TYPE_REQUEST_VOTE_REQ,
			RelationId: fellow.RelationId.Load(),
			Payload:    payloadBs,
		}.Encode()
		conn := b.FakeTransport.ConnMap[fellow.Id]
		written := <-conn.WrittenCh

		if diff := cmp.Diff(expectedFrame, written); diff != "" {
			ass.Failf("Request Vote Request", "mismatch for %s (-want +got):\n%s", fellow.Id, diff)
		}
	}

	b.Brain.raftState.StopElection()
	newErrErr := errors.New("404 not found in the system 404 the new era era")
	b.Brain.Close(newErrErr)
	ass.ErrorIs(newErrErr, b.Brain.handleWaitForElectionTimerTick(), "Election should not start after brain is closed")
}

func TestWaitForElectionTimer(t *testing.T) {
	t.Run("Cant stop election", func(t *testing.T) {
		ass := assert.New(t)
		b := giveMeATestBrain(5)

		b.Deps.ElectionTimer.(*fakeTimer).PassTime()
		err := b.Brain.waitForElectionTimer(context.Background(), b.Brain.roleGen)

		ass.ErrorIs(err, core.RaftStateNotACandidateErr, "Should return error when election stopping fails")
	})

	t.Run("Role Gen Incremented", func(t *testing.T) {
		ass := assert.New(t)
		b := giveMeATestBrain(5)
		b.Brain.raftState.UpdateRole(core.RAFT_ROLE_CANDIDATE)
		b.Brain.raftState.UpdateRole(core.RAFT_ROLE_LEADER)

		b.Brain.roleGen++
		err := b.Brain.waitForElectionTimer(context.Background(), b.Brain.roleGen-1)

		ass.ErrorIs(err, BrainRoleGenExpErr, "Role Gen checking is not implemented")
	})

	t.Run("Context Done", func(t *testing.T) {
		ass := assert.New(t)
		b := giveMeATestBrain(5)
		b.Brain.raftState.UpdateRole(core.RAFT_ROLE_CANDIDATE)
		b.Brain.raftState.UpdateRole(core.RAFT_ROLE_LEADER)

		ctx, cancelFunc := context.WithCancel(context.Background())
		cancelFunc()
		err := b.Brain.waitForElectionTimer(ctx, b.Brain.roleGen)

		ass.ErrorIs(context.Canceled, err, "Ctx cancelling should cancel the wait")
	})

	t.Run("Brain Done", func(t *testing.T) {
		ass := assert.New(t)
		b := giveMeATestBrain(5)
		b.Brain.raftState.UpdateRole(core.RAFT_ROLE_CANDIDATE)
		b.Brain.raftState.UpdateRole(core.RAFT_ROLE_LEADER)

		newErrErr := errors.New("404 not found in the system 404 the new era era")
		b.Brain.Close(newErrErr)
		err := b.Brain.waitForElectionTimer(context.Background(), b.Brain.roleGen)

		ass.ErrorIs(newErrErr, err, "Brain closed should stop the timer")
	})
}

func TestHandleElectionTimerTick(t *testing.T) {
	ass := assert.New(t)
	b := giveMeATestBrain(5)

	err := b.Brain.handleElectionTimerTick()
	ass.ErrorIs(err, core.RaftStateNotACandidateErr, "Should return error when election stopping fails")

	b.Brain.raftState.UpdateRole(core.RAFT_ROLE_CANDIDATE)
	err = b.Brain.handleElectionTimerTick()
	ass.NoError(err, "Should have no error when election stopping succeed")
}

func TestHandleHeartBeatTimerTick(t *testing.T) {
	ass := assert.New(t)
	b := giveMeATestBrain(5)
	latestIdx, latestLog := b.Brain.entries.Restore()

	// Restore should add one dummy item at the head, so idx will eq len
	ass.Equal(b.FakeStore.Saved.Len(), latestIdx, "Unexpected restore failure: idx mismatch")
	if diff := cmp.Diff(b.FakeStore.Saved.LatestLog(), latestLog); diff != "" {
		ass.FailNowf("Unexpected restore failure: latest log mismatch", "mismatch (-want +got):\n%s", diff)
	}

	b.Brain.deps.Transport.RegisterSelf(b.Brain.id, b.Brain.addr, b.Brain.handleRPC, b.TransportCfg)
	b.Brain.raftState.RegisterNode(b.Brain.id)

	for _, fellow := range b.Brain.fellows {
		b.Brain.deps.Transport.RegisterPeer(fellow.Id, fellow.Addr, b.Brain.handleRPC, b.TransportCfg)
		b.Brain.raftState.RegisterNode(fellow.Id)

		if fellow.ReqMap == nil {
			fellow.ReqMap = make(map[uint32]rpc.RpcPayload, 0)
		}
	}

	ass.ErrorIs(core.RaftStateNotALeaderErr, b.Brain.handleHeartBeatTimerTick(), "Heartbeat should not be sent if role is still follower")

	prevLogIdx, prevLog := b.Brain.entries.LatestLog()
	b.Brain.raftState.UpdateRole(core.RAFT_ROLE_CANDIDATE)
	b.Brain.raftState.UpdateRole(core.RAFT_ROLE_LEADER)
	b.Brain.raftState.InitAsLeader(b.Brain.id, prevLogIdx)

	ass.NoError(b.Brain.handleHeartBeatTimerTick(), "Send heartbeat should not have error after promoted to leader")

	payload := &rpc.AppendEntriesReq{
		Term:         b.Brain.raftState.Term(),
		LeaderId:     b.Brain.id,
		PrevLogIndex: prevLogIdx,
		PrevLogTerm:  prevLog.Term,
		LeaderCommit: b.Brain.raftState.CommitIdx(),
		Entries:      []byte{},
	}
	payloadBs := payload.Encode()

	for _, fellow := range b.Brain.fellows {
		if diff := cmp.Diff(payload, fellow.ReqMap[fellow.RelationId.Load()]); diff != "" {
			ass.Failf("Related Payload", "mismatch for %s (-want +got):\n%s", fellow.Id, diff)
		}
		expectedFrame := rpc.Frame{
			RPCType:    rpc.RPC_TYPE_APPEND_ENTRIES_REQ,
			RelationId: fellow.RelationId.Load(),
			Payload:    payloadBs,
		}
		conn := b.FakeTransport.ConnMap[fellow.Id]
		written := <-conn.WrittenCh

		if diff := cmp.Diff(expectedFrame, rpc.DecodeRPCFrame(written)); diff != "" {
			ass.Failf("Request Vote Request", "mismatch for %s (-want +got):\n%s", fellow.Id, diff)
		}
	}

	newErrErr := errors.New("404 not found in the system 404 the new era era")
	b.Brain.Close(newErrErr)
	ass.ErrorIs(newErrErr, b.Brain.handleHeartBeatTimerTick(), "Should not sent heartbeat after brain is closed")
}

func giveMeATestBrain(quorumCount int) *testBrain {
	ftr := &fakeTransport{
		ConnMap: make(map[string]*fakeConn),
	}
	fs := &fakeStore{
		Saved: core.AppendEntries{
			{
				Term:         1,
				Action:       core.STATE_ADD,
				CounterDelta: 1,
			},
			{
				Term:         2,
				Action:       core.STATE_ADD,
				CounterDelta: 1,
			},
			{
				Term:         3,
				Action:       core.STATE_ADD,
				CounterDelta: 1,
			},
		},
	}
	now := time.Now()
	deps := &BrainDeps{
		ElectionTimer: &fakeTimer{
			Duration: 1 * time.Second,
			Now:      now,
			CCh:      make(chan time.Time, 10),
		},
		WaitForElectionTimer: &fakeTimer{
			Duration: 1 * time.Second,
			Now:      now,
			CCh:      make(chan time.Time, 10),
		},
		ElectionTimeoutTimer: &fakeTimer{
			Duration: 1 * time.Second,
			Now:      now,
			CCh:      make(chan time.Time, 10),
		},
		HeartbeatTimer: &fakeTimer{
			Duration: 1 * time.Second,
			Now:      now,
			CCh:      make(chan time.Time, 10),
		},
		EntriesStore: fs,
		Transport:    ftr,
	}
	cfg := &BrainConfig{
		Id:      "iamhaha",
		Addr:    "local",
		Fellows: map[string]*Fellow{},
	}
	fellows := []*Fellow{}

	for i := range quorumCount - 1 {
		id := strconv.Itoa(i)
		cfg.Fellows[id] = &Fellow{
			Id:   id,
			Addr: id,
		}
		fellows = append(fellows, cfg.Fellows[id])
	}

	l := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	b := NewBrain(l, deps, cfg)

	tcfg := core.TransportCfg{}

	return &testBrain{
		FakeTransport: ftr,
		FakeStore:     fs,
		Deps:          deps,
		BrainCfg:      cfg,
		Brain:         b,
		TransportCfg:  tcfg,
		Fellows:       fellows,
	}
}

type testBrain struct {
	FakeTransport *fakeTransport
	FakeStore     *fakeStore
	Deps          *BrainDeps
	BrainCfg      *BrainConfig
	Brain         *Brain
	TransportCfg  core.TransportCfg
	Fellows       []*Fellow
}

type fakeTransport struct {
	ConnMap map[string]*fakeConn
}

func (t *fakeTransport) Send(id string, bs []byte) error {
	if t.ConnMap == nil {
		return errors.New("Id not registered")
	}
	if _, ok := t.ConnMap[id]; !ok {
		return errors.New("Id not registered")
	}

	_, err := t.ConnMap[id].Write(bs)
	return err
}

func (t *fakeTransport) RegisterSelf(id string, addr string, th core.TransportHandler, cfg core.TransportCfg) error {
	if _, ok := t.ConnMap[id]; ok {
		return errors.New("Self registered")
	}

	t.ConnMap[id] = &fakeConn{
		WrittenCh: make(chan []byte, 100),
	}

	return nil
}

func (t *fakeTransport) RegisterPeer(id string, addr string, th core.TransportHandler, cfg core.TransportCfg) error {
	if _, ok := t.ConnMap[id]; ok {
		return errors.New("Id registered")
	}
	t.ConnMap[id] = &fakeConn{
		WrittenCh: make(chan []byte, 100),
	}

	return nil
}

func (t *fakeTransport) CloseAll(reason error) {}

type fakeStore struct {
	Saved core.AppendEntries
}

func (s *fakeStore) ReplaceFrom(idx uint32, entries core.AppendEntries) {
	if s.Saved == nil {
		s.Saved = make(core.AppendEntries, 1)
	}

	if idx > uint32(len(s.Saved)) {
		return
	}

	s.Saved = append(s.Saved[:idx], entries...)
}

func (s *fakeStore) Restore() core.AppendEntries {
	return s.Saved
}

type fakeTimer struct {
	CCh      chan time.Time
	Duration time.Duration
	Now      time.Time
}

func (t *fakeTimer) C() <-chan time.Time {
	return t.CCh
}

func (f *fakeTimer) PassTime() {
	f.CCh <- f.Now.Add(f.Duration)
}

type fakeConn struct {
	WrittenCh chan []byte
}

func (f *fakeConn) Read() ([]byte, error) {
	return []byte{}, nil
}

func (f *fakeConn) Write(b []byte) (int, error) {
	f.WrittenCh <- b

	return len(b), nil
}

func (f *fakeConn) LocalAddr() net.Addr {
	return nil
}

func (f *fakeConn) RemoteAddr() net.Addr {
	return nil
}

func (f *fakeConn) SetDeadline(t time.Time) error {
	return nil
}

func (f *fakeConn) SetReadDeadline(t time.Time) error {
	return nil
}

func (f *fakeConn) SetWriteDeadline(t time.Time) error {
	return nil
}
