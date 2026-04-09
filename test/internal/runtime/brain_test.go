package runtime_test

import (
	"errors"
	"log/slog"
	"net"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"com.lwc.raft/internal/core"
	"com.lwc.raft/internal/rpc"
	"com.lwc.raft/internal/runtime"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
)

// THIS SERVE AS A DETERMINISTIC TESTING FOR THE RUNTIME (kind of, you know go is not that deterministic)
//   - Deterministic timer / ticker
//   - Deterministic transport
//   - Fake follower
//
// THE GOAL IS TO CHECK IF EACH "TICK": RPC / TIMER, TRIGGERS A CORRECT RESULT

// 1. Start as follower, restore Store's AppendEntries
// 2. Election timeout
// 3. Wait for election
// 4. Send RequestVote RPC with correct Param
// 5. Receive vote from majority, become leader
// 6. Wait for heartbeat timer
// 6. Send AE with correct Param
func TestPromoteToLeader(t *testing.T) {
	ass := assert.New(t)
	tb := giveMeATestBrain(3)
	tb.FakeStore.Saved = core.AppendEntries{
		{
			Term: 1,
		},
		{
			Term: 2,
		},
		{
			Term: 3,
		},
	}

	reqVotReq := rpc.Frame{
		RPCType: rpc.RPC_TYPE_REQUEST_VOTE_REQ,
		// First request, should be 1
		RelationId: 1,
		Payload: rpc.RequestVoteReq{
			Term:         tb.FakeStore.Saved.LatestLog().Term + 1,
			CandidateId:  tb.BrainCfg.Id,
			LastLogIndex: tb.FakeStore.Saved.LatestIdx() + 1,
			LastLogTerm:  tb.FakeStore.Saved.LatestLog().Term,
		}.Encode(),
	}.Encode()
	reqVoteRes := rpc.Frame{
		RPCType:    rpc.RPC_TYPE_REQUEST_VOTE_RES,
		RelationId: 1,
		Payload: rpc.RequestVoteRes{
			Term:        tb.FakeStore.Saved.LatestLog().Term + 1,
			VoteGranted: true,
		}.Encode(),
	}.Encode()
	appEntReq := rpc.Frame{
		RPCType:    rpc.RPC_TYPE_APPEND_ENTRIES_REQ,
		RelationId: 2,
		Payload: rpc.AppendEntriesReq{
			Term:         tb.FakeStore.Saved.LatestLog().Term + 1,
			LeaderCommit: 0,
			PrevLogIndex: tb.FakeStore.Saved.LatestIdx() + 1,
			PrevLogTerm:  tb.FakeStore.Saved.LatestLog().Term,
			LeaderId:     tb.BrainCfg.Id,
			Entries:      []byte{},
		}.Encode(),
	}.Encode()

	run(t, ass, tb, []testStep{
		startBrain,
		passTime(tb.Deps.ElectionTimeoutTimer),
		passTime(tb.Deps.WaitForElectionTimer),
		checkOutboundRpc(reqVotReq),
		sendInboundRpc([][]byte{reqVoteRes}),
		checkOutboundRpc(appEntReq),
	})
}

func TestTiedVote_Promoted(t *testing.T) {
}

func TestTiedVote_Demoted(t *testing.T) {
}

func TestVoting(t *testing.T) {
}

func TestReplication_AsLeader(t *testing.T) {
}

func TestReplication_AsFollower(t *testing.T) {
}

func TestClientReq_AsLeader(t *testing.T) {
}

func TestClientReq_AsFollower(t *testing.T) {
}

func giveMeATestBrain(quorumCount int) *testBrain {
	ftr := &fakeTransport{
		ConnMap: make(map[string]*fakeConn),
	}
	fs := &fakeStore{}
	now := time.Now()
	deps := &runtime.BrainDeps{
		ElectionTimer: &fakeTimer{
			Duration: 1 * time.Second,
			Now:      now,
			CCh:      make(chan time.Time, 10),
			SCh:      make(chan struct{}, 10),
		},
		WaitForElectionTimer: &fakeTimer{
			Duration: 1 * time.Second,
			Now:      now,
			CCh:      make(chan time.Time, 10),
			SCh:      make(chan struct{}, 10),
		},
		ElectionTimeoutTimer: &fakeTimer{
			Duration: 1 * time.Second,
			Now:      now,
			CCh:      make(chan time.Time, 10),
			SCh:      make(chan struct{}, 10),
		},
		HeartbeatTimer: &fakeTimer{
			Duration: 1 * time.Second,
			Now:      now,
			CCh:      make(chan time.Time, 10),
			SCh:      make(chan struct{}, 10),
		},
		EntriesStore: fs,
		Transport:    ftr,
	}
	cfg := &runtime.BrainConfig{
		Id:   "iamhaha",
		Addr: "local",
		Fellows: map[string]*runtime.Fellow{},
	}

	for i := range quorumCount-1 {
		id := strconv.Itoa(i)
		cfg.Fellows[id] = &runtime.Fellow{
			Id: id,
			Addr: id,
		}
	}

	l := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	b := runtime.NewBrain(l, deps, cfg)

	tcfg := core.TransportCfg{}

	return &testBrain{
		FakeTransport: ftr,
		FakeStore:     fs,
		Deps:          deps,
		BrainCfg:      cfg,
		Brain:         b,
		TransportCfg:  tcfg,
	}
}

type testStep = func(ass *assert.Assertions, b *testBrain) bool

func run(t *testing.T, ass *assert.Assertions, b *testBrain, steps []testStep) bool {
	t.Helper()

	ok := true
	for _, step := range steps {
		ok = ok && step(ass, b)
	}

	return ok
}

func startBrain(ass *assert.Assertions, b *testBrain) bool {
	b.Brain.Start(b.TransportCfg)
	return ass.Equal(len(b.BrainCfg.Fellows)+1, len(b.FakeTransport.ConnMap), "Fellow not regiestered")
}

func passTime(timer core.Timer) testStep {
	return func(ass *assert.Assertions, b *testBrain) bool {
		timer.(*fakeTimer).PassTime()
		return true
	}
}

func checkOutboundRpc(bs []byte) testStep {
	return func(ass *assert.Assertions, b *testBrain) bool {
		ass.Equal(len(b.BrainCfg.Fellows)+1, len(b.FakeTransport.ConnMap), "Fellow not regiestered")
		ok := true

		for id, conn := range b.FakeTransport.ConnMap {
			if id == "self" {
				continue
			}
			// Ensure the write comes through, since we can't be sure the timing
			// Also put a escape hatch: timer stop if the program is bugged and the write never happened
			select {
			case <-conn.WrittenCh:
			case <-time.After(3 * time.Second):
				ok = false
				ass.Failf("RequestVote Failed", "Timer expired for %s", id)
			}

			if diff := cmp.Diff(bs, conn.ClearWriteBuff()); diff != "" {
				ok = false
				ass.Failf("RequestVote Failed", "TestBs mismatch for %s (-want +got):\n%s", id, diff)
			}
		}

		return ok
	}
}

func sendInboundRpc(bs [][]byte) testStep {
	return func(ass *assert.Assertions, b *testBrain) bool {
		ass.Equal(len(b.BrainCfg.Fellows)+1, len(b.FakeTransport.ConnMap), "Fellow not regiestered")

		idx := 0
		for id, conn := range b.FakeTransport.ConnMap {
			if id == "self" {
				continue
			}
			conn.AddReadBuf(bs[idx%len(bs)])
		}

		return true
	}
}

type testBrain struct {
	FakeTransport *fakeTransport
	FakeStore     *fakeStore
	Deps          *runtime.BrainDeps
	BrainCfg      *runtime.BrainConfig
	Brain         *runtime.Brain
	TransportCfg  core.TransportCfg
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

func (t *fakeTransport) RegisterSelf(addr string, th core.TransportHandler, cfg core.TransportCfg) error {
	if _, ok := t.ConnMap["self"]; ok {
		return errors.New("Self registered")
	}

	t.ConnMap["self"] = &fakeConn{
		ReadBuf:   make([]byte, 0),
		WriteBuf:  make([]byte, 0),
		ReadCh:    make(chan struct{}, 100),
		WrittenCh: make(chan struct{}, 100),
		Cond:      sync.NewCond(&sync.Mutex{}),
	}

	go t.handleRead("self", th)

	return nil
}

func (t *fakeTransport) RegisterPeer(id string, addr string, th core.TransportHandler, cfg core.TransportCfg) error {
	if _, ok := t.ConnMap[id]; ok {
		return errors.New("Id registered")
	}
	t.ConnMap[id] = &fakeConn{
		ReadBuf:   make([]byte, 0),
		WriteBuf:  make([]byte, 0),
		ReadCh:    make(chan struct{}, 100),
		WrittenCh: make(chan struct{}, 100),
		Cond:      sync.NewCond(&sync.Mutex{}),
	}

	go t.handleRead(id, th)

	return nil
}

func (t *fakeTransport) CloseAll(reason error) {}

func (t *fakeTransport) handleRead(id string, th core.TransportHandler) {
	conn, ok := t.ConnMap[id]
	if !ok {
		return
	}

	for {
		bs, err := conn.Read()

		// conn only return err for Read when server initiate the close
		// that means no need to read anymore
		if err != nil {
			return
		}
		th(id, bs)
	}
}

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

func (s *fakeStore) Restore() []byte {
	return s.Saved.Encode()
}

type fakeTimer struct {
	mu sync.Mutex

	CCh      chan time.Time
	SCh      chan struct{}
	Duration time.Duration
	Now      time.Time
}

func (t *fakeTimer) C() <-chan time.Time {
	return t.CCh
}

func (t *fakeTimer) S() <-chan struct{} {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.SCh == nil {
		t.SCh = make(chan struct{}, 100)
	}

	return t.SCh
}

func (t *fakeTimer) Stop() {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.SCh == nil {
		t.SCh = make(chan struct{}, 100)
		return
	}
	close(t.SCh)
	t.SCh = make(chan struct{}, 100)
}

func (f *fakeTimer) PassTime() {
	f.CCh <- f.Now.Add(f.Duration)
}

type fakeConn struct {
	Cond    *sync.Cond
	writeMu sync.Mutex

	ReadDln   time.Time
	WriteDln  time.Time
	ReadBuf   []byte
	WriteBuf  []byte
	ReadCh    chan struct{}
	WrittenCh chan struct{}
}

func (f *fakeConn) AddReadBuf(b []byte) {
	f.Cond.L.Lock()
	defer f.Cond.L.Unlock()
	defer f.Cond.Broadcast()

	f.ReadBuf = append(f.ReadBuf, b...)
}

func (f *fakeConn) Read() ([]byte, error) {
	f.Cond.L.Lock()
	defer f.Cond.L.Unlock()

	for len(f.ReadBuf) == 0 {
		f.Cond.Wait()
	}

	bs := make([]byte, len(f.ReadBuf))
	copy(bs, f.ReadBuf)
	f.ReadBuf = make([]byte, 0)
	f.ReadCh <- struct{}{}

	return bs, nil
}

func (f *fakeConn) Write(b []byte) (int, error) {
	f.writeMu.Lock()
	defer f.writeMu.Unlock()

	f.WriteBuf = append(f.WriteBuf, b...)
	f.WrittenCh <- struct{}{}

	return len(b), nil
}

func (f *fakeConn) ClearWriteBuff() []byte {
	f.writeMu.Lock()
	defer f.writeMu.Unlock()

	bs := make([]byte, len(f.WriteBuf))
	copy(bs, f.WriteBuf)
	f.WriteBuf = []byte{}

	return bs
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
	f.ReadDln = t
	return nil
}

func (f *fakeConn) SetWriteDeadline(t time.Time) error {
	f.WriteDln = t
	return nil
}
