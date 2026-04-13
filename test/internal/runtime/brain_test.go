package runtime_test

import (
	"context"
	"errors"
	"fmt"
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

// THIS SERVE AS A PART OF DETERMINISTIC COORDINATION TESTING FOR THE RUNTIME.
// THE GOAL IS TO CHECK IF THE INTEGRATIONS WORKS CORRECTLY: RPC / TIMER, TRIGGERS A CORRECT RESULT.
// THIS WILL TEST:
//   - Inbound RPC communications: Triggered by transport, whether a response is received (some does not require a response)
//   - Outbound RPC communications: Triggered by timer tick, whether a request is received

func TestCandidatePromotion(t *testing.T) {
	b := giveMeATestBrain(3)

	reqVoteReq := rpc.Frame{
		RPCType: rpc.RPC_TYPE_REQUEST_VOTE_REQ,
		// First request, should be 1
		RelationId: 1,
		Payload: rpc.RequestVoteReq{
			Term:         b.FakeStore.Saved.LatestLog().Term + 1,
			CandidateId:  b.BrainCfg.Id,
			LastLogIndex: b.FakeStore.Saved.LatestIdx() + 1,
			LastLogTerm:  b.FakeStore.Saved.LatestLog().Term,
		}.Encode(),
	}.Encode()

	reqVoteRes := rpc.Frame{
		RPCType:    rpc.RPC_TYPE_REQUEST_VOTE_RES,
		RelationId: 1,
		Payload: rpc.RequestVoteRes{
			Term:        b.FakeStore.Saved.LatestLog().Term + 1,
			VoteGranted: true,
		}.Encode(),
	}.Encode()

	firstAppEntReq := rpc.Frame{
		RPCType:    rpc.RPC_TYPE_APPEND_ENTRIES_REQ,
		RelationId: 2,
		Payload: rpc.AppendEntriesReq{
			Term:         b.FakeStore.Saved.LatestLog().Term + 1,
			LeaderCommit: 0,
			PrevLogIndex: b.FakeStore.Saved.LatestIdx() + 1,
			PrevLogTerm:  b.FakeStore.Saved.LatestLog().Term,
			LeaderId:     b.BrainCfg.Id,
			Entries:      []byte{},
		}.Encode(),
	}.Encode()

	secondAppEntReq := rpc.Frame{
		RPCType:    rpc.RPC_TYPE_APPEND_ENTRIES_REQ,
		RelationId: 3,
		Payload: rpc.AppendEntriesReq{
			Term:         b.FakeStore.Saved.LatestLog().Term + 1,
			LeaderCommit: 0,
			PrevLogIndex: b.FakeStore.Saved.LatestIdx() + 1,
			PrevLogTerm:  b.FakeStore.Saved.LatestLog().Term,
			LeaderId:     b.BrainCfg.Id,
			Entries:      []byte{},
		}.Encode(),
	}.Encode()

	run(t, b, []testStep{
		startBrain(),
		passTime("ElectionTimeoutTimer"),
		passTime("WaitForElectionTimer"),
		checkBroadcastedRpc("Request Vote Req", reqVoteReq),
		replyRpc("Request Vote Res", b.Fellows[0].Id, reqVoteRes),
		checkBroadcastedRpc("Append Entries Request", firstAppEntReq),
		passTime("HeartbeatTimer"),
		checkBroadcastedRpc("Append Entries Request", secondAppEntReq),
	})
}

func TestFellowHaveHigherTerm(t *testing.T) {
	t.Run("Append Entries", func(t *testing.T) {
		b := giveMeATestBrain(3)

		appEntReq := rpc.Frame{
			RPCType:    rpc.RPC_TYPE_APPEND_ENTRIES_REQ,
			RelationId: 2,
			Payload: rpc.AppendEntriesReq{
				Term:         b.FakeStore.Saved.LatestLog().Term + 1,
				LeaderCommit: 0,
				PrevLogIndex: b.FakeStore.Saved.LatestIdx() + 1,
				PrevLogTerm:  b.FakeStore.Saved.LatestLog().Term,
				LeaderId:     b.Fellows[0].Id,
				Entries:      []byte{},
			}.Encode(),
		}.Encode()

		appEntRes := rpc.Frame{
			RPCType:    rpc.RPC_TYPE_APPEND_ENTRIES_RES,
			RelationId: 2,
			Payload: rpc.AppendEntriesRes{
				Term:    b.FakeStore.Saved.LatestLog().Term + 1,
				Success: true,
			}.Encode(),
		}.Encode()

		run(t, b, []testStep{
			startBrain(),
			sendInboundRpc("Append Entries Req", appEntReq),
			checkOutboundRpc("Append Entries Res", appEntRes),
		})
	})

	t.Run("Request Vote Request Term Bigger", func(t *testing.T) {
		b := giveMeATestBrain(3)
		inboundReqVoteReq := rpc.Frame{
			RPCType: rpc.RPC_TYPE_REQUEST_VOTE_REQ,
			// First request, should be 1
			RelationId: 1,
			Payload: rpc.RequestVoteReq{
				Term:         b.FakeStore.Saved.LatestLog().Term + 2,
				CandidateId:  b.Fellows[0].Id,
				LastLogIndex: b.FakeStore.Saved.LatestIdx() + 1,
				LastLogTerm:  b.FakeStore.Saved.LatestLog().Term,
			}.Encode(),
		}.Encode()

		outBoundReqVoteRes := rpc.Frame{
			RPCType:    rpc.RPC_TYPE_REQUEST_VOTE_RES,
			RelationId: 1,
			Payload: rpc.RequestVoteRes{
				Term:        b.FakeStore.Saved.LatestLog().Term + 2,
				VoteGranted: true,
			}.Encode(),
		}.Encode()

		run(t, b, []testStep{
			startBrain(),
			sendInboundRpc("Request Vote Req", inboundReqVoteReq),
			checkOutboundRpc("Request Vote Res", outBoundReqVoteRes),
		})
	})
}

func TestRestartElection_AsCandidate(t *testing.T) {
	t.Run("Won election after restart", func(t *testing.T) {
		b := giveMeATestBrain(5)

		firstRoundBroadcastReqVoteReq := rpc.Frame{
			RPCType: rpc.RPC_TYPE_REQUEST_VOTE_REQ,
			// First request, should be 1
			RelationId: 1,
			Payload: rpc.RequestVoteReq{
				Term:         b.FakeStore.Saved.LatestLog().Term + 1,
				CandidateId:  b.BrainCfg.Id,
				LastLogIndex: b.FakeStore.Saved.LatestIdx() + 1,
				LastLogTerm:  b.FakeStore.Saved.LatestLog().Term,
			}.Encode(),
		}.Encode()

		firstRoundInboundReqVoteRes := rpc.Frame{
			RPCType:    rpc.RPC_TYPE_REQUEST_VOTE_RES,
			RelationId: 1,
			Payload: rpc.RequestVoteRes{
				Term:        b.FakeStore.Saved.LatestLog().Term + 1,
				VoteGranted: true,
			}.Encode(),
		}.Encode()

		secondRoundBroadcastReqVoteReq := rpc.Frame{
			RPCType:    rpc.RPC_TYPE_REQUEST_VOTE_REQ,
			RelationId: 2,
			Payload: rpc.RequestVoteReq{
				Term:         b.FakeStore.Saved.LatestLog().Term + 2,
				CandidateId:  b.BrainCfg.Id,
				LastLogIndex: b.FakeStore.Saved.LatestIdx() + 1,
				LastLogTerm:  b.FakeStore.Saved.LatestLog().Term,
			}.Encode(),
		}.Encode()

		secondRoundInboundReqVoteRes := rpc.Frame{
			RPCType:    rpc.RPC_TYPE_REQUEST_VOTE_RES,
			RelationId: 2,
			Payload: rpc.RequestVoteRes{
				Term:        b.FakeStore.Saved.LatestLog().Term + 2,
				VoteGranted: true,
			}.Encode(),
		}.Encode()

		appEntReq := rpc.Frame{
			RPCType:    rpc.RPC_TYPE_APPEND_ENTRIES_REQ,
			RelationId: 3,
			Payload: rpc.AppendEntriesReq{
				Term:         b.FakeStore.Saved.LatestLog().Term + 2,
				LeaderCommit: 0,
				PrevLogIndex: b.FakeStore.Saved.LatestIdx() + 1,
				PrevLogTerm:  b.FakeStore.Saved.LatestLog().Term,
				LeaderId:     b.BrainCfg.Id,
				Entries:      []byte{},
			}.Encode(),
		}.Encode()

		run(t, b, []testStep{
			startBrain(),
			passTime("ElectionTimeoutTimer"),
			passTime("WaitForElectionTimer"),
			checkBroadcastedRpc("First Round Request Vote Req", firstRoundBroadcastReqVoteReq),
			sendInboundRpc("First Round Request Vote Res", firstRoundInboundReqVoteRes),
			passTime("ElectionTimer"), // Restart Election
			passTime("WaitForElectionTimer"),
			checkBroadcastedRpc("Second Round Request Vote Req", secondRoundBroadcastReqVoteReq),
			sendInboundRpc("Second Round Request Vote Res #1", secondRoundInboundReqVoteRes), // One vote, not enough
			sendInboundRpc("Second Round Request Vote Res #2", secondRoundInboundReqVoteRes), // Second vote, should promote
			checkBroadcastedRpc("Append Entries Req", appEntReq),
		})
	})

	t.Run("Vote for someone after restart", func(t *testing.T) {
		b := giveMeATestBrain(5)

		firstRoundBroadcastReqVoteReq := rpc.Frame{
			RPCType: rpc.RPC_TYPE_REQUEST_VOTE_REQ,
			// First request, should be 1
			RelationId: 1,
			Payload: rpc.RequestVoteReq{
				Term:         b.FakeStore.Saved.LatestLog().Term + 1,
				CandidateId:  b.BrainCfg.Id,
				LastLogIndex: b.FakeStore.Saved.LatestIdx() + 1,
				LastLogTerm:  b.FakeStore.Saved.LatestLog().Term,
			}.Encode(),
		}.Encode()

		firstRoundInboundReqVoteRes := rpc.Frame{
			RPCType:    rpc.RPC_TYPE_REQUEST_VOTE_RES,
			RelationId: 1,
			Payload: rpc.RequestVoteRes{
				Term:        b.FakeStore.Saved.LatestLog().Term + 1,
				VoteGranted: true,
			}.Encode(),
		}.Encode()

		inboundReqVoteReq := rpc.Frame{
			RPCType: rpc.RPC_TYPE_REQUEST_VOTE_REQ,
			// First request, should be 1
			RelationId: 1,
			Payload: rpc.RequestVoteReq{
				Term:         b.FakeStore.Saved.LatestLog().Term + 2,
				CandidateId:  b.Fellows[1].Id,
				LastLogIndex: b.FakeStore.Saved.LatestIdx() + 1,
				LastLogTerm:  b.FakeStore.Saved.LatestLog().Term,
			}.Encode(),
		}.Encode()

		outBoundReqVoteRes := rpc.Frame{
			RPCType:    rpc.RPC_TYPE_REQUEST_VOTE_RES,
			RelationId: 1,
			Payload: rpc.RequestVoteRes{
				Term:        b.FakeStore.Saved.LatestLog().Term + 2,
				VoteGranted: true,
			}.Encode(),
		}.Encode()

		run(t, b, []testStep{
			startBrain(),
			passTime("ElectionTimeoutTimer"),
			passTime("WaitForElectionTimer"),
			checkBroadcastedRpc("First Round Request Vote Req", firstRoundBroadcastReqVoteReq),
			sendInboundRpc("First Round Request Vote Res", firstRoundInboundReqVoteRes), // One vote, not enough
			passTime("ElectionTimer"), // Restart Election
			sendInboundRpc("Request Vote Req", inboundReqVoteReq),
			checkOutboundRpc("Request Vote Res", outBoundReqVoteRes),
		})
	})
}

func TestRestartElection_AsFollower(t *testing.T) {
	b := giveMeATestBrain(5)

	firstRoundInboundReqVoteReq := rpc.Frame{
		RPCType: rpc.RPC_TYPE_REQUEST_VOTE_REQ,
		// First request, should be 1
		RelationId: 1,
		Payload: rpc.RequestVoteReq{
			Term:         b.FakeStore.Saved.LatestLog().Term + 1,
			CandidateId:  b.Fellows[0].Id,
			LastLogIndex: b.FakeStore.Saved.LatestIdx() + 1,
			LastLogTerm:  b.FakeStore.Saved.LatestLog().Term,
		}.Encode(),
	}.Encode()

	firstRoundInboundReqVoteRes := rpc.Frame{
		RPCType:    rpc.RPC_TYPE_REQUEST_VOTE_RES,
		RelationId: 1,
		Payload: rpc.RequestVoteRes{
			Term:        b.FakeStore.Saved.LatestLog().Term + 1,
			VoteGranted: true,
		}.Encode(),
	}.Encode()

	secondRoundInboundReqVoteReq := rpc.Frame{
		RPCType: rpc.RPC_TYPE_REQUEST_VOTE_REQ,
		// First request, should be 1
		RelationId: 1,
		Payload: rpc.RequestVoteReq{
			Term:         b.FakeStore.Saved.LatestLog().Term + 2,
			CandidateId:  b.Fellows[1].Id,
			LastLogIndex: b.FakeStore.Saved.LatestIdx() + 1,
			LastLogTerm:  b.FakeStore.Saved.LatestLog().Term,
		}.Encode(),
	}.Encode()

	secondRoundOutBoundReqVoteRes := rpc.Frame{
		RPCType:    rpc.RPC_TYPE_REQUEST_VOTE_RES,
		RelationId: 1,
		Payload: rpc.RequestVoteRes{
			Term:        b.FakeStore.Saved.LatestLog().Term + 2,
			VoteGranted: true,
		}.Encode(),
	}.Encode()

	run(t, b, []testStep{
		startBrain(),
		sendInboundRpc("First Round Request Vote Req", firstRoundInboundReqVoteReq),
		checkOutboundRpc("First Round Request Vote Res", firstRoundInboundReqVoteRes),
		sendInboundRpc("Second Round Request Vote Req", secondRoundInboundReqVoteReq),
		checkOutboundRpc("Second Round Request Vote Res", secondRoundOutBoundReqVoteRes),
	})
}

func TestVoting_RejectVote(t *testing.T) {
	t.Run("Voted", func(t *testing.T) {
		b := giveMeATestBrain(5)

		firstRoundInboundReqVoteReq := rpc.Frame{
			RPCType: rpc.RPC_TYPE_REQUEST_VOTE_REQ,
			// First request, should be 1
			RelationId: 1,
			Payload: rpc.RequestVoteReq{
				Term:         b.FakeStore.Saved.LatestLog().Term + 1,
				CandidateId:  b.Fellows[0].Id,
				LastLogIndex: b.FakeStore.Saved.LatestIdx() + 1,
				LastLogTerm:  b.FakeStore.Saved.LatestLog().Term,
			}.Encode(),
		}.Encode()

		firstRoundInboundReqVoteRes := rpc.Frame{
			RPCType:    rpc.RPC_TYPE_REQUEST_VOTE_RES,
			RelationId: 1,
			Payload: rpc.RequestVoteRes{
				Term:        b.FakeStore.Saved.LatestLog().Term + 1,
				VoteGranted: true,
			}.Encode(),
		}.Encode()

		secondRoundInboundReqVoteReq := rpc.Frame{
			RPCType: rpc.RPC_TYPE_REQUEST_VOTE_REQ,
			// First request, should be 1
			RelationId: 1,
			Payload: rpc.RequestVoteReq{
				Term:         b.FakeStore.Saved.LatestLog().Term + 1,
				CandidateId:  b.Fellows[1].Id,
				LastLogIndex: b.FakeStore.Saved.LatestIdx() + 1,
				LastLogTerm:  b.FakeStore.Saved.LatestLog().Term,
			}.Encode(),
		}.Encode()

		secondRoundOutBoundReqVoteRes := rpc.Frame{
			RPCType:    rpc.RPC_TYPE_REQUEST_VOTE_RES,
			RelationId: 1,
			Payload: rpc.RequestVoteRes{
				Term:        b.FakeStore.Saved.LatestLog().Term + 1,
				VoteGranted: false,
			}.Encode(),
		}.Encode()

		run(t, b, []testStep{
			startBrain(),
			sendInboundRpc("First Round Request Vote Req", firstRoundInboundReqVoteReq),
			checkOutboundRpc("First Round Request Vote Res", firstRoundInboundReqVoteRes),
			sendInboundRpc("Second Round Request Vote Req", secondRoundInboundReqVoteReq),
			checkOutboundRpc("Second Round Request Vote Res", secondRoundOutBoundReqVoteRes),
		})
	})

	t.Run("Last Log Mismatch", func(t *testing.T) {
		b := giveMeATestBrain(5)

		firstRoundInboundReqVoteReq := rpc.Frame{
			RPCType: rpc.RPC_TYPE_REQUEST_VOTE_REQ,
			// First request, should be 1
			RelationId: 1,
			Payload: rpc.RequestVoteReq{
				Term:         b.FakeStore.Saved.LatestLog().Term + 1,
				CandidateId:  b.Fellows[0].Id,
				LastLogIndex: b.FakeStore.Saved.LatestIdx() + 2,
				LastLogTerm:  b.FakeStore.Saved.LatestLog().Term - 1,
			}.Encode(),
		}.Encode()

		secondRoundInboundReqVoteReq := rpc.Frame{
			RPCType: rpc.RPC_TYPE_REQUEST_VOTE_REQ,
			// First request, should be 1
			RelationId: 1,
			Payload: rpc.RequestVoteReq{
				Term:         b.FakeStore.Saved.LatestLog().Term + 1,
				CandidateId:  b.Fellows[1].Id,
				LastLogIndex: b.FakeStore.Saved.LatestIdx() - 1,
				LastLogTerm:  b.FakeStore.Saved.LatestLog().Term - 1,
			}.Encode(),
		}.Encode()

		voteNotGrantedInboundReqVoteRes := rpc.Frame{
			RPCType:    rpc.RPC_TYPE_REQUEST_VOTE_RES,
			RelationId: 1,
			Payload: rpc.RequestVoteRes{
				Term:        b.FakeStore.Saved.LatestLog().Term + 1,
				VoteGranted: false,
			}.Encode(),
		}.Encode()

		run(t, b, []testStep{
			startBrain(),
			sendInboundRpc("First Round Request Vote Req", firstRoundInboundReqVoteReq),
			checkOutboundRpc("First Round Request Vote Res", voteNotGrantedInboundReqVoteRes),
			sendInboundRpc("Second Round Request Vote Req", secondRoundInboundReqVoteReq),
			checkOutboundRpc("Second Round Request Vote Res", voteNotGrantedInboundReqVoteRes),
		})
	})

	t.Run("Current Term Larger", func(t *testing.T) {
		b := giveMeATestBrain(5)

		firstRoundInboundReqVoteReq := rpc.Frame{
			RPCType: rpc.RPC_TYPE_REQUEST_VOTE_REQ,
			// First request, should be 1
			RelationId: 1,
			Payload: rpc.RequestVoteReq{
				Term:         b.FakeStore.Saved.LatestLog().Term - 1,
				CandidateId:  b.Fellows[0].Id,
				LastLogIndex: b.FakeStore.Saved.LatestIdx() + 1,
				LastLogTerm:  b.FakeStore.Saved.LatestLog().Term,
			}.Encode(),
		}.Encode()

		firstRoundInboundReqVoteRes := rpc.Frame{
			RPCType:    rpc.RPC_TYPE_REQUEST_VOTE_RES,
			RelationId: 1,
			Payload: rpc.RequestVoteRes{
				Term:        b.FakeStore.Saved.LatestLog().Term,
				VoteGranted: false,
			}.Encode(),
		}.Encode()

		run(t, b, []testStep{
			startBrain(),
			sendInboundRpc("First Round Request Vote Req", firstRoundInboundReqVoteReq),
			checkOutboundRpc("First Round Request Vote Req", firstRoundInboundReqVoteRes),
		})
	})

}

func giveMeATestBrain(quorumCount int) *testBrain {
	ftr := &fakeTransport{
		ConnMap: make(map[string]*fakeConn),
	}
	fs := &fakeStore{
		Saved: core.AppendEntries{
			{
				Term: 1,
			},
			{
				Term: 2,
			},
			{
				Term: 3,
			},
		},
	}
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
		Id:      "iamhaha",
		Addr:    "local",
		Fellows: map[string]*runtime.Fellow{},
	}
	fellows := []*runtime.Fellow{}

	for i := range quorumCount - 1 {
		id := strconv.Itoa(i)
		cfg.Fellows[id] = &runtime.Fellow{
			Id:   id,
			Addr: id,
		}
		fellows = append(fellows, cfg.Fellows[id])
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
		Fellows:       fellows,
	}
}

type testStep struct {
	Name string
	F    func(ass *assert.Assertions, b *testBrain) bool
}

func run(t *testing.T, b *testBrain, steps []testStep) {
	t.Helper()

	for _, step := range steps {
		ok := t.Run(step.Name, func(t *testing.T) {
			ass := assert.New(t)
			step.F(ass, b)
		})

		if !ok {
			t.Fatalf("Step %s Failed", step.Name)
		}
	}
}

func startBrain() testStep {
	return testStep{
		Name: "Start Brain",
		F: func(ass *assert.Assertions, b *testBrain) bool {
			b.Brain.Start(context.Background(), b.TransportCfg)
			return ass.Equal(len(b.BrainCfg.Fellows)+1, len(b.FakeTransport.ConnMap), "Fellow not regiestered")
		},
	}
}

func passTime(timerName string) testStep {
	return testStep{
		Name: fmt.Sprintf("Passing time for timer:  %s", timerName),
		F: func(ass *assert.Assertions, b *testBrain) bool {
			var timer *fakeTimer
			switch timerName {
			case "ElectionTimer":
				timer = b.Deps.ElectionTimer.(*fakeTimer)
			case "WaitForElectionTimer":
				timer = b.Deps.WaitForElectionTimer.(*fakeTimer)
			case "ElectionTimeoutTimer":
				timer = b.Deps.ElectionTimeoutTimer.(*fakeTimer)
			case "HeartbeatTimer":
				timer = b.Deps.HeartbeatTimer.(*fakeTimer)
			}
			if timer != nil {
				timer.PassTime()
			}
			return true
		},
	}
}

func checkBroadcastedRpc(rpc string, bs []byte) testStep {
	return testStep{
		Name: fmt.Sprintf("Check Broadcasted RPC: %s", rpc),
		F: func(ass *assert.Assertions, b *testBrain) bool {
			ass.Equal(len(b.BrainCfg.Fellows)+1, len(b.FakeTransport.ConnMap), "Fellow not regiestered")
			ok := true

			for id, conn := range b.FakeTransport.ConnMap {
				if id == b.FakeTransport.SelfId {
					continue
				}
				// Ensure the write comes through, since we can't be sure the timing
				// Also put a escape hatch: timer stop if the program is bugged and the write never happened
				var gotBs []byte
				select {
				case gotBs = <-conn.WrittenCh:
				case <-time.After(3 * time.Second):
					ok = false
					ass.Failf("Check Broadcast RPC Failed", "Timer expired for %s", id)
				}
				ass.Equalf(0, len(conn.WrittenCh), "Unexpected write: %s", id)

				if diff := cmp.Diff(bs, gotBs); diff != "" {
					ok = false
					ass.Failf("Check Broadcast RPC Failed", "TestBs mismatch for %s (-want +got):\n%s", id, diff)
				}
			}

			return ok
		},
	}
}

func replyRpc(rpc string, id string, bs []byte) testStep {
	return testStep{
		Name: fmt.Sprintf("Broadcast Inbound RPC: %s", rpc),
		F: func(ass *assert.Assertions, b *testBrain) bool {
			ass.Equal(len(b.BrainCfg.Fellows)+1, len(b.FakeTransport.ConnMap), "Fellow not regiestered")

			conn := b.FakeTransport.ConnMap[id]
			conn.ReadCh <- bs
			return true
		},
	}
}

func sendInboundRpc(rpc string, bs []byte) testStep {
	return testStep{
		Name: fmt.Sprintf("Send Inbound RPC: %s", rpc),
		F: func(ass *assert.Assertions, b *testBrain) bool {
			ass.Equal(len(b.BrainCfg.Fellows)+1, len(b.FakeTransport.ConnMap), "Fellow not regiestered")

			conn, ok := b.FakeTransport.ConnMap[b.FakeTransport.SelfId]
			if !ok {
				ass.Failf("Send Inbound RPC", "Either you messed up, or the brain did not register this id: %s", "self")
				return false
			}

			conn.ReadCh <- bs
			return true
		},
	}
}

func checkOutboundRpc(rpc string, bs []byte) testStep {
	return testStep{
		Name: fmt.Sprintf("Check Outbound RPC: %s", rpc),
		F: func(ass *assert.Assertions, b *testBrain) bool {
			ass.Equal(len(b.BrainCfg.Fellows)+1, len(b.FakeTransport.ConnMap), "Fellow not regiestered")

			conn, ok := b.FakeTransport.ConnMap[b.FakeTransport.SelfId]
			if !ok {
				ass.Failf("Check Outbound RPC", "Either you messed up, or the brain did not register this id: %s", b.FakeTransport.SelfId)
				return false
			}

			// Ensure the write comes through, since we can't be sure the timing
			// Also put a escape hatch: timer stop if the program is bugged and the write never happened
			var gotBs []byte
			select {
			case gotBs = <-conn.WrittenCh:
			case <-time.After(3 * time.Second):
				ok = false
				ass.Failf("Check Outbound RPC", "Timer expired for %s", b.FakeTransport.SelfId)
			}
			ass.Equalf(0, len(conn.WrittenCh), "Unexpected write for %s", b.FakeTransport.SelfId)

			if diff := cmp.Diff(bs, gotBs); diff != "" {
				ok = false
				ass.Failf("Check Outbound RPC", "TestBs mismatch for %s (-want +got):\n%s", b.FakeTransport.SelfId, diff)
			}
			return true
		},
	}
}

type testBrain struct {
	FakeTransport *fakeTransport
	FakeStore     *fakeStore
	Deps          *runtime.BrainDeps
	BrainCfg      *runtime.BrainConfig
	Brain         *runtime.Brain
	TransportCfg  core.TransportCfg
	Fellows       []*runtime.Fellow
}

type fakeTransport struct {
	ConnMap map[string]*fakeConn
	SelfId  string
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
	t.SelfId = id

	t.ConnMap[t.SelfId] = &fakeConn{
		ReadCh:    make(chan []byte, 100),
		WrittenCh: make(chan []byte, 100),
	}

	go t.handleRead(t.SelfId, th)

	return nil
}

func (t *fakeTransport) RegisterPeer(id string, addr string, th core.TransportHandler, cfg core.TransportCfg) error {
	if _, ok := t.ConnMap[id]; ok {
		return errors.New("Id registered")
	}
	t.ConnMap[id] = &fakeConn{
		ReadCh:    make(chan []byte, 100),
		WrittenCh: make(chan []byte, 100),
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
		bs := <-conn.Read()

		res, err := th(id, bs)
		if err != nil {
			continue
		}
		conn.Write(res)
	}
}

type fakeStore struct {
	Saved core.AppendEntries
}

func (s *fakeStore) Append(entry core.AppendEntry) {
	if s.Saved == nil {
		s.Saved = make(core.AppendEntries, 1)
	}

	s.Saved = append(s.Saved, entry)
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
	mu sync.Mutex

	CCh      chan time.Time
	SCh      chan struct{}
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
	ReadCh    chan []byte
	WrittenCh chan []byte

	ReadDln  time.Time
	WriteDln time.Time
}

func (f *fakeConn) Read() <-chan []byte {
	return f.ReadCh
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
	f.ReadDln = t
	return nil
}

func (f *fakeConn) SetWriteDeadline(t time.Time) error {
	f.WriteDln = t
	return nil
}
