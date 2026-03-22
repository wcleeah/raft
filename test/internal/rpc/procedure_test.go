package rpc_test

import (
	"flag"
	"os"
	"path/filepath"
	"testing"

	"com.lwc.raft/internal/rpc"
	"github.com/google/go-cmp/cmp"
)

var update = flag.Bool("update", false, "update golden files")

func TestRPCBaseEncode(t *testing.T) {
	rb := rpc.Frame{
		RPCType:    1,
		RelationId: 2,
		Payload:    []byte("payload for rpc base"),
	}

	out := rb.Encode()
	cmpBytesWithGolden(t, "RPCBaseEncode.golden", out)
}

func TestRPCBaseDecode(t *testing.T) {
	rb := rpc.Frame{
		RPCType:    3,
		RelationId: 4,
		Payload:    []byte("payload from golden data"),
	}

	out := rb.Encode()

	golden := getBytesFromGolden(t, "RPCBaseDecode.golden", out)
	rbOut := rpc.DecodeRPCBase(golden)

	if diff := cmp.Diff(rb, *rbOut); diff != "" {
		t.Fatalf("RPCBase mismatch (-want +got):\n%s", diff)
	}
}

func TestRPCBaseByteLayoutSync(t *testing.T) {
	rb := rpc.Frame{
		RPCType:    5,
		RelationId: 6,
		Payload:    []byte("rpc base byte layout"),
	}

	out := rb.Encode()
	rbOut := rpc.DecodeRPCBase(out)
	if diff := cmp.Diff(rb, *rbOut); diff != "" {
		t.Fatalf("RPCBase byte layout mismatch, (-want +got):\n%s", diff)
	}
}

func TestAEREncode(t *testing.T) {
	aer := rpc.AppendEntriesReq{
		Term:         1,
		LeaderCommit: 2,
		PrevLogIndex: 3,
		PrevLogTerm:  4,
		LeaderId:     "leader-5",
		Entries:      []byte("test i am a list of entries, i am also a string"),
	}

	out := aer.Encode()
	cmpBytesWithGolden(t, "AEREncode.golden", out)
}

func TestAERDecode(t *testing.T) {
	aer := rpc.AppendEntriesReq{
		Term:         6,
		LeaderCommit: 7,
		PrevLogIndex: 8,
		PrevLogTerm:  9,
		LeaderId:     "leader-10",
		Entries:      []byte("hello from the other sideeeeeeeeee ii"),
	}

	out := aer.Encode()

	golden := getBytesFromGolden(t, "AERDecode.golden", out)
	aerOut := rpc.DecodeAppendEntriesReq(golden)

	if diff := cmp.Diff(aer, *aerOut); diff != "" {
		t.Fatalf("AER mismatch (-want +got):\n%s", diff)
	}
}

func TestAERByteLayoutSync(t *testing.T) {
	aer := rpc.AppendEntriesReq{
		Term:         1,
		LeaderCommit: 2,
		PrevLogIndex: 3,
		PrevLogTerm:  4,
		LeaderId:     "leader-5",
		Entries:      []byte("different different different"),
	}

	out := aer.Encode()
	aerOut := rpc.DecodeAppendEntriesReq(out)
	if diff := cmp.Diff(aer, *aerOut); diff != "" {
		t.Fatalf("AER byte layout mismatch, (-want +got):\n%s", diff)
	}
}

func TestAEResEncode(t *testing.T) {
	aer := rpc.AppendEntriesRes{
		Term:    11,
		Success: true,
	}

	out := aer.Encode()
	cmpBytesWithGolden(t, "AEResEncode.golden", out)
}

func TestAEResDecode(t *testing.T) {
	aer := rpc.AppendEntriesRes{
		Term:    12,
		Success: false,
	}

	out := aer.Encode()

	golden := getBytesFromGolden(t, "AEResDecode.golden", out)
	aerOut := rpc.DecodeAppendEntriesRes(golden)

	if diff := cmp.Diff(aer, *aerOut); diff != "" {
		t.Fatalf("AERes mismatch (-want +got):\n%s", diff)
	}
}

func TestAEResByteLayoutSync(t *testing.T) {
	aer := rpc.AppendEntriesRes{
		Term:    13,
		Success: true,
	}

	out := aer.Encode()
	aerOut := rpc.DecodeAppendEntriesRes(out)
	if diff := cmp.Diff(aer, *aerOut); diff != "" {
		t.Fatalf("AERes byte layout mismatch, (-want +got):\n%s", diff)
	}
}

func TestRVReqEncode(t *testing.T) {
	rvr := rpc.RequestVoteReq{
		Term:         13,
		CandidateId:  "candidate-14",
		LastLogIndex: 15,
		LastLogTerm:  16,
	}

	out := rvr.Encode()
	cmpBytesWithGolden(t, "RVReqEncode.golden", out)
}

func TestRVReqDecode(t *testing.T) {
	rvr := rpc.RequestVoteReq{
		Term:         17,
		CandidateId:  "candidate-18",
		LastLogIndex: 19,
		LastLogTerm:  20,
	}

	out := rvr.Encode()

	golden := getBytesFromGolden(t, "RVReqDecode.golden", out)
	rvrOut := rpc.DecodeRequestVoteReq(golden)

	if diff := cmp.Diff(rvr, *rvrOut); diff != "" {
		t.Fatalf("RVReq mismatch (-want +got):\n%s", diff)
	}
}

func TestRVReqByteLayoutSync(t *testing.T) {
	rvr := rpc.RequestVoteReq{
		Term:         21,
		CandidateId:  "candidate-22",
		LastLogIndex: 23,
		LastLogTerm:  24,
	}

	out := rvr.Encode()
	rvrOut := rpc.DecodeRequestVoteReq(out)
	if diff := cmp.Diff(rvr, *rvrOut); diff != "" {
		t.Fatalf("RVReq byte layout mismatch, (-want +got):\n%s", diff)
	}
}

func TestRVResEncode(t *testing.T) {
	rvr := rpc.RequestVoteRes{
		Term:        21,
		VoteGranted: true,
	}

	out := rvr.Encode()
	cmpBytesWithGolden(t, "RVResEncode.golden", out)
}

func TestRVResDecode(t *testing.T) {
	rvr := rpc.RequestVoteRes{
		Term:        22,
		VoteGranted: false,
	}

	out := rvr.Encode()

	golden := getBytesFromGolden(t, "RVResDecode.golden", out)
	rvrOut := rpc.DecodeRequestVoteRes(golden)

	if diff := cmp.Diff(rvr, *rvrOut); diff != "" {
		t.Fatalf("RVRes mismatch (-want +got):\n%s", diff)
	}
}

func TestRVResByteLayoutSync(t *testing.T) {
	rvr := rpc.RequestVoteRes{
		Term:        23,
		VoteGranted: true,
	}

	out := rvr.Encode()
	rvrOut := rpc.DecodeRequestVoteRes(out)
	if diff := cmp.Diff(rvr, *rvrOut); diff != "" {
		t.Fatalf("RVRes byte layout mismatch, (-want +got):\n%s", diff)
	}
}

func cmpBytesWithGolden(t *testing.T, path string, got []byte) {
	t.Helper()
	want := getBytesFromGolden(t, path, got)

	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatalf("byte mismatch (-want +got):\n%s", diff)
	}
}

func getBytesFromGolden(t *testing.T, path string, forUpdate []byte) []byte {
	t.Helper()
	golden := filepath.Join("goldenfiles", path)
	if *update {
		if err := os.WriteFile(golden, forUpdate, 0o644); err != nil {
			t.Fatal(err)
		}
	}

	want, err := os.ReadFile(golden)
	if err != nil {
		t.Fatal(err)
	}

	return want
}
