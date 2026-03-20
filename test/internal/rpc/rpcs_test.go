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

func TestAERToBytes(t *testing.T) {
	aer := rpc.AppendEntriesReq{
		Term:         1,
		LeaderCommit: 2,
		PrevLogIndex: 3,
		PrevLogTerm:  4,
		LeaderId:     5,
		Entries:      []byte("test i am a list of entries, i am also a string"),
	}

	out := aer.ToBytes()
	cmpBytesWithGolden(t, "AERToBytes.golden", out)
}

func TestAERFromBytes(t *testing.T) {
	aer := rpc.AppendEntriesReq{
		Term:         6,
		LeaderCommit: 7,
		PrevLogIndex: 8,
		PrevLogTerm:  9,
		LeaderId:     10,
		Entries:      []byte("hello from the other sideeeeeeeeee ii"),
	}

	out := aer.ToBytes()

	golden := getBytesFromGolden(t, "AERFromBytes.golden", out)
	aerOut := rpc.ToAppendEntriesReq(golden)

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
		LeaderId:     5,
		Entries:      []byte("different different different"),
	}

	out := aer.ToBytes()
	aerOut := rpc.ToAppendEntriesReq(out)
	if diff := cmp.Diff(aer, *aerOut); diff != "" {
		t.Fatalf("AER byte layout mismatch, (-want +got):\n%s", diff)
	}
}

func TestAEResToBytes(t *testing.T) {
	aer := rpc.AppendEntriesRes{
		Term:    11,
		Success: true,
	}

	out := aer.ToBytes()
	cmpBytesWithGolden(t, "AEResToBytes.golden", out)
}

func TestAEResFromBytes(t *testing.T) {
	aer := rpc.AppendEntriesRes{
		Term:    12,
		Success: false,
	}

	out := aer.ToBytes()

	golden := getBytesFromGolden(t, "AEResFromBytes.golden", out)
	aerOut := rpc.ToAppendEntriesRes(golden)

	if diff := cmp.Diff(aer, *aerOut); diff != "" {
		t.Fatalf("AERes mismatch (-want +got):\n%s", diff)
	}
}

func TestAEResByteLayoutSync(t *testing.T) {
	aer := rpc.AppendEntriesRes{
		Term:    13,
		Success: true,
	}

	out := aer.ToBytes()
	aerOut := rpc.ToAppendEntriesRes(out)
	if diff := cmp.Diff(aer, *aerOut); diff != "" {
		t.Fatalf("AERes byte layout mismatch, (-want +got):\n%s", diff)
	}
}

func TestRVReqToBytes(t *testing.T) {
	rvr := rpc.RequestVoteReq{
		Term:         13,
		CandidateId:  14,
		LastLogIndex: 15,
		LastLogTerm:  16,
	}

	out := rvr.ToBytes()
	cmpBytesWithGolden(t, "RVReqToBytes.golden", out)
}

func TestRVReqFromBytes(t *testing.T) {
	rvr := rpc.RequestVoteReq{
		Term:         17,
		CandidateId:  18,
		LastLogIndex: 19,
		LastLogTerm:  20,
	}

	out := rvr.ToBytes()

	golden := getBytesFromGolden(t, "RVReqFromBytes.golden", out)
	rvrOut := rpc.ToRequestVoteReq(golden)

	if diff := cmp.Diff(rvr, *rvrOut); diff != "" {
		t.Fatalf("RVReq mismatch (-want +got):\n%s", diff)
	}
}

func TestRVReqByteLayoutSync(t *testing.T) {
	rvr := rpc.RequestVoteReq{
		Term:         21,
		CandidateId:  22,
		LastLogIndex: 23,
		LastLogTerm:  24,
	}

	out := rvr.ToBytes()
	rvrOut := rpc.ToRequestVoteReq(out)
	if diff := cmp.Diff(rvr, *rvrOut); diff != "" {
		t.Fatalf("RVReq byte layout mismatch, (-want +got):\n%s", diff)
	}
}

func TestRVResToBytes(t *testing.T) {
	rvr := rpc.RequestVoteRes{
		Term:        21,
		VoteGranted: true,
	}

	out := rvr.ToBytes()
	cmpBytesWithGolden(t, "RVResToBytes.golden", out)
}

func TestRVResFromBytes(t *testing.T) {
	rvr := rpc.RequestVoteRes{
		Term:        22,
		VoteGranted: false,
	}

	out := rvr.ToBytes()

	golden := getBytesFromGolden(t, "RVResFromBytes.golden", out)
	rvrOut := rpc.ToRequestVoteRes(golden)

	if diff := cmp.Diff(rvr, *rvrOut); diff != "" {
		t.Fatalf("RVRes mismatch (-want +got):\n%s", diff)
	}
}

func TestRVResByteLayoutSync(t *testing.T) {
	rvr := rpc.RequestVoteRes{
		Term:        23,
		VoteGranted: true,
	}

	out := rvr.ToBytes()
	rvrOut := rpc.ToRequestVoteRes(out)
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
