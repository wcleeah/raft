package core_test

import (
	"flag"
	"os"
	"path/filepath"
	"testing"

	"com.lwc.raft/internal/core"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
)

var update = flag.Bool("update", false, "update golden files")

func TestAppendEntryEncode(t *testing.T) {
	ae := core.AppendEntry{
		Term:         1,
		CounterDelta: 2,
		Action:       core.STATE_ADD,
	}

	cmpBytesWithGolden(t, "AppendEntryEncode.golden", ae.Encode())
}

func TestAppendEntryDecode(t *testing.T) {
	ae := core.AppendEntry{
		Term:         3,
		CounterDelta: 4,
		Action:       core.STATE_MINUS,
	}

	expected := ae.Encode()
	golden := getBytesFromGolden(t, "AppendEntryDecode.golden", expected)
	goldenAe := core.DecodeAppendEntry(golden)

	if diff := cmp.Diff(ae, goldenAe); diff != "" {
		t.Fatalf("AE mismatch (-want +got):\n%s", diff)
	}
}

func TestAppendEntryLayoutSync(t *testing.T) {
	ae := core.AppendEntry{
		Term:         3,
		CounterDelta: 4,
		Action:       core.STATE_MINUS,
	}

	out := ae.Encode()
	aeOut := core.DecodeAppendEntry(out)
	if diff := cmp.Diff(ae, aeOut); diff != "" {
		t.Fatalf("AppendEntry byte layout mismatch, (-want +got):\n%s", diff)
	}

}

func TestAppendEntriesLen(t *testing.T) {
	aes := core.AppendEntries{
		{
			Term:         1,
			CounterDelta: 2,
			Action:       core.STATE_ADD,
		},
		{
			Term:         3,
			CounterDelta: 4,
			Action:       core.STATE_ADD,
		},
		{
			Term:         5,
			CounterDelta: 6,
			Action:       core.STATE_FLIP,
		},
	}

	assert.Equal(t, uint32(3), aes.Len())
}

func TestAppendEntriesLastIndex(t *testing.T) {
	assert := assert.New(t)

	aes := core.AppendEntries{}
	assert.Equal(uint32(0), aes.LatestIdx())

	aes = append(aes, core.AppendEntry{})
	assert.Equal(uint32(0), aes.LatestIdx())

	aes = append(aes, core.AppendEntry{})
	assert.Equal(uint32(1), aes.LatestIdx())
}

func TestAppendEntriesLatestLog(t *testing.T) {
	aes := core.AppendEntries{}

	if diff := cmp.Diff(core.AppendEntry{}, aes.LatestLog()); diff != "" {
		t.Fatalf("AES LatestLog mismatch: no entry (-watch +got):\n%s", diff)
	}

	ae := core.AppendEntry{
		Term:         1,
		CounterDelta: 2,
		Action:       core.STATE_ADD,
	}
	aes = append(aes, ae)
	if diff := cmp.Diff(ae, aes.LatestLog()); diff != "" {
		t.Fatalf("AES LatestLog mismatch: one entry (-watch +got):\n%s", diff)
	}
}

func TestAppendEntriesCopy(t *testing.T) {
	aes := core.AppendEntries{
		{
			Term:         1,
			CounterDelta: 2,
			Action:       core.STATE_ADD,
		},
		{
			Term:         3,
			CounterDelta: 4,
			Action:       core.STATE_ADD,
		},
		{
			Term:         5,
			CounterDelta: 6,
			Action:       core.STATE_FLIP,
		},
	}

	if diff := cmp.Diff(aes, aes.Copy()); diff != "" {
		t.Fatalf("AES copy mismatch (-watch +got):\n%s", diff)
	}
}

func TestAppendEntriesEncode(t *testing.T) {
	aes := core.AppendEntries{
		{
			Term:         1,
			CounterDelta: 2,
			Action:       core.STATE_ADD,
		},
		{
			Term:         3,
			CounterDelta: 4,
			Action:       core.STATE_ADD,
		},
		{
			Term:         5,
			CounterDelta: 6,
			Action:       core.STATE_FLIP,
		},
	}

	cmpBytesWithGolden(t, "AppendEntriesEncode.golden", aes.Encode())
}

func TestAppendEntriesDecode(t *testing.T) {
	aes := core.AppendEntries{
		{
			Term:         5,
			CounterDelta: 6,
			Action:       core.STATE_FLIP,
		},
		{
			Term:         3,
			CounterDelta: 4,
			Action:       core.STATE_ADD,
		},
		{
			Term:         1,
			CounterDelta: 2,
			Action:       core.STATE_ADD,
		},
	}

	expected := aes.Encode()
	golden := getBytesFromGolden(t, "AppendEntriesDecode.golden", expected)
	goldenAes := core.DecodeAppendEntries(golden)

	if diff := cmp.Diff(aes, goldenAes); diff != "" {
		t.Fatalf("AES mismatch (-watch +got):\n%s", diff)
	}
}

func TestAppendEntriesByteLayoutSync(t *testing.T) {
	aes := core.AppendEntries{
		{
			Term:         5,
			CounterDelta: 6,
			Action:       core.STATE_FLIP,
		},
		{
			Term:         3,
			CounterDelta: 4,
			Action:       core.STATE_ADD,
		},
		{
			Term:         1,
			CounterDelta: 2,
			Action:       core.STATE_ADD,
		},
	}

	out := aes.Encode()
	aesOut := core.DecodeAppendEntries(out)

	if diff := cmp.Diff(aes, aesOut); diff != "" {
		t.Fatalf("AES byte layout mismatch (-watch +got):\n%s", diff)
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
