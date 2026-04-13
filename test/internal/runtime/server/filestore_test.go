package server_test

import (
	"path/filepath"
	"sync"
	"testing"

	"com.lwc.raft/internal/core"
	"com.lwc.raft/internal/runtime/server"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
)

func TestFileStoreRestore(t *testing.T) {
	assert := assert.New(t)

	dir := t.TempDir()
	fs := &server.FileStore{Cond: sync.NewCond(&sync.Mutex{}), Path: filepath.Join(dir, "store.bin")}

	// missing file returns nil
	restored := fs.Restore()
	assert.Nil(restored)

	// write some entries then restore
	entries := core.AppendEntries{
		{
			Term:         1,
			CounterDelta: 10,
			Action:       core.STATE_ADD,
		},
		{
			Term:         2,
			CounterDelta: 5,
			Action:       core.STATE_MINUS,
		},
		{
			Term:         3,
			CounterDelta: 0,
			Action:       core.STATE_FLIP,
		},
	}
	fs.ReplaceFrom(1, entries)

	restored = fs.Restore()
	if diff := cmp.Diff(entries, restored); diff != "" {
		assert.Failf("Restore", "entries mismatch (-want +got):\n%s", diff)
	}
}

func TestFileStoreReplaceFrom(t *testing.T) {
	assert := assert.New(t)

	dir := t.TempDir()
	fs := &server.FileStore{Cond: sync.NewCond(&sync.Mutex{}), Path: filepath.Join(dir, "store.bin")}

	// initial write on empty store
	first := core.AppendEntries{
		{
			Term:         1,
			CounterDelta: 10,
			Action:       core.STATE_ADD,
		},
		{
			Term:         2,
			CounterDelta: 5,
			Action:       core.STATE_MINUS,
		},
		{
			Term:         3,
			CounterDelta: 0,
			Action:       core.STATE_FLIP,
		},
	}
	fs.ReplaceFrom(1, first)

	restored := fs.Restore()
	if diff := cmp.Diff(first, restored); diff != "" {
		assert.Failf("Initial write", "entries mismatch (-want +got):\n%s", diff)
	}

	// truncate-and-append: replace from raft index 3 (file index 2)
	second := core.AppendEntries{
		{Term: 4, CounterDelta: 7, Action: core.STATE_ADD},
		{Term: 5, CounterDelta: 2, Action: core.STATE_MINUS},
	}
	fs.ReplaceFrom(3, second)

	want := append(first[:2], second...)
	restored = fs.Restore()
	if diff := cmp.Diff(want, restored); diff != "" {
		assert.Failf("Truncate and append", "entries mismatch (-want +got):\n%s", diff)
	}
}

func TestFileStoreReplaceFromFailCase(t *testing.T) {
	dir := t.TempDir()
	fs := &server.FileStore{Cond: sync.NewCond(&sync.Mutex{}), Path: filepath.Join(dir, "store.bin")}

	entries := core.AppendEntries{
		{
			Term:         1,
			CounterDelta: 10,
			Action:       core.STATE_ADD,
		},
	}
	fs.ReplaceFrom(1, entries)

	// out-of-range index is a noop
	fs.ReplaceFrom(10, core.AppendEntries{
		{Term: 99, CounterDelta: 99, Action: core.STATE_FLIP},
	})

	restored := fs.Restore()
	if diff := cmp.Diff(entries, restored); diff != "" {
		t.Fatalf("entries should be unchanged (-want +got):\n%s", diff)
	}
}
