package core_test

import (
	"testing"

	"com.lwc.raft/internal/core"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
)

type FakeStore struct {
	Saved core.AppendEntries
}

func (s *FakeStore) Append(entry core.AppendEntry) {
	if s.Saved == nil {
		s.Saved = make(core.AppendEntries, 1)
	}

	s.Saved = append(s.Saved, entry)
}

func (s *FakeStore) ReplaceFrom(idx uint32, entries core.AppendEntries) {
	if s.Saved == nil {
		s.Saved = make(core.AppendEntries, 1)
	}

	if idx > uint32(len(s.Saved)) {
		return
	}

	s.Saved = append(s.Saved[:idx], entries...)
}

func (s *FakeStore) Restore() core.AppendEntries {
	return s.Saved
}

func TestAppendEntriesStoreAppendAndCopy(t *testing.T) {
	fs := &FakeStore{}

	store := &core.AppendEntriesStore{
		Store: fs,
	}

	aes := core.AppendEntries{
		{},
		{
			Term:         1,
			Action:       core.STATE_ADD,
			CounterDelta: 1,
		},
		{
			Term:         2,
			Action:       core.STATE_FLIP,
			CounterDelta: 2,
		},
		{
			Term:         3,
			Action:       core.STATE_MINUS,
			CounterDelta: 3,
		},
	}
	for _, entry := range aes[1:] {
		store.Append(entry)
	}

	if diff := cmp.Diff(aes, store.Copy()); diff != "" {
		t.Fatalf("AEStore Copy Entries mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(aes, fs.Saved); diff != "" {
		t.Fatalf("AEStore persisted entries mismatch (-want +got):\n%s", diff)
	}
}

func TestAppendEntriesStoreReplicate(t *testing.T) {
	assert := assert.New(t)

	fs := &FakeStore{}

	ae := &core.AppendEntriesStore{
		Store: fs,
	}

	// replicate the first batch entries
	// it should just save all of them
	firstEntries := core.AppendEntries{
		{
			Term:         3,
			CounterDelta: 3,
			Action:       core.STATE_ADD,
		}, {
			Term:         4,
			CounterDelta: 1,
			Action:       core.STATE_MINUS,
		}, {
			Term:   5,
			Action: core.STATE_FLIP,
		},
	}

	latestLogIdx, err := ae.Replicate(firstEntries.Encode(), 0, 0)

	assert.NoError(err)
	assert.Equal(uint32(3), latestLogIdx)

	entriesAfter := ae.GetHeartbeatEntries(1)
	if diff := cmp.Diff(firstEntries, entriesAfter); diff != "" {
		assert.Failf("First entries", "AEStore Entries mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(firstEntries, fs.Saved[1:]); diff != "" {
		assert.Failf("First entries", "FakeStore Entries mismatch (-want +got):\n%s", diff)
	}

	// replicate the second batch entries
	// it tests when prevLogIndex is not the latestLogIndex, which happens when leader changes, logs might diverge between node and leader
	secondEntries := core.AppendEntries{
		{
			Term:         3,
			CounterDelta: 3,
			Action:       core.STATE_ADD,
		}, {
			Term:         4,
			CounterDelta: 1,
			Action:       core.STATE_MINUS,
		}, {
			Term:   5,
			Action: core.STATE_FLIP,
		},
	}

	// 2, 4 -> this matches firstEntries second item
	latestLogIdx, err = ae.Replicate(secondEntries.Encode(), 2, 4)
	assert.NoError(err)
	assert.Equal(uint32(5), latestLogIdx)

	entriesAfter = ae.GetHeartbeatEntries(1)
	allEntries := append(firstEntries[:2], secondEntries...)

	if diff := cmp.Diff(allEntries, entriesAfter); diff != "" {
		assert.Failf("All entries", "entriesAfter mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(allEntries, fs.Saved[1:]); diff != "" {
		assert.Failf("All entries", "fake store mismatch (-want +got):\n%s", diff)
	}
}

func TestAppendEntriesStoreReplicateFailCase(t *testing.T) {
	assert := assert.New(t)

	ae := core.NewAppendEntriesStore(&FakeStore{})
	entries := core.AppendEntries{
		{
			Term:         2,
			CounterDelta: 1,
			Action:       core.STATE_ADD,
		},
	}

	_, err := ae.Replicate(entries.Encode(), 0, 0)
	assert.NoError(err)

	_, err = ae.Replicate(entries.Encode(), 3, 0)
	assert.EqualError(err, "Prev log index does not exist")

	_, err = ae.Replicate(entries.Encode(), 1, 1)
	assert.EqualError(err, "Prev log term does not match")

	_, err = ae.Replicate(entries.Encode(), 1, 3)
	assert.EqualError(err, "Prev log term does not match")

}

func TestAppendEntriesStoreApplyAll(t *testing.T) {
	assert := assert.New(t)

	ae := core.NewAppendEntriesStore(&FakeStore{})
	entries := core.AppendEntries{
		{
			Term:         3,
			CounterDelta: 3,
			Action:       core.STATE_ADD,
		}, {
			Term:         4,
			CounterDelta: 1,
			Action:       core.STATE_MINUS,
		}, {
			Term:   5,
			Action: core.STATE_FLIP,
		},
	}

	latestLogIndex, err := ae.Replicate(entries.Encode(), 0, 0)
	assert.NoError(err)

	applyEntries := ae.ApplyAll(latestLogIndex)

	if diff := cmp.Diff(entries, applyEntries); diff != "" {
		assert.Failf("Apply All", "Entries mismatch (-want +got):\n%s", diff)
	}

	emptyEntries := ae.ApplyAll(latestLogIndex)
	assert.Empty(emptyEntries)
}

func TestAppendEntriesStoreApplyAllFailedCase(t *testing.T) {
	assert := assert.New(t)

	ae := core.NewAppendEntriesStore(&FakeStore{})
	emptyEntries := ae.ApplyAll(100)
	assert.Empty(emptyEntries)
}

func TestAppendEntriesStoreLatestLog(t *testing.T) {
	assert := assert.New(t)
	ae := core.NewAppendEntriesStore(&FakeStore{})

	latestLogIdx, latestLog := ae.LatestLog()

	assert.Equal(uint32(0), latestLogIdx)
	if diff := cmp.Diff(core.AppendEntry{}, latestLog); diff != "" {
		assert.Failf("Latest Log", "Placeholder entry mismatch (-want +got):\n%s", diff)
	}

	entries := core.AppendEntries{
		{
			Term:         3,
			CounterDelta: 3,
			Action:       core.STATE_ADD,
		}, {
			Term:         4,
			CounterDelta: 1,
			Action:       core.STATE_MINUS,
		}, {
			Term:   5,
			Action: core.STATE_FLIP,
		},
	}

	_, err := ae.Replicate(entries.Encode(), 0, 0)
	assert.NoError(err)

	latestLogIdxAfterReplicate, latestLogAfterReplicate := ae.LatestLog()

	assert.Equal(uint32(3), latestLogIdxAfterReplicate)
	if diff := cmp.Diff(entries[2], latestLogAfterReplicate); diff != "" {
		assert.Failf("Latest Log", "Placeholder entry mismatch (-want +got):\n%s", diff)
	}
}

func TestAppendEntriesStoreRestore(t *testing.T) {
	entries := core.AppendEntries{
		{
			Term:         3,
			CounterDelta: 3,
			Action:       core.STATE_ADD,
		}, {
			Term:         4,
			CounterDelta: 1,
			Action:       core.STATE_MINUS,
		}, {
			Term:   5,
			Action: core.STATE_FLIP,
		},
	}

	ae := core.NewAppendEntriesStore(&FakeStore{
		Saved: entries,
	})

	ae.Restore()

	if diff := cmp.Diff(entries, ae.GetHeartbeatEntries(1)); diff != "" {
		t.Fatalf("Restore entry mismatch (-want +got):\n%s", diff)
	}
}

func TestGet(t *testing.T) {
	assert := assert.New(t)
	ae := core.NewAppendEntriesStore(&FakeStore{})
	_, err := ae.Get(1)

	assert.EqualError(err, "Idx overflow")
	entries := core.AppendEntries{
		{
			Term:         3,
			CounterDelta: 3,
			Action:       core.STATE_ADD,
		}, {
			Term:         4,
			CounterDelta: 1,
			Action:       core.STATE_MINUS,
		}, {
			Term:   5,
			Action: core.STATE_FLIP,
		},
	}

	_, err = ae.Replicate(entries.Encode(), 0, 0)
	assert.NoError(err)

	entry, err := ae.Get(1)
	assert.NoError(err)

	if diff := cmp.Diff(entries[0], entry); diff != "" {
		assert.Failf("Get entry", "entry mismatch (-want +got):\n%s", diff)
	}
}

func TestAppendEntriesStoreGetHeartbeatEntries(t *testing.T) {
	fs := &FakeStore{}

	store := &core.AppendEntriesStore{
		Store: fs,
	}

	aes := core.AppendEntries{
		{},
		{
			Term:         1,
			Action:       core.STATE_ADD,
			CounterDelta: 1,
		},
		{
			Term:         2,
			Action:       core.STATE_FLIP,
			CounterDelta: 2,
		},
		{
			Term:         3,
			Action:       core.STATE_MINUS,
			CounterDelta: 3,
		},
	}
	for _, entry := range aes[1:] {
		store.Append(entry)
	}

	if diff := cmp.Diff(aes, store.GetHeartbeatEntries(0)); diff != "" {
		t.Fatalf("AEStore Get Heartbeat Entries mismatch: 0 (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(aes[1:], store.GetHeartbeatEntries(1)); diff != "" {
		t.Fatalf("AEStore Get Heartbeat Entries mismatch: 1 (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(aes[3:], store.GetHeartbeatEntries(3)); diff != "" {
		t.Fatalf("AEStore Get Heartbeat Entries mismatch: 3 (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(core.AppendEntries{}, store.GetHeartbeatEntries(8)); diff != "" {
		t.Fatalf("AEStore Get Heartbeat Entries mismatch: 8 (-want +got):\n%s", diff)
	}

}
