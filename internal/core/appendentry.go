package core

import (
	"encoding/binary"
	"errors"
	"sync"
)

const AE_SNAPSHOT_BS_LEN = 7

type Store interface {
	Save(AppendEntry) bool
	Restore() []byte
}

type AppendEntry struct {
	Term         uint32
	CounterDelta uint16
	Action       StateAction
}

func (ae AppendEntry) Snapshot() []byte {
	ss := make([]byte, AE_SNAPSHOT_BS_LEN)
	binary.BigEndian.PutUint32(ss[:4], ae.Term)
	binary.BigEndian.PutUint16(ss[4:6], ae.CounterDelta)
	ss[6] = ae.Action
	return ss
}

func (ae *AppendEntry) Restore(ss []byte) {
	ae.Term = binary.BigEndian.Uint32(ss[:4])
	ae.CounterDelta = binary.BigEndian.Uint16(ss[4:6])
	ae.Action = ss[6]
}

type AppendEntries struct {
	mu             sync.RWMutex
	entries        []AppendEntry
	latestLogIdx   uint32
	lastAppliedIdx uint32
	store          Store
}

func NewAppendEntries(store Store) *AppendEntries {
	return &AppendEntries{
		entries: make([]AppendEntry, 1),
		store:   store,
	}
}

func (ae *AppendEntries) Replicate(bs []byte, after uint32) {
	ae.mu.Lock()
	defer ae.mu.Unlock()

	newEntries := ae.decode(bs)
	if after > ae.latestLogIdx {
		ae.entries = append(ae.entries, newEntries...)
		return
	}

	ae.entries = append(ae.entries[:after], newEntries...)
}

func (ae *AppendEntries) ApplyNext() AppendEntry {
	ae.mu.RLock()
	defer ae.mu.RUnlock()

	ae.lastAppliedIdx++
	return ae.entries[ae.lastAppliedIdx]
}

func (ae *AppendEntries) ApplyAll(commitIdx uint32) []AppendEntry {
	ae.mu.Lock()
	defer ae.mu.Unlock()
	if commitIdx < ae.lastAppliedIdx {
		return []AppendEntry{}
	}
	if commitIdx >= uint32(len(ae.entries)) {
		panic("Idx overflow")
	}
	res := make([]AppendEntry, commitIdx-ae.lastAppliedIdx)

	copy(res, ae.entries[ae.lastAppliedIdx+1:commitIdx+1])
	ae.lastAppliedIdx = commitIdx

	return res
}

func (ae *AppendEntries) LatestLog() (uint32, AppendEntry) {
	ae.mu.RLock()
	defer ae.mu.RUnlock()

	return ae.latestLogIdx, ae.entries[ae.latestLogIdx]
}

func (ae *AppendEntries) Restore() {
	ae.mu.Lock()
	defer ae.mu.Lock()

	ss := ae.store.Restore()
	if ae.entries == nil {
		ae.entries = make([]AppendEntry, 1)
	}
	ae.entries = append(ae.entries, ae.decode(ss)...)
}

func (ae *AppendEntries) Get(idx uint32) (AppendEntry, error) {
	ae.mu.RLock()
	defer ae.mu.RUnlock()
	if idx >= uint32(len(ae.entries)) {
		return AppendEntry{}, errors.New("Idx overflow")
	}

	return ae.entries[idx], nil
}

func (ae *AppendEntries) GetBSAfter(idx uint32) []byte {
	ae.mu.RLock()
	defer ae.mu.RUnlock()
	if idx >= uint32(len(ae.entries)) {
		return []byte{}
	}

	view := ae.entries[idx:]
	res := make([]byte, len(view)*AE_SNAPSHOT_BS_LEN)
	for i, entry := range view {
		start := i * AE_SNAPSHOT_BS_LEN
		copy(res[start:start+AE_SNAPSHOT_BS_LEN], entry.Snapshot())
	}

	return res
}

func (ae *AppendEntries) decode(bs []byte) []AppendEntry {
	entries := make([]AppendEntry, len(bs)/AE_SNAPSHOT_BS_LEN)

	start := 0
	idx := 0
	for start+AE_SNAPSHOT_BS_LEN < len(bs) {
		entry := &AppendEntry{}
		entry.Restore(bs[start : start+AE_SNAPSHOT_BS_LEN])
		ae.entries[idx] = *entry
		idx++
		ae.latestLogIdx++
		start += AE_SNAPSHOT_BS_LEN
	}

	return entries
}
