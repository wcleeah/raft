package core

import (
	"encoding/binary"
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
	prevLogIdx     uint32
	commitIdx      uint32
	lastAppliedIdx uint32
	store          Store
}

func NewAppendEntries(store Store) *AppendEntries {
	return &AppendEntries{
		entries: make([]AppendEntry, 0),
		store:   store,
	}
}

func (ae *AppendEntries) Append(entry AppendEntry) {
	ae.mu.Lock()
	defer ae.mu.Unlock()

	ae.entries = append(ae.entries, entry)
	ae.prevLogIdx++
	for {
		ok := ae.store.Save(entry)
		if ok {
			break
		}
	}
}

func (ae *AppendEntries) Commit(idx int) {
	ae.mu.Lock()
	defer ae.mu.Unlock()
	if idx >= len(ae.entries) {
		return
	}

	ae.commitIdx = uint32(idx)
}

func (ae *AppendEntries) ApplyNext() AppendEntry {
	ae.mu.RLock()
	defer ae.mu.RUnlock()

	ae.lastAppliedIdx++
	return ae.entries[ae.lastAppliedIdx]
}

func (ae *AppendEntries) ApplyAll() []AppendEntry {
	ae.mu.RLock()
	defer ae.mu.RUnlock()
	res := make([]AppendEntry, ae.commitIdx-ae.lastAppliedIdx)

	copy(res, ae.entries[ae.lastAppliedIdx+1:ae.commitIdx+1])
	ae.lastAppliedIdx = ae.commitIdx

	return res
}

func (ae *AppendEntries) CommitIdx() uint32 {
	return ae.commitIdx
}

func (ae *AppendEntries) LastAppliedIndex() uint32 {
	ae.mu.RLock()
	defer ae.mu.RUnlock()

	return ae.lastAppliedIdx
}

func (ae *AppendEntries) PrevLog() (uint32, AppendEntry) {
	ae.mu.RLock()
	defer ae.mu.RUnlock()

	return ae.prevLogIdx, ae.entries[ae.prevLogIdx]
}

func (ae *AppendEntries) Snapshot() []byte {
	ae.mu.RLock()
	defer ae.mu.RUnlock()

	ss := make([]byte, AE_SNAPSHOT_BS_LEN*len(ae.entries))

	for i, entry := range ae.entries {
		start := i * AE_SNAPSHOT_BS_LEN
		copy(ss[start:start+AE_SNAPSHOT_BS_LEN], entry.Snapshot())
	}

	return ss
}

func (ae *AppendEntries) Restore() {
	ae.mu.Lock()
	defer ae.mu.Lock()

	ss := ae.store.Restore()
	ae.entries = make([]AppendEntry, len(ss)/AE_SNAPSHOT_BS_LEN)

	start := 0
	idx := 0
	for start+AE_SNAPSHOT_BS_LEN < len(ss) {
		entry := &AppendEntry{}
		entry.Restore(ss[start : start+AE_SNAPSHOT_BS_LEN])
		ae.entries[idx] = *entry
		idx++
		start += AE_SNAPSHOT_BS_LEN
	}
	if idx == 0 {
		return
	}

	ae.commitIdx = uint32(idx - 1)
}

func (ae *AppendEntries) GetAfter(idx int) []AppendEntry {
	ae.mu.RLock()
	defer ae.mu.RUnlock()
	if idx >= len(ae.entries) {
		return []AppendEntry{}
	}

	view := ae.entries[idx:]
	res := make([]AppendEntry, len(view))
	copy(res, view)

	return res
}

func (ae *AppendEntries) GetBSAfter(idx int) []byte {
	ae.mu.RLock()
	defer ae.mu.RUnlock()
	if idx >= len(ae.entries) {
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
