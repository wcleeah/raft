package core

import (
	"encoding/binary"
	"sync"
)

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
	ss := make([]byte, 7)
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

func (ae *AppendEntries) Lastest() (int, AppendEntry) {
	ae.mu.RLock()
	defer ae.mu.RUnlock()

	return len(ae.entries) - 1, ae.entries[len(ae.entries)-1]
}

func (ae *AppendEntries) Snapshot() []byte {
	ae.mu.RLock()
	defer ae.mu.RUnlock()

	ss := make([]byte, 7*(ae.commitIdx+1))

	for i, entry := range ae.entries[:ae.commitIdx] {
		start := i * 7
		copy(ss[start:start+7], entry.Snapshot())
	}

	return ss
}

func (ae *AppendEntries) Restore() {
	ae.mu.Lock()
	defer ae.mu.Lock()

	ss := ae.store.Restore()
	ae.entries = make([]AppendEntry, len(ss)/5)

	start := 0
	idx := 0
	for start+7 < len(ss) {
		entry := &AppendEntry{}
		entry.Restore(ss[start : start+5])
		ae.entries[idx] = *entry
		idx++
		start += 7
	}
}

func (ae *AppendEntries) GetAfter(idx int) []AppendEntry {
	ae.mu.RLock()
	defer ae.mu.RUnlock()

	view := ae.entries[idx:]
	res := make([]AppendEntry, len(view))
	copy(res, view)

	return res
}
