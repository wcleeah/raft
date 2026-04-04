package core

import (
	"encoding/binary"
	"errors"
	"sync"
)

const AE_SNAPSHOT_BS_LEN = 7

type Store interface {
	ReplaceFrom(uint32, AppendEntries) 
	Restore() []byte
}

type AppendEntry struct {
	Term         uint32
	CounterDelta uint16
	Action       StateAction
}

func (ae AppendEntry) Encode() []byte {
	ss := make([]byte, AE_SNAPSHOT_BS_LEN)
	binary.BigEndian.PutUint32(ss[:4], ae.Term)
	binary.BigEndian.PutUint16(ss[4:6], ae.CounterDelta)
	ss[6] = ae.Action
	return ss
}

func DecodeAppendEntry(ss []byte) AppendEntry {
	term := binary.BigEndian.Uint32(ss[:4])
	counterDelta := binary.BigEndian.Uint16(ss[4:6])
	action := ss[6]

	return AppendEntry{
		Term:         term,
		CounterDelta: counterDelta,
		Action:       action,
	}
}

type AppendEntries []AppendEntry

func (ae AppendEntries) Len() uint32 {
	return uint32(len(ae))
}

func (ae AppendEntries) LastIdx() uint32 {
	l := ae.Len()

	// a weird mixture to try to match 1-based idx of raft log + 0-based idx of go arr/slice
	if l != 0 {
		return l - 1
	}

	return 0
}

func (ae AppendEntries) Copy() AppendEntries {
	entries := make(AppendEntries, len(ae))
	copy(entries, ae)

	return entries
}

func (ae AppendEntries) Encode() []byte {
	entries := make([]byte, 0)
	for _, entry := range ae {
		entries = append(entries, entry.Encode()...)
	}

	return entries
}

func DecodeAppendEntries(bs []byte) AppendEntries {
	ae := make(AppendEntries, len(bs)/AE_SNAPSHOT_BS_LEN)

	start := 0
	idx := 0
	for start+AE_SNAPSHOT_BS_LEN <= len(bs) {
		ae[idx] = DecodeAppendEntry(bs[start : start+AE_SNAPSHOT_BS_LEN])
		idx++
		start += AE_SNAPSHOT_BS_LEN
	}

	return ae
}

type AppendEntriesStore struct {
	mu sync.Mutex

	entries        AppendEntries
	lastAppliedIdx uint32
	Store          Store
}

func NewAppendEntriesStore(store Store) *AppendEntriesStore {
	return &AppendEntriesStore{
		// idx 0 is a dummy record
		// in raft logs start at 1
		entries: make(AppendEntries, 1),
		Store:   store,
	}
}

func (ae *AppendEntriesStore) Append(entry AppendEntry) {
	ae.mu.Lock()
	defer ae.mu.Unlock()

	ae.entries = append(ae.entries, entry)
}

func (ae *AppendEntriesStore) Replicate(bs []byte, prevLogIndex uint32, prevLogTerm uint32) (uint32, error) {
	ae.mu.Lock()
	defer ae.mu.Unlock()

	if ae.entries == nil {
		ae.entries = make([]AppendEntry, 1)
	}

	if prevLogIndex >= ae.entries.Len() {
		return 0, errors.New("Prev log index does not exist")
	}

	if ae.entries[prevLogIndex].Term != prevLogTerm {
		return 0, errors.New("Prev log term does not match")
	}

	newEntries := DecodeAppendEntries(bs)
	ae.entries = append(ae.entries[:prevLogIndex+1], newEntries...)
	ae.Store.ReplaceFrom(prevLogIndex+1, newEntries)

	return ae.entries.LastIdx(), nil
}

func (ae *AppendEntriesStore) ApplyAll(commitIdx uint32) AppendEntries {
	ae.mu.Lock()
	defer ae.mu.Unlock()

	if commitIdx <= ae.lastAppliedIdx {
		return AppendEntries{}
	}

	if ae.entries == nil {
		ae.entries = make(AppendEntries, 1)
	}

	if commitIdx >= ae.entries.Len() {
		return AppendEntries{}
	}
	res := make(AppendEntries, commitIdx-ae.lastAppliedIdx)

	copy(res, ae.entries[ae.lastAppliedIdx+1:commitIdx+1])
	ae.lastAppliedIdx = commitIdx

	return res
}

func (ae *AppendEntriesStore) LatestLog() (uint32, AppendEntry) {
	ae.mu.Lock()
	defer ae.mu.Unlock()

	if ae.entries.Len() == 0 {
		return 0, AppendEntry{}
	}

	lastIdx := ae.entries.LastIdx()
	return lastIdx, ae.entries[lastIdx]
}

func (ae *AppendEntriesStore) Restore() {
	ae.mu.Lock()
	defer ae.mu.Unlock()

	ss := ae.Store.Restore()
	if ae.entries == nil {
		ae.entries = make([]AppendEntry, 1)
	}

	ae.entries = append(ae.entries, DecodeAppendEntries(ss)...)
}

func (ae *AppendEntriesStore) Get(idx uint32) (AppendEntry, error) {
	ae.mu.Lock()
	defer ae.mu.Unlock()

	if idx >= ae.entries.Len() {
		return AppendEntry{}, errors.New("Idx overflow")
	}

	return ae.entries[idx], nil
}

// Get the entries that should send along with AppendEntries rpc.
//   - nextIndex: raft define nextIndex as the index to send in the heartbeat, so nextIndex is inclusive
func (ae *AppendEntriesStore) GetHeartbeatEntries(nextIndex uint32) AppendEntries {
	ae.mu.Lock()
	defer ae.mu.Unlock()

	if nextIndex >= ae.entries.Len() {
		return AppendEntries{}
	}

	view := ae.entries[nextIndex:]
	return view.Copy()
}
