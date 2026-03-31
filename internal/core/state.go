package core

import (
	"sync"
)

type RaftRole = uint

const (
	RAFT_ROLE_LEADER RaftRole = iota
	RAFT_ROLE_FOLLOWER
	RAFT_ROLE_CANDIDATE
)

type RaftStateEvent = uint

const (
	RAFT_STATE_EVENT_ROLE_CHANGE RaftStateEvent = iota
)

type RaftState struct {
	mu      sync.RWMutex
	eventCh chan RaftStateEvent

	term     uint32
	role     RaftRole
	leaderId string

	// leader related
	nextIndex  map[string]uint32
	matchIndex map[string]uint32
}

func NewRaftState() *RaftState {
	return &RaftState{
		eventCh: make(chan RaftStateEvent, 1000),
	}
}

func (rs *RaftState) Role() RaftRole {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	return rs.role
}

func (rs *RaftState) UpdateRole(role RaftRole) {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	if rs.role == role {
		return
	}
	rs.role = role
	rs.eventCh <- RAFT_STATE_EVENT_ROLE_CHANGE
}

func (rs *RaftState) Term() uint32 {
	rs.mu.RLock()
	defer rs.mu.RUnlock()

	return rs.term
}

func (rs *RaftState) SetTerm(term uint32) {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	rs.term = term
}

func (rs *RaftState) IncrementTerm() uint32 {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	rs.term += 1
	return rs.term
}

func (rs *RaftState) NextIndex(key string) uint32 {
	rs.mu.RLock()
	defer rs.mu.RUnlock()

	return rs.nextIndex[key]
}

func (rs *RaftState) MatchIndex(key string) uint32 {
	rs.mu.RLock()
	defer rs.mu.RUnlock()

	return rs.matchIndex[key]
}
