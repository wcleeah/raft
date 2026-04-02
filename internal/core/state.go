package core

import (
	"errors"
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
	RAFT_STATE_EVENT_COMMIT_IDX_CHANGE
)

type RaftState struct {
	mu      sync.RWMutex
	eventCh chan RaftStateEvent

	term     uint32
	role     RaftRole
	leaderId string
	commitIndex uint32

	// leader related
	nextIndex   map[string]uint32
	matchIndex  map[string]uint32

	// election related
	votedFor     string
	votedForTerm uint32
	voteCount    uint32
}

func NewRaftState() *RaftState {
	return &RaftState{
		eventCh: make(chan RaftStateEvent, 1000),
	}
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

func (rs *RaftState) CommitIdx() uint32 {
	rs.mu.RLock()
	defer rs.mu.RUnlock()

	return rs.commitIndex
}

func (rs *RaftState) Role() RaftRole {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	return rs.role
}

func (rs *RaftState) UpdateRole(role RaftRole) bool {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	if rs.role == role {
		return true
	}

	if role == RAFT_ROLE_LEADER && rs.role != RAFT_ROLE_CANDIDATE {
		return false
	}

	if role == RAFT_ROLE_CANDIDATE && rs.role != RAFT_ROLE_FOLLOWER {
		return false
	}

	rs.updateRole(role)
	return true
}

func (rs *RaftState) StartElection(id string) error {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	if rs.role != RAFT_ROLE_CANDIDATE {
		return errors.New("Not a candidate")
	}

	if rs.term+1 <= rs.votedForTerm {
		return errors.New("Voted for higher term already")
	}
	rs.term++
	rs.votedFor = id
	rs.votedForTerm = rs.term
	rs.voteCount = 0

	return nil
}

func (rs *RaftState) Vote(candidateId string, term uint32) (uint32, error) {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	if rs.term > term {
		return rs.term, errors.New("Current term is higher")
	}

	if rs.votedForTerm >= term {
		return rs.term, errors.New("Voted for higher term")
	}

	if rs.votedFor != "" {
		return rs.term, errors.New("Voted already")
	}

	rs.votedFor = candidateId
	rs.votedForTerm = term
	rs.updateRole(RAFT_ROLE_FOLLOWER)

	return rs.term, nil
}

func (rs *RaftState) GotVote(voteGranted bool, followerTerm uint32, threshold uint32) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if followerTerm > rs.term {
		rs.term = followerTerm
		rs.updateRole(RAFT_ROLE_FOLLOWER)
		return
	}
	if !voteGranted {
		return
	}

	rs.voteCount++
	if rs.voteCount > threshold {
		rs.updateRole(RAFT_ROLE_LEADER)
	}
}

func (rs *RaftState) IsVoted() bool {
	rs.mu.RLock()
	defer rs.mu.RUnlock()

	return rs.votedForTerm == rs.term && rs.votedFor != ""
}

func (rs *RaftState) InitAsLeader(latestLogIdx uint32) error {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	if rs.role != RAFT_ROLE_LEADER {
		return errors.New("Not a leader")
	}

	rs.commitIndex = 0
	for k := range rs.matchIndex {
		rs.matchIndex[k] = 0
	}
	for k := range rs.nextIndex {
		rs.nextIndex[k] = latestLogIdx + 1
	}

	return nil
}

func (rs *RaftState) GotAEReq(id string, term uint32, newCommitIdx uint32) {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	if term > rs.term && rs.role != RAFT_ROLE_FOLLOWER {
		rs.term = term
		rs.updateRole(RAFT_ROLE_FOLLOWER)
	}

	if rs.commitIndex < newCommitIdx {
		rs.updateCommitIdx(newCommitIdx)
	}

}

func (rs *RaftState) GotAERes(id string, success bool, term uint32) {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	if term > rs.term {
		rs.term = term
		rs.updateRole(RAFT_ROLE_FOLLOWER)
		return
	}

	if success {
		rs.matchIndex[id] = rs.nextIndex[id]
		rs.nextIndex[id]++
		count := 0
		for _, v := range rs.nextIndex {
			if v >= rs.nextIndex[id] {
				count++
			}
		}
		if count > len(rs.nextIndex) / 2 && count > int(rs.commitIndex) {
			rs.updateCommitIdx(rs.nextIndex[id])
		}
		return
	}
	if rs.nextIndex[id] > 0 {
		rs.nextIndex[id]--
	}

}

func (rs *RaftState) updateRole(role uint) {
	rs.role = role
	// might be a bad design, keeping it for now
	// it potentially holds the lock for super long, or even causes dead lock
	rs.eventCh <- RAFT_STATE_EVENT_ROLE_CHANGE
}

func (rs *RaftState) updateCommitIdx(commitIdx uint32) {
	rs.commitIndex = commitIdx
	rs.eventCh <- RAFT_STATE_EVENT_COMMIT_IDX_CHANGE
}
