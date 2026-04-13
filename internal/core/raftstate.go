package core

import (
	"errors"
	"slices"
	"sync"
)

type RaftRole = uint

const (
	RAFT_ROLE_FOLLOWER RaftRole = iota
	RAFT_ROLE_CANDIDATE
	RAFT_ROLE_LEADER
)

type RaftStateEvent = uint

const (
	RAFT_STATE_EVENT_ROLE_CHANGE RaftStateEvent = iota
	RAFT_STATE_EVENT_COMMIT_IDX_CHANGE
)

var (
	RaftStateNotALeaderErr = errors.New("Not a Leader")
	RaftStateNotACandidateErr = errors.New("Not a Candidate")
	RaftStateCurrentTermHigherErr = errors.New("Current Term is higher")
)

type RaftState struct {
	mu          sync.RWMutex
	setTermOnce sync.Once
	eventCh     chan RaftStateEvent

	term      uint32
	role      RaftRole
	threshold uint32

	// for leader: majority of followers matched this index
	// for follower: min(leader's commit index, latest log index)
	commitIndex uint32

	// follower related
	leaderId string

	// leader related
	// next heartbeat sent from this index
	nextIndex map[string]uint32
	// known highest index to be replicated
	matchIndex map[string]uint32

	// candidate / election related
	votedFor string
	// used for checking if follower voted for current term
	votedForTerm uint32
	voteCount    uint32
}


func (rs *RaftState) EventCh() <-chan RaftStateEvent {
	rs.mu.RLock()
	defer rs.mu.RUnlock()

	if rs.eventCh == nil {
		rs.eventCh = make(chan RaftStateEvent, 1000)
	}

	return rs.eventCh
}

func (rs *RaftState) Term() uint32 {
	rs.mu.RLock()
	defer rs.mu.RUnlock()

	return rs.term
}

func (rs *RaftState) MatchIndex(key string) uint32 {
	rs.mu.RLock()
	defer rs.mu.RUnlock()

	if rs.matchIndex == nil {
		rs.matchIndex = make(map[string]uint32)
	}

	return rs.matchIndex[key]
}

func (rs *RaftState) NextIndex(key string) uint32 {
	rs.mu.RLock()
	defer rs.mu.RUnlock()

	if rs.nextIndex == nil {
		rs.nextIndex = make(map[string]uint32)
	}
	return rs.nextIndex[key]
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

func (rs *RaftState) LeaderId() string {
	rs.mu.RLock()
	defer rs.mu.RUnlock()

	return rs.leaderId
}

func (rs *RaftState) Threshold() uint32 {
	rs.mu.RLock()
	defer rs.mu.RUnlock()

	return rs.threshold
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

func (rs *RaftState) UpdateCommitIdx(newCommitIdx uint32) {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	if newCommitIdx <= rs.commitIndex {
		return
	}

	rs.updateCommitIdx(newCommitIdx)
}

func (rs *RaftState) StartElection(id string) error {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	if rs.role != RAFT_ROLE_CANDIDATE {
		return RaftStateNotACandidateErr
	}

	rs.term++
	rs.votedFor = id
	rs.votedForTerm = rs.term
	rs.voteCount++

	return nil
}

func (rs *RaftState) StopElection() error {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	if rs.role != RAFT_ROLE_CANDIDATE {
		return RaftStateNotACandidateErr
	}

	rs.votedFor = ""
	rs.voteCount = 0

	return nil
}

func (rs *RaftState) InitAsLeader(id string, latestLogIdx uint32) error {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	if rs.role != RAFT_ROLE_LEADER {
		return RaftStateNotALeaderErr
	}

	rs.commitIndex = 0

	if rs.matchIndex == nil {
		rs.matchIndex = make(map[string]uint32)
	}
	if rs.nextIndex == nil {
		rs.nextIndex = make(map[string]uint32)
	}

	for k := range rs.matchIndex {
		if k == id {
			rs.matchIndex[k] = latestLogIdx
			continue
		}
		rs.matchIndex[k] = 0
	}

	for k := range rs.nextIndex {
		rs.nextIndex[k] = latestLogIdx + 1
	}

	return nil
}

func (rs *RaftState) InitAsFollower() {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	rs.commitIndex = 0
}

func (rs *RaftState) Vote(candidateId string, term uint32, lastLogIndex uint32, lastLogTerm uint32, entries AppendEntries) (uint32, error) {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	if rs.term > term {
		return rs.term, RaftStateCurrentTermHigherErr
	}

	if rs.term == term && rs.votedFor != "" {
		return rs.term, errors.New("Voted already")
	}

	if term > rs.term {
		rs.term = term
		rs.updateRole(RAFT_ROLE_FOLLOWER)
	}

	log := entries.LatestLog()
	idx := entries.LatestIdx()

	if lastLogTerm < log.Term {
		return rs.term, errors.New("Candidate log term behind")
	}
	if lastLogIndex < idx {
		return rs.term, errors.New("Candidate log index behind")
	}

	rs.votedFor = candidateId
	rs.votedForTerm = term
	rs.term = term
	rs.updateRole(RAFT_ROLE_FOLLOWER)

	return rs.term, nil
}

func (rs *RaftState) GotVote(voteGranted bool, followerTerm uint32) {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	if followerTerm > rs.term {
		rs.term = followerTerm
		rs.votedFor = ""
		rs.voteCount = 0
		rs.updateRole(RAFT_ROLE_FOLLOWER)
		return
	}
	if !voteGranted {
		return
	}

	rs.voteCount++
	if rs.voteCount >= rs.threshold {
		rs.updateRole(RAFT_ROLE_LEADER)
	}
}

func (rs *RaftState) IsVoted() bool {
	rs.mu.RLock()
	defer rs.mu.RUnlock()

	// 1. votedForTerm == term: it means follower / candidate voted for the current term
	// 2. votedFor != "": follower / candidate voted for someone, it will reset after receiving the first AE (which also infer the election is over)
	return rs.votedForTerm == rs.term && rs.votedFor != ""
}

func (rs *RaftState) GotAEReq(id string, term uint32) error {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	if term < rs.term {
		return RaftStateCurrentTermHigherErr
	}

	if term > rs.term {
		rs.term = term
		rs.updateRole(RAFT_ROLE_FOLLOWER)
	}

	// Valid AE also infers election ended
	rs.leaderId = id
	rs.votedFor = ""

	return nil
}

func (rs *RaftState) GotAERes(id string, success bool, term uint32, matchIdx uint32, logs AppendEntries) {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	if term > rs.term {
		rs.term = term
		rs.updateRole(RAFT_ROLE_FOLLOWER)
		return
	}

	if rs.role != RAFT_ROLE_LEADER {
		return
	}

	if !success {
		if rs.nextIndex[id] > 0 {
			rs.nextIndex[id]--
		}
		return
	}

	if rs.matchIndex == nil {
		rs.matchIndex = make(map[string]uint32)
	}

	if rs.nextIndex == nil {
		rs.nextIndex = make(map[string]uint32)
	}

	rs.matchIndex[id] = max(matchIdx, rs.matchIndex[id])
	rs.nextIndex[id] = rs.matchIndex[id] + 1

	matchIndexes := make([]uint32, 0)
	for _, v := range rs.matchIndex {
		matchIndexes = append(matchIndexes, v)
	}

	slices.Sort(matchIndexes)

	tIdx := rs.threshold - 1
	possibleNewCommitIndex := matchIndexes[tIdx]
	if rs.commitIndex < possibleNewCommitIndex && rs.term == logs[possibleNewCommitIndex].Term {
		rs.updateCommitIdx(matchIndexes[tIdx])
	}
}

func (rs *RaftState) IncLeaderIndexes(id string) error {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	if rs.role != RAFT_ROLE_LEADER {
		return RaftStateNotALeaderErr
	}

	if rs.matchIndex == nil {
		rs.matchIndex = make(map[string]uint32)
	}

	if rs.nextIndex == nil {
		rs.nextIndex = make(map[string]uint32)
	}

	rs.matchIndex[id]++
	rs.nextIndex[id]++

	return nil
}

func (rs *RaftState) RestoreTerm(term uint32) {
	rs.setTermOnce.Do(func() {
		rs.mu.Lock()
		defer rs.mu.Unlock()

		rs.term = term
	})
}

func (rs *RaftState) RegisterNode(id string) {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	if rs.matchIndex == nil {
		rs.matchIndex = make(map[string]uint32)
	}

	if rs.nextIndex == nil {
		rs.nextIndex = make(map[string]uint32)
	}

	rs.nextIndex[id] = 0
	rs.matchIndex[id] = 0

	total := len(rs.nextIndex)
	rs.threshold = uint32(total / 2)
	if total%2 == 1 {
		rs.threshold++
	}
}

func (rs *RaftState) ResetIndexes(nextIndex uint32) {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	if rs.matchIndex == nil {
		rs.matchIndex = make(map[string]uint32)
	}

	if rs.nextIndex == nil {
		rs.nextIndex = make(map[string]uint32)
	}

	for k := range rs.nextIndex {
		rs.nextIndex[k] = nextIndex
	}

	for k := range rs.matchIndex {
		rs.matchIndex[k] = 0
	}
}

func (rs *RaftState) updateRole(role uint) {
	if rs.role == role {
		return
	}
	rs.role = role
	if rs.eventCh == nil {
		rs.eventCh = make(chan RaftStateEvent, 1000)
	}
	// might be a bad design, keeping it for now
	// it potentially holds the lock for super long, or even causes dead lock
	rs.eventCh <- RAFT_STATE_EVENT_ROLE_CHANGE
}

func (rs *RaftState) updateCommitIdx(commitIdx uint32) {
	rs.commitIndex = commitIdx
	if rs.eventCh == nil {
		rs.eventCh = make(chan RaftStateEvent, 1000)
	}
	rs.eventCh <- RAFT_STATE_EVENT_COMMIT_IDX_CHANGE
}
