package core

import "sync"

type Election struct {
	mu sync.RWMutex

	votedFor  string
	voteCount uint32
	Threshold int
}

func (e *Election) Vote(id string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.votedFor != "" {
		return false
	}
	e.votedFor = id

	return true
}

func (e *Election) GotVote() (uint32, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.voteCount++

	return e.voteCount, e.voteCount > uint32(e.Threshold)
}
