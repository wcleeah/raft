package server

import (
	"sync"
	"time"
)

type Timer struct {
	mu sync.Mutex

	c        chan time.Time
	s        chan struct{}
	Duration time.Duration
	After    func(d time.Duration) <-chan time.Time
}

func (t *Timer) C() <-chan time.Time {
	return t.After(t.Duration)
}

func (t *Timer) S() <-chan struct{} {
	t.mu.Lock()
	if t.s == nil {
		t.s = make(chan struct{}, 100)
	}
	t.mu.Unlock()

	return t.s
}

func (t *Timer) Stop() {
	t.mu.Lock()
	if t.s == nil {
		t.s = make(chan struct{}, 100)
	}
	t.mu.Unlock()

	t.s <- struct{}{}
}
