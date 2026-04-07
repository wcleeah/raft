package server

import (
	"sync"
	"time"
)

type Timer struct {
	mu sync.Mutex

	s        chan struct{}
	Duration time.Duration
	After    func(d time.Duration) <-chan time.Time
}

func (t *Timer) C() <-chan time.Time {
	return t.After(t.Duration)
}

func (t *Timer) S() <-chan struct{} {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.s == nil {
		t.s = make(chan struct{}, 100)
	}

	return t.s
}

func (t *Timer) Stop() {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.s == nil {
		t.s = make(chan struct{}, 100)
		return
	}
	close(t.s)
	t.s = make(chan struct{}, 100)
}
