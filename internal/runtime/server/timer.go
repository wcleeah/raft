package server

import (
	"math/rand/v2"
	"sync"
	"time"
)

type Timer struct {
	mu sync.Mutex

	Min          time.Duration
	Max          time.Duration
	StartupGrace time.Duration
	After        func(d time.Duration) <-chan time.Time
	Rand         *rand.Rand

	used bool
}

func (t *Timer) C() <-chan time.Time {
	after := t.After
	if after == nil {
		after = time.After
	}

	return after(t.nextDuration())
}

func (t *Timer) nextDuration() time.Duration {
	t.mu.Lock()
	defer t.mu.Unlock()

	min := t.Min
	max := t.Max
	if min == 0 {
		min = max
	}
	if max == 0 {
		max = min
	}

	duration := min
	if max > min {
		rng := t.Rand
		if rng == nil {
			rng = rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64()))
			t.Rand = rng
		}

		delta := max - min
		duration += time.Duration(rng.Int64N(int64(delta) + 1))
	}
	if !t.used {
		t.used = true
		duration += t.StartupGrace
	}

	return duration
}
