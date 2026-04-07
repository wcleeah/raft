package server_test

import (
	"testing"
	"time"

	"com.lwc.raft/internal/runtime/server"
	"github.com/stretchr/testify/assert"
)

type fakeTime struct {
	T   chan time.Time
	D   chan time.Duration
	Now time.Time
}

func (f *fakeTime) After(d time.Duration) <-chan time.Time {
	f.D <- d
	return f.T
}

func (f *fakeTime) Fire() {
	d := <-f.D
	f.T <- f.Now.Add(d)
}

func TestC(t *testing.T) {
	assert := assert.New(t)

	testDur := 5 * time.Second
	now := time.Now()
	ft := &fakeTime{
		T:   make(chan time.Time, 1),
		D:   make(chan time.Duration, 1),
		Now: now,
	}
	timer := &server.Timer{
		Duration: testDur,
		After:    ft.After,
	}

	go func() {
		ft.Fire()
	}()

	timeReceived := <-timer.C()

	assert.Equal(now.Add(testDur), timeReceived)
}

func TestStop(t *testing.T) {

	timer := &server.Timer{}

	ch := timer.S()
	timer.Stop()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("timer.S did not fire")
	}
}
