package server_test

import (
	"sync"
	"testing"
	"time"

	"com.lwc.raft/internal/runtime/server"
	"github.com/stretchr/testify/assert"
)

type fakeTime struct {
	T chan time.Time
	d time.Duration
}

func (f *fakeTime) After(d time.Duration) <-chan time.Time {
	f.d = d
	return f.T
}

func (f *fakeTime) Fire(now time.Time) {
	f.T <- now.Add(f.d)
}

func TestC(t *testing.T) {
	assert := assert.New(t)

	testDur := 5 * time.Second
	now := time.Now()
	ft := &fakeTime{
		T: make(chan time.Time, 1),
	}
	timer := &server.Timer{
		Duration: testDur,
		After:    ft.After,
	}

	var wg sync.WaitGroup
	go func() {
		defer wg.Wait()

		timeReceived := <-timer.C()
		assert.Equal(now.Add(testDur), timeReceived)
	}()
	ft.Fire(now)

	wg.Wait()
}

func TestStop(t *testing.T) {

	timer := &server.Timer{}

	timer.Stop()
	select {
	case <-timer.S():
	case <-time.After(2 * time.Second):
		t.Fatal("timer.S did not fire")
	}
}
