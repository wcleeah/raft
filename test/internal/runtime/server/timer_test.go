package server_test

import (
	"math/rand/v2"
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

func TestTimerC(t *testing.T) {
	assert := assert.New(t)

	testDur := 5 * time.Second
	now := time.Now()
	ft := &fakeTime{
		T:   make(chan time.Time, 1),
		D:   make(chan time.Duration, 1),
		Now: now,
	}
	timer := &server.Timer{
		Min:   testDur,
		Max:   testDur,
		After: ft.After,
	}

	go func() {
		ft.Fire()
	}()

	timeReceived := <-timer.C()

	assert.Equal(now.Add(testDur), timeReceived)
}

func TestTimerCStartupGrace(t *testing.T) {
	assert := assert.New(t)

	testDur := 5 * time.Second
	startupGrace := 3 * time.Second
	now := time.Now()
	ft := &fakeTime{
		T:   make(chan time.Time, 2),
		D:   make(chan time.Duration, 2),
		Now: now,
	}
	timer := &server.Timer{
		Min:          testDur,
		Max:          testDur,
		StartupGrace: startupGrace,
		After:        ft.After,
	}

	go func() {
		ft.Fire()
		ft.Fire()
	}()

	firstTick := <-timer.C()
	secondTick := <-timer.C()

	assert.Equal(now.Add(testDur+startupGrace), firstTick)
	assert.Equal(now.Add(testDur), secondTick)
}

func TestTimerCRandomRange(t *testing.T) {
	assert := assert.New(t)

	min := 5 * time.Second
	max := 7 * time.Second
	ft := &fakeTime{
		T:   make(chan time.Time, 1),
		D:   make(chan time.Duration, 1),
		Now: time.Now(),
	}
	timer := &server.Timer{
		Min:   min,
		Max:   max,
		After: ft.After,
		Rand:  rand.New(rand.NewPCG(1, 2)),
	}

	ch := timer.C()
	duration := <-ft.D
	assert.GreaterOrEqual(duration, min)
	assert.LessOrEqual(duration, max)

	ft.T <- ft.Now.Add(duration)
	<-ch
}
