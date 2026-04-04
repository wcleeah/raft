package core

import (
	"errors"
	"sync"
)

type StateAction = uint8 

const (
	STATE_ADD StateAction = iota
	STATE_MINUS
	STATE_FLIP
)

var (
	STATE_INVAILD = errors.New("State Invalid")
)

type StateMachine struct {
	mu      sync.Mutex
	counter int32
	flip    bool
}

func (sm *StateMachine) Act(action StateAction, counterDelta uint16) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	switch action {
	case STATE_ADD:
		sm.counter += int32(counterDelta)
	case STATE_MINUS:
		sm.counter -= int32(counterDelta)
	case STATE_FLIP:
		sm.flip = !sm.flip
	default:
		return STATE_INVAILD
	}
	return nil
}
