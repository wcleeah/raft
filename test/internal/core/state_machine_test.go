package core_test

import (
	"testing"

	"com.lwc.raft/internal/core"
	"github.com/stretchr/testify/assert"
)

func TestStateMachine(t *testing.T) {
	assert := assert.New(t)

	sm := &core.StateMachine{}

	err := sm.Act(core.STATE_ADD, 1)
	assert.NoError(err, "Unexpected error during STATE_ADD")
	assert.Equal(int32(1), sm.Counter(), "STATE_ADD: Counter mismatch")

	err = sm.Act(core.STATE_MINUS, 10)
	assert.NoError(err, "Unexpected error during STATE_MINUS")
	assert.Equal(int32(-9), sm.Counter(), "STATE_MINUS: Counter mismatch")

	err = sm.Act(core.STATE_FLIP, 100)
	assert.NoError(err, "Unexpected error during STATE_FLIP")
	assert.Equal(int32(-9), sm.Counter(), "STATE_FLIP: Counter mismatch")
	assert.Equal(true, sm.Flip(), "STATE_FLIP: Counter mismatch")

	err = sm.Act(100, 100)
	assert.ErrorIs(core.STATE_INVAILD, err, "Invalid action should return error")
}
