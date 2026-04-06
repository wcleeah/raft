package core

import (
	"time"
)

type Timer interface {
	C() <-chan time.Time
	Stop()
	S() <-chan struct{}
}
