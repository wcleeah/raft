package core

import (
	"time"
)

type Ticker interface {
	C() <-chan time.Time
	Reset()
	Stop()
	S() <-chan struct{}
}
