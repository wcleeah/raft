package core

import (
	"time"
)

type Timer interface {
	C() <-chan time.Time
}
