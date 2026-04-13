package server

import "time"

type NowFunc = func() time.Time

var (
	DEFAULT_READ_DLN  = 10 * time.Second
	DEFAULT_WRITE_DLN = 10 * time.Second
)
