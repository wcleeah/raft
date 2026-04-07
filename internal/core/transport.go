package core

import (
	"time"

)

type TransportHandler func(id string, bs []byte)
type TransportCfg struct {
	ReadDln  time.Duration
	WriteDln time.Duration
}

type Transport interface {
	Send(id string, bs []byte) error
	RegisterSelf(addr string, th TransportHandler, cfg TransportCfg) error
	RegisterPeer(id string, addr string, th TransportHandler, cfg TransportCfg) error
	CloseAll(reason error)
}
