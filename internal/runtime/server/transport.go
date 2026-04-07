package server

import (
	"errors"
	"sync"
	"time"

	"com.lwc.raft/internal/core"
)

type Transport struct {
	once sync.Once

	connMap   map[string]*Conn
	ListenGcf GetConnFunc
	DialGcf   GetConnFunc
}

func (t *Transport) Send(id string, bs []byte) error {
	if t.connMap == nil {
		return errors.New("Id nto registered")
	}
	if _, ok := t.connMap[id]; !ok {
		return errors.New("Id not registered")
	}

	return t.connMap[id].Write(bs)
}

func (t *Transport) RegisterSelf(addr string, th core.TransportHandler, cfg core.TransportCfg) error {
	t.setupMap()
	if _, ok := t.connMap["self"]; ok {
		return errors.New("Self registered")
	}

	t.connMap["self"] = &Conn{
		Addr: addr,
		Gcf:  t.ListenGcf,
		Now:  time.Now,
		ReadDln: cfg.ReadDln,
		WriteDln: cfg.WriteDln,
	}

	go t.connMap["self"].Start()
	go t.handleRead("self", th)

	return nil
}

func (t *Transport) RegisterPeer(id string, addr string, th core.TransportHandler, cfg core.TransportCfg) error {
	t.setupMap()
	if _, ok := t.connMap[id]; ok {
		return errors.New("Id registered")
	}
	t.connMap[id] = &Conn{
		Addr: addr,
		Gcf:  t.DialGcf,
		Now:  time.Now,
		ReadDln: cfg.ReadDln,
		WriteDln: cfg.WriteDln,
	}

	go t.connMap[id].Start()
	go t.handleRead(id, th)

	return nil
}

func (t *Transport) CloseAll(reason error) {
	if t.connMap == nil {
		return
	}
	for _, v := range t.connMap {
		v.Close(reason)
	}
}

func (t *Transport) setupMap() {
	t.once.Do(func() {
		if t.connMap == nil {
			t.connMap = make(map[string]*Conn, 0)
		}
	})
}

func (t *Transport) handleRead(id string, th core.TransportHandler) {
	conn, ok := t.connMap[id]
	if !ok {
		return
	}

	for {
		bs, err := conn.Read()

		// conn only return err for Read when server initiate the close
		// that means no need to read anymore
		if err != nil {
			return
		}
		th(id, bs)
	}
}
