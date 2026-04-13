package server

import (
	"errors"
	"sync"
	"time"

	"com.lwc.raft/internal/core"
)

var (
	TransIdNotRegErr = errors.New("Id not registered")
	TransIdRegErr    = errors.New("Id registered")
)

type Transport struct {
	once sync.Once

	dialMap   map[string]*Dial
	selfId    string
	selfLis   *Listen
	ListenGcf ListenFunc
	DialGcf   GetConnFunc
}

func (t *Transport) Send(id string, bs []byte) error {
	if t.dialMap == nil {
		return TransIdNotRegErr
	}
	if _, ok := t.dialMap[id]; !ok {
		return TransIdNotRegErr
	}

	return t.dialMap[id].Write(bs)
}

func (t *Transport) RegisterSelf(id string, addr string, th core.TransportHandler, cfg core.TransportCfg) error {
	t.setupMap()
	if t.selfLis != nil {
		return TransIdRegErr
	}

	t.selfLis = &Listen{
		Addr:     addr,
		Lf:       t.ListenGcf,
		Now:      time.Now,
		ReadDln:  cfg.ReadDln,
		WriteDln: cfg.WriteDln,
	}

	err := t.selfLis.Start()
	if err != nil {
		return err
	}
	go t.handleListenRead(id, th)

	return nil
}

func (t *Transport) RegisterPeer(id string, addr string, th core.TransportHandler, cfg core.TransportCfg) error {
	t.setupMap()
	if _, ok := t.dialMap[id]; ok {
		return TransIdRegErr
	}

	t.dialMap[id] = &Dial{
		Addr:     addr,
		Gcf:      t.DialGcf,
		Now:      time.Now,
		ReadDln:  cfg.ReadDln,
		WriteDln: cfg.WriteDln,
	}

	go t.dialMap[id].Start()
	go t.handleDialRead(id, th)

	return nil
}

func (t *Transport) CloseAll(reason error) {
	if t.dialMap != nil {
		for _, v := range t.dialMap {
			v.Close(reason)
		}
	}
	if t.selfLis != nil {
		t.selfLis.Close(reason)
	}
}

func (t *Transport) setupMap() {
	t.once.Do(func() {
		if t.dialMap == nil {
			t.dialMap = make(map[string]*Dial, 0)
		}
	})
}

func (t *Transport) handleListenRead(id string, th core.TransportHandler) {
	if t.selfLis == nil {
		return
	}

	for {
		dto, err := t.selfLis.Read()

		// conn only return err for Read when server initiate the close
		// that means no need to read anymore
		if err != nil {
			return
		}

		res, err := th(id, dto.Frame)
		if err != nil {
			continue
		}
		t.selfLis.Write(ListenDTO{
			Id:    dto.Id,
			Frame: res,
		})
	}
}

func (t *Transport) handleDialRead(id string, th core.TransportHandler) {
	dial, ok := t.dialMap[id]
	if !ok {
		return
	}

	for {
		bs, err := dial.Read()

		// conn only return err for Read when server initiate the close
		// that means no need to read anymore
		if err != nil {
			return
		}
		res, err := th(id, bs)
		if err != nil {
			continue
		}
		dial.Write(res)
	}
}
