package rpc

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"sync"
)

type RPC struct {
	once        sync.Once
	ctx         context.Context
	conn        net.Conn
	writeCh     chan *Frame
	closeCh     chan struct{}
	CloseReason error
}

func NewRPC(ctx context.Context, conn net.Conn) *RPC {
	return &RPC{
		ctx:     ctx,
		conn:    conn,
		writeCh: make(chan *Frame, 100),
		closeCh: make(chan struct{}),
	}
}

func (l *RPC) Setup() {
	go l.monitorCtx()
	go l.write()
}

func (l *RPC) monitorCtx() {
	select {
	case <-l.ctx.Done():
		l.Close(l.ctx.Err())
	case <-l.closeCh:
	}
}

func (l *RPC) Close(err error) {
	l.once.Do(func() {
		l.CloseReason = err
		l.conn.Close()
		close(l.closeCh)
	})
}

func (l *RPC) CloseCh() <-chan struct{} {
	return l.closeCh
}

func (l *RPC) Read(outCh chan *Frame) {
	lenBytes := make([]byte, 4)
	var closeErr error
Outer:
	for {
		select {
		case <-l.closeCh:
			break Outer
		default:
		}
		_, err := io.ReadFull(l.conn, lenBytes)
		if err != nil {
			closeErr = err
			break
		}

		frameLen := binary.BigEndian.Uint32(lenBytes)
		frame := make([]byte, frameLen)

		select {
		case <-l.closeCh:
			break Outer
		default:
		}
		_, err = io.ReadFull(l.conn, frame)
		if err != nil {
			closeErr = err
			break
		}

		outCh <- DecodeRPCBase(frame)
	}

	l.Close(closeErr)
}

func (l *RPC) Write(frame *Frame) error {
	select {
	case <-l.closeCh:
		return l.CloseReason
	case l.writeCh <- frame:
	}

	return nil
}

func (l *RPC) write() {
	var closeErr error
	for frame := range l.writeCh {
		frameBytes := frame.Encode()
		frameLen := make([]byte, 4)
		binary.BigEndian.PutUint32(frameLen, uint32(len(frameBytes)))
		fullFrame := append(frameLen, frameBytes...)

		_, err := l.conn.Write(fullFrame)
		if err != nil {
			closeErr = err
			break
		}
	}

	l.Close(closeErr)
}
