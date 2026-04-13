package server

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

var (
	ListenConnClosedErr = errors.New("Conn closed")
)

type ListenFunc = func(network string, addr string) (net.Listener, error)

type ListenDTO struct {
	Frame []byte
	Id    uint32
}

type Listen struct {
	closeOnce sync.Once
	setupOnce sync.Once
	mu        sync.Mutex

	Addr     string
	ReadDln  time.Duration
	WriteDln time.Duration
	Lf       ListenFunc
	Now      NowFunc

	writeId     uint32
	writeCh     map[uint32]chan []byte
	readCh      chan ListenDTO
	closeCh     chan struct{}
	closed      atomic.Bool
	closeReason error
	listener    net.Listener
}

func (l *Listen) Start() error {
	if l.writeCh == nil {
		l.writeCh = make(map[uint32]chan []byte)
	}
	if l.readCh == nil {
		l.readCh = make(chan ListenDTO, 1000)
	}
	if l.closeCh == nil {
		l.closeCh = make(chan struct{})
	}
	return l.listen()
}

func (l *Listen) listen() error {
	listener, err := l.Lf("tcp", l.Addr)

	if err != nil {
		return err
	}
	l.listener = listener

	go func() {
		for {
			conn, err := listener.Accept()
			if l.closed.Load() {
				if conn != nil {
					conn.Close()
				}
				break
			}
			if err != nil {
				continue
			}

			l.mu.Lock()
			ch := make(chan []byte)
			id := l.writeId
			l.writeCh[id] = ch
			l.writeId++
			l.mu.Unlock()

			connCtx, cancelFunc := context.WithCancel(context.Background())
			go l.read(connCtx, cancelFunc, id, conn)
			go l.write(connCtx, cancelFunc, id, conn)
		}
	}()

	return nil
}

func (l *Listen) Read() (ListenDTO, error) {
	if l.closed.Load() {
		return ListenDTO{}, l.closeReason
	}
	if l.readCh == nil {
		l.readCh = make(chan ListenDTO, 1000)
	}

	select {
	case lr := <-l.readCh:
		return lr, nil
	case <-l.closeCh:
		return ListenDTO{}, l.closeReason
	}
}

func (l *Listen) Write(dto ListenDTO) error {
	if l.closed.Load() {
		return l.closeReason
	}

	l.mu.Lock()
	ch, ok := l.writeCh[dto.Id]
	l.mu.Unlock()

	if !ok {
		return errors.New("Id not found")
	}

	select {
	case ch <- dto.Frame:
	case <-l.closeCh:
		return l.closeReason
	}

	return nil
}

func (l *Listen) Close(reason error) {
	l.closeOnce.Do(func() {
		if l.closeCh == nil {
			l.closeCh = make(chan struct{})
		}
		l.closeReason = reason
		l.closed.Store(true)

		close(l.closeCh)
		if l.listener != nil {
			l.listener.Close()
		}
	})
}

func (l *Listen) read(ctx context.Context, cancelFunc context.CancelFunc, id uint32, conn net.Conn) error {
	defer l.cleanUpConn(cancelFunc, id)

	// there is still a little bit of rpc logic here (len as the first 4 byte, rpc byte layout assumption)
	// but i think thats fine for now
	lenBytes := make([]byte, 4)
Outer:
	for {
		if l.closed.Load() {
			break
		}

		if l.ReadDln == 0 {
			l.ReadDln = DEFAULT_READ_DLN
		}

		// ignoring the error here:
		// - if conn closed -> this won't matter
		// - if conn does not support dln, thats ok
		conn.SetReadDeadline(l.Now().Add(l.ReadDln))
		_, err := io.ReadFull(conn, lenBytes)
		if err != nil {
			if l.closed.Load() {
				break
			}

			// if somehow c.closed is still false, check if conn closed by us, break
			if errors.Is(err, net.ErrClosed) || errors.Is(err, io.EOF) {
				return ListenConnClosedErr
			}

			// Raft spec specifies any error from peer will causes indefinite retry, we will do so here
			continue
		}

		frameLen := binary.BigEndian.Uint32(lenBytes)
		bs := make([]byte, frameLen)

		for {
			if l.ReadDln == 0 {
				l.ReadDln = DEFAULT_READ_DLN
			}

			conn.SetReadDeadline(l.Now().Add(l.ReadDln))
			_, err = io.ReadFull(conn, bs)
			if err != nil {
				if l.closed.Load() {
					break Outer
				}

				// if somehow c.closed is still false, check if conn closed by us, break
				if errors.Is(err, net.ErrClosed) || errors.Is(err, io.EOF) {
					return ListenConnClosedErr
				}

				// Raft spec specifies any error from peer will causes indefinite retry, we will do so here
				continue
			}
			break

		}
		select {
		case l.readCh <- ListenDTO{
			Frame: bs,
			Id:    id,
		}:
		case <-ctx.Done():
			return ctx.Err()
		case <-l.closeCh:
			break Outer
		}
	}

	conn.Close()
	return l.closeReason
}

func (l *Listen) write(ctx context.Context, cancelFunc context.CancelFunc, id uint32, conn net.Conn) error {
	defer l.cleanUpConn(cancelFunc, id)
	l.mu.Lock()
	ch, ok := l.writeCh[id]
	l.mu.Unlock()

	if !ok {
		return errors.New("Write Ch not registered")
	}
Outer:
	for {
		var bs []byte
		select {
		case <-l.closeCh:
			break Outer
		case <-ctx.Done():
			return ctx.Err()
		case bs = <-ch:
		}

		frameLen := make([]byte, 4)
		binary.BigEndian.PutUint32(frameLen, uint32(len(bs)))
		fullFrame := append(frameLen, bs...)

		ffLen := len(fullFrame)
		written := 0
		for written < ffLen {
			if l.closed.Load() {
				break Outer
			}

			if l.WriteDln == 0 {
				l.WriteDln = DEFAULT_WRITE_DLN
			}
			// ignoring the error here:
			// - if conn closed -> this won't matter
			// - if conn does not support dln, thats ok
			conn.SetWriteDeadline(l.Now().Add(l.WriteDln))
			n, err := conn.Write(fullFrame[written:])
			written += n

			// Note: because of tcp buffer, the write looks like successful, but actually not
			// Not a huge problem here since we will not get a response, and each request is idempotent
			if err != nil {
				// if somehow c.closed is still false, check if conn closed by us, break
				if l.closed.Load() || errors.Is(err, net.ErrClosed) || errors.Is(err, syscall.EPIPE) || errors.Is(err, syscall.ECONNRESET) {
					break Outer
				}

				// Raft spec specifies any error from peer will causes indefinite retry, we will do so here
				continue
			}
		}
	}

	conn.Close()
	return l.closeReason
}

func (l *Listen) cleanUpConn(cancelFunc context.CancelFunc, id uint32) {
	cancelFunc()
	l.mu.Lock()
	delete(l.writeCh, id)
	l.mu.Unlock()
}
