package server

import (
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
	DEFAULT_READ_DLN  = 10 * time.Second
	DEFAULT_WRITE_DLN = 10 * time.Second
)

type GetConnFunc = func(network string, addr string) (net.Conn, error)
type NowFunc = func() time.Time

type Conn struct {
	closeOnce sync.Once
	setupOnce sync.Once
	cond      *sync.Cond

	Addr     string
	ReadDln  time.Duration
	WriteDln time.Duration
	Gcf      GetConnFunc
	Now      NowFunc

	conn        net.Conn
	writeCh     chan []byte
	readCh      chan []byte
	closeCh     chan struct{}
	closed      atomic.Bool
	closeReason error
}

func (c *Conn) Start() error {
	c.cond = sync.NewCond(&sync.Mutex{})
	err := c.getConn()
	if err != nil {
		return err
	}
	if c.writeCh == nil {
		c.writeCh = make(chan []byte, 1000)
	}
	if c.readCh == nil {
		c.readCh = make(chan []byte, 1000)
	}
	if c.closeCh == nil {
		c.closeCh = make(chan struct{})
	}
	go c.read()
	go c.write()

	return nil
}

func (c *Conn) Read() ([]byte, error) {
	if c.closed.Load() {
		return nil, c.closeReason
	}
	if c.readCh == nil {
		c.readCh = make(chan []byte, 1000)
	}

	select {
	case bs := <-c.readCh:
		return bs, nil
	case <-c.closeCh:
		return nil, c.closeReason
	}
}

func (c *Conn) Write(bs []byte) error {
	if c.closed.Load() {
		return c.closeReason
	}
	if c.writeCh == nil {
		c.writeCh = make(chan []byte, 1000)
	}

	select {
	case c.writeCh <- bs:
	case <-c.closeCh:
		return c.closeReason
	}

	return nil
}

func (c *Conn) Close(reason error) {
	c.closeOnce.Do(func() {
		if c.closeCh == nil {
			c.closeCh = make(chan struct{})
		}
		close(c.closeCh)
		c.closeReason = reason
		c.closed.Store(true)

		if c.conn != nil {
			c.conn.Close()
		}
	})
}

func (c *Conn) waitConn() (net.Conn, error) {
	c.cond.L.Lock()
	defer c.cond.L.Unlock()
	if c.closed.Load() {
		return nil, c.closeReason
	}
	if c.conn != nil {
		return c.conn, nil
	}
	go c.getConn()

	for c.conn == nil && !c.closed.Load() {
		c.cond.Wait()
	}

	return c.conn, nil
}

func (c *Conn) markBadConn(conn net.Conn) error {
	c.cond.L.Lock()
	defer c.cond.L.Unlock()
	if c.closed.Load() {
		return c.closeReason
	}
	if conn != c.conn {
		return nil
	}
	c.conn = nil
	go c.getConn()

	return nil
}

func (c *Conn) getConn() error {
	c.cond.L.Lock()
	defer c.cond.Broadcast()
	defer c.cond.L.Unlock()

	if c.conn != nil {
		return nil
	}
	for {
		if c.closed.Load() {
			return c.closeReason
		}
		if c.Gcf == nil {
			return errors.New("Gcf is not set")
		}
		conn, err := c.Gcf("tcp", c.Addr)

		// Try indefinitely to connect to addr
		if err != nil {
			continue
		}

		if c.closed.Load() {
			// We are closed before dial success, closing the new conn since we are closed
			conn.Close()
			return c.closeReason
		}

		c.conn = conn
		break
	}

	return nil
}

func (c *Conn) read() {
	// there is still a little bit of rpc logic here (len as the first 4 byte, rpc byte layout assumption)
	// but i think thats fine for now
	lenBytes := make([]byte, 4)
Outer:
	for {
		if c.closed.Load() {
			break
		}

		if c.ReadDln == 0 {
			c.ReadDln = DEFAULT_READ_DLN
		}

		conn, err := c.waitConn()
		if err != nil {
			break
		}
		// ignoring the error here:
		// - if conn closed -> this won't matter
		// - if conn does not support dln, thats ok
		conn.SetReadDeadline(c.Now().Add(c.ReadDln))
		_, err = io.ReadFull(conn, lenBytes)
		if err != nil {
			if c.closed.Load() {
				break
			}

			// if somehow c.closed is still false, check if conn closed by us, break
			if errors.Is(err, net.ErrClosed) {
				break
			}

			// Peer closed, re-dial, and wait
			if errors.Is(err, io.EOF) {
				c.markBadConn(conn)
			}

			// Raft spec specifies any error from peer will causes indefinite retry, we will do so here
			continue
		}

		frameLen := binary.BigEndian.Uint32(lenBytes)
		bs := make([]byte, frameLen)

		for {
			if c.ReadDln == 0 {
				c.ReadDln = DEFAULT_READ_DLN
			}

			// ignoring the error here:
			// - if conn closed -> this won't matter
			// - if conn does not support dln, thats ok
			conn, err := c.waitConn()
			if err != nil {
				break Outer
			}
			conn.SetReadDeadline(c.Now().Add(c.ReadDln))
			_, err = io.ReadFull(conn, bs)
			if err != nil {
				if c.closed.Load() {
					break Outer
				}

				// if somehow c.closed is still false, check if conn closed by us, break
				if errors.Is(err, net.ErrClosed) {
					break Outer
				}

				// Peer closed, re-dial, and wait
				if errors.Is(err, io.EOF) {
					err := c.markBadConn(conn)
					if err != nil {
						break Outer
					}

					// previous frame len will be invalid, restart from reading frame len after new conn is ready
					continue Outer
				}

				// Raft spec specifies any error from peer will causes indefinite retry, we will do so here
				continue
			}
			break

		}
		c.readCh <- bs
	}

}

func (c *Conn) write() {
Outer:
	for {
		var bs []byte
		select {
		case <-c.closeCh:
			break Outer
		case bs = <-c.writeCh:
		}

		frameLen := make([]byte, 4)
		binary.BigEndian.PutUint32(frameLen, uint32(len(bs)))
		fullFrame := append(frameLen, bs...)

		ffLen := len(fullFrame)
		written := 0
		for written < ffLen {
			if c.closed.Load() {
				break Outer
			}

			conn, err := c.waitConn()
			if err != nil {
				break Outer
			}

			if c.WriteDln == 0 {
				c.WriteDln = DEFAULT_WRITE_DLN
			}
			// ignoring the error here:
			// - if conn closed -> this won't matter
			// - if conn does not support dln, thats ok
			conn.SetWriteDeadline(c.Now().Add(c.WriteDln))
			n, err := conn.Write(fullFrame[written:])
			written += n

			// Note: because of tcp buffer, the write looks like successful, but actually not
			// Not a huge problem here since we will not get a response, and each request is idempotent
			if err != nil {
				// if somehow c.closed is still false, check if conn closed by us, break
				if c.closed.Load() || errors.Is(err, net.ErrClosed) {
					break Outer
				}

				// connection closed by peer
				if errors.Is(err, syscall.EPIPE) || errors.Is(err, syscall.ECONNRESET) {
					err := c.markBadConn(conn)
					if err != nil {
						break Outer
					}

					// since the bytes are not fully sent, after the peer restarted, send from start
					continue Outer
				}

				// Raft spec specifies any error from peer will causes indefinite retry, we will do so here
				continue
			}
		}
	}
}
