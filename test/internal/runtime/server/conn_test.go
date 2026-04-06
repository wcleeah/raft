package server_test

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"syscall"
	"testing"
	"time"

	"com.lwc.raft/internal/rpc"
	"com.lwc.raft/internal/runtime/server"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
)

type fakeConn struct {
	mu sync.Mutex

	MaxWrite  int
	Closed    bool
	ReadDln   time.Time
	WriteDln  time.Time
	WriteBuf  []byte
	WrittenCh chan struct{}
	ReadBuf   []byte
	ReadErr   error
	WriteErr  error
}

func (f *fakeConn) AddReadBuf(b []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.ReadBuf = append(f.ReadBuf, b...)
}

func (f *fakeConn) Read(b []byte) (n int, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.ReadErr != nil {
		return 0, f.ReadErr
	}
	if f.Closed {
		return 0, io.EOF
	}

	n = copy(b, f.ReadBuf)
	f.ReadBuf = f.ReadBuf[n:]

	return
}

func (f *fakeConn) SetReadErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.ReadErr = err
}

func (f *fakeConn) Write(b []byte) (n int, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.WriteErr != nil {
		return 0, f.WriteErr
	}
	if f.Closed {
		return 0, syscall.ECONNRESET
	}

	n = min(f.MaxWrite, len(b))
	f.WriteBuf = append(f.WriteBuf, b[:n]...)
	f.WrittenCh <- struct{}{}

	return
}

func (f *fakeConn) SetWriteErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.WriteErr = err
}

func (f *fakeConn) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.Closed = true

	return nil
}

func (f *fakeConn) LocalAddr() net.Addr {
	return nil
}

func (f *fakeConn) RemoteAddr() net.Addr {
	return nil
}

func (f *fakeConn) SetDeadline(t time.Time) error {
	return nil
}

func (f *fakeConn) SetReadDeadline(t time.Time) error {
	f.ReadDln = t
	return nil
}

func (f *fakeConn) SetWriteDeadline(t time.Time) error {
	f.WriteDln = t
	return nil
}

type deps struct {
	mu   sync.Mutex
	conn *fakeConn

	NowSnapshot time.Time
}

func (d *deps) Gcf(network string, addr string) (net.Conn, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.conn == nil || d.conn.Closed {
		return nil, errors.New("No conn")
	}
	return d.conn, nil
}

func (d *deps) Now() time.Time {
	return d.NowSnapshot
}

func (d *deps) SetConn(conn *fakeConn) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.conn = conn
}

func TestSimpleRead(t *testing.T) {
	assert := assert.New(t)

	f, sendBs := getBs()
	readDln := 5 * time.Second

	netConn := &fakeConn{
		ReadBuf: make([]byte, 0),
	}
	netConn.AddReadBuf(sendBs)
	d := deps{
		NowSnapshot: time.Now(),
	}
	d.SetConn(netConn)

	serverConn := server.Conn{
		Addr:    "test",
		Gcf:     d.Gcf,
		Now:     d.Now,
		ReadDln: readDln,
	}

	err := serverConn.Start()
	assert.NoError(err, "Unexpected error when starting connection")

	gotBs, err := serverConn.Read()

	assert.NoError(err, "Unexpected error during reading")
	assert.Equal(d.Now().Add(readDln), netConn.ReadDln, "Dln mismatch")
	if diff := cmp.Diff(f.Encode(), gotBs); diff != "" {
		t.Fatalf("TestBs mismatch (-want +got):\n%s", diff)
	}
}

func TestSimpleWrite(t *testing.T) {
	assert := assert.New(t)

	f, testBs := getBs()
	writeDln := 9 * time.Second
	netConn := &fakeConn{
		MaxWrite:  4096,
		WriteBuf:  make([]byte, 0),
		WrittenCh: make(chan struct{}, 10),
	}

	d := deps{
		NowSnapshot: time.Now(),
	}
	d.SetConn(netConn)

	serverConn := server.Conn{
		Addr:     "test",
		Gcf:      d.Gcf,
		Now:      d.Now,
		WriteDln: writeDln,
	}

	err := serverConn.Start()
	assert.NoError(err, "Unexpected error during start")

	err = serverConn.Write(f.Encode())
	assert.NoError(err, "Unexpected error during write")

	// .Write sends the bs to another go routine for writing, use ch to ensure we check after that go routine written to conn
	<-d.conn.WrittenCh

	assert.Equal(d.Now().Add(writeDln), netConn.WriteDln, "Dln mismatch")
	if diff := cmp.Diff(testBs, d.conn.WriteBuf); diff != "" {
		t.Fatalf("TestBs mismatch (-want +got):\n%s", diff)
	}
}

func TestMultiWrite(t *testing.T) {
	assert := assert.New(t)

	f, testBs := getBs()
	writeDln := 9 * time.Second
	netConn := &fakeConn{
		MaxWrite:  1,
		WriteBuf:  make([]byte, 0),
		WrittenCh: make(chan struct{}, 10),
	}

	d := deps{
		NowSnapshot: time.Now(),
	}
	d.SetConn(netConn)

	serverConn := server.Conn{
		Addr:     "test",
		Gcf:      d.Gcf,
		Now:      d.Now,
		WriteDln: writeDln,
	}

	err := serverConn.Start()
	assert.NoError(err, "Unexpected error during start")

	err = serverConn.Write(f.Encode())
	assert.NoError(err, "Unexpected error during write")

	// .Write sends the bs to another go routine for writing, use ch to ensure we check after that go routine written to conn
	for range len(testBs) {
		select {
		case <-d.conn.WrittenCh:
		case <-time.After(2 * time.Second):
			assert.FailNow("Unexpected timeout")
		}
	}

	assert.Equal(d.Now().Add(writeDln), netConn.WriteDln, "Dln mismatch")
	if diff := cmp.Diff(testBs, d.conn.WriteBuf); diff != "" {
		t.Fatalf("TestBs mismatch (-want +got):\n%s", diff)
	}
}

func TestClose(t *testing.T) {
	assert := assert.New(t)

	d := deps{
		NowSnapshot: time.Now(),
	}
	d.SetConn(&fakeConn{})

	serverConn := server.Conn{
		Addr: "test",
		Gcf:  d.Gcf,
		Now:  d.Now,
	}
	closeReason := errors.New("Closed")
	serverConn.Close(closeReason)

	err := serverConn.Start()
	assert.EqualError(err, closeReason.Error(), "Start should return close error")

	_, err = serverConn.Read()
	assert.EqualError(err, closeReason.Error(), "Read should return close error")

	err = serverConn.Write([]byte{})
	assert.EqualError(err, closeReason.Error(), "Write should return close error")
}

func TestKeetTrying_ReadError(t *testing.T) {
	assert := assert.New(t)

	f, sendBs := getBs()
	readDln := 5 * time.Second

	netConn := &fakeConn{
		ReadBuf: make([]byte, 0),
	}
	netConn.AddReadBuf(sendBs)
	netConn.SetReadErr(errors.New("Keep trying read"))

	d := deps{
		NowSnapshot: time.Now(),
	}
	d.SetConn(netConn)

	serverConn := server.Conn{
		Addr:    "test",
		Gcf:     d.Gcf,
		Now:     d.Now,
		ReadDln: readDln,
	}

	err := serverConn.Start()
	assert.NoError(err, "Unexpected error when starting connection")

	go func(conn *fakeConn) {
		select {
		case <-time.After(1 * time.Second):
			conn.SetReadErr(nil)
		}
	}(netConn)
	gotBs, err := serverConn.Read()

	assert.NoError(err, "Unexpected error during reading")
	assert.Equal(d.Now().Add(readDln), netConn.ReadDln, "Dln mismatch")
	if diff := cmp.Diff(f.Encode(), gotBs); diff != "" {
		t.Fatalf("TestBs mismatch (-want +got):\n%s", diff)
	}
}

func TestKeepTrying_WriteError(t *testing.T) {
	assert := assert.New(t)

	f, testBs := getBs()
	writeDln := 9 * time.Second
	netConn := &fakeConn{
		MaxWrite:  4096,
		WriteBuf:  make([]byte, 0),
		WrittenCh: make(chan struct{}, 10),
	}
	netConn.SetWriteErr(errors.New("Keep trying write"))

	d := deps{
		NowSnapshot: time.Now(),
	}
	d.SetConn(netConn)

	serverConn := server.Conn{
		Addr:     "test",
		Gcf:      d.Gcf,
		Now:      d.Now,
		WriteDln: writeDln,
	}

	err := serverConn.Start()
	assert.NoError(err, "Unexpected error during start")

	go func(conn *fakeConn) {
		select {
		case <-time.After(1 * time.Second):
			conn.SetWriteErr(nil)
		}
	}(netConn)

	err = serverConn.Write(f.Encode())
	assert.NoError(err, "Unexpected error during write")

	// .Write sends the bs to another go routine for writing, use ch to ensure we check after that go routine written to conn
	<-d.conn.WrittenCh

	assert.Equal(d.Now().Add(writeDln), netConn.WriteDln, "Dln mismatch")
	if diff := cmp.Diff(testBs, d.conn.WriteBuf); diff != "" {
		t.Fatalf("TestBs mismatch (-want +got):\n%s", diff)
	}
}

func TestKeepTrying_ClosedConn(t *testing.T) {
	assert := assert.New(t)

	f, testBs := getBs()
	writeDln := 9 * time.Second
	readDln := 14 * time.Second
	netConn := &fakeConn{
		MaxWrite:  4096,
		ReadBuf:   make([]byte, 0),
		WriteBuf:  make([]byte, 0),
		WrittenCh: make(chan struct{}, 10),
	}

	d := deps{
		NowSnapshot: time.Now(),
	}
	d.SetConn(netConn)

	serverConn := server.Conn{
		Addr:     "test",
		Gcf:      d.Gcf,
		Now:      d.Now,
		ReadDln:  readDln,
		WriteDln: writeDln,
	}

	err := serverConn.Start()
	assert.NoError(err, "Unexpected error during start")

	netConn.Close()

	newNetConn := &fakeConn{
		MaxWrite:  4096,
		ReadBuf:   make([]byte, 0),
		WriteBuf:  make([]byte, 0),
		WrittenCh: make(chan struct{}, 10),
	}
	newNetConn.AddReadBuf(testBs)

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		t.Helper()
		gotBs, err := serverConn.Read()

		assert.NoError(err, "Unexpected error during reading")
		assert.Equal(d.Now().Add(readDln), newNetConn.ReadDln, "Read Dln mismatch")
		if diff := cmp.Diff(f.Encode(), gotBs); diff != "" {
			t.Fatalf("Read TestBs mismatch (-want +got):\n%s", diff)
		}
	}()

	go func() {
		defer wg.Done()
		t.Helper()
		err = serverConn.Write(f.Encode())
		assert.NoError(err, "Unexpected error during write")

		// .Write sends the bs to another go routine for writing, use ch to ensure we check after that go routine written to conn
		<-newNetConn.WrittenCh

		assert.Equal(d.Now().Add(writeDln), newNetConn.WriteDln, "Write Dln mismatch")
		if diff := cmp.Diff(testBs, d.conn.WriteBuf); diff != "" {
			t.Fatalf("Write TestBs mismatch (-want +got):\n%s", diff)
		}
	}()

	d.SetConn(newNetConn)

	wg.Wait()
}

func getBs() (rpc.Frame, []byte) {
	f := rpc.Frame{
		RelationId: 10,
		RPCType:    rpc.RPC_TYPE_APPEND_ENTRIES_REQ,
		Payload:    []byte("Simple read over over"),
	}
	testBs := f.Encode()

	final := make([]byte, 4)
	binary.BigEndian.PutUint32(final, uint32(len(testBs)))

	return f, append(final, testBs...)
}
