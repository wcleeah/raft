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

func TestSimpleRead(t *testing.T) {
	assert := assert.New(t)

	f, sendBs := getBs()
	readDln := 5 * time.Second

	netConn := &fakeConn{
		Cond:    sync.NewCond(&sync.Mutex{}),
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
		assert.Failf("Simple Read", "TestBs mismatch (-want +got):\n%s", diff)
	}
}

func TestSimpleWrite(t *testing.T) {
	assert := assert.New(t)

	f, testBs := getBs()
	writeDln := 9 * time.Second
	netConn := &fakeConn{
		Cond:      sync.NewCond(&sync.Mutex{}),
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
		assert.Failf("Simple Write", "TestBs mismatch (-want +got):\n%s", diff)
	}
}

func TestMultiWrite(t *testing.T) {
	assert := assert.New(t)

	f, testBs := getBs()
	writeDln := 9 * time.Second
	netConn := &fakeConn{
		Cond:      sync.NewCond(&sync.Mutex{}),
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
		assert.Failf("MultiWrite", "TestBs mismatch (-want +got):\n%s", diff)
	}
}

func TestClose(t *testing.T) {
	assert := assert.New(t)

	d := deps{
		NowSnapshot: time.Now(),
	}
	d.SetConn(&fakeConn{
		Cond: sync.NewCond(&sync.Mutex{}),
	})

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
		Cond:    sync.NewCond(&sync.Mutex{}),
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

	type readRes struct {
		GotBs []byte
		Err   error
	}

	readResCh := make(chan readRes)
	go func() {
		gotBs, err := serverConn.Read()
		readResCh <- readRes{
			GotBs: gotBs,
			Err:   err,
		}
	}()

	netConn.EnsureReadErr()
	netConn.SetReadErr(nil)
	rr := <-readResCh

	assert.NoError(rr.Err, "Unexpected error during reading")
	assert.Equal(d.Now().Add(readDln), netConn.ReadDln, "Dln mismatch")
	if diff := cmp.Diff(f.Encode(), rr.GotBs); diff != "" {
		assert.Failf("Keep Trying: read error", "TestBs mismatch (-want +got):\n%s", diff)
	}
}

func TestKeepTrying_WriteError(t *testing.T) {
	assert := assert.New(t)

	f, testBs := getBs()
	writeDln := 9 * time.Second
	netConn := &fakeConn{
		Cond:      sync.NewCond(&sync.Mutex{}),
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

	type writeRes struct {
		Err error
	}

	writeResCh := make(chan writeRes)
	go func() {
		err = serverConn.Write(f.Encode())

		// .Write sends the bs to another go routine for writing, use ch to ensure we check after that go routine written to conn
		<-netConn.WrittenCh
		writeResCh <- writeRes{
			Err: err,
		}
	}()

	netConn.EnsureWriteErr()
	netConn.SetWriteErr(nil)

	wr := <-writeResCh
	assert.NoError(wr.Err, "Unexpected error during write")

	assert.Equal(d.Now().Add(writeDln), netConn.WriteDln, "Dln mismatch")
	if diff := cmp.Diff(testBs, d.conn.WriteBuf); diff != "" {
		assert.Failf("Keep Trying: write error", "TestBs mismatch (-want +got):\n%s", diff)
	}
}

func TestKeepTrying_ClosedConn(t *testing.T) {
	ass := assert.New(t)

	f, testBs := getBs()
	writeDln := 9 * time.Second
	readDln := 14 * time.Second
	netConn := &fakeConn{
		Cond:      sync.NewCond(&sync.Mutex{}),
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
	ass.NoError(err, "Unexpected error during start")

	netConn.Close()

	newNetConn := &fakeConn{
		Cond:      sync.NewCond(&sync.Mutex{}),
		MaxWrite:  4096,
		ReadBuf:   make([]byte, 0),
		WriteBuf:  make([]byte, 0),
		WrittenCh: make(chan struct{}, 10),
	}
	newNetConn.AddReadBuf(testBs)

	type readRes struct {
		GotBs []byte
		Err   error
	}
	type writeRes struct {
		Err error
	}

	readResCh := make(chan readRes)
	writeResCh := make(chan writeRes)
	go func() {
		gotBs, err := serverConn.Read()
		readResCh <- readRes{
			GotBs: gotBs,
			Err:   err,
		}
	}()

	go func() {
		err = serverConn.Write(f.Encode())
		<-newNetConn.WrittenCh

		writeResCh <- writeRes{
			Err: err,
		}
	}()

	netConn.EnsureClose()
	d.SetConn(newNetConn)

	rr := <-readResCh
	wr := <-writeResCh

	ass.NoError(rr.Err, "Unexpected error during reading")
	ass.Equal(d.Now().Add(readDln), newNetConn.ReadDln, "Read Dln mismatch")
	if diff := cmp.Diff(f.Encode(), rr.GotBs); diff != "" {
		ass.Failf("Read TestBs mismatch (-want +got):\n%s", diff)
	}

	ass.NoError(wr.Err, "Unexpected error during reading")
	ass.Equal(d.Now().Add(writeDln), newNetConn.WriteDln, "Write Dln mismatch")
	if diff := cmp.Diff(testBs, newNetConn.WriteBuf); diff != "" {
		ass.Failf("Write TestBs mismatch (-want +got):\n%s", diff)
	}
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

type fakeConn struct {
	Cond *sync.Cond

	MaxWrite              int
	Closed                bool
	ReadDln               time.Time
	WriteDln              time.Time
	ReadBuf               []byte
	WriteBuf              []byte
	WrittenCh             chan struct{}
	ReadErr               error
	WriteErr              error
	ReadCloseEncountered  bool
	WriteCloseEncountered bool
	ReadErrEncountered    bool
	WriteErrEncountered   bool
}

func (f *fakeConn) EnsureReadErr() {
	f.Cond.L.Lock()
	defer f.Cond.L.Unlock()

	for !f.ReadErrEncountered {
		f.Cond.Wait()
	}
}

func (f *fakeConn) EnsureWriteErr() {
	f.Cond.L.Lock()
	defer f.Cond.L.Unlock()

	for !f.WriteErrEncountered {
		f.Cond.Wait()
	}
}

func (f *fakeConn) EnsureClose() {
	f.Cond.L.Lock()
	defer f.Cond.L.Unlock()

	for !f.ReadCloseEncountered && !f.WriteCloseEncountered {
		f.Cond.Wait()
	}
}

func (f *fakeConn) AddReadBuf(b []byte) {
	f.Cond.L.Lock()
	defer f.Cond.L.Unlock()

	f.ReadBuf = append(f.ReadBuf, b...)
}

func (f *fakeConn) Read(b []byte) (n int, err error) {
	f.Cond.L.Lock()
	defer f.Cond.L.Unlock()

	if f.ReadErr != nil {
		if !f.ReadErrEncountered {
			f.ReadErrEncountered = true
			f.Cond.Broadcast()
		}
		return 0, f.ReadErr
	}
	if f.Closed {
		if !f.ReadCloseEncountered {
			f.ReadCloseEncountered = true
			f.Cond.Broadcast()
		}
		return 0, io.EOF
	}

	n = copy(b, f.ReadBuf)
	f.ReadBuf = f.ReadBuf[n:]

	return
}

func (f *fakeConn) SetReadErr(err error) {
	f.Cond.L.Lock()
	defer f.Cond.L.Unlock()

	f.ReadErr = err
}

func (f *fakeConn) Write(b []byte) (n int, err error) {
	f.Cond.L.Lock()
	defer f.Cond.L.Unlock()

	if f.WriteErr != nil {
		if !f.WriteErrEncountered {
			f.WriteErrEncountered = true
			f.Cond.Broadcast()
		}
		return 0, f.WriteErr
	}
	if f.Closed {
		if !f.WriteCloseEncountered {
			f.WriteCloseEncountered = true
			f.Cond.Broadcast()
		}
		return 0, syscall.ECONNRESET
	}

	n = min(f.MaxWrite, len(b))
	f.WriteBuf = append(f.WriteBuf, b[:n]...)
	f.WrittenCh <- struct{}{}

	return
}

func (f *fakeConn) SetWriteErr(err error) {
	f.Cond.L.Lock()
	defer f.Cond.L.Unlock()

	f.WriteErr = err
}

func (f *fakeConn) Close() error {
	f.Cond.L.Lock()
	defer f.Cond.L.Unlock()

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
