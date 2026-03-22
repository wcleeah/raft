package rpc_test

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"testing"

	"com.lwc.raft/internal/rpc"
	"github.com/google/go-cmp/cmp"
)

func TestRead(t *testing.T) {
	frame := rpc.Frame{
		RPCType: 1,
		RelationId: 2,
		Payload: []byte("Read Read Read Read Read Not WRiTe"),
	}

	frameBytes := frame.Encode()
	frameLen := make([]byte, 4)
	binary.BigEndian.PutUint32(frameLen, uint32(len(frameBytes)))
	fullFrame := append(frameLen, frameBytes...)

	client, server := net.Pipe()
	rpclistener := rpc.NewRPC(context.Background(), server)

	outCh := make(chan *rpc.Frame)

	go rpclistener.Read(outCh)
	_, err := client.Write(fullFrame)
	if err != nil {
		t.Fatal(err)
	}
	serverFrame := <-outCh

	if diff := cmp.Diff(frame, *serverFrame); diff != "" {
		t.Fatalf("Read frame mismatch, (-want +got):\n%s", diff)
	}
}

func TestWrite(t * testing.T) {
	frame := rpc.Frame{
		RPCType: 2,
		RelationId: 3,
		Payload: []byte("???? Write This is for all my writes and readssSSS!!!"),
	}

	client, server := net.Pipe()
	rpclistener := rpc.NewRPC(context.Background(), server)
	rpclistener.Setup()

	rpclistener.Write(&frame)

	frameBytes := frame.Encode()
	frameLen := make([]byte, 4)
	binary.BigEndian.PutUint32(frameLen, uint32(len(frameBytes)))
	fullFrame := append(frameLen, frameBytes...)

	clientFrame := make([]byte, len(fullFrame))
	_, err := io.ReadFull(client, clientFrame)
	if err != nil {
		t.Fatal(err)
	}

	if diff := cmp.Diff(fullFrame, clientFrame); diff != "" {
		t.Fatalf("Read frame mismatch, (-want +got):\n%s", diff)
	}
}
