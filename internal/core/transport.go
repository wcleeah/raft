package core

import "com.lwc.raft/internal/rpc"

type TransportHandler func(id string, frame rpc.Frame, relatedReqFrame rpc.Frame) (rpc.RpcPayload, error) 

type Transport interface {
	Boardcast(rpc.RpcPayload)
	Send(id string, payload rpc.RpcPayload)
	Listen(TransportHandler)
	RegisterPeer(id string, addr string, th TransportHandler)
}
