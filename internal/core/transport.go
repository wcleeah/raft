package core

import "com.lwc.raft/internal/rpc"

type Transport interface {
	Boardcast(rpc.RpcPayload)
	Send(id string, payload rpc.RpcPayload)
	Listen(func(id string, frame *rpc.Frame) (rpc.RpcPayload, error))
	RegisterPeer(id string, addr string)
}
