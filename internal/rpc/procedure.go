package rpc

import "encoding/binary"

type Frame struct {
	RPCType    RPC_TYPE
	RelationId uint16
	Payload    []byte
}

// byte layout:
//
//	RPCType -> 1 byte
//	RelationId -> 2 byte
//	Payload -> len
func (rb Frame) Encode() []byte {
	out := make([]byte, 3+len(rb.Payload))

	out[0] = byte(rb.RPCType)
	binary.BigEndian.PutUint16(out[1:3], rb.RelationId)
	copy(out[3:], rb.Payload)

	return out
}

func DecodeRPCBase(bs []byte) *Frame {
	rb := &Frame{
		RPCType:    uint(bs[0]),
		RelationId: binary.BigEndian.Uint16(bs[1:3]),
		Payload:    make([]byte, len(bs[3:])),
	}

	copy(rb.Payload, bs[3:])

	return rb
}

type RpcPayload interface {
	Encode() []byte
}

// byte layout:
//
//	Term -> 4 byte
//	LeaderId -> len
//	LeaderCommit -> 4 byte
//	PrevLogIndex -> 4 byte
//	PrevLogTerm -> 4 byte
//	Entries -> len
type AppendEntriesReq struct {
	Term         uint32
	LeaderCommit uint32
	PrevLogIndex uint32
	PrevLogTerm  uint32
	LeaderId     string
	Entries      []byte // up to the user to define how the entries are split, or not split, or each entry is just a byte. the rpc layer is only responsible for converting the incoming stream to a byte array
}

func (aer AppendEntriesReq) Encode() []byte {
	leaderIdInBytes := []byte(aer.LeaderId)
	out := make([]byte, 18+len(leaderIdInBytes)+len(aer.Entries))

	binary.BigEndian.PutUint32(out[0:4], aer.Term)
	binary.BigEndian.PutUint32(out[4:8], aer.LeaderCommit)
	binary.BigEndian.PutUint32(out[8:12], aer.PrevLogIndex)
	binary.BigEndian.PutUint32(out[12:16], aer.PrevLogTerm)
	binary.BigEndian.PutUint16(out[16:18], uint16(len(leaderIdInBytes)))
	copy(out[18:], leaderIdInBytes)
	copy(out[18+len(leaderIdInBytes):], aer.Entries)

	return out
}

func DecodeAppendEntriesReq(bs []byte) *AppendEntriesReq {
	aer := &AppendEntriesReq{
		Term:         binary.BigEndian.Uint32(bs[0:4]),
		LeaderCommit: binary.BigEndian.Uint32(bs[4:8]),
		PrevLogIndex: binary.BigEndian.Uint32(bs[8:12]),
		PrevLogTerm:  binary.BigEndian.Uint32(bs[12:16]),
	}

	leaderIdLen := binary.BigEndian.Uint16(bs[16:18])
	aer.LeaderId = string(bs[18 : 18+leaderIdLen])
	aer.Entries = make([]byte, len(bs[18+leaderIdLen:]))

	// The copy path is intentional:
	// - Directly assigning the slice view keeps the underlying array alive, and risk unexpected alias problem
	// - This does incur an extra copy, but as if any performance optimization guide will say, only optimise when it is actually the problem
	copy(aer.Entries, bs[18+leaderIdLen:])

	return aer
}

// byte layout:
//
//	Term -> 4 byte
//	Success -> 1 byte
type AppendEntriesRes struct {
	Term    uint32
	Success bool
}

func (aer AppendEntriesRes) Encode() []byte {
	out := make([]byte, 5)

	binary.BigEndian.PutUint32(out[0:4], aer.Term)
	out[4] = boolToByte(aer.Success)

	return out
}

func DecodeAppendEntriesRes(bs []byte) *AppendEntriesRes {
	return &AppendEntriesRes{
		Term:    binary.BigEndian.Uint32(bs[0:4]),
		Success: byteToBool(bs[4]),
	}
}

// byte layout:
//
//	Term -> 4 byte
//	LastLogIndex -> 4 byte
//	LastLogTerm -> 4 byte
//	CandidateId -> 1 byte
type RequestVoteReq struct {
	Term         uint32
	LastLogIndex uint32
	LastLogTerm  uint32
	CandidateId  string
}

func (rvr RequestVoteReq) Encode() []byte {
	candidateIdInBytes := []byte(rvr.CandidateId)
	out := make([]byte, 12+len(candidateIdInBytes))

	binary.BigEndian.PutUint32(out[0:4], rvr.Term)
	binary.BigEndian.PutUint32(out[4:8], rvr.LastLogIndex)
	binary.BigEndian.PutUint32(out[8:12], rvr.LastLogTerm)
	copy(out[12:], candidateIdInBytes)

	return out
}

func DecodeRequestVoteReq(bs []byte) *RequestVoteReq {
	return &RequestVoteReq{
		Term:         binary.BigEndian.Uint32(bs[0:4]),
		LastLogIndex: binary.BigEndian.Uint32(bs[4:8]),
		LastLogTerm:  binary.BigEndian.Uint32(bs[8:12]),
		CandidateId:  string(bs[12:]),
	}
}

// byte layout:
//
//	Term -> 4 byte
//	VoteGranted -> 1 byte
type RequestVoteRes struct {
	Term        uint32
	VoteGranted bool
}

func (rvr RequestVoteRes) Encode() []byte {
	out := make([]byte, 5)

	binary.BigEndian.PutUint32(out[0:4], rvr.Term)
	out[4] = boolToByte(rvr.VoteGranted)

	return out
}

func DecodeRequestVoteRes(bs []byte) *RequestVoteRes {
	return &RequestVoteRes{
		Term:        binary.BigEndian.Uint32(bs[0:4]),
		VoteGranted: byteToBool(bs[4]),
	}
}

// byte layout:
//
//	CounterDelta -> 2 byte
//	Action -> 1 byte
type StateActionReq struct {
	CounterDelta uint16
	Action       uint8
}

func (rvr StateActionReq) Encode() []byte {
	out := make([]byte, 3)

	binary.BigEndian.PutUint16(out[0:2], rvr.CounterDelta)
	out[2] = rvr.Action

	return out
}

func DecodeStateActionReq(bs []byte) *StateActionReq {
	return &StateActionReq{
		CounterDelta: binary.BigEndian.Uint16(bs[0:2]),
		Action:       bs[2],
	}
}

type StateActionRes struct {
	Success      bool
	RedirectAddr string
}

func (rvr StateActionRes) Encode() []byte {
	out := make([]byte, 1+len(rvr.RedirectAddr))

	out[0] = boolToByte(rvr.Success)
	copy(out[1:], []byte(rvr.RedirectAddr))

	return out
}

func DecodeStateActionRes(bs []byte) *StateActionRes {
	return &StateActionRes{
		Success:      byteToBool(bs[0]),
		RedirectAddr: string(bs[1:]),
	}
}
func boolToByte(v bool) byte {
	if v {
		return 0x01
	}

	return 0x00
}

func byteToBool(b byte) bool {
	return b == 0x01
}
