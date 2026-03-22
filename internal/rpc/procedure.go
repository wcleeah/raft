package rpc

import "encoding/binary"

type Frame struct {
	RPCType    uint
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

// byte layout:
//
//	Term -> 2 byte
//	LeaderId -> len
//	LeaderCommit -> 4 byte
//	PrevLogIndex -> 4 byte
//	PrevLogTerm -> 2 byte
//	Entries -> len
type AppendEntriesReq struct {
	Term         uint16
	LeaderCommit uint32
	PrevLogIndex uint32
	PrevLogTerm  uint16
	LeaderId     string
	Entries      []byte // up to the user to define how the entries are split, or not split, or each entry is just a byte. the rpc layer is only responsible for converting the incoming stream to a byte array
}

func (aer AppendEntriesReq) Encode() []byte {
	leaderIdInBytes := []byte(aer.LeaderId)
	out := make([]byte, 14+len(leaderIdInBytes)+len(aer.Entries))

	binary.BigEndian.PutUint16(out[0:2], aer.Term)
	binary.BigEndian.PutUint32(out[2:6], aer.LeaderCommit)
	binary.BigEndian.PutUint32(out[6:10], aer.PrevLogIndex)
	binary.BigEndian.PutUint16(out[10:12], aer.PrevLogTerm)
	binary.BigEndian.PutUint16(out[12:14], uint16(len(leaderIdInBytes)))
	copy(out[14:], leaderIdInBytes)
	copy(out[14+len(leaderIdInBytes):], aer.Entries)

	return out
}

func DecodeAppendEntriesReq(bs []byte) *AppendEntriesReq {
	aer := &AppendEntriesReq{
		Term:         binary.BigEndian.Uint16(bs[0:2]),
		LeaderCommit: binary.BigEndian.Uint32(bs[2:6]),
		PrevLogIndex: binary.BigEndian.Uint32(bs[6:10]),
		PrevLogTerm:  binary.BigEndian.Uint16(bs[10:12]),
	}

	leaderIdLen := binary.BigEndian.Uint16(bs[12:14])
	aer.LeaderId = string(bs[14 : 14+leaderIdLen])
	aer.Entries = make([]byte, len(bs[14+leaderIdLen:]))

	// The copy path is intentional:
	// - Directly assigning the slice view keeps the underlying array alive, and risk unexpected alias problem
	// - This does incur an extra copy, but as if any performance optimization guide will say, only optimise when it is actually the problem
	copy(aer.Entries, bs[14+leaderIdLen:])

	return aer
}

// byte layout:
//
//	Term -> 2 byte
//	Success -> 1 byte
type AppendEntriesRes struct {
	Term    uint16
	Success bool
}

func (aer AppendEntriesRes) Encode() []byte {
	out := make([]byte, 3)

	binary.BigEndian.PutUint16(out[0:2], aer.Term)
	out[2] = boolToByte(aer.Success)

	return out
}

func DecodeAppendEntriesRes(bs []byte) *AppendEntriesRes {
	return &AppendEntriesRes{
		Term:    binary.BigEndian.Uint16(bs[0:2]),
		Success: byteToBool(bs[2]),
	}
}

// byte layout:
//
//	Term -> 2 byte
//	LastLogIndex -> 4 byte
//	LastLogTerm -> 2 byte
//	CandidateId -> 1 byte
type RequestVoteReq struct {
	Term         uint16
	LastLogIndex uint32
	LastLogTerm  uint16
	CandidateId  string
}

func (rvr RequestVoteReq) Encode() []byte {
	candidateIdInBytes := []byte(rvr.CandidateId)
	out := make([]byte, 8+len(candidateIdInBytes))

	binary.BigEndian.PutUint16(out[0:2], rvr.Term)
	binary.BigEndian.PutUint32(out[2:6], rvr.LastLogIndex)
	binary.BigEndian.PutUint16(out[6:8], rvr.LastLogTerm)
	copy(out[8:], candidateIdInBytes)

	return out
}

func DecodeRequestVoteReq(bs []byte) *RequestVoteReq {
	return &RequestVoteReq{
		Term:         binary.BigEndian.Uint16(bs[0:2]),
		LastLogIndex: binary.BigEndian.Uint32(bs[2:6]),
		LastLogTerm:  binary.BigEndian.Uint16(bs[6:8]),
		CandidateId:  string(bs[8:]),
	}
}

// byte layout:
//
//	Term -> 2 byte
//	VoteGranted -> 1 byte
type RequestVoteRes struct {
	Term        uint16
	VoteGranted bool
}

func (rvr RequestVoteRes) Encode() []byte {
	out := make([]byte, 3)

	binary.BigEndian.PutUint16(out[0:2], rvr.Term)
	out[2] = boolToByte(rvr.VoteGranted)

	return out
}

func DecodeRequestVoteRes(bs []byte) *RequestVoteRes {
	return &RequestVoteRes{
		Term:        binary.BigEndian.Uint16(bs[0:2]),
		VoteGranted: byteToBool(bs[2]),
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
