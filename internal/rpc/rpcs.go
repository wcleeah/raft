package rpc

import "encoding/binary"

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
	LeaderId     uint8
	Entries      []byte // up to the user to define how the entries are split, or not split, or each entry is just a byte. the rpc layer is only responsible for converting the incoming stream to a byte array
}

func (aer *AppendEntriesReq) ToBytes() []byte {
	out := make([]byte, 13+len(aer.Entries))

	binary.BigEndian.PutUint16(out[0:2], aer.Term)
	binary.BigEndian.PutUint32(out[2:6], aer.LeaderCommit)
	binary.BigEndian.PutUint32(out[6:10], aer.PrevLogIndex)
	binary.BigEndian.PutUint16(out[10:12], aer.PrevLogTerm)
	out[12] = aer.LeaderId
	copy(out[13:], aer.Entries)

	return out
}

func ToAppendEntriesReq(bs []byte) *AppendEntriesReq {
	aer := &AppendEntriesReq{
		Term:         binary.BigEndian.Uint16(bs[0:2]),
		LeaderCommit: binary.BigEndian.Uint32(bs[2:6]),
		PrevLogIndex: binary.BigEndian.Uint32(bs[6:10]),
		PrevLogTerm:  binary.BigEndian.Uint16(bs[10:12]),
		LeaderId:     bs[12],
		Entries:      make([]byte, len(bs[13:])),
	}

	// The copy path is intentional:
	// - Directly assigning the slice view keeps the underlying array alive, and risk unexpected alias problem
	// - This does incur an extra copy, but as if any performance optimization guide will say, only optimise when it is actually the problem
	copy(aer.Entries, bs[13:])

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

func (aer *AppendEntriesRes) ToBytes() []byte {
	out := make([]byte, 3)

	binary.BigEndian.PutUint16(out[0:2], aer.Term)
	out[2] = boolToByte(aer.Success)

	return out
}

func ToAppendEntriesRes(bs []byte) *AppendEntriesRes {
	return &AppendEntriesRes{
		Term:    binary.BigEndian.Uint16(bs[0:2]),
		Success: byteToBool(bs[2]),
	}
}

// byte layout:
//
//	Term -> 2 byte
//	CandidateId -> 1 byte
//	LastLogIndex -> 4 byte
//	LastLogTerm -> 2 byte
type RequestVoteReq struct {
	Term         uint16
	CandidateId  uint8
	LastLogIndex uint32
	LastLogTerm  uint16
}

func (rvr *RequestVoteReq) ToBytes() []byte {
	out := make([]byte, 9)

	binary.BigEndian.PutUint16(out[0:2], rvr.Term)
	out[2] = rvr.CandidateId
	binary.BigEndian.PutUint32(out[3:7], rvr.LastLogIndex)
	binary.BigEndian.PutUint16(out[7:9], rvr.LastLogTerm)

	return out
}

func ToRequestVoteReq(bs []byte) *RequestVoteReq {
	return &RequestVoteReq{
		Term:         binary.BigEndian.Uint16(bs[0:2]),
		CandidateId:  bs[2],
		LastLogIndex: binary.BigEndian.Uint32(bs[3:7]),
		LastLogTerm:  binary.BigEndian.Uint16(bs[7:9]),
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

func (rvr *RequestVoteRes) ToBytes() []byte {
	out := make([]byte, 3)

	binary.BigEndian.PutUint16(out[0:2], rvr.Term)
	out[2] = boolToByte(rvr.VoteGranted)

	return out
}

func ToRequestVoteRes(bs []byte) *RequestVoteRes {
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
