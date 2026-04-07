package core_test

import (
	"testing"
	"time"

	"com.lwc.raft/internal/core"
	"github.com/stretchr/testify/assert"
)

func TestUpdateRole(t *testing.T) {
	assert := assert.New(t)
	rs := &core.RaftState{}

	roleChanges := []core.RaftRole{
		core.RAFT_ROLE_CANDIDATE,
		core.RAFT_ROLE_LEADER,
		core.RAFT_ROLE_FOLLOWER,
	}

	for _, v := range roleChanges {
		ok := rs.UpdateRole(v)
		assert.Truef(ok, "%d update failed", v)
		select {
		case event := <-rs.EventCh():
			assert.Equal(core.RAFT_STATE_EVENT_ROLE_CHANGE, event)
		case <-time.After(2 * time.Second):
			t.Fatal("Event ch is not sending event")
		}
	}

	ok := rs.UpdateRole(core.RAFT_ROLE_FOLLOWER)
	assert.True(ok, "Same role update failed")
	assert.Equal(0, len(rs.EventCh()), "Event ch is not empty")
}

func TestUpdateRoleFailCase(t *testing.T) {
	assert := assert.New(t)
	// default role is Follower
	rs := &core.RaftState{}

	ok := rs.UpdateRole(core.RAFT_ROLE_LEADER)
	assert.False(ok, "Follower can't be promote to leader directly")

	// update role to leader
	ok = rs.UpdateRole(core.RAFT_ROLE_CANDIDATE)
	assert.True(ok, "Unexpected role update fail")
	ok = rs.UpdateRole(core.RAFT_ROLE_LEADER)
	assert.True(ok, "Unexpected role update fail")

	// check leader to candidate path
	ok = rs.UpdateRole(core.RAFT_ROLE_CANDIDATE)
	assert.False(ok, "Only follower can be promote to candidate")
}

func TestStartElection(t *testing.T) {
	assert := assert.New(t)
	// default role is Follower
	rs := &core.RaftState{}

	ok := rs.UpdateRole(core.RAFT_ROLE_CANDIDATE)
	assert.True(ok, "Unexpected role update fail")

	err := rs.StartElection("test")
	assert.NoError(err, "Start election has error when running happy path")
	assert.Equal(uint32(1), rs.Term(), "Term is not incremented, or does not match")
	assert.True(rs.IsVoted(), "Self vote failed")
}

func TestStartElectionFailCase(t *testing.T) {
	assert := assert.New(t)
	// default role is Follower
	rs := &core.RaftState{}

	err := rs.StartElection("test")
	assert.EqualError(err, "Not a candidate")
}

func TestVote(t *testing.T) {
	assert := assert.New(t)
	// default role is Follower
	rs := &core.RaftState{}

	assert.False(rs.IsVoted(), "Before voting: Is voted shows voted")

	voteTerm := uint32(3)
	term, err := rs.Vote("test", voteTerm)
	assert.NoError(err, "Vote has error when running happy path")
	assert.Equal(voteTerm, term, "Returned Term after Vote is wrong")
	assert.Equal(voteTerm, rs.Term(), "State Term after Vote is wrong")
	assert.True(rs.IsVoted(), "After first vote: Is voted shows not voted")

	voteTerm++
	term, err = rs.Vote("hehe", voteTerm)
	assert.NoError(err, "Second Vote has error when running happy path")
	assert.Equal(voteTerm, term, "Returned Term after Second Vote is wrong")
	assert.Equal(voteTerm, rs.Term(), "State Term after Second Vote is wrong")
	assert.True(rs.IsVoted(), "After second vote: Is voted shows not voted")
}

func TestVoteFailCase(t *testing.T) {
	assert := assert.New(t)
	// default role is Follower
	rs := &core.RaftState{}

	voteTerm := uint32(3)
	_, err := rs.Vote("test", voteTerm)
	assert.NoError(err, "Unexpected error during first voting")

	_, err = rs.Vote("hehe", voteTerm)
	assert.EqualError(err, "Voted already")

	voteTerm--
	_, err = rs.Vote("hoho", voteTerm)
	assert.EqualError(err, "Current term is higher")
}

func TestGotVote(t *testing.T) {
	assert := assert.New(t)
	// default role is Follower
	rs := &core.RaftState{}
	ok := rs.UpdateRole(core.RAFT_ROLE_CANDIDATE)
	assert.True(ok, "Unexpected error for updating raft role")

	// well, leader is indeed the one and only kcw
	rs.StartElection("kcw")
	rs.RegisterNode("sakura")
	rs.RegisterNode("hyj")
	rs.RegisterNode("kazuha")
	rs.RegisterNode("hec")

	followerTerm := uint32(0)

	// for a 5 node quorum, 2 votes received (+ self vote)
	rs.GotVote(true, followerTerm)
	assert.Equal(core.RAFT_ROLE_CANDIDATE, rs.Role(), "First granted vote: should be candidate")
	rs.GotVote(false, followerTerm)
	assert.Equal(core.RAFT_ROLE_CANDIDATE, rs.Role(), "First not granted vote: should be candidate")
	rs.GotVote(false, followerTerm)
	assert.Equal(core.RAFT_ROLE_CANDIDATE, rs.Role(), "Second not granted vote: should be candidate")
	rs.GotVote(true, followerTerm)
	assert.Equal(core.RAFT_ROLE_LEADER, rs.Role(), "Second granted vote: should be leader")
	assert.Equal(uint32(1), rs.Term(), "Term is incorrect")

	// The first event for promoting to candidate, and the second event for promoting to leader
	assert.Equal(2, len(rs.EventCh()), "Too many/little event")
}

func TestGotVoteFailCase(t *testing.T) {
	assert := assert.New(t)
	// default role is Follower
	rs := &core.RaftState{}

	ok := rs.UpdateRole(core.RAFT_ROLE_CANDIDATE)
	assert.True(ok, "Unexpected error for updating raft role")

	// well, leader is indeed the one and only kcw
	rs.StartElection("kcw")
	rs.RegisterNode("sakura")
	rs.RegisterNode("hyj")
	rs.RegisterNode("kazuha")
	rs.RegisterNode("hec")

	followerTerm := uint32(2)

	// for a 5 node quorum, 2 votes received (+ self vote)
	rs.GotVote(false, followerTerm)
	assert.Equal(core.RAFT_ROLE_FOLLOWER, rs.Role(), "Should be demoted to follower")

	// The first event for promoting to candidate, and the second event for demoting to follower
	assert.Equal(2, len(rs.EventCh()), "Too many/little event")
}

func TestGotAEReq(t *testing.T) {
	assert := assert.New(t)
	// default role is Follower
	rs := &core.RaftState{}

	leaderId := "kcw"
	term := uint32(1)
	newCommitIdx := uint32(3)
	lastLogIndex := uint32(4)
	rs.GotAEReq(leaderId, term, newCommitIdx, lastLogIndex)
	assert.Equal(term, rs.Term(), "First AE: Term not updated")
	assert.Equal(newCommitIdx, rs.CommitIdx(), "First AE: Commit idx is wrong")

	newCommitIdx++
	rs.GotAEReq(leaderId, term, newCommitIdx, lastLogIndex)
	assert.Equal(newCommitIdx, rs.CommitIdx(), "Second AE: Commit idx is wrong")

	newCommitIdx++
	rs.GotAEReq(leaderId, term, newCommitIdx, lastLogIndex)
	assert.Equal(lastLogIndex, rs.CommitIdx(), "Third AE: Commit idx is wrong")

	smallCommitIdx := lastLogIndex - 1
	rs.GotAEReq(leaderId, term, smallCommitIdx, smallCommitIdx)
	assert.Equal(lastLogIndex, rs.CommitIdx(), "Forth AE: Commit idx is wrong")

	secondRs := &core.RaftState{}
	ok := secondRs.UpdateRole(core.RAFT_ROLE_CANDIDATE)
	assert.True(ok, "Unexpected false from promoting to candidate")

	secondTerm := uint32(2)
	secondRs.GotAEReq(leaderId, secondTerm, 1, 1)
	assert.Equal(secondTerm, secondRs.Term(), "Term did not get updated")
	assert.Equal(core.RAFT_ROLE_FOLLOWER, secondRs.Role(), "Not demoted to follower")
}

func TestGotAERes(t *testing.T) {
	assert := assert.New(t)
	ae := core.AppendEntries{
		{},
		{
			Term: 1,
		},
		{
			Term: 1,
		},
		{
			Term: 1,
		},
		{
			Term: 1,
		},
		{
			Term: 1,
		},
		{
			Term: 1,
		},
	}
	// default role is Follower
	rs := &core.RaftState{}

	ok := rs.UpdateRole(core.RAFT_ROLE_CANDIDATE)
	assert.True(ok, "Unexpected error for updating raft role")
	select {
	case <-rs.EventCh():
	case <-time.After(2 * time.Second):
		t.Fatal("Unexpected no event")
	}

	// well, leader is indeed the one and only kcw
	rs.StartElection("kcw")
	rs.RegisterNode("sakura")
	rs.RegisterNode("hyj")
	rs.RegisterNode("kazuha")
	rs.RegisterNode("hec")

	ok = rs.UpdateRole(core.RAFT_ROLE_LEADER)
	assert.True(ok, "Unexpected error for updating raft role")
	select {
	case <-rs.EventCh():
	case <-time.After(2 * time.Second):
		t.Fatal("Unexpected no event")
	}

	// Init:
	// - Set all follower's next index to be last log index + 1, in this case 7
	// - Set all follower's match index to be 0
	// - Set leader's match index to be last log index
	err := rs.InitAsLeader("kcw", 6)
	assert.NoError(err, "Unexpected error for init as leader")

	// First AE success:
	// - Term check should pass (same term)
	// - Sakura's next index should be 2, she matches the log on index 1, that means the next log index the system should send should be 2 (match index +1)
	// - Commit index stays untouched, no majority, or i should say 0 is still the majority (match index: 1 for sakura, 6 for leader, the rest is 0)
	rs.GotAERes("sakura", true, rs.Term(), 1, ae)
	assert.Equal(uint32(2), rs.NextIndex("sakura"), "sakura first next index incorrect")
	assert.Equal(uint32(0), rs.CommitIdx(), "Commit index got updated unexpectedly after sakura update")

	// Second AE success:
	// - Term check should pass (same term)
	// - hyj's next index should be 6, she matches the log on index 6, that means the next log index the system should send should be 7 (match index +1)
	// - Commit index changed, majority established: 1 (match index: 1 for sakura, 6 for leader, 6 for hyj,  the rest is 0)
	rs.GotAERes("hyj", true, rs.Term(), 6, ae)
	assert.Equal(uint32(7), rs.NextIndex("hyj"), "hyj first next index incorrect")
	assert.Equal(uint32(1), rs.CommitIdx(), "First commit index update incorrect")
	select {
	case event := <-rs.EventCh():
		assert.Equal(core.RAFT_STATE_EVENT_COMMIT_IDX_CHANGE, event, "second commit idx event not received")
	case <-time.After(2 * time.Second):
		t.Fatal("Unexpected no commit idx event")
	}

	// kazuha AE failure:
	// - Term check should pass (same term)
	// - kazyha's next index should be 6, init as 7, failure will cause next index to --
	// - Commit index stays untouched, match indexes did not change
	rs.GotAERes("kazuha", false, rs.Term(), 1, ae)
	assert.Equal(uint32(6), rs.NextIndex("kazuha"), "kazuha first next index incorrect")
	assert.Equal(uint32(1), rs.CommitIdx(), "Commit index got updated unexpectedly after kazuha update")

	// Third AE success:
	// - Term check should pass (same term)
	// - hec's next index should be 6, she matches the log on index 6, that means the next log index the system should send should be 7 (match index +1)
	// - Commit index changed, majority established again: 6 (match index: 1 for sakura, 6 for leader, 6 for hyj, 6 for hec, kazuha is 0)
	rs.GotAERes("hec", true, rs.Term(), 6, ae)
	assert.Equal(uint32(7), rs.NextIndex("hec"), "hec first next index incorrect")
	assert.Equal(uint32(6), rs.CommitIdx(), "Second commit index update incorrect")
	select {
	case event := <-rs.EventCh():
		assert.Equal(core.RAFT_STATE_EVENT_COMMIT_IDX_CHANGE, event, "second commit idx event not received")
	case <-time.After(2 * time.Second):
		t.Fatal("Unexpected no commit idx event")
	}

	// Term change
	rs.GotAERes("kazuha", true, rs.Term()+1, 6, ae)
	assert.Equal(core.RAFT_ROLE_FOLLOWER, rs.Role(), "Should get demoted to follower")
	select {
	case event := <-rs.EventCh():
		assert.Equal(core.RAFT_STATE_EVENT_ROLE_CHANGE, event, "event role change event did not arrive")
	case <-time.After(2 * time.Second):
		t.Fatal("Unexpected no commit idx event")
	}
}

func TestGotAEResTermRejection(t *testing.T) {
	assert := assert.New(t)
	ae := core.AppendEntries{
		{},
		{
			Term: 1,
		},
		{
			Term: 3,
		},
	}
	// default role is Follower
	rs := &core.RaftState{}
	// start election will ++
	rs.RestoreTerm(2)
	assert.Equal(uint32(2), rs.Term(), "Term did not get restored")

	ok := rs.UpdateRole(core.RAFT_ROLE_CANDIDATE)
	assert.True(ok, "Unexpected error for updating raft role")
	select {
	case <-rs.EventCh():
	case <-time.After(2 * time.Second):
		t.Fatal("Unexpected no event")
	}

	// well, leader is indeed the one and only kcw
	rs.StartElection("kcw")
	rs.RegisterNode("sakura")
	rs.RegisterNode("hyj")
	rs.RegisterNode("kazuha")
	rs.RegisterNode("hec")

	ok = rs.UpdateRole(core.RAFT_ROLE_LEADER)
	assert.True(ok, "Unexpected error for updating raft role")
	select {
	case <-rs.EventCh():
	case <-time.After(2 * time.Second):
		t.Fatal("Unexpected no event")
	}

	// Init:
	// - Set all follower's next index to be last log index + 1, in this case 3
	// - Set all follower's match index to be 0
	// - Set leader's match index to be last log index
	err := rs.InitAsLeader("kcw", 2)
	assert.NoError(err, "Unexpected error for init as leader")

	// First AE success:
	// - Term check should pass (same term)
	// - Sakura's next index should be 2, she matches the log on index 1, that means the next log index the system should send should be 2 (match index +1)
	// - Commit index stays untouched, no majority, or i should say 0 is still the majority (match index: 1 for sakura, 2 for leader, the rest is 0)
	rs.GotAERes("sakura", true, rs.Term(), 1, ae)
	assert.Equal(uint32(2), rs.NextIndex("sakura"), "sakura first next index incorrect")
	assert.Equal(uint32(0), rs.CommitIdx(), "Commit index got updated unexpectedly after sakura update")

	// Second AE success:
	// - Term check should pass (same term)
	// - hyj's next index should be 2, she matches the log on index 1, that means the next log index the system should send should be 2 (match index +1)
	// - Commit index should not change, although there are majority established: 1 (match index: 1 for sakura, 1 for hyj, 2 for leader, the rest is 0)
	// - Log at index 1 has a smaller term, Raft specifies only current term logs can be committed, so no.
	rs.GotAERes("hyj", true, rs.Term(), 1, ae)
	assert.Equal(uint32(2), rs.NextIndex("hyj"), "hyj first next index incorrect")
	assert.Equal(uint32(0), rs.CommitIdx(), "Commit index got updated unexpectedly after hyj update")

	// Third AE success:
	// - Term check should pass (same term)
	// - kazuha's next index should be 3, she matches the log on index 2, that means the next log index the system should send should be 3 (match index +1)
	// - Commit index should not change, although there are majority established: 1 (match index: 1 for sakura, 1 for hyj, 2 for kazuha, 2 for leader, hec is 0)
	// - Log at index 1 has a smaller term, Raft specifies only current term logs can be committed, so no.
	rs.GotAERes("kazuha", true, rs.Term(), 2, ae)
	assert.Equal(uint32(3), rs.NextIndex("kazuha"), "kazuha first next index incorrect")
	assert.Equal(uint32(0), rs.CommitIdx(), "Commit index got updated unexpectedly after kazuha update")

	// Forth AE success:
	// - Term check should pass (same term)
	// - hec's next index should be 3, she matches the log on index 2, that means the next log index the system should send should be 3 (match index +1)
	// - Commit index changed, majority established: 2 (match index: 1 for sakura, 1 for hyj, 2 for the rest)
	rs.GotAERes("hec", true, rs.Term(), 2, ae)
	assert.Equal(uint32(3), rs.NextIndex("hec"), "hec first next index incorrect")
	assert.Equal(uint32(2), rs.CommitIdx(), "Commit index update incorrect")
	select {
	case event := <-rs.EventCh():
		assert.Equal(core.RAFT_STATE_EVENT_COMMIT_IDX_CHANGE, event, "second commit idx event not received")
	case <-time.After(2 * time.Second):
		t.Fatal("Unexpected no commit idx event")
	}

}
