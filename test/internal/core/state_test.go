package core_test

import (
	"testing"
	"time"

	"com.lwc.raft/internal/core"
	"github.com/stretchr/testify/assert"
)

func TestStateUpdateRole(t *testing.T) {
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

func TestStateUpdateRoleFailCase(t *testing.T) {
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

func TestStateUpdateCommitIdx(t *testing.T) {
	assert := assert.New(t)
	rs := &core.RaftState{}

	rs.UpdateCommitIdx(3)
	assert.Equal(uint32(3), rs.CommitIdx(), "Commit idx mismatch after update")

	rs.UpdateCommitIdx(2)
	assert.Equal(uint32(3), rs.CommitIdx(), "Commit idx mismatch after supposedly failed update")
}

func TestStateStartElection(t *testing.T) {
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

func TestStateStartElectionFailCase(t *testing.T) {
	assert := assert.New(t)
	// default role is Follower
	rs := &core.RaftState{}

	err := rs.StartElection("test")
	assert.ErrorIs(core.RaftStateNotACandidateErr, err, "Error mismatched")
}

func TestStateStopElection(t *testing.T) {
	assert := assert.New(t)

	rs := &core.RaftState{}
	err := rs.StopElection()
	assert.ErrorIs(err, core.RaftStateNotACandidateErr, "Should return error when role is not candidate")

	ok := rs.UpdateRole(core.RAFT_ROLE_CANDIDATE)
	assert.True(ok, "Unexpected failed update to candidate")

	err = rs.StopElection()
	assert.NoError(err, "Unexpected error when stopping election")
	assert.False(rs.IsVoted(), "Stopping election should make node not voted")
}

func TestStateVote(t *testing.T) {
	assert := assert.New(t)
	// default role is Follower
	rs := &core.RaftState{}

	assert.False(rs.IsVoted(), "Before voting: Is voted shows voted")

	voteTerm := uint32(3)
	term, err := rs.Vote("test", voteTerm, 0, 0, core.AppendEntries{})
	assert.NoError(err, "Vote has error when running happy path")
	assert.Equal(voteTerm, term, "Returned Term after Vote is wrong")
	assert.Equal(voteTerm, rs.Term(), "State Term after Vote is wrong")
	assert.True(rs.IsVoted(), "After first vote: Is voted shows not voted")
	assert.Equal(core.RAFT_ROLE_FOLLOWER, rs.Role(), "After first vote: role is not follower")

	voteTerm++
	term, err = rs.Vote("hehe", voteTerm, 0, 0, core.AppendEntries{})
	assert.NoError(err, "Second Vote has error when running happy path")
	assert.Equal(voteTerm, term, "Returned Term after Second Vote is wrong")
	assert.Equal(voteTerm, rs.Term(), "State Term after Second Vote is wrong")
	assert.True(rs.IsVoted(), "After second vote: Is voted shows not voted")
	assert.Equal(core.RAFT_ROLE_FOLLOWER, rs.Role(), "After first vote: role is not follower")
}

func TestStateVoteFailCase(t *testing.T) {
	assert := assert.New(t)
	// default role is Follower
	rs := &core.RaftState{}

	voteTerm := uint32(3)
	_, err := rs.Vote("test", voteTerm, 1, 1, core.AppendEntries{{}, {
		Term: 2,
	}})
	assert.EqualError(err, "Candidate log term behind")

	_, err = rs.Vote("test", voteTerm, 1, 1, core.AppendEntries{
		{},
		{
			Term: 1,
		},
		{
			Term: 1,
		},
	})
	assert.EqualError(err, "Candidate log index behind")

	_, err = rs.Vote("test", voteTerm, 0, 0, core.AppendEntries{})
	assert.NoError(err, "Unexpected error during first voting")

	_, err = rs.Vote("hehe", voteTerm, 0, 0, core.AppendEntries{})
	assert.EqualError(err, "Voted already")

	voteTerm--
	_, err = rs.Vote("hoho", voteTerm, 0, 0, core.AppendEntries{})
	assert.ErrorIs(core.RaftStateCurrentTermHigherErr, err, "Current term higher error mismatched")
}

func TestStateGotVote(t *testing.T) {
	assert := assert.New(t)
	// default role is Follower
	rs := &core.RaftState{}
	ok := rs.UpdateRole(core.RAFT_ROLE_CANDIDATE)
	assert.True(ok, "Unexpected error for updating raft role")

	// well, leader is indeed the one and only kcw
	rs.RegisterNode("kcw")
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

func TestStateGotVoteFailCase(t *testing.T) {
	assert := assert.New(t)
	// default role is Follower
	rs := &core.RaftState{}

	ok := rs.UpdateRole(core.RAFT_ROLE_CANDIDATE)
	assert.True(ok, "Unexpected error for updating raft role")

	// well, leader is indeed the one and only kcw
	rs.RegisterNode("kcw")
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

func TestStateIsVoted(t *testing.T) {
	assert := assert.New(t)
	rs := &core.RaftState{}

	// votedFor == "", term and votedTerm are the same
	assert.False(rs.IsVoted(), "Initial voted should be false")

	voteTerm := uint32(3)
	term, err := rs.Vote("test", voteTerm, 0, 0, core.AppendEntries{})
	assert.NoError(err, "Unexpected error during voting")
	assert.Equal(voteTerm, term, "Unexpected mismatch between vote term and returned term")
	assert.Equal(voteTerm, rs.Term(), "Unexpected mismatch between vote term and updated term")
	assert.True(rs.IsVoted(), "IsVoted should be true")

	rs.RestoreTerm(4)
	// votedFor != "", term and votedTerm diverge
	assert.False(rs.IsVoted(), "Term advanced, IsVoted should be false")
}

func TestStateGotAEReq(t *testing.T) {
	assert := assert.New(t)
	// default role is Follower
	rs := &core.RaftState{}

	leaderId := "kcw"
	term := uint32(1)
	err := rs.GotAEReq(leaderId, term)
	assert.NoError(err, "First AE: unexpected error")
	assert.Equal(term, rs.Term(), "First AE: Term not updated")
	assert.Equal(leaderId, rs.LeaderId(), "First AE: Leader ID not updated")

	err = rs.GotAEReq(leaderId, term-1)
	assert.ErrorIs(err, core.RaftStateCurrentTermHigherErr, "Second AE: Term higher error is not returned")
	assert.Equal(term, rs.Term(), "Second AE: Term should not be updated ")

	secondRs := &core.RaftState{}
	ok := secondRs.UpdateRole(core.RAFT_ROLE_CANDIDATE)
	assert.True(ok, "Unexpected false from promoting to candidate")

	secondTerm := uint32(2)
	secondRs.GotAEReq(leaderId, secondTerm)
	assert.Equal(secondTerm, secondRs.Term(), "Third AE: Term did not get updated")
	assert.Equal(core.RAFT_ROLE_FOLLOWER, secondRs.Role(), "Third AE: Not demoted to follower")
	assert.Equal(leaderId, rs.LeaderId(), "Third AE: Leader ID not updated")
}

func TestStateGotAERes(t *testing.T) {
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
	rs.RegisterNode("kcw")
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

func TestStateGotAEResTermRejection(t *testing.T) {
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
	rs.RegisterNode("kcw")
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

func TestStateIncLeaderIndexes(t *testing.T) {
	assert := assert.New(t)
	rs := &core.RaftState{}

	leaderId := "kcw"
	err := rs.IncLeaderIndexes(leaderId)
	assert.ErrorIs(err, core.RaftStateNotALeaderErr, "Should return not leader error when not a leader")

	ok := rs.UpdateRole(core.RAFT_ROLE_CANDIDATE)
	assert.True(ok, "Unexpected false when updating to candidate")

	ok = rs.UpdateRole(core.RAFT_ROLE_LEADER)
	assert.True(ok, "Unexpected false when updating to leader")

	err = rs.IncLeaderIndexes(leaderId)
	assert.NoError(err, "Should not have error")
	assert.Equal(uint32(1), rs.NextIndex(leaderId), "Next Index mismatch")
	assert.Equal(uint32(1), rs.MatchIndex(leaderId), "Match Index mismatch")
}

func TestRestoreTerm(t *testing.T) {
	assert := assert.New(t)
	rs := &core.RaftState{}

	term := uint32(3)
	rs.RestoreTerm(term)
	assert.Equal(term, rs.Term(), "Term not restored")

	term++
	rs.RestoreTerm(term)
	assert.Equal(term-1, rs.Term(), "Term restored for the second time")
}

func TestStateRegisterNode(t *testing.T) {
	assert := assert.New(t)
	rs := &core.RaftState{}

	id := "kcw"
	rs.RegisterNode(id)
	assert.Equal(uint32(0), rs.NextIndex(id), "Quorum count 1: Next Index mismatch")
	assert.Equal(uint32(0), rs.MatchIndex(id), "Quorum count 1: Match Index mismatch")
	assert.Equal(uint32(1), rs.Threshold(), "Quorum count 1: Threshold mismatch")

	id = "hec"
	rs.RegisterNode(id)
	assert.Equal(uint32(0), rs.NextIndex(id), "Quorum count 2: Next Index mismatch")
	assert.Equal(uint32(0), rs.MatchIndex(id), "Quorum count 2: Match Index mismatch")
	assert.Equal(uint32(1), rs.Threshold(), "Quorum count 2: Threshold mismatch")

	id = "hyj"
	rs.RegisterNode(id)
	assert.Equal(uint32(0), rs.NextIndex(id), "Quorum count 3: Next Index mismatch")
	assert.Equal(uint32(0), rs.MatchIndex(id), "Quorum count 3: Match Index mismatch")
	assert.Equal(uint32(2), rs.Threshold(), "Quorum count 3: Threshold mismatch")
}

func TestStateResetIndexes(t *testing.T) {
	assert := assert.New(t)
	rs := &core.RaftState{}

	ids := []string{
		"kcw",
		"sakura",
		"hyj",
		"kazuha",
		"hec",
	}

	for _, id := range ids {
		rs.RegisterNode(id)
	}

	idx := uint32(3)
	rs.ResetIndexes(idx)

	for _, id := range ids {
		assert.Equalf(idx, rs.NextIndex(id), "%s: Next Index mismatch", id)
		assert.Equalf(uint32(0), rs.MatchIndex(id), "%s: Match Index mismatch", id)
	}
}
