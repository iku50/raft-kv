package facade

import (
	"context"
	"raft-kv/proto"
	"raft-kv/raft"
)

// func (c *ClientEnd) Call(method string, args interface{}, reply interface{}) bool {
// 	return false
// }

type ClientEnd struct {
	rc proto.RaftClient
}

func (c *ClientEnd) RequestVote(args *raft.RequestVoteArgs, reply *raft.RequestVoteReply) bool {
	a := &proto.RequestVoteArgs{
		Term:         int32(args.Term),
		CandidateId:  int32(args.CandidateId),
		LastLogIndex: int64(args.LastLogIndex),
		LastLogTerm:  int32(args.LastLogTerm),
	}
	vote, err := c.rc.RequestVote(context.Background(), a)
	if err != nil {
		return false
	}
	reply.Term = int(vote.GetTerm())
	reply.VoteGranted = vote.GetVoteGranted()
	return true
}
func (c *ClientEnd) AppendEntries(args *raft.AppendEntriesArgs, reply *raft.AppendEntriesReply) bool {
	a := &proto.AppendEntriesArgs{
		Term:         int32(args.Term),
		LeaderId:     int32(args.LeaderId),
		PrevLogIndex: int64(args.PrevLogIndex),
		PrevLogTerm:  int32(args.PrevLogTerm),
		Entries:      nil,
		LeaderCommit: int64(args.LeaderCommit),
	}
	for _, e := range args.Entries {
		a.Entries = append(a.Entries, &proto.Entry{
			Term: int32(e.Term),
			Cmd:  e.Cmd.ToBytes(),
		})
	}
	ae, err := c.rc.AppendEntries(context.Background(), a)
	if err != nil {
		return false
	}
	reply.Term = int(ae.GetTerm())
	reply.Success = ae.GetSuccess()
	reply.XTerm = int(ae.GetXTerm())
	reply.XIndex = int(ae.GetXIndex())
	reply.XLen = int(ae.GetXLen())
	return true
}
func (c *ClientEnd) InstallSnapshot(args *raft.InstallSnapshotArgs, reply *raft.InstallSnapshotReply) bool {
	a := &proto.InstallSnapshotArgs{
		Term:              int32(args.Term),
		LeaderId:          int32(args.LeaderId),
		LastIncludedIndex: int64(args.LastIncludedIndex),
		LastIncludedTerm:  int32(args.LastIncludedTerm),
		Data:              args.Data,
		LastIncludedCmd:   args.LastIncludedCmd.ToBytes(),
	}
	is, err := c.rc.InstallSnapshot(context.Background(), a)
	if err != nil {
		return false
	}
	reply.Term = int(is.GetTerm())
	return true
}
