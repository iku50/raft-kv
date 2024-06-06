package rpc

import (
	"context"
	"raft-kv/proto"
	"raft-kv/raft"
)

type RpcRaft struct {
	rf *raft.Raft
	proto.UnimplementedRaftServer
}

func (rr *RpcRaft) RequestVote(ctx context.Context, p *proto.RequestVoteArgs) (*proto.RequestVoteReply, error) {
	args := raft.RequestVoteArgs{
		Term:         int(p.Term),
		CandidateId:  int(p.CandidateId),
		LastLogIndex: int(p.LastLogIndex),
		LastLogTerm:  int(p.LastLogTerm),
	}
	reply := raft.RequestVoteReply{}
	rr.rf.RequestVote(&args, &reply)
	return &proto.RequestVoteReply{
		Term:        int32(reply.Term),
		VoteGranted: reply.VoteGranted,
	}, nil
}
func (rr *RpcRaft) AppendEntries(ctx context.Context, p *proto.AppendEntriesArgs) (*proto.AppendEntriesReply, error) {
	args := raft.AppendEntriesArgs{
		Term:         int(p.Term),
		LeaderId:     int(p.LeaderId),
		PrevLogIndex: int(p.PrevLogIndex),
		PrevLogTerm:  int(p.PrevLogTerm),
		Entries:      nil,
		LeaderCommit: int(p.LeaderCommit),
	}
	args.Entries = make([]raft.Entry, len(p.Entries))
	for i, e := range p.Entries {
		args.Entries[i] = raft.Entry{
			Term: int(e.Term),
			Cmd:  raft.CommandFromBytes(e.Cmd),
		}
	}
	reply := raft.AppendEntriesReply{}
	rr.rf.AppendEntries(&args, &reply)
	return &proto.AppendEntriesReply{
		Term:    int32(reply.Term),
		Success: reply.Success,
		XTerm:   int32(reply.XTerm),
		XIndex:  int64(reply.XIndex),
		XLen:    int64(reply.XLen),
	}, nil
}
func (rr *RpcRaft) InstallSnapshot(ctx context.Context, p *proto.InstallSnapshotArgs) (*proto.InstallSnapshotReply, error) {
	args := raft.InstallSnapshotArgs{
		Term:              int(p.Term),
		LeaderId:          int(p.LeaderId),
		LastIncludedIndex: int(p.LastIncludedIndex),
		LastIncludedTerm:  int(p.LastIncludedTerm),
		Data:              p.Data,
		LastIncludedCmd:   raft.CommandFromBytes(p.LastIncludedCmd),
	}
	reply := raft.InstallSnapshotReply{}
	rr.rf.InstallSnapshot(&args, &reply)
	return &proto.InstallSnapshotReply{
		Term: int32(reply.Term),
	}, nil
}
