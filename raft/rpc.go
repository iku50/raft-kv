package raft

import (
	"context"
	"raft-kv/raft/proto"
)

func (rf *Raft) sendRequestVote(server int, args *proto.RequestVoteArgs) (reply *proto.RequestVoteReply, ok bool) {
	reply, err := rf.peers[server].RequestVote(context.Background(), args)
	return reply, err == nil
}

func (rf *Raft) sendInstallSnapshot(serverTo int, args *proto.InstallSnapshotArgs) (reply *proto.InstallSnapshotReply, ok bool) {
	reply, err := rf.peers[serverTo].InstallSnapshot(context.Background(), args)
	return reply, err == nil
}

func (rf *Raft) sendAppendEntries(serverTo int, args *proto.AppendEntriesArgs) (reply *proto.AppendEntriesReply, ok bool) {
	reply, err := rf.peers[serverTo].AppendEntries(context.Background(), args)
	return reply, err == nil
}
