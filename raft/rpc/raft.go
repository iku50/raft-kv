package rpc

import (
	"context"
	"log"
	"net"
	"raft-kv/proto"
	"raft-kv/raft"
	"strconv"

	"google.golang.org/grpc"
)

type RpcRaft struct {
	rf *raft.Raft
	proto.UnimplementedRaftServer
	proto.UnimplementedApplyServer
}

func (rr *RpcRaft) RequestVote(ctx context.Context, p *proto.RequestVoteArgs) (*proto.RequestVoteReply, error) {
	reply := proto.RequestVoteReply{}
	rr.rf.RequestVote(p, &reply)
	return &reply, nil
}
func (rr *RpcRaft) AppendEntries(ctx context.Context, p *proto.AppendEntriesArgs) (*proto.AppendEntriesReply, error) {
	reply := proto.AppendEntriesReply{}
	rr.rf.AppendEntries(p, &reply)
	return &reply, nil
}
func (rr *RpcRaft) InstallSnapshot(ctx context.Context, p *proto.InstallSnapshotArgs) (*proto.InstallSnapshotReply, error) {
	reply := proto.InstallSnapshotReply{}
	rr.rf.InstallSnapshot(p, &reply)
	return &reply, nil
}
func (rr *RpcRaft) Apply(ctx context.Context, p *proto.ApplyArgs) (*proto.ApplyReply, error) {
	reply, _ := rr.rf.Apply(p)
	return reply, nil
}

func Start(port int, rf *raft.Raft, killCh chan bool, okCh chan bool) {
	lis, err := net.Listen("tcp", ":"+strconv.Itoa(port))
	if err != nil {
		panic(err)
	}
	server := grpc.NewServer()
	proto.RegisterRaftServer(server, &RpcRaft{rf: rf})
	proto.RegisterApplyServer(server, &RpcRaft{rf: rf})
	log.Printf("Raft server started on port %d", port)
	okCh <- true
	if err := server.Serve(lis); err != nil {
		return
	}
	<-killCh
	server.GracefulStop()
}
