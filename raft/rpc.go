package raft

import (
	"context"
	"raft-kv/bitcask"
	"raft-kv/proto"
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

func (rf *Raft) Apply(args *proto.ApplyArgs) (p *proto.ApplyReply, ok bool) {
	command := bitcask.Command{
		Op:    bitcask.Op(args.Command.Op),
		Key:   []byte(args.Command.Key),
		Value: []byte(args.Command.Value),
	}
	index, term, success := rf.Start(&command)
	return &proto.ApplyReply{
		VirtualIndex: index,
		Term:         term,
		Success:      success,
	}, success

}

// InstallSnapshot handler
func (rf *Raft) InstallSnapshot(args *proto.InstallSnapshotArgs, reply *proto.InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	rf.role = Follower
	rf.resetVoteTimer()

	if args.LastIncludedIndex < rf.lastIncludedIndex || args.LastIncludedIndex < rf.commitIndex {
		reply.Term = rf.currentTerm
		return
	}

	hasEntry := false
	rIdx := 0
	for ; rIdx < len(rf.log); rIdx++ {
		if rf.virtualLogIdx(int64(rIdx)) == args.LastIncludedIndex && rf.log[rIdx].Term == args.LastIncludedTerm {
			hasEntry = true
			break
		}
	}

	msg := &ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}

	if hasEntry {
		rf.log = rf.log[rIdx:]
	} else {
		rf.log = make([]*proto.Entry, 0)
		rf.log = append(rf.log, &proto.Entry{Term: rf.lastIncludedTerm, Cmd: args.LastIncludedCmd}) // 索引为0处占位
	}

	rf.snapShot = args.Data
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm

	if rf.commitIndex < args.LastIncludedIndex {
		rf.commitIndex = args.LastIncludedIndex
	}

	if rf.lastApplied < args.LastIncludedIndex {
		rf.lastApplied = args.LastIncludedIndex
	}

	reply.Term = rf.currentTerm
	rf.applyCh <- *msg
	rf.persist()
}

// AppendEntries handler
func (rf *Raft) AppendEntries(args *proto.AppendEntriesArgs, reply *proto.AppendEntriesReply) {
	rf.mu.Lock()
	defer func() {
		rf.mu.Unlock()
	}()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	rf.resetVoteTimer()

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term // 更新iterm
		rf.votedFor = -1           // 更新投票记录为未投票
		rf.role = Follower
		rf.persist()
	}

	isConflict := false

	if args.PrevLogIndex < rf.lastIncludedIndex {
		reply.Success = true
		reply.Term = rf.currentTerm
		return
	} else if args.PrevLogIndex >= rf.virtualLogIdx(int64(len(rf.log))) {
		reply.XTerm = -1
		reply.XLen = rf.virtualLogIdx(int64(len(rf.log))) // Log长度, 包括了已经snapShot的部分
		isConflict = true
	} else if rf.log[rf.realLogIdx(args.PrevLogIndex)].Term != args.PrevLogTerm {
		reply.XTerm = rf.log[rf.realLogIdx(args.PrevLogIndex)].Term
		i := args.PrevLogIndex
		for i > rf.commitIndex && rf.log[rf.realLogIdx(i)].Term == reply.XTerm {
			i -= 1
		}
		reply.XIndex = i + 1
		reply.XLen = rf.virtualLogIdx(int64(len(rf.log))) // Log长度, 包括了已经snapShot的部分
		isConflict = true
	}

	if isConflict {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	for idx, log := range args.Entries {
		ridx := int(rf.realLogIdx(args.PrevLogIndex)) + 1 + idx
		if ridx < len(rf.log) && rf.log[ridx].Term != log.Term {
			rf.log = rf.log[:ridx]
			rf.log = append(rf.log, args.Entries[idx:]...)
			break
		} else if ridx == len(rf.log) {
			rf.log = append(rf.log, args.Entries[idx:]...)
			break
		}
	}

	rf.persist()

	reply.Success = true
	reply.Term = rf.currentTerm

	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit > rf.virtualLogIdx(int64(len(rf.log)-1)) {
			rf.commitIndex = rf.virtualLogIdx(int64(len(rf.log) - 1))
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		rf.condApply.Signal() // 唤醒检查commit的协程
	}
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *proto.RequestVoteArgs, reply *proto.RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()
	rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term // 更新到更新的term
		rf.votedFor = -1
		rf.role = Follower
		rf.persist()
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		if args.LastLogTerm > rf.log[len(rf.log)-1].Term ||
			(args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= rf.virtualLogIdx(int64(len(rf.log)-1))) {
			rf.currentTerm = args.Term
			reply.Term = rf.currentTerm
			rf.votedFor = args.CandidateId
			rf.role = Follower
			rf.resetVoteTimer()
			rf.persist()

			reply.VoteGranted = true
			return
		}
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false
}
