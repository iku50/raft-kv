package raft

import (
	"context"
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

// InstallSnapshot handler
func (rf *Raft) InstallSnapshot(args *proto.InstallSnapshotArgs, reply *proto.InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		DPrintf("server %v 拒绝来自 %v 的 InstallSnapshot, 更小的Term\n", rf.me, args.LeaderId)

		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		DPrintf("server %v 接受来自 %v 的 InstallSnapshot, 且发现了更大的Term\n", rf.me, args.LeaderId)
	}

	rf.role = Follower
	rf.ResetVoteTimer()
	DPrintf("server %v 接收到 leader %v 的InstallSnapshot, 重设定时器", rf.me, args.LeaderId)

	if args.LastIncludedIndex < rf.lastIncludedIndex || args.LastIncludedIndex < rf.commitIndex {
		reply.Term = rf.currentTerm
		return
	}

	hasEntry := false
	rIdx := 0
	for ; rIdx < len(rf.log); rIdx++ {
		if rf.VirtualLogIdx(int64(rIdx)) == args.LastIncludedIndex && rf.log[rIdx].Term == args.LastIncludedTerm {
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
		DPrintf("server %v InstallSnapshot: args.LastIncludedIndex= %v 位置存在, 保留后面的log\n", rf.me, args.LastIncludedIndex)

		rf.log = rf.log[rIdx:]
	} else {
		DPrintf("server %v InstallSnapshot: 清空log\n", rf.me)
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

func (rf *Raft) sendAppendEntries(serverTo int, args *proto.AppendEntriesArgs) (reply *proto.AppendEntriesReply, ok bool) {
	reply, err := rf.peers[serverTo].AppendEntries(context.Background(), args)
	return reply, err == nil
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
		DPrintf("server %v 收到了旧的leader% v 的心跳函数, args=%+v, 更新的term: %v\n", rf.me, args.LeaderId, args, reply.Term)
		return
	}

	rf.ResetVoteTimer()

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term // 更新iterm
		rf.votedFor = -1           // 更新投票记录为未投票
		rf.role = Follower
		rf.persist()
	}

	if len(args.Entries) == 0 {
		DPrintf("server %v 接收到 leader %v 的心跳, 自身lastIncludedIndex=%v, PrevLogIndex=%v, len(Entries) = %v\n", rf.me, args.LeaderId, rf.lastIncludedIndex, args.PrevLogIndex, len(args.Entries))
	} else {
		DPrintf("server %v 收到 leader %v 的AppendEntries, 自身lastIncludedIndex=%v, PrevLogIndex=%v, len(Entries)= %+v \n", rf.me, args.LeaderId, rf.lastIncludedIndex, args.PrevLogIndex, len(args.Entries))
	}

	isConflict := false

	if args.PrevLogIndex < rf.lastIncludedIndex {
		reply.Success = true
		reply.Term = rf.currentTerm
		return
	} else if args.PrevLogIndex >= rf.VirtualLogIdx(int64(len(rf.log))) {
		reply.XTerm = -1
		reply.XLen = rf.VirtualLogIdx(int64(len(rf.log))) // Log长度, 包括了已经snapShot的部分
		isConflict = true
		DPrintf("server %v 的log在PrevLogIndex: %v 位置不存在日志项, Log长度为%v\n", rf.me, args.PrevLogIndex, reply.XLen)
	} else if rf.log[rf.RealLogIdx(args.PrevLogIndex)].Term != args.PrevLogTerm {
		reply.XTerm = rf.log[rf.RealLogIdx(args.PrevLogIndex)].Term
		i := args.PrevLogIndex
		for i > rf.commitIndex && rf.log[rf.RealLogIdx(i)].Term == reply.XTerm {
			i -= 1
		}
		reply.XIndex = i + 1
		reply.XLen = rf.VirtualLogIdx(int64(len(rf.log))) // Log长度, 包括了已经snapShot的部分
		isConflict = true
		DPrintf("server %v 的log在PrevLogIndex: %v 位置Term不匹配, args.Term=%v, 实际的term=%v\n", rf.me, args.PrevLogIndex, args.PrevLogTerm, reply.XTerm)
	}

	if isConflict {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	for idx, log := range args.Entries {
		ridx := int(rf.RealLogIdx(args.PrevLogIndex)) + 1 + idx
		if ridx < len(rf.log) && rf.log[ridx].Term != log.Term {
			rf.log = rf.log[:ridx]
			rf.log = append(rf.log, args.Entries[idx:]...)
			break
		} else if ridx == len(rf.log) {
			rf.log = append(rf.log, args.Entries[idx:]...)
			break
		}
	}
	if len(args.Entries) != 0 {
		DPrintf("server %v 成功进行apeend, lastApplied=%v, len(log)=%v\n", rf.me, rf.lastApplied, len(rf.log))
	}

	rf.persist()

	reply.Success = true
	reply.Term = rf.currentTerm

	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit > rf.VirtualLogIdx(int64(len(rf.log)-1)) {
			rf.commitIndex = rf.VirtualLogIdx(int64(len(rf.log) - 1))
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		DPrintf("server %v 唤醒检查commit的协程, commitIndex=%v, len(log)=%v\n", rf.me, rf.commitIndex, len(rf.log))
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
		DPrintf("server %v 拒绝向 server %v 投票: 旧的term: %v, args = %+v\n", rf.me, args.CandidateId, args.Term, args)
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
			(args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= rf.VirtualLogIdx(int64(len(rf.log)-1))) {
			rf.currentTerm = args.Term
			reply.Term = rf.currentTerm
			rf.votedFor = args.CandidateId
			rf.role = Follower
			rf.ResetVoteTimer()
			rf.persist()

			reply.VoteGranted = true
			DPrintf("server %v 同意向 server %v 投票, args = %+v, len(rf.log)=%v\n", rf.me, args.CandidateId, args, len(rf.log))
			return
		} else {
			if args.LastLogTerm < rf.log[len(rf.log)-1].Term {
				DPrintf("server %v 拒绝向 server %v 投票: 更旧的LastLogTerm, args = %+v\n", rf.me, args.CandidateId, args)
			} else {
				DPrintf("server %v 拒绝向 server %v 投票: 更短的Log, args = %+v\n", rf.me, args.CandidateId, args)
			}
		}
	} else {
		DPrintf("server %v 拒绝向 server %v投票: 已投票, args = %+v\n", rf.me, args.CandidateId, args)
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false
}
