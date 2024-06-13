package raft

import (
	"raft-kv/raft/proto"
	"sync"
)

func (rf *Raft) getVoteAnswer(server int, args *proto.RequestVoteArgs) bool {
	reply, ok := rf.sendRequestVote(server, args)
	if !ok {
		return false
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != Candidate || args.Term != rf.currentTerm {
		// 易错点: 函数调用的间隙被修改了
		return false
	}

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.role = Follower
		rf.persist()
	}
	return reply.VoteGranted
}

func (rf *Raft) collectVote(serverTo int, args *proto.RequestVoteArgs, muVote *sync.Mutex, voteCount *int) {
	voteAnswer := rf.getVoteAnswer(serverTo, args)
	if !voteAnswer {
		return
	}
	muVote.Lock()
	if *voteCount > len(rf.peers)/2 {
		muVote.Unlock()
		return
	}

	*voteCount += 1
	if *voteCount > len(rf.peers)/2 {
		rf.mu.Lock()
		if rf.role != Candidate || rf.currentTerm != args.Term {
			rf.mu.Unlock()
			muVote.Unlock()
			return
		}
		rf.role = Leader
		for i := 0; i < len(rf.nextIndex); i++ {
			rf.nextIndex[i] = rf.virtualLogIdx(int64(len(rf.log)))
			rf.matchIndex[i] = rf.lastIncludedIndex // 由于matchIndex初始化为lastIncludedIndex, 因此在崩溃恢复后, 大概率触发InstallSnapshot RPC
		}
		rf.mu.Unlock()

		go rf.sendHeartBeats()
	}

	muVote.Unlock()
}

func (rf *Raft) elect() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.currentTerm += 1 // 自增term
	rf.role = Candidate // 成为候选人
	rf.votedFor = rf.me // 给自己投票
	rf.persist()

	voteCount := 1 // 自己有一票
	var muVote sync.Mutex

	args := &proto.RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.virtualLogIdx(int64(len(rf.log) - 1)),
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}

	for i := 0; i < len(rf.peers); i++ {
		if int32(i) == rf.me {
			continue
		}
		go rf.collectVote(i, args, &muVote, &voteCount)
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
