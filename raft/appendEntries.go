package raft

import "raft-kv/raft/proto"

func (rf *Raft) handleAppendEntries(serverTo int, args *proto.AppendEntriesArgs) {
	reply, ok := rf.sendAppendEntries(serverTo, args)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != Leader || args.Term != rf.currentTerm {
		return
	}

	if reply.Success {
		newMatchIdx := args.PrevLogIndex + int64(len(args.Entries))
		if newMatchIdx > rf.matchIndex[serverTo] {
			rf.matchIndex[serverTo] = newMatchIdx
		}

		rf.nextIndex[serverTo] = rf.matchIndex[serverTo] + 1

		N := rf.virtualLogIdx(int64(len(rf.log) - 1))

		for N > rf.commitIndex {
			count := 1
			for i := 0; i < len(rf.peers); i++ {
				if int32(i) == rf.me {
					continue
				}
				if rf.matchIndex[i] >= N && rf.log[rf.realLogIdx(N)].Term == rf.currentTerm {
					count += 1
				}
			}
			if count > len(rf.peers)/2 {
				break
			}
			N -= 1
		}
		rf.commitIndex = N
		rf.condApply.Signal()
		return
	}

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.role = Follower
		rf.votedFor = -1
		rf.resetVoteTimer()
		rf.persist()
		return
	}

	if reply.Term == rf.currentTerm && rf.role == Leader {
		if reply.XTerm == -1 {
			if rf.lastIncludedIndex >= reply.XLen {
				rf.nextIndex[serverTo] = rf.lastIncludedIndex
			} else {
				rf.nextIndex[serverTo] = reply.XLen
			}
			return
		}

		i := rf.nextIndex[serverTo] - 1
		if i < rf.lastIncludedIndex {
			i = rf.lastIncludedIndex
		}
		for i > rf.lastIncludedIndex && rf.log[rf.realLogIdx(i)].Term > reply.XTerm {
			i -= 1
		}

		if i == rf.lastIncludedIndex && rf.log[rf.realLogIdx(i)].Term > reply.XTerm {
			rf.nextIndex[serverTo] = rf.lastIncludedIndex
		} else if rf.log[rf.realLogIdx(i)].Term == reply.XTerm {
			rf.nextIndex[serverTo] = i + 1
		} else {
			if reply.XIndex <= rf.lastIncludedIndex {
				rf.nextIndex[serverTo] = rf.lastIncludedIndex
			} else {
				rf.nextIndex[serverTo] = reply.XIndex
			}
		}
		return
	}
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
