package raft

import "raft-kv/raft/proto"

func (rf *Raft) readSnapshot(data []byte) {
	if len(data) == 0 {
		return
	}
	rf.snapShot = data
}

func (rf *Raft) GetSnapshot() []byte {
	return clone(rf.snapShot)
}

func (rf *Raft) handleInstallSnapshot(serverTo int) {
	rf.mu.Lock()

	if rf.role != Leader {
		rf.mu.Unlock()
		return
	}

	args := &proto.InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              rf.snapShot,
		LastIncludedCmd:   rf.log[0].Cmd,
	}

	rf.mu.Unlock()

	reply, ok := rf.sendInstallSnapshot(serverTo, args)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer func() {
		rf.mu.Unlock()
	}()

	if rf.role != Leader || rf.currentTerm != args.Term {
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

	if rf.matchIndex[serverTo] < args.LastIncludedIndex {
		rf.matchIndex[serverTo] = args.LastIncludedIndex
	}
	rf.nextIndex[serverTo] = rf.matchIndex[serverTo] + 1
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

// Snapshot means the service no longer needs the log through (and including)
// that index. Raft now trim its log as much as possible.
func (rf *Raft) Snapshot(index int64, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.commitIndex < index || index <= rf.lastIncludedIndex {
		return
	}

	rf.snapShot = snapshot

	rf.lastIncludedTerm = rf.log[rf.realLogIdx(index)].Term
	rf.log = rf.log[rf.realLogIdx(index):]
	rf.lastIncludedIndex = index
	if rf.lastApplied < index {
		rf.lastApplied = index
	}

	rf.persist()
}
