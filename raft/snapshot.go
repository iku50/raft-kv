package raft

func (rf *Raft) readSnapshot(data []byte) {
	if len(data) == 0 {
		DPrintf("server %v 读取快照失败: 无快照\n", rf.me)
		return
	}
	rf.snapShot = data
	DPrintf("server %v 读取快照c成功\n", rf.me)
}

// Snapshot means the service no longer needs the log through (and including)
// that index. Raft now trim its log as much as possible.
func (rf *Raft) Snapshot(index int64, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.commitIndex < index || index <= rf.lastIncludedIndex {
		DPrintf("server %v 拒绝了 Snapshot 请求, 其index=%v, 自身commitIndex=%v, lastIncludedIndex=%v\n", rf.me, index, rf.commitIndex, rf.lastIncludedIndex)
		return
	}

	DPrintf("server %v 同意了 Snapshot 请求, 其index=%v, 自身commitIndex=%v, 原来的lastIncludedIndex=%v, 快照后的lastIncludedIndex=%v\n", rf.me, index, rf.commitIndex, rf.lastIncludedIndex, index)

	rf.snapShot = snapshot

	rf.lastIncludedTerm = rf.log[rf.RealLogIdx(index)].Term
	rf.log = rf.log[rf.RealLogIdx(index):]
	rf.lastIncludedIndex = index
	if rf.lastApplied < index {
		rf.lastApplied = index
	}

	rf.persist()
}
