package raft

func (rf *Raft) readSnapshot(data []byte) {
	if len(data) == 0 {
		return
	}
	rf.snapShot = data
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
