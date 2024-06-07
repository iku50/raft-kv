package raft

import (
	"bytes"
	"encoding/gob"
	"raft-kv/proto"
)

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	_ = e.Encode(rf.votedFor)
	_ = e.Encode(rf.currentTerm)
	_ = e.Encode(rf.log)
	_ = e.Encode(rf.lastIncludedIndex)
	_ = e.Encode(rf.lastIncludedTerm)

	rf.persister.Save(w.Bytes(), rf.snapShot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if len(data) == 0 {
		return
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)

	var votedFor int32
	var currentTerm int32
	var log []*proto.Entry
	var lastIncludedIndex int64
	var lastIncludedTerm int32
	if d.Decode(&votedFor) != nil ||
		d.Decode(&currentTerm) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
	} else {
		rf.votedFor = votedFor
		rf.currentTerm = currentTerm
		rf.log = log
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
		rf.commitIndex = lastIncludedIndex
		rf.lastApplied = lastIncludedIndex
	}
}
