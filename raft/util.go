package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

func (rf *Raft) Print() {
	DPrintf("raft%v:{currentTerm=%v, role=%v, votedFor=%v}\n", rf.me, rf.currentTerm, rf.role, rf.votedFor)
}

func GetRandomElectTimeOut(rd *rand.Rand) int {
	return rd.Intn(ElectTimeOutBase) + 300
}

func (rf *Raft) ResetVoteTimer() {
	rdTimeOut := GetRandomElectTimeOut(rf.rd)
	rf.voteTimer.Reset(time.Duration(rdTimeOut) * time.Millisecond)
}

func (rf *Raft) ResetHeartTimer(timeStamp int) {
	rf.heartTimer.Reset(time.Duration(timeStamp) * time.Millisecond)
}

func (rf *Raft) RealLogIdx(vIdx int) int {
	return vIdx - rf.lastIncludedIndex
}

func (rf *Raft) VirtualLogIdx(rIdx int) int {
	return rIdx + rf.lastIncludedIndex
}
