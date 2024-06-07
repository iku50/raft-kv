package raft

import (
	"math/rand"
	"time"
)

func getRandomElectTimeOut(rd *rand.Rand) int {
	return rd.Intn(ElectTimeOutBase) + 300
}

func (rf *Raft) resetVoteTimer() {
	rdTimeOut := getRandomElectTimeOut(rf.rd)
	rf.voteTimer.Reset(time.Duration(rdTimeOut) * time.Millisecond)
}

func (rf *Raft) resetHeartTimer(timeStamp int) {
	rf.heartTimer.Reset(time.Duration(timeStamp) * time.Millisecond)
}

func (rf *Raft) realLogIdx(vIdx int64) int64 {
	return vIdx - rf.lastIncludedIndex
}

func (rf *Raft) virtualLogIdx(rIdx int64) int64 {
	return rIdx + rf.lastIncludedIndex
}
