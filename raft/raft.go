package raft

import (
	"fmt"
	"math/rand"
	"raft-kv/raft/proto"
	"sync"
	"sync/atomic"
	"time"
)

type Raft struct {
	mu          sync.Mutex         // Lock to protect shared access to this peer's state
	peers       []proto.RaftClient // RPC end points of all peers
	persister   *Persister         // Object to hold this peer's persisted state
	me          int32              // this peer's index into peers[]
	dead        int32              // set by Kill()
	currentTerm int32
	votedFor    int32
	log         []*proto.Entry

	nextIndex  []int64
	matchIndex []int64

	// self set field for impl
	voteTimer  *time.Timer
	heartTimer *time.Timer
	rd         *rand.Rand
	role       int32

	commitIndex int64
	lastApplied int64
	applyCh     chan ApplyMsg
	condApply   *sync.Cond

	snapShot          []byte
	lastIncludedIndex int64
	lastIncludedTerm  int32
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int32, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == Leader
}

// Kill the server and release all resources.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// Start means start agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command Command) (int64, int32, bool) {
	rf.mu.Lock()
	defer func() {
		rf.resetHeartTimer(15)
		rf.mu.Unlock()
	}()
	if rf.role != Leader {
		return -1, -1, false
	}
	newEntry := &proto.Entry{Term: rf.currentTerm, Cmd: command.ToBytes()}
	rf.log = append(rf.log, newEntry)
	rf.persist()

	return rf.virtualLogIdx(int64(len(rf.log) - 1)), rf.currentTerm, true
}

func (rf *Raft) CommitChecker() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.commitIndex <= rf.lastApplied {
			rf.condApply.Wait()
		}
		msgBuf := make([]*ApplyMsg, 0, rf.commitIndex-rf.lastApplied)
		tmpApplied := rf.lastApplied
		for rf.commitIndex > tmpApplied {
			tmpApplied += 1
			if tmpApplied <= rf.lastIncludedIndex {
				continue
			}
			msg := &ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.realLogIdx(tmpApplied)].Cmd,
				CommandIndex: tmpApplied,
				SnapshotTerm: rf.log[rf.realLogIdx(tmpApplied)].Term,
			}

			msgBuf = append(msgBuf, msg)
		}
		rf.mu.Unlock()

		for _, msg := range msgBuf {
			rf.mu.Lock()
			if msg.CommandIndex != rf.lastApplied+1 {
				rf.mu.Unlock()
				continue
			}
			rf.mu.Unlock()

			rf.applyCh <- *msg

			rf.mu.Lock()
			if msg.CommandIndex != rf.lastApplied+1 {
				rf.mu.Unlock()
				continue
			}
			rf.lastApplied = msg.CommandIndex
			rf.mu.Unlock()
		}
	}
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

func (rf *Raft) sendHeartBeats() {
	for !rf.killed() {
		<-rf.heartTimer.C
		rf.mu.Lock()
		if rf.role != Leader {
			rf.mu.Unlock()
			return
		}
		for i := 0; i < len(rf.peers); i++ {
			if int32(i) == rf.me {
				continue
			}
			args := &proto.AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: rf.nextIndex[i] - 1,
				LeaderCommit: rf.commitIndex,
			}
			sendInstallSnapshot := false

			if args.PrevLogIndex < rf.lastIncludedIndex {
				sendInstallSnapshot = true
			} else if rf.virtualLogIdx(int64(len(rf.log)-1)) > args.PrevLogIndex {
				args.Entries = rf.log[rf.realLogIdx(args.PrevLogIndex+1):]
			} else {
				args.Entries = make([]*proto.Entry, 0)
			}
			if sendInstallSnapshot {
				go rf.handleInstallSnapshot(i)
			} else {
				args.PrevLogTerm = rf.log[rf.realLogIdx(args.PrevLogIndex)].Term
				go rf.handleAppendEntries(i, args)
			}
		}

		rf.mu.Unlock()
		rf.resetHeartTimer(HeartBeatTimeOut)
	}
}

func (rf *Raft) getVoteAnswer(server int, args *proto.RequestVoteArgs) bool {
	reply, ok := rf.sendRequestVote(server, args)
	fmt.Printf(">>> server %v 收到 server %v 的RequestVoteReply, Term=%v, VoteGranted=%v\n", rf.me, server, reply.Term, reply.VoteGranted)
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
		// todo: change it to self log
		fmt.Printf(">>> server %v 赢得了选举, 成为Leader\n", rf.me)
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

func (rf *Raft) ticker() {
	for !rf.killed() {
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		<-rf.voteTimer.C
		rf.mu.Lock()
		if rf.role != Leader {
			go rf.elect()
		}
		rf.resetVoteTimer()
		rf.mu.Unlock()
	}
}

// Make the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []proto.RaftClient, me int32,
	persister *Persister, applyCh chan ApplyMsg) *Raft {

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.log = make([]*proto.Entry, 0)
	rf.log = append(rf.log, &proto.Entry{Term: 0})

	rf.nextIndex = make([]int64, len(peers))
	rf.matchIndex = make([]int64, len(peers))
	// rf.timeStamp = time.Now()
	rf.role = Follower
	rf.applyCh = applyCh
	rf.condApply = sync.NewCond(&rf.mu)
	rf.rd = rand.New(rand.NewSource(int64(rf.me)))
	rf.voteTimer = time.NewTimer(0)
	rf.heartTimer = time.NewTimer(0)
	rf.resetVoteTimer()

	// initialize from state persisted before a crash
	rf.readSnapshot(persister.ReadSnapshot())
	rf.readPersist(persister.ReadRaftState())

	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = rf.virtualLogIdx(int64(len(rf.log))) // raft中的index是从1开始的
	}
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.CommitChecker()

	return rf
}
