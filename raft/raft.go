package raft

import (
	//	"bytes"

	"math/rand"
	"raft-kv/proto"
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

	condApply *sync.Cond

	snapShot          []byte
	lastIncludedIndex int64
	lastIncludedTerm  int32
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int32, bool) {
	rf.mu.Lock()
	// DPrintf("server %v GetState 获取锁mu", rf.me)
	defer func() {
		rf.mu.Unlock()
		// DPrintf("server %v GetState 释放锁mu", rf.me)
	}()
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
	// Your code here (2B).
	rf.mu.Lock()
	defer func() {
		rf.ResetHeartTimer(15)
		rf.mu.Unlock()
	}()
	if rf.role != Leader {
		return -1, -1, false
	}

	newEntry := &proto.Entry{Term: rf.currentTerm, Cmd: command.ToBytes()}
	rf.log = append(rf.log, newEntry)
	DPrintf("leader %v 准备持久化", rf.me)
	rf.persist()

	return rf.VirtualLogIdx(int64(len(rf.log) - 1)), rf.currentTerm, true
}

func (rf *Raft) CommitChecker() {
	DPrintf("server %v 的 CommitChecker 开始运行", rf.me)
	for !rf.killed() {
		rf.mu.Lock()
		// DPrintf("server %v CommitChecker 获取锁mu", rf.me)
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
			// if rf.RealLogIdx(int(tmpApplied)) >= int64(len(rf.log)) {
			// 	// DPrintf("server %v CommitChecker数组越界: tmpApplied=%v,  rf.RealLogIdx(tmpApplied)=%v>=len(rf.log)=%v, lastIncludedIndex=%v", rf.me, tmpApplied, rf.RealLogIdx(tmpApplied), len(rf.log), rf.lastIncludedIndex)
			// }
			msg := &ApplyMsg{
				CommandValid: true,
				Command:      CommandFromBytes(rf.log[rf.RealLogIdx(tmpApplied)].Cmd),
				CommandIndex: tmpApplied,
				SnapshotTerm: rf.log[rf.RealLogIdx(tmpApplied)].Term,
			}

			msgBuf = append(msgBuf, msg)
		}
		rf.mu.Unlock()
		// DPrintf("server %v CommitChecker 释放锁mu", rf.me)

		for _, msg := range msgBuf {
			rf.mu.Lock()
			if msg.CommandIndex != rf.lastApplied+1 {
				rf.mu.Unlock()
				continue
			}
			DPrintf("server %v 准备commit, log = %v:%v, lastIncludedIndex=%v", rf.me, msg.CommandIndex, msg.SnapshotTerm, rf.lastIncludedIndex)
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
	reply := &proto.InstallSnapshotReply{}

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

	ok := rf.sendInstallSnapshot(serverTo, args, reply)
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
		rf.ResetVoteTimer()
		rf.persist()
		return
	}

	if rf.matchIndex[serverTo] < args.LastIncludedIndex {
		rf.matchIndex[serverTo] = args.LastIncludedIndex
	}
	rf.nextIndex[serverTo] = rf.matchIndex[serverTo] + 1
}

func (rf *Raft) handleAppendEntries(serverTo int, args *proto.AppendEntriesArgs) {
	reply := &proto.AppendEntriesReply{}
	ok := rf.sendAppendEntries(serverTo, args, reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	// DPrintf("server %v handleAppendEntries 获取锁mu", rf.me)
	defer func() {
		// DPrintf("server %v handleAppendEntries 释放锁mu", rf.me)
		rf.mu.Unlock()
	}()

	if rf.role != Leader || args.Term != rf.currentTerm {
		return
	}

	if reply.Success {
		newMatchIdx := args.PrevLogIndex + int64(len(args.Entries))
		if newMatchIdx > rf.matchIndex[serverTo] {
			rf.matchIndex[serverTo] = newMatchIdx
		}

		rf.nextIndex[serverTo] = rf.matchIndex[serverTo] + 1

		N := rf.VirtualLogIdx(int64(len(rf.log) - 1))

		DPrintf("leader %v 确定N以决定新的commitIndex, lastIncludedIndex=%v, commitIndex=%v", rf.me, rf.lastIncludedIndex, rf.commitIndex)

		for int64(N) > rf.commitIndex {
			count := 1 // 1表示包括了leader自己
			for i := 0; i < len(rf.peers); i++ {
				if int32(i) == rf.me {
					continue
				}
				if rf.matchIndex[i] >= int64(N) && rf.log[rf.RealLogIdx(N)].Term == rf.currentTerm {
					count += 1
				}
			}
			if count > len(rf.peers)/2 {
				break
			}
			N -= 1
		}

		rf.commitIndex = int64(N)
		rf.condApply.Signal() // 唤醒检查commit的协程

		return
	}

	if reply.Term > rf.currentTerm {
		DPrintf("server %v 旧的leader收到了来自 server % v 的心跳函数中更新的term: %v, 转化为Follower\n", rf.me, serverTo, reply.Term)

		rf.currentTerm = reply.Term
		rf.role = Follower
		rf.votedFor = -1
		rf.ResetVoteTimer()
		rf.persist()
		return
	}

	if reply.Term == rf.currentTerm && rf.role == Leader {
		if reply.XTerm == -1 {
			DPrintf("leader %v 收到 server %v 的回退请求, 原因是log过短, 回退前的nextIndex[%v]=%v, 回退后的nextIndex[%v]=%v\n", rf.me, serverTo, serverTo, rf.nextIndex[serverTo], serverTo, reply.XLen)
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
		for i > rf.lastIncludedIndex && rf.log[rf.RealLogIdx(i)].Term > reply.XTerm {
			i -= 1
		}

		if i == rf.lastIncludedIndex && rf.log[rf.RealLogIdx(i)].Term > reply.XTerm {
			// 要找的位置已经由于snapshot被截断
			// 下一次心跳添加InstallSnapshot的处理
			rf.nextIndex[serverTo] = rf.lastIncludedIndex
		} else if rf.log[rf.RealLogIdx(i)].Term == reply.XTerm {
			// 之前PrevLogIndex发生冲突位置时, Follower的Term自己也有

			DPrintf("leader %v 收到 server %v 的回退请求, 冲突位置的Term为%v, server的这个Term从索引%v开始, 而leader对应的最后一个XTerm索引为%v, 回退前的nextIndex[%v]=%v, 回退后的nextIndex[%v]=%v\n", rf.me, serverTo, reply.XTerm, reply.XIndex, i, serverTo, rf.nextIndex[serverTo], serverTo, i+1)
			rf.nextIndex[serverTo] = i + 1 // i + 1是确保没有被截断的
		} else {
			// 之前PrevLogIndex发生冲突位置时, Follower的Term自己没有
			DPrintf("leader %v 收到 server %v 的回退请求, 冲突位置的Term为%v, server的这个Term从索引%v开始, 而leader对应的XTerm不存在, 回退前的nextIndex[%v]=%v, 回退后的nextIndex[%v]=%v\n", rf.me, serverTo, reply.XTerm, reply.XIndex, serverTo, rf.nextIndex[serverTo], serverTo, reply.XIndex)
			if reply.XIndex <= rf.lastIncludedIndex {
				// XIndex位置也被截断了
				// 添加InstallSnapshot的处理
				rf.nextIndex[serverTo] = rf.lastIncludedIndex
			} else {
				rf.nextIndex[serverTo] = reply.XIndex
			}
		}
		return
	}
}

func (rf *Raft) SendHeartBeats() {
	// 2B相对2A的变化, 真实的AppendEntries也通过心跳发送
	DPrintf("leader %v 开始发送心跳\n", rf.me)

	for !rf.killed() {
		<-rf.heartTimer.C
		rf.mu.Lock()
		// DPrintf("server %v SendHeartBeats 获取锁mu", rf.me)
		// if the server is dead or is not the leader, just return
		if rf.role != Leader {
			rf.mu.Unlock()
			// DPrintf("server %v SendHeartBeats 释放锁mu", rf.me)
			// 不是leader则终止心跳的发送
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
				// 表示Follower有落后的部分且被截断, 改为发送同步心跳
				DPrintf("leader %v 取消向 server %v 广播新的心跳, 改为发送sendInstallSnapshot, lastIncludedIndex=%v, nextIndex[%v]=%v, args = %+v \n", rf.me, i, rf.lastIncludedIndex, i, rf.nextIndex[i], args)
				sendInstallSnapshot = true
			} else if rf.VirtualLogIdx(int64(len(rf.log)-1)) > args.PrevLogIndex {
				// 如果有新的log需要发送, 则就是一个真正的AppendEntries而不是心跳
				args.Entries = rf.log[rf.RealLogIdx(args.PrevLogIndex+1):]
				DPrintf("leader %v 开始向 server %v 广播新的AppendEntries, lastIncludedIndex=%v, nextIndex[%v]=%v, PrevLogIndex=%v, len(Entries) = %v\n", rf.me, i, rf.lastIncludedIndex, i, rf.nextIndex[i], args.PrevLogIndex, len(args.Entries))
			} else {
				// 如果没有新的log发送, 就发送一个长度为0的切片, 表示心跳
				DPrintf("leader %v 开始向 server %v 广播新的心跳, lastIncludedIndex=%v, nextIndex[%v]=%v, PrevLogIndex=%v, len(Entries) = %v \n", rf.me, i, rf.lastIncludedIndex, i, rf.nextIndex[i], args.PrevLogIndex, len(args.Entries))
				args.Entries = make([]*proto.Entry, 0)
			}

			if sendInstallSnapshot {
				go rf.handleInstallSnapshot(i)
			} else {
				args.PrevLogTerm = rf.log[rf.RealLogIdx(args.PrevLogIndex)].Term
				go rf.handleAppendEntries(i, args)
			}
		}

		rf.mu.Unlock()
		// DPrintf("server %v SendHeartBeats 释放锁mu", rf.me)
		rf.ResetHeartTimer(HeartBeatTimeOut)
	}
}

func (rf *Raft) GetVoteAnswer(server int, args *proto.RequestVoteArgs) bool {
	sendArgs := *args
	reply := proto.RequestVoteReply{}
	ok := rf.sendRequestVote(server, &sendArgs, &reply)
	if !ok {
		return false
	}

	rf.mu.Lock()
	// DPrintf("server %v GetVoteAnswer 获取锁mu", rf.me)
	defer func() {
		// DPrintf("server %v GetVoteAnswer 释放锁mu", rf.me)
		rf.mu.Unlock()
	}()

	if rf.role != Candidate || sendArgs.Term != rf.currentTerm {
		// 易错点: 函数调用的间隙被修改了
		return false
	}

	if reply.Term > rf.currentTerm {
		// 已经是过时的term了
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.role = Follower
		rf.persist()
	}
	return reply.VoteGranted
}

func (rf *Raft) collectVote(serverTo int, args *proto.RequestVoteArgs, muVote *sync.Mutex, voteCount *int) {
	voteAnswer := rf.GetVoteAnswer(serverTo, args)
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
		// DPrintf("server %v collectVote 获取锁mu", rf.me)
		if rf.role != Candidate || rf.currentTerm != args.Term {
			// 有另外一个投票的协程收到了更新的term而更改了自身状态为Follower
			// 或者自己的term已经过期了, 也就是被新一轮的选举追上了
			rf.mu.Unlock()
			// DPrintf("server %v 释放锁mu", rf.me)

			muVote.Unlock()
			return
		}
		DPrintf("server %v 成为了新的 leader", rf.me)
		rf.role = Leader
		// 需要重新初始化nextIndex和matchIndex
		for i := 0; i < len(rf.nextIndex); i++ {
			rf.nextIndex[i] = rf.VirtualLogIdx(int64(len(rf.log)))
			rf.matchIndex[i] = rf.lastIncludedIndex // 由于matchIndex初始化为lastIncludedIndex, 因此在崩溃恢复后, 大概率触发InstallSnapshot RPC
		}
		rf.mu.Unlock()
		// DPrintf("server %v collectVote 释放锁mu", rf.me)

		go rf.SendHeartBeats()
	}

	muVote.Unlock()
}

func (rf *Raft) Elect() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.currentTerm += 1 // 自增term
	rf.role = Candidate // 成为候选人
	rf.votedFor = rf.me // 给自己投票
	rf.persist()

	voteCount := 1 // 自己有一票
	var muVote sync.Mutex

	DPrintf("server %v 开始发起新一轮投票, 新一轮的term为: %v", rf.me, rf.currentTerm)

	args := &proto.RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.VirtualLogIdx(int64(len(rf.log) - 1)),
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
			go rf.Elect()
		}
		rf.ResetVoteTimer()
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
	DPrintf("server %v 调用Make启动", me)

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
	rf.ResetVoteTimer()

	// initialize from state persisted before a crash
	// 如果读取成功, 将覆盖log, votedFor和currentTerm
	rf.readSnapshot(persister.ReadSnapshot())
	rf.readPersist(persister.ReadRaftState())

	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = rf.VirtualLogIdx(int64(len(rf.log))) // raft中的index是从1开始的
	}

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.CommitChecker()

	return rf
}
