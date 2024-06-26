package raft

import (
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

// Do means start agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Do(command Command) (int64, int32, bool) {
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

func (rf *Raft) Read(command Command) (int64, int32, bool) {
	// for a read-only command, we use lease read
	// to insure this rf server is leader
	return rf.Do(command)
}

func (rf *Raft) commitChecker() {
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

// Make the service to create a Raft server.
//
// @Param:
//
// peers: the ports of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order.
//
// persister: a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any.
//
// applyCh: a channel on which the
// service expects Raft to send ApplyMsg messages.
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
	go rf.commitChecker()

	return rf
}
