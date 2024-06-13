package raft

const (
	Follower = iota
	Candidate
	Leader
)

const (
	HeartBeatTimeOut = 101
	ElectTimeOutBase = 300
)

type Command interface {
	ToBytes() []byte
	FromBytes([]byte)
}

// ApplyMsg is the message sent from Raft to the service.
type ApplyMsg struct {
	CommandValid bool
	Command      []byte
	CommandIndex int64

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int32
	SnapshotIndex int64
}
