package raft

// ApplyMsg is the message sent from Raft to the service.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Entry struct {
	Term int
	Cmd  interface{}
}
