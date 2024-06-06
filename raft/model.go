package raft

type Command interface {
	ToBytes() []byte
}

func CommandFromBytes(b []byte) Command {
	return nil
}

// ApplyMsg is the message sent from Raft to the service.
type ApplyMsg struct {
	CommandValid bool
	Command      Command
	CommandIndex int64

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int32
	SnapshotIndex int64
}

type Entry struct {
	Term int32
	Cmd  Command
}
