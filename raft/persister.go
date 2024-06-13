package raft

import (
	"encoding/binary"
	"os"
	"sync"
)

type State struct {
	raftState []byte
	snapshot  []byte
}

// Binary format:
// +-------------+--------------+-----------+---------------+
// |raftStateSize| snapshotSize | raftState |   snapshot    |
// +-------------+--------------+-----------+---------------+
// |dynamic(max5)| dynamic(max5)|  dynamic  |    dynamic    |
// +-------------+--------------+-----------+---------------+

type Persister struct {
	mu    sync.RWMutex
	fio   *os.File
	state *State
}

func encodeState(state *State) []byte {
	buf := make([]byte, 10)
	index := 0
	index += binary.PutVarint(buf[index:], int64(len(state.raftState)))
	index += binary.PutVarint(buf[index:], int64(len(state.snapshot)))
	size := index + len(state.raftState) + len(state.raftState)
	bs := make([]byte, size)
	copy(bs, buf)
	copy(bs[index:], state.raftState)
	index += len(state.raftState)
	copy(bs[index:], state.snapshot)

	return bs
}

func decodeState(bs []byte) *State {
	var index = 0
	raftStateSize, n := binary.Varint(bs[index:])
	index += n
	snapshotSize, n := binary.Varint(bs[index:])
	index += n
	raftState := bs[index : index+int(raftStateSize)]
	index += int(raftStateSize)
	snapshot := bs[index : index+int(snapshotSize)]
	return &State{
		raftState: raftState,
		snapshot:  snapshot,
	}
}

func MakePersister(filename string) *Persister {
	fio, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0755)
	if err != nil {
		panic(err)
	}

	return &Persister{
		state: &State{},
		mu:    sync.RWMutex{},
		fio:   fio,
	}
}

func clone(orig []byte) []byte {
	x := make([]byte, len(orig))
	copy(x, orig)
	return x
}

func (ps *Persister) ReadRaftState() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return clone(ps.state.raftState)
}

func (ps *Persister) RaftStateSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.state.raftState)
}

func (ps *Persister) ReadSnapshot() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return clone(ps.state.snapshot)
}

func (ps *Persister) SnapshotSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.state.snapshot)
}

// Save both Raft state and K/V snapshot as a single atomic action,
// to help avoid them getting out of sync.
func (ps *Persister) Save(raftState []byte, snapshot []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.state.raftState = clone(raftState)
	ps.state.snapshot = clone(snapshot)
}

func (ps *Persister) sync() error {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	bs := encodeState(ps.state)
	err := ps.fio.Truncate(int64(len(bs)))
	if err != nil {
		return err
	}
	_, err = ps.fio.WriteAt(bs, 0)
	if err != nil {
		return err
	}
	return nil
}
