package raft

import (
	"testing"

	"github.com/go-playground/assert/v2"
)

func TestState(t *testing.T) {
	raftState := make([]byte, 50)
	snapshot := make([]byte, 50)
	dc := State{
		raftState: raftState,
		snapshot:  snapshot,
	}
	p := encodeState(&dc)
	q := decodeState(p)
	assert.Equal(t, q.raftState, raftState)
	assert.Equal(t, q.snapshot, snapshot)
}
