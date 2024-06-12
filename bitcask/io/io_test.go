package io

import (
	"testing"

	"github.com/go-playground/assert/v2"
)

func TestFileIO(t *testing.T) {
	filename := "test.txt"
	manager, err := NewManager(filename, FIO)
	assert.Equal(t, err, nil)
	defer manager.Close()
	write, err := manager.Write([]byte("hello world"))
	assert.Equal(t, err, nil)
	t.Log(write)
	err = manager.Sync()
	assert.Equal(t, err, nil)
	bytes := make([]byte, write)
	size, err := manager.Read(bytes, 0)
	assert.Equal(t, err, nil)
	t.Log(string(bytes[:size]))
	t.Log(size)
}

func TestMemoryIO(t *testing.T) {
	filename := "test.txt"
	manager, err := NewManager(filename, MemoryMap)
	assert.Equal(t, err, nil)
	defer manager.Close()
	write, err := manager.Write([]byte("hello world"))
	assert.Equal(t, err, nil)
	t.Log(write)
	err = manager.Sync()
	assert.Equal(t, err, nil)
	bytes := make([]byte, write)
	size, err := manager.Read(bytes, 0)
	assert.Equal(t, err, nil)
	t.Log(string(bytes[:size]))
	t.Log(size)
}
