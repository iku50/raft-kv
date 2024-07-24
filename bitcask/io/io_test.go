package io

import (
	"os"
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

func BenchmarkFileIO_Write(b *testing.B) {
	filename := "fio_write_test.txt"
	manager, err := NewManager(filename, FIO)
	assert.Equal(b, err, nil)
	defer manager.Close()
	for i := 0; i < b.N; i++ {
		_, _ = manager.Write([]byte("hello world\n"))
	}
}

func BenchmarkFileIO_Read(b *testing.B) {
	filename := "fio_write_test.txt"
	manager, err := NewManager(filename, FIO)
	assert.Equal(b, err, nil)
	defer manager.Close()
	for i := 0; i < b.N; i++ {
		_, _ = manager.Read(make([]byte, 10), 0)
	}
}

func BenchmarkMemoryIO_Write(b *testing.B) {
	filename := "mio_write_test.txt"
	manager, err := NewManager(filename, MemoryMap)
	assert.Equal(b, err, nil)
	defer manager.Close()
	for i := 0; i < b.N; i++ {
		_, _ = manager.Write([]byte("hello world\n"))
	}
}

func BenchmarkMemoryIO_Read(b *testing.B) {
	filename := "mio_write_test.txt"
	manager, err := NewManager(filename, MemoryMap)
	assert.Equal(b, err, nil)
	defer manager.Close()
	for i := 0; i < b.N; i++ {
		_, _ = manager.Read(make([]byte, 10), 0)
	}
}

func TestMMap_Size(t *testing.T) {
	filename := "test.txt"
	manager, err := NewManager(filename, MemoryMap)
	assert.Equal(t, err, nil)
	defer manager.Close()
	fd, err := os.Stat(filename)
	assert.Equal(t, err, nil)
	t.Log(fd.Size())
	t.Log(manager.Size())
}
