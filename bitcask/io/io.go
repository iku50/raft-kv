package io

import "errors"

type Manager interface {
	Read([]byte, int64) (int, error)
	Write([]byte) (int, error)
	Sync() error
	Close() error
	Size() (int64, error)
	GetFileName() string
}

type FileIOType uint8

var (
	ErrEOF = errors.New("end of file")
)

const (
	DataFilePerm = 0644
	MMapFilePerm = 0755
)
const (
	FIO FileIOType = iota
	MemoryMap
)

func NewManager(filename string, ioType FileIOType) (Manager, error) {
	switch ioType {
	case FIO:
		return NewFileIOManager(filename)
	case MemoryMap:
		return NewMMapIOManager(filename)
	default:
		panic("unsupported io type")
	}
}
