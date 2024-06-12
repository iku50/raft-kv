package io

import (
	"os"

	"github.com/edsrzf/mmap-go"
)

// for now mmap don't work.
type MMap struct {
	data mmap.MMap
	fd   *os.File
}

func NewMMapIOManager(fileName string) (*MMap, error) {
	fd, err := os.OpenFile(
		fileName,
		os.O_CREATE|os.O_RDWR,
		MMapFilePerm,
	)
	if err != nil {
		return nil, err
	}
	mm, err := mmap.Map(fd, mmap.RDWR, 0)
	if err != nil {
		return nil, err
	}
	return &MMap{data: mm, fd: fd}, nil
}

func (mio *MMap) Read(b []byte, offset int64) (int, error) {
	if offset > int64(len(mio.data)) {
		return 0, ErrEOF
	}
	n := copy(b, mio.data[offset:])
	return n, nil
}

func (mio *MMap) Write(b []byte) (int, error) {
	if 0 > len(mio.data) {
		return 0, ErrEOF
	}
	n := copy(mio.data, b)
	return n, nil
}

func (mio *MMap) Sync() error {
	return mio.data.Flush()
}

func (mio *MMap) Close() error {
	err := mio.data.Unmap()
	if err != nil {
		return err
	}
	err = mio.fd.Close()
	if err != nil {
		return err
	}
	return nil
}

func (mio *MMap) Size() (int64, error) {
	return int64(len(mio.data)), nil
}
