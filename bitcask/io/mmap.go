package io

import (
	"os"
	"syscall"
	"unsafe"
)

const defaultMemMapSize = 128 * (1 << 20)

// for now mmap don't work.
type MMap struct {
	fd          *os.File
	data        *[defaultMemMapSize]byte
	dataRef     []byte
	writeOffset int64
}

func NewMMapIOManager(fileName string) (*MMap, error) {
	fd, err := os.OpenFile(
		fileName,
		os.O_CREATE|os.O_RDWR,
		DataFilePerm,
	)
	if err != nil {
		return nil, err
	}
	mmap, err := syscall.Mmap(int(fd.Fd()), 0, defaultMemMapSize, syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return nil, err
	}
	info, err := os.Stat(fileName)
	if err != nil {
		return nil, err
	}
	offset := info.Size()
	err = fd.Truncate(defaultMemMapSize)
	if err != nil {
		return nil, err
	}
	return &MMap{
		data:        (*[defaultMemMapSize]byte)(unsafe.Pointer(&mmap[0])),
		fd:          fd,
		dataRef:     mmap,
		writeOffset: offset,
	}, nil
}

func (mio *MMap) grow(size int64) {
	if size <= mio.writeOffset {
		return
	}
	if size > int64(len(mio.data)) {
		panic("grow too large")
	}
	err := mio.fd.Truncate(size)
	if err != nil {
		panic(err)
	}
}

func (mio *MMap) Read(b []byte, offset int64) (int, error) {
	if offset > mio.writeOffset {
		return 0, ErrEOF
	}
	if offset+int64(len(b)) > mio.writeOffset {
		n := copy(b, mio.data[offset:mio.writeOffset])
		return n, nil
	}
	n := copy(b, mio.data[offset:])
	return n, nil
}

func (mio *MMap) Write(b []byte) (int, error) {
	n := copy(mio.data[mio.writeOffset:], b)
	mio.writeOffset += int64(n)
	return n, nil
}

func (mio *MMap) Sync() error {
	return nil
}

func (mio *MMap) Close() error {
	err := mio.fd.Truncate(mio.writeOffset)
	if err != nil {
		return err
	}
	err = syscall.Munmap(mio.dataRef)
	if err != nil {
		panic(err)
	}
	mio.data = nil
	mio.dataRef = nil
	err = mio.fd.Close()
	if err != nil {
		return err
	}
	return nil
}

func (mio *MMap) Size() (int64, error) {
	return mio.writeOffset, nil
}

func (mio *MMap) GetFileName() string {
	return mio.fd.Name()
}
