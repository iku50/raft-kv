package bitcask

import (
	"errors"
	"os"
	"raft-kv/bitcask/data"
	"raft-kv/bitcask/index"
	"raft-kv/bitcask/io"
	"sync"
	"time"
)

// now it just a simple key-value store

type BitCask interface {
	Stat() *Stat

	Get(key []byte) (value []byte, err error)

	Put(key, value []byte) error

	Delete(key []byte) error

	// List() (key []string, err error)

	Merge() error

	Sync() error

	Close()
}

type DB struct {
	mu     *sync.RWMutex
	wg     *sync.WaitGroup
	stopCh chan struct{}

	fileIds []int
	dirPath string

	activeFile *data.File
	hintFile   *data.File
	olderFiles map[uint32]*data.File

	index       index.Indexer
	bytesWrite  uint
	reclaimSize int64
}

const DataFileSize = 1024 * 1024 * 1024

type Stat struct {
	KeyNum          uint
	DataFileNum     uint
	ReclaimableSize int64
	DiskSize        int64
}

var (
	ErrKeyIsEmpty             = errors.New("key is empty")
	ErrKeyNotFound            = errors.New("key not found")
	ErrFileNotFound           = errors.New("file not found")
	ErrDataDirectoryCorrupted = errors.New("data directory corrupted")
)

func (db *DB) Close() {
	db.stopCh <- struct{}{}
	db.wg.Wait()
	db.mu.Lock()
	defer db.mu.Unlock()
	// close index
	err := db.index.Close()
	if err != nil {
		return
	}

	// close active file and older files
	err = db.activeFile.Close()
	for _, file := range db.olderFiles {
		if err := file.Close(); err != nil {
			return
		}
	}
	if err != nil {
		return
	}
}

type Option func(db *DB) error

func WithDirPath(dir string) Option {
	return func(db *DB) error {
		if dir == "" {
			return errors.New("empty directory path")
		}
		db.dirPath = dir
		return nil
	}
}

func NewDB(option ...Option) (*DB, error) {
	db := DB{
		mu:         &sync.RWMutex{},
		dirPath:    "data",
		activeFile: nil,
		olderFiles: make(map[uint32]*data.File),
		index:      nil,
		wg:         &sync.WaitGroup{},
		stopCh:     make(chan struct{}),
	}
	for _, opt := range option {
		if err := opt(&db); err != nil {
			return nil, err
		}
	}
	if _, err := os.Stat(db.dirPath); os.IsNotExist(err) {
		if err = os.MkdirAll(db.dirPath, os.ModePerm); err != nil {
			return nil, err
		}
	} else {
		entries, err := os.ReadDir(db.dirPath)
		if err != nil {
			return nil, err
		}
		if len(entries) == 0 {
			if _, err = data.OpenDataFile(db.dirPath, 0, io.FIO); err != nil {
				return nil, err
			}
		}
	}
	db.index = index.NewSimMap()

	if err := db.loadDataFile(); err != nil {
		return nil, err
	}
	db.wg.Add(1)
	go db.mergeLoop()
	return &db, nil
}

func (db *DB) mergeLoop() {
	for {
		timer := time.NewTimer(time.Second * 30)
		select {
		case <-db.stopCh:
			db.wg.Done()
			return
		case <-timer.C:
			db.mu.RLock()
			if db.reclaimSize > 3*DataFileSize {
				db.mu.RUnlock()
				err := db.Merge()
				if err != nil {
					panic(err)
				}
			} else {
				db.mu.RUnlock()
			}
			timer.Reset(time.Second * 30)
		}
	}
}

func (db *DB) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	pos, err := db.appendLogRecordWithLock(&data.LogRecord{
		Key:   key,
		Value: value,
		Type:  data.LogRecordNormal,
	})
	if err != nil {
		return err
	}
	if oldPos := db.index.Put(key, pos); oldPos != nil {
		db.reclaimSize += int64(oldPos.Size)
	}
	return nil
}

func (db *DB) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	if pos := db.index.Get(key); pos == nil {
		return nil
	}
	logRecord := &data.LogRecord{
		Key:  key,
		Type: data.LogRecordDeleted,
	}
	pos, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return nil
	}
	db.reclaimSize += int64(pos.Size)
	oldPos, ok := db.index.Delete(key)
	if !ok {
		return ErrKeyNotFound
	}
	if oldPos != nil {
		db.reclaimSize += int64(oldPos.Size)
	}
	return nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}
	logRecordPos := db.index.Get(key)
	if logRecordPos == nil {
		return nil, ErrKeyNotFound
	}
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.getValueByPosition(logRecordPos)
}

func (db *DB) Sync() error {
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()

	return db.activeFile.Sync()
}

func (db *DB) Stat() *Stat {
	db.mu.RLock()
	defer db.mu.RUnlock()

	var dataFiles = uint(len(db.olderFiles))
	if db.activeFile != nil {
		dataFiles += 1
	}
	return &Stat{
		KeyNum:          uint(db.index.Size()),
		DataFileNum:     dataFiles,
		ReclaimableSize: db.reclaimSize,
	}
}
