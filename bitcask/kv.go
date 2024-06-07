package bitcask

import (
	"errors"
	"os"
	"raft-kv/bitcask/data"
	"raft-kv/bitcask/index"
	"raft-kv/bitcask/io"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// now it just a simple key-value store

type BitCask interface {
	Stat() *Stat

	Get(key []byte) (value []byte, err error)

	Put(key, value []byte) error

	Delete(key []byte) error

	// List() (key []string, err error)
	// Merge() error
	Sync() error

	Close()
}

type DB struct {
	mu          *sync.RWMutex
	fileIds     []int
	dirPath     string
	activeFile  *data.File
	olderFiles  map[uint32]*data.File
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
	ErrFileNotFound           = errors.New("key not found")
	ErrDataDirectoryCorrupted = errors.New("data directory corrupted")
)

func (db *DB) Close() {
	db.mu.Lock()
	defer db.mu.Unlock()
	err := db.index.Close()
	if err != nil {
		return
	}
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
		dirPath:    "",
		activeFile: nil,
		olderFiles: make(map[uint32]*data.File),
		index:      nil,
	}
	for _, opt := range option {
		if err := opt(&db); err != nil {
			return nil, err
		}
	}
	var isInitial bool
	if _, err := os.Stat(db.dirPath); os.IsNotExist(err) {
		isInitial = true
		if err = os.MkdirAll(db.dirPath, os.ModePerm); err != nil {
			return nil, err
		}
	} else {
		entries, err := os.ReadDir(db.dirPath)
		if err != nil {
			return nil, err
		}

		if len(entries) == 0 {
			isInitial = true
		}
	}
	if !isInitial {
		return nil, errors.New("not initial")
	}
	db.index = index.NewSimMap()
	if err := db.loadDataFile(); err != nil {
		return nil, err
	}
	return &db, nil
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
	db.mu.RLock()
	defer db.mu.RUnlock()

	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}
	logRecordPos := db.index.Get(key)
	if logRecordPos == nil {
		return nil, ErrKeyNotFound
	}
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

func (db *DB) getValueByPosition(logRecordPos *data.LogRecordIndex) ([]byte, error) {
	var dataFile *data.File
	if db.activeFile.FileId == logRecordPos.Fid {
		dataFile = db.activeFile
	} else {
		dataFile = db.olderFiles[logRecordPos.Fid]
	}
	if dataFile == nil {
		return nil, ErrFileNotFound
	}

	logRecord, _, err := dataFile.ReadLogRecord(logRecordPos.Offset)
	if err != nil {
		return nil, err
	}

	if logRecord.Type == data.LogRecordDeleted {
		return nil, ErrKeyNotFound
	}

	return logRecord.Value, nil
}

func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordIndex, error) {
	if db.activeFile == nil {
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	encRecord, size := data.EncodeLogRecord(logRecord)
	if db.activeFile.WriteOff+size > DataFileSize {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}

		db.olderFiles[db.activeFile.FileId] = db.activeFile

		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	writeOff := db.activeFile.WriteOff
	if err := db.activeFile.Write(encRecord); err != nil {
		return nil, err
	}

	db.bytesWrite += uint(size)

	if err := db.activeFile.Sync(); err != nil {
		return nil, err
	}
	if db.bytesWrite > 0 {
		db.bytesWrite = 0
	}

	// 构造内存索引信息
	pos := &data.LogRecordIndex{
		Fid:    db.activeFile.FileId,
		Offset: writeOff,
		Size:   uint32(size),
	}
	return pos, nil
}

func (db *DB) appendLogRecordWithLock(logRecord *data.LogRecord) (*data.LogRecordIndex, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.appendLogRecord(logRecord)
}

func (db *DB) setActiveDataFile() error {
	var initialField uint32 = 0
	if db.activeFile != nil {
		initialField = db.activeFile.FileId + 1
	}

	dataFile, err := data.OpenDataFile(db.dirPath, initialField, io.FIO)
	if err != nil {
		return err
	}
	db.activeFile = dataFile

	return nil
}

func (db *DB) loadDataFile() error {
	dirEntries, err := os.ReadDir(db.dirPath)
	if err != nil {
		return err
	}

	var fileIds []int
	for _, entry := range dirEntries {
		if strings.HasSuffix(entry.Name(), data.FileNameSuffix) {
			splitNames := strings.Split(entry.Name(), ".")
			fileId, err := strconv.Atoi(splitNames[0])
			if err != nil {
				return ErrDataDirectoryCorrupted
			}
			fileIds = append(fileIds, fileId)
		}
	}

	sort.Ints(fileIds)
	db.fileIds = fileIds

	for i, fid := range fileIds {
		ioType := io.FIO
		dataFile, err := data.OpenDataFile(db.dirPath, uint32(fid), ioType)
		if err != nil {
			return err
		}
		if i == len(fileIds)-1 {
			db.activeFile = dataFile
		} else {
			db.olderFiles[uint32(fid)] = dataFile
		}
	}
	return nil
}
