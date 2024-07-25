package bitcask

import (
	"os"
	"raft-kv/bitcask/data"
	"raft-kv/bitcask/io"
	"sort"
	"strconv"
	"strings"
)

func (db *DB) setActiveDataFile() error {
	var initialField uint32 = 0
	if db.activeFile != nil {
		initialField = db.activeFile.FileId + 1
		db.fileIds = append(db.fileIds, int(initialField))
	}

	dataFile, err := data.OpenDataFile(db.dirPath, initialField, io.MemoryMap)
	if err != nil {
		return err
	}
	db.activeFile = dataFile

	return nil
}

func (db *DB) loadDataFile() error {
	db.mu.Lock()
	defer db.mu.Unlock()
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
		var dataFile *data.File
		if i == len(fileIds)-1 {
			dataFile, err = data.OpenDataFile(db.dirPath, uint32(fid), io.MemoryMap)
			if err != nil {
				return err
			}
			db.activeFile = dataFile
		} else {
			dataFile, err = data.OpenDataFile(db.dirPath, uint32(fid), io.FIO)
			if err != nil {
				return err
			}
			db.olderFiles[uint32(fid)] = dataFile
		}
		db.reclaimSize += dataFile.WriteOff
		offset := int64(0)
		for offset < dataFile.WriteOff {
			record, l, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				return err
			}
			recordIndex := &data.LogRecordIndex{
				Fid:    dataFile.FileId,
				Offset: offset,
				Size:   uint32(l),
			}
			db.index.Put(record.Key, recordIndex)
			offset += l
		}
	}
	return nil
}

func (db *DB) getValueByPosition(logRecordPos *data.LogRecordIndex) ([]byte, error) {
	var dataFile *data.File
	if db.activeFile.FileId == logRecordPos.Fid {
		db.mu.RLock()
		defer db.mu.RUnlock()
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
		var err error
		activeFileId := db.activeFile.FileId
		if err = db.activeFile.Close(); err != nil {
			return nil, err
		}
		db.olderFiles[activeFileId], err = data.OpenDataFile(db.dirPath, activeFileId, io.FIO)
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
