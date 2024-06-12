package data

import (
	"raft-kv/bitcask/io"
	"testing"
)

func TestData(t *testing.T) {
	file := "test.db"
	fileId := 0
	db, err := OpenDataFile(file, uint32(fileId), io.MemoryMap)
	if err != nil {
		panic(err)
	}
	defer db.Close()
	record := LogRecord{
		Key:   []byte{1, 2, 3, 4},
		Value: []byte{4, 5, 6},
		Type:  LogRecordNormal,
	}
	p, size := EncodeLogRecord(&record)
	err = db.Write(p)
	if err != nil {
		return
	}
	l, size, err := db.ReadLogRecord(0)
	t.Log(l)

	t.Log(size)

}
