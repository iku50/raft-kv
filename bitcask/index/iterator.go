package index

import "raft-kv/bitcask/data"

type Iterator interface {
	Rewind()
	Seek(key []byte)
	Next()
	Valid() bool
	Key() []byte
	Value() *data.LogRecordIndex
	Close()
}
