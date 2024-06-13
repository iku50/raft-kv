package index

import "raft-kv/bitcask/data"

type Indexer interface {
	Put(key []byte, pos *data.LogRecordIndex) *data.LogRecordIndex
	Get(key []byte) *data.LogRecordIndex
	Delete(key []byte) (*data.LogRecordIndex, bool)
	Scan() map[string]*data.LogRecordIndex
	Iterator(reverse bool) Iterator
	Size() int
	Close() error
}

type Type uint8
