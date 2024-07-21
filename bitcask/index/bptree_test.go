package index

import (
	"fmt"
	"raft-kv/bitcask/data"
	"testing"
)

func TestBPTree_Put(t *testing.T) {
	b := NewBPTree(4)
	b.Put([]byte("1"), &data.LogRecordIndex{Fid: 1})
	b.Put([]byte("3"), &data.LogRecordIndex{Fid: 3})
	b.Put([]byte("8"), &data.LogRecordIndex{Fid: 8})
	b.Put([]byte("4"), &data.LogRecordIndex{Fid: 4})
	b.Put([]byte("2"), &data.LogRecordIndex{Fid: 2})
	b.Put([]byte("9"), &data.LogRecordIndex{Fid: 9})
	b.Put([]byte("5"), &data.LogRecordIndex{Fid: 5})
	b.Put([]byte("6"), &data.LogRecordIndex{Fid: 6})
	b.Put([]byte("7"), &data.LogRecordIndex{Fid: 7})
	b.Put([]byte("10"), &data.LogRecordIndex{Fid: 10})
	b.Put([]byte("11"), &data.LogRecordIndex{Fid: 11})
	b.Put([]byte("12"), &data.LogRecordIndex{Fid: 12})
	b.Put([]byte("13"), &data.LogRecordIndex{Fid: 13})
	b.Put([]byte("14"), &data.LogRecordIndex{Fid: 14})
	b.Put([]byte("15"), &data.LogRecordIndex{Fid: 15})
	b.Put([]byte("16"), &data.LogRecordIndex{Fid: 16})
	println(b.root.String(0))
	fmt.Println(b.Get([]byte("1")).Fid)
	fmt.Println(b.Get([]byte("3")).Fid)
}
