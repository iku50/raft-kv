package index

import (
	"fmt"
	"raft-kv/bitcask/data"
	"testing"

	"github.com/go-playground/assert/v2"
)

func TestBPTree(t *testing.T) {
	b := NewBPTree(4)
	m := map[string]*data.LogRecordIndex{}
	for i := 0; i < 30; i++ {
		m[fmt.Sprintf("%d", i)] = &data.LogRecordIndex{Fid: uint32(i)}
	}
	for k, v := range m {
		b.Put([]byte(k), v)
	}
	println(b.root.String(0))
	data := [][]byte{
		[]byte("1"), []byte("2"), []byte("15"), []byte("18"),
	}
	for _, v := range data {
		assert.Equal(t, b.Get(v), m[string(v)])
	}
	for _, v := range data {
		b.Delete(v)
	}
	println(b.root.String(0))

	for _, v := range data {
		assert.Equal(t, b.Get(v), nil)
	}
	fmt.Printf("%v", b.Scan())
}

func BenchmarkBPTreeInsert(b *testing.B) {
	tree := NewBPTree(32)
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("%d", i)
		value := &data.LogRecordIndex{Fid: uint32(i)}
		tree.Put([]byte(key), value)
	}
}

func BenchmarkBPTreeGet(b *testing.B) {
	tree := NewBPTree(32)
	m := map[string]*data.LogRecordIndex{}
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("%d", i)
		value := &data.LogRecordIndex{Fid: uint32(i)}
		m[key] = value
		tree.Put([]byte(key), value)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("%d", i%1000)
		tree.Get([]byte(key))
	}
}

func BenchmarkBPTreeDelete(b *testing.B) {
	tree := NewBPTree(32)
	m := map[string]*data.LogRecordIndex{}
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("%d", i)
		value := &data.LogRecordIndex{Fid: uint32(i)}
		m[key] = value
		tree.Put([]byte(key), value)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("%d", i%1000)
		tree.Delete([]byte(key))
	}
}
