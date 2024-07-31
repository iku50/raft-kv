package index

import (
	"fmt"
	"raft-kv/bitcask/data"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/go-playground/assert/v2"
)

func TestMapTree(t *testing.T) {
	b := NewSimMap()
	m := map[string]*data.LogRecordIndex{}
	for i := 0; i < 30; i++ {
		m[fmt.Sprintf("%d", i)] = &data.LogRecordIndex{Fid: uint32(i)}
	}
	for k, v := range m {
		b.Put([]byte(k), v)
	}
	// println(b.root.String(0))
	data := [][]byte{
		[]byte("1"), []byte("2"), []byte("15"), []byte("18"),
	}
	for _, v := range data {
		assert.Equal(t, b.Get(v), m[string(v)])
	}
	for _, v := range data {
		b.Delete(v)
	}
	// println(b.root.String(0))

	for _, v := range data {
		assert.Equal(t, b.Get(v), nil)
	}
	fmt.Printf("%v", b.Scan())
}

func BenchmarkMapInsert(b *testing.B) {
	m := NewSimMap()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("%d", i)
		value := &data.LogRecordIndex{Fid: uint32(i)}
		m.Put([]byte(key), value)
	}
}

func BenchmarkMapTreeGet(b *testing.B) {
	mm := NewSimMap()
	m := map[string]*data.LogRecordIndex{}
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("%d", i)
		value := &data.LogRecordIndex{Fid: uint32(i)}
		m[key] = value
		mm.Put([]byte(key), value)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("%d", i%1000)
		mm.Get([]byte(key))
	}
}

func BenchmarkMapDelete(b *testing.B) {
	mm := NewSimMap()
	m := map[string]*data.LogRecordIndex{}
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("%d", i)
		value := &data.LogRecordIndex{Fid: uint32(i)}
		m[key] = value
		mm.Put([]byte(key), value)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("%d", i%1000)
		mm.Delete([]byte(key))
		assert.Equal(b, mm.Get([]byte(key)), nil)
	}
}

func TestMapPara(t *testing.T) {
	b := NewSimMap()
	wg := sync.WaitGroup{}
	N := 100000
	ch := make(chan int, 100)
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < N; j++ {
				ch <- j
			}
		}()
	}
	deleteCh := make(chan int, 10)
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case k := <-ch:
					value := data.LogRecordIndex{Fid: uint32(k)}
					b.Put([]byte(strconv.Itoa(k)), &value)
					v := b.Get([]byte(strconv.Itoa(k)))
					if v == nil {
						t.Error("get error")
						continue
					}
					if v.Fid != uint32(k) {
						t.Error("get error")
					}
					// deleteCh <- k
				case <-time.After(10 * time.Millisecond):
					return
				}

			}
		}()
	}
	for i := 0; i < 10; i++ {
		go func() {
			for {
				select {
				case k := <-deleteCh:
					b.Delete([]byte(strconv.Itoa(k)))
					v := b.Get([]byte(strconv.Itoa(k)))
					if v != nil {
						t.Error("delete error")
					}
				case <-time.After(10 * time.Millisecond):
					return
				}
			}
		}()
	}
	wg.Wait()
	time.Sleep(time.Second)

	close(ch)
}
