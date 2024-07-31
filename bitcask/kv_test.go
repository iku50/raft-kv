package bitcask

import (
	"bytes"
	"math/rand"
	"os"
	"raft-kv/bitcask/index"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-playground/assert/v2"
)

func TestNewDB(t *testing.T) {
	currentDir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	testDir, err := os.MkdirTemp(currentDir, "bitcask_test")
	defer os.RemoveAll(testDir)
	db := initBitCask(testDir)
	key := []byte("hello")
	value := []byte("world")
	err = db.Put(key, value)
	if err != nil {
		t.Error(err)
	}
	val, err := db.Get(key)
	if string(val) != "world" {
		t.Errorf("%s != %s", val, "world")
	}
}

func TestDB_Merge(t *testing.T) {
	currentDir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	testDir, err := os.MkdirTemp(currentDir, "bitcask_test")
	defer os.RemoveAll(testDir)
	db := initBitCask(testDir)
	ch := make(chan struct{})
	go func() {
		for {
			select {
			case <-ch:
				return
			default:
				err := db.Merge()
				if err != nil {
					t.Error(err)
				}
			}
		}
	}()
	key := make([]byte, 10)
	value := make([]byte, 1024*1024)
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	r.Read(key)
	r.Read(value)
	for i := 0; i < 2048; i++ {
		err := db.Put(key, value)
		if err != nil {
			t.Error(err)
		}
	}
	val, err := db.Get(key)
	assert.Equal(t, err, nil)
	assert.Equal(t, string(val), string(value))
	ch <- struct{}{}
}

type benchmarkTestCase struct {
	name string
	size int
}

var tests = []benchmarkTestCase{
	{"128B", 128},
	{"256B", 256},
	{"512B", 512},
	{"1K", 1024},
	{"2K", 2048},
	{"4K", 4096},
	{"8K", 8192},
	{"16K", 16384},
	{"32K", 32768},
}

func BenchmarkGet(b *testing.B) {
	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			b.SetBytes(int64(tt.size))
			currentDir, err := os.Getwd()
			if err != nil {
				b.Fatal(err)
			}
			testDir, err := os.MkdirTemp(currentDir, "bitcask_test")
			defer os.RemoveAll(testDir)
			db := initBitCask(testDir)
			key := []byte("foo")
			value := []byte(strings.Repeat(" ", tt.size))
			err = db.Put(key, value)
			if err != nil {
				b.Fatal(err)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				val, err := db.Get(key)
				if err != nil {
					b.Fatal(err)
				}
				if !bytes.Equal(val, value) {
					b.Errorf("unexpected value")
				}
			}
			b.StopTimer()
			db.Close()
		})
	}
}

func BenchmarkPut(b *testing.B) {
	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			b.SetBytes(int64(tt.size))
			currentDir, err := os.Getwd()
			if err != nil {
				b.Fatal(err)
			}
			testDir, err := os.MkdirTemp(currentDir, "bitcask_test")
			defer os.RemoveAll(testDir)
			db := initBitCask(testDir)
			key := []byte("foo")
			value := []byte(strings.Repeat(" ", tt.size))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				err := db.Put(key, value)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkDB_Para(b *testing.B) {
	currentDir, err := os.Getwd()
	if err != nil {
		b.Error(err)
	}
	testDir, err := os.MkdirTemp(currentDir, "bitcask_test")
	defer os.RemoveAll(testDir)
	db := initBitCask(testDir)
	// rand 20k key
	value := make([]byte, 1024*10)
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	r.Read(value)
	ch := make(chan []byte, 100)
	ok := make(chan struct{})
	go func() {
		for i := 0; i < b.N; i++ {
			key := make([]byte, 100)
			r := rand.New(rand.NewSource(time.Now().UnixNano()))
			r.Read(key)
			ch <- key
		}
		close(ok)
	}()

	b.ResetTimer()
	wg := sync.WaitGroup{}
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for k := range ch {
				err := db.Put(k, value)
				if err != nil {
					b.Error(err)
				}
				v, err := db.Get(k)
				if err != nil {
					b.Error(err)
				}
				assert.Equal(b, v, value)
			}
		}()
	}
	<-ok
	close(ch)
	wg.Wait()
}

func initBitCask(testDir string) BitCask {
	db, err := NewDB(
		WithDirPath(testDir),
		WithIndex(index.NewBPTree(32)),
	)
	if err != nil {
		panic(err)
	}
	return db
}
