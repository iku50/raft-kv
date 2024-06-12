package bitcask

import (
	"bytes"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"
)

func TestNewDB(t *testing.T) {
	db, err := NewDB(
		WithDirPath("test_db"),
	)
	if err != nil {
		t.Error(err)
	}
	defer db.Close()
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

func randomKeyValue() ([]byte, []byte) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	key := make([]byte, 16)
	value := make([]byte, 128)
	r.Read(key)
	r.Read(value)
	return key, value
}

type benchmarkTestCase struct {
	name string
	size int
}

func BenchmarkGet(b *testing.B) {

	tests := []benchmarkTestCase{
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

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			b.SetBytes(int64(tt.size))
			db := initBitCask()
			key := []byte("foo")
			value := []byte(strings.Repeat(" ", tt.size))
			err := db.Put(key, value)
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

	tests := []benchmarkTestCase{
		{"128B", 128},
		{"256B", 256},
		{"1K", 1024},
		{"2K", 2048},
		{"4K", 4096},
		{"8K", 8192},
		{"16K", 16384},
		{"32K", 32768},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			b.SetBytes(int64(tt.size))
			db := initBitCask()
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

func initBitCask() BitCask {
	currentDir, err := os.Getwd()
	testdir, err := os.MkdirTemp(currentDir, "bitcask_bench")
	defer os.RemoveAll(testdir)
	db, err := NewDB(
		WithDirPath("testdb"),
	)
	if err != nil {
		panic(err)
	}
	return db
}
