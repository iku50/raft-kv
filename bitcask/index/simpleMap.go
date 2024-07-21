package index

import (
	"raft-kv/bitcask/data"
	"sync"
)

type SimMap struct {
	m *sync.Map
}

func (s SimMap) Put(key []byte, pos *data.LogRecordIndex) *data.LogRecordIndex {
	// trans bytes to string
	sk := string(key)
	s.m.Store(sk, pos)
	return pos
}

func (s SimMap) Get(key []byte) *data.LogRecordIndex {
	// trans bytes to string
	sk := string(key)
	pos, _ := s.m.Load(sk)
	if pos == nil {
		return nil
	}
	return pos.(*data.LogRecordIndex)
}

func (s SimMap) Delete(key []byte) (*data.LogRecordIndex, bool) {
	// trans bytes to string
	sk := string(key)
	pos, ok := s.m.Load(sk)
	if !ok {
		return nil, false
	}
	s.m.Delete(sk)
	return pos.(*data.LogRecordIndex), true
}

func (s SimMap) Scan() map[string]*data.LogRecordIndex {
	pos := make(map[string]*data.LogRecordIndex)
	s.m.Range(func(key, value interface{}) bool {
		pos[key.(string)] = value.(*data.LogRecordIndex)
		return true
	})
	return pos
}

func (s SimMap) Iterator(reverse bool) Iterator {
	// we just don't support this
	return nil
}

func (s SimMap) Size() int {
	// we just don't support this
	return 0
}

func (s SimMap) Close() error {
	// do nothing
	return nil
}

func NewSimMap() *SimMap {
	return &SimMap{
		m: &sync.Map{},
	}
}
