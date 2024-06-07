package bitcask

import (
	"bytes"
	"encoding/gob"
)

type Op uint32

const (
	Put Op = iota
	Get
	Delete
)

type Command struct {
	Op    Op
	Key   []byte
	Value []byte
}

func (c *Command) ToBytes() []byte {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(c)
	if err != nil {
		panic(err)
	}
	return buf.Bytes()
}

func (c *Command) FromBytes(b []byte) {
	var command Command
	dec := gob.NewDecoder(bytes.NewBuffer(b))
	err := dec.Decode(&command)
	if err != nil {
		panic(err)
	}
	c.Key = command.Key
	c.Value = command.Value
	c.Op = command.Op
}
