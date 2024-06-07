package server

import (
	"fmt"
	"raft-kv/bitcask"
	"raft-kv/facade"
	"raft-kv/proto"
	"raft-kv/raft"
	"raft-kv/raft/rpc"
)

type Server struct {
	knownServers []string
	port         int
	me           int32
	address      string
	persister    *raft.Persister
	applyCh      chan raft.ApplyMsg
	raft         *raft.Raft
	db           *bitcask.DB
	killCh       chan bool
}

type SerOption func(s *Server)

func WithKnownServers(servers []string) SerOption {
	return func(s *Server) {
		s.knownServers = servers
	}
}

func WithPort(port int) SerOption {
	return func(s *Server) {
		s.port = port
	}
}

func WithAddress(address string) SerOption {
	return func(s *Server) {
		s.address = address
	}
}

func WithId(id int32) SerOption {
	return func(s *Server) {
		s.me = id
	}
}

func WithPersister(p *raft.Persister) SerOption {
	return func(s *Server) {
		s.persister = p
	}
}

func Default() SerOption {
	return func(s *Server) {
		s.address = "127.0.0.1"
		s.me = 1
		s.port = 5001
		s.persister = raft.MakePersister()
	}
}

func NewServer(opts ...SerOption) *Server {
	ch := make(chan raft.ApplyMsg)
	db, err := bitcask.NewDB(bitcask.WithDirPath("./data"))
	if err != nil {
		panic(err)
	}
	s := &Server{
		applyCh: ch,
		db:      db,
	}
	Default()(s)
	for _, opt := range opts {
		opt(s)
	}
	fmt.Println(s.me, s.address)
	return s
}

func (s *Server) IsLeader() bool {
	term, isLeader := s.raft.GetState()
	if 0 == term {
		return false
	}
	return isLeader
}

func (s *Server) apply(command bitcask.Command) (int64, int32, bool) {
	return s.raft.Start(&command)
}

func (s *Server) Get(key []byte) ([]byte, error) {
	return s.db.Get(key)
}

func (s *Server) Put(key []byte, value []byte) error {
	c := bitcask.Command{Op: bitcask.Put, Key: key, Value: value}
	_, _, p := s.apply(c)
	if !p {
		return fmt.Errorf("put failed")
	}
	return nil
}

func (s *Server) Delete(key []byte) error {
	c := bitcask.Command{Op: bitcask.Delete, Key: key}
	_, _, p := s.apply(c)
	if !p {
		return fmt.Errorf("delete failed")
	}
	return nil
}

func (s *Server) applyLoop() {
	for {
		select {
		case msg := <-s.applyCh:
			c := bitcask.Command{}
			c.FromBytes(msg.Command)
			fmt.Printf("Apply: %v\n", c)
			switch c.Op {
			case bitcask.Put:
				err := s.db.Put(c.Key, c.Value)
				if err != nil {
					panic(err)
				}
				fmt.Printf("Put: %v\n", c)
			case bitcask.Delete:
				err := s.db.Delete(c.Key)
				if err != nil {
					panic(err)
				}
				fmt.Printf("Delete: %v\n", c)
			case bitcask.Get:
				// don't need to do anything
			}
		case <-s.killCh:
			s.db.Close()
			return
		}
	}
}

func (s *Server) Start() {
	p := make([]proto.RaftClient, len(s.knownServers))
	for i, server := range s.knownServers {
		p[i] = facade.NewClient(server)
	}
	fmt.Println(p, s.me, s.address)
	s.raft = raft.Make(p, s.me, s.persister, s.applyCh)
	s.killCh = make(chan bool)
	okCh := make(chan bool)
	go rpc.Start(s.port, s.raft, s.killCh, okCh)
	<-okCh
	go s.applyLoop()
	go s.wait()
}

func (s *Server) wait() {
	<-s.killCh
	s.raft.Kill()
	close(s.applyCh)
	close(s.killCh)
}

func (s *Server) Close() {
	s.killCh <- true
}