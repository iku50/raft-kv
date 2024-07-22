package server

import (
	"fmt"
	"raft-kv/bitcask"
	"raft-kv/bitcask/index"
	"raft-kv/raft"
	"raft-kv/raft/proto"
	"raft-kv/raft/rpc"
)

type Server struct {
	knownServers []proto.RaftClient
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
		s.knownServers = make([]proto.RaftClient, len(servers))
		for i := range servers {
			conn := rpc.NewClient(servers[i])
			s.knownServers[i] = conn
		}
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

func Default() *Server {
	return &Server{
		address:   "127.0.0.1",
		me:        1,
		applyCh:   make(chan raft.ApplyMsg),
		port:      5001,
		persister: raft.MakePersister("raftServer"),
		killCh:    make(chan bool),
	}
}

func (s *Server) AddKnownServers(servers []string) {
	for i := range servers {
		conn := rpc.NewClient(servers[i])
		s.knownServers = append(s.knownServers, conn)
	}
}

func NewServer(opts ...SerOption) *Server {
	s := Default()
	for _, opt := range opts {
		opt(s)
	}
	db, err := bitcask.NewDB(
		bitcask.WithDirPath("./data/"+fmt.Sprint(s.address)),
		bitcask.WithIndex(index.NewBPTree(32)),
	)
	if err != nil {
		panic(err)
	}
	s.db = db
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
	return s.raft.Do(&command)
}

func (s *Server) Get(key []byte) ([]byte, error) {
	_, isLeader := s.raft.GetState()
	if !isLeader {
		return nil, fmt.Errorf("not leader")
	}
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
			if msg.Command == nil {
				return
			}
			c := bitcask.Command{}
			c.FromBytes(msg.Command)
			fmt.Printf("Apply: %v\n", c)
			switch c.Op {
			case bitcask.Put:
				err := s.db.Put(c.Key, c.Value)
				if err != nil {
					panic(err)
				}
				fmt.Printf("PUT: %v\n", c)
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
	s.raft = raft.Make(s.knownServers, s.me, s.persister, s.applyCh)
	s.killCh = make(chan bool)
	okCh := make(chan bool)
	go rpc.Start(s.port, s.raft, s.killCh, okCh)
	<-okCh
	go s.applyLoop()
	go s.wait()
}

func (s *Server) AddKnownServer(address string) {

	s.knownServers = append(s.knownServers)
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
