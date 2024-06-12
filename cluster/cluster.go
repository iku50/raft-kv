package cluster

import (
	"errors"
	"raft-kv/server"
	"strconv"
	"strings"
)

type Cluster struct {
	Name    string
	Servers []*server.Server
}

type Option func(*Cluster)

func NewClusters(opts ...Option) *Cluster {
	clu := Cluster{
		Servers: make([]*server.Server, 0),
	}
	for _, opt := range opts {
		opt(&clu)
	}
	return &clu
}

func WithName(name string) Option {
	return func(clu *Cluster) {
		clu.Name = name
	}
}

func WithServers(servers []string) Option {
	return func(clu *Cluster) {
		serves := make([]*server.Server, 0)
		for i := 0; i < len(servers); i++ {
			portStr := strings.Split(servers[i], ":")[1]
			port, _ := strconv.Atoi(portStr)
			ser := server.NewServer(
				server.WithKnownServers(servers),
				server.WithPort(port),
				server.WithAddress(servers[i]),
				server.WithId(int32(i)),
			)
			serves = append(serves, ser)
		}
		clu.Servers = serves
	}
}

func (c *Cluster) AddServer(server string) {

}
func (c *Cluster) RemoveServer(server string) {

}

func (c *Cluster) GetLeader() *server.Server {
	for i := 0; i < len(c.Servers); i++ {
		if c.Servers[i].IsLeader() {
			return c.Servers[i]
		}
	}
	return nil
}

func (c *Cluster) Get(key []byte) ([]byte, error) {
	leader := c.GetLeader()
	if leader == nil {
		return nil, errors.New("no leader")
	}
	return leader.Get(key)
}
func (c *Cluster) Put(key []byte, value []byte) error {
	leader := c.GetLeader()
	if leader == nil {
		return errors.New("no leader")
	}
	return leader.Put(key, value)
}
func (c *Cluster) Delete(key []byte) error {
	leader := c.GetLeader()
	if leader == nil {
		return errors.New("no leader")
	}
	return leader.Delete(key)
}

func (c *Cluster) Start() {
	for i := 0; i < len(c.Servers); i++ {
		go c.Servers[i].Start()
	}
}
