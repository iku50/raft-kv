package client

import (
	"fmt"
	"raft-kv/cluster"
)

type Client struct {
	clusters     map[string]*cluster.Cluster
	loadBalancer LoadBalancer
}

func NewClient(opts ...Option) *Client {
	cli := &Client{
		clusters: make(map[string]*cluster.Cluster),
	}
	for _, opt := range opts {
		opt(cli)
	}
	if cli.loadBalancer != nil {
		for _, c := range cli.clusters {
			cli.loadBalancer.Add(c.Name)
		}
	}
	return cli
}

func (c *Client) Start() {
	for _, s := range c.clusters {
		fmt.Println(s.Name, "start")
		s.Start()
	}
}
