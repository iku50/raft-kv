package client

import "raft-kv/cluster"

type Client struct {
	clusters     []*cluster.Cluster
	loadBalancer *LoadBalancer
}

func NewClient(opts ...Option) *Client {
	cli := &Client{}
	for _, opt := range opts {
		opt(cli)
	}
	return cli
}
