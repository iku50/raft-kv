package client

import (
	"raft-kv/cluster"
	"strconv"
)

type Option func(*Client)

func WithClusters(clusters ...string) Option {
	return func(c *Client) {
		for i, clu := range clusters {
			// todo: use config here
			ports := []int{3001 + i*3, 3002 + i*3, 3003 + i*3}
			addrs := []string{
				"127.0.0.1" + ":" + strconv.Itoa(ports[0]),
				"127.0.0.1" + ":" + strconv.Itoa(ports[1]),
				"127.0.0.1" + ":" + strconv.Itoa(ports[2]),
			}
			c.clusters = append(c.clusters, cluster.NewClusters(
				cluster.WithName(clu),
				cluster.WithServers(addrs),
			))
		}
	}
}

func WithLoadBalance(lb *LoadBalancer) Option {
	return func(c *Client) {
		c.loadBalancer = lb
	}
}
