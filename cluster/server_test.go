package cluster

import (
	"testing"
	"time"
)

func TestCluster(t *testing.T) {
	c := NewClusters(
		WithName("mm"),
		WithServers([]string{
			"127.0.0.1:3001",
			"127.0.0.1:3002",
			"127.0.0.1:3003",
		}),
	)
	c.Start()
	time.Sleep(time.Second * 10)
}
