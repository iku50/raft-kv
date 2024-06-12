package main

import "raft-kv/client"

func main() {
	// TODO: add parse args
	// TODO: add config
	lb := client.NewMap(3, nil)
	cli := client.NewClient(
		client.WithClusters("region", "country"),
		client.WithLoadBalance(&lb),
	)
	cli.Start()
}
