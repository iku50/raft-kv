package main

import "raft-kv/client"

func main() {
	// TODO: add parse args
	// TODO: add config
	lb := client.LoadBalancer(&client.Map{})

	cli := client.NewClient(
		client.WithClusters("region", "country"),
		client.WithLoadBalance(&lb),
	)

}
