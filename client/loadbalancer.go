package client

type LoadBalancer interface {
	Add(...string)
	Get(string) string
}
