package rpc

type ClientEnd struct {
}

func (c *ClientEnd) Call(method string, args interface{}, reply interface{}) bool {
	return false
}
