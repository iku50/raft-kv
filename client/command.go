package client

func (c *Client) Get(key string) (value string, err error) {
	p := c.loadBalancer.Get(key)
	v, err := c.clusters[p].Get([]byte(key))
	return string(v), err
}

func (c *Client) Put(key string, value string) error {
	p := c.loadBalancer.Get(key)
	return c.clusters[p].Put([]byte(key), []byte(value))
}

func (c *Client) Delete(key string) error {
	p := c.loadBalancer.Get(key)
	return c.clusters[p].Delete([]byte(key))
}
