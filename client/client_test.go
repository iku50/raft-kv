package client

import (
	"testing"
	"time"

	"github.com/go-playground/assert/v2"
)

func TestClient(t *testing.T) {
	lb := NewMap(3, nil)
	cli := NewClient(
		WithLoadBalance(&lb), WithClusters("one", "two"))
	cli.Start()
	time.Sleep(time.Second)
	err := cli.Put("mm", "ok")
	if err != nil {
		t.Error(err)
	}
	time.Sleep(time.Second)
	v, _ := cli.Get("mm")
	assert.Equal(t, v, "ok")
}
