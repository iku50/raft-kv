package server

import (
	"fmt"
	"testing"
	"time"
)

func TestInit(t *testing.T) {
	servers := 3
	addrs := []string{
		"127.0.0.1:9991",
		"127.0.0.1:9992",
		"127.0.0.1:9993",
	}
	ports := []int{
		9991,
		9992,
		9993,
	}
	serves := make([]*Server, servers)
	for i := 0; i < servers; i++ {
		p := make([]string, 0)
		for j := 0; j < servers; j++ {
			if j != i {
				p = append(p, addrs[j])
			}
		}
		ser := NewServer(
			WithKnownServers(p),
			WithPort(ports[i]),
			WithAddress(addrs[i]),
			WithId(int32(i)),
		)
		serves[i] = ser
		ser.Start()
	}

	time.Sleep(5000 * time.Millisecond)
	leader := GetLeader(serves)
	fmt.Printf(">>> Leader: %v\n", leader)
	if leader == -1 {
		t.Fatalf("should have a leader")
	}
	str := "hello"
	err := serves[leader].Put([]byte{123, 123}, []byte(str))
	if err != nil {
		return
	}
	time.Sleep(1000 * time.Millisecond)
	for i := 0; i < servers; i++ {
		value, err := serves[i].Get([]byte{123, 123})
		if err != nil {
			t.Fatalf("Get failed")
		} else {
			fmt.Printf(">>> %v: %s\n", i, value)
		}
	}
	time.Sleep(5000 * time.Millisecond)
	// test delete
	err = serves[leader].Delete([]byte{123, 123})
	if err != nil {
		t.Fatalf("Delete failed")
	}
	time.Sleep(5000 * time.Millisecond)
	for i := 0; i < servers; i++ {
		_, err := serves[i].Get([]byte{123, 123})
		if err != nil {
			t.Log("ok")
		} else {
			t.Fatalf("should be delete")
		}
	}
}

func GetLeader(servers []*Server) int {
	for i := 0; i < len(servers); i++ {
		if servers[i].IsLeader() == true {
			return i
		}
	}
	return -1
}
