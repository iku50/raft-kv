package infra

import "raft-kv/facade"

type Network struct {
	servers map[int]*Server
}

func NewNetwork() *Network {
	return &Network{}
}

func (n *Network) AddServer(id int, srv Server) {
	n.servers[id] = &srv
}

func (n *Network) LongDelays(yes bool) {

}

func (n *Network) DeleteServer(server int) {
	delete(n.servers, server)
}

func (n *Network) Enable(name string, yes bool) {
}

func (n *Network) Cleanup() {
}

func (n *Network) GetCount(id int) int {
	return 0
}

func (n *Network) GetTotalCount() int {
	return 0
}

func (n *Network) GetTotalBytes() int64 {
	return 0
}

func (n *Network) MakeClientEnd(name string) *facade.ClientEnd {
	return &facade.ClientEnd{}
}

func (n *Network) Connect(name string, server int) {
}

func (n *Network) Reliable(b bool) {

}

func (n *Network) LongReordering(longrel bool) {

}

type Service interface {
}

func MakeService(s any) Service {
	return nil
}

type Server struct {
}

func (s *Server) AddService(service Service) {
}

func MakeServer() Server {
	return Server{}
}
