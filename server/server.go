package server

type Server struct {
	KnownServers []string
	Port         int
	Name         string
	Address      string
}

func NewServer(name string, address string) *Server {
	return &Server{Name: name, Address: address}
}

func (s *Server) Start() {

}
