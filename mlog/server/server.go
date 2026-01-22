package server

import "context"

type Server struct {
	Port int
}

func NewServer(port int) *Server {
	return &Server{
		Port: port,
	}
}

func (s *Server) Start(ctx context.Context) error {

}
