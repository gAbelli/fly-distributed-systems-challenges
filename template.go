package main

import maelstrom "github.com/jepsen-io/maelstrom/demo/go"

type Server struct {
	n *maelstrom.Node
}

func NewServer() *Server {
	return &Server{
		n: maelstrom.NewNode(),
	}
}

func main() {
	s := NewServer()
}
