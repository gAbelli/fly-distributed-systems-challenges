package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Server struct {
	n *maelstrom.Node
}

func NewServer() *Server {
	return &Server{
		n: maelstrom.NewNode(),
	}
}

type EchoInput struct {
	Type string `json:"type"`
	Echo string `json:"echo"`
}

type EchoOutput struct {
	Type string `json:"type"`
	Echo string `json:"echo"`
}

func (s *Server) echoHandler(msg maelstrom.Message) error {
	var inputBody EchoInput
	if err := json.Unmarshal(msg.Body, &inputBody); err != nil {
		return err
	}

	outputBody := EchoOutput{
		Type: "echo_ok",
		Echo: inputBody.Echo,
	}
	return s.n.Reply(msg, outputBody)
}

func main() {
	s := NewServer()

	s.n.Handle("echo", s.echoHandler)

	if err := s.n.Run(); err != nil {
		log.Fatal(err)
	}
}
