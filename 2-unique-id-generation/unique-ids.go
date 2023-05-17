package main

import (
	"encoding/json"
	"fmt"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Server struct {
	n *maelstrom.Node

	counter int
}

func NewServer() *Server {
	return &Server{
		n: maelstrom.NewNode(),
	}
}

type GenerateInput struct {
	Type string `json:"type"`
}

type GenerateOutput struct {
	Type string `json:"type"`
	Id   string `json:"id"`
}

func (s *Server) handleGenerate(msg maelstrom.Message) error {
	var inputBody GenerateInput
	if err := json.Unmarshal(msg.Body, &inputBody); err != nil {
		return err
	}

	outputBody := GenerateOutput{
		Type: "generate_ok",
		Id:   fmt.Sprintf("%v_%d", s.n.ID(), s.counter),
	}
	s.counter++

	return s.n.Reply(msg, outputBody)
}

func main() {
	s := NewServer()

	s.n.Handle("generate", s.handleGenerate)

	if err := s.n.Run(); err != nil {
		log.Fatal(err)
	}
}
