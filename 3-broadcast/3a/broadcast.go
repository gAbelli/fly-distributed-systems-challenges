package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Server struct {
	n         *maelstrom.Node
	neighbors []string

	msgs map[int]bool
}

func NewServer() *Server {
	return &Server{
		n:    maelstrom.NewNode(),
		msgs: make(map[int]bool),
	}
}

type BroadcastInput struct {
	Type    string `json:"type"`
	Message int    `json:"message"`
}

type BroadcastOutput struct {
	Type string `json:"type"`
}

func (s *Server) broadcastHandler(msg maelstrom.Message) error {
	var inputBody BroadcastInput
	if err := json.Unmarshal(msg.Body, &inputBody); err != nil {
		return err
	}
	s.msgs[inputBody.Message] = true

	outputBody := BroadcastOutput{
		Type: "broadcast_ok",
	}
	return s.n.Reply(msg, outputBody)
}

type ReadInput struct {
	Type string `json:"type"`
}

type ReadOutput struct {
	Type     string `json:"type"`
	Messages []int  `json:"messages"`
}

func (s *Server) readHandler(msg maelstrom.Message) error {
	var inputBody ReadInput
	if err := json.Unmarshal(msg.Body, &inputBody); err != nil {
		return err
	}

	messages := []int{}
	for msg := range s.msgs {
		messages = append(messages, msg)
	}

	outputBody := ReadOutput{
		Type:     "read_ok",
		Messages: messages,
	}
	return s.n.Reply(msg, outputBody)
}

type TopologyInput struct {
	Type     string              `json:"type"`
	Topology map[string][]string `json:"topology"`
}

type TopologyOutput struct {
	Type string `json:"type"`
}

func (s *Server) topologyHandler(msg maelstrom.Message) error {
	var inputBody TopologyInput
	if err := json.Unmarshal(msg.Body, &inputBody); err != nil {
		return err
	}
	s.neighbors = inputBody.Topology[s.n.ID()]

	outputBody := TopologyOutput{
		Type: "topology_ok",
	}
	return s.n.Reply(msg, outputBody)
}

func main() {
	s := NewServer()

	s.n.Handle("broadcast", s.broadcastHandler)
	s.n.Handle("read", s.readHandler)
	s.n.Handle("topology", s.topologyHandler)

	if err := s.n.Run(); err != nil {
		log.Fatal(err)
	}
}
