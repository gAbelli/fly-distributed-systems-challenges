package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Server struct {
	n *maelstrom.Node

	neighbors   []string
	neighborsMu sync.Mutex

	msgs   map[int]bool
	msgsMu sync.RWMutex
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
	s.msgsMu.Lock()
	s.msgs[inputBody.Message] = true
	s.msgsMu.Unlock()

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
	s.msgsMu.RLock()
	for msg := range s.msgs {
		messages = append(messages, msg)
	}
	s.msgsMu.RUnlock()

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
	s.neighborsMu.Lock()
	s.neighbors = inputBody.Topology[s.n.ID()]
	s.neighborsMu.Unlock()

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
