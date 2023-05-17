package main

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const (
	SYNC_TIMEOUT = 200 * time.Millisecond
)

type Server struct {
	n *maelstrom.Node

	neighbors     []string
	neighborsMsgs map[string]map[int]bool
	neighborsMu   sync.RWMutex

	msgs   map[int]bool
	msgsMu sync.RWMutex
}

func NewServer() *Server {
	return &Server{
		n:             maelstrom.NewNode(),
		msgs:          make(map[int]bool),
		neighborsMsgs: make(map[string]map[int]bool),
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

	s.msgsMu.RLock()
	messages := []int{}
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
	for _, neighbor := range s.neighbors {
		if _, ok := s.neighborsMsgs[neighbor]; !ok {
			s.neighborsMsgs[neighbor] = make(map[int]bool)
		}
	}
	s.neighborsMu.Unlock()

	outputBody := TopologyOutput{
		Type: "topology_ok",
	}
	return s.n.Reply(msg, outputBody)
}

type SyncInput struct {
	Type     string `json:"type"`
	Messages []int  `json:"messages"`
}

type SyncOutput struct {
	Type string `json:"type"`
}

func (s *Server) syncHandler(msg maelstrom.Message) error {
	var inputBody SyncInput
	if err := json.Unmarshal(msg.Body, &inputBody); err != nil {
		return err
	}

	s.msgsMu.Lock()
	for _, msg := range inputBody.Messages {
		s.msgs[msg] = true
	}
	s.msgsMu.Unlock()

	outputBody := SyncOutput{
		Type: "sync_ok",
	}
	return s.n.Reply(msg, outputBody)
}

func main() {
	s := NewServer()

	s.n.Handle("broadcast", s.broadcastHandler)
	s.n.Handle("read", s.readHandler)
	s.n.Handle("topology", s.topologyHandler)
	s.n.Handle("sync", s.syncHandler)

	done := make(chan struct{})
	// Every SYNC_TIMEOUT milliseconds, we call the sync rpc on all neighbors.
	// If there are no errors, we update the set of known messages
	// for that neighbor
	go func() {
		t := time.NewTicker(SYNC_TIMEOUT)
		for {
			select {
			case <-t.C:
				s.neighborsMu.RLock()
				for _, neighbor := range s.neighbors {
					neighbor := neighbor
					newMsgs := []int{}
					s.msgsMu.RLock()
					for msg := range s.msgs {
						if !s.neighborsMsgs[neighbor][msg] {
							newMsgs = append(newMsgs, msg)
						}
					}
					s.msgsMu.RUnlock()

					body := SyncInput{
						Type:     "sync",
						Messages: newMsgs,
					}
					s.n.RPC(neighbor, body, func(msg maelstrom.Message) error {
						s.neighborsMu.Lock()
						defer s.neighborsMu.Unlock()
						if msg.RPCError() != nil {
							return msg.RPCError()
						}
						for _, newMsg := range newMsgs {
							s.neighborsMsgs[neighbor][newMsg] = true
						}
						return nil
					})
				}
				s.neighborsMu.RUnlock()

			case <-done:
				return
			}
		}
	}()

	err := s.n.Run()
	close(done)
	if err != nil {
		log.Fatal(err)
	}
}
