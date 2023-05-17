package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Server struct {
	n  *maelstrom.Node
	kv *maelstrom.KV

	counter   int
	counterMu sync.Mutex
}

func NewServer() *Server {
	n := maelstrom.NewNode()
	return &Server{
		n:  n,
		kv: maelstrom.NewSeqKV(n),
	}
}

type AddInput struct {
	Type  string `json:"type"`
	Delta int    `json:"delta"`
}

type AddOutput struct {
	Type string `json:"type"`
}

func (s *Server) addHandler(msg maelstrom.Message) error {
	var inputBody AddInput
	if err := json.Unmarshal(msg.Body, &inputBody); err != nil {
		return err
	}
	s.counterMu.Lock()
	defer s.counterMu.Unlock()
	ctx := context.Background()
	err := s.kv.CompareAndSwap(ctx, s.n.ID(), s.counter, s.counter+inputBody.Delta, true)
	if err != nil {
		return err
	}
	s.counter += inputBody.Delta

	outputBody := AddOutput{
		Type: "add_ok",
	}
	return s.n.Reply(msg, outputBody)
}

type ReadInput struct {
	Type string `json:"type"`
}

type ReadOutput struct {
	Type  string `json:"type"`
	Value int    `json:"value"`
}

func (s *Server) readHandler(msg maelstrom.Message) error {
	var inputBody ReadInput
	if err := json.Unmarshal(msg.Body, &inputBody); err != nil {
		return err
	}
	total := make(chan int, len(s.n.NodeIDs()))
	total <- s.counter

	for _, id := range s.n.NodeIDs() {
		if id != s.n.ID() {
			id := id
			go func() {
				val, err := s.kv.ReadInt(context.Background(), id)
				if err != nil {
					val = 0
				}
				total <- val
			}()
		}
	}

	value := 0
	for range s.n.NodeIDs() {
		value += <-total
	}

	outputBody := ReadOutput{
		Type:  "read_ok",
		Value: value,
	}
	return s.n.Reply(msg, outputBody)
}

func main() {
	s := NewServer()

	s.n.Handle("add", s.addHandler)
	s.n.Handle("read", s.readHandler)

	if err := s.n.Run(); err != nil {
		log.Fatal(err)
	}
}
