package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Server struct {
	n *maelstrom.Node

	logs             map[string][]int
	committedOffsets map[string]int
	logsMu           sync.RWMutex
}

func NewServer() *Server {
	return &Server{
		n:                maelstrom.NewNode(),
		logs:             make(map[string][]int),
		committedOffsets: make(map[string]int),
	}
}

type SendInput struct {
	Type string `json:"type"`
	Key  string `json:"key"`
	Msg  int    `json:"msg"`
}

type SendOutput struct {
	Type   string `json:"type"`
	Offset int    `json:"offset"`
}

func (s *Server) sendHandler(msg maelstrom.Message) error {
	var inputBody SendInput
	if err := json.Unmarshal(msg.Body, &inputBody); err != nil {
		return err
	}
	s.logsMu.Lock()
	offset := len(s.logs[inputBody.Key])
	if _, ok := s.committedOffsets[inputBody.Key]; !ok {
		s.committedOffsets[inputBody.Key] = -1
	}
	s.logs[inputBody.Key] = append(s.logs[inputBody.Key], inputBody.Msg)
	s.logsMu.Unlock()

	outputBody := SendOutput{
		Type:   "send_ok",
		Offset: offset,
	}
	return s.n.Reply(msg, outputBody)
}

type PollInput struct {
	Type    string         `json:"type"`
	Offsets map[string]int `json:"offsets"`
}

type PollOutput struct {
	Type string              `json:"type"`
	Msgs map[string][][2]int `json:"msgs"`
}

func (s *Server) pollHandler(msg maelstrom.Message) error {
	var inputBody PollInput
	if err := json.Unmarshal(msg.Body, &inputBody); err != nil {
		return err
	}
	res := make(map[string][][2]int)
	s.logsMu.RLock()
	for key, offset := range inputBody.Offsets {
		log := s.logs[key]
		i := offset
		for i < len(log) {
			res[key] = append(res[key], [2]int{i, log[i]})
			i++
		}
	}
	s.logsMu.RUnlock()

	outputBody := PollOutput{
		Type: "poll_ok",
		Msgs: res,
	}
	return s.n.Reply(msg, outputBody)
}

type CommitOffsetsInput struct {
	Type    string         `json:"type"`
	Offsets map[string]int `json:"offsets"`
}

type CommitOffsetsOutput struct {
	Type string `json:"type"`
}

func (s *Server) commitOffsetsHandler(msg maelstrom.Message) error {
	var inputBody CommitOffsetsInput
	if err := json.Unmarshal(msg.Body, &inputBody); err != nil {
		return err
	}
	s.logsMu.Lock()
	for key, offset := range inputBody.Offsets {
		s.committedOffsets[key] = offset
	}
	s.logsMu.Unlock()

	outputBody := CommitOffsetsOutput{
		Type: "commit_offsets_ok",
	}
	return s.n.Reply(msg, outputBody)
}

type ListCommittedOffsetsInput struct {
	Type string   `json:"type"`
	Keys []string `json:"keys"`
}

type ListCommittedOffsetsOutput struct {
	Type    string         `json:"type"`
	Offsets map[string]int `json:"offsets"`
}

func (s *Server) listCommittedOffsetsHandler(msg maelstrom.Message) error {
	var inputBody ListCommittedOffsetsInput
	if err := json.Unmarshal(msg.Body, &inputBody); err != nil {
		return err
	}
	res := make(map[string]int)
	s.logsMu.RLock()
	for _, key := range inputBody.Keys {
		if _, ok := s.committedOffsets[key]; ok {
			res[key] = s.committedOffsets[key]
		}
	}
	s.logsMu.RUnlock()

	outputBody := ListCommittedOffsetsOutput{
		Type:    "list_committed_offsets_ok",
		Offsets: res,
	}
	return s.n.Reply(msg, outputBody)
}

func main() {
	s := NewServer()

	s.n.Handle("send", s.sendHandler)
	s.n.Handle("poll", s.pollHandler)
	s.n.Handle("commit_offsets", s.commitOffsetsHandler)
	s.n.Handle("list_committed_offsets", s.listCommittedOffsetsHandler)

	if err := s.n.Run(); err != nil {
		log.Fatal(err)
	}
}
