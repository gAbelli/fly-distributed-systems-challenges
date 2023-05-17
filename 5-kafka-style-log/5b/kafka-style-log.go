package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Server struct {
	n  *maelstrom.Node
	kv *maelstrom.KV
}

func NewServer() *Server {
	n := maelstrom.NewNode()
	return &Server{
		n:  n,
		kv: maelstrom.NewLinKV(n),
	}
}

func (s *Server) getResponsibleServer(key string) string {
	hash := int(sha256.Sum256([]byte(key))[0])
	return s.n.NodeIDs()[hash%len(s.n.NodeIDs())]
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
	id := s.getResponsibleServer(inputBody.Key)
	if id != s.n.ID() {
		forwardBody := ForwardInput{
			Type: "forward",
			Key:  inputBody.Key,
			Msg:  inputBody.Msg,
		}
		response, err := s.n.SyncRPC(context.Background(), id, forwardBody)
		if err != nil {
			return err
		}
		var forwardOutput ForwardOutput
		if err := json.Unmarshal(response.Body, &forwardOutput); err != nil {
			return err
		}
		outputBody := SendOutput{
			Type:   "send_ok",
			Offset: forwardOutput.Offset,
		}
		return s.n.Reply(msg, outputBody)
	}
	offset, err := s.send(inputBody.Key, inputBody.Msg)
	if err != nil {
		return err
	}
	outputBody := ForwardOutput{
		Type:   "send_ok",
		Offset: offset,
	}
	return s.n.Reply(msg, outputBody)
}

type ForwardInput struct {
	Type string `json:"type"`
	Key  string `json:"key"`
	Msg  int    `json:"msg"`
}

type ForwardOutput struct {
	Type   string `json:"type"`
	Offset int    `json:"offset"`
}

func (s *Server) forwardHandler(msg maelstrom.Message) error {
	var inputBody ForwardInput
	if err := json.Unmarshal(msg.Body, &inputBody); err != nil {
		return err
	}
	offset, err := s.send(inputBody.Key, inputBody.Msg)
	if err != nil {
		return err
	}
	outputBody := ForwardOutput{
		Type:   "forward_ok",
		Offset: offset,
	}
	return s.n.Reply(msg, outputBody)
}

func (s *Server) send(key string, msg int) (int, error) {
	offset, err := s.kv.ReadInt(context.Background(), fmt.Sprintf("offset_%v", key))
	if err != nil {
		err := err.(*maelstrom.RPCError)
		if err.Code == maelstrom.KeyDoesNotExist {
			offset = -1
		} else {
			return 0, err
		}
	}
	offset++
	for {
		_err := s.kv.CompareAndSwap(context.Background(), fmt.Sprintf("offset_%v", key), offset-1, offset, true)
		if _err == nil {
			break
		}
		err := _err.(*maelstrom.RPCError)
		if err.Code == maelstrom.KeyDoesNotExist {
			break
		} else if err.Code == maelstrom.PreconditionFailed {
			offset++
		} else {
			return 0, err
		}
	}
	err = s.kv.Write(context.Background(), fmt.Sprintf("%v_%d", key, offset), msg)
	return offset, err
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
	var mu sync.Mutex
	var wg sync.WaitGroup
	for key, offset := range inputBody.Offsets {
		i := offset
		key := key
		wg.Add(1)
		go func() {
			for {
				val, err := s.kv.ReadInt(context.Background(), fmt.Sprintf("%v_%d", key, i))
				if err != nil {
					break
				}
				mu.Lock()
				res[key] = append(res[key], [2]int{i, val})
				mu.Unlock()
				i++
			}
			wg.Done()
		}()
	}
	wg.Wait()

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
	errChan := make(chan error, len(inputBody.Offsets))
	for key, offset := range inputBody.Offsets {
		key := key
		offset := offset
		go func() {
			err := s.kv.Write(context.Background(), fmt.Sprintf("committed_%v", key), offset)
			errChan <- err
		}()
	}
	for range inputBody.Offsets {
		err := <-errChan
		if err != nil {
			return err
		}
	}

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
	var mu sync.Mutex
	errChan := make(chan error, len(inputBody.Keys))
	for _, key := range inputBody.Keys {
		key := key
		go func() {
			offset, err := s.kv.ReadInt(context.Background(), fmt.Sprintf("committed_%v", key))
			if err != nil {
				mu.Lock()
				res[key] = offset
				mu.Unlock()
			}
			errChan <- err
		}()
	}
	for range inputBody.Keys {
		err := <-errChan
		if err != nil {
			return err
		}
	}

	outputBody := ListCommittedOffsetsOutput{
		Type:    "list_committed_offsets_ok",
		Offsets: res,
	}
	return s.n.Reply(msg, outputBody)
}

func main() {
	s := NewServer()

	s.n.Handle("send", s.sendHandler)
	s.n.Handle("forward", s.forwardHandler)
	s.n.Handle("poll", s.pollHandler)
	s.n.Handle("commit_offsets", s.commitOffsetsHandler)
	s.n.Handle("list_committed_offsets", s.listCommittedOffsetsHandler)

	if err := s.n.Run(); err != nil {
		log.Fatal(err)
	}
}
