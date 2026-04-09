package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type State struct {
	mu        sync.RWMutex
	messages  map[int]struct{}
	neighbors []string
	pending   map[string]map[int]struct{}
}

func NewState() *State {
	return &State{
		messages: make(map[int]struct{}),
		pending:  make(map[string]map[int]struct{}),
	}
}

func (s *State) AddMesage(message int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.messages[message]; ok {
		return false
	}
	s.messages[message] = struct{}{}
	return true
}

func (s *State) AddPending(neighbor string, message int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pending[neighbor] == nil {
		s.pending[neighbor] = make(map[int]struct{})
	}
	s.pending[neighbor][message] = struct{}{}
}

func (s *State) GetPending() map[string][]int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make(map[string][]int, len(s.pending))
	for neighbor, msgs := range s.pending {
		batch := make([]int, 0, len(msgs))
		for msg := range msgs {
			batch = append(batch, msg)
		}
		result[neighbor] = batch
	}
	return result
}

func (s *State) RemovePending(neighbor string, messages []int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, m := range messages {
		delete(s.pending[neighbor], m)
	}
	if len(s.pending[neighbor]) == 0 {
		delete(s.pending, neighbor)
	}
}

func (s *State) Messages() []int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	messages := make([]int, 0, len(s.messages))
	for msg := range s.messages {
		messages = append(messages, msg)
	}
	return messages
}

const RPCTimeout = 300 * time.Millisecond

func getEnvInt(key string, defaultVal int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return defaultVal
}

func main() {
	retryMs := getEnvInt("RETRY", 500)
	branching := getEnvInt("BRANCH", 6)
	retryInterval := time.Duration(retryMs) * time.Millisecond

	n := maelstrom.NewNode()

	state := NewState()

	ctx := context.Background()

	ticker := time.NewTicker(retryInterval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return

			case <-ticker.C:
				for neighbor, pending := range state.GetPending() {
					type req struct {
						Type     string `json:"type"`
						Messages []int  `json:"messages"`
					}
					r := req{Type: "gossip", Messages: pending}

					go func() {
						ctx, cancel := context.WithTimeout(context.Background(), RPCTimeout)
						defer cancel()
						_, err := n.SyncRPC(ctx, neighbor, r)
						if err == nil {
							state.RemovePending(neighbor, pending)
						}
					}()
				}
			}
		}
	}()

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var req struct {
			Type    string `json:"type"`
			Message int    `json:"message"`
		}
		if err := json.Unmarshal(msg.Body, &req); err != nil {
			return err
		}

		// if we don't add this we congest the network
		if !state.AddMesage(req.Message) {
			return n.Reply(msg, map[string]string{"type": "broadcast_ok"})
		}

		for _, neighbor := range state.neighbors {
			if msg.Src == neighbor {
				continue
			}

			dest := neighbor
			message := req.Message
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), RPCTimeout)
				defer cancel()
				_, err := n.SyncRPC(ctx, dest, map[string]any{
					"type":     "gossip",
					"messages": []int{message},
				})
				if err != nil {
					state.AddPending(dest, message)
				}
			}()
		}

		return n.Reply(msg, map[string]string{"type": "broadcast_ok"})
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		var req struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(msg.Body, &req); err != nil {
			return err
		}

		type response struct {
			Type     string `json:"type"`
			Messages []int  `json:"messages"`
		}

		resp := response{
			Type:     "read_ok",
			Messages: state.Messages(),
		}

		// Echo the original message back with the updated message type.
		return n.Reply(msg, resp)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		var req struct {
			Type     string              `json:"type"`
			Topology map[string][]string `json:"topology"`
		}
		if err := json.Unmarshal(msg.Body, &req); err != nil {
			return err
		}

		state.neighbors = buildTree(n.ID(), n.NodeIDs(), branching)

		type response struct {
			Type string `json:"type"`
		}

		resp := response{
			Type: "topology_ok",
		}

		// Echo the original message back with the updated message type.
		return n.Reply(msg, resp)
	})

	n.Handle("gossip", func(msg maelstrom.Message) error {
		var req struct {
			Type     string `json:"type"`
			Messages []int  `json:"messages"`
		}
		if err := json.Unmarshal(msg.Body, &req); err != nil {
			return err
		}

		var newMessages []int
		for _, message := range req.Messages {
			if state.AddMesage(message) {
				newMessages = append(newMessages, message)
			}
		}

		if len(newMessages) == 0 {
			return n.Reply(msg, map[string]string{"type": "gossip_ok"})
		}

		for _, neighbor := range state.neighbors {
			if msg.Src == neighbor {
				continue
			}

			for _, m := range newMessages {
				state.AddPending(neighbor, m)
			}
		}

		return n.Reply(msg, map[string]string{"type": "gossip_ok"})
	})

	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}

// buildTree returns the neighbors for nodeID in a tree with the given branching factor.
func buildTree(nodeID string, nodeIDs []string, branching int) []string {
	sort.Strings(nodeIDs)
	idx := -1
	for i, id := range nodeIDs {
		if id == nodeID {
			idx = i
			break
		}
	}

	var neighbors []string
	if idx > 0 {
		parentIdx := (idx - 1) / branching
		neighbors = append(neighbors, nodeIDs[parentIdx])
	}
	for j := 1; j <= branching; j++ {
		childIdx := idx*branching + j
		if childIdx < len(nodeIDs) {
			neighbors = append(neighbors, nodeIDs[childIdx])
		}
	}
	return neighbors
}
