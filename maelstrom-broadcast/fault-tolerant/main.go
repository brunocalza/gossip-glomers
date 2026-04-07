package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type RetryMessage struct {
	destination string
	message     int
}

type State struct {
	mu        sync.RWMutex
	messages  map[int]struct{}
	neighbors []string
}

func NewState() *State {
	return &State{
		messages:  make(map[int]struct{}),
		neighbors: make([]string, 0),
	}
}

func (n *State) AddMesage(message int) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	if _, ok := n.messages[message]; ok {
		return false
	}
	n.messages[message] = struct{}{}
	return true
}

func (n *State) Messages() []int {
	n.mu.RLock()
	defer n.mu.RUnlock()
	messages := make([]int, 0, len(n.messages))
	for msg := range n.messages {
		messages = append(messages, msg)
	}
	return messages
}

const RPCTimeout = 300 * time.Millisecond
const RetryInterval = 150 * time.Millisecond

func main() {
	n := maelstrom.NewNode()

	state := NewState()

	retries := make(chan RetryMessage, 100)

	// have 5 background jobs periodically retrying messages
	for i := 0; i < 5; i++ {
		go func() {

			for job := range retries {
				type req struct {
					Type    string `json:"type"`
					Message int    `json:"message"`
				}
				r := req{Type: "broadcast", Message: job.message}

				ctx, cancel := context.WithTimeout(context.Background(), RPCTimeout)
				_, err := n.SyncRPC(ctx, job.destination, r)
				cancel()
				if err != nil {
					time.Sleep(RetryInterval)
					retries <- job
				}
			}

		}()
	}

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

			go func(neighbor string) {
				ctx, cancel := context.WithTimeout(context.Background(), RPCTimeout)
				_, err := n.SyncRPC(ctx, neighbor, req)
				cancel()
				if err != nil {
					retries <- RetryMessage{destination: neighbor, message: req.Message}
				}
			}(neighbor)
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

		nodeID := n.ID()
		if neighbors, ok := req.Topology[nodeID]; ok {
			for _, neighbor := range neighbors {
				state.neighbors = append(state.neighbors, neighbor)
			}
		}

		type response struct {
			Type string `json:"type"`
		}

		resp := response{
			Type: "topology_ok",
		}

		// Echo the original message back with the updated message type.
		return n.Reply(msg, resp)
	})

	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}
