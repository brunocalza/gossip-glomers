package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

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

func main() {
	n := maelstrom.NewNode()

	state := NewState()

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

			n.SyncRPC(context.Background(), neighbor, req)
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
