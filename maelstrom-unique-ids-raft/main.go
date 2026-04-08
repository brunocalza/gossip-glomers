package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"go.etcd.io/raft/v3"
)

func main() {
	n := maelstrom.NewNode()

	var counter *DistributedCounter

	n.Handle("init", func(msg maelstrom.Message) error {
		nodeNum, err := strconv.ParseUint(n.ID()[1:], 10, 64)
		if err != nil {
			return fmt.Errorf("failed to parse node id: %v", err)
		}
		raftID := nodeNum + 1

		var peers []raft.Peer
		for _, nid := range n.NodeIDs() {
			num, err := strconv.ParseUint(nid[1:], 10, 64)
			if err != nil {
				return fmt.Errorf("failed to parse peer id: %v", err)
			}
			peers = append(peers, raft.Peer{ID: num + 1})
		}

		storage := raft.NewMemoryStorage()
		cfg := &raft.Config{
			ID:              raftID,
			ElectionTick:    10,
			HeartbeatTick:   1,
			Storage:         storage,
			MaxSizePerMsg:   4096, // keep small: messages are base64+JSON over stdin (64KB scanner limit)
			MaxInflightMsgs: 256,
		}

		raftNode := raft.StartNode(cfg, peers)
		counter = NewDistributedCounter(raftNode, storage, n.Send)
		go counter.run()

		return nil
	})

	n.Handle("generate", func(msg maelstrom.Message) error {
		next, err := counter.Next(context.Background(), generateRequestID(16))
		if err != nil {
			return maelstrom.NewRPCError(maelstrom.TemporarilyUnavailable, err.Error())
		}

		type response struct {
			Type string `json:"type"`
			Id   string `json:"id"`
		}

		return n.Reply(msg, response{
			Type: "generate_ok",
			Id:   fmt.Sprintf("%d", next),
		})
	})

	n.Handle("raft", func(msg maelstrom.Message) error {
		var body struct {
			Data []byte `json:"data"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			log.Printf("failed to unmarshal raft envelope: %v", err)
			return nil
		}

		if err := counter.Sync(context.Background(), body.Data); err != nil {
			log.Printf("failed to sync raft: %v", err)
		}
		return nil
	})

	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}

func generateRequestID(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}
