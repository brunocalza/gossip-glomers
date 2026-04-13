package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	kv := maelstrom.NewSeqKV(n)

	n.Handle("init", func(msg maelstrom.Message) error {
		return kv.Write(context.Background(), "counter", 0)
	})

	n.Handle("add", func(msg maelstrom.Message) error {
		var req struct {
			Type  string `json:"type"`
			Delta int    `json:"delta"`
		}
		if err := json.Unmarshal(msg.Body, &req); err != nil {
			return err
		}

		old, err := kv.ReadInt(context.Background(), "counter")
		if err != nil {
			return err
		}
		new := old + req.Delta
		if err := kv.Write(context.Background(), "counter", new); err != nil {
			return err
		}

		type response struct {
			Type string `json:"type"`
		}

		return n.Reply(msg, response{Type: "add_ok"})
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		kv.Write(context.Background(), "rand", time.Now().UnixMilli())

		value, err := kv.ReadInt(context.Background(), "counter")
		if err != nil {
			return err
		}

		type response struct {
			Type  string `json:"type"`
			Value int    `json:"value"`
		}

		return n.Reply(msg, response{Type: "read_ok", Value: value})
	})

	// Execute the node's message loop. This will run until STDIN is closed.
	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}
