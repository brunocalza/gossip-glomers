package main

import (
	"encoding/json"
	"log"
	"os"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	kv := maelstrom.NewSeqKV(n)

	n.Handle("add", func(msg maelstrom.Message) error {
		var req struct {
			Type  string `json:"type"`
			Delta int    `json:"int"`
		}
		if err := json.Unmarshal(msg.Body, &req); err != nil {
			return err
		}

		type response struct {
			Type string `json:"type"`
		}

		return n.Reply(msg, response{Type: "add_ok"})
	})

	// Execute the node's message loop. This will run until STDIN is closed.
	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}
