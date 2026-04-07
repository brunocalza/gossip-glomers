package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	var generator *Generator

	n.Handle("init", func(msg maelstrom.Message) error {
		id, err := strconv.ParseInt(n.ID()[1:], 10, 64)
		if err != nil {
			return fmt.Errorf("failed to convert node id")
		}
		generator = NewSnowflakeGenerator(id, RealClock{})
		return nil
	})

	n.Handle("generate", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		next := generator.Next()

		type response struct {
			Type string `json:"type"`
			Id   string `json:"id"`
		}

		resp := response{
			Type: "generate_ok",
			Id:   fmt.Sprintf("%d", next),
		}

		return n.Reply(msg, resp)
	})

	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}
