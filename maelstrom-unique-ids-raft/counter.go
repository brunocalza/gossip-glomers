package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

// Result is the id returned to the client
type Result int

type DistributedCounter struct {
	// state under consensus
	seq int

	// deps
	sendFn  func(dest string, body any) error
	node    raft.Node
	storage *raft.MemoryStorage

	//control
	mu      sync.Mutex
	waiters map[string]chan Result
}

func NewDistributedCounter(node raft.Node, storage *raft.MemoryStorage, sendFn func(dest string, body any) error) *DistributedCounter {
	return &DistributedCounter{
		node:    node,
		storage: storage,
		sendFn:  sendFn,
		seq:     0,
		waiters: make(map[string]chan Result),
	}
}

// Next generates a new id
func (dc *DistributedCounter) Next(ctx context.Context, reqID string) (Result, error) {
	ch := make(chan Result, 1)
	dc.mu.Lock()
	dc.waiters[reqID] = ch
	dc.mu.Unlock()

	// If this node is a leader, it will append the entry and send to the followers waiting for majority
	// If not, the proposal is sent internally to the leader
	// We retry until raft accepts the proposal (e.g., no leader yet).
	for {
		if err := dc.node.Propose(ctx, []byte(reqID)); err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// blocked waiting for a result
	return <-ch, nil
}

func (dc *DistributedCounter) Sync(ctx context.Context, data []byte) error {
	var msg raftpb.Message
	if err := msg.Unmarshal(data); err != nil {
		return err
	}
	return dc.node.Step(ctx, msg)
}

func (dc *DistributedCounter) run() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			dc.node.Tick()

		case rd := <-dc.node.Ready():
			if !raft.IsEmptyHardState(rd.HardState) {
				if err := dc.storage.SetHardState(rd.HardState); err != nil {
					log.Printf("failed to set hard state: %v", err)
				}
			}

			if len(rd.Entries) > 0 {
				if err := dc.storage.Append(rd.Entries); err != nil {
					log.Printf("failed to append entries: %v", err)
				}
			}

			for _, msg := range rd.Messages {
				dc.send(msg)
			}

			for _, ent := range rd.CommittedEntries {
				dc.applyEntry(ent)
			}

			dc.node.Advance()
		}
	}
}
func (dc *DistributedCounter) applyEntry(ent raftpb.Entry) {
	if ent.Type == raftpb.EntryConfChange {
		var cc raftpb.ConfChange
		if err := cc.Unmarshal(ent.Data); err != nil {
			log.Printf("failed to unmarshal conf change: %v", err)
			return
		}
		dc.node.ApplyConfChange(cc)
		return
	}

	if ent.Type != raftpb.EntryNormal || len(ent.Data) == 0 {
		return
	}

	reqID := string(ent.Data)

	dc.mu.Lock()
	dc.seq++
	res := Result(dc.seq)

	ch := dc.waiters[reqID]
	if ch != nil {
		delete(dc.waiters, reqID)
	}
	dc.mu.Unlock()

	if ch != nil {
		ch <- res
	}
}

func (dc *DistributedCounter) send(msg raftpb.Message) {
	destID := fmt.Sprintf("n%d", msg.To-1)

	data, err := msg.Marshal()
	if err != nil {
		log.Printf("failed to marshal raft message: %v", err)
		return
	}

	if err := dc.sendFn(destID, map[string]any{
		"type": "raft",
		"data": data,
	}); err != nil {
		log.Printf("failed to send raft message to %s: %v", destID, err)
	}
}
