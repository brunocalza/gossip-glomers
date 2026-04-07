package main

import (
	"sync"
)

const (
	nodeBits = 10
	seqBits  = 12

	maxNodeID = (1 << nodeBits) - 1
	maxSeq    = (1 << seqBits) - 1

	nodeShift = seqBits
	timeShift = seqBits + nodeBits
)

// 2025-01-01T00:00:00Z
const customEpoch int64 = 1735689600000

type Generator struct {
	mu sync.Mutex

	lastMs int64
	seq    int64
	nodeID int64

	clock Clock
}

func NewSnowflakeGenerator(nodeID int64, clock Clock) *Generator {
	return &Generator{
		lastMs: -1,
		seq:    0,
		nodeID: nodeID,
		clock:  clock,
	}
}

func (g *Generator) Next() int64 {
	g.mu.Lock()
	defer g.mu.Unlock()

	now := g.clock.Now().UnixMilli()

	if now == g.lastMs {
		if g.seq < maxSeq {
			g.seq++
		} else {
			now = WaitNextMillis(g.clock, g.lastMs)
			g.lastMs = now
			g.seq = 0
		}
	} else {
		g.lastMs = now
		g.seq = 0
	}

	ts := int64(g.lastMs - customEpoch)

	return (ts << timeShift) |
		(g.nodeID << nodeShift) |
		g.seq
}
