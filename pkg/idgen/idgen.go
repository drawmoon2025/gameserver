// Package idgen issues unique, roughly time-ordered 63-bit IDs without a
// central service: 41 bits of milliseconds, 10 bits of node, 12 of sequence.
package idgen

import (
	"fmt"
	"sync"
	"time"
)

// epochMS is 2026-01-01 UTC. Never change it: IDs could then repeat.
const epochMS int64 = 1767225600000

// MaxNode is the largest node ID.
const MaxNode = maxNode

const (
	maxNode  = 1<<10 - 1
	maxSeq   = 1<<12 - 1
	nodeBits = 12
	timeBits = 22
)

// Generator is safe for concurrent use. Each running process needs its own
// node ID.
type Generator struct {
	mu   sync.Mutex
	node int64
	last int64
	seq  int64
	now  func() time.Time
}

// New creates a generator for a node ID in [0, 1023].
func New(node int64) (*Generator, error) {
	if node < 0 || node > maxNode {
		return nil, fmt.Errorf("idgen: node %d out of range", node)
	}
	return &Generator{node: node, now: time.Now}, nil
}

// Next returns a new ID. If the clock steps backwards it keeps issuing from
// the last timestamp, borrowing ahead, rather than risk a duplicate.
func (g *Generator) Next() int64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	ts := g.now().UnixMilli() - epochMS
	if ts > g.last {
		g.last, g.seq = ts, 0
	} else {
		g.seq++
		if g.seq > maxSeq {
			g.last++
			g.seq = 0
		}
	}
	return g.last<<timeBits | g.node<<nodeBits | g.seq
}
