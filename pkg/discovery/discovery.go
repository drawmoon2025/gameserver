// Package discovery resolves a service and scene key to the node that owns it.
// Static is the only implementation today; an etcd-backed one is the same
// interface plus a watch that updates the node list.
package discovery

import (
	"errors"
	"hash/fnv"
	"sync"
)

// ErrNoNode means the service has no live node.
var ErrNoNode = errors.New("discovery: no node for service")

// Resolver finds the owner of a key.
type Resolver interface {
	Resolve(service, key string) (addr string, err error)
}

// Static is a fixed or manually updated service list.
type Static struct {
	mu    sync.RWMutex
	nodes map[string][]string
}

// NewStatic creates a resolver from service -> addresses.
func NewStatic(nodes map[string][]string) *Static { return &Static{nodes: nodes} }

// Set replaces a service's nodes.
func (s *Static) Set(service string, addrs []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nodes[service] = addrs
}

// Resolve implements Resolver with rendezvous hashing: removing a node only
// moves the keys that node owned.
func (s *Static) Resolve(service, key string) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var (
		best  string
		score uint64
	)
	for _, addr := range s.nodes[service] {
		h := fnv.New64a()
		_, _ = h.Write([]byte(addr))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(key))
		if v := mix(h.Sum64()); best == "" || v > score {
			best, score = addr, v
		}
	}
	if best == "" {
		return "", ErrNoNode
	}
	return best, nil
}

// mix is a finalizer: FNV alone distributes similar keys poorly.
func mix(x uint64) uint64 {
	x ^= x >> 33
	x *= 0xff51afd7ed558ccd
	x ^= x >> 33
	x *= 0xc4ceb9fe1a85ec53
	x ^= x >> 33
	return x
}
