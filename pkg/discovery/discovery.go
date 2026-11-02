// Package discovery resolves a service and scene key to the node that owns it.
// Static suits one node per stateful service. pkg/cluster provides the
// etcd-backed Resolver for anything larger.
package discovery

import (
	"errors"
	"hash/fnv"
	"sync"
)

// Slots is permanent: timer jobs are stored under their slot, so changing it
// would strand them.
const Slots = 256

// Slot returns the slot a scene key belongs to.
func Slot(key string) int {
	h := fnv.New64a()
	_, _ = h.Write([]byte(key))
	return int(mix(h.Sum64()) % Slots)
}

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
	best := Pick(key, s.nodes[service])
	if best == "" {
		return "", ErrNoNode
	}
	return best, nil
}

// Pick chooses one of candidates for key by rendezvous hashing, or "" if there
// are none. The answer does not depend on the order of candidates.
func Pick(key string, candidates []string) string {
	var (
		best  string
		score uint64
	)
	for _, c := range candidates {
		h := fnv.New64a()
		_, _ = h.Write([]byte(c))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(key))
		if v := mix(h.Sum64()); best == "" || v > score {
			best, score = c, v
		}
	}
	return best
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
