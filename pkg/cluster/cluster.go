// Package cluster decides which node hosts which scenes.
//
// Keys hash to a fixed set of slots, and a node owns slots through etcd keys
// tied to its lease. Per-slot rather than per-scene ownership keeps etcd traffic
// independent of how many rooms exist. Timer shards are the same slots, so a
// kingdom's timers always run where the kingdom is hosted.
//
// A node that dies loses its lease and its slots become free. A node that can
// no longer reach etcd sees its lease end and gives up its slots, which is what
// stops two nodes hosting one scene for longer than a lease TTL.
package cluster

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path"
	"strconv"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"

	"github.com/yourname/gameserver/pkg/discovery"
)

// ErrLeaseLost means the node's slots are gone and the process should restart.
var ErrLeaseLost = errors.New("cluster: lease lost")

const opTimeout = 3 * time.Second

// Config describes one node.
type Config struct {
	Client   *clientv3.Client
	Prefix   string // Default "/gameserver".
	Service  string
	NodeID   string        // Unique within the service and stable across restarts.
	Addr     string        // What gateways dial.
	TTL      int           // Lease seconds. Default 10.
	Interval time.Duration // Reconcile period. Default 1s.
	// MaxDrain bounds how long a slot this node should give up stays here
	// waiting to become idle. Default 10m.
	MaxDrain time.Duration
	// Idle reports that a slot hosts nothing, so moving it interrupts nobody.
	Idle func(slot int) bool
	// Acquired runs after the slot is owned.
	Acquired func(slot int)
	// Released runs before the slot is given up. When it returns, this node
	// must have stopped serving the slot.
	Released func(slot int)
}

// Node is one member of a service.
type Node struct {
	cfg Config

	mu    sync.RWMutex
	owned map[int]bool

	draining map[int]time.Time // Run goroutine only.
}

// New creates a Node. Call Run to join.
func New(cfg Config) *Node {
	if cfg.Prefix == "" {
		cfg.Prefix = "/gameserver"
	}
	if cfg.TTL <= 0 {
		cfg.TTL = 10
	}
	if cfg.Interval <= 0 {
		cfg.Interval = time.Second
	}
	if cfg.MaxDrain <= 0 {
		cfg.MaxDrain = 10 * time.Minute
	}
	return &Node{cfg: cfg, owned: map[int]bool{}, draining: map[int]time.Time{}}
}

// Owns reports whether this node currently serves key.
func (n *Node) Owns(key string) bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.owned[discovery.Slot(key)]
}

func nodesPrefix(prefix, service string) string { return path.Join(prefix, service, "nodes") + "/" }
func slotsPrefix(prefix, service string) string { return path.Join(prefix, service, "slots") + "/" }

func (n *Node) slotKey(slot int) string {
	return slotsPrefix(n.cfg.Prefix, n.cfg.Service) + strconv.Itoa(slot)
}

// Run joins the cluster and keeps this node's slots correct until ctx ends,
// when it hands them back, or until the lease is lost.
func (n *Node) Run(ctx context.Context) error {
	sess, err := concurrency.NewSession(n.cfg.Client, concurrency.WithTTL(n.cfg.TTL))
	if err != nil {
		return fmt.Errorf("cluster: session: %w", err)
	}
	defer sess.Close() //nolint:errcheck // Revokes the lease; expiry covers a failure.
	lease := sess.Lease()

	pctx, cancel := context.WithTimeout(ctx, opTimeout)
	_, err = n.cfg.Client.Put(pctx, nodesPrefix(n.cfg.Prefix, n.cfg.Service)+n.cfg.NodeID,
		n.cfg.Addr, clientv3.WithLease(lease))
	cancel()
	if err != nil {
		return fmt.Errorf("cluster: register: %w", err)
	}

	t := time.NewTicker(n.cfg.Interval)
	defer t.Stop()
	for {
		if err := n.reconcile(ctx, lease); err != nil && ctx.Err() == nil {
			slog.Warn("cluster reconcile", "service", n.cfg.Service, "err", err)
		}
		select {
		case <-ctx.Done():
			n.releaseAll()
			return ctx.Err()
		case <-sess.Done():
			n.releaseAll()
			return ErrLeaseLost
		case <-t.C:
		}
	}
}

func (n *Node) reconcile(ctx context.Context, lease clientv3.LeaseID) error {
	rctx, cancel := context.WithTimeout(ctx, opTimeout)
	defer cancel()
	members, owners, err := read(rctx, n.cfg.Client, n.cfg.Prefix, n.cfg.Service)
	if err != nil {
		return err
	}
	if len(members) == 0 {
		return nil // Not even this node is listed yet.
	}
	ids := make([]string, 0, len(members))
	for id := range members {
		ids = append(ids, id)
	}
	me := n.cfg.NodeID
	for slot := range discovery.Slots {
		want, owner, mine := discovery.Pick(strconv.Itoa(slot), ids), owners[slot], n.holds(slot)
		switch {
		case mine && owner != me:
			// The key is gone though the lease lives, so this copy is stale.
			n.release(slot)
		case mine && want == me:
			delete(n.draining, slot)
		case mine:
			if err := n.drain(ctx, slot); err != nil {
				return err
			}
		case owner == "" && want == me:
			if err := n.claim(ctx, slot, lease); err != nil {
				return err
			}
		}
	}
	return nil
}

func (n *Node) holds(slot int) bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.owned[slot]
}

func (n *Node) claim(ctx context.Context, slot int, lease clientv3.LeaseID) error {
	key := n.slotKey(slot)
	cctx, cancel := context.WithTimeout(ctx, opTimeout)
	defer cancel()
	r, err := n.cfg.Client.Txn(cctx).
		If(clientv3.Compare(clientv3.CreateRevision(key), "=", 0)).
		Then(clientv3.OpPut(key, n.cfg.NodeID, clientv3.WithLease(lease))).
		Commit()
	if err != nil {
		return fmt.Errorf("cluster: claim slot %d: %w", slot, err)
	}
	if !r.Succeeded {
		return nil
	}
	n.mu.Lock()
	n.owned[slot] = true
	n.mu.Unlock()
	if n.cfg.Acquired != nil {
		n.cfg.Acquired(slot)
	}
	return nil
}

// drain gives up a slot another node should have, once doing so hurts nobody
// or the wait is over.
func (n *Node) drain(ctx context.Context, slot int) error {
	since, ok := n.draining[slot]
	if !ok {
		since = time.Now()
		n.draining[slot] = since
	}
	idle := n.cfg.Idle == nil || n.cfg.Idle(slot)
	if !idle && time.Since(since) < n.cfg.MaxDrain {
		return nil
	}
	n.release(slot)
	key := n.slotKey(slot)
	dctx, cancel := context.WithTimeout(ctx, opTimeout)
	defer cancel()
	_, err := n.cfg.Client.Txn(dctx).
		If(clientv3.Compare(clientv3.Value(key), "=", n.cfg.NodeID)).
		Then(clientv3.OpDelete(key)).
		Commit()
	if err != nil {
		// The next owner only waits longer: this node has stopped serving,
		// and the key goes when the lease does.
		return fmt.Errorf("cluster: release slot %d: %w", slot, err)
	}
	return nil
}

// release stops serving a slot. Owns turns false first, so nothing new starts
// while Released tears the slot down.
func (n *Node) release(slot int) {
	n.mu.Lock()
	delete(n.owned, slot)
	n.mu.Unlock()
	delete(n.draining, slot)
	if n.cfg.Released != nil {
		n.cfg.Released(slot)
	}
}

func (n *Node) releaseAll() {
	n.mu.RLock()
	slots := make([]int, 0, len(n.owned))
	for slot := range n.owned {
		slots = append(slots, slot)
	}
	n.mu.RUnlock()
	for _, slot := range slots {
		n.release(slot)
	}
}

// read returns live members (id to address) and slot owners (slot to id), both
// from one etcd revision.
func read(ctx context.Context, c *clientv3.Client, prefix, service string) (map[string]string, map[int]string, error) {
	np, sp := nodesPrefix(prefix, service), slotsPrefix(prefix, service)
	nodes, err := c.Get(ctx, np, clientv3.WithPrefix())
	if err != nil {
		return nil, nil, fmt.Errorf("cluster: read nodes: %w", err)
	}
	slots, err := c.Get(ctx, sp, clientv3.WithPrefix(), clientv3.WithRev(nodes.Header.Revision))
	if err != nil {
		return nil, nil, fmt.Errorf("cluster: read slots: %w", err)
	}
	members := make(map[string]string, len(nodes.Kvs))
	for _, kv := range nodes.Kvs {
		members[string(kv.Key)[len(np):]] = string(kv.Value)
	}
	owners := make(map[int]string, len(slots.Kvs))
	for _, kv := range slots.Kvs {
		slot, err := strconv.Atoi(string(kv.Key)[len(sp):])
		if err != nil {
			return nil, nil, fmt.Errorf("cluster: bad slot key %q", kv.Key)
		}
		owners[slot] = string(kv.Value)
	}
	return members, owners, nil
}

// Resolver is what gateways use to find a scene's node. It works from a table
// refreshed by polling, so a request never waits on etcd. A stale answer is
// safe: the node refuses a scene it does not own and the client reconnects.
type Resolver struct {
	client   *clientv3.Client
	prefix   string
	services []string
	interval time.Duration

	mu     sync.RWMutex
	tables map[string]map[int]string // Service to slot to address.
}

// NewResolver creates a Resolver for the named services. Empty prefix and zero
// interval select the defaults.
func NewResolver(c *clientv3.Client, prefix string, interval time.Duration, services ...string) *Resolver {
	if prefix == "" {
		prefix = "/gameserver"
	}
	if interval <= 0 {
		interval = time.Second
	}
	return &Resolver{client: c, prefix: prefix, services: services, interval: interval,
		tables: map[string]map[int]string{}}
}

// Refresh reloads every service's table. A service that fails keeps its table.
func (r *Resolver) Refresh(ctx context.Context) error {
	var errs []error
	for _, service := range r.services {
		rctx, cancel := context.WithTimeout(ctx, opTimeout)
		members, owners, err := read(rctx, r.client, r.prefix, service)
		cancel()
		if err != nil {
			errs = append(errs, err)
			continue
		}
		table := make(map[int]string, len(owners))
		for slot, id := range owners {
			if addr, ok := members[id]; ok {
				table[slot] = addr
			}
		}
		r.mu.Lock()
		r.tables[service] = table
		r.mu.Unlock()
	}
	return errors.Join(errs...)
}

// Run refreshes until ctx ends.
func (r *Resolver) Run(ctx context.Context) {
	t := time.NewTicker(r.interval)
	defer t.Stop()
	for {
		if err := r.Refresh(ctx); err != nil && ctx.Err() == nil {
			slog.Warn("cluster resolver", "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// Resolve implements discovery.Resolver.
func (r *Resolver) Resolve(service, key string) (string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if addr, ok := r.tables[service][discovery.Slot(key)]; ok {
		return addr, nil
	}
	return "", discovery.ErrNoNode
}
