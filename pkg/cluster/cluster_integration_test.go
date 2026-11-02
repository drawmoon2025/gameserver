//go:build integration

package cluster

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/yourname/gameserver/internal/itest"
	"github.com/yourname/gameserver/pkg/discovery"
)

func client(t *testing.T) *clientv3.Client {
	t.Helper()
	c, err := clientv3.New(clientv3.Config{Endpoints: itest.EtcdEndpoints(), DialTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() }) //nolint:errcheck // Some tests close it first.
	return c
}

// ledger records which node believes it owns each slot.
type ledger struct {
	mu      sync.Mutex
	holders map[int]map[string]bool
	overlap atomic.Int64
}

func newLedger() *ledger { return &ledger{holders: map[int]map[string]bool{}} }

func (l *ledger) acquired(id string, slot int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.holders[slot] == nil {
		l.holders[slot] = map[string]bool{}
	}
	l.holders[slot][id] = true
	if len(l.holders[slot]) > 1 {
		l.overlap.Add(1)
	}
}

func (l *ledger) released(id string, slot int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.holders[slot], id)
}

func (l *ledger) count(id string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, h := range l.holders {
		if h[id] {
			n++
		}
	}
	return n
}

func eventually(t *testing.T, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out: %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

type started struct {
	node *Node
	stop context.CancelFunc
	done chan error
}

func start(t *testing.T, c *clientv3.Client, prefix, id string, l *ledger, tune func(*Config)) *started {
	t.Helper()
	cfg := Config{Client: c, Prefix: prefix, Service: "room", NodeID: id, Addr: id + ":1",
		TTL: 2, Interval: 50 * time.Millisecond,
		Acquired: func(slot int) { l.acquired(id, slot) },
		Released: func(slot int) { l.released(id, slot) }}
	if tune != nil {
		tune(&cfg)
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := &started{node: New(cfg), stop: cancel, done: make(chan error, 1)}
	go func() { s.done <- s.node.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		<-s.done
	})
	return s
}

func prefix(t *testing.T) string { return fmt.Sprintf("/itest/%s/%d", t.Name(), itest.ID()) }

func TestTwoNodesSplitEverySlotOnce(t *testing.T) {
	c, p, l := client(t), prefix(t), newLedger()
	a := start(t, c, p, "a", l, nil)
	eventually(t, "a owns every slot", func() bool { return l.count("a") == discovery.Slots })
	b := start(t, c, p, "b", l, nil)
	eventually(t, "the slots split", func() bool {
		return l.count("b") > 0 && l.count("a")+l.count("b") == discovery.Slots && l.count("a") < discovery.Slots
	})
	if n := l.overlap.Load(); n != 0 {
		t.Fatalf("%d slots had two owners at once", n)
	}

	r := NewResolver(c, p, 0, "room")
	if err := r.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	for i := range 200 {
		key := fmt.Sprint("room-", i)
		addr, err := r.Resolve("room", key)
		if err != nil {
			t.Fatalf("resolve %s: %v", key, err)
		}
		if (addr == "a:1") != a.node.Owns(key) || (addr == "b:1") != b.node.Owns(key) {
			t.Fatalf("%s resolves to %s, but ownership disagrees", key, addr)
		}
	}
}

func TestLeavingNodeHandsOverItsSlots(t *testing.T) {
	c, p, l := client(t), prefix(t), newLedger()
	a := start(t, c, p, "a", l, nil)
	eventually(t, "a owns every slot", func() bool { return l.count("a") == discovery.Slots })
	start(t, c, p, "b", l, nil)
	eventually(t, "b has slots", func() bool { return l.count("b") > 0 })

	a.stop()
	if err := <-a.done; !errors.Is(err, context.Canceled) {
		t.Fatalf("run ended with %v", err)
	}
	a.done <- nil // For the cleanup.
	eventually(t, "b owns every slot", func() bool { return l.count("b") == discovery.Slots })
	if l.count("a") != 0 {
		t.Fatalf("a still believes it owns %d slots", l.count("a"))
	}
}

func TestNodeCutOffFromEtcdGivesUpAndIsReplaced(t *testing.T) {
	gone, survivor, p, l := client(t), client(t), prefix(t), newLedger()
	a := start(t, gone, p, "a", l, nil)
	eventually(t, "a owns every slot", func() bool { return l.count("a") == discovery.Slots })
	start(t, survivor, p, "b", l, func(c *Config) { c.Idle = func(int) bool { return true } })

	if err := gone.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-a.done:
		if !errors.Is(err, ErrLeaseLost) {
			t.Fatalf("run ended with %v", err)
		}
		a.done <- nil // For the cleanup.
	case <-time.After(20 * time.Second):
		t.Fatal("a kept running without etcd")
	}
	if l.count("a") != 0 || a.node.Owns("anything") {
		t.Fatal("a kept serving after losing its lease")
	}
	eventually(t, "b owns every slot", func() bool { return l.count("b") == discovery.Slots })
}

func TestBusySlotWaitsForIdleBeforeMoving(t *testing.T) {
	c, p, l := client(t), prefix(t), newLedger()
	var idle atomic.Bool
	start(t, c, p, "a", l, func(c *Config) { c.Idle = func(int) bool { return idle.Load() } })
	eventually(t, "a owns every slot", func() bool { return l.count("a") == discovery.Slots })
	start(t, c, p, "b", l, nil)

	time.Sleep(500 * time.Millisecond)
	if l.count("b") != 0 {
		t.Fatalf("b took %d busy slots", l.count("b"))
	}
	idle.Store(true)
	eventually(t, "idle slots move", func() bool { return l.count("b") > 0 })
}

func TestMaxDrainForcesAMove(t *testing.T) {
	c, p, l := client(t), prefix(t), newLedger()
	start(t, c, p, "a", l, func(c *Config) {
		c.Idle = func(int) bool { return false }
		c.MaxDrain = 200 * time.Millisecond
	})
	eventually(t, "a owns every slot", func() bool { return l.count("a") == discovery.Slots })
	start(t, c, p, "b", l, nil)
	eventually(t, "slots move despite being busy", func() bool { return l.count("b") > 0 })
}
