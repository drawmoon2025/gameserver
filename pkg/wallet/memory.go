package wallet

import (
	"context"
	"sync"

	"github.com/drawmoon2025/gameserver/pkg/events"
)

type memKey struct {
	uid int64
	req string
}

type memEntry struct {
	change  Change
	balance int64
}

// Memory is an in-process Wallet for tests and local development.
type Memory struct {
	mu       sync.Mutex
	balances map[int64]int64
	entries  map[memKey]memEntry
	outbox   *events.MemoryStore
}

// NewMemory creates an empty wallet. outbox may be nil.
func NewMemory(outbox *events.MemoryStore) *Memory {
	return &Memory{balances: map[int64]int64{}, entries: map[memKey]memEntry{}, outbox: outbox}
}

// Apply implements Wallet.
func (m *Memory) Apply(_ context.Context, c Change) (Receipt, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := memKey{c.UID, c.RequestID}
	if e, ok := m.entries[key]; ok {
		if e.change.fingerprint() != c.fingerprint() || e.change.Reason != c.Reason {
			return Receipt{}, ErrKeyReuse
		}
		return Receipt{Balance: e.balance, Result: e.change.Result, Replayed: true}, nil
	}
	cur := m.balances[c.UID]
	next := cur + c.Delta
	if cur < c.Require || next < 0 {
		return Receipt{}, ErrInsufficient
	}
	m.balances[c.UID] = next
	m.entries[key] = memEntry{change: c, balance: next}
	if c.Event != nil && m.outbox != nil {
		m.outbox.Add(*c.Event)
	}
	return Receipt{Balance: next, Result: c.Result}, nil
}

// Balance implements Wallet.
func (m *Memory) Balance(_ context.Context, uid int64) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.balances[uid], nil
}
