package timer

import (
	"context"
	"sort"
	"sync"
	"time"
)

// MemStore is an in-memory Store for tests and local development.
type MemStore struct {
	mu     sync.Mutex
	jobs   map[int]map[uint64]Job
	tokens map[int]uint64
}

// NewMemStore creates an empty store.
func NewMemStore() *MemStore {
	return &MemStore{jobs: map[int]map[uint64]Job{}, tokens: map[int]uint64{}}
}

// Claim implements Store.
func (m *MemStore) Claim(_ context.Context, shard int, _ string) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.tokens[shard]++
	return m.tokens[shard], nil
}

// Add implements Store. Re-adding an ID keeps the first copy.
func (m *MemStore) Add(_ context.Context, j Job) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.jobs[j.Shard] == nil {
		m.jobs[j.Shard] = map[uint64]Job{}
	}
	if _, ok := m.jobs[j.Shard][j.ID]; !ok {
		m.jobs[j.Shard][j.ID] = j
	}
	return nil
}

// Due implements Store.
func (m *MemStore) Due(_ context.Context, shard int, until time.Time, limit int) ([]Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Job, 0)
	for _, j := range m.jobs[shard] {
		if !j.FireAt.After(until) {
			out = append(out, j)
		}
	}
	sort.Slice(out, func(a, b int) bool { return out[a].FireAt.Before(out[b].FireAt) })
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// Complete implements Store.
func (m *MemStore) Complete(_ context.Context, shard int, token, id uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if token != m.tokens[shard] {
		return ErrFenced
	}
	delete(m.jobs[shard], id)
	return nil
}

// Reschedule implements Store.
func (m *MemStore) Reschedule(_ context.Context, shard int, token, id uint64, at time.Time, attempt int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if token != m.tokens[shard] {
		return ErrFenced
	}
	if j, ok := m.jobs[shard][id]; ok {
		j.FireAt, j.Attempt = at, attempt
		m.jobs[shard][id] = j
	}
	return nil
}

// Cancel implements Store.
func (m *MemStore) Cancel(_ context.Context, shard int, id uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.jobs[shard], id)
	return nil
}

// Len reports stored jobs in a shard.
func (m *MemStore) Len(shard int) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.jobs[shard])
}
