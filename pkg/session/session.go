// Package session issues the per-uid login epoch. A newer epoch always beats
// an older one, which is how a second login evicts the first even when the two
// connections sit on different gateways.
package session

import (
	"context"
	"fmt"
	"strconv"
	"sync"

	"github.com/redis/go-redis/v9"
)

// Epochs issues and reads login epochs.
type Epochs interface {
	Next(ctx context.Context, uid int64) (uint64, error)
	Current(ctx context.Context, uid int64) (uint64, error)
}

// Memory is an in-process Epochs.
type Memory struct {
	mu sync.Mutex
	m  map[int64]uint64
}

// NewMemory creates an empty store.
func NewMemory() *Memory { return &Memory{m: map[int64]uint64{}} }

// Next implements Epochs.
func (s *Memory) Next(_ context.Context, uid int64) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[uid]++
	return s.m[uid], nil
}

// Current implements Epochs.
func (s *Memory) Current(_ context.Context, uid int64) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.m[uid], nil
}

// Redis stores epochs in Redis. The hash tag keeps a future per-uid key set on
// one cluster slot. The key has no TTL: an expired counter restarting at 1
// would make old tokens look current.
type Redis struct {
	Client redis.UniversalClient
}

func epochKey(uid int64) string { return "epoch:{" + strconv.FormatInt(uid, 10) + "}" }

// Next implements Epochs.
func (s *Redis) Next(ctx context.Context, uid int64) (uint64, error) {
	v, err := s.Client.Incr(ctx, epochKey(uid)).Result()
	if err != nil {
		return 0, fmt.Errorf("session: next epoch: %w", err)
	}
	return uint64(v), nil
}

// Current implements Epochs.
func (s *Redis) Current(ctx context.Context, uid int64) (uint64, error) {
	v, err := s.Client.Get(ctx, epochKey(uid)).Uint64()
	if err == redis.Nil {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("session: current epoch: %w", err)
	}
	return v, nil
}
