// Package session issues the per-uid login epoch. A newer epoch always beats
// an older one, which is how a second login evicts the first even when the two
// connections sit on different gateways.
package session

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"

	"github.com/redis/go-redis/v9"
)

// Epochs issues and reads login epochs.
type Epochs interface {
	Next(ctx context.Context, uid int64) (uint64, error)
	Current(ctx context.Context, uid int64) (uint64, error)
}

// Notifier announces new logins, so gateways can close superseded sockets at
// once. It is best effort: gateways also re-check epochs periodically.
type Notifier interface {
	// Subscribe calls fn for each new epoch until ctx ends.
	Subscribe(ctx context.Context, fn func(uid int64, epoch uint64)) error
}

// Memory is an in-process Epochs.
type Memory struct {
	mu   sync.Mutex
	m    map[int64]uint64
	subs map[*func(int64, uint64)]struct{}
}

// NewMemory creates an empty store.
func NewMemory() *Memory {
	return &Memory{m: map[int64]uint64{}, subs: map[*func(int64, uint64)]struct{}{}}
}

// Subscribe implements Notifier.
func (s *Memory) Subscribe(ctx context.Context, fn func(uid int64, epoch uint64)) error {
	s.mu.Lock()
	s.subs[&fn] = struct{}{}
	s.mu.Unlock()
	<-ctx.Done()
	s.mu.Lock()
	delete(s.subs, &fn)
	s.mu.Unlock()
	return ctx.Err()
}

// Next implements Epochs.
func (s *Memory) Next(_ context.Context, uid int64) (uint64, error) {
	s.mu.Lock()
	s.m[uid]++
	epoch := s.m[uid]
	subs := make([]func(int64, uint64), 0, len(s.subs))
	for fn := range s.subs {
		subs = append(subs, *fn)
	}
	s.mu.Unlock()
	for _, fn := range subs {
		fn(uid, epoch)
	}
	return epoch, nil
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

const epochChannel = "session:epoch"

// Next implements Epochs.
func (s *Redis) Next(ctx context.Context, uid int64) (uint64, error) {
	v, err := s.Client.Incr(ctx, epochKey(uid)).Result()
	if err != nil {
		return 0, fmt.Errorf("session: next epoch: %w", err)
	}
	// The login stands even if this fails: gateways re-check periodically.
	if err := s.Client.Publish(ctx, epochChannel, fmt.Sprintf("%d:%d", uid, v)).Err(); err != nil {
		slog.Warn("session: publish epoch", "uid", uid, "err", err)
	}
	return uint64(v), nil
}

// Subscribe implements Notifier. The client reconnects by itself, and anything
// published meanwhile is covered by the periodic re-check.
func (s *Redis) Subscribe(ctx context.Context, fn func(uid int64, epoch uint64)) error {
	sub := s.Client.Subscribe(ctx, epochChannel)
	defer sub.Close() //nolint:errcheck // Best-effort.
	msgs := sub.Channel()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case m, ok := <-msgs:
			if !ok {
				return fmt.Errorf("session: subscription closed")
			}
			left, right, _ := strings.Cut(m.Payload, ":")
			uid, uerr := strconv.ParseInt(left, 10, 64)
			epoch, eerr := strconv.ParseUint(right, 10, 64)
			if uerr != nil || eerr != nil {
				slog.Warn("session: bad epoch message", "payload", m.Payload)
				continue
			}
			fn(uid, epoch)
		}
	}
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
