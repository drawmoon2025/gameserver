package timer

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// Group runs a Scheduler per shard, for the shards this node currently owns.
type Group struct {
	cfg      Config
	handlers map[string]Handler

	mu   sync.Mutex
	runs map[int]*groupRun
}

type groupRun struct {
	cancel context.CancelFunc
	done   chan struct{}
	sched  atomic.Pointer[Scheduler]
}

// NewGroup creates a Group. cfg.Shard is ignored. Register handlers before Start.
func NewGroup(cfg Config) *Group {
	return &Group{cfg: cfg, handlers: map[string]Handler{}, runs: map[int]*groupRun{}}
}

// Handle registers the handler for a job kind.
func (g *Group) Handle(kind string, h Handler) { g.handlers[kind] = h }

// Start runs a shard until Stop or ctx ends. Starting a running shard does
// nothing. Failures retry: this node still owns the shard, so giving up would
// leave its timers unfired.
func (g *Group) Start(ctx context.Context, shard int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if _, ok := g.runs[shard]; ok {
		return
	}
	ctx, cancel := context.WithCancel(ctx)
	run := &groupRun{cancel: cancel, done: make(chan struct{})}
	g.runs[shard] = run
	go func() {
		defer close(run.done)
		for ctx.Err() == nil {
			cfg := g.cfg
			cfg.Shard = g.cfg.ShardBase + shard
			s := New(cfg)
			s.handlers = g.handlers
			run.sched.Store(s)
			err := s.Run(ctx)
			if ctx.Err() != nil || errors.Is(err, context.Canceled) {
				return
			}
			slog.Warn("timer shard restarting", "shard", shard, "err", err)
			select {
			case <-ctx.Done():
			case <-time.After(time.Second):
			}
		}
	}()
}

// Stop ends a shard and waits for its running handlers. Its jobs stay stored.
func (g *Group) Stop(shard int) {
	g.mu.Lock()
	run := g.runs[shard]
	delete(g.runs, shard)
	g.mu.Unlock()
	if run != nil {
		run.cancel()
		<-run.done
	}
}

// StopAll stops every shard.
func (g *Group) StopAll() {
	g.mu.Lock()
	shards := make([]int, 0, len(g.runs))
	for shard := range g.runs {
		shards = append(shards, shard)
	}
	g.mu.Unlock()
	for _, shard := range shards {
		g.Stop(shard)
	}
}

// Wake makes a shard scan now, for a job stored outside this package. A shard
// not running here is a no-op: its owner finds the job on its next poll.
func (g *Group) Wake(shard int) {
	g.mu.Lock()
	run := g.runs[shard]
	g.mu.Unlock()
	if run == nil {
		return
	}
	if s := run.sched.Load(); s != nil {
		s.Wake()
	}
}
