// Package timer is the durable scheduler SLG needs: marches, builds, research
// and shields can run for days and must survive restarts and node moves.
//
// Delivery is at-least-once. A job is stored before it is scheduled and only
// removed after its handler returns nil, so a crash in between re-fires it.
// Handlers must therefore be idempotent, keyed on Job.ID.
//
// Jobs are partitioned into shards, and each shard is owned by exactly one
// Scheduler at a time. Claim returns a fencing token; a stale owner's Complete
// is rejected, so a paused node cannot delete work its successor now owns.
package timer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// ErrFenced means this scheduler lost shard ownership and must stop.
var ErrFenced = errors.New("timer: shard owned by a newer scheduler")

// Job is one durable timer.
type Job struct {
	ID      uint64
	Shard   int
	Kind    string
	Payload []byte
	FireAt  time.Time
	Attempt int
}

// Store persists jobs. MySQL in production, memory in tests.
type Store interface {
	// Claim takes ownership of a shard and returns a fencing token larger
	// than every token issued before.
	Claim(ctx context.Context, shard int, owner string) (token uint64, err error)
	Add(ctx context.Context, j Job) error
	// Due lists a shard's unfinished jobs firing at or before until.
	Due(ctx context.Context, shard int, until time.Time, limit int) ([]Job, error)
	// Complete removes a job. It fails with ErrFenced for a stale token.
	Complete(ctx context.Context, shard int, token uint64, id uint64) error
	// Reschedule moves a failed job for retry, under the same fence.
	Reschedule(ctx context.Context, shard int, token uint64, id uint64, at time.Time, attempt int) error
	// Cancel removes a job, for a recalled march or a sped-up build.
	Cancel(ctx context.Context, shard int, id uint64) error
}

// Handler processes a due job. A non-nil error means retry later.
type Handler func(ctx context.Context, j Job) error

// Config tunes a Scheduler.
type Config struct {
	Owner      string
	Shard      int
	Store      Store
	Poll       time.Duration // How often the store is scanned. Default 1s.
	Lookahead  time.Duration // How far ahead jobs are loaded. Default 2*Poll.
	BatchSize  int           // Default 1000.
	MaxBackoff time.Duration // Default 1m.
	Now        func() time.Time
}

// Scheduler fires one shard's jobs.
type Scheduler struct {
	cfg      Config
	token    uint64
	handlers map[string]Handler
	wake     chan struct{}

	mu      sync.Mutex
	pending map[uint64]*time.Timer // Jobs already armed, to avoid double arming.
	wg      sync.WaitGroup
}

// New creates a Scheduler. Register handlers before Run.
func New(cfg Config) *Scheduler {
	if cfg.Poll <= 0 {
		cfg.Poll = time.Second
	}
	if cfg.Lookahead <= 0 {
		cfg.Lookahead = 2 * cfg.Poll
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 1000
	}
	if cfg.MaxBackoff <= 0 {
		cfg.MaxBackoff = time.Minute
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &Scheduler{
		cfg:      cfg,
		handlers: map[string]Handler{},
		pending:  map[uint64]*time.Timer{},
		wake:     make(chan struct{}, 1),
	}
}

// Handle registers the handler for a job kind.
func (s *Scheduler) Handle(kind string, h Handler) { s.handlers[kind] = h }

// Schedule persists a job, which a scan then arms. A job due within the
// lookahead wakes the scan loop instead of waiting for the next poll.
func (s *Scheduler) Schedule(ctx context.Context, j Job) error {
	j.Shard = s.cfg.Shard
	if err := s.cfg.Store.Add(ctx, j); err != nil {
		return fmt.Errorf("timer: add job %d: %w", j.ID, err)
	}
	if j.FireAt.Sub(s.cfg.Now()) <= s.cfg.Lookahead {
		// Arming happens on Run's goroutine, under Run's context. The caller's
		// ctx is often a request that ends long before the job fires.
		select {
		case s.wake <- struct{}{}:
		default:
		}
	}
	return nil
}

// Run claims the shard and fires jobs until ctx ends or the shard is lost.
// On ctx cancel, armed jobs are abandoned, not lost: they are still stored.
func (s *Scheduler) Run(ctx context.Context) error {
	token, err := s.cfg.Store.Claim(ctx, s.cfg.Shard, s.cfg.Owner)
	if err != nil {
		return fmt.Errorf("timer: claim shard %d: %w", s.cfg.Shard, err)
	}
	s.mu.Lock()
	s.token = token
	s.mu.Unlock()

	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	defer s.stopAll()

	// Catch up immediately: everything overdue from downtime fires now.
	if err := s.scan(ctx, cancel); err != nil {
		slog.Error("timer scan", "shard", s.cfg.Shard, "err", err)
	}
	t := time.NewTicker(s.cfg.Poll)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			if cause := context.Cause(ctx); errors.Is(cause, ErrFenced) {
				return ErrFenced
			}
			return ctx.Err()
		case <-t.C:
		case <-s.wake:
		}
		if err := s.scan(ctx, cancel); err != nil {
			slog.Error("timer scan", "shard", s.cfg.Shard, "err", err)
		}
	}
}

func (s *Scheduler) scan(ctx context.Context, cancel context.CancelCauseFunc) error {
	jobs, err := s.cfg.Store.Due(ctx, s.cfg.Shard, s.cfg.Now().Add(s.cfg.Lookahead), s.cfg.BatchSize)
	if err != nil {
		return err
	}
	for _, j := range jobs {
		s.arm(ctx, cancel, j)
	}
	return nil
}

func (s *Scheduler) arm(ctx context.Context, cancel context.CancelCauseFunc, j Job) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, armed := s.pending[j.ID]; armed {
		return
	}
	delay := j.FireAt.Sub(s.cfg.Now())
	if delay < 0 {
		delay = 0
	}
	s.wg.Add(1)
	s.pending[j.ID] = time.AfterFunc(delay, func() {
		defer s.wg.Done()
		s.fire(ctx, cancel, j)
	})
}

func (s *Scheduler) fire(ctx context.Context, cancel context.CancelCauseFunc, j Job) {
	defer func() {
		s.mu.Lock()
		delete(s.pending, j.ID)
		s.mu.Unlock()
	}()
	if ctx.Err() != nil {
		return
	}
	s.mu.Lock()
	token := s.token
	s.mu.Unlock()

	h, ok := s.handlers[j.Kind]
	err := fmt.Errorf("no handler for kind %q", j.Kind)
	if ok {
		err = s.safeCall(ctx, h, j)
	}
	if err == nil {
		err = s.cfg.Store.Complete(ctx, s.cfg.Shard, token, j.ID)
		if errors.Is(err, ErrFenced) {
			cancel(ErrFenced)
		} else if err != nil {
			slog.Error("timer complete; job will re-fire", "job", j.ID, "err", err)
		}
		return
	}
	slog.Warn("timer job failed", "job", j.ID, "kind", j.Kind, "attempt", j.Attempt, "err", err)
	at := s.cfg.Now().Add(backoff(j.Attempt, s.cfg.MaxBackoff))
	if err := s.cfg.Store.Reschedule(ctx, s.cfg.Shard, token, j.ID, at, j.Attempt+1); errors.Is(err, ErrFenced) {
		cancel(ErrFenced)
	} else if err != nil {
		slog.Error("timer reschedule", "job", j.ID, "err", err)
	}
}

func (s *Scheduler) safeCall(ctx context.Context, h Handler, j Job) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v", r)
		}
	}()
	return h(ctx, j)
}

func (s *Scheduler) stopAll() {
	s.mu.Lock()
	for id, t := range s.pending {
		if t.Stop() {
			s.wg.Done() // The callback will never run.
			delete(s.pending, id)
		}
	}
	s.mu.Unlock()
	s.wg.Wait()
}

func backoff(attempt int, limit time.Duration) time.Duration {
	if attempt > 16 {
		return limit
	}
	d := time.Second << attempt
	if d > limit {
		return limit
	}
	return d
}
