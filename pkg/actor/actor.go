// Package actor runs state on a single goroutine fed by a bounded mailbox.
// Rooms, SLG world zones and online players are actors, so their logic never
// needs a lock.
package actor

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
	"time"
)

var (
	// ErrMailboxFull is returned instead of blocking the caller. A stalled
	// actor must never stall the gateway tunnel feeding it.
	ErrMailboxFull = errors.New("actor: mailbox full")
	ErrStopped     = errors.New("actor: stopped")
)

// Actor owns one mailbox and one goroutine.
type Actor struct {
	name    string
	mailbox chan func()
	stopCh  chan struct{}
	doneCh  chan struct{}

	// mu makes "not stopped" and the mailbox send one step for Tell, so nothing
	// is accepted after the drain has finished.
	mu      sync.RWMutex
	stopped bool
}

// Spawn starts an actor. mailboxSize bounds queued work.
func Spawn(name string, mailboxSize int) *Actor {
	if mailboxSize <= 0 {
		mailboxSize = 1024
	}
	a := &Actor{
		name:    name,
		mailbox: make(chan func(), mailboxSize),
		stopCh:  make(chan struct{}),
		doneCh:  make(chan struct{}),
	}
	go a.run()
	return a
}

func (a *Actor) run() {
	defer close(a.doneCh)
	for {
		select {
		case fn := <-a.mailbox:
			a.exec(fn)
		case <-a.stopCh:
			// Drain what was accepted, so an accepted Ask always completes.
			for {
				select {
				case fn := <-a.mailbox:
					a.exec(fn)
				default:
					return
				}
			}
		}
	}
}

func (a *Actor) exec(fn func()) {
	defer func() {
		// One bad message must not kill a room or a whole world zone.
		if r := recover(); r != nil {
			slog.Error("actor panic", "actor", a.name, "panic", fmt.Sprint(r), "stack", string(debug.Stack()))
		}
	}()
	fn()
}

// Tell enqueues fn without blocking.
func (a *Actor) Tell(fn func()) error {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if a.stopped {
		return ErrStopped
	}
	select {
	case a.mailbox <- fn:
		return nil
	default:
		return ErrMailboxFull
	}
}

// Post is Tell for work that must not be lost to a full mailbox, such as
// cleanup. It never blocks the caller; it retries until accepted or stopped.
func (a *Actor) Post(fn func()) {
	if !errors.Is(a.Tell(fn), ErrMailboxFull) {
		return
	}
	go func() {
		for errors.Is(a.Tell(fn), ErrMailboxFull) {
			time.Sleep(20 * time.Millisecond)
		}
	}()
}

// Ask runs fn on the actor and waits for its result. Never call Ask from
// inside an actor at another actor that may Ask back: that deadlocks.
// Cross-actor flows use Tell plus a reply Tell.
func Ask[T any](ctx context.Context, a *Actor, fn func() (T, error)) (T, error) {
	type result struct {
		v   T
		err error
	}
	var zero T
	ch := make(chan result, 1)
	err := a.Tell(func() {
		defer func() {
			if r := recover(); r != nil {
				ch <- result{err: fmt.Errorf("actor %s: panic: %v", a.name, r)}
				panic(r) // Let exec log the stack.
			}
		}()
		v, err := fn()
		ch <- result{v: v, err: err}
	})
	if err != nil {
		return zero, err
	}
	select {
	case r := <-ch:
		return r.v, r.err
	case <-ctx.Done():
		return zero, ctx.Err()
	}
}

// After schedules fn on the actor. This is for volatile timers only, such as
// a turn countdown: it dies with the process. Anything that must survive a
// restart (SLG marches, builds) belongs in pkg/timer.
func (a *Actor) After(d time.Duration, fn func()) *time.Timer {
	return time.AfterFunc(d, func() { a.Post(fn) })
}

// Stop rejects new work, drains accepted work and waits for the goroutine.
func (a *Actor) Stop() {
	a.mu.Lock()
	if !a.stopped {
		a.stopped = true
		close(a.stopCh)
	}
	a.mu.Unlock()
	<-a.doneCh
}
