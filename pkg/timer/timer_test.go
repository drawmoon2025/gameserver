package timer

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met")
}

func newSched(st Store) *Scheduler {
	return New(Config{Owner: "t", Store: st, Poll: 10 * time.Millisecond, MaxBackoff: 10 * time.Millisecond})
}

func TestOverdueJobFiresAfterRestart(t *testing.T) {
	st := NewMemStore()
	// Stored by a previous process, due while no scheduler was running.
	if err := st.Add(context.Background(), Job{ID: 1, Kind: "k", FireAt: time.Now().Add(-time.Hour)}); err != nil {
		t.Fatal(err)
	}
	var fired atomic.Int32
	s := newSched(st)
	s.Handle("k", func(context.Context, Job) error { fired.Add(1); return nil })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.Run(ctx) //nolint:errcheck // Ends with the test.
	waitFor(t, func() bool { return st.Len(0) == 0 })
	if fired.Load() != 1 {
		t.Fatalf("fired %d times, want 1", fired.Load())
	}
}

func TestScheduleOutlivesCallerContext(t *testing.T) {
	st := NewMemStore()
	var fired atomic.Int32
	s := newSched(st)
	s.Handle("k", func(context.Context, Job) error { fired.Add(1); return nil })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.Run(ctx) //nolint:errcheck // Ends with the test.

	req, endReq := context.WithCancel(context.Background())
	if err := s.Schedule(req, Job{ID: 2, Kind: "k", FireAt: time.Now().Add(30 * time.Millisecond)}); err != nil {
		t.Fatal(err)
	}
	endReq() // The HTTP request is over long before the job is due.
	waitFor(t, func() bool { return fired.Load() == 1 })
}

func TestFailedJobRetriesThenCompletes(t *testing.T) {
	st := NewMemStore()
	var calls atomic.Int32
	s := newSched(st)
	s.Handle("k", func(context.Context, Job) error {
		if calls.Add(1) < 3 {
			return errors.New("boom")
		}
		return nil
	})
	if err := st.Add(context.Background(), Job{ID: 3, Kind: "k", FireAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.Run(ctx) //nolint:errcheck // Ends with the test.
	waitFor(t, func() bool { return st.Len(0) == 0 })
	if calls.Load() != 3 {
		t.Fatalf("calls %d, want 3", calls.Load())
	}
}

func TestPanickingHandlerDoesNotLoseJob(t *testing.T) {
	st := NewMemStore()
	var calls atomic.Int32
	s := newSched(st)
	s.Handle("k", func(context.Context, Job) error {
		if calls.Add(1) == 1 {
			panic("boom")
		}
		return nil
	})
	if err := st.Add(context.Background(), Job{ID: 4, Kind: "k", FireAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.Run(ctx) //nolint:errcheck // Ends with the test.
	waitFor(t, func() bool { return st.Len(0) == 0 })
}

func TestStaleOwnerIsFenced(t *testing.T) {
	st := NewMemStore()
	ctx := context.Background()
	old, _ := st.Claim(ctx, 0, "a")
	if err := st.Add(ctx, Job{ID: 5, Kind: "k", FireAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Claim(ctx, 0, "b"); err != nil {
		t.Fatal(err)
	}
	if err := st.Complete(ctx, 0, old, 5); !errors.Is(err, ErrFenced) {
		t.Fatalf("got %v, want ErrFenced", err)
	}
	if st.Len(0) != 1 {
		t.Fatal("stale owner deleted a job")
	}
}

func TestRunStopsWhenFenced(t *testing.T) {
	st := NewMemStore()
	s := newSched(st)
	s.Handle("k", func(ctx context.Context, _ Job) error {
		_, err := st.Claim(ctx, 0, "usurper") // Lose ownership mid-job.
		return err
	})
	if err := st.Add(context.Background(), Job{ID: 6, Kind: "k", FireAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- s.Run(context.Background()) }()
	select {
	case err := <-done:
		if !errors.Is(err, ErrFenced) {
			t.Fatalf("got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not stop")
	}
}
