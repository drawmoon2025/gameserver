package timer

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func eventually(t *testing.T, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatal("timed out")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestGroupRunsOnlyStartedShards(t *testing.T) {
	store := NewMemStore()
	g := NewGroup(Config{Owner: "a", Store: store, Poll: 5 * time.Millisecond})
	var fired atomic.Int64
	g.Handle("k", func(context.Context, Job) error {
		fired.Add(1)
		return nil
	})
	ctx := context.Background()
	for shard := range 2 {
		if err := store.Add(ctx, Job{ID: uint64(shard + 1), Shard: shard, Kind: "k", FireAt: time.Now()}); err != nil {
			t.Fatal(err)
		}
	}
	g.Start(ctx, 0)
	g.Start(ctx, 0) // Must not start a second scheduler.
	defer g.StopAll()
	eventually(t, func() bool { return store.Len(0) == 0 })
	time.Sleep(30 * time.Millisecond)
	if fired.Load() != 1 || store.Len(1) != 1 {
		t.Fatalf("fired %d, shard 1 holds %d", fired.Load(), store.Len(1))
	}
}

func TestStoppedShardResumesOnAnotherGroup(t *testing.T) {
	store := NewMemStore()
	ctx := context.Background()
	a := NewGroup(Config{Owner: "a", Store: store, Poll: 5 * time.Millisecond})
	a.Start(ctx, 7)
	a.Stop(7)
	a.Wake(7) // A shard not running here is a no-op.
	if err := store.Add(ctx, Job{ID: 1, Shard: 7, Kind: "k", FireAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	b := NewGroup(Config{Owner: "b", Store: store, Poll: 5 * time.Millisecond})
	b.Handle("k", func(context.Context, Job) error { return nil })
	b.Start(ctx, 7)
	defer b.StopAll()
	eventually(t, func() bool { return store.Len(7) == 0 })
}

// flakyClaim fails its first Claim, as a database restart would.
type flakyClaim struct {
	*MemStore
	failed atomic.Bool
}

func (f *flakyClaim) Claim(ctx context.Context, shard int, owner string) (uint64, error) {
	if f.failed.CompareAndSwap(false, true) {
		return 0, errors.New("database down")
	}
	return f.MemStore.Claim(ctx, shard, owner)
}

func TestGroupRetriesAfterClaimFailure(t *testing.T) {
	store := &flakyClaim{MemStore: NewMemStore()}
	ctx := context.Background()
	if err := store.Add(ctx, Job{ID: 1, Shard: 0, Kind: "k", FireAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	g := NewGroup(Config{Owner: "a", Store: store, Poll: 5 * time.Millisecond})
	g.Handle("k", func(context.Context, Job) error { return nil })
	g.Start(ctx, 0)
	defer g.StopAll()
	eventually(t, func() bool { return store.Len(0) == 0 })
}
