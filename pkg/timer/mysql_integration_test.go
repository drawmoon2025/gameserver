//go:build integration

package timer

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/drawmoon2025/gameserver/internal/itest"
)

func TestMySQLStoreFencingAndLifecycle(t *testing.T) {
	ctx := context.Background()
	m := &MySQLStore{DB: itest.MySQL(t)}
	shard, id := int(itest.ID()%1_000_000_000), uint64(itest.ID())

	old, err := m.Claim(ctx, shard, "a")
	if err != nil {
		t.Fatal(err)
	}
	cur, err := m.Claim(ctx, shard, "b")
	if err != nil || cur <= old {
		t.Fatalf("tokens %d then %d: %v", old, cur, err)
	}
	fire := time.Now().Add(-time.Second).Truncate(time.Millisecond)
	if err := m.Add(ctx, Job{ID: id, Shard: shard, Kind: "k", Payload: []byte("p"), FireAt: fire}); err != nil {
		t.Fatal(err)
	}
	if err := m.Add(ctx, Job{ID: id, Shard: shard, Kind: "other", Payload: []byte("q"), FireAt: fire}); err != nil {
		t.Fatal(err)
	}
	due, err := m.Due(ctx, shard, time.Now(), 10)
	if err != nil || len(due) != 1 {
		t.Fatalf("due %v: %v", due, err)
	}
	if j := due[0]; j.Kind != "k" || string(j.Payload) != "p" || !j.FireAt.Equal(fire) {
		t.Fatalf("job %+v, fire %v", j, fire)
	}

	if err := m.Complete(ctx, shard, old, id); !errors.Is(err, ErrFenced) {
		t.Fatalf("stale complete: %v", err)
	}
	if err := m.Reschedule(ctx, shard, old, id, fire, 1); !errors.Is(err, ErrFenced) {
		t.Fatalf("stale reschedule: %v", err)
	}
	later := time.Now().Add(time.Hour)
	if err := m.Reschedule(ctx, shard, cur, id, later, 3); err != nil {
		t.Fatal(err)
	}
	if due, _ := m.Due(ctx, shard, time.Now(), 10); len(due) != 0 {
		t.Fatalf("rescheduled job still due: %v", due)
	}
	due, err = m.Due(ctx, shard, later.Add(time.Minute), 10)
	if err != nil || len(due) != 1 || due[0].Attempt != 3 {
		t.Fatalf("after reschedule %v: %v", due, err)
	}
	if err := m.Complete(ctx, shard, cur, id); err != nil {
		t.Fatal(err)
	}
	if due, _ := m.Due(ctx, shard, later.Add(time.Minute), 10); len(due) != 0 {
		t.Fatalf("completed job remains: %v", due)
	}
}

func TestMySQLConcurrentClaimsGetDistinctTokens(t *testing.T) {
	ctx := context.Background()
	m := &MySQLStore{DB: itest.MySQL(t)}
	shard := int(itest.ID() % 1_000_000_000)
	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		seen = map[uint64]bool{}
	)
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tok, err := m.Claim(ctx, shard, "x")
			if err != nil {
				t.Error(err)
				return
			}
			mu.Lock()
			defer mu.Unlock()
			if seen[tok] {
				t.Errorf("token %d issued twice", tok)
			}
			seen[tok] = true
		}()
	}
	wg.Wait()
}

func TestSchedulerFiresFromMySQL(t *testing.T) {
	m := &MySQLStore{DB: itest.MySQL(t)}
	shard, id := int(itest.ID()%1_000_000_000), uint64(itest.ID())
	g := NewGroup(Config{Owner: "t", Store: m, Poll: 20 * time.Millisecond})
	fired := make(chan uint64, 4)
	g.Handle("k", func(_ context.Context, j Job) error {
		fired <- j.ID
		return nil
	})
	g.Start(context.Background(), shard)
	defer g.StopAll()
	if err := m.Add(context.Background(), Job{ID: id, Shard: shard, Kind: "k", Payload: []byte{}, FireAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-fired:
		if got != id {
			t.Fatalf("fired %d", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("job never fired")
	}
}
