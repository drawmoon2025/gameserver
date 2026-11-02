//go:build integration

package realm

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/yourname/gameserver/internal/itest"
)

func TestMySQLCapacityHoldsUnderConcurrency(t *testing.T) {
	ctx := context.Background()
	m := &MySQL{DB: itest.MySQL(t)}
	id := fmt.Sprint("c", itest.ID()%1_000_000_000)
	if err := m.Put(ctx, Server{ID: id, Name: "n", WSURL: "wss://a", Capacity: 5}); err != nil {
		t.Fatal(err)
	}
	base := itest.ID()
	var (
		wg     sync.WaitGroup
		joined atomic.Int64
	)
	for i := range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			switch _, err := m.Enter(ctx, base+int64(i), id); {
			case err == nil:
				joined.Add(1)
			case !errors.Is(err, ErrFull):
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if joined.Load() != 5 {
		t.Fatalf("%d joined a server of 5", joined.Load())
	}
	list, err := m.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range list {
		if s.ID == id && s.Population != 5 {
			t.Fatalf("population %d", s.Population)
		}
	}
}

func TestMySQLOnePlayerOneServer(t *testing.T) {
	ctx := context.Background()
	m := &MySQL{DB: itest.MySQL(t)}
	a, b := fmt.Sprint("a", itest.ID()%1_000_000_000), fmt.Sprint("b", itest.ID()%1_000_000_000)
	for _, id := range []string{a, b} {
		if err := m.Put(ctx, Server{ID: id, Name: "n", WSURL: "wss://a", Capacity: 5}); err != nil {
			t.Fatal(err)
		}
	}
	uid := itest.ID()
	first, err := m.Enter(ctx, uid, a)
	if err != nil {
		t.Fatal(err)
	}
	if again, err := m.Enter(ctx, uid, a); err != nil || again != first {
		t.Fatalf("repeat %+v: %v", again, err)
	}
	if _, err := m.Enter(ctx, uid, b); !errors.Is(err, ErrAlreadyIn) {
		t.Fatalf("second server: %v", err)
	}
	if got, err := m.MemberOf(ctx, uid); err != nil || got != first {
		t.Fatalf("member %+v: %v", got, err)
	}
	if list, err := m.List(ctx); err == nil {
		for _, s := range list {
			if s.ID == a && s.Population != 1 {
				t.Fatalf("a repeat took a seat: %d", s.Population)
			}
		}
	}
}
