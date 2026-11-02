//go:build integration

package wallet

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/drawmoon2025/gameserver/internal/itest"
	"github.com/drawmoon2025/gameserver/pkg/events"
	"github.com/drawmoon2025/gameserver/pkg/shard"
)

func mysqlWallet(t *testing.T) (*MySQL, *sql.DB) {
	t.Helper()
	db := itest.MySQL(t)
	r, err := shard.NewEven([]*sql.DB{db})
	if err != nil {
		t.Fatal(err)
	}
	return &MySQL{Shards: r}, db
}

func TestMySQLConcurrentRetriesApplyOnce(t *testing.T) {
	ctx := context.Background()
	w, _ := mysqlWallet(t)
	uid := itest.ID()
	if _, err := w.Apply(ctx, Change{UID: uid, RequestID: "seed", Delta: 100, Reason: "seed"}); err != nil {
		t.Fatal(err)
	}
	var (
		wg       sync.WaitGroup
		replayed atomic.Int64
	)
	for range 30 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := w.Apply(ctx, Change{UID: uid, RequestID: "bet", Delta: -10, Reason: "bet", Result: []byte("x")})
			if err != nil {
				t.Error(err)
				return
			}
			if r.Replayed {
				replayed.Add(1)
			}
			if r.Balance != 90 || string(r.Result) != "x" {
				t.Errorf("receipt %+v", r)
			}
		}()
	}
	wg.Wait()
	if b, err := w.Balance(ctx, uid); err != nil || b != 90 {
		t.Fatalf("balance %d: %v", b, err)
	}
	if replayed.Load() != 29 {
		t.Fatalf("replayed %d of 30", replayed.Load())
	}
}

func TestMySQLRacingRetriesOfLastCoinAllSucceed(t *testing.T) {
	ctx := context.Background()
	w, _ := mysqlWallet(t)
	uid := itest.ID()
	if _, err := w.Apply(ctx, Change{UID: uid, RequestID: "seed", Delta: 10, Reason: "seed"}); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for range 30 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := Change{UID: uid, RequestID: "bet", Delta: -10, Require: 10, Reason: "bet"}
			if r, err := w.Apply(ctx, c); err != nil || r.Balance != 0 {
				t.Errorf("receipt %+v: %v", r, err)
			}
		}()
	}
	wg.Wait()
}

func TestMySQLConcurrentBetsNeverOverdraw(t *testing.T) {
	ctx := context.Background()
	w, _ := mysqlWallet(t)
	uid := itest.ID()
	if _, err := w.Apply(ctx, Change{UID: uid, RequestID: "seed", Delta: 50, Reason: "seed"}); err != nil {
		t.Fatal(err)
	}
	var (
		wg       sync.WaitGroup
		accepted atomic.Int64
	)
	for i := range 30 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := w.Apply(ctx, Change{UID: uid, RequestID: fmt.Sprint("bet-", i), Delta: -10, Reason: "bet"})
			switch {
			case err == nil:
				accepted.Add(1)
			case !errors.Is(err, ErrInsufficient):
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if b, err := w.Balance(ctx, uid); err != nil || b != 0 || accepted.Load() != 5 {
		t.Fatalf("balance %d, accepted %d: %v", b, accepted.Load(), err)
	}
}

func TestMySQLFailedChangeLeavesNoTrace(t *testing.T) {
	ctx := context.Background()
	w, db := mysqlWallet(t)
	uid := itest.ID()
	topic := fmt.Sprint("wallet-", uid)
	c := Change{UID: uid, RequestID: "r", Delta: -1, Require: 1, Reason: "bet",
		Event: &events.Event{Topic: topic, Key: "k", Payload: []byte("p")}}
	if _, err := w.Apply(ctx, c); !errors.Is(err, ErrInsufficient) {
		t.Fatalf("got %v", err)
	}
	count := func() (n int) {
		if err := db.QueryRow(`SELECT COUNT(*) FROM event_outbox WHERE topic = ?`, topic).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	if count() != 0 {
		t.Fatal("a rejected change wrote an event")
	}
	if _, err := w.Apply(ctx, Change{UID: uid, RequestID: "seed", Delta: 5, Reason: "seed"}); err != nil {
		t.Fatal(err)
	}
	for range 2 { // The retry must not emit a second event.
		if _, err := w.Apply(ctx, c); err != nil {
			t.Fatal(err)
		}
	}
	if count() != 1 {
		t.Fatalf("%d events", count())
	}
	if _, err := w.Apply(ctx, Change{UID: uid, RequestID: "r", Delta: -2, Require: 1, Reason: "bet"}); !errors.Is(err, ErrKeyReuse) {
		t.Fatalf("reused key: %v", err)
	}
}
