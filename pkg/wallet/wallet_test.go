package wallet

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/yourname/gameserver/pkg/events"
)

func funded(t *testing.T, amount int64) *Memory {
	t.Helper()
	w := NewMemory(nil)
	if _, err := w.Apply(context.Background(), Change{UID: 1, RequestID: "seed", Delta: amount, Reason: "seed"}); err != nil {
		t.Fatal(err)
	}
	return w
}

func TestRetryDoesNotApplyTwice(t *testing.T) {
	w := funded(t, 100)
	c := Change{UID: 1, RequestID: "r1", Delta: -30, Reason: "bet", Result: []byte("x")}
	if _, err := w.Apply(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	r, err := w.Apply(context.Background(), c)
	if err != nil || !r.Replayed || r.Balance != 70 || string(r.Result) != "x" {
		t.Fatalf("r=%+v err=%v", r, err)
	}
}

func TestConcurrentRetriesApplyOnce(t *testing.T) {
	w := funded(t, 100)
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = w.Apply(context.Background(), Change{UID: 1, RequestID: "same", Delta: -10, Reason: "bet"})
		}()
	}
	wg.Wait()
	if b, _ := w.Balance(context.Background(), 1); b != 90 {
		t.Fatalf("balance %d", b)
	}
}

func TestKeyReuseIsRejected(t *testing.T) {
	w := funded(t, 100)
	ctx := context.Background()
	if _, err := w.Apply(ctx, Change{UID: 1, RequestID: "k", Delta: -1, Reason: "bet"}); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Apply(ctx, Change{UID: 1, RequestID: "k", Delta: -2, Reason: "bet"}); !errors.Is(err, ErrKeyReuse) {
		t.Fatalf("got %v", err)
	}
}

func TestFingerprintLetsDifferentDeltaReplay(t *testing.T) {
	w := funded(t, 100)
	ctx := context.Background()
	first, err := w.Apply(ctx, Change{UID: 1, RequestID: "s", Delta: 40, Reason: "spin", Fingerprint: "bet:10", Result: []byte("a")})
	if err != nil {
		t.Fatal(err)
	}
	// A retry rolled a different outcome. The first one stands.
	r, err := w.Apply(ctx, Change{UID: 1, RequestID: "s", Delta: -10, Reason: "spin", Fingerprint: "bet:10", Result: []byte("b")})
	if err != nil || !r.Replayed || r.Balance != first.Balance || string(r.Result) != "a" {
		t.Fatalf("r=%+v err=%v", r, err)
	}
}

func TestRequireStopsBetFundedByItsOwnWin(t *testing.T) {
	w := funded(t, 5)
	_, err := w.Apply(context.Background(), Change{UID: 1, RequestID: "s", Delta: 90, Require: 10, Reason: "spin"})
	if !errors.Is(err, ErrInsufficient) {
		t.Fatalf("got %v", err)
	}
}

func TestOutboxOnlyOnFirstApply(t *testing.T) {
	box := &events.MemoryStore{}
	w := NewMemory(box)
	c := Change{UID: 1, RequestID: "e", Delta: 1, Reason: "x", Event: &events.Event{Topic: "t"}}
	for i := 0; i < 3; i++ {
		if _, err := w.Apply(context.Background(), c); err != nil {
			t.Fatal(err)
		}
	}
	rows, _ := box.Fetch(context.Background(), 10)
	if len(rows) != 1 {
		t.Fatalf("rows %d", len(rows))
	}
}
