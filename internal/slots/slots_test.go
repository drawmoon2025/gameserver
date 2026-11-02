package slots

import (
	"context"
	"errors"
	"testing"

	"github.com/drawmoon2025/gameserver/pkg/idgen"
	"github.com/drawmoon2025/gameserver/pkg/wallet"
)

func TestRTPIsInRange(t *testing.T) {
	total := 0
	for _, w := range Weights {
		total += w
	}
	var paid, ways float64
	n := int32(len(Weights))
	for a := int32(0); a < n; a++ {
		for b := int32(0); b < n; b++ {
			for c := int32(0); c < n; c++ {
				w := float64(Weights[a] * Weights[b] * Weights[c])
				paid += w * float64(Evaluate([]int32{a, b, c}, 1))
				ways += w
			}
		}
	}
	rtp := paid / ways
	t.Logf("exact RTP = %.4f", rtp)
	if rtp < 0.85 || rtp > 0.98 {
		t.Fatalf("RTP %.4f outside the sample model's range", rtp)
	}
}

func newService(t *testing.T, balance int64) *Service {
	t.Helper()
	w := wallet.NewMemory(nil)
	if _, err := w.Apply(context.Background(), wallet.Change{UID: 1, RequestID: "seed", Delta: balance, Reason: "seed"}); err != nil {
		t.Fatal(err)
	}
	ids, err := idgen.New(1)
	if err != nil {
		t.Fatal(err)
	}
	return &Service{Wallet: w, IDs: ids}
}

func TestRetryReturnsOriginalOutcome(t *testing.T) {
	s := newService(t, 1000)
	ctx := context.Background()
	// Each retry rolls fresh reels, so repeat until a difference would show.
	first, err := s.Spin(ctx, 1, "req", 10)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 200; i++ {
		again, err := s.Spin(ctx, 1, "req", 10)
		if err != nil {
			t.Fatal(err)
		}
		if again.GetRoundId() != first.GetRoundId() || again.GetBalance() != first.GetBalance() || again.GetWin() != first.GetWin() {
			t.Fatalf("retry changed the outcome: %v vs %v", again, first)
		}
	}
}

func TestRetryWithDifferentBetIsRejected(t *testing.T) {
	s := newService(t, 1000)
	ctx := context.Background()
	if _, err := s.Spin(ctx, 1, "req", 10); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Spin(ctx, 1, "req", 20); !errors.Is(err, wallet.ErrKeyReuse) {
		t.Fatalf("got %v", err)
	}
}

func TestCannotBetMoreThanBalanceEvenOnWin(t *testing.T) {
	s := newService(t, 5)
	s.RNG = func(int) (int, error) { return 0, nil } // Three top symbols: a huge win.
	if _, err := s.Spin(context.Background(), 1, "req", 10); !errors.Is(err, wallet.ErrInsufficient) {
		t.Fatalf("got %v", err)
	}
}

func TestBadBets(t *testing.T) {
	s := newService(t, 5)
	for _, bet := range []int64{0, -1, maxBet + 1} {
		if _, err := s.Spin(context.Background(), 1, "r", bet); !errors.Is(err, errBadRequest) {
			t.Fatalf("bet %d: %v", bet, err)
		}
	}
	if _, err := s.Spin(context.Background(), 1, "", 1); !errors.Is(err, errBadRequest) {
		t.Fatal("missing request id must fail")
	}
}
