package slots

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	gamev1 "github.com/yourname/gameserver/gen/game/v1"
	"github.com/yourname/gameserver/pkg/auth"
	"github.com/yourname/gameserver/pkg/session"
)

type downEpochs struct{}

func (downEpochs) Next(context.Context, int64) (uint64, error) { return 0, errors.New("down") }
func (downEpochs) Current(context.Context, int64) (uint64, error) {
	return 0, errors.New("down")
}

func spinHTTP(t *testing.T, s *Service, token string) int {
	t.Helper()
	p, err := proto.Marshal(&gamev1.SpinReq{Bet: 1})
	if err != nil {
		t.Fatal(err)
	}
	body, err := proto.Marshal(&gamev1.Envelope{RequestId: "req", Payload: p})
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	s.Routes(mux)
	req := httptest.NewRequest(http.MethodPost, "/v1/slots/spin", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	return rec.Code
}

func signed(t *testing.T, signer *auth.Signer, epoch uint64) string {
	t.Helper()
	tok, _, err := signer.Issue(1, "s", epoch)
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

func TestSupersededTokenCannotSpin(t *testing.T) {
	s := newService(t, 100)
	s.Signer = &auth.Signer{Key: []byte("0123456789abcdef0123456789abcdef"), TTL: time.Hour}
	epochs := session.NewMemory()
	s.Epochs = epochs
	ctx := context.Background()
	first, err := epochs.Next(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	stale := signed(t, s.Signer, first)
	if code := spinHTTP(t, s, stale); code != http.StatusOK {
		t.Fatalf("current token: %d", code)
	}

	second, err := epochs.Next(ctx, 1) // The player logs in again.
	if err != nil {
		t.Fatal(err)
	}
	if code := spinHTTP(t, s, stale); code != http.StatusUnauthorized {
		t.Fatalf("superseded token: %d", code)
	}
	if code := spinHTTP(t, s, signed(t, s.Signer, second)); code != http.StatusOK {
		t.Fatalf("new token: %d", code)
	}
}

func TestSpinFailsClosedWhenEpochsUnavailable(t *testing.T) {
	s := newService(t, 100)
	s.Signer = &auth.Signer{Key: []byte("0123456789abcdef0123456789abcdef"), TTL: time.Hour}
	s.Epochs = downEpochs{}
	if code := spinHTTP(t, s, signed(t, s.Signer, 1)); code != http.StatusServiceUnavailable {
		t.Fatalf("got %d", code)
	}
}
