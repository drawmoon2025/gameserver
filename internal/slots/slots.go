// Package slots is a stateless HTTP game: any instance can serve any spin, so
// it scales by adding instances. The RNG draws on the server, and the debit,
// the payout and the outcome commit as one wallet entry.
package slots

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net/http"
	"strings"

	"google.golang.org/protobuf/proto"

	gamev1 "github.com/drawmoon2025/gameserver/gen/game/v1"
	"github.com/drawmoon2025/gameserver/pkg/auth"
	"github.com/drawmoon2025/gameserver/pkg/events"
	"github.com/drawmoon2025/gameserver/pkg/idgen"
	"github.com/drawmoon2025/gameserver/pkg/session"
	"github.com/drawmoon2025/gameserver/pkg/wallet"
)

// Reel weights and pays are sample data. Replace them with a certified math
// model before any real-money launch.
var (
	Weights = []int{3, 5, 8, 12, 16, 20}
	// Pays is the multiplier for three of a kind, by symbol.
	Pays = []int64{100, 40, 20, 10, 5, 2}
	// PairPay is the multiplier for exactly two matching adjacent symbols
	// starting at reel 0.
	PairPay int64 = 4
)

const maxBet int64 = 1_000_000

// Result is one spin outcome.
type Result struct {
	Reels []int32
	Win   int64
}

// Evaluate pays a set of reels. Pure, so the RTP test can enumerate it.
func Evaluate(reels []int32, bet int64) int64 {
	if reels[0] == reels[1] && reels[1] == reels[2] {
		return bet * Pays[reels[0]]
	}
	if reels[0] == reels[1] {
		return bet * PairPay
	}
	return 0
}

// RNG draws an integer in [0, n).
type RNG func(n int) (int, error)

// CryptoRNG uses the OS CSPRNG. math/rand is predictable from its output,
// which is unacceptable in a value-moving game.
func CryptoRNG(n int) (int, error) {
	v, err := rand.Int(rand.Reader, big.NewInt(int64(n)))
	if err != nil {
		return 0, err
	}
	return int(v.Int64()), nil
}

func spin(rng RNG, bet int64) (Result, error) {
	total := 0
	for _, w := range Weights {
		total += w
	}
	reels := make([]int32, 3)
	for i := range reels {
		v, err := rng(total)
		if err != nil {
			return Result{}, err
		}
		for sym, w := range Weights {
			if v < w {
				reels[i] = int32(sym)
				break
			}
			v -= w
		}
	}
	return Result{Reels: reels, Win: Evaluate(reels, bet)}, nil
}

// Service handles spins.
type Service struct {
	Wallet wallet.Wallet
	Signer *auth.Signer
	Epochs session.Epochs
	IDs    *idgen.Generator
	RNG    RNG
}

// Routes returns the HTTP handlers.
func (s *Service) Routes(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/slots/spin", s.handleSpin)
}

// Spin plays one round. A retry with the same requestID returns the original
// outcome. Racing retries roll different reels, but only the entry that
// commits counts, and the loser reads that entry back.
func (s *Service) Spin(ctx context.Context, uid int64, requestID string, bet int64) (*gamev1.SpinRsp, error) {
	if requestID == "" || bet <= 0 || bet > maxBet {
		return nil, errBadRequest
	}
	rng := s.RNG
	if rng == nil {
		rng = CryptoRNG
	}
	res, err := spin(rng, bet)
	if err != nil {
		return nil, fmt.Errorf("slots: rng: %w", err)
	}
	roundID := uint64(s.IDs.Next())
	result, err := proto.Marshal(&gamev1.SpinRsp{Reels: res.Reels, Win: res.Win, RoundId: roundID})
	if err != nil {
		return nil, err
	}
	r, err := s.Wallet.Apply(ctx, wallet.Change{
		UID: uid, RequestID: requestID, Delta: res.Win - bet, Require: bet,
		Reason: "slots.spin", Fingerprint: fmt.Sprint("bet:", bet), Result: result,
		Event: &events.Event{Topic: "slots.round", Key: fmt.Sprint(uid), Payload: result},
	})
	if err != nil {
		return nil, err
	}
	out := &gamev1.SpinRsp{}
	if err := proto.Unmarshal(r.Result, out); err != nil {
		return nil, fmt.Errorf("slots: stored result: %w", err)
	}
	out.Balance = r.Balance
	return out, nil
}

var errBadRequest = errors.New("slots: bad request")

func (s *Service) handleSpin(w http.ResponseWriter, r *http.Request) {
	// Read first. A client may stall its body for as long as the server allows,
	// and the login must still be current when the bet is placed.
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 4<<10))
	if err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	claims, err := s.Signer.Verify(token)
	if err != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	// A spin moves value, so a superseded login must not be able to bet. This
	// fails closed: with Redis down, spins stop rather than skip the check.
	cur, err := s.Epochs.Current(r.Context(), claims.UID)
	if err != nil {
		slog.Error("spin epoch check", "uid", claims.UID, "err", err)
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
		return
	}
	if claims.Epoch != cur {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	env, req := &gamev1.Envelope{}, &gamev1.SpinReq{}
	if proto.Unmarshal(body, env) != nil || proto.Unmarshal(env.GetPayload(), req) != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	out := &gamev1.Envelope{MsgId: uint32(gamev1.MsgId_MSG_ID_SPIN_RSP), RequestId: env.GetRequestId()}
	rsp, err := s.Spin(r.Context(), claims.UID, env.GetRequestId(), req.GetBet())
	switch {
	case err == nil:
		if out.Payload, err = proto.Marshal(rsp); err != nil {
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
	case errors.Is(err, errBadRequest):
		out.Code = int32(gamev1.ErrorCode_ERROR_CODE_BAD_REQUEST)
	case errors.Is(err, wallet.ErrInsufficient):
		out.Code = int32(gamev1.ErrorCode_ERROR_CODE_INSUFFICIENT_FUNDS)
	default:
		slog.Error("spin", "uid", claims.UID, "err", err)
		out.Code = int32(gamev1.ErrorCode_ERROR_CODE_INTERNAL)
	}
	b, err := proto.Marshal(out)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/x-protobuf")
	_, _ = w.Write(b)
}
