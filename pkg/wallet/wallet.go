// Package wallet moves balances. Every change is one atomic, idempotent ledger
// entry, keyed by (uid, requestID). A retried request returns the original
// result instead of moving money twice.
package wallet

import (
	"context"
	"errors"
	"strconv"

	"github.com/drawmoon2025/gameserver/pkg/events"
)

var (
	ErrInsufficient = errors.New("wallet: insufficient funds")
	// ErrKeyReuse means one request ID was used for a different operation.
	// That is a caller bug and must not be treated as a successful retry.
	ErrKeyReuse = errors.New("wallet: request id reused with different parameters")
)

// Change is one balance movement. Delta may be negative.
type Change struct {
	UID       int64
	RequestID string
	Delta     int64
	// Require is the minimum balance before the change. A spin settles
	// win-bet as one Delta, so without this a player holding 5 could place a
	// bet of 10 and still pass when the win is large enough.
	Require int64
	Reason  string
	// Fingerprint identifies the operation for retry detection. It defaults to
	// Delta. Set it when Delta depends on a roll: a spin retry rolls again, so
	// it fingerprints the bet, and the stored outcome wins.
	Fingerprint string
	// Result is stored opaquely with the entry and returned on retry. Slots
	// stores the spin outcome here, so a retry shows the same reels.
	Result []byte
	// Event, when set, commits atomically with the balance change, so the
	// outbox can never disagree with the ledger.
	Event *events.Event
}

func (c Change) fingerprint() string {
	if c.Fingerprint != "" {
		return c.Fingerprint
	}
	return strconv.FormatInt(c.Delta, 10)
}

// Receipt is the outcome of an applied Change.
type Receipt struct {
	Balance  int64
	Result   []byte
	Replayed bool // True if this was a retry of an applied change.
}

// Wallet is the balance authority. Implementations must apply each change and
// its ledger entry atomically. An operator's seamless wallet implements this
// same interface.
type Wallet interface {
	Apply(ctx context.Context, c Change) (Receipt, error)
	Balance(ctx context.Context, uid int64) (int64, error)
}
