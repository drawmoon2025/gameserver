package wallet

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/go-sql-driver/mysql"

	"github.com/yourname/gameserver/pkg/shard"
)

const mysqlDuplicateEntry = 1062

// MySQL is the production Wallet. Schema: deploy/mysql/001_core.sql.
//
// The conditional UPDATE makes the balance check and the change one atomic
// statement. The ledger's unique key (uid, request_id) makes a concurrent
// retry fail and roll back, after which it reads the winner's entry.
type MySQL struct {
	Shards *shard.Router
}

// Apply implements Wallet.
func (w *MySQL) Apply(ctx context.Context, c Change) (Receipt, error) {
	db := w.Shards.For(c.UID)
	r, err := apply(ctx, db, c)
	var dup *mysql.MySQLError
	if errors.As(err, &dup) && dup.Number == mysqlDuplicateEntry {
		return replay(ctx, db, c)
	}
	return r, err
}

func apply(ctx context.Context, db *sql.DB, c Change) (Receipt, error) {
	// Fast path for retries. It also stops a retry reporting ErrInsufficient
	// when the balance has since been spent.
	if r, err := replay(ctx, db, c); !errors.Is(err, sql.ErrNoRows) {
		return r, err
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return Receipt{}, fmt.Errorf("wallet: begin: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck // No-op after Commit.

	if _, err := tx.ExecContext(ctx,
		`INSERT IGNORE INTO wallet_account (uid, balance) VALUES (?, 0)`, c.UID); err != nil {
		return Receipt{}, fmt.Errorf("wallet: ensure account: %w", err)
	}
	// Lock the row so the balance read below is the one this change produced.
	var cur int64
	if err := tx.QueryRowContext(ctx,
		`SELECT balance FROM wallet_account WHERE uid = ? FOR UPDATE`, c.UID).Scan(&cur); err != nil {
		return Receipt{}, fmt.Errorf("wallet: lock account: %w", err)
	}
	next := cur + c.Delta
	if cur < c.Require || next < 0 {
		return Receipt{}, ErrInsufficient
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE wallet_account SET balance = ? WHERE uid = ?`, next, c.UID); err != nil {
		return Receipt{}, fmt.Errorf("wallet: update: %w", err)
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO wallet_ledger (uid, request_id, delta, reason, fingerprint, balance_after, result)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		c.UID, c.RequestID, c.Delta, c.Reason, c.fingerprint(), next, c.Result); err != nil {
		return Receipt{}, err // Keep the driver error: Apply checks for a duplicate.
	}
	if c.Event != nil {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO event_outbox (topic, partition_key, payload) VALUES (?, ?, ?)`,
			c.Event.Topic, c.Event.Key, c.Event.Payload); err != nil {
			return Receipt{}, fmt.Errorf("wallet: outbox: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return Receipt{}, fmt.Errorf("wallet: commit: %w", err)
	}
	return Receipt{Balance: next, Result: c.Result}, nil
}

func replay(ctx context.Context, db *sql.DB, c Change) (Receipt, error) {
	var (
		reason      string
		fingerprint string
		balance     int64
		result      []byte
	)
	err := db.QueryRowContext(ctx,
		`SELECT reason, fingerprint, balance_after, result FROM wallet_ledger
		 WHERE uid = ? AND request_id = ?`, c.UID, c.RequestID).
		Scan(&reason, &fingerprint, &balance, &result)
	if errors.Is(err, sql.ErrNoRows) {
		return Receipt{}, sql.ErrNoRows
	}
	if err != nil {
		return Receipt{}, fmt.Errorf("wallet: read ledger: %w", err)
	}
	if fingerprint != c.fingerprint() || reason != c.Reason {
		return Receipt{}, ErrKeyReuse
	}
	return Receipt{Balance: balance, Result: result, Replayed: true}, nil
}

// Balance implements Wallet.
func (w *MySQL) Balance(ctx context.Context, uid int64) (int64, error) {
	var balance int64
	err := w.Shards.For(uid).QueryRowContext(ctx,
		`SELECT balance FROM wallet_account WHERE uid = ?`, uid).Scan(&balance)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("wallet: balance: %w", err)
	}
	return balance, nil
}
