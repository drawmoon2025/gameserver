package timer

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// MySQLStore persists jobs. Schema: deploy/mysql/001_core.sql.
type MySQLStore struct {
	DB *sql.DB
}

// Claim implements Store. The upsert increments atomically, so concurrent
// claimers always receive different tokens.
func (m *MySQLStore) Claim(ctx context.Context, shard int, owner string) (uint64, error) {
	tx, err := m.DB.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback() //nolint:errcheck // No-op after Commit.
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO timer_shard (shard, token, owner) VALUES (?, 1, ?)
		 ON DUPLICATE KEY UPDATE token = token + 1, owner = VALUES(owner)`, shard, owner); err != nil {
		return 0, err
	}
	var token uint64
	if err := tx.QueryRowContext(ctx,
		`SELECT token FROM timer_shard WHERE shard = ?`, shard).Scan(&token); err != nil {
		return 0, err
	}
	return token, tx.Commit()
}

// Add implements Store. Re-adding an ID keeps the first copy.
func (m *MySQLStore) Add(ctx context.Context, j Job) error {
	_, err := m.DB.ExecContext(ctx,
		`INSERT IGNORE INTO timer_job (id, shard, kind, payload, fire_at, attempt) VALUES (?, ?, ?, ?, ?, ?)`,
		j.ID, j.Shard, j.Kind, j.Payload, j.FireAt.UTC(), j.Attempt)
	return err
}

// Due implements Store.
func (m *MySQLStore) Due(ctx context.Context, shard int, until time.Time, limit int) ([]Job, error) {
	rows, err := m.DB.QueryContext(ctx,
		`SELECT id, kind, payload, fire_at, attempt FROM timer_job
		 WHERE shard = ? AND fire_at <= ? ORDER BY fire_at LIMIT ?`, shard, until.UTC(), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Job
	for rows.Next() {
		j := Job{Shard: shard}
		if err := rows.Scan(&j.ID, &j.Kind, &j.Payload, &j.FireAt, &j.Attempt); err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

// fenced runs fn in a transaction holding the shard row, so the token check
// and the write cannot interleave with a new owner's Claim.
func (m *MySQLStore) fenced(ctx context.Context, shard int, token uint64, fn func(*sql.Tx) error) error {
	tx, err := m.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck // No-op after Commit.
	var cur uint64
	err = tx.QueryRowContext(ctx, `SELECT token FROM timer_shard WHERE shard = ? FOR UPDATE`, shard).Scan(&cur)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrFenced
	}
	if err != nil {
		return fmt.Errorf("timer: read fence: %w", err)
	}
	if cur != token {
		return ErrFenced
	}
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

// Complete implements Store.
func (m *MySQLStore) Complete(ctx context.Context, shard int, token, id uint64) error {
	return m.fenced(ctx, shard, token, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `DELETE FROM timer_job WHERE shard = ? AND id = ?`, shard, id)
		return err
	})
}

// Reschedule implements Store.
func (m *MySQLStore) Reschedule(ctx context.Context, shard int, token, id uint64, at time.Time, attempt int) error {
	return m.fenced(ctx, shard, token, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx,
			`UPDATE timer_job SET fire_at = ?, attempt = ? WHERE shard = ? AND id = ?`,
			at.UTC(), attempt, shard, id)
		return err
	})
}

// Cancel implements Store.
func (m *MySQLStore) Cancel(ctx context.Context, shard int, id uint64) error {
	_, err := m.DB.ExecContext(ctx, `DELETE FROM timer_job WHERE shard = ? AND id = ?`, shard, id)
	return err
}
