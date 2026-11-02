package mail

import (
	"context"
	"database/sql"
	"errors"

	"github.com/yourname/gameserver/pkg/timer"
)

// MySQL persists mail. timer_job must be in the same database, so that a claim
// and its grant commit together.
type MySQL struct {
	DB *sql.DB
}

// Deliver implements Store. The unique key is the exactly-once point.
func (s *MySQL) Deliver(ctx context.Context, m Mail) (bool, error) {
	res, err := s.DB.ExecContext(ctx,
		`INSERT IGNORE INTO mail_box (id, uid, dedupe_key, kind, title, body, reward_gold, sent_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		m.ID, m.UID, m.Key, m.Kind, m.Title, m.Body, m.RewardGold, m.SentAt.UTC())
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n == 1, err
}

// List implements Store.
func (s *MySQL) List(ctx context.Context, uid int64, limit int) ([]Mail, error) {
	rows, err := s.DB.QueryContext(ctx,
		`SELECT id, dedupe_key, kind, title, body, reward_gold, claimed, sent_at FROM mail_box
		 WHERE uid = ? ORDER BY id DESC LIMIT ?`, uid, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Mail
	for rows.Next() {
		m := Mail{Letter: Letter{UID: uid}}
		if err := rows.Scan(&m.ID, &m.Key, &m.Kind, &m.Title, &m.Body, &m.RewardGold, &m.Claimed, &m.SentAt); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// Claim implements Store. The row lock orders concurrent claims, and the grant
// commits with the flag, so gold is neither doubled nor lost.
func (s *MySQL) Claim(ctx context.Context, uid int64, id uint64) (int64, error) {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback() //nolint:errcheck // No-op after Commit.
	m := Mail{ID: id, Letter: Letter{UID: uid}}
	err = tx.QueryRowContext(ctx, `SELECT reward_gold, claimed FROM mail_box WHERE id = ? AND uid = ? FOR UPDATE`, id, uid).
		Scan(&m.RewardGold, &m.Claimed)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, ErrNotFound
	}
	if err != nil {
		return 0, err
	}
	if m.Claimed {
		return m.RewardGold, nil
	}
	if _, err := tx.ExecContext(ctx, `UPDATE mail_box SET claimed = 1 WHERE id = ?`, id); err != nil {
		return 0, err
	}
	if m.RewardGold > 0 {
		j, err := grantFor(m)
		if err != nil {
			return 0, err
		}
		if err := timer.AddTx(ctx, tx, j); err != nil {
			return 0, err
		}
	}
	return m.RewardGold, tx.Commit()
}
