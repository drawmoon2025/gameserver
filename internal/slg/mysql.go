package slg

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/go-sql-driver/mysql"

	"github.com/yourname/gameserver/pkg/timer"
)

// MySQLStore persists marches. The kingdom's database holds them all, and it
// must also hold timer_job.
type MySQLStore struct {
	DB *sql.DB
}

// Create implements Store.
func (s *MySQLStore) Create(ctx context.Context, m March, arrive timer.Job) (March, error) {
	err := s.create(ctx, m, arrive)
	var dup *mysql.MySQLError
	if !errors.As(err, &dup) || dup.Number != 1062 {
		return m, err
	}
	first := March{UID: m.UID, RequestID: m.RequestID}
	rerr := s.DB.QueryRowContext(ctx,
		`SELECT id, kingdom, from_x, from_y, to_x, to_y, troops, arrive_at FROM slg_march
		 WHERE uid = ? AND request_id = ?`, m.UID, m.RequestID).
		Scan(&first.ID, &first.Kingdom, &first.FromX, &first.FromY, &first.ToX, &first.ToY, &first.Troops, &first.ArriveAt)
	if errors.Is(rerr, sql.ErrNoRows) {
		return March{}, err // The duplicate was something else, such as the timer.
	}
	if rerr != nil {
		return March{}, rerr
	}
	return first, nil
}

func (s *MySQLStore) create(ctx context.Context, m March, arrive timer.Job) error {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck // No-op after Commit.
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO slg_march (id, kingdom, uid, request_id, from_x, from_y, to_x, to_y, troops, arrive_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		m.ID, m.Kingdom, m.UID, m.RequestID, m.FromX, m.FromY, m.ToX, m.ToY, m.Troops, m.ArriveAt.UTC()); err != nil {
		return err
	}
	if err := timer.AddTx(ctx, tx, arrive); err != nil {
		return err
	}
	return tx.Commit()
}

// Troops implements Store.
func (s *MySQLStore) Troops(ctx context.Context, kingdom string, uid int64) (int64, error) {
	var total int64
	err := s.DB.QueryRowContext(ctx,
		`SELECT COALESCE(SUM(troops), 0) FROM slg_tile WHERE uid = ? AND kingdom = ?`,
		uid, kingdom).Scan(&total)
	return total, err
}

// Arrive implements Store. The state guard in the UPDATE is the exactly-once
// point: of any number of re-fires, one sees a changed row.
func (s *MySQLStore) Arrive(ctx context.Context, id uint64) (March, bool, error) {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return March{}, false, err
	}
	defer tx.Rollback() //nolint:errcheck // No-op after Commit.
	res, err := tx.ExecContext(ctx, `UPDATE slg_march SET state = 1 WHERE id = ? AND state = 0`, id)
	if err != nil {
		return March{}, false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return March{}, false, err
	}
	m := March{ID: id}
	err = tx.QueryRowContext(ctx,
		`SELECT kingdom, uid, from_x, from_y, to_x, to_y, troops, arrive_at FROM slg_march WHERE id = ?`, id).
		Scan(&m.Kingdom, &m.UID, &m.FromX, &m.FromY, &m.ToX, &m.ToY, &m.Troops, &m.ArriveAt)
	if errors.Is(err, sql.ErrNoRows) {
		return March{}, false, fmt.Errorf("slg: unknown march %d", id)
	}
	if err != nil {
		return March{}, false, err
	}
	if n == 0 {
		return m, false, nil
	}

	var t tile
	err = tx.QueryRowContext(ctx, `SELECT uid, troops FROM slg_tile WHERE kingdom = ? AND x = ? AND y = ? FOR UPDATE`,
		m.Kingdom, m.ToX, m.ToY).Scan(&t.Owner, &t.Troops)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return March{}, false, err
	}
	after, left, jobs, err := settle(m, t)
	if err != nil {
		return March{}, false, err
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO slg_tile (kingdom, x, y, uid, troops) VALUES (?, ?, ?, ?, ?)
		 ON DUPLICATE KEY UPDATE uid = VALUES(uid), troops = VALUES(troops)`,
		m.Kingdom, m.ToX, m.ToY, after.Owner, after.Troops); err != nil {
		return March{}, false, err
	}
	for _, j := range jobs {
		if err := timer.AddTx(ctx, tx, j); err != nil {
			return March{}, false, err
		}
	}
	if err := tx.Commit(); err != nil {
		return March{}, false, err
	}
	m.Troops = left
	return m, true, nil
}
