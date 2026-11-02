package slg

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/yourname/gameserver/internal/city"
	"github.com/yourname/gameserver/pkg/timer"
)

// MySQLStore persists marches. The kingdom's database holds them all, and it
// must also hold timer_job.
type MySQLStore struct {
	DB *sql.DB
}

// Create implements Store. Unlike a tile, a city may not exist yet: it appears
// on first use, and must be created outside this transaction.
func (s *MySQLStore) Create(ctx context.Context, m March, arrive timer.Job) (March, error) {
	take := func(tx *sql.Tx) error { return city.TakeTroopsTx(ctx, tx, m.UID, m.Troops) }
	got, err := s.place(ctx, m, arrive, take)
	if !errors.Is(err, city.ErrNoCity) {
		return got, err
	}
	if err := (&city.MySQL{DB: s.DB}).Ensure(ctx, m.UID, time.Now()); err != nil {
		return March{}, err
	}
	return s.place(ctx, m, arrive, take)
}

// Recall implements Store.
func (s *MySQLStore) Recall(ctx context.Context, m March, arrive timer.Job) (March, error) {
	return s.place(ctx, m, arrive, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx,
			`UPDATE slg_tile SET troops = troops - ?
			 WHERE kingdom = ? AND x = ? AND y = ? AND uid = ? AND troops >= ?`,
			m.Troops, m.Kingdom, m.FromX, m.FromY, m.UID, m.Troops)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if n == 0 {
			return city.ErrNoTroops
		}
		_, err = tx.ExecContext(ctx, `DELETE FROM slg_tile WHERE kingdom = ? AND x = ? AND y = ? AND troops = 0`,
			m.Kingdom, m.FromX, m.FromY)
		return err
	})
}

// place commits a march, its timer and spend. A repeated request hits the
// unique key before spend runs, so it spends nothing.
func (s *MySQLStore) place(ctx context.Context, m March, arrive timer.Job, spend func(*sql.Tx) error) (March, error) {
	err := s.insert(ctx, m, arrive, spend)
	var dup *mysql.MySQLError
	if !errors.As(err, &dup) || dup.Number != 1062 {
		return m, err
	}
	first := March{UID: m.UID, RequestID: m.RequestID}
	rerr := s.DB.QueryRowContext(ctx,
		`SELECT id, kingdom, from_x, from_y, to_x, to_y, troops, arrive_at, home FROM slg_march
		 WHERE uid = ? AND request_id = ?`, m.UID, m.RequestID).
		Scan(&first.ID, &first.Kingdom, &first.FromX, &first.FromY, &first.ToX, &first.ToY, &first.Troops, &first.ArriveAt, &first.Home)
	if errors.Is(rerr, sql.ErrNoRows) {
		return March{}, err // The duplicate was something else, such as the timer.
	}
	if rerr != nil {
		return March{}, rerr
	}
	return first, nil
}

func (s *MySQLStore) insert(ctx context.Context, m March, arrive timer.Job, spend func(*sql.Tx) error) error {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck // No-op after Commit.
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO slg_march (id, kingdom, uid, request_id, from_x, from_y, to_x, to_y, troops, arrive_at, home)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		m.ID, m.Kingdom, m.UID, m.RequestID, m.FromX, m.FromY, m.ToX, m.ToY, m.Troops, m.ArriveAt.UTC(), m.Home); err != nil {
		return err
	}
	if err := spend(tx); err != nil {
		return err
	}
	if err := timer.AddTx(ctx, tx, arrive); err != nil {
		return err
	}
	return tx.Commit()
}

func scanTiles(rows *sql.Rows) ([]Tile, error) {
	defer rows.Close()
	var out []Tile
	for rows.Next() {
		var t Tile
		if err := rows.Scan(&t.X, &t.Y, &t.Owner, &t.Troops); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// Owned implements Store.
func (s *MySQLStore) Owned(ctx context.Context, kingdom string, uid int64) ([]Tile, error) {
	rows, err := s.DB.QueryContext(ctx,
		`SELECT x, y, uid, troops FROM slg_tile WHERE uid = ? AND kingdom = ? ORDER BY x, y LIMIT ?`,
		uid, kingdom, maxOwned)
	if err != nil {
		return nil, err
	}
	return scanTiles(rows)
}

// View implements Store.
func (s *MySQLStore) View(ctx context.Context, kingdom string, x, y, radius int32) ([]Tile, error) {
	rows, err := s.DB.QueryContext(ctx,
		`SELECT x, y, uid, troops FROM slg_tile
		 WHERE kingdom = ? AND x BETWEEN ? AND ? AND y BETWEEN ? AND ? ORDER BY x, y`,
		kingdom, x-radius, x+radius, y-radius, y+radius)
	if err != nil {
		return nil, err
	}
	return scanTiles(rows)
}

// Active implements Store.
func (s *MySQLStore) Active(ctx context.Context, kingdom string, uid int64) ([]March, error) {
	rows, err := s.DB.QueryContext(ctx,
		`SELECT id, from_x, from_y, to_x, to_y, troops, arrive_at, home FROM slg_march
		 WHERE uid = ? AND state = 0 AND kingdom = ? ORDER BY id`, uid, kingdom)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []March
	for rows.Next() {
		m := March{UID: uid, Kingdom: kingdom}
		if err := rows.Scan(&m.ID, &m.FromX, &m.FromY, &m.ToX, &m.ToY, &m.Troops, &m.ArriveAt, &m.Home); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
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
		`SELECT kingdom, uid, from_x, from_y, to_x, to_y, troops, arrive_at, home FROM slg_march WHERE id = ?`, id).
		Scan(&m.Kingdom, &m.UID, &m.FromX, &m.FromY, &m.ToX, &m.ToY, &m.Troops, &m.ArriveAt, &m.Home)
	if errors.Is(err, sql.ErrNoRows) {
		return March{}, false, fmt.Errorf("slg: unknown march %d", id)
	}
	if err != nil {
		return March{}, false, err
	}
	if n == 0 {
		return m, false, nil
	}
	if m.Home {
		if err := city.AddTroopsTx(ctx, tx, m.UID, m.Troops); err != nil {
			return March{}, false, err
		}
		return m, true, tx.Commit()
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
