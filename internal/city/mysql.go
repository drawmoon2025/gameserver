package city

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/yourname/gameserver/pkg/timer"
)

// MySQL persists cities. timer_job must be in the same database, so that an
// upgrade and its timer commit together.
type MySQL struct {
	DB *sql.DB
}

// withState runs fn in a transaction holding the city's row, which serializes
// everything that changes one city. A new city's rows are created in their own
// transaction first: an insert here would hold a shared lock that concurrent
// callers then deadlock upgrading.
func (m *MySQL) withState(ctx context.Context, uid int64, now time.Time, fn func(*sql.Tx, *State) error) error {
	for attempt := 0; ; attempt++ {
		err := m.tryState(ctx, uid, fn)
		if !errors.Is(err, sql.ErrNoRows) || attempt > 0 {
			return err
		}
		if err := m.ensure(ctx, uid, now); err != nil {
			return fmt.Errorf("city: create: %w", err)
		}
	}
}

func (m *MySQL) tryState(ctx context.Context, uid int64, fn func(*sql.Tx, *State) error) error {
	tx, err := m.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck // No-op after Commit.
	s, err := lock(ctx, tx, uid)
	if err != nil {
		return err
	}
	if err := fn(tx, s); err != nil {
		return err
	}
	return tx.Commit()
}

func (m *MySQL) ensure(ctx context.Context, uid int64, now time.Time) error {
	s := newState(uid, now)
	tx, err := m.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck // No-op after Commit.
	if _, err := tx.ExecContext(ctx, `INSERT IGNORE INTO city_state (uid, gold, gold_at) VALUES (?, ?, ?)`,
		uid, s.Gold, s.GoldAt.UTC()); err != nil {
		return err
	}
	for kind, level := range s.Buildings {
		if _, err := tx.ExecContext(ctx, `INSERT IGNORE INTO city_building (uid, kind, level) VALUES (?, ?, ?)`,
			uid, kind, level); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func lock(ctx context.Context, tx *sql.Tx, uid int64) (*State, error) {
	s := &State{UID: uid, Buildings: map[string]int32{}}
	err := tx.QueryRowContext(ctx, `SELECT gold, gold_at FROM city_state WHERE uid = ? FOR UPDATE`, uid).
		Scan(&s.Gold, &s.GoldAt)
	if err != nil {
		return nil, err
	}
	rows, err := tx.QueryContext(ctx, `SELECT kind, level FROM city_building WHERE uid = ?`, uid)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var (
			kind  string
			level int32
		)
		if err := rows.Scan(&kind, &level); err != nil {
			return nil, err
		}
		s.Buildings[kind] = level
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	ups, err := tx.QueryContext(ctx,
		`SELECT id, kind, to_level, request_id, finish_at FROM city_upgrade WHERE uid = ? AND state = 0`, uid)
	if err != nil {
		return nil, err
	}
	defer ups.Close()
	for ups.Next() {
		u := Upgrade{UID: uid}
		if err := ups.Scan(&u.ID, &u.Kind, &u.ToLevel, &u.RequestID, &u.FinishAt); err != nil {
			return nil, err
		}
		s.Upgrades = append(s.Upgrades, u)
	}
	return s, ups.Err()
}

func save(ctx context.Context, tx *sql.Tx, s *State) error {
	if _, err := tx.ExecContext(ctx, `UPDATE city_state SET gold = ?, gold_at = ? WHERE uid = ?`,
		s.Gold, s.GoldAt.UTC(), s.UID); err != nil {
		return err
	}
	for kind, level := range s.Buildings {
		if _, err := tx.ExecContext(ctx, `UPDATE city_building SET level = ? WHERE uid = ? AND kind = ?`,
			level, s.UID, kind); err != nil {
			return err
		}
	}
	return nil
}

// Load implements Store.
func (m *MySQL) Load(ctx context.Context, uid int64, now time.Time) (*State, error) {
	var out *State
	err := m.withState(ctx, uid, now, func(_ *sql.Tx, s *State) error {
		s.settle(now)
		out = s
		return nil
	})
	return out, err
}

// Begin implements Store.
func (m *MySQL) Begin(ctx context.Context, uid int64, requestID, kind string, id uint64, now time.Time, speed float64) (Upgrade, int64, error) {
	var (
		u    Upgrade
		gold int64
	)
	err := m.withState(ctx, uid, now, func(tx *sql.Tx, s *State) error {
		// Holding the city's lock, so a racing retry has finished or not begun.
		u = Upgrade{UID: uid, RequestID: requestID}
		err := tx.QueryRowContext(ctx,
			`SELECT id, kind, to_level, finish_at FROM city_upgrade WHERE uid = ? AND request_id = ?`, uid, requestID).
			Scan(&u.ID, &u.Kind, &u.ToLevel, &u.FinishAt)
		if err == nil {
			s.settle(now)
			gold = s.Gold
			return nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if u, err = s.begin(kind, requestID, id, now, speed); err != nil {
			return err
		}
		gold = s.Gold
		if err := save(ctx, tx, s); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO city_upgrade (id, uid, kind, to_level, request_id, finish_at) VALUES (?, ?, ?, ?, ?, ?)`,
			u.ID, u.UID, u.Kind, u.ToLevel, u.RequestID, u.FinishAt.UTC()); err != nil {
			return err
		}
		return timer.AddTx(ctx, tx, upgradeJob(u))
	})
	return u, gold, err
}

// Finish implements Store. The guard in the UPDATE is the exactly-once point.
func (m *MySQL) Finish(ctx context.Context, id uint64, now time.Time) (Upgrade, bool, error) {
	u := Upgrade{ID: id}
	err := m.DB.QueryRowContext(ctx, `SELECT uid, kind, to_level, request_id, finish_at FROM city_upgrade WHERE id = ?`, id).
		Scan(&u.UID, &u.Kind, &u.ToLevel, &u.RequestID, &u.FinishAt)
	if err != nil {
		return Upgrade{}, false, fmt.Errorf("city: read upgrade %d: %w", id, err)
	}
	applied := false
	err = m.withState(ctx, u.UID, now, func(tx *sql.Tx, s *State) error {
		res, err := tx.ExecContext(ctx, `UPDATE city_upgrade SET state = 1 WHERE id = ? AND state = 0`, id)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil || n == 0 {
			return err
		}
		applied = true
		s.finish(u, now)
		return save(ctx, tx, s)
	})
	return u, applied, err
}

// Grant implements Store. The unique key is the exactly-once point.
func (m *MySQL) Grant(ctx context.Context, uid int64, key string, gold int64, now time.Time) error {
	return m.withState(ctx, uid, now, func(tx *sql.Tx, s *State) error {
		res, err := tx.ExecContext(ctx, `INSERT IGNORE INTO city_grant (uid, grant_key) VALUES (?, ?)`, uid, key)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil || n == 0 {
			return err
		}
		s.settle(now)
		s.Gold += gold
		return save(ctx, tx, s)
	})
}
