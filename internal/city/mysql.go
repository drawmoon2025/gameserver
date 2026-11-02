package city

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/drawmoon2025/gameserver/pkg/timer"
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
		if err := m.Ensure(ctx, uid, now); err != nil {
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

// Ensure creates a city if it does not exist.
func (m *MySQL) Ensure(ctx context.Context, uid int64, now time.Time) error {
	s := newState(uid, now)
	tx, err := m.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck // No-op after Commit.
	if _, err := tx.ExecContext(ctx, `INSERT IGNORE INTO city_state (uid, gold, gold_at, troops) VALUES (?, ?, ?, ?)`,
		uid, s.Gold, s.GoldAt.UTC(), s.Troops); err != nil {
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
	err := tx.QueryRowContext(ctx, `SELECT gold, gold_at, troops FROM city_state WHERE uid = ? FOR UPDATE`, uid).
		Scan(&s.Gold, &s.GoldAt, &s.Troops)
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
	if err := ups.Err(); err != nil {
		return nil, err
	}
	drills, err := tx.QueryContext(ctx,
		`SELECT id, count, request_id, finish_at FROM city_training WHERE uid = ? AND state = 0`, uid)
	if err != nil {
		return nil, err
	}
	defer drills.Close()
	for drills.Next() {
		t := Training{UID: uid}
		if err := drills.Scan(&t.ID, &t.Count, &t.RequestID, &t.FinishAt); err != nil {
			return nil, err
		}
		s.Training = append(s.Training, t)
	}
	return s, drills.Err()
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

// Train implements Store.
func (m *MySQL) Train(ctx context.Context, uid int64, requestID string, count int64, id uint64, now time.Time, speed float64) (Training, int64, error) {
	var (
		t    Training
		gold int64
	)
	err := m.withState(ctx, uid, now, func(tx *sql.Tx, s *State) error {
		t = Training{UID: uid, RequestID: requestID}
		err := tx.QueryRowContext(ctx,
			`SELECT id, count, finish_at FROM city_training WHERE uid = ? AND request_id = ?`, uid, requestID).
			Scan(&t.ID, &t.Count, &t.FinishAt)
		if err == nil {
			s.settle(now)
			gold = s.Gold
			return nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if t, err = s.train(count, requestID, id, now, speed); err != nil {
			return err
		}
		gold = s.Gold
		if err := save(ctx, tx, s); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO city_training (id, uid, count, request_id, finish_at) VALUES (?, ?, ?, ?, ?)`,
			t.ID, t.UID, t.Count, t.RequestID, t.FinishAt.UTC()); err != nil {
			return err
		}
		return timer.AddTx(ctx, tx, trainingJob(t))
	})
	return t, gold, err
}

// FinishTraining implements Store. The guard in the UPDATE is the exactly-once
// point, and the troops commit with it.
func (m *MySQL) FinishTraining(ctx context.Context, id uint64) (Training, bool, error) {
	tx, err := m.DB.BeginTx(ctx, nil)
	if err != nil {
		return Training{}, false, err
	}
	defer tx.Rollback() //nolint:errcheck // No-op after Commit.
	t := Training{ID: id}
	if err := tx.QueryRowContext(ctx, `SELECT uid, count, request_id, finish_at FROM city_training WHERE id = ?`, id).
		Scan(&t.UID, &t.Count, &t.RequestID, &t.FinishAt); err != nil {
		return Training{}, false, fmt.Errorf("city: read training %d: %w", id, err)
	}
	res, err := tx.ExecContext(ctx, `UPDATE city_training SET state = 1 WHERE id = ? AND state = 0`, id)
	if err != nil {
		return Training{}, false, err
	}
	n, err := res.RowsAffected()
	if err != nil || n == 0 {
		return t, false, err
	}
	if err := AddTroopsTx(ctx, tx, t.UID, t.Count); err != nil {
		return Training{}, false, err
	}
	return t, true, tx.Commit()
}

// TakeTroopsTx moves troops out of a city in the caller's transaction, so that
// an army and the troops it left with commit as one. It fails with ErrNoCity if
// the city does not exist yet: the caller creates it with Ensure, outside its
// transaction, and tries again.
func TakeTroopsTx(ctx context.Context, tx *sql.Tx, uid, n int64) error {
	res, err := tx.ExecContext(ctx, `UPDATE city_state SET troops = troops - ? WHERE uid = ? AND troops >= ?`, n, uid, n)
	if err != nil {
		return err
	}
	changed, err := res.RowsAffected()
	if err != nil || changed == 1 {
		return err
	}
	var one int
	err = tx.QueryRowContext(ctx, `SELECT 1 FROM city_state WHERE uid = ?`, uid).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrNoCity
	}
	if err != nil {
		return err
	}
	return ErrNoTroops
}

// AddTroopsTx returns troops to a city in the caller's transaction.
func AddTroopsTx(ctx context.Context, tx *sql.Tx, uid, n int64) error {
	res, err := tx.ExecContext(ctx, `UPDATE city_state SET troops = troops + ? WHERE uid = ?`, n, uid)
	if err != nil {
		return err
	}
	changed, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if changed != 1 {
		return ErrNoCity
	}
	return nil
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
