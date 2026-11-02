package alliance

import (
	"context"
	"database/sql"
	"errors"

	"github.com/go-sql-driver/mysql"

	"github.com/drawmoon2025/gameserver/pkg/timer"
)

const mysqlDuplicateEntry = 1062

// MySQL persists alliances. timer_job must be in the same database. The primary
// key of alliance_member is what keeps a player in one alliance; the row lock on
// alliance is what makes the size limit and leader changes safe.
type MySQL struct {
	DB *sql.DB
}

func isDuplicate(err error) bool {
	var me *mysql.MySQLError
	return errors.As(err, &me) && me.Number == mysqlDuplicateEntry
}

func (m *MySQL) tx(ctx context.Context, fn func(*sql.Tx) error) error {
	tx, err := m.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck // No-op after Commit.
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

func allianceOf(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, uid int64) (string, error) {
	var name string
	err := q.QueryRowContext(ctx, `SELECT alliance FROM alliance_member WHERE uid = ?`, uid).Scan(&name)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return name, err
}

// Create implements Store.
func (m *MySQL) Create(ctx context.Context, name string, uid int64) error {
	return m.tx(ctx, func(tx *sql.Tx) error {
		mine, err := allianceOf(ctx, tx, uid)
		if err != nil {
			return err
		}
		if mine == name {
			return nil
		}
		if mine != "" {
			return ErrAlreadyIn
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO alliance (name, leader) VALUES (?, ?)`, name, uid); err != nil {
			if isDuplicate(err) {
				return ErrTaken
			}
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO alliance_member (uid, alliance) VALUES (?, ?)`, uid, name); err != nil {
			if isDuplicate(err) {
				return ErrAlreadyIn
			}
			return err
		}
		return nil
	})
}

func lockLeader(ctx context.Context, tx *sql.Tx, name string) (int64, error) {
	var leader int64
	err := tx.QueryRowContext(ctx, `SELECT leader FROM alliance WHERE name = ? FOR UPDATE`, name).Scan(&leader)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, ErrNotFound
	}
	return leader, err
}

// Join implements Store.
func (m *MySQL) Join(ctx context.Context, name string, uid int64) error {
	return m.tx(ctx, func(tx *sql.Tx) error {
		if _, err := lockLeader(ctx, tx, name); err != nil {
			return err
		}
		mine, err := allianceOf(ctx, tx, uid)
		if err != nil {
			return err
		}
		if mine == name {
			return nil
		}
		if mine != "" {
			return ErrAlreadyIn
		}
		var n int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM alliance_member WHERE alliance = ?`, name).Scan(&n); err != nil {
			return err
		}
		if n >= MaxMembers {
			return ErrFull
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO alliance_member (uid, alliance) VALUES (?, ?)`, uid, name); err != nil {
			if isDuplicate(err) {
				return ErrAlreadyIn // A join to another alliance won the race.
			}
			return err
		}
		return nil
	})
}

// remove runs with the alliance row locked.
func remove(ctx context.Context, tx *sql.Tx, name string, leader, uid int64) error {
	res, err := tx.ExecContext(ctx, `DELETE FROM alliance_member WHERE uid = ? AND alliance = ?`, uid, name)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil || n == 0 {
		return err
	}
	j, err := revokeJob(name, uid)
	if err != nil {
		return err
	}
	if err := timer.AddTx(ctx, tx, j); err != nil {
		return err
	}
	if uid != leader {
		return nil
	}
	var next int64
	err = tx.QueryRowContext(ctx, `SELECT uid FROM alliance_member WHERE alliance = ? ORDER BY seq LIMIT 1`, name).Scan(&next)
	if errors.Is(err, sql.ErrNoRows) {
		_, err = tx.ExecContext(ctx, `DELETE FROM alliance WHERE name = ?`, name)
		return err
	}
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `UPDATE alliance SET leader = ? WHERE name = ?`, next, name)
	return err
}

// Leave implements Store.
func (m *MySQL) Leave(ctx context.Context, name string, uid int64) error {
	return m.tx(ctx, func(tx *sql.Tx) error {
		leader, err := lockLeader(ctx, tx, name)
		if errors.Is(err, ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		return remove(ctx, tx, name, leader, uid)
	})
}

// Kick implements Store.
func (m *MySQL) Kick(ctx context.Context, name string, leader, target int64) error {
	return m.tx(ctx, func(tx *sql.Tx) error {
		cur, err := lockLeader(ctx, tx, name)
		if err != nil {
			return err
		}
		if cur != leader || target == leader {
			return ErrNotLeader
		}
		return remove(ctx, tx, name, cur, target)
	})
}

// Info implements Store.
func (m *MySQL) Info(ctx context.Context, name string) (Info, error) {
	i := Info{Name: name}
	err := m.DB.QueryRowContext(ctx, `SELECT leader FROM alliance WHERE name = ?`, name).Scan(&i.Leader)
	if errors.Is(err, sql.ErrNoRows) {
		return i, nil
	}
	if err != nil {
		return i, err
	}
	rows, err := m.DB.QueryContext(ctx, `SELECT uid FROM alliance_member WHERE alliance = ? ORDER BY seq`, name)
	if err != nil {
		return i, err
	}
	defer rows.Close()
	for rows.Next() {
		var uid int64
		if err := rows.Scan(&uid); err != nil {
			return i, err
		}
		i.Members = append(i.Members, uid)
	}
	return i, rows.Err()
}

// AllianceOf implements Store.
func (m *MySQL) AllianceOf(ctx context.Context, uid int64) (string, error) {
	return allianceOf(ctx, m.DB, uid)
}
