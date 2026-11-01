package slg

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// MySQLStore persists marches. The kingdom's database holds them all.
type MySQLStore struct {
	DB *sql.DB
}

// Create implements Store.
func (s *MySQLStore) Create(ctx context.Context, m March) error {
	_, err := s.DB.ExecContext(ctx,
		`INSERT INTO slg_march (id, kingdom, uid, from_x, from_y, to_x, to_y, troops, arrive_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		m.ID, m.Kingdom, m.UID, m.FromX, m.FromY, m.ToX, m.ToY, m.Troops, m.ArriveAt.UTC())
	return err
}

// Arrive implements Store. The state guard in the UPDATE is the exactly-once
// point: of any number of re-fires, one sees a changed row.
func (s *MySQLStore) Arrive(ctx context.Context, id uint64) (March, bool, error) {
	res, err := s.DB.ExecContext(ctx, `UPDATE slg_march SET state = 1 WHERE id = ? AND state = 0`, id)
	if err != nil {
		return March{}, false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return March{}, false, err
	}
	m := March{ID: id}
	err = s.DB.QueryRowContext(ctx,
		`SELECT kingdom, uid, from_x, from_y, to_x, to_y, troops, arrive_at FROM slg_march WHERE id = ?`, id).
		Scan(&m.Kingdom, &m.UID, &m.FromX, &m.FromY, &m.ToX, &m.ToY, &m.Troops, &m.ArriveAt)
	if errors.Is(err, sql.ErrNoRows) {
		return March{}, false, fmt.Errorf("slg: unknown march %d", id)
	}
	if err != nil {
		return March{}, false, err
	}
	return m, n == 1, nil
}
