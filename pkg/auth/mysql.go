package auth

import (
	"context"
	"database/sql"
	"errors"

	"github.com/go-sql-driver/mysql"
)

// MySQLAccounts stores identities. These tables are looked up by provider and
// subject, not uid, so they live in one directory database, not the uid shards.
type MySQLAccounts struct {
	DB *sql.DB
}

// Resolve implements Accounts. INSERT IGNORE then SELECT means two concurrent
// first logins agree on whichever uid won.
func (m *MySQLAccounts) Resolve(ctx context.Context, id Identity, newUID int64) (int64, error) {
	if _, err := m.DB.ExecContext(ctx,
		`INSERT IGNORE INTO account_bind (provider, subject, uid) VALUES (?, ?, ?)`,
		id.Provider, id.Subject, newUID); err != nil {
		return 0, err
	}
	var uid int64
	err := m.DB.QueryRowContext(ctx,
		`SELECT uid FROM account_bind WHERE provider = ? AND subject = ?`, id.Provider, id.Subject).Scan(&uid)
	return uid, err
}

// CreatePassword implements Accounts.
func (m *MySQLAccounts) CreatePassword(ctx context.Context, username string, hash []byte) error {
	_, err := m.DB.ExecContext(ctx,
		`INSERT INTO account_password (username, hash) VALUES (?, ?)`, username, hash)
	var dup *mysql.MySQLError
	if errors.As(err, &dup) && dup.Number == 1062 {
		return ErrUsernameTaken
	}
	return err
}

// PasswordHash implements Accounts.
func (m *MySQLAccounts) PasswordHash(ctx context.Context, username string) ([]byte, error) {
	var h []byte
	err := m.DB.QueryRowContext(ctx,
		`SELECT hash FROM account_password WHERE username = ?`, username).Scan(&h)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrBadCredential
	}
	return h, err
}
