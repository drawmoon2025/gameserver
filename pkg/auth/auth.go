// Package auth turns any credential into one internal token. Providers verify
// the outside world; everything behind them only ever sees our JWT.
package auth

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"golang.org/x/crypto/bcrypt"
)

var (
	ErrBadCredential = errors.New("auth: bad credential")
	ErrUnknownProvider = errors.New("auth: unknown provider")
	ErrUsernameTaken = errors.New("auth: username taken")
)

// Identity is who a provider vouches for.
type Identity struct {
	Provider string
	Subject  string
}

// Credential is what a client presents.
type Credential struct {
	Username string
	Secret   string
}

// Provider verifies a credential. Implement this to add a third party.
type Provider interface {
	Name() string
	Verify(ctx context.Context, c Credential) (Identity, error)
}

// Accounts maps identities to UIDs and stores local passwords.
type Accounts interface {
	// Resolve returns the identity's uid, creating it with newUID if unseen.
	Resolve(ctx context.Context, id Identity, newUID int64) (int64, error)
	CreatePassword(ctx context.Context, username string, hash []byte) error
	PasswordHash(ctx context.Context, username string) ([]byte, error)
}

// Local is the built-in username and password provider.
type Local struct {
	Accounts Accounts
}

// Name implements Provider.
func (*Local) Name() string { return "local" }

// dummyHash lets a miss cost as much as a hit, hiding which usernames exist.
var dummyHash, _ = bcrypt.GenerateFromPassword([]byte("dummy-password"), bcrypt.DefaultCost)

// Verify implements Provider.
func (l *Local) Verify(ctx context.Context, c Credential) (Identity, error) {
	hash, err := l.Accounts.PasswordHash(ctx, c.Username)
	if err != nil {
		_ = bcrypt.CompareHashAndPassword(dummyHash, []byte(c.Secret))
		return Identity{}, ErrBadCredential
	}
	if bcrypt.CompareHashAndPassword(hash, []byte(c.Secret)) != nil {
		return Identity{}, ErrBadCredential
	}
	return Identity{Provider: "local", Subject: c.Username}, nil
}

// Register creates a local account.
func (l *Local) Register(ctx context.Context, username, password string) error {
	if username == "" || len(password) < 8 || len(password) > 72 {
		return ErrBadCredential
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return err
	}
	return l.Accounts.CreatePassword(ctx, username, hash)
}

// HTTPProvider verifies a token against a third party's HTTP endpoint. It
// POSTs {"token": ...} and expects 200 with {"subject": ...}.
type HTTPProvider struct {
	ProviderName string
	URL          string
	Client       *http.Client
}

// Name implements Provider.
func (h *HTTPProvider) Name() string { return h.ProviderName }

// Verify implements Provider.
func (h *HTTPProvider) Verify(ctx context.Context, c Credential) (Identity, error) {
	body, err := json.Marshal(map[string]string{"token": c.Secret})
	if err != nil {
		return Identity{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.URL, bytes.NewReader(body))
	if err != nil {
		return Identity{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	client := h.Client
	if client == nil {
		client = &http.Client{Timeout: 3 * time.Second}
	}
	resp, err := client.Do(req)
	if err != nil {
		return Identity{}, fmt.Errorf("auth: %s: %w", h.ProviderName, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return Identity{}, ErrBadCredential
	}
	var out struct {
		Subject string `json:"subject"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<16)).Decode(&out); err != nil || out.Subject == "" {
		return Identity{}, ErrBadCredential
	}
	return Identity{Provider: h.ProviderName, Subject: out.Subject}, nil
}

// Claims is our token's content.
type Claims struct {
	UID     int64  `json:"uid"`
	Session string `json:"sid"`
	Epoch   uint64 `json:"epoch"`
	jwt.RegisteredClaims
}

// Signer issues and verifies tokens.
type Signer struct {
	Key []byte
	TTL time.Duration
	Now func() time.Time
}

func (s *Signer) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

// Issue signs a token.
func (s *Signer) Issue(uid int64, session string, epoch uint64) (string, time.Time, error) {
	exp := s.now().Add(s.TTL)
	c := Claims{UID: uid, Session: session, Epoch: epoch,
		RegisteredClaims: jwt.RegisteredClaims{ExpiresAt: jwt.NewNumericDate(exp)}}
	tok, err := jwt.NewWithClaims(jwt.SigningMethodHS256, c).SignedString(s.Key)
	return tok, exp, err
}

// Verify checks signature and expiry.
func (s *Signer) Verify(token string) (*Claims, error) {
	c := &Claims{}
	_, err := jwt.ParseWithClaims(token, c, func(*jwt.Token) (any, error) { return s.Key, nil },
		jwt.WithValidMethods([]string{"HS256"}), jwt.WithTimeFunc(s.now), jwt.WithExpirationRequired())
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrBadCredential, err)
	}
	return c, nil
}

// MemoryAccounts is an in-process Accounts.
type MemoryAccounts struct {
	mu        sync.Mutex
	binds     map[Identity]int64
	passwords map[string][]byte
}

// NewMemoryAccounts creates an empty store.
func NewMemoryAccounts() *MemoryAccounts {
	return &MemoryAccounts{binds: map[Identity]int64{}, passwords: map[string][]byte{}}
}

// Resolve implements Accounts.
func (m *MemoryAccounts) Resolve(_ context.Context, id Identity, newUID int64) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if uid, ok := m.binds[id]; ok {
		return uid, nil
	}
	m.binds[id] = newUID
	return newUID, nil
}

// CreatePassword implements Accounts.
func (m *MemoryAccounts) CreatePassword(_ context.Context, username string, hash []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.passwords[username]; ok {
		return ErrUsernameTaken
	}
	m.passwords[username] = hash
	return nil
}

// PasswordHash implements Accounts.
func (m *MemoryAccounts) PasswordHash(_ context.Context, username string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	h, ok := m.passwords[username]
	if !ok {
		return nil, ErrBadCredential
	}
	return h, nil
}
