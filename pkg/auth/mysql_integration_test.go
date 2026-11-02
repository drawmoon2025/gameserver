//go:build integration

package auth

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/drawmoon2025/gameserver/internal/itest"
)

func TestMySQLConcurrentFirstLoginsAgree(t *testing.T) {
	m := &MySQLAccounts{DB: itest.MySQL(t)}
	id := Identity{Provider: "itest", Subject: fmt.Sprint(itest.ID())}
	base := itest.ID()
	got := make([]int64, 16)
	var wg sync.WaitGroup
	for i := range got {
		wg.Add(1)
		go func() {
			defer wg.Done()
			uid, err := m.Resolve(context.Background(), id, base+int64(i))
			if err != nil {
				t.Error(err)
			}
			got[i] = uid
		}()
	}
	wg.Wait()
	for _, uid := range got {
		if uid != got[0] || uid == 0 {
			t.Fatalf("uids %v", got)
		}
	}
}

func TestMySQLPasswords(t *testing.T) {
	ctx := context.Background()
	m := &MySQLAccounts{DB: itest.MySQL(t)}
	name := fmt.Sprint("u", itest.ID())
	if err := m.CreatePassword(ctx, name, []byte("hash")); err != nil {
		t.Fatal(err)
	}
	if err := m.CreatePassword(ctx, name, []byte("other")); !errors.Is(err, ErrUsernameTaken) {
		t.Fatalf("duplicate: %v", err)
	}
	if h, err := m.PasswordHash(ctx, name); err != nil || string(h) != "hash" {
		t.Fatalf("hash %q: %v", h, err)
	}
	if _, err := m.PasswordHash(ctx, name+"x"); !errors.Is(err, ErrBadCredential) {
		t.Fatalf("missing user: %v", err)
	}
}
