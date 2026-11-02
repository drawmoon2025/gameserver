//go:build integration

package alliance

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/drawmoon2025/gameserver/internal/itest"
)

func name(prefix string) string { return fmt.Sprint(prefix, itest.ID()) }

func TestMySQLLifecycleAndLeaderHandover(t *testing.T) {
	ctx := context.Background()
	m := &MySQL{DB: itest.MySQL(t)}
	n := name("a")
	lead, second, third := itest.ID(), itest.ID()+1, itest.ID()+2
	if err := m.Create(ctx, n, lead); err != nil {
		t.Fatal(err)
	}
	if err := m.Create(ctx, n, lead); err != nil {
		t.Fatalf("repeat create: %v", err)
	}
	if err := m.Create(ctx, n, second); !errors.Is(err, ErrTaken) {
		t.Fatalf("taken name: %v", err)
	}
	for _, uid := range []int64{second, third, second} {
		if err := m.Join(ctx, n, uid); err != nil {
			t.Fatal(err)
		}
	}
	if err := m.Kick(ctx, n, second, third); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("kick by a member: %v", err)
	}
	if err := m.Kick(ctx, n, lead, third); err != nil {
		t.Fatal(err)
	}
	if mine, err := m.AllianceOf(ctx, third); err != nil || mine != "" {
		t.Fatalf("kicked player is in %q: %v", mine, err)
	}
	if err := m.Leave(ctx, n, lead); err != nil {
		t.Fatal(err)
	}
	i, err := m.Info(ctx, n)
	if err != nil || i.Leader != second || len(i.Members) != 1 {
		t.Fatalf("after handover %+v: %v", i, err)
	}
	if err := m.Leave(ctx, n, second); err != nil {
		t.Fatal(err)
	}
	if i, err := m.Info(ctx, n); err != nil || i.Leader != 0 {
		t.Fatalf("an empty alliance remains: %+v, %v", i, err)
	}
}

func TestMySQLSizeLimitHoldsUnderConcurrency(t *testing.T) {
	ctx := context.Background()
	m := &MySQL{DB: itest.MySQL(t)}
	n := name("b")
	base := itest.ID()
	if err := m.Create(ctx, n, base); err != nil {
		t.Fatal(err)
	}
	var (
		wg     sync.WaitGroup
		joined atomic.Int64
	)
	for i := range MaxMembers + 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			switch err := m.Join(ctx, n, base+int64(i)+1); {
			case err == nil:
				joined.Add(1)
			case !errors.Is(err, ErrFull):
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if joined.Load() != MaxMembers-1 {
		t.Fatalf("%d joined beside the leader", joined.Load())
	}
}

func TestMySQLPlayerIsInOneAllianceUnderConcurrency(t *testing.T) {
	ctx := context.Background()
	m := &MySQL{DB: itest.MySQL(t)}
	uid := itest.ID()
	var names []string
	for i := range 8 {
		n := name(fmt.Sprint("c", i))
		if err := m.Create(ctx, n, uid+int64(i)+1); err != nil {
			t.Fatal(err)
		}
		names = append(names, n)
	}
	var (
		wg     sync.WaitGroup
		joined atomic.Int64
	)
	for _, n := range names {
		wg.Add(1)
		go func() {
			defer wg.Done()
			switch err := m.Join(ctx, n, uid); {
			case err == nil:
				joined.Add(1)
			case !errors.Is(err, ErrAlreadyIn):
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if joined.Load() != 1 {
		t.Fatalf("joined %d alliances", joined.Load())
	}
}
