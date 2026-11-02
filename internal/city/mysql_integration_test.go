//go:build integration

package city

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/yourname/gameserver/internal/itest"
	"github.com/yourname/gameserver/pkg/timer"
)

func TestMySQLUpgradeLifecycle(t *testing.T) {
	ctx := context.Background()
	db := itest.MySQL(t)
	m, timers := &MySQL{DB: db}, &timer.MySQLStore{DB: db}
	uid := itest.ID()
	now := time.Now().Truncate(time.Second)

	u, gold, err := m.Begin(ctx, uid, "r", Mine, uint64(itest.ID()), now, 1)
	if err != nil || gold != 300 || u.ToLevel != 2 {
		t.Fatalf("begin %+v, gold %d: %v", u, gold, err)
	}
	due, err := timers.Due(ctx, shardOf(uid), u.FinishAt.Add(time.Second), 10)
	if err != nil || len(due) != 1 || due[0].ID != u.ID {
		t.Fatalf("timer %v: %v", due, err)
	}
	again, gold, err := m.Begin(ctx, uid, "r", Mine, uint64(itest.ID()), now, 1)
	if err != nil || again.ID != u.ID || gold != 300 {
		t.Fatalf("retry %+v, gold %d: %v", again, gold, err)
	}
	if _, _, err := m.Begin(ctx, uid, "other", Mine, uint64(itest.ID()), now, 1); !errors.Is(err, ErrBusy) {
		t.Fatalf("second upgrade of one building: %v", err)
	}

	later := now.Add(10 * time.Second)
	if _, applied, err := m.Finish(ctx, u.ID, later); err != nil || !applied {
		t.Fatalf("finish %v: %v", applied, err)
	}
	if _, applied, err := m.Finish(ctx, u.ID, later); err != nil || applied {
		t.Fatalf("a re-fire applied again: %v", err)
	}
	s, err := m.Load(ctx, uid, later)
	if err != nil || s.Buildings[Mine] != 2 || len(s.Upgrades) != 0 {
		t.Fatalf("state %+v: %v", s, err)
	}
	if s.Gold != 300+10*2 { // 10s at the level 1 rate of 2 gold per second.
		t.Fatalf("gold %d", s.Gold)
	}
}

func TestMySQLGrantsApplyOnceUnderConcurrency(t *testing.T) {
	ctx := context.Background()
	m := &MySQL{DB: itest.MySQL(t)}
	uid := itest.ID()
	now := time.Now()
	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := m.Grant(ctx, uid, "g", 7, now); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if s, err := m.Load(ctx, uid, now); err != nil || s.Gold != startGold+7 {
		t.Fatalf("state %+v: %v", s, err)
	}
}
