//go:build integration

package city

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/drawmoon2025/gameserver/internal/itest"
	"github.com/drawmoon2025/gameserver/pkg/timer"
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

func TestMySQLTrainingLifecycle(t *testing.T) {
	ctx := context.Background()
	db := itest.MySQL(t)
	m := &MySQL{DB: db}
	uid := itest.ID()
	now := time.Now().Truncate(time.Second)

	b, gold, err := m.Train(ctx, uid, "r", 100, uint64(itest.ID()), now, 1)
	if err != nil || gold != startGold-200 || !b.FinishAt.Equal(now.Add(100*time.Second)) {
		t.Fatalf("train %+v, gold %d: %v", b, gold, err)
	}
	if again, gold, err := m.Train(ctx, uid, "r", 100, uint64(itest.ID()), now, 1); err != nil || again.ID != b.ID || gold != startGold-200 {
		t.Fatalf("retry %+v, gold %d: %v", again, gold, err)
	}
	if _, _, err := m.Train(ctx, uid, "other", 1, uint64(itest.ID()), now, 1); !errors.Is(err, ErrBusy) {
		t.Fatalf("second batch: %v", err)
	}
	if _, applied, err := m.FinishTraining(ctx, b.ID); err != nil || !applied {
		t.Fatalf("finish %v: %v", applied, err)
	}
	if _, applied, err := m.FinishTraining(ctx, b.ID); err != nil || applied {
		t.Fatalf("a re-fire applied again: %v", err)
	}
	s, err := m.Load(ctx, uid, now)
	if err != nil || s.Troops != startTroops+100 || len(s.Training) != 0 {
		t.Fatalf("state %+v: %v", s, err)
	}
}

func TestMySQLTroopsCannotGoNegativeUnderConcurrency(t *testing.T) {
	ctx := context.Background()
	db := itest.MySQL(t)
	m := &MySQL{DB: db}
	uid := itest.ID()
	if err := m.Ensure(ctx, uid, time.Now()); err != nil {
		t.Fatal(err)
	}
	take := func() error {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback() //nolint:errcheck // No-op after Commit.
		if err := TakeTroopsTx(ctx, tx, uid, 300); err != nil {
			return err
		}
		return tx.Commit()
	}
	var (
		wg     sync.WaitGroup
		mu     sync.Mutex
		passed int
	)
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			switch err := take(); {
			case err == nil:
				mu.Lock()
				passed++
				mu.Unlock()
			case !errors.Is(err, ErrNoTroops):
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if passed != startTroops/300 {
		t.Fatalf("%d takes of 300 from %d", passed, startTroops)
	}
}

func TestTakeTroopsNamesAMissingCity(t *testing.T) {
	ctx := context.Background()
	db := itest.MySQL(t)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback() //nolint:errcheck // Read only.
	if err := TakeTroopsTx(ctx, tx, itest.ID(), 1); !errors.Is(err, ErrNoCity) {
		t.Fatalf("got %v", err)
	}
}