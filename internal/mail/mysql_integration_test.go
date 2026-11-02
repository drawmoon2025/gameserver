//go:build integration

package mail

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/yourname/gameserver/internal/itest"
	"github.com/yourname/gameserver/pkg/timer"
)

func TestMySQLDeliverListClaim(t *testing.T) {
	ctx := context.Background()
	db := itest.MySQL(t)
	s, timers := &MySQL{DB: db}, &timer.MySQLStore{DB: db}
	uid := itest.ID()
	m := Mail{Letter: Letter{Key: "k", UID: uid, Kind: "battle", Title: "t", Body: "b", RewardGold: 9},
		ID: uint64(itest.ID()), SentAt: time.Now()}
	if ok, err := s.Deliver(ctx, m); err != nil || !ok {
		t.Fatalf("deliver %v: %v", ok, err)
	}
	dup := m
	dup.ID = uint64(itest.ID())
	if ok, err := s.Deliver(ctx, dup); err != nil || ok {
		t.Fatalf("a re-sent letter was stored: %v", err)
	}
	list, err := s.List(ctx, uid, 10)
	if err != nil || len(list) != 1 || list[0].Title != "t" || list[0].Claimed {
		t.Fatalf("list %+v: %v", list, err)
	}
	if _, err := s.Claim(ctx, uid+1, m.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("claim of another's mail: %v", err)
	}

	var wg sync.WaitGroup
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if g, err := s.Claim(ctx, uid, m.ID); err != nil || g != 9 {
				t.Errorf("claim %d: %v", g, err)
			}
		}()
	}
	wg.Wait()
	j, err := grantFor(m)
	if err != nil {
		t.Fatal(err)
	}
	due, err := timers.Due(ctx, j.Shard, time.Now().Add(time.Hour), 1000)
	if err != nil {
		t.Fatal(err)
	}
	found := 0
	for _, d := range due {
		if d.ID == j.ID {
			found++
		}
	}
	if found != 1 {
		t.Fatalf("%d grants for one claim", found)
	}
}
