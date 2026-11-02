//go:build integration

package rank

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/redis/go-redis/v9"

	"github.com/drawmoon2025/gameserver/internal/itest"
)

func TestRealRedisAppliesScoreOnce(t *testing.T) {
	ctx := context.Background()
	c := redis.NewClient(&redis.Options{Addr: itest.RedisAddr()})
	defer c.Close()
	s := &Redis{Client: c}
	board := fmt.Sprint("itest:", itest.ID())
	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.Add(ctx, Score{Board: board, UID: 1, Delta: 5, Key: "k"}); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if err := s.Add(ctx, Score{Board: board, UID: 2, Delta: 9, Key: "other"}); err != nil {
		t.Fatal(err)
	}
	top, err := s.Top(ctx, board, 10)
	if err != nil || len(top) != 2 || top[0].UID != 2 || top[1].Score != 5 {
		t.Fatalf("top %+v: %v", top, err)
	}
	if e, err := s.Position(ctx, board, 1); err != nil || e.Rank != 2 {
		t.Fatalf("entry %+v: %v", e, err)
	}
}
