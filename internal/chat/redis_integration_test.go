//go:build integration

package chat

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/yourname/gameserver/internal/itest"
)

func TestRealRedisHistory(t *testing.T) {
	ctx := context.Background()
	c := redis.NewClient(&redis.Options{Addr: itest.RedisAddr()})
	defer c.Close()
	s := &Redis{Client: c}
	key := fmt.Sprint("kingdom:", itest.ID())
	for i := range historyN + 3 {
		if err := s.Append(ctx, key, Message{ID: uint64(i), UID: 1, Text: "x", SentAt: time.Now()}); err != nil {
			t.Fatal(err)
		}
	}
	log, err := s.Recent(ctx, key)
	if err != nil || len(log) != historyN || log[0].ID != 3 {
		t.Fatalf("log of %d: %v", len(log), err)
	}
}
