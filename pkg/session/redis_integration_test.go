//go:build integration

package session

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9"

	"github.com/yourname/gameserver/internal/itest"
)

func TestRedisEpochsOnRealServer(t *testing.T) {
	ctx := context.Background()
	c := redis.NewClient(&redis.Options{Addr: itest.RedisAddr()})
	defer c.Close()
	s := &Redis{Client: c}
	uid := itest.ID()
	if v, err := s.Current(ctx, uid); err != nil || v != 0 {
		t.Fatalf("fresh uid: %d, %v", v, err)
	}
	a, err := s.Next(ctx, uid)
	if err != nil {
		t.Fatal(err)
	}
	b, err := s.Next(ctx, uid)
	if err != nil || b <= a {
		t.Fatalf("epochs %d then %d: %v", a, b, err)
	}
	if v, err := s.Current(ctx, uid); err != nil || v != b {
		t.Fatalf("current %d: %v", v, err)
	}
}
