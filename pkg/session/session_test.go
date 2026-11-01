package session

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestRedisEpochs(t *testing.T) {
	mr := miniredis.RunT(t)
	s := &Redis{Client: redis.NewClient(&redis.Options{Addr: mr.Addr()})}
	ctx := context.Background()
	if v, err := s.Current(ctx, 7); err != nil || v != 0 {
		t.Fatalf("v=%d err=%v", v, err)
	}
	for want := uint64(1); want <= 3; want++ {
		if v, err := s.Next(ctx, 7); err != nil || v != want {
			t.Fatalf("v=%d err=%v", v, err)
		}
	}
	if v, err := s.Current(ctx, 7); err != nil || v != 3 {
		t.Fatalf("v=%d err=%v", v, err)
	}
}
