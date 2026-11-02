package session

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestRedisAnnouncesNewEpochs(t *testing.T) {
	mr := miniredis.RunT(t)
	s := &Redis{Client: redis.NewClient(&redis.Options{Addr: mr.Addr()})}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	got := make(chan [2]int64, 1)
	go s.Subscribe(ctx, func(uid int64, epoch uint64) { got <- [2]int64{uid, int64(epoch)} }) //nolint:errcheck // Ends with ctx.
	deadline := time.Now().Add(3 * time.Second)
	for {
		if _, err := s.Next(ctx, 9); err != nil {
			t.Fatal(err)
		}
		select {
		case m := <-got:
			if m[0] != 9 || m[1] == 0 {
				t.Fatalf("announced %v", m)
			}
			return
		case <-time.After(20 * time.Millisecond): // The subscription may not be up yet.
		}
		if time.Now().After(deadline) {
			t.Fatal("no announcement")
		}
	}
}

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
