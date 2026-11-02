package rank

import (
	"context"
	"sync"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func stores(t *testing.T) map[string]Store {
	mr := miniredis.RunT(t)
	return map[string]Store{
		"memory": NewMemory(),
		"redis":  &Redis{Client: redis.NewClient(&redis.Options{Addr: mr.Addr()})},
	}
}

func TestScoresApplyOncePerKey(t *testing.T) {
	ctx := context.Background()
	for name, s := range stores(t) {
		t.Run(name, func(t *testing.T) {
			var wg sync.WaitGroup
			for range 20 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					if err := s.Add(ctx, Score{Board: "b", UID: 1, Delta: 5, Key: "k"}); err != nil {
						t.Error(err)
					}
				}()
			}
			wg.Wait()
			if e, err := s.Position(ctx, "b", 1); err != nil || e.Score != 5 || e.Rank != 1 {
				t.Fatalf("entry %+v: %v", e, err)
			}
		})
	}
}

func TestDistinctKeysAccumulateAndMissingPlayerIsUnranked(t *testing.T) {
	ctx := context.Background()
	for name, s := range stores(t) {
		t.Run(name, func(t *testing.T) {
			adds := []Score{{"b", 1, 10, "a"}, {"b", 2, 30, "b"}, {"b", 3, 20, "c"}, {"b", 1, 5, "d"}}
			for _, a := range adds {
				if err := s.Add(ctx, a); err != nil {
					t.Fatal(err)
				}
			}
			top, err := s.Top(ctx, "b", 2)
			if err != nil || len(top) != 2 || top[0].UID != 2 || top[1].UID != 3 || top[1].Rank != 2 {
				t.Fatalf("top %+v: %v", top, err)
			}
			if e, err := s.Position(ctx, "b", 1); err != nil || e.Score != 15 || e.Rank != 3 {
				t.Fatalf("entry %+v: %v", e, err)
			}
			if e, err := s.Position(ctx, "b", 99); err != nil || e.Rank != 0 {
				t.Fatalf("absent player %+v: %v", e, err)
			}
		})
	}
}
