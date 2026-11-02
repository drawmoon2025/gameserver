package chat

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestHistoryIsCappedAndOrdered(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	for name, s := range map[string]Store{
		"memory": NewMemory(),
		"redis":  &Redis{Client: redis.NewClient(&redis.Options{Addr: mr.Addr()})},
	} {
		t.Run(name, func(t *testing.T) {
			for i := range historyN + 5 {
				m := Message{ID: uint64(i), UID: 1, Text: fmt.Sprint(i), SentAt: time.Now()}
				if err := s.Append(ctx, "kingdom:k", m); err != nil {
					t.Fatal(err)
				}
			}
			log, err := s.Recent(ctx, "kingdom:k")
			if err != nil || len(log) != historyN || log[0].ID != 5 || log[historyN-1].ID != historyN+4 {
				t.Fatalf("log of %d: %v", len(log), err)
			}
		})
	}
}

func TestChannelAccess(t *testing.T) {
	s := &Service{cfg: Config{AllianceOf: func(_ context.Context, uid int64) (string, error) {
		if uid == 1 {
			return "wolves", nil
		}
		return "", nil
	}}}
	ctx := context.Background()
	for _, c := range []struct {
		uid  int64
		key  string
		want bool
	}{
		{1, "kingdom:k", true},
		{2, "kingdom:k", true},
		{1, "alliance:wolves", true},
		{2, "alliance:wolves", false},
		{1, "alliance:bears", false},
		{2, "alliance:", false}, // No alliance must not match an empty name.
		{1, "anything", false},
	} {
		if got, err := s.allowed(ctx, c.uid, c.key); err != nil || got != c.want {
			t.Errorf("uid %d in %q: %v, %v", c.uid, c.key, got, err)
		}
	}
}
