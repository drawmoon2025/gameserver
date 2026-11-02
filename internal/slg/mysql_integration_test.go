//go:build integration

package slg

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/yourname/gameserver/internal/itest"
	"github.com/yourname/gameserver/pkg/timer"
)

func marchFixture() (March, timer.Job) {
	id := uint64(itest.ID())
	at := time.Now().Add(time.Hour)
	m := March{ID: id, Kingdom: fmt.Sprint("k", id%1_000_000), UID: itest.ID(), ToX: 1, ToY: 2, Troops: 7, ArriveAt: at}
	return m, timer.Job{ID: id, Shard: int(id % 1_000_000), Kind: KindMarchArrive, Payload: []byte("p"), FireAt: at}
}

func TestMySQLMarchAndTimerCommitTogether(t *testing.T) {
	ctx := context.Background()
	db := itest.MySQL(t)
	s, timers := &MySQLStore{DB: db}, &timer.MySQLStore{DB: db}
	m, job := marchFixture()
	if err := s.Create(ctx, m, job); err != nil {
		t.Fatal(err)
	}
	due, err := timers.Due(ctx, job.Shard, m.ArriveAt.Add(time.Second), 10)
	if err != nil || len(due) != 1 || due[0].ID != m.ID {
		t.Fatalf("due %v: %v", due, err)
	}

	got, applied, err := s.Arrive(ctx, m.ID)
	if err != nil || !applied || got.Troops != 7 || got.UID != m.UID || !got.ArriveAt.Equal(m.ArriveAt.Truncate(time.Millisecond)) &&
		got.ArriveAt.Sub(m.ArriveAt).Abs() > time.Millisecond {
		t.Fatalf("arrive %+v, %v: %v", got, applied, err)
	}
	if _, applied, err := s.Arrive(ctx, m.ID); err != nil || applied {
		t.Fatalf("a re-fire applied again: %v", err)
	}
	if n, err := s.Troops(ctx, m.Kingdom, m.UID); err != nil || n != 7 {
		t.Fatalf("troops %d: %v", n, err)
	}
}

func TestMySQLMarchIsRolledBackIfItsTimerFails(t *testing.T) {
	ctx := context.Background()
	db := itest.MySQL(t)
	s, timers := &MySQLStore{DB: db}, &timer.MySQLStore{DB: db}
	m, job := marchFixture()
	if err := timers.Add(ctx, job); err != nil { // The march's insert will collide.
		t.Fatal(err)
	}
	if err := s.Create(ctx, m, job); err == nil {
		t.Fatal("expected the timer insert to fail")
	}
	if _, _, err := s.Arrive(ctx, m.ID); err == nil {
		t.Fatal("the march survived its timer's failure")
	}
}
