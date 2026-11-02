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
	m := March{ID: id, Kingdom: fmt.Sprint("k", id%1_000_000), UID: itest.ID(), RequestID: fmt.Sprint("r", id), ToX: 1, ToY: 2, Troops: 7, ArriveAt: at}
	return m, timer.Job{ID: id, Shard: int(id % 1_000_000), Kind: KindMarchArrive, Payload: []byte("p"), FireAt: at}
}

func TestMySQLMarchAndTimerCommitTogether(t *testing.T) {
	ctx := context.Background()
	db := itest.MySQL(t)
	s, timers := &MySQLStore{DB: db}, &timer.MySQLStore{DB: db}
	m, job := marchFixture()
	if _, err := s.Create(ctx, m, job); err != nil {
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

func TestMySQLRetriedRequestReturnsFirstMarch(t *testing.T) {
	ctx := context.Background()
	db := itest.MySQL(t)
	s, timers := &MySQLStore{DB: db}, &timer.MySQLStore{DB: db}
	first, job := marchFixture()
	if _, err := s.Create(ctx, first, job); err != nil {
		t.Fatal(err)
	}
	retry, retryJob := marchFixture()
	retry.UID, retry.RequestID, retry.Kingdom = first.UID, first.RequestID, first.Kingdom
	retryJob.Shard = job.Shard
	got, err := s.Create(ctx, retry, retryJob)
	if err != nil || got.ID != first.ID || got.Troops != first.Troops {
		t.Fatalf("retry returned %+v: %v", got, err)
	}
	due, err := timers.Due(ctx, job.Shard, first.ArriveAt.Add(time.Second), 10)
	if err != nil || len(due) != 1 {
		t.Fatalf("a retry must add no timer, due %v: %v", due, err)
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
	if _, err := s.Create(ctx, m, job); err == nil {
		t.Fatal("expected the timer insert to fail")
	}
	if _, _, err := s.Arrive(ctx, m.ID); err == nil {
		t.Fatal("the march survived its timer's failure")
	}
}
