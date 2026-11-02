//go:build integration

package slg

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/yourname/gameserver/internal/city"
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

func TestMySQLBattleCommitsTileAndMailTogether(t *testing.T) {
	ctx := context.Background()
	db := itest.MySQL(t)
	s := &MySQLStore{DB: db}
	def, _ := marchFixture()
	def.Troops = 10
	atk, _ := marchFixture()
	atk.Kingdom, atk.Troops = def.Kingdom, 1000
	for _, m := range []March{def, atk} {
		job := timer.Job{ID: m.ID, Shard: 999, Kind: KindMarchArrive, Payload: []byte("p"), FireAt: m.ArriveAt}
		if _, err := s.Create(ctx, m, job); err != nil {
			t.Fatal(err)
		}
		if _, applied, err := s.Arrive(ctx, m.ID); err != nil || !applied {
			t.Fatalf("arrive %v: %v", applied, err)
		}
	}
	if n, err := s.Troops(ctx, def.Kingdom, def.UID); err != nil || n != 0 {
		t.Fatalf("defender holds %d: %v", n, err)
	}
	if n, err := s.Troops(ctx, atk.Kingdom, atk.UID); err != nil || n <= 0 {
		t.Fatalf("attacker holds %d: %v", n, err)
	}
	var letters int
	if err := db.QueryRow(`SELECT COUNT(*) FROM timer_job WHERE kind = 'mail.deliver' AND payload LIKE ?`,
		fmt.Sprintf(`%%battle:%d:%%`, atk.ID)).Scan(&letters); err != nil {
		t.Fatal(err)
	}
	if letters != 2 {
		t.Fatalf("%d letters for one battle", letters)
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

func TestMySQLArmyLeavesAndReturnsToItsCity(t *testing.T) {
	ctx := context.Background()
	db := itest.MySQL(t)
	s, cities := &MySQLStore{DB: db}, &city.MySQL{DB: db}
	m, job := marchFixture()
	m.Troops = 900
	cityTroops := func() int64 {
		t.Helper()
		c, err := cities.Load(ctx, m.UID, time.Now())
		if err != nil {
			t.Fatal(err)
		}
		return c.Troops
	}
	if _, err := s.Create(ctx, m, job); err != nil {
		t.Fatal(err)
	}
	if got := cityTroops(); got != 100 {
		t.Fatalf("city holds %d after the army left", got)
	}
	if _, err := s.Create(ctx, m, job); err != nil {
		t.Fatal(err)
	}
	if got := cityTroops(); got != 100 {
		t.Fatalf("a retry took troops again: %d", got)
	}
	over, overJob := marchFixture()
	over.UID, over.Troops = m.UID, 101
	if _, err := s.Create(ctx, over, overJob); !errors.Is(err, city.ErrNoTroops) {
		t.Fatalf("too many troops: %v", err)
	}
	if active, err := s.Active(ctx, m.Kingdom, m.UID); err != nil || len(active) != 1 {
		t.Fatalf("active %+v: %v", active, err)
	}
	if _, applied, err := s.Arrive(ctx, m.ID); err != nil || !applied {
		t.Fatalf("arrive %v: %v", applied, err)
	}

	back := March{ID: uint64(itest.ID()), Kingdom: m.Kingdom, UID: m.UID, RequestID: fmt.Sprint("b", m.ID),
		FromX: m.ToX, FromY: m.ToY, Troops: 900, ArriveAt: time.Now(), Home: true}
	backJob := timer.Job{ID: back.ID, Shard: job.Shard, Kind: KindMarchArrive, Payload: []byte("p"), FireAt: back.ArriveAt}
	tooMany := back
	tooMany.Troops, tooMany.RequestID = 901, "x"+back.RequestID
	if _, err := s.Recall(ctx, tooMany, backJob); !errors.Is(err, city.ErrNoTroops) {
		t.Fatalf("recall of too many: %v", err)
	}
	if _, err := s.Recall(ctx, back, backJob); err != nil {
		t.Fatal(err)
	}
	if tiles, err := s.Owned(ctx, m.Kingdom, m.UID); err != nil || len(tiles) != 0 {
		t.Fatalf("emptied tile remains: %+v, %v", tiles, err)
	}
	for range 2 {
		if _, _, err := s.Arrive(ctx, back.ID); err != nil {
			t.Fatal(err)
		}
	}
	if got := cityTroops(); got != 1000 {
		t.Fatalf("city holds %d after the return", got)
	}
}

func TestMySQLViewShowsNearbyTiles(t *testing.T) {
	ctx := context.Background()
	db := itest.MySQL(t)
	s := &MySQLStore{DB: db}
	m, job := marchFixture()
	m.ToX, m.ToY = 100, 100
	if _, err := s.Create(ctx, m, job); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Arrive(ctx, m.ID); err != nil {
		t.Fatal(err)
	}
	near, err := s.View(ctx, m.Kingdom, 105, 95, viewRadius)
	if err != nil || len(near) != 1 || near[0].Owner != m.UID {
		t.Fatalf("near %+v: %v", near, err)
	}
	if far, err := s.View(ctx, m.Kingdom, 200, 100, viewRadius); err != nil || len(far) != 0 {
		t.Fatalf("far %+v: %v", far, err)
	}
}