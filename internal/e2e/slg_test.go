package e2e

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	gamev1 "github.com/yourname/gameserver/gen/game/v1"
	"github.com/yourname/gameserver/internal/city"
	"github.com/yourname/gameserver/internal/mail"
	"github.com/yourname/gameserver/internal/slg"
	"github.com/yourname/gameserver/pkg/discovery"
	"github.com/yourname/gameserver/pkg/timer"
)

// kingdom is world, city and mail sharing one job store, as they share a
// database in production.
type kingdom struct {
	*stack
	cities *city.Memory
	jobs   *timer.MemStore
	now    atomic.Int64 // Frozen city clock, so gold changes only when told to.
}

func newKingdom(t *testing.T, grace time.Duration) *kingdom {
	t.Helper()
	_, _, ids := worldDeps(t)
	k := &kingdom{jobs: timer.NewMemStore()}
	k.cities = city.NewMemory(k.jobs)
	k.now.Store(time.Now().UnixMilli())
	now := func() time.Time { return time.UnixMilli(k.now.Load()) }
	group := func(base int) *timer.Group {
		g := timer.NewGroup(timer.Config{Owner: "t", Store: k.jobs, Poll: 5 * time.Millisecond, ShardBase: base})
		t.Cleanup(g.StopAll)
		return g
	}
	wg, cg, mg := group(0), group(city.ShardBase), group(mail.ShardBase)
	w := slg.New(slg.Config{Store: slg.NewMemoryStore(k.jobs), Timers: wg, IDs: ids, Speed: 1000})
	c := city.New(city.Config{Store: k.cities, Timers: cg, IDs: ids, Speed: 1000, Now: now})
	m := mail.New(mail.Config{Store: mail.NewMemory(k.jobs), Timers: mg, IDs: ids})
	ctx := context.Background()
	wg.Start(ctx, discovery.Slot("k1"))
	for _, uid := range []string{"1", "2"} {
		cg.Start(ctx, discovery.Slot(uid))
		mg.Start(ctx, discovery.Slot(uid))
	}
	k.stack = newStack(t, map[string][]string{
		"world": {startBackend(t, w.Factory, grace)},
		"city":  {startBackend(t, c.Factory, grace)},
		"mail":  {startBackend(t, m.Factory, grace)},
	})
	return k
}

func cityBinding(uid string) *gamev1.Binding { return &gamev1.Binding{Service: "city", Key: uid} }
func mailBinding(uid string) *gamev1.Binding { return &gamev1.Binding{Service: "mail", Key: uid} }

func upgrade(c *client, seq uint64, requestID, kind string) *gamev1.Envelope {
	c.t.Helper()
	c.send(&gamev1.Envelope{MsgId: uint32(gamev1.MsgId_MSG_ID_CITY_UPGRADE_REQ), ClientSeq: seq, RequestId: requestID},
		&gamev1.CityUpgradeReq{Kind: kind})
	return c.read()
}

func decode[T proto.Message](t *testing.T, env *gamev1.Envelope, msg T) T {
	t.Helper()
	if err := proto.Unmarshal(env.GetPayload(), msg); err != nil {
		t.Fatal(err)
	}
	return msg
}

func TestUpgradeChargesOnceAndCompletes(t *testing.T) {
	k := newKingdom(t, 20*time.Millisecond)
	tok := k.login(t, 1)
	c := k.dial(t, tok, cityBinding("1"))
	c.until(gamev1.MsgId_MSG_ID_CITY_SNAPSHOT)

	rsp := decode(t, upgrade(c, 1, "u1", city.Mine), &gamev1.CityUpgradeRsp{})
	if rsp.GetGold() != 300 || rsp.GetUpgrade().GetToLevel() != 2 {
		t.Fatalf("rsp %v", rsp)
	}
	done := decode(t, c.until(gamev1.MsgId_MSG_ID_CITY_UPGRADED), &gamev1.CityUpgraded{})
	if done.GetKind() != city.Mine || done.GetLevel() != 2 {
		t.Fatalf("upgraded %v", done)
	}
	if err := c.ws.CloseNow(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond) // The seat, and its dedupe state, expire.

	c2 := k.dial(t, tok, cityBinding("1"))
	snap := decode(t, c2.until(gamev1.MsgId_MSG_ID_CITY_SNAPSHOT), &gamev1.CitySnapshot{})
	if snap.GetGold() != 300 {
		t.Fatalf("snapshot %v", snap)
	}
	again := decode(t, upgrade(c2, 1, "u1", city.Mine), &gamev1.CityUpgradeRsp{})
	if again.GetUpgrade().GetUpgradeId() != rsp.GetUpgrade().GetUpgradeId() || again.GetGold() != 300 {
		t.Fatalf("a retry after seat loss charged again: %v", again)
	}
}

func TestUpgradeRules(t *testing.T) {
	k := newKingdom(t, time.Minute)
	c := k.dial(t, k.login(t, 1), cityBinding("1"))
	c.until(gamev1.MsgId_MSG_ID_CITY_SNAPSHOT)
	if code := upgrade(c, 1, "a", "palace").GetCode(); code != int32(gamev1.ErrorCode_ERROR_CODE_BAD_REQUEST) {
		t.Fatalf("unknown building: code %d", code)
	}
	if code := upgrade(c, 2, "", city.Mine).GetCode(); code != int32(gamev1.ErrorCode_ERROR_CODE_BAD_REQUEST) {
		t.Fatalf("no request id: code %d", code)
	}
	// Level 1 costs 200 and level 2 costs 400, against 500 gold.
	if env := upgrade(c, 3, "b", city.Hall); env.GetCode() != 0 {
		t.Fatalf("first upgrade: %v", env)
	}
	c.until(gamev1.MsgId_MSG_ID_CITY_UPGRADED)
	if code := upgrade(c, 4, "c", city.Hall).GetCode(); code != int32(gamev1.ErrorCode_ERROR_CODE_INSUFFICIENT_FUNDS) {
		t.Fatalf("too poor: code %d", code)
	}
}

func TestOneCannotOpenAnothersCityOrMail(t *testing.T) {
	k := newKingdom(t, time.Minute)
	closes(t, k.dial(t, k.login(t, 1), cityBinding("2")))
	closes(t, k.dial(t, k.login(t, 1), mailBinding("2")))
}

func TestBattleReportsAndLootReachBothPlayers(t *testing.T) {
	k := newKingdom(t, time.Minute)
	march := func(c *client, seq uint64, id string, troops int64) *gamev1.MarchArrived {
		c.send(&gamev1.Envelope{MsgId: uint32(gamev1.MsgId_MSG_ID_MARCH_REQ), ClientSeq: seq, RequestId: id},
			&gamev1.MarchReq{ToX: 5, Troops: troops})
		return decode(t, c.until(gamev1.MsgId_MSG_ID_MARCH_ARRIVED), &gamev1.MarchArrived{})
	}
	binding := func(uid string) []*gamev1.Binding {
		return []*gamev1.Binding{{Service: "world", Key: "k1"}, mailBinding(uid), cityBinding(uid)}
	}
	ready := func(c *client) { // Three tunnels, so the snapshots arrive in any order.
		seen := map[uint32]bool{}
		for len(seen) < 3 {
			switch id := c.read().GetMsgId(); id {
			case uint32(gamev1.MsgId_MSG_ID_ROOM_SNAPSHOT), uint32(gamev1.MsgId_MSG_ID_MAIL_SNAPSHOT), uint32(gamev1.MsgId_MSG_ID_CITY_SNAPSHOT):
				seen[id] = true
			}
		}
	}
	defender := k.dial(t, k.login(t, 1), binding("1")...)
	ready(defender)
	attacker := k.dial(t, k.login(t, 2), binding("2")...)
	ready(attacker)

	march(defender, 1, "d1", 10)
	if left := march(attacker, 1, "a1", 1000); left.GetTroops() <= 0 || left.GetTroops() > 1000 {
		t.Fatalf("attacker arrived with %d", left.GetTroops())
	}

	lost := decode(t, defender.until(gamev1.MsgId_MSG_ID_MAIL_NEW), &gamev1.MailNew{}).GetMail()
	if lost.GetTitle() != "Defeat" || lost.GetRewardGold() != 0 {
		t.Fatalf("defender's mail %v", lost)
	}
	won := decode(t, attacker.until(gamev1.MsgId_MSG_ID_MAIL_NEW), &gamev1.MailNew{}).GetMail()
	if won.GetTitle() != "Victory" || won.GetRewardGold() != 10 {
		t.Fatalf("attacker's mail %v", won)
	}

	claim := func(seq uint64) *gamev1.MailClaimRsp {
		attacker.send(&gamev1.Envelope{MsgId: uint32(gamev1.MsgId_MSG_ID_MAIL_CLAIM_REQ), ClientSeq: seq, Channel: 1},
			&gamev1.MailClaimReq{MailId: won.GetMailId()})
		return decode(t, attacker.until(gamev1.MsgId_MSG_ID_MAIL_CLAIM_RSP), &gamev1.MailClaimRsp{})
	}
	if r := claim(1); r.GetGold() != 10 {
		t.Fatalf("claim %v", r)
	}
	claim(2) // A second claim must grant nothing.

	deadline := time.Now().Add(3 * time.Second)
	for {
		s, err := k.cities.Load(context.Background(), 2, time.UnixMilli(k.now.Load()))
		if err != nil {
			t.Fatal(err)
		}
		if s.Gold == 510 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("city gold %d, want 510", s.Gold)
		}
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond)
	if s, _ := k.cities.Load(context.Background(), 2, time.UnixMilli(k.now.Load())); s.Gold != 510 {
		t.Fatalf("gold granted twice: %d", s.Gold)
	}
}
