package e2e

import (
	"context"
	"testing"
	"time"

	gamev1 "github.com/yourname/gameserver/gen/game/v1"
	"github.com/yourname/gameserver/internal/alliance"
	"github.com/yourname/gameserver/internal/chat"
	"github.com/yourname/gameserver/pkg/discovery"
	"github.com/yourname/gameserver/pkg/idgen"
	"github.com/yourname/gameserver/pkg/timer"
)

// society is alliance and chat sharing one job store.
type society struct {
	*stack
}

func newSociety(t *testing.T, minGap time.Duration) *society {
	t.Helper()
	ids, err := idgen.New(1)
	if err != nil {
		t.Fatal(err)
	}
	jobs := timer.NewMemStore()
	timers := timer.NewGroup(timer.Config{Owner: "t", Store: jobs, Poll: 5 * time.Millisecond, ShardBase: chat.ShardBase})
	t.Cleanup(timers.StopAll)
	members := alliance.NewMemory(jobs)
	c := chat.New(chat.Config{Store: chat.NewMemory(), Timers: timers, IDs: ids, AllianceOf: members.AllianceOf, MinGap: minGap})
	timers.Start(context.Background(), discovery.Slot(chat.AllianceChannel("wolves")))
	a := &alliance.Service{Store: members}
	return &society{newStack(t, map[string][]string{
		"alliance": {startBackend(t, a.Factory, time.Minute)},
		"chat":     {startBackend(t, c.Factory, time.Minute)},
	})}
}

func allianceOp(c *client, seq uint64, id gamev1.MsgId, msg *gamev1.AllianceKickReq) *gamev1.AllianceInfo {
	c.t.Helper()
	c.send(&gamev1.Envelope{MsgId: uint32(id), ClientSeq: seq}, msg)
	env := c.until(gamev1.MsgId_MSG_ID_ALLIANCE_RSP)
	return decode(c.t, env, &gamev1.AllianceInfo{})
}

func say(c *client, seq uint64, channel uint32, text string) *gamev1.Envelope {
	c.t.Helper()
	c.send(&gamev1.Envelope{MsgId: uint32(gamev1.MsgId_MSG_ID_CHAT_SEND_REQ), ClientSeq: seq, Channel: channel},
		&gamev1.ChatSendReq{Text: text})
	return c.read()
}

func TestAllianceChatFollowsMembership(t *testing.T) {
	s := newSociety(t, time.Nanosecond)
	wolves := &gamev1.Binding{Service: "alliance", Key: "wolves"}
	room := &gamev1.Binding{Service: "chat", Key: chat.AllianceChannel("wolves")}

	lead := s.dial(t, s.login(t, 1), wolves)
	lead.until(gamev1.MsgId_MSG_ID_ALLIANCE_SNAPSHOT)
	if i := allianceOp(lead, 1, gamev1.MsgId_MSG_ID_ALLIANCE_CREATE_REQ, &gamev1.AllianceKickReq{}); i.GetLeader() != 1 {
		t.Fatalf("created %v", i)
	}

	outsiderTok := s.login(t, 2)
	closes(t, s.dial(t, outsiderTok, room)) // Not a member yet.

	join := s.dial(t, outsiderTok, wolves)
	join.until(gamev1.MsgId_MSG_ID_ALLIANCE_SNAPSHOT)
	if i := allianceOp(join, 1, gamev1.MsgId_MSG_ID_ALLIANCE_JOIN_REQ, &gamev1.AllianceKickReq{}); len(i.GetMembers()) != 2 {
		t.Fatalf("joined %v", i)
	}
	lead.until(gamev1.MsgId_MSG_ID_ALLIANCE_SNAPSHOT) // Told of the new member.

	leadChat := s.dial(t, s.login(t, 1), wolves, room)
	seen := map[uint32]bool{}
	for len(seen) < 2 {
		seen[leadChat.read().GetMsgId()] = true
		delete(seen, uint32(gamev1.MsgId_MSG_ID_RESUME_RSP))
	}
	mem := s.dial(t, outsiderTok, room)
	last := mem.until(gamev1.MsgId_MSG_ID_CHAT_SNAPSHOT).GetSeq()

	if env := say(leadChat, 1, 1, "hello"); env.GetCode() != 0 {
		t.Fatalf("send %v", env)
	}
	got := decode(t, mem.until(gamev1.MsgId_MSG_ID_CHAT_MESSAGE), &gamev1.ChatPush{})
	if got.GetMessage().GetText() != "hello" || got.GetMessage().GetUid() != 1 {
		t.Fatalf("message %v", got)
	}

	// Kicking closes the member's channel, and the seat must not be a way back.
	allianceOp(leadChat, 2, gamev1.MsgId_MSG_ID_ALLIANCE_KICK_REQ, &gamev1.AllianceKickReq{Uid: 2})
	mem.until(gamev1.MsgId_MSG_ID_KICK)
	closes(t, mem)
	back := s.dial(t, outsiderTok, &gamev1.Binding{Service: "chat", Key: room.GetKey(), LastSeq: last})
	for {
		env, err := back.tryRead(2 * time.Second)
		if err != nil {
			break
		}
		if env.GetMsgId() == uint32(gamev1.MsgId_MSG_ID_CHAT_MESSAGE) || env.GetMsgId() == uint32(gamev1.MsgId_MSG_ID_RESUME_RSP) {
			t.Fatalf("a removed member got %v", env)
		}
	}
}

func TestChatRejectsFloodAndBadText(t *testing.T) {
	s := newSociety(t, time.Hour)
	c := s.dial(t, s.login(t, 1), &gamev1.Binding{Service: "chat", Key: "kingdom:k"})
	c.until(gamev1.MsgId_MSG_ID_CHAT_SNAPSHOT)
	if code := say(c, 1, 0, "").GetCode(); code != int32(gamev1.ErrorCode_ERROR_CODE_BAD_REQUEST) {
		t.Fatalf("empty text: code %d", code)
	}
	if env := say(c, 2, 0, "first"); env.GetCode() != 0 {
		t.Fatalf("first: %v", env)
	}
	if code := say(c, 3, 0, "second").GetCode(); code != int32(gamev1.ErrorCode_ERROR_CODE_RATE_LIMITED) {
		t.Fatalf("flood: code %d", code)
	}
}

func TestBattleScoreReachesTheBoard(t *testing.T) {
	k := newKingdom(t, time.Minute)
	board := &gamev1.Binding{Service: "rank", Key: "kingdom:k1"}
	war := func(c *client, id string, troops int64) {
		c.send(&gamev1.Envelope{MsgId: uint32(gamev1.MsgId_MSG_ID_MARCH_REQ), ClientSeq: 1, RequestId: id},
			&gamev1.MarchReq{ToX: 5, Troops: troops})
		c.until(gamev1.MsgId_MSG_ID_MARCH_ARRIVED)
	}
	world := &gamev1.Binding{Service: "world", Key: "k1"}
	d := k.dial(t, k.login(t, 1), world)
	d.until(gamev1.MsgId_MSG_ID_ROOM_SNAPSHOT)
	war(d, "d1", 10)
	a := k.dial(t, k.login(t, 2), world)
	a.until(gamev1.MsgId_MSG_ID_ROOM_SNAPSHOT)
	war(a, "a1", 1000)

	deadline := time.Now().Add(3 * time.Second)
	for {
		e, err := k.boards.Position(context.Background(), "kingdom:k1", 2)
		if err != nil {
			t.Fatal(err)
		}
		if e.Score == 10 && e.Rank == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("board entry %+v", e)
		}
		time.Sleep(5 * time.Millisecond)
	}
	c := k.dial(t, k.login(t, 2), board)
	got := decode(t, c.until(gamev1.MsgId_MSG_ID_RANK_SNAPSHOT), &gamev1.RankBoard{})
	if got.GetSelfRank() != 1 || got.GetSelfScore() != 10 || len(got.GetEntries()) != 1 {
		t.Fatalf("board %v", got)
	}
}
