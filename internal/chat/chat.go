// Package chat runs channels. One scene per channel key: "kingdom:<id>" is open,
// and "alliance:<name>" is for that alliance's members. History is a short
// recent list in Redis, which is enough for chat: nothing of value depends on it.
package chat

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"

	gamev1 "github.com/drawmoon2025/gameserver/gen/game/v1"
	"github.com/drawmoon2025/gameserver/pkg/gamehost"
	"github.com/drawmoon2025/gameserver/pkg/realm"
	"github.com/drawmoon2025/gameserver/pkg/discovery"
	"github.com/drawmoon2025/gameserver/pkg/idgen"
	"github.com/drawmoon2025/gameserver/pkg/timer"
)

// ShardBase places this service's timer shards after the rank service's.
const ShardBase = 4000

// KindRevoke is the durable timer kind for removing someone from a channel.
const KindRevoke = "chat.revoke"

const (
	maxText   = 512
	historyN  = 50
	historyAt = 7 * 24 * time.Hour
	minGap    = 500 * time.Millisecond
)

// Message is one line of chat.
type Message struct {
	ID     uint64    `json:"id"`
	UID    int64     `json:"uid"`
	Text   string    `json:"text"`
	SentAt time.Time `json:"sent_at"`
}

type revoke struct {
	Key string `json:"key"`
	UID int64  `json:"uid"`
}

// AllianceChannel is the channel key of an alliance.
func AllianceChannel(name string) string { return "alliance:" + name }

// RevokeJob builds the durable job that removes uid from channel key. Sender
// commits it in the transaction that ended the membership.
func RevokeJob(key string, uid int64, now time.Time) (timer.Job, error) {
	p, err := json.Marshal(revoke{Key: key, UID: uid})
	if err != nil {
		return timer.Job{}, err
	}
	id := timer.KeyedID(fmt.Sprint("chat:", key, ":", uid, ":", now.UnixNano()))
	return timer.Job{ID: id, Shard: ShardBase + discovery.Slot(key), Kind: KindRevoke, Payload: p, FireAt: now}, nil
}

// Store keeps recent messages.
type Store interface {
	Append(ctx context.Context, key string, m Message) error
	// Recent returns up to historyN messages, oldest first.
	Recent(ctx context.Context, key string) ([]Message, error)
}

// Memory is an in-process Store.
type Memory struct {
	mu   sync.Mutex
	logs map[string][]Message
}

// NewMemory creates an empty store.
func NewMemory() *Memory { return &Memory{logs: map[string][]Message{}} }

// Append implements Store.
func (s *Memory) Append(_ context.Context, key string, m Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	log := append(s.logs[key], m)
	if len(log) > historyN {
		log = log[len(log)-historyN:]
	}
	s.logs[key] = log
	return nil
}

// Recent implements Store.
func (s *Memory) Recent(_ context.Context, key string) ([]Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Message(nil), s.logs[key]...), nil
}

// Redis keeps each channel's recent messages in a capped list.
type Redis struct {
	Client redis.UniversalClient
}

func listKey(key string) string { return "chat:{" + key + "}" }

// Append implements Store.
func (s *Redis) Append(ctx context.Context, key string, m Message) error {
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	pipe := s.Client.TxPipeline()
	pipe.RPush(ctx, listKey(key), b)
	pipe.LTrim(ctx, listKey(key), -historyN, -1)
	pipe.Expire(ctx, listKey(key), historyAt)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("chat: append: %w", err)
	}
	return nil
}

// Recent implements Store.
func (s *Redis) Recent(ctx context.Context, key string) ([]Message, error) {
	raw, err := s.Client.LRange(ctx, listKey(key), 0, -1).Result()
	if err != nil {
		return nil, fmt.Errorf("chat: recent: %w", err)
	}
	out := make([]Message, 0, len(raw))
	for _, r := range raw {
		var m Message
		if err := json.Unmarshal([]byte(r), &m); err != nil {
			return nil, fmt.Errorf("chat: bad message: %w", err)
		}
		out = append(out, m)
	}
	return out, nil
}

// Config wires the service.
type Config struct {
	Store  Store
	Timers *timer.Group
	IDs    *idgen.Generator
	// AllianceOf names a player's alliance, or "" if none.
	AllianceOf func(ctx context.Context, uid int64) (string, error)
	MemberOf func(ctx context.Context, uid int64) (realm.Member, error)
	MinGap     time.Duration // Between one player's messages. Default 500ms.
	Now        func() time.Time
}

// Service creates channel scenes.
type Service struct {
	cfg    Config
	scenes gamehost.Registry[*Channel]
}

// New creates a Service.
func New(cfg Config) *Service {
	if cfg.MinGap <= 0 {
		cfg.MinGap = minGap
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	s := &Service{cfg: cfg}
	cfg.Timers.Handle(KindRevoke, s.onRevoke)
	return s
}

// Factory is a gamehost.Factory.
func (s *Service) Factory(key string, ctx *gamehost.SceneContext) gamehost.Scene {
	c := &Channel{key: key, svc: s, ctx: ctx, last: map[int64]time.Time{}}
	s.scenes.Put(key, c)
	return c
}

func (s *Service) onRevoke(_ context.Context, j timer.Job) error {
	var r revoke
	if err := json.Unmarshal(j.Payload, &r); err != nil {
		return fmt.Errorf("chat: bad revoke in job %d: %w", j.ID, err)
	}
	if c, ok := s.scenes.Get(r.Key); ok {
		c.ctx.Actor.Post(func() {
			if p := c.ctx.Player(r.UID); p != nil {
				p.Close("removed from channel")
			}
		})
	}
	return nil
}

// allowed decides who may be in a channel.
func (s *Service) allowed(ctx context.Context, uid int64, key string) (bool, error) {
	if id, ok := strings.CutPrefix(key, "kingdom:"); ok {
		me, err := s.cfg.MemberOf(ctx, uid)
		return id != "" && me.Kingdom == id, err
	}
	name, ok := strings.CutPrefix(key, "alliance:")
	if !ok || name == "" {
		return false, nil
	}
	mine, err := s.cfg.AllianceOf(ctx, uid)
	return mine == name, err
}

// Channel is one chat channel.
type Channel struct {
	key  string
	svc  *Service
	ctx  *gamehost.SceneContext
	last map[int64]time.Time // Actor-only.
}

// Admit implements gamehost.Gate.
func (c *Channel) Admit(uid int64) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	ok, err := c.svc.allowed(ctx, uid, c.key)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("chat: uid %d may not join %s", uid, c.key)
	}
	return nil
}

// OnJoin implements gamehost.Scene.
func (*Channel) OnJoin(*gamehost.Player) {}

// OnDisconnect implements gamehost.Scene.
func (*Channel) OnDisconnect(*gamehost.Player) {}

// OnReconnect implements gamehost.Scene.
func (*Channel) OnReconnect(*gamehost.Player) {}

// OnExpire implements gamehost.Scene.
func (c *Channel) OnExpire(p *gamehost.Player) { delete(c.last, p.UID) }

// OnRetire implements gamehost.Scene.
func (c *Channel) OnRetire() { c.svc.scenes.Remove(c.key, c) }

func messageMsg(m Message) *gamev1.ChatMessage {
	return &gamev1.ChatMessage{MessageId: m.ID, Uid: m.UID, Text: m.Text, SentAtMs: m.SentAt.UnixMilli()}
}

// Snapshot implements gamehost.Scene.
func (c *Channel) Snapshot(*gamehost.Player) (gamev1.MsgId, proto.Message, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	log, err := c.svc.cfg.Store.Recent(ctx, c.key)
	if err != nil {
		return 0, nil, err
	}
	msg := &gamev1.ChatSnapshot{Channel: c.key}
	for _, m := range log {
		msg.Messages = append(msg.Messages, messageMsg(m))
	}
	return gamev1.MsgId_MSG_ID_CHAT_SNAPSHOT, msg, nil
}

// OnMessage implements gamehost.Scene.
func (c *Channel) OnMessage(p *gamehost.Player, env *gamev1.Envelope) gamehost.Reply {
	req := &gamev1.ChatSendReq{}
	if env.GetMsgId() != uint32(gamev1.MsgId_MSG_ID_CHAT_SEND_REQ) ||
		proto.Unmarshal(env.GetPayload(), req) != nil ||
		req.GetText() == "" || len(req.GetText()) > maxText {
		return gamehost.Reply{Code: gamev1.ErrorCode_ERROR_CODE_BAD_REQUEST}
	}
	s := c.svc
	now := s.cfg.Now()
	if now.Sub(c.last[p.UID]) < s.cfg.MinGap {
		return gamehost.Reply{Code: gamev1.ErrorCode_ERROR_CODE_RATE_LIMITED}
	}
	c.last[p.UID] = now
	m := Message{ID: uint64(s.cfg.IDs.Next()), UID: p.UID, Text: req.GetText(), SentAt: now}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := s.cfg.Store.Append(ctx, c.key, m); err != nil {
		slog.Error("chat append", "channel", c.key, "err", err)
		return gamehost.Reply{Code: gamev1.ErrorCode_ERROR_CODE_INTERNAL}
	}
	push := &gamev1.ChatPush{Channel: c.key, Message: messageMsg(m)}
	for uid, other := range c.ctx.Players() {
		if uid != p.UID {
			other.Push(gamev1.MsgId_MSG_ID_CHAT_MESSAGE, push)
		}
	}
	return gamehost.Reply{MsgID: gamev1.MsgId_MSG_ID_CHAT_SEND_RSP, Msg: push}
}
