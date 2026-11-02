// Package mail holds each player's letters: battle reports and rewards. One
// scene per uid. Other services send a letter by committing DeliveryJob in their
// own transaction, so a report cannot be lost once the fight is recorded, and
// Kafka being off changes nothing.
package mail

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	gamev1 "github.com/yourname/gameserver/gen/game/v1"
	"github.com/yourname/gameserver/internal/city"
	"github.com/yourname/gameserver/internal/gamehost"
	"github.com/yourname/gameserver/pkg/discovery"
	"github.com/yourname/gameserver/pkg/idgen"
	"github.com/yourname/gameserver/pkg/timer"
)

// ShardBase places this service's timer shards after the city's.
const ShardBase = 2000

// KindDeliver is the durable timer kind for an incoming letter.
const KindDeliver = "mail.deliver"

const listLimit = 50

// ErrNotFound means the player has no such mail.
var ErrNotFound = errors.New("mail: no such mail")

// Letter is what a service sends. Key dedupes a re-sent letter, up to 64 bytes.
type Letter struct {
	Key        string `json:"key"`
	UID        int64  `json:"uid"`
	Kind       string `json:"kind"`
	Title      string `json:"title"`
	Body       string `json:"body"`
	RewardGold int64  `json:"reward_gold"`
}

// Mail is a delivered letter.
type Mail struct {
	Letter
	ID      uint64
	SentAt  time.Time
	Claimed bool
}

func uidKey(uid int64) string { return strconv.FormatInt(uid, 10) }

// DeliveryJob builds the durable job that delivers l.
func DeliveryJob(l Letter) (timer.Job, error) {
	p, err := json.Marshal(l)
	if err != nil {
		return timer.Job{}, err
	}
	return timer.Job{ID: timer.KeyedID("mail:" + l.Key), Shard: ShardBase + discovery.Slot(uidKey(l.UID)),
		Kind: KindDeliver, Payload: p, FireAt: time.Now()}, nil
}

// Store persists mail.
type Store interface {
	// Deliver stores a letter once per (UID, Key). false means it already was.
	Deliver(ctx context.Context, m Mail) (bool, error)
	// List returns the newest mail first.
	List(ctx context.Context, uid int64, limit int) ([]Mail, error)
	// Claim marks a reward taken and commits the grant with it, in one commit.
	// Claiming twice is safe and returns the same gold.
	Claim(ctx context.Context, uid int64, id uint64) (int64, error)
}

func grantFor(m Mail) (timer.Job, error) {
	return city.GrantJob("mail:"+strconv.FormatUint(m.ID, 10), m.UID, m.RewardGold)
}

// Memory is an in-process Store.
type Memory struct {
	timers timer.Store
	mu     sync.Mutex
	mail   map[uint64]*Mail
	keys   map[string]bool
}

// NewMemory creates an empty store that writes jobs to timers.
func NewMemory(timers timer.Store) *Memory {
	return &Memory{timers: timers, mail: map[uint64]*Mail{}, keys: map[string]bool{}}
}

// Deliver implements Store.
func (s *Memory) Deliver(_ context.Context, m Mail) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := fmt.Sprint(m.UID, ":", m.Key)
	if s.keys[key] {
		return false, nil
	}
	s.keys[key] = true
	s.mail[m.ID] = &m
	return true, nil
}

// List implements Store.
func (s *Memory) List(_ context.Context, uid int64, limit int) ([]Mail, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []Mail
	for _, m := range s.mail {
		if m.UID == uid {
			out = append(out, *m)
		}
	}
	sort.Slice(out, func(a, b int) bool { return out[a].ID > out[b].ID })
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// Claim implements Store.
func (s *Memory) Claim(ctx context.Context, uid int64, id uint64) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.mail[id]
	if !ok || m.UID != uid {
		return 0, ErrNotFound
	}
	if !m.Claimed && m.RewardGold > 0 {
		j, err := grantFor(*m)
		if err != nil {
			return 0, err
		}
		if err := s.timers.Add(ctx, j); err != nil {
			return 0, err
		}
	}
	m.Claimed = true
	return m.RewardGold, nil
}

// Config wires the service.
type Config struct {
	Store  Store
	Timers *timer.Group
	IDs    *idgen.Generator
	Now    func() time.Time
}

// Service creates mailbox scenes and delivers letters.
type Service struct {
	cfg    Config
	scenes gamehost.Registry[*Scene]
}

// New creates a Service.
func New(cfg Config) *Service {
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	s := &Service{cfg: cfg}
	cfg.Timers.Handle(KindDeliver, s.onDeliver)
	return s
}

// Factory is a gamehost.Factory.
func (s *Service) Factory(key string, ctx *gamehost.SceneContext) gamehost.Scene {
	c := &Scene{key: key, svc: s, ctx: ctx}
	s.scenes.Put(key, c)
	return c
}

func (s *Service) onDeliver(ctx context.Context, j timer.Job) error {
	var l Letter
	if err := json.Unmarshal(j.Payload, &l); err != nil {
		return fmt.Errorf("mail: bad letter in job %d: %w", j.ID, err)
	}
	m := Mail{Letter: l, ID: uint64(s.cfg.IDs.Next()), SentAt: s.cfg.Now()}
	stored, err := s.cfg.Store.Deliver(ctx, m)
	if err != nil || !stored {
		return err
	}
	if c, ok := s.scenes.Get(uidKey(l.UID)); ok {
		// Best effort: the snapshot shows it to anyone who misses this.
		c.ctx.Actor.Post(func() {
			if p := c.ctx.Player(l.UID); p != nil {
				p.Push(gamev1.MsgId_MSG_ID_MAIL_NEW, &gamev1.MailNew{Mail: mailMsg(m)})
			}
		})
	}
	return nil
}

func mailMsg(m Mail) *gamev1.Mail {
	return &gamev1.Mail{MailId: m.ID, Kind: m.Kind, Title: m.Title, Body: m.Body, RewardGold: m.RewardGold,
		Claimed: m.Claimed, SentAtMs: m.SentAt.UnixMilli()}
}

// Scene is one player's mailbox.
type Scene struct {
	key string
	svc *Service
	ctx *gamehost.SceneContext
}

// OnJoin implements gamehost.Scene.
func (*Scene) OnJoin(*gamehost.Player) {}

// OnDisconnect implements gamehost.Scene.
func (*Scene) OnDisconnect(*gamehost.Player) {}

// OnReconnect implements gamehost.Scene.
func (*Scene) OnReconnect(*gamehost.Player) {}

// OnExpire implements gamehost.Scene.
func (*Scene) OnExpire(*gamehost.Player) {}

// OnRetire implements gamehost.Scene.
func (c *Scene) OnRetire() { c.svc.scenes.Remove(c.key, c) }

// Snapshot implements gamehost.Scene.
func (c *Scene) Snapshot(p *gamehost.Player) (gamev1.MsgId, proto.Message, error) {
	if !p.IsOwner(c.key) {
		return 0, nil, fmt.Errorf("mail: uid %d opened mailbox %s", p.UID, c.key)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	list, err := c.svc.cfg.Store.List(ctx, p.UID, listLimit)
	if err != nil {
		return 0, nil, err
	}
	msg := &gamev1.MailSnapshot{}
	for _, m := range list {
		msg.Mail = append(msg.Mail, mailMsg(m))
	}
	return gamev1.MsgId_MSG_ID_MAIL_SNAPSHOT, msg, nil
}

// OnMessage implements gamehost.Scene.
func (c *Scene) OnMessage(p *gamehost.Player, env *gamev1.Envelope) gamehost.Reply {
	req := &gamev1.MailClaimReq{}
	if env.GetMsgId() != uint32(gamev1.MsgId_MSG_ID_MAIL_CLAIM_REQ) || !p.IsOwner(c.key) ||
		proto.Unmarshal(env.GetPayload(), req) != nil {
		return gamehost.Reply{Code: gamev1.ErrorCode_ERROR_CODE_BAD_REQUEST}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	gold, err := c.svc.cfg.Store.Claim(ctx, p.UID, req.GetMailId())
	switch {
	case err == nil:
	case errors.Is(err, ErrNotFound):
		return gamehost.Reply{Code: gamev1.ErrorCode_ERROR_CODE_BAD_REQUEST}
	default:
		slog.Error("claim mail", "uid", p.UID, "err", err)
		return gamehost.Reply{Code: gamev1.ErrorCode_ERROR_CODE_INTERNAL}
	}
	return gamehost.Reply{MsgID: gamev1.MsgId_MSG_ID_MAIL_CLAIM_RSP,
		Msg: &gamev1.MailClaimRsp{MailId: req.GetMailId(), Gold: gold}}
}
