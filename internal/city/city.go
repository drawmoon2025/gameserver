// Package city is each player's base: buildings, upgrades and gold. One scene per
// uid. Its rules live on State, as pure functions, so that every Store applies
// them identically.
package city

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	gamev1 "github.com/yourname/gameserver/gen/game/v1"
	"github.com/yourname/gameserver/internal/gamehost"
	"github.com/yourname/gameserver/pkg/discovery"
	"github.com/yourname/gameserver/pkg/idgen"
	"github.com/yourname/gameserver/pkg/timer"
)

// ShardBase places this service's timer shards after the world's.
const ShardBase = 1000

// Durable timer kinds.
const (
	KindFinish = "city.upgrade"
	KindGrant  = "city.grant"
)

const (
	Hall = "hall"
	Mine = "mine" // Each level adds one gold per second.

	startGold    = 500
	maxLevel     = 30
	maxRequestID = 64
)

// Rule errors.
var (
	ErrBadKind      = errors.New("city: no such building, or it is at its top level")
	ErrBusy         = errors.New("city: building is already upgrading")
	ErrInsufficient = errors.New("city: not enough gold")
)

// Upgrade is one building level in progress.
type Upgrade struct {
	ID        uint64
	UID       int64
	Kind      string
	ToLevel   int32
	RequestID string
	FinishAt  time.Time
}

// State is a city. Gold is computed lazily: it is correct as of GoldAt.
type State struct {
	UID       int64
	Gold      int64
	GoldAt    time.Time
	Buildings map[string]int32
	Upgrades  []Upgrade // In progress.
}

func newState(uid int64, now time.Time) *State {
	return &State{UID: uid, Gold: startGold, GoldAt: now, Buildings: map[string]int32{Hall: 1, Mine: 1}}
}

func (s *State) settle(now time.Time) {
	secs := int64(now.Sub(s.GoldAt) / time.Second)
	if secs <= 0 {
		return
	}
	s.Gold += secs * int64(1+s.Buildings[Mine])
	s.GoldAt = s.GoldAt.Add(time.Duration(secs) * time.Second)
}

// begin charges for and starts an upgrade. Each level costs and takes twice the
// last. speed divides the duration.
func (s *State) begin(kind, requestID string, id uint64, now time.Time, speed float64) (Upgrade, error) {
	level, ok := s.Buildings[kind]
	if !ok || level >= maxLevel {
		return Upgrade{}, ErrBadKind
	}
	for _, u := range s.Upgrades {
		if u.Kind == kind {
			return Upgrade{}, ErrBusy
		}
	}
	s.settle(now)
	cost := int64(100) << level
	if s.Gold < cost {
		return Upgrade{}, ErrInsufficient
	}
	s.Gold -= cost
	took := time.Duration(float64((10 * time.Second) << level) / speed)
	u := Upgrade{ID: id, UID: s.UID, Kind: kind, ToLevel: level + 1, RequestID: requestID, FinishAt: now.Add(took)}
	s.Upgrades = append(s.Upgrades, u)
	return u, nil
}

func (s *State) finish(u Upgrade, now time.Time) {
	s.settle(now) // Gold earned so far was earned at the old rate.
	s.Buildings[u.Kind] = u.ToLevel
	for i := range s.Upgrades {
		if s.Upgrades[i].ID == u.ID {
			s.Upgrades = append(s.Upgrades[:i], s.Upgrades[i+1:]...)
			break
		}
	}
}

// Store persists cities.
type Store interface {
	// Load returns a city, creating it on first use.
	Load(ctx context.Context, uid int64, now time.Time) (*State, error)
	// Begin applies State.begin and stores the upgrade with its timer in one
	// commit. A repeated (uid, requestID) returns the first upgrade unchanged.
	Begin(ctx context.Context, uid int64, requestID, kind string, id uint64, now time.Time, speed float64) (Upgrade, int64, error)
	// Finish completes an upgrade once. It returns false if already done.
	Finish(ctx context.Context, id uint64, now time.Time) (Upgrade, bool, error)
	// Grant adds gold once per key.
	Grant(ctx context.Context, uid int64, key string, gold int64, now time.Time) error
}

func uidKey(uid int64) string { return strconv.FormatInt(uid, 10) }

func shardOf(uid int64) int { return ShardBase + discovery.Slot(uidKey(uid)) }

func upgradeJob(u Upgrade) timer.Job {
	return timer.Job{ID: u.ID, Shard: shardOf(u.UID), Kind: KindFinish,
		Payload: binary.BigEndian.AppendUint64(nil, u.ID), FireAt: u.FinishAt}
}

type grant struct {
	UID  int64  `json:"uid"`
	Key  string `json:"key"`
	Gold int64  `json:"gold"`
}

// GrantJob builds a durable gold grant, for another service to commit in its
// own transaction. Both the job and the grant dedupe on key.
func GrantJob(key string, uid, gold int64) (timer.Job, error) {
	p, err := json.Marshal(grant{UID: uid, Key: key, Gold: gold})
	if err != nil {
		return timer.Job{}, err
	}
	return timer.Job{ID: timer.KeyedID("grant:" + key), Shard: shardOf(uid), Kind: KindGrant,
		Payload: p, FireAt: time.Now()}, nil
}

// Memory is an in-process Store.
type Memory struct {
	timers   timer.Store
	mu       sync.Mutex
	cities   map[int64]*State
	upgrades map[uint64]*Upgrade
	requests map[string]Upgrade
	grants   map[string]bool
}

// NewMemory creates an empty store that writes jobs to timers.
func NewMemory(timers timer.Store) *Memory {
	return &Memory{timers: timers, cities: map[int64]*State{}, upgrades: map[uint64]*Upgrade{},
		requests: map[string]Upgrade{}, grants: map[string]bool{}}
}

func (m *Memory) city(uid int64, now time.Time) *State {
	s, ok := m.cities[uid]
	if !ok {
		s = newState(uid, now)
		m.cities[uid] = s
	}
	return s
}

// Load implements Store.
func (m *Memory) Load(_ context.Context, uid int64, now time.Time) (*State, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.city(uid, now)
	s.settle(now)
	c := *s
	c.Buildings = make(map[string]int32, len(s.Buildings))
	for k, v := range s.Buildings {
		c.Buildings[k] = v
	}
	c.Upgrades = append([]Upgrade(nil), s.Upgrades...)
	return &c, nil
}

// Begin implements Store.
func (m *Memory) Begin(ctx context.Context, uid int64, requestID, kind string, id uint64, now time.Time, speed float64) (Upgrade, int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.city(uid, now)
	key := fmt.Sprint(uid, ":", requestID)
	if u, ok := m.requests[key]; ok {
		s.settle(now)
		return u, s.Gold, nil
	}
	gold := s.Gold
	before := s.GoldAt
	u, err := s.begin(kind, requestID, id, now, speed)
	if err != nil {
		return Upgrade{}, 0, err
	}
	if err := m.timers.Add(ctx, upgradeJob(u)); err != nil {
		s.Gold, s.GoldAt = gold, before
		s.Upgrades = s.Upgrades[:len(s.Upgrades)-1]
		return Upgrade{}, 0, err
	}
	m.requests[key] = u
	m.upgrades[u.ID] = &u
	return u, s.Gold, nil
}

// Finish implements Store.
func (m *Memory) Finish(_ context.Context, id uint64, now time.Time) (Upgrade, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	u, ok := m.upgrades[id]
	if !ok {
		for _, r := range m.requests {
			if r.ID == id {
				return r, false, nil
			}
		}
		return Upgrade{}, false, fmt.Errorf("city: unknown upgrade %d", id)
	}
	m.city(u.UID, now).finish(*u, now)
	delete(m.upgrades, id)
	return *u, true, nil
}

// Grant implements Store.
func (m *Memory) Grant(_ context.Context, uid int64, key string, gold int64, now time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.grants[key] {
		return nil
	}
	m.grants[key] = true
	s := m.city(uid, now)
	s.settle(now)
	s.Gold += gold
	return nil
}

// Config wires the service.
type Config struct {
	Store  Store
	Timers *timer.Group
	IDs    *idgen.Generator
	Speed  float64 // Divides upgrade times. Default 1.
	Now    func() time.Time
}

// Service creates city scenes and handles their timers.
type Service struct {
	cfg    Config
	scenes gamehost.Registry[*Scene]
}

// New creates a Service.
func New(cfg Config) *Service {
	if cfg.Speed <= 0 {
		cfg.Speed = 1
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	s := &Service{cfg: cfg}
	cfg.Timers.Handle(KindFinish, s.onFinish)
	cfg.Timers.Handle(KindGrant, s.onGrant)
	return s
}

// Factory is a gamehost.Factory.
func (s *Service) Factory(key string, ctx *gamehost.SceneContext) gamehost.Scene {
	c := &Scene{key: key, svc: s, ctx: ctx}
	s.scenes.Put(key, c)
	return c
}

func (s *Service) onFinish(ctx context.Context, j timer.Job) error {
	if len(j.Payload) != 8 {
		return fmt.Errorf("city: bad payload for job %d", j.ID)
	}
	u, applied, err := s.cfg.Store.Finish(ctx, binary.BigEndian.Uint64(j.Payload), s.cfg.Now())
	if err != nil || !applied {
		return err
	}
	if c, ok := s.scenes.Get(uidKey(u.UID)); ok {
		// Best effort: the snapshot shows it to anyone who misses this.
		c.ctx.Actor.Post(func() {
			if p := c.ctx.Player(u.UID); p != nil {
				p.Push(gamev1.MsgId_MSG_ID_CITY_UPGRADED, &gamev1.CityUpgraded{Kind: u.Kind, Level: u.ToLevel})
			}
		})
	}
	return nil
}

func (s *Service) onGrant(ctx context.Context, j timer.Job) error {
	var g grant
	if err := json.Unmarshal(j.Payload, &g); err != nil {
		return fmt.Errorf("city: bad grant in job %d: %w", j.ID, err)
	}
	return s.cfg.Store.Grant(ctx, g.UID, g.Key, g.Gold, s.cfg.Now())
}

// Scene is one player's city.
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

func upgradeMsg(u Upgrade) *gamev1.CityUpgrade {
	return &gamev1.CityUpgrade{UpgradeId: u.ID, Kind: u.Kind, ToLevel: u.ToLevel, FinishAtMs: u.FinishAt.UnixMilli()}
}

// Snapshot implements gamehost.Scene.
func (c *Scene) Snapshot(p *gamehost.Player) (gamev1.MsgId, proto.Message, error) {
	if !p.IsOwner(c.key) {
		return 0, nil, fmt.Errorf("city: uid %d opened city %s", p.UID, c.key)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	s, err := c.svc.cfg.Store.Load(ctx, p.UID, c.svc.cfg.Now())
	if err != nil {
		return 0, nil, err
	}
	msg := &gamev1.CitySnapshot{Gold: s.Gold}
	for kind, level := range s.Buildings {
		msg.Buildings = append(msg.Buildings, &gamev1.Building{Kind: kind, Level: level})
	}
	for _, u := range s.Upgrades {
		msg.Upgrades = append(msg.Upgrades, upgradeMsg(u))
	}
	return gamev1.MsgId_MSG_ID_CITY_SNAPSHOT, msg, nil
}

// OnMessage implements gamehost.Scene.
func (c *Scene) OnMessage(p *gamehost.Player, env *gamev1.Envelope) gamehost.Reply {
	req := &gamev1.CityUpgradeReq{}
	if env.GetMsgId() != uint32(gamev1.MsgId_MSG_ID_CITY_UPGRADE_REQ) || !p.IsOwner(c.key) ||
		proto.Unmarshal(env.GetPayload(), req) != nil ||
		env.GetRequestId() == "" || len(env.GetRequestId()) > maxRequestID {
		return gamehost.Reply{Code: gamev1.ErrorCode_ERROR_CODE_BAD_REQUEST}
	}
	s := c.svc
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	u, gold, err := s.cfg.Store.Begin(ctx, p.UID, env.GetRequestId(), req.GetKind(),
		uint64(s.cfg.IDs.Next()), s.cfg.Now(), s.cfg.Speed)
	switch {
	case err == nil:
	case errors.Is(err, ErrBadKind):
		return gamehost.Reply{Code: gamev1.ErrorCode_ERROR_CODE_BAD_REQUEST}
	case errors.Is(err, ErrBusy):
		return gamehost.Reply{Code: gamev1.ErrorCode_ERROR_CODE_DUPLICATE_IN_PROGRESS}
	case errors.Is(err, ErrInsufficient):
		return gamehost.Reply{Code: gamev1.ErrorCode_ERROR_CODE_INSUFFICIENT_FUNDS}
	default:
		slog.Error("begin upgrade", "uid", p.UID, "err", err)
		return gamehost.Reply{Code: gamev1.ErrorCode_ERROR_CODE_INTERNAL}
	}
	s.cfg.Timers.Wake(discovery.Slot(c.key))
	return gamehost.Reply{MsgID: gamev1.MsgId_MSG_ID_CITY_UPGRADE_RSP,
		Msg: &gamev1.CityUpgradeRsp{Upgrade: upgradeMsg(u), Gold: gold}}
}
