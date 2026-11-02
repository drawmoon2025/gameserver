// Package slg is the world service of an SLG: one Scene per kingdom. It shows
// how long-running actions work. State and a durable timer are written first,
// the client is acknowledged, and arrival is applied exactly once by a
// conditional state change, even though the timer fires at-least-once.
//
// See docs/ARCHITECTURE.md for how it relates to the other SLG services.
package slg

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	gamev1 "github.com/drawmoon2025/gameserver/gen/game/v1"
	"github.com/drawmoon2025/gameserver/internal/city"
	"github.com/drawmoon2025/gameserver/pkg/gamehost"
	"github.com/drawmoon2025/gameserver/pkg/realm"
	"github.com/drawmoon2025/gameserver/pkg/actor"
	"github.com/drawmoon2025/gameserver/pkg/discovery"
	"github.com/drawmoon2025/gameserver/pkg/idgen"
	"github.com/drawmoon2025/gameserver/pkg/timer"
)

// KindMarchArrive is the durable timer kind for a march reaching its target.
const KindMarchArrive = "march.arrive"

// March is one army moving across the map.
type March struct {
	ID        uint64
	Kingdom   string
	UID       int64
	RequestID string // Unique per UID. It makes a retry find the first march.
	FromX     int32
	FromY     int32
	ToX       int32
	ToY       int32
	Troops    int64
	ArriveAt  time.Time
	Home      bool // A recalled army: its arrival returns troops to the city.
}

// Tile is one held map cell.
type Tile struct {
	X, Y   int32
	Owner  int64
	Troops int64
}

// Limits on what one read returns.
const (
	maxOwned   = 1000
	viewRadius = 10 // A view is 21 by 21 tiles.
)

// Store persists marches.
type Store interface {
	// Create takes a march's troops from its city and stores it in the marching
	// state with the timer that ends it. All of this commits as one: a march
	// without a timer would never arrive. It fails with city.ErrNoTroops.
	// If (UID, RequestID) exists it stores nothing and returns that march, since
	// client_seq dedupe does not outlive the seat but a march does.
	Create(ctx context.Context, m March, arrive timer.Job) (March, error)
	// Recall is Create for a Home march. It takes the troops from the tile at
	// (FromX, FromY), and fails with city.ErrNoTroops unless UID holds that many.
	Recall(ctx context.Context, m March, arrive timer.Job) (March, error)
	// Arrive moves a marching march to arrived and applies settle, in one
	// commit that includes any mail. The returned march holds the troops left.
	// It returns false if already arrived, making a re-fired timer harmless.
	Arrive(ctx context.Context, id uint64) (March, bool, error)
	// Troops sums the troops a player holds on tiles in a kingdom.
	Troops(ctx context.Context, kingdom string, uid int64) (int64, error)
	// Owned lists a player's tiles, up to maxOwned.
	Owned(ctx context.Context, kingdom string, uid int64) ([]Tile, error)
	// View lists the held tiles within radius of a point.
	View(ctx context.Context, kingdom string, x, y, radius int32) ([]Tile, error)
	// Active lists a player's armies on the road.
	Active(ctx context.Context, kingdom string, uid int64) ([]March, error)
}

// Barracks is the part of a city a MemoryStore needs. The MySQL store does the
// same through city.TakeTroopsTx, inside its own transaction.
type Barracks interface {
	TakeTroops(ctx context.Context, uid, n int64, now time.Time) error
	AddTroops(ctx context.Context, uid, n int64, now time.Time) error
}

// MemoryStore is an in-process Store.
type MemoryStore struct {
	timers   timer.Store
	barracks Barracks
	mu       sync.Mutex
	marches  map[uint64]March
	arrived  map[uint64]bool
	requests map[string]uint64
	tiles    map[tileKey]tile
}

type tileKey struct {
	kingdom string
	x, y    int32
}

// NewMemoryStore creates an empty store that writes jobs to timers.
func NewMemoryStore(timers timer.Store, barracks Barracks) *MemoryStore {
	return &MemoryStore{timers: timers, barracks: barracks, marches: map[uint64]March{}, arrived: map[uint64]bool{},
		requests: map[string]uint64{}, tiles: map[tileKey]tile{}}
}

// Create implements Store.
func (s *MemoryStore) Create(ctx context.Context, m March, arrive timer.Job) (March, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := fmt.Sprint(m.UID, ":", m.RequestID)
	if id, ok := s.requests[key]; ok {
		return s.marches[id], nil
	}
	if err := s.barracks.TakeTroops(ctx, m.UID, m.Troops, time.Now()); err != nil {
		return March{}, err
	}
	if err := s.timers.Add(ctx, arrive); err != nil {
		if rerr := s.barracks.AddTroops(ctx, m.UID, m.Troops, time.Now()); rerr != nil {
			return March{}, errors.Join(err, rerr)
		}
		return March{}, err
	}
	s.marches[m.ID], s.requests[key] = m, m.ID
	return m, nil
}

// Recall implements Store.
func (s *MemoryStore) Recall(ctx context.Context, m March, arrive timer.Job) (March, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := fmt.Sprint(m.UID, ":", m.RequestID)
	if id, ok := s.requests[key]; ok {
		return s.marches[id], nil
	}
	at := tileKey{m.Kingdom, m.FromX, m.FromY}
	t := s.tiles[at]
	if t.Owner != m.UID || t.Troops < m.Troops {
		return March{}, city.ErrNoTroops
	}
	if err := s.timers.Add(ctx, arrive); err != nil {
		return March{}, err
	}
	t.Troops -= m.Troops
	if t.Troops == 0 {
		delete(s.tiles, at)
	} else {
		s.tiles[at] = t
	}
	s.marches[m.ID], s.requests[key] = m, m.ID
	return m, nil
}

// Arrive implements Store.
func (s *MemoryStore) Arrive(ctx context.Context, id uint64) (March, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.marches[id]
	if !ok {
		return March{}, false, fmt.Errorf("slg: unknown march %d", id)
	}
	if s.arrived[id] {
		return m, false, nil
	}
	if m.Home {
		if err := s.barracks.AddTroops(ctx, m.UID, m.Troops, time.Now()); err != nil {
			return March{}, false, err
		}
		s.arrived[id] = true
		return m, true, nil
	}
	key := tileKey{m.Kingdom, m.ToX, m.ToY}
	after, left, jobs, err := settle(m, s.tiles[key])
	if err != nil {
		return March{}, false, err
	}
	for _, j := range jobs {
		if err := s.timers.Add(ctx, j); err != nil {
			return March{}, false, err
		}
	}
	s.arrived[id] = true
	s.tiles[key] = after
	m.Troops = left
	return m, true, nil
}

// Troops implements Store.
func (s *MemoryStore) Troops(_ context.Context, kingdom string, uid int64) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var total int64
	for key, t := range s.tiles {
		if key.kingdom == kingdom && t.Owner == uid {
			total += t.Troops
		}
	}
	return total, nil
}

func sortTiles(out []Tile) {
	sort.Slice(out, func(a, b int) bool {
		if out[a].X != out[b].X {
			return out[a].X < out[b].X
		}
		return out[a].Y < out[b].Y
	})
}

// Owned implements Store.
func (s *MemoryStore) Owned(_ context.Context, kingdom string, uid int64) ([]Tile, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []Tile
	for key, t := range s.tiles {
		if key.kingdom == kingdom && t.Owner == uid {
			out = append(out, Tile{X: key.x, Y: key.y, Owner: t.Owner, Troops: t.Troops})
		}
	}
	sortTiles(out)
	if len(out) > maxOwned {
		out = out[:maxOwned]
	}
	return out, nil
}

// View implements Store.
func (s *MemoryStore) View(_ context.Context, kingdom string, x, y, radius int32) ([]Tile, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []Tile
	for key, t := range s.tiles {
		if key.kingdom == kingdom && abs(key.x-x) <= int64(radius) && abs(key.y-y) <= int64(radius) {
			out = append(out, Tile{X: key.x, Y: key.y, Owner: t.Owner, Troops: t.Troops})
		}
	}
	sortTiles(out)
	return out, nil
}

// Active implements Store.
func (s *MemoryStore) Active(_ context.Context, kingdom string, uid int64) ([]March, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []March
	for id, m := range s.marches {
		if m.Kingdom == kingdom && m.UID == uid && !s.arrived[id] {
			out = append(out, m)
		}
	}
	sort.Slice(out, func(a, b int) bool { return out[a].ID < out[b].ID })
	return out, nil
}

// Config wires the world service.
type Config struct {
	Store    Store
	Timers   *timer.Group
	IDs      *idgen.Generator
	MemberOf func(ctx context.Context, uid int64) (realm.Member, error) // Required.
	Speed    float64                                                    // Map tiles per second. Default 1.
	Now      func() time.Time
}

// World creates kingdom scenes and registers the timer handler. A kingdom's
// timer shard is its cluster slot, so timers run on the node hosting it.
type World struct {
	cfg Config

	mu     sync.RWMutex
	scenes map[string]*Kingdom
}

// New creates a World.
func New(cfg Config) *World {
	if cfg.Speed <= 0 {
		cfg.Speed = 1
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	w := &World{cfg: cfg, scenes: map[string]*Kingdom{}}
	cfg.Timers.Handle(KindMarchArrive, w.onArrive)
	return w
}

// Factory is a gamehost.Factory.
func (w *World) Factory(key string, ctx *gamehost.SceneContext) gamehost.Scene {
	k := &Kingdom{key: key, world: w, ctx: ctx}
	w.mu.Lock()
	w.scenes[key] = k
	w.mu.Unlock()
	return k
}

func (w *World) onArrive(ctx context.Context, j timer.Job) error {
	if len(j.Payload) != 8 {
		return fmt.Errorf("slg: bad payload for job %d", j.ID)
	}
	id := binary.BigEndian.Uint64(j.Payload)
	// Commit first. A crash after this commit re-fires into applied=false, so
	// the state change happens once.
	m, applied, err := w.cfg.Store.Arrive(ctx, id)
	if err != nil {
		return err
	}
	if !applied {
		return nil
	}
	w.mu.RLock()
	k := w.scenes[m.Kingdom]
	w.mu.RUnlock()
	if k == nil {
		return nil // Kingdom not loaded here: the client sees it in the snapshot.
	}
	// The notification is best effort; the committed state is the truth.
	if err := k.ctx.Actor.Tell(func() { k.arrived(m) }); err != nil && !errors.Is(err, actor.ErrStopped) {
		slog.Warn("arrival push dropped", "march", m.ID, "err", err)
	}
	return nil
}

const maxRequestID = 64 // The width of slg_march.request_id.

// Kingdom is one world scene. It deliberately caches no troop counts: arrivals
// commit with no scene loaded, so the store is the only complete record.
type Kingdom struct {
	key   string
	world *World
	ctx   *gamehost.SceneContext
}

func (k *Kingdom) arrived(m March) {
	if p := k.ctx.Player(m.UID); p != nil {
		p.Push(gamev1.MsgId_MSG_ID_MARCH_ARRIVED, &gamev1.MarchArrived{
			MarchId: m.ID, X: m.ToX, Y: m.ToY, Troops: m.Troops, Home: m.Home})
	}
}

// Admit implements gamehost.Gate. Only a player who entered this kingdom over
// HTTP has a base in it to march from.
func (k *Kingdom) Admit(uid int64) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	me, err := k.world.cfg.MemberOf(ctx, uid)
	if err != nil {
		return err
	}
	if me.Kingdom != k.key {
		return fmt.Errorf("slg: uid %d is not in kingdom %s", uid, k.key)
	}
	return nil
}

// OnJoin implements gamehost.Scene.
func (*Kingdom) OnJoin(*gamehost.Player) {}

// OnDisconnect implements gamehost.Scene. In SLG the world keeps running: an
// offline player's marches still arrive.
func (*Kingdom) OnDisconnect(*gamehost.Player) {}

// OnReconnect implements gamehost.Scene.
func (*Kingdom) OnReconnect(*gamehost.Player) {}

// OnExpire implements gamehost.Scene.
func (*Kingdom) OnExpire(*gamehost.Player) {}

// OnRetire implements gamehost.Scene.
func (k *Kingdom) OnRetire() {
	k.world.mu.Lock()
	defer k.world.mu.Unlock()
	if k.world.scenes[k.key] == k {
		delete(k.world.scenes, k.key)
	}
}

func tileMsgs(tiles []Tile) []*gamev1.Tile {
	out := make([]*gamev1.Tile, 0, len(tiles))
	for _, t := range tiles {
		out = append(out, &gamev1.Tile{X: t.X, Y: t.Y, Uid: t.Owner, Troops: t.Troops})
	}
	return out
}

// Snapshot implements gamehost.Scene. It includes armies on the road, or a
// client that reconnects would not know of them until they arrived.
func (k *Kingdom) Snapshot(p *gamehost.Player) (gamev1.MsgId, proto.Message, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	w := k.world
	me, err := w.cfg.MemberOf(ctx, p.UID)
	if err != nil {
		return 0, nil, fmt.Errorf("slg: load base: %w", err)
	}
	troops, err := w.cfg.Store.Troops(ctx, k.key, p.UID)
	if err != nil {
		return 0, nil, fmt.Errorf("slg: load troops: %w", err)
	}
	tiles, err := w.cfg.Store.Owned(ctx, k.key, p.UID)
	if err != nil {
		return 0, nil, fmt.Errorf("slg: load tiles: %w", err)
	}
	active, err := w.cfg.Store.Active(ctx, k.key, p.UID)
	if err != nil {
		return 0, nil, fmt.Errorf("slg: load marches: %w", err)
	}
	msg := &gamev1.WorldSnapshot{Kingdom: k.key, BaseX: me.X, BaseY: me.Y, MapSize: realm.MapSize,
		Troops: troops, Tiles: tileMsgs(tiles)}
	for _, m := range active {
		msg.Marches = append(msg.Marches, &gamev1.MarchInfo{MarchId: m.ID, ToX: m.ToX, ToY: m.ToY,
			Troops: m.Troops, ArriveAtMs: m.ArriveAt.UnixMilli(), Home: m.Home})
	}
	return gamev1.MsgId_MSG_ID_WORLD_SNAPSHOT, msg, nil
}

// OnMessage implements gamehost.Scene.
func (k *Kingdom) OnMessage(p *gamehost.Player, env *gamev1.Envelope) gamehost.Reply {
	switch env.GetMsgId() {
	case uint32(gamev1.MsgId_MSG_ID_MARCH_REQ):
		return k.march(p, env)
	case uint32(gamev1.MsgId_MSG_ID_RECALL_REQ):
		return k.recall(p, env)
	case uint32(gamev1.MsgId_MSG_ID_MAP_VIEW_REQ):
		return k.view(env)
	}
	return gamehost.Reply{Code: gamev1.ErrorCode_ERROR_CODE_BAD_REQUEST}
}

func onMap(x, y int32) bool { return x >= 0 && x < realm.MapSize && y >= 0 && y < realm.MapSize }

func (k *Kingdom) view(env *gamev1.Envelope) gamehost.Reply {
	req := &gamev1.MapViewReq{}
	if proto.Unmarshal(env.GetPayload(), req) != nil || !onMap(req.GetX(), req.GetY()) {
		return gamehost.Reply{Code: gamev1.ErrorCode_ERROR_CODE_BAD_REQUEST}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	tiles, err := k.world.cfg.Store.View(ctx, k.key, req.GetX(), req.GetY(), viewRadius)
	if err != nil {
		slog.Error("map view", "err", err)
		return gamehost.Reply{Code: gamev1.ErrorCode_ERROR_CODE_INTERNAL}
	}
	return gamehost.Reply{MsgID: gamev1.MsgId_MSG_ID_MAP_VIEW_RSP, Msg: &gamev1.MapViewRsp{Tiles: tileMsgs(tiles)}}
}

func (k *Kingdom) march(p *gamehost.Player, env *gamev1.Envelope) gamehost.Reply {
	req := &gamev1.MarchReq{}
	if proto.Unmarshal(env.GetPayload(), req) != nil || req.GetTroops() <= 0 || !onMap(req.GetToX(), req.GetToY()) {
		return gamehost.Reply{Code: gamev1.ErrorCode_ERROR_CODE_BAD_REQUEST}
	}
	return k.send(p, env, req.GetTroops(), func(me realm.Member, m *March) {
		m.FromX, m.FromY, m.ToX, m.ToY = me.X, me.Y, req.GetToX(), req.GetToY()
	}, k.world.cfg.Store.Create)
}

func (k *Kingdom) recall(p *gamehost.Player, env *gamev1.Envelope) gamehost.Reply {
	req := &gamev1.RecallReq{}
	if proto.Unmarshal(env.GetPayload(), req) != nil || req.GetTroops() <= 0 || !onMap(req.GetX(), req.GetY()) {
		return gamehost.Reply{Code: gamev1.ErrorCode_ERROR_CODE_BAD_REQUEST}
	}
	return k.send(p, env, req.GetTroops(), func(me realm.Member, m *March) {
		m.FromX, m.FromY, m.ToX, m.ToY, m.Home = req.GetX(), req.GetY(), me.X, me.Y, true
	}, k.world.cfg.Store.Recall)
}

// send starts an army. A retried request finds the first march, so its troops
// are taken once.
func (k *Kingdom) send(p *gamehost.Player, env *gamev1.Envelope, troops int64, route func(realm.Member, *March),
	store func(context.Context, March, timer.Job) (March, error)) gamehost.Reply {
	if env.GetRequestId() == "" || len(env.GetRequestId()) > maxRequestID {
		return gamehost.Reply{Code: gamev1.ErrorCode_ERROR_CODE_BAD_REQUEST}
	}
	// These block the actor on storage, bounded by the timeout. The rate of
	// marches is low; a busy kingdom should batch these writes.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	w := k.world
	me, err := w.cfg.MemberOf(ctx, p.UID)
	if err != nil {
		slog.Error("load base", "uid", p.UID, "err", err)
		return gamehost.Reply{Code: gamev1.ErrorCode_ERROR_CODE_INTERNAL}
	}
	m := March{ID: uint64(w.cfg.IDs.Next()), Kingdom: k.key, UID: p.UID, RequestID: env.GetRequestId(), Troops: troops}
	route(me, &m)
	tiles := abs(m.ToX-m.FromX) + abs(m.ToY-m.FromY)
	m.ArriveAt = w.cfg.Now().Add(time.Duration(float64(tiles) / w.cfg.Speed * float64(time.Second)))
	job := timer.Job{ID: m.ID, Shard: discovery.Slot(k.key), Kind: KindMarchArrive,
		Payload: binary.BigEndian.AppendUint64(nil, m.ID), FireAt: m.ArriveAt}
	m, err = store(ctx, m, job)
	if errors.Is(err, city.ErrNoTroops) {
		return gamehost.Reply{Code: gamev1.ErrorCode_ERROR_CODE_NOT_ENOUGH_TROOPS}
	}
	if err != nil {
		slog.Error("create march", "err", err)
		return gamehost.Reply{Code: gamev1.ErrorCode_ERROR_CODE_INTERNAL}
	}
	w.cfg.Timers.Wake(job.Shard)
	return gamehost.Reply{MsgID: gamev1.MsgId_MSG_ID_MARCH_RSP,
		Msg: &gamev1.MarchRsp{MarchId: m.ID, ArriveAtMs: m.ArriveAt.UnixMilli()}}
}

func abs(v int32) int64 {
	if v < 0 {
		return int64(-int64(v))
	}
	return int64(v)
}
