// Package slg is the world service of an SLG: one Scene per kingdom. It shows
// how long-running actions work. State and a durable timer are written first,
// the client is acknowledged, and arrival is applied exactly once by a
// conditional state change, even though the timer fires at-least-once.
//
// The other SLG services (city, alliance, battle, mail, chat, rank) are
// separate deployments that follow the same pattern. See docs/ARCHITECTURE.md.
package slg

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	gamev1 "github.com/yourname/gameserver/gen/game/v1"
	"github.com/yourname/gameserver/internal/gamehost"
	"github.com/yourname/gameserver/pkg/actor"
	"github.com/yourname/gameserver/pkg/discovery"
	"github.com/yourname/gameserver/pkg/idgen"
	"github.com/yourname/gameserver/pkg/timer"
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
}

// Store persists marches.
type Store interface {
	// Create stores a march in the marching state together with the timer that
	// ends it. They commit as one: a march without a timer would never arrive.
	// If (UID, RequestID) exists it stores nothing and returns that march, since
	// client_seq dedupe does not outlive the seat but a march does.
	Create(ctx context.Context, m March, arrive timer.Job) (March, error)
	// Arrive moves a marching march to arrived. It returns false if it was
	// already arrived, which is what makes a re-fired timer harmless.
	Arrive(ctx context.Context, id uint64) (March, bool, error)
	// Troops sums a player's arrived troops in a kingdom.
	Troops(ctx context.Context, kingdom string, uid int64) (int64, error)
}

// MemoryStore is an in-process Store.
type MemoryStore struct {
	timers   timer.Store
	mu       sync.Mutex
	marches  map[uint64]March
	arrived  map[uint64]bool
	requests map[string]uint64
}

// NewMemoryStore creates an empty store that writes jobs to timers.
func NewMemoryStore(timers timer.Store) *MemoryStore {
	return &MemoryStore{timers: timers, marches: map[uint64]March{}, arrived: map[uint64]bool{}, requests: map[string]uint64{}}
}

// Create implements Store.
func (s *MemoryStore) Create(ctx context.Context, m March, arrive timer.Job) (March, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := fmt.Sprint(m.UID, ":", m.RequestID)
	if id, ok := s.requests[key]; ok {
		return s.marches[id], nil
	}
	if err := s.timers.Add(ctx, arrive); err != nil {
		return March{}, err
	}
	s.marches[m.ID], s.requests[key] = m, m.ID
	return m, nil
}

// Arrive implements Store.
func (s *MemoryStore) Arrive(_ context.Context, id uint64) (March, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.marches[id]
	if !ok {
		return March{}, false, fmt.Errorf("slg: unknown march %d", id)
	}
	if s.arrived[id] {
		return m, false, nil
	}
	s.arrived[id] = true
	return m, true, nil
}

// Troops implements Store.
func (s *MemoryStore) Troops(_ context.Context, kingdom string, uid int64) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var total int64
	for id, m := range s.marches {
		if s.arrived[id] && m.Kingdom == kingdom && m.UID == uid {
			total += m.Troops
		}
	}
	return total, nil
}

// Config wires the world service.
type Config struct {
	Store  Store
	Timers *timer.Group
	IDs    *idgen.Generator
	Speed  float64 // Map tiles per second. Default 1.
	Now    func() time.Time
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
			MarchId: m.ID, X: m.ToX, Y: m.ToY, Troops: m.Troops})
	}
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

// Snapshot implements gamehost.Scene.
func (k *Kingdom) Snapshot(p *gamehost.Player) (gamev1.MsgId, proto.Message, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	troops, err := k.world.cfg.Store.Troops(ctx, k.key, p.UID)
	if err != nil {
		return 0, nil, fmt.Errorf("slg: load troops: %w", err)
	}
	return gamev1.MsgId_MSG_ID_ROOM_SNAPSHOT, &gamev1.RoomSnapshot{RoomId: k.key, Value: troops}, nil
}

// OnMessage implements gamehost.Scene.
func (k *Kingdom) OnMessage(p *gamehost.Player, env *gamev1.Envelope) gamehost.Reply {
	if env.GetMsgId() != uint32(gamev1.MsgId_MSG_ID_MARCH_REQ) {
		return gamehost.Reply{Code: gamev1.ErrorCode_ERROR_CODE_BAD_REQUEST}
	}
	req := &gamev1.MarchReq{}
	if proto.Unmarshal(env.GetPayload(), req) != nil || req.GetTroops() <= 0 ||
		env.GetRequestId() == "" || len(env.GetRequestId()) > maxRequestID {
		return gamehost.Reply{Code: gamev1.ErrorCode_ERROR_CODE_BAD_REQUEST}
	}
	// Distance would come from the city position; the origin stands in here.
	w := k.world
	tiles := abs(req.GetToX()) + abs(req.GetToY())
	travel := time.Duration(float64(tiles) / w.cfg.Speed * float64(time.Second))
	m := March{ID: uint64(w.cfg.IDs.Next()), Kingdom: k.key, UID: p.UID, RequestID: env.GetRequestId(),
		ToX: req.GetToX(), ToY: req.GetToY(), Troops: req.GetTroops(),
		ArriveAt: w.cfg.Now().Add(travel)}

	// This blocks the actor on storage, bounded by the timeout. The rate of
	// marches is low; a busy kingdom should batch these writes.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	job := timer.Job{ID: m.ID, Shard: discovery.Slot(k.key), Kind: KindMarchArrive,
		Payload: binary.BigEndian.AppendUint64(nil, m.ID), FireAt: m.ArriveAt}
	m, err := w.cfg.Store.Create(ctx, m, job)
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
