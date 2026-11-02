// Package alliance runs player alliances. One scene per alliance, keyed by its
// name, so a player can look at one before joining it. Every operation is safe
// to repeat: joining twice or leaving twice ends in the same state.
package alliance

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	gamev1 "github.com/drawmoon2025/gameserver/gen/game/v1"
	"github.com/drawmoon2025/gameserver/internal/chat"
	"github.com/drawmoon2025/gameserver/pkg/gamehost"
	"github.com/drawmoon2025/gameserver/pkg/timer"
)

// MaxMembers is the size limit of an alliance.
const MaxMembers = 50

const maxName = 32 // The width of alliance.name.

// Rule errors.
var (
	ErrNotFound  = errors.New("alliance: no such alliance")
	ErrTaken     = errors.New("alliance: name is taken")
	ErrAlreadyIn = errors.New("alliance: already in an alliance")
	ErrFull      = errors.New("alliance: full")
	ErrNotLeader = errors.New("alliance: leader only")
)

// Info is an alliance's public state. Leader 0 means it does not exist.
type Info struct {
	Name    string
	Leader  int64
	Members []int64 // By join order.
}

// Store persists alliances. A player is in at most one.
type Store interface {
	Create(ctx context.Context, name string, uid int64) error
	Join(ctx context.Context, name string, uid int64) error
	// Leave removes uid, passing leadership to the longest-serving member, or
	// ending the alliance if nobody is left. It commits with a chat revoke.
	Leave(ctx context.Context, name string, uid int64) error
	// Kick is Leave at the leader's demand.
	Kick(ctx context.Context, name string, leader, target int64) error
	Info(ctx context.Context, name string) (Info, error)
	// AllianceOf names uid's alliance, or "" if none.
	AllianceOf(ctx context.Context, uid int64) (string, error)
}

func revokeJob(name string, uid int64) (timer.Job, error) {
	return chat.RevokeJob(chat.AllianceChannel(name), uid, time.Now())
}

// Memory is an in-process Store.
type Memory struct {
	timers timer.Store
	mu     sync.Mutex
	all    map[string]*Info
	of     map[int64]string
}

// NewMemory creates an empty store that writes jobs to timers.
func NewMemory(timers timer.Store) *Memory {
	return &Memory{timers: timers, all: map[string]*Info{}, of: map[int64]string{}}
}

// Create implements Store.
func (m *Memory) Create(_ context.Context, name string, uid int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	switch mine := m.of[uid]; {
	case mine == name && mine != "":
		return nil
	case mine != "":
		return ErrAlreadyIn
	}
	if _, ok := m.all[name]; ok {
		return ErrTaken
	}
	m.all[name] = &Info{Name: name, Leader: uid, Members: []int64{uid}}
	m.of[uid] = name
	return nil
}

// Join implements Store.
func (m *Memory) Join(_ context.Context, name string, uid int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	a, ok := m.all[name]
	if !ok {
		return ErrNotFound
	}
	switch mine := m.of[uid]; {
	case mine == name:
		return nil
	case mine != "":
		return ErrAlreadyIn
	}
	if len(a.Members) >= MaxMembers {
		return ErrFull
	}
	a.Members = append(a.Members, uid)
	m.of[uid] = name
	return nil
}

func (m *Memory) remove(ctx context.Context, name string, uid int64) error {
	a, ok := m.all[name]
	if !ok || m.of[uid] != name {
		return nil
	}
	j, err := revokeJob(name, uid)
	if err != nil {
		return err
	}
	if err := m.timers.Add(ctx, j); err != nil {
		return err
	}
	delete(m.of, uid)
	kept := a.Members[:0]
	for _, id := range a.Members {
		if id != uid {
			kept = append(kept, id)
		}
	}
	a.Members = kept
	switch {
	case len(kept) == 0:
		delete(m.all, name)
	case a.Leader == uid:
		a.Leader = kept[0]
	}
	return nil
}

// Leave implements Store.
func (m *Memory) Leave(ctx context.Context, name string, uid int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.remove(ctx, name, uid)
}

// Kick implements Store.
func (m *Memory) Kick(ctx context.Context, name string, leader, target int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	a, ok := m.all[name]
	if !ok {
		return ErrNotFound
	}
	if a.Leader != leader || target == leader {
		return ErrNotLeader
	}
	return m.remove(ctx, name, target)
}

// Info implements Store.
func (m *Memory) Info(_ context.Context, name string) (Info, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	a, ok := m.all[name]
	if !ok {
		return Info{Name: name}, nil
	}
	c := *a
	c.Members = append([]int64(nil), a.Members...)
	return c, nil
}

// AllianceOf implements Store.
func (m *Memory) AllianceOf(_ context.Context, uid int64) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.of[uid], nil
}

// Service creates alliance scenes.
type Service struct {
	Store Store
}

// Factory is a gamehost.Factory.
func (s *Service) Factory(key string, ctx *gamehost.SceneContext) gamehost.Scene {
	return &Scene{key: key, svc: s, ctx: ctx}
}

// Scene is one alliance. It holds no state of its own, because members change
// it through whichever node hosts it and the store is the only record.
type Scene struct {
	key string
	svc *Service
	ctx *gamehost.SceneContext
}

// Admit implements gamehost.Gate.
func (a *Scene) Admit(int64) error {
	if a.key == "" || len(a.key) > maxName {
		return fmt.Errorf("alliance: bad name %q", a.key)
	}
	return nil
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
func (*Scene) OnRetire() {}

func (a *Scene) view(ctx context.Context) (*gamev1.AllianceInfo, error) {
	i, err := a.svc.Store.Info(ctx, a.key)
	if err != nil {
		return nil, err
	}

	return &gamev1.AllianceInfo{Name: i.Name, Leader: i.Leader, Members: i.Members}, nil
}

// Snapshot implements gamehost.Scene.
func (a *Scene) Snapshot(*gamehost.Player) (gamev1.MsgId, proto.Message, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	msg, err := a.view(ctx)
	return gamev1.MsgId_MSG_ID_ALLIANCE_SNAPSHOT, msg, err
}

// OnMessage implements gamehost.Scene.
func (a *Scene) OnMessage(p *gamehost.Player, env *gamev1.Envelope) gamehost.Reply {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	s := a.svc.Store
	var err error
	switch env.GetMsgId() {
	case uint32(gamev1.MsgId_MSG_ID_ALLIANCE_CREATE_REQ):
		err = s.Create(ctx, a.key, p.UID)
	case uint32(gamev1.MsgId_MSG_ID_ALLIANCE_JOIN_REQ):
		err = s.Join(ctx, a.key, p.UID)
	case uint32(gamev1.MsgId_MSG_ID_ALLIANCE_LEAVE_REQ):
		err = s.Leave(ctx, a.key, p.UID)
	case uint32(gamev1.MsgId_MSG_ID_ALLIANCE_KICK_REQ):
		req := &gamev1.AllianceKickReq{}
		if proto.Unmarshal(env.GetPayload(), req) != nil {
			return gamehost.Reply{Code: gamev1.ErrorCode_ERROR_CODE_BAD_REQUEST}
		}
		err = s.Kick(ctx, a.key, p.UID, req.GetUid())
	default:
		return gamehost.Reply{Code: gamev1.ErrorCode_ERROR_CODE_BAD_REQUEST}
	}
	switch {
	case err == nil:
	case errors.Is(err, ErrNotFound), errors.Is(err, ErrNotLeader):
		return gamehost.Reply{Code: gamev1.ErrorCode_ERROR_CODE_BAD_REQUEST}
	case errors.Is(err, ErrTaken), errors.Is(err, ErrAlreadyIn), errors.Is(err, ErrFull):
		return gamehost.Reply{Code: gamev1.ErrorCode_ERROR_CODE_DUPLICATE_IN_PROGRESS}
	default:
		slog.Error("alliance", "name", a.key, "uid", p.UID, "err", err)
		return gamehost.Reply{Code: gamev1.ErrorCode_ERROR_CODE_INTERNAL}
	}
	msg, err := a.view(ctx)
	if err != nil {
		slog.Error("alliance view", "name", a.key, "err", err)
		return gamehost.Reply{Code: gamev1.ErrorCode_ERROR_CODE_INTERNAL}
	}
	for uid, other := range a.ctx.Players() {
		if uid != p.UID {
			other.Push(gamev1.MsgId_MSG_ID_ALLIANCE_SNAPSHOT, msg)
		}
	}
	return gamehost.Reply{MsgID: gamev1.MsgId_MSG_ID_ALLIANCE_RSP, Msg: msg}
}
