// Package gamehost runs stateful scenes (rooms, SLG kingdoms) on a game node.
// It owns the player-session rules every scene shares: epochs, replay, dedupe,
// and the disconnect grace period. Scenes contain game logic only.
package gamehost

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	gamev1 "github.com/yourname/gameserver/gen/game/v1"
	backendv1 "github.com/yourname/gameserver/gen/backend/v1"
	"github.com/yourname/gameserver/pkg/actor"
	"github.com/yourname/gameserver/pkg/resume"
)

// Reply is a handler's response. Code is ignored when Msg is set by mistake
// alongside a failure; failures carry only a code.
type Reply struct {
	MsgID gamev1.MsgId
	Msg   proto.Message
	Code  gamev1.ErrorCode
}

// Scene is game logic. All methods run on the scene's actor, one at a time.
type Scene interface {
	// OnJoin runs when a player is new to the scene.
	OnJoin(p *Player)
	// OnDisconnect runs when the connection drops; the seat is still held.
	OnDisconnect(p *Player)
	// OnReconnect runs when a player returns within the grace period.
	OnReconnect(p *Player)
	// OnExpire runs when the grace period ends with no reconnect.
	OnExpire(p *Player)
	// OnMessage handles one client request.
	OnMessage(p *Player, env *gamev1.Envelope) Reply
	// Snapshot builds full state, sent when replay cannot cover a gap. On
	// error the connection closes and the client retries: a client must never
	// be given a partial snapshot as if it were the truth.
	Snapshot(p *Player) (gamev1.MsgId, proto.Message, error)
}

// Factory creates a scene. The context gives it its actor, for timers.
type Factory func(key string, ctx *SceneContext) Scene

// SceneContext is what a scene may use from its host.
type SceneContext struct {
	Actor *actor.Actor
	scene *sceneRun
}

// Player returns a joined player, for pushes from timers. Actor-only.
func (c *SceneContext) Player(uid int64) *Player { return c.scene.players[uid] }

// Players returns all joined players. Actor-only.
func (c *SceneContext) Players() map[int64]*Player { return c.scene.players }

// Config tunes a Host.
type Config struct {
	Factory      Factory
	Grace        time.Duration // How long a seat is held. Default 45s.
	ReplayFrames int           // Default 256.
	MailboxSize  int           // Default 4096.
}

// Host implements the GameBackend gRPC service.
type Host struct {
	backendv1.UnimplementedGameBackendServer

	cfg Config

	mu     sync.Mutex
	scenes map[string]*sceneRun
}

// New creates a Host.
func New(cfg Config) *Host {
	if cfg.Grace <= 0 {
		cfg.Grace = 45 * time.Second
	}
	if cfg.MailboxSize <= 0 {
		cfg.MailboxSize = 4096
	}
	return &Host{cfg: cfg, scenes: map[string]*sceneRun{}}
}

type sceneRun struct {
	key     string
	actor   *actor.Actor
	scene   Scene
	players map[int64]*Player // Actor-only.
	host    *Host
}

// Scene returns the running scene for key, starting it if needed.
func (h *Host) sceneFor(key string) *sceneRun {
	h.mu.Lock()
	defer h.mu.Unlock()
	if s, ok := h.scenes[key]; ok {
		return s
	}
	s := &sceneRun{key: key, players: map[int64]*Player{}, host: h,
		actor: actor.Spawn("scene:"+key, h.cfg.MailboxSize)}
	s.scene = h.cfg.Factory(key, &SceneContext{Actor: s.actor, scene: s})
	h.scenes[key] = s
	return s
}

// Stop stops every scene.
func (h *Host) Stop() {
	h.mu.Lock()
	scenes := h.scenes
	h.scenes = map[string]*sceneRun{}
	h.mu.Unlock()
	for _, s := range scenes {
		s.actor.Stop()
	}
}

// Player is one player's seat in a scene. Actor-only.
type Player struct {
	UID int64

	sess    *resume.Session
	epoch   uint64
	channel uint32
	conn    uint64  // The socket on tun that currently owns this seat.
	tun     *tunnel // Nil while disconnected.
	offGen  uint64  // Invalidates an older grace timer.
}

// Online reports whether a connection is attached.
func (p *Player) Online() bool { return p.tun != nil }

// Push sends a replayable message. It is retained even while the player is
// offline, so a reconnect within the window receives it.
func (p *Player) Push(id gamev1.MsgId, msg proto.Message) {
	payload, err := proto.Marshal(msg)
	if err != nil {
		slog.Error("marshal push", "msg", id, "err", err)
		return
	}
	_, frame := p.sess.Push(p.stamp(&gamev1.Envelope{MsgId: uint32(id), Payload: payload}))
	p.send(frame)
}

func (p *Player) stamp(env *gamev1.Envelope) resume.Stamp {
	return func(seq uint64) []byte {
		env.Seq, env.Channel = seq, p.channel
		b, err := proto.Marshal(env)
		if err != nil {
			panic(fmt.Sprintf("marshal envelope: %v", err)) // Only possible with invalid UTF-8.
		}
		return b
	}
}

func (p *Player) send(frame []byte) {
	if p.tun != nil {
		p.tun.send(&backendv1.Downstream{Uid: p.UID, Conn: p.conn,
			Body: &backendv1.Downstream_Frame{Frame: frame}})
	}
}

// resend sends a retained frame on the current channel. A frame keeps the
// channel of the connection it was stamped for, and a reconnecting client may
// list its bindings in a different order.
func (p *Player) resend(frame []byte) {
	env := &gamev1.Envelope{}
	if err := proto.Unmarshal(frame, env); err != nil {
		slog.Error("unmarshal retained frame", "uid", p.UID, "err", err)
		return
	}
	if env.GetChannel() == p.channel {
		p.send(frame)
		return
	}
	env.Channel = p.channel
	b, err := proto.Marshal(env)
	if err != nil {
		slog.Error("marshal retained frame", "uid", p.UID, "err", err)
		return
	}
	p.send(b)
}

// Tunnel implements GameBackend: one stream per gateway.
func (h *Host) Tunnel(stream grpc.BidiStreamingServer[backendv1.Upstream, backendv1.Downstream]) error {
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()
	t := &tunnel{out: make(chan *backendv1.Downstream, 8192), cancel: cancel}
	defer h.tunnelGone(t)

	done := make(chan error, 1)
	go func() {
		for {
			select {
			case <-ctx.Done():
				done <- ctx.Err()
				return
			case m := <-t.out:
				if err := stream.Send(m); err != nil {
					done <- err
					cancel()
					return
				}
			}
		}
	}()

	for {
		m, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		h.dispatch(t, m)
	}
}

// tunnel is the per-gateway send side. Sends never block an actor.
type tunnel struct {
	out    chan *backendv1.Downstream
	cancel context.CancelFunc

	mu     sync.Mutex
	scenes map[*sceneRun]map[int64]struct{}
}

func (t *tunnel) send(m *backendv1.Downstream) {
	select {
	case t.out <- m:
	default:
		// The gateway cannot keep up. Dropping the tunnel is safe: frames are
		// retained, and its players reconnect and replay.
		slog.Warn("tunnel backpressure; closing")
		t.cancel()
	}
}

func (t *tunnel) track(s *sceneRun, uid int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.scenes == nil {
		t.scenes = map[*sceneRun]map[int64]struct{}{}
	}
	if t.scenes[s] == nil {
		t.scenes[s] = map[int64]struct{}{}
	}
	t.scenes[s][uid] = struct{}{}
}

// tunnelGone detaches everyone on a dead tunnel, starting their grace period.
func (h *Host) tunnelGone(t *tunnel) {
	t.mu.Lock()
	scenes := t.scenes
	t.scenes = nil
	t.mu.Unlock()
	for s, uids := range scenes {
		s := s
		uids := uids
		if err := s.actor.Tell(func() {
			for uid := range uids {
				if p := s.players[uid]; p != nil && p.tun == t {
					s.disconnect(p)
				}
			}
		}); err != nil {
			slog.Warn("tunnel cleanup dropped", "scene", s.key, "err", err)
		}
	}
}

func (h *Host) dispatch(t *tunnel, m *backendv1.Upstream) {
	s := h.sceneFor(m.GetKey())
	uid, conn := m.GetUid(), m.GetConn()
	var err error
	switch b := m.GetBody().(type) {
	case *backendv1.Upstream_Attach:
		t.track(s, uid)
		err = s.actor.Tell(func() { s.attach(t, uid, conn, b.Attach) })
	case *backendv1.Upstream_Frame:
		err = s.actor.Tell(func() { s.frame(t, uid, conn, b.Frame) })
	case *backendv1.Upstream_Detach:
		err = s.actor.Tell(func() {
			// A replaced socket may detach after its successor has attached.
			if p := s.players[uid]; p != nil && p.tun == t && p.conn == conn {
				s.disconnect(p)
			}
		})
	}
	if err != nil {
		// Overloaded scene: reject rather than queue without bound.
		t.send(&backendv1.Downstream{Uid: uid, Conn: conn, Body: &backendv1.Downstream_CloseReason{CloseReason: "scene busy"}})
	}
}

func (s *sceneRun) attach(t *tunnel, uid int64, conn uint64, a *backendv1.Attach) {
	p, known := s.players[uid]
	if known && a.GetEpoch() < p.epoch {
		t.send(&backendv1.Downstream{Uid: uid, Conn: conn,
			Body: &backendv1.Downstream_CloseReason{CloseReason: "stale login"}})
		return
	}
	if !known {
		p = &Player{UID: uid, sess: resume.New(s.host.cfg.ReplayFrames)}
		s.players[uid] = p
	}
	wasOffline := !p.Online()
	if p.tun != nil {
		// A seat has one socket: the newest. It may be a newer login, or the
		// same token reconnecting before the old socket was noticed as dead.
		reason := "connection replaced"
		if p.epoch != a.GetEpoch() {
			reason = "logged in elsewhere"
		}
		p.tun.send(&backendv1.Downstream{Uid: uid, Conn: p.conn,
			Body: &backendv1.Downstream_CloseReason{CloseReason: reason}})
	}
	p.tun, p.conn, p.epoch, p.channel = t, conn, a.GetEpoch(), a.GetChannel()
	p.offGen++ // Cancels any pending grace expiry.

	var (
		frames   [][]byte
		replayed bool
	)
	if known && a.GetResume() {
		frames, replayed = p.sess.Replay(a.GetLastSeq())
	}
	p.direct(gamev1.MsgId_MSG_ID_RESUME_RSP, &gamev1.ResumeRsp{Replayed: replayed, NextSeq: p.sess.NextSeq()})
	switch {
	case replayed:
		for _, f := range frames {
			p.resend(f)
		}
	default:
		if !known {
			s.scene.OnJoin(p)
		}
		id, snap, err := s.scene.Snapshot(p)
		if err != nil {
			slog.Error("snapshot", "scene", s.key, "uid", uid, "err", err)
			t.send(&backendv1.Downstream{Uid: uid, Conn: conn,
				Body: &backendv1.Downstream_CloseReason{CloseReason: "snapshot unavailable"}})
			return
		}
		p.Push(id, snap)
	}
	if known && wasOffline {
		s.scene.OnReconnect(p)
	}
}

// direct sends a connection-control frame outside the replay stream.
func (p *Player) direct(id gamev1.MsgId, msg proto.Message) {
	payload, err := proto.Marshal(msg)
	if err != nil {
		slog.Error("marshal direct", "msg", id, "err", err)
		return
	}
	b, err := proto.Marshal(&gamev1.Envelope{MsgId: uint32(id), Payload: payload, Channel: p.channel})
	if err != nil {
		slog.Error("marshal envelope", "msg", id, "err", err)
		return
	}
	p.send(b)
}

func (s *sceneRun) frame(t *tunnel, uid int64, conn uint64, raw []byte) {
	p := s.players[uid]
	if p == nil || p.tun != t || p.conn != conn {
		return // Racing a kick or a tunnel change.
	}
	env := &gamev1.Envelope{}
	if err := proto.Unmarshal(raw, env); err != nil {
		p.reply(env, Reply{Code: gamev1.ErrorCode_ERROR_CODE_BAD_REQUEST}, 0)
		return
	}
	verdict, last := p.sess.CheckClientSeq(env.GetClientSeq())
	switch verdict {
	case resume.DropStale:
		return
	case resume.ResendLast:
		if last != nil {
			p.resend(last)
		}
		return
	}
	r := s.scene.OnMessage(p, env)
	p.reply(env, r, env.GetClientSeq())
}

func (p *Player) reply(req *gamev1.Envelope, r Reply, clientSeq uint64) {
	env := &gamev1.Envelope{MsgId: uint32(r.MsgID), ClientSeq: req.GetClientSeq(),
		RequestId: req.GetRequestId(), Code: int32(r.Code)}
	if r.Msg != nil {
		b, err := proto.Marshal(r.Msg)
		if err != nil {
			slog.Error("marshal reply", "msg", r.MsgID, "err", err)
			env.Code = int32(gamev1.ErrorCode_ERROR_CODE_INTERNAL)
		}
		env.Payload = b
	}
	_, frame := p.sess.Push(p.stamp(env))
	p.sess.Commit(clientSeq, frame)
	p.send(frame)
}

func (s *sceneRun) disconnect(p *Player) {
	p.tun = nil
	p.offGen++
	gen := p.offGen
	s.scene.OnDisconnect(p)
	s.actor.After(s.host.cfg.Grace, func() {
		if p.offGen == gen && !p.Online() && s.players[p.UID] == p {
			s.scene.OnExpire(p)
			delete(s.players, p.UID)
		}
	})
}
