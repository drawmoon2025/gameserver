// Package e2e runs the real gateway, gRPC tunnel and game host in one process.
package e2e

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"

	gamev1 "github.com/yourname/gameserver/gen/game/v1"
	backendv1 "github.com/yourname/gameserver/gen/backend/v1"
	"github.com/yourname/gameserver/internal/gamehost"
	"github.com/yourname/gameserver/internal/gateway"
	"github.com/yourname/gameserver/internal/room"
	"github.com/yourname/gameserver/internal/slg"
	"github.com/yourname/gameserver/pkg/auth"
	"github.com/yourname/gameserver/pkg/discovery"
	"github.com/yourname/gameserver/pkg/idgen"
	"github.com/yourname/gameserver/pkg/session"
	"github.com/yourname/gameserver/pkg/timer"
)

type stack struct {
	url    string
	signer *auth.Signer
	epochs *session.Memory
}

func startBackend(t *testing.T, f gamehost.Factory, grace time.Duration) string {
	t.Helper()
	addr, _ := startHost(t, gamehost.Config{Factory: f, Grace: grace, ReplayFrames: 8})
	return addr
}

func startHost(t *testing.T, cfg gamehost.Config) (string, *gamehost.Host) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	host := gamehost.New(cfg)
	srv := grpc.NewServer()
	backendv1.RegisterGameBackendServer(srv, host)
	go srv.Serve(lis) //nolint:errcheck // Stops with the test.
	t.Cleanup(func() { srv.Stop(); host.Stop() })
	return lis.Addr().String(), host
}

func newStack(t *testing.T, nodes map[string][]string) *stack {
	t.Helper()
	return newStackWith(t, nodes, nil)
}

func newStackWith(t *testing.T, nodes map[string][]string, tune func(*gateway.Config)) *stack {
	t.Helper()
	s := &stack{signer: &auth.Signer{Key: []byte("0123456789abcdef0123456789abcdef"), TTL: time.Hour},
		epochs: session.NewMemory()}
	cfg := gateway.Config{Signer: s.signer, Epochs: s.epochs, Resolver: discovery.NewStatic(nodes)}
	if tune != nil {
		tune(&cfg)
	}
	g := gateway.New(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go g.Run(ctx)
	mux := http.NewServeMux()
	mux.Handle("GET /ws", g)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	s.url = "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws"
	return s
}

func (s *stack) login(t *testing.T, uid int64) string {
	t.Helper()
	e, err := s.epochs.Next(context.Background(), uid)
	if err != nil {
		t.Fatal(err)
	}
	tok, _, err := s.signer.Issue(uid, fmt.Sprint("s", e), e)
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

type client struct {
	t  *testing.T
	ws *websocket.Conn
}

func (s *stack) dial(t *testing.T, token string, bindings ...*gamev1.Binding) *client {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ws, _, err := websocket.Dial(ctx, s.url, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ws.CloseNow() }) //nolint:errcheck // Best-effort.
	c := &client{t: t, ws: ws}
	c.send(&gamev1.Envelope{MsgId: uint32(gamev1.MsgId_MSG_ID_CONNECT_REQ)},
		&gamev1.ConnectReq{Token: token, Bindings: bindings})
	return c
}

func (c *client) send(env *gamev1.Envelope, msg proto.Message) {
	c.t.Helper()
	p, err := proto.Marshal(msg)
	if err != nil {
		c.t.Fatal(err)
	}
	env.Payload = p
	b, err := proto.Marshal(env)
	if err != nil {
		c.t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := c.ws.Write(ctx, websocket.MessageBinary, b); err != nil {
		c.t.Fatal(err)
	}
}

func (c *client) tryRead(d time.Duration) (*gamev1.Envelope, error) {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	_, b, err := c.ws.Read(ctx)
	if err != nil {
		return nil, err
	}
	env := &gamev1.Envelope{}
	return env, proto.Unmarshal(b, env)
}

func (c *client) read() *gamev1.Envelope {
	c.t.Helper()
	env, err := c.tryRead(3 * time.Second)
	if err != nil {
		c.t.Fatal(err)
	}
	return env
}

// until reads frames until one has the wanted ID.
func (c *client) until(id gamev1.MsgId) *gamev1.Envelope {
	c.t.Helper()
	for i := 0; i < 32; i++ {
		if env := c.read(); env.GetMsgId() == uint32(id) {
			return env
		}
	}
	c.t.Fatalf("never received %v", id)
	return nil
}

func roomBinding(last uint64) *gamev1.Binding {
	return &gamev1.Binding{Service: "room", Key: "r1", LastSeq: last}
}

func action(c *client, clientSeq uint64, delta int32) {
	c.send(&gamev1.Envelope{MsgId: uint32(gamev1.MsgId_MSG_ID_ROOM_ACTION_REQ), ClientSeq: clientSeq},
		&gamev1.RoomActionReq{Delta: delta})
}

func value(t *testing.T, env *gamev1.Envelope) int64 {
	t.Helper()
	r := &gamev1.RoomActionRsp{}
	if err := proto.Unmarshal(env.GetPayload(), r); err != nil {
		t.Fatal(err)
	}
	return r.GetValue()
}

func TestRejectsBadToken(t *testing.T) {
	s := newStack(t, map[string][]string{"room": {startBackend(t, room.Factory, time.Minute)}})
	c := s.dial(t, "not-a-token", roomBinding(0))
	if _, err := c.tryRead(2 * time.Second); err == nil {
		t.Fatal("a bad token must close the socket")
	}
}

func TestReconnectReplaysMissedFrames(t *testing.T) {
	s := newStack(t, map[string][]string{"room": {startBackend(t, room.Factory, time.Minute)}})
	tokA, tokB := s.login(t, 1), s.login(t, 2)

	a := s.dial(t, tokA, roomBinding(0))
	a.until(gamev1.MsgId_MSG_ID_ROOM_SNAPSHOT)
	b := s.dial(t, tokB, roomBinding(0))
	b.until(gamev1.MsgId_MSG_ID_ROOM_SNAPSHOT)

	action(a, 1, 5)
	got := a.until(gamev1.MsgId_MSG_ID_ROOM_ACTION_RSP)
	lastSeq := got.GetSeq()
	b.until(gamev1.MsgId_MSG_ID_ROOM_ACTION_RSP)

	if err := a.ws.CloseNow(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond) // Let the host see the disconnect.

	// While A is away, B acts. These pushes must reach A later.
	action(b, 1, 2)
	b.until(gamev1.MsgId_MSG_ID_ROOM_ACTION_RSP)
	action(b, 2, 3)
	b.until(gamev1.MsgId_MSG_ID_ROOM_ACTION_RSP)

	a2 := s.dial(t, tokA, roomBinding(lastSeq))
	rsp := &gamev1.ResumeRsp{}
	if err := proto.Unmarshal(a2.until(gamev1.MsgId_MSG_ID_RESUME_RSP).GetPayload(), rsp); err != nil {
		t.Fatal(err)
	}
	if !rsp.GetReplayed() {
		t.Fatal("expected replay, got snapshot")
	}
	f1, f2 := a2.until(gamev1.MsgId_MSG_ID_ROOM_ACTION_RSP), a2.until(gamev1.MsgId_MSG_ID_ROOM_ACTION_RSP)
	if value(t, f1) != 7 || value(t, f2) != 10 {
		t.Fatalf("replayed values %d, %d", value(t, f1), value(t, f2))
	}
	if f1.GetSeq() != lastSeq+1 || f2.GetSeq() != lastSeq+2 {
		t.Fatalf("seqs %d, %d after %d", f1.GetSeq(), f2.GetSeq(), lastSeq)
	}
}

func TestGapBeyondWindowFallsBackToSnapshot(t *testing.T) {
	s := newStack(t, map[string][]string{"room": {startBackend(t, room.Factory, time.Minute)}})
	tokA, tokB := s.login(t, 1), s.login(t, 2)
	a := s.dial(t, tokA, roomBinding(0))
	a.until(gamev1.MsgId_MSG_ID_ROOM_SNAPSHOT)
	b := s.dial(t, tokB, roomBinding(0))
	b.until(gamev1.MsgId_MSG_ID_ROOM_SNAPSHOT)
	action(a, 1, 1)
	first := a.until(gamev1.MsgId_MSG_ID_ROOM_ACTION_RSP)
	if err := a.ws.CloseNow(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)

	for i := uint64(1); i <= 12; i++ { // The replay window is 8 frames.
		action(b, i, 1)
		b.until(gamev1.MsgId_MSG_ID_ROOM_ACTION_RSP)
	}
	a2 := s.dial(t, tokA, roomBinding(first.GetSeq()))
	snap := &gamev1.RoomSnapshot{}
	if err := proto.Unmarshal(a2.until(gamev1.MsgId_MSG_ID_ROOM_SNAPSHOT).GetPayload(), snap); err != nil {
		t.Fatal(err)
	}
	if snap.GetValue() != 13 {
		t.Fatalf("snapshot value %d", snap.GetValue())
	}
}

func TestRetriedRequestIsNotAppliedTwice(t *testing.T) {
	s := newStack(t, map[string][]string{"room": {startBackend(t, room.Factory, time.Minute)}})
	c := s.dial(t, s.login(t, 1), roomBinding(0))
	c.until(gamev1.MsgId_MSG_ID_ROOM_SNAPSHOT)
	action(c, 1, 5)
	r1 := c.until(gamev1.MsgId_MSG_ID_ROOM_ACTION_RSP)
	action(c, 1, 5) // The client never saw the reply and retries.
	r2 := c.until(gamev1.MsgId_MSG_ID_ROOM_ACTION_RSP)
	if value(t, r1) != 5 || value(t, r2) != 5 || r1.GetSeq() != r2.GetSeq() {
		t.Fatalf("r1=%v r2=%v", r1, r2)
	}
}

func TestNewLoginKicksOldConnection(t *testing.T) {
	s := newStack(t, map[string][]string{"room": {startBackend(t, room.Factory, time.Minute)}})
	old := s.dial(t, s.login(t, 1), roomBinding(0))
	old.until(gamev1.MsgId_MSG_ID_ROOM_SNAPSHOT)

	tok2 := s.login(t, 1)
	fresh := s.dial(t, tok2, roomBinding(0))
	fresh.until(gamev1.MsgId_MSG_ID_RESUME_RSP)

	old.until(gamev1.MsgId_MSG_ID_KICK)
	action(fresh, 1, 1)
	fresh.until(gamev1.MsgId_MSG_ID_ROOM_ACTION_RSP) // The new session works.
}

func TestStaleTokenCannotConnect(t *testing.T) {
	s := newStack(t, map[string][]string{"room": {startBackend(t, room.Factory, time.Minute)}})
	stale := s.login(t, 1)
	s.login(t, 1)
	c := s.dial(t, stale, roomBinding(0))
	if _, err := c.tryRead(2 * time.Second); err == nil {
		t.Fatal("a superseded token must be refused")
	}
}

func TestOneConnectionReachesTwoServices(t *testing.T) {
	timers, store, ids := worldDeps(t)
	w := slg.New(slg.Config{Store: store, Timers: timers, IDs: ids, Speed: 1000})
	timers.Start(context.Background(), discovery.Slot("k1"))

	s := newStack(t, map[string][]string{
		"room":  {startBackend(t, room.Factory, time.Minute)},
		"world": {startBackend(t, w.Factory, time.Minute)},
	})
	c := s.dial(t, s.login(t, 1), roomBinding(0), &gamev1.Binding{Service: "world", Key: "k1"})
	c.until(gamev1.MsgId_MSG_ID_ROOM_SNAPSHOT)
	c.until(gamev1.MsgId_MSG_ID_ROOM_SNAPSHOT)

	action(c, 1, 1)
	if env := c.until(gamev1.MsgId_MSG_ID_ROOM_ACTION_RSP); env.GetChannel() != 0 {
		t.Fatalf("room channel %d", env.GetChannel())
	}
	c.send(&gamev1.Envelope{MsgId: uint32(gamev1.MsgId_MSG_ID_MARCH_REQ), ClientSeq: 1, Channel: 1, RequestId: "m1"},
		&gamev1.MarchReq{ToX: 3, ToY: 4, Troops: 100})
	if env := c.until(gamev1.MsgId_MSG_ID_MARCH_RSP); env.GetChannel() != 1 {
		t.Fatalf("world channel %d", env.GetChannel())
	}
	arrived := &gamev1.MarchArrived{}
	if err := proto.Unmarshal(c.until(gamev1.MsgId_MSG_ID_MARCH_ARRIVED).GetPayload(), arrived); err != nil {
		t.Fatal(err)
	}
	if arrived.GetTroops() != 100 || arrived.GetX() != 3 {
		t.Fatalf("arrived %v", arrived)
	}
}

func worldDeps(t *testing.T) (*timer.Group, *slg.MemoryStore, *idgen.Generator) {
	t.Helper()
	ids, err := idgen.New(1)
	if err != nil {
		t.Fatal(err)
	}
	jobs := timer.NewMemStore()
	timers := timer.NewGroup(timer.Config{Owner: "t", Store: jobs, Poll: 10 * time.Millisecond})
	t.Cleanup(timers.StopAll)
	return timers, slg.NewMemoryStore(jobs), ids
}

func TestMarchSurvivesWorldRestart(t *testing.T) {
	store := timer.NewMemStore()
	marches := slg.NewMemoryStore(store)
	slot := discovery.Slot("k1")
	ids, err := idgen.New(1)
	if err != nil {
		t.Fatal(err)
	}
	// First process: accept a march, then die before it arrives.
	first := timer.NewGroup(timer.Config{Owner: "a", Store: store})
	w1 := slg.New(slg.Config{Store: marches, Timers: first, IDs: ids, Speed: 100})
	s := newStack(t, map[string][]string{"world": {startBackend(t, w1.Factory, time.Minute)}})
	c := s.dial(t, s.login(t, 1), &gamev1.Binding{Service: "world", Key: "k1"})
	c.until(gamev1.MsgId_MSG_ID_ROOM_SNAPSHOT)
	c.send(&gamev1.Envelope{MsgId: uint32(gamev1.MsgId_MSG_ID_MARCH_REQ), ClientSeq: 1, RequestId: "m1"},
		&gamev1.MarchReq{ToX: 5, ToY: 5, Troops: 1})
	c.until(gamev1.MsgId_MSG_ID_MARCH_RSP)
	if store.Len(slot) != 1 {
		t.Fatalf("stored jobs %d", store.Len(slot))
	}

	// Second process: picks the job up from storage and completes it.
	second := timer.NewGroup(timer.Config{Owner: "b", Store: store, Poll: 10 * time.Millisecond})
	w2 := slg.New(slg.Config{Store: marches, Timers: second, IDs: ids})
	second.Start(context.Background(), slot)
	defer second.StopAll()
	deadline := time.Now().Add(3 * time.Second)
	for store.Len(slot) != 0 {
		if time.Now().After(deadline) {
			t.Fatal("march never completed after restart")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// The kingdom was not loaded when the march arrived, so only the store
	// knows. A fresh snapshot must still show the troops.
	s2 := newStack(t, map[string][]string{"world": {startBackend(t, w2.Factory, time.Minute)}})
	c2 := s2.dial(t, s2.login(t, 1), &gamev1.Binding{Service: "world", Key: "k1"})
	snap := &gamev1.RoomSnapshot{}
	if err := proto.Unmarshal(c2.until(gamev1.MsgId_MSG_ID_ROOM_SNAPSHOT).GetPayload(), snap); err != nil {
		t.Fatal(err)
	}
	if snap.GetValue() != 1 {
		t.Fatalf("snapshot troops %d, want 1", snap.GetValue())
	}
}

// watchScene reports disconnects, to observe what the backend believes.
type watchScene struct {
	gamehost.Scene
	gone chan int64
}

func (w watchScene) OnDisconnect(p *gamehost.Player) { w.gone <- p.UID }

func TestFailedHandshakeLeavesNoOnlineBinding(t *testing.T) {
	gone := make(chan int64, 1)
	addr := startBackend(t, func(key string, ctx *gamehost.SceneContext) gamehost.Scene {
		return watchScene{Scene: room.Factory(key, ctx), gone: gone}
	}, time.Minute)
	s := newStack(t, map[string][]string{"room": {addr}})
	// The first binding attaches, the second has no node to resolve.
	s.dial(t, s.login(t, 1), roomBinding(0), &gamev1.Binding{Service: "missing", Key: "x"})
	select {
	case uid := <-gone:
		if uid != 1 {
			t.Fatalf("uid %d", uid)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the first binding stayed online after the handshake failed")
	}
}

func TestDuplicateBindingIsRejected(t *testing.T) {
	s := newStack(t, map[string][]string{"room": {startBackend(t, room.Factory, time.Minute)}})
	c := s.dial(t, s.login(t, 1), roomBinding(0), roomBinding(0))
	for {
		if _, err := c.tryRead(2 * time.Second); err != nil {
			return // Closed, as required.
		}
	}
}

func TestChannelSelectsBindingOfSameService(t *testing.T) {
	s := newStack(t, map[string][]string{"room": {startBackend(t, room.Factory, time.Minute)}})
	c := s.dial(t, s.login(t, 1),
		&gamev1.Binding{Service: "room", Key: "a"}, &gamev1.Binding{Service: "room", Key: "b"})
	c.until(gamev1.MsgId_MSG_ID_ROOM_SNAPSHOT)
	c.until(gamev1.MsgId_MSG_ID_ROOM_SNAPSHOT)

	c.send(&gamev1.Envelope{MsgId: uint32(gamev1.MsgId_MSG_ID_ROOM_ACTION_REQ), ClientSeq: 1, Channel: 1},
		&gamev1.RoomActionReq{Delta: 9})
	if env := c.until(gamev1.MsgId_MSG_ID_ROOM_ACTION_RSP); env.GetChannel() != 1 || value(t, env) != 9 {
		t.Fatalf("second room: %v", env)
	}
	c.send(&gamev1.Envelope{MsgId: uint32(gamev1.MsgId_MSG_ID_ROOM_ACTION_REQ), ClientSeq: 1, Channel: 0},
		&gamev1.RoomActionReq{Delta: 1})
	if env := c.until(gamev1.MsgId_MSG_ID_ROOM_ACTION_RSP); env.GetChannel() != 0 || value(t, env) != 1 {
		t.Fatalf("first room must be untouched by the second: %v", env)
	}
}

func TestSocketsOfOneTokenOnlySeeTheirOwnRooms(t *testing.T) {
	s := newStack(t, map[string][]string{"room": {startBackend(t, room.Factory, time.Minute)}})
	tok := s.login(t, 1)
	a := s.dial(t, tok, &gamev1.Binding{Service: "room", Key: "a"})
	a.until(gamev1.MsgId_MSG_ID_ROOM_SNAPSHOT)
	b := s.dial(t, tok, &gamev1.Binding{Service: "room", Key: "b"})
	b.until(gamev1.MsgId_MSG_ID_ROOM_SNAPSHOT)
	action(b, 1, 1)
	b.until(gamev1.MsgId_MSG_ID_ROOM_ACTION_RSP)
	if env, err := a.tryRead(300 * time.Millisecond); err == nil {
		t.Fatalf("socket bound to room a received a frame from room b: %v", env)
	}
}

// swapped binds rooms a and b in the opposite order to the first connection.
func swapped(last uint64) []*gamev1.Binding {
	return []*gamev1.Binding{{Service: "room", Key: "b"}, {Service: "room", Key: "a", LastSeq: last}}
}

func TestReplayUsesCurrentChannel(t *testing.T) {
	s := newStack(t, map[string][]string{"room": {startBackend(t, room.Factory, time.Minute)}})
	tok := s.login(t, 1)
	other := s.dial(t, s.login(t, 2), &gamev1.Binding{Service: "room", Key: "a"})
	other.until(gamev1.MsgId_MSG_ID_ROOM_SNAPSHOT)
	c := s.dial(t, tok, &gamev1.Binding{Service: "room", Key: "a"}, &gamev1.Binding{Service: "room", Key: "b"})
	c.until(gamev1.MsgId_MSG_ID_ROOM_SNAPSHOT)
	c.until(gamev1.MsgId_MSG_ID_ROOM_SNAPSHOT)
	action(c, 1, 1)
	last := c.until(gamev1.MsgId_MSG_ID_ROOM_ACTION_RSP).GetSeq()
	other.until(gamev1.MsgId_MSG_ID_ROOM_ACTION_RSP)
	if err := c.ws.CloseNow(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	action(other, 1, 2) // Stored for player 1 while room a was channel 0.
	other.until(gamev1.MsgId_MSG_ID_ROOM_ACTION_RSP)

	c2 := s.dial(t, tok, swapped(last)...)
	env := c2.until(gamev1.MsgId_MSG_ID_ROOM_ACTION_RSP)
	if env.GetChannel() != 1 || value(t, env) != 3 {
		t.Fatalf("replayed frame: %v", env)
	}
}

func TestRetryAfterReconnectUsesCurrentChannel(t *testing.T) {
	s := newStack(t, map[string][]string{"room": {startBackend(t, room.Factory, time.Minute)}})
	tok := s.login(t, 1)
	c := s.dial(t, tok, &gamev1.Binding{Service: "room", Key: "a"}, &gamev1.Binding{Service: "room", Key: "b"})
	c.until(gamev1.MsgId_MSG_ID_ROOM_SNAPSHOT)
	c.until(gamev1.MsgId_MSG_ID_ROOM_SNAPSHOT)
	action(c, 1, 5)
	first := c.until(gamev1.MsgId_MSG_ID_ROOM_ACTION_RSP)
	if err := c.ws.CloseNow(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)

	c2 := s.dial(t, tok, swapped(first.GetSeq())...)
	c2.until(gamev1.MsgId_MSG_ID_ROOM_SNAPSHOT) // Room b, which is fresh.
	c2.send(&gamev1.Envelope{MsgId: uint32(gamev1.MsgId_MSG_ID_ROOM_ACTION_REQ), ClientSeq: 1, Channel: 1},
		&gamev1.RoomActionReq{Delta: 5})
	env := c2.until(gamev1.MsgId_MSG_ID_ROOM_ACTION_RSP)
	if env.GetChannel() != 1 || env.GetSeq() != first.GetSeq() || value(t, env) != 5 {
		t.Fatalf("resent response: %v", env)
	}
}

func TestReplacedSocketCannotDetachItsSuccessor(t *testing.T) {
	s := newStack(t, map[string][]string{"room": {startBackend(t, room.Factory, time.Minute)}})
	tok := s.login(t, 1)
	old := s.dial(t, tok, roomBinding(0))
	old.until(gamev1.MsgId_MSG_ID_ROOM_SNAPSHOT)
	fresh := s.dial(t, tok, roomBinding(0))
	fresh.until(gamev1.MsgId_MSG_ID_ROOM_SNAPSHOT)
	for {
		if _, err := old.tryRead(2 * time.Second); err != nil {
			break // The host closed the replaced socket.
		}
	}
	time.Sleep(100 * time.Millisecond) // Let its detach reach the host.
	action(fresh, 1, 1)
	fresh.until(gamev1.MsgId_MSG_ID_ROOM_ACTION_RSP)
}

func TestNewLoginClosesSocketsItDidNotReplace(t *testing.T) {
	s := newStack(t, map[string][]string{"room": {startBackend(t, room.Factory, time.Minute)}})
	c := s.dial(t, s.login(t, 1), roomBinding(0))
	c.until(gamev1.MsgId_MSG_ID_ROOM_SNAPSHOT)
	s.login(t, 1) // No new socket, and so no attach, to replace this one.
	c.until(gamev1.MsgId_MSG_ID_KICK)
	closes(t, c)
}

// quiet is Epochs without notifications, as if every one had been missed.
type quiet struct{ session.Epochs }

func TestRecheckClosesSocketWhenNotificationWasMissed(t *testing.T) {
	s := newStackWith(t, map[string][]string{"room": {startBackend(t, room.Factory, time.Minute)}},
		func(c *gateway.Config) {
			c.Epochs = quiet{c.Epochs}
			c.RecheckEvery = 20 * time.Millisecond
		})
	c := s.dial(t, s.login(t, 1), roomBinding(0))
	c.until(gamev1.MsgId_MSG_ID_ROOM_SNAPSHOT)
	s.login(t, 1)
	time.Sleep(100 * time.Millisecond)
	action(c, 1, 1) // The re-check runs as the socket is used.
	c.until(gamev1.MsgId_MSG_ID_KICK)
	closes(t, c)
}

func TestRecheckClosesSocketWhenTokenExpires(t *testing.T) {
	s := newStackWith(t, map[string][]string{"room": {startBackend(t, room.Factory, time.Minute)}},
		func(c *gateway.Config) { c.RecheckEvery = 20 * time.Millisecond })
	s.signer.TTL = 1200 * time.Millisecond
	c := s.dial(t, s.login(t, 1), roomBinding(0))
	c.until(gamev1.MsgId_MSG_ID_ROOM_SNAPSHOT)
	time.Sleep(1500 * time.Millisecond)
	action(c, 1, 1)
	c.until(gamev1.MsgId_MSG_ID_KICK)
	closes(t, c)
}

func TestNewLoginStartsANewDedupeCounter(t *testing.T) {
	s := newStack(t, map[string][]string{"room": {startBackend(t, room.Factory, time.Minute)}})
	a := s.dial(t, s.login(t, 1), roomBinding(0))
	a.until(gamev1.MsgId_MSG_ID_ROOM_SNAPSHOT)
	action(a, 7, 1)
	a.until(gamev1.MsgId_MSG_ID_ROOM_ACTION_RSP)

	b := s.dial(t, s.login(t, 1), roomBinding(0))
	b.until(gamev1.MsgId_MSG_ID_ROOM_SNAPSHOT)
	action(b, 1, 2) // Below A's counter, but this is another device.
	if env := b.until(gamev1.MsgId_MSG_ID_ROOM_ACTION_RSP); value(t, env) != 3 {
		t.Fatalf("value %d", value(t, env))
	}
}

func TestOldPositionFromLostSeatGetsSnapshot(t *testing.T) {
	s := newStack(t, map[string][]string{"room": {startBackend(t, room.Factory, 20*time.Millisecond)}})
	tok := s.login(t, 1)
	other := s.dial(t, s.login(t, 2), roomBinding(0)) // Keeps the scene alive.
	other.until(gamev1.MsgId_MSG_ID_ROOM_SNAPSHOT)
	c := s.dial(t, tok, roomBinding(0))
	last := c.until(gamev1.MsgId_MSG_ID_ROOM_SNAPSHOT).GetSeq()
	if err := c.ws.CloseNow(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond) // The seat expires.

	c2 := s.dial(t, tok, roomBinding(last))
	rsp := &gamev1.ResumeRsp{}
	if err := proto.Unmarshal(c2.until(gamev1.MsgId_MSG_ID_RESUME_RSP).GetPayload(), rsp); err != nil {
		t.Fatal(err)
	}
	if rsp.GetReplayed() {
		t.Fatal("a position from a lost seat must not replay")
	}
	c2.until(gamev1.MsgId_MSG_ID_ROOM_SNAPSHOT)
}

func TestMarchRetriedAfterSeatLossIsNotDoubled(t *testing.T) {
	timers, store, ids := worldDeps(t)
	w := slg.New(slg.Config{Store: store, Timers: timers, IDs: ids})
	s := newStack(t, map[string][]string{"world": {startBackend(t, w.Factory, 20*time.Millisecond)}})
	tok := s.login(t, 1)
	bind := &gamev1.Binding{Service: "world", Key: "k1"}
	march := func(c *client) uint64 {
		c.send(&gamev1.Envelope{MsgId: uint32(gamev1.MsgId_MSG_ID_MARCH_REQ), ClientSeq: 1, RequestId: "m1"},
			&gamev1.MarchReq{ToX: 1, Troops: 5})
		rsp := &gamev1.MarchRsp{}
		if err := proto.Unmarshal(c.until(gamev1.MsgId_MSG_ID_MARCH_RSP).GetPayload(), rsp); err != nil {
			t.Fatal(err)
		}
		return rsp.GetMarchId()
	}
	c := s.dial(t, tok, bind)
	c.until(gamev1.MsgId_MSG_ID_ROOM_SNAPSHOT)
	first := march(c)
	if err := c.ws.CloseNow(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond) // The seat, and its dedupe state, expire.

	c2 := s.dial(t, tok, bind)
	c2.until(gamev1.MsgId_MSG_ID_ROOM_SNAPSHOT)
	if again := march(c2); again != first {
		t.Fatalf("retry made march %d after %d", again, first)
	}
}

func TestMarchWithoutRequestIDIsRejected(t *testing.T) {
	timers, store, ids := worldDeps(t)
	w := slg.New(slg.Config{Store: store, Timers: timers, IDs: ids})
	s := newStack(t, map[string][]string{"world": {startBackend(t, w.Factory, time.Minute)}})
	c := s.dial(t, s.login(t, 1), &gamev1.Binding{Service: "world", Key: "k1"})
	c.until(gamev1.MsgId_MSG_ID_ROOM_SNAPSHOT)
	c.send(&gamev1.Envelope{MsgId: uint32(gamev1.MsgId_MSG_ID_MARCH_REQ), ClientSeq: 1},
		&gamev1.MarchReq{ToX: 1, Troops: 5})
	if env := c.read(); env.GetCode() != int32(gamev1.ErrorCode_ERROR_CODE_BAD_REQUEST) {
		t.Fatalf("reply %v", env)
	}
}

func TestStuckGatewayLosesItsTunnel(t *testing.T) {
	addr, _ := startHost(t, gamehost.Config{Factory: room.Factory, TunnelQueue: 8})
	cc, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer cc.Close() //nolint:errcheck // Best-effort.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stream, err := backendv1.NewGameBackendClient(cc).Tunnel(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&backendv1.Upstream{Uid: 1, Key: "r", Conn: 1, Body: &backendv1.Upstream_Attach{
		Attach: &backendv1.Attach{Epoch: 1, Key: "r"}}}); err != nil {
		t.Fatal(err)
	}
	payload, err := proto.Marshal(&gamev1.RoomActionReq{Delta: 1})
	if err != nil {
		t.Fatal(err)
	}
	// This client never reads, so the host's queue must fill and it must end
	// the stream. Send fails once it has.
	for seq := uint64(1); ; seq++ {
		frame, err := proto.Marshal(&gamev1.Envelope{MsgId: uint32(gamev1.MsgId_MSG_ID_ROOM_ACTION_REQ), ClientSeq: seq, Payload: payload})
		if err != nil {
			t.Fatal(err)
		}
		if stream.Send(&backendv1.Upstream{Uid: 1, Key: "r", Conn: 1, Body: &backendv1.Upstream_Frame{Frame: frame}}) != nil {
			if ctx.Err() != nil {
				t.Fatal("the host never closed a tunnel it could not send on")
			}
			return
		}
		if seq%256 == 0 {
			time.Sleep(time.Millisecond) // Lets the scene keep up, so replies pile up.
		}
	}
}

func TestDeadNodeDoesNotStallOtherConnections(t *testing.T) {
	// Accepts and then says nothing, so a gRPC dial hangs rather than fails.
	black, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer black.Close() //nolint:errcheck // Best-effort.
	go func() {
		for {
			if _, err := black.Accept(); err != nil {
				return
			}
		}
	}()
	s := newStackWith(t, map[string][]string{
		"room":  {startBackend(t, room.Factory, time.Minute)},
		"world": {black.Addr().String()},
	}, func(c *gateway.Config) { c.DialTimeout = 2 * time.Second })

	stuck := s.dial(t, s.login(t, 1), &gamev1.Binding{Service: "world", Key: "k"})
	defer stuck.ws.CloseNow() //nolint:errcheck // Best-effort.
	time.Sleep(100 * time.Millisecond)

	start := time.Now()
	c := s.dial(t, s.login(t, 2), roomBinding(0))
	c.until(gamev1.MsgId_MSG_ID_ROOM_SNAPSHOT)
	if d := time.Since(start); d > time.Second {
		t.Fatalf("a healthy connection waited %v behind a dead node", d)
	}
}

func closes(t *testing.T, c *client) {
	t.Helper()
	for {
		env, err := c.tryRead(2 * time.Second)
		if err != nil {
			return
		}
		if env.GetMsgId() == uint32(gamev1.MsgId_MSG_ID_ROOM_SNAPSHOT) {
			t.Fatal("received a snapshot from a node that must refuse")
		}
	}
}

func TestNodeRefusesSceneItDoesNotOwn(t *testing.T) {
	addr, host := startHost(t, gamehost.Config{Factory: room.Factory,
		Owns: func(string) bool { return false }})
	s := newStack(t, map[string][]string{"room": {addr}})
	closes(t, s.dial(t, s.login(t, 1), roomBinding(0)))
	if !host.Empty(func(string) bool { return true }) {
		t.Fatal("a refused attach must not start a scene")
	}
}

func TestEvictClosesPlayersAndStopsScene(t *testing.T) {
	addr, host := startHost(t, gamehost.Config{Factory: room.Factory})
	s := newStack(t, map[string][]string{"room": {addr}})
	c := s.dial(t, s.login(t, 1), roomBinding(0))
	c.until(gamev1.MsgId_MSG_ID_ROOM_SNAPSHOT)

	all := func(string) bool { return true }
	host.Evict(all)
	if !host.Empty(all) {
		t.Fatal("scene still running after Evict")
	}
	c.until(gamev1.MsgId_MSG_ID_KICK)
	closes(t, c)
}

func TestLastPlayerLeavingRetiresScene(t *testing.T) {
	addr, host := startHost(t, gamehost.Config{Factory: room.Factory, Grace: 20 * time.Millisecond})
	s := newStack(t, map[string][]string{"room": {addr}})
	tok := s.login(t, 1)
	c := s.dial(t, tok, roomBinding(0))
	c.until(gamev1.MsgId_MSG_ID_ROOM_SNAPSHOT)
	action(c, 1, 5)
	c.until(gamev1.MsgId_MSG_ID_ROOM_ACTION_RSP)
	if err := c.ws.CloseNow(); err != nil {
		t.Fatal(err)
	}

	all := func(string) bool { return true }
	deadline := time.Now().Add(3 * time.Second)
	for !host.Empty(all) {
		if time.Now().After(deadline) {
			t.Fatal("empty scene was never retired")
		}
		time.Sleep(10 * time.Millisecond)
	}
	c2 := s.dial(t, tok, roomBinding(0))
	snap := &gamev1.RoomSnapshot{}
	if err := proto.Unmarshal(c2.until(gamev1.MsgId_MSG_ID_ROOM_SNAPSHOT).GetPayload(), snap); err != nil {
		t.Fatal(err)
	}
	if snap.GetValue() != 0 {
		t.Fatalf("a new scene must start clean, got value %d", snap.GetValue())
	}
}

func TestChannelForWrongServiceClosesConnection(t *testing.T) {
	s := newStack(t, map[string][]string{"room": {startBackend(t, room.Factory, time.Minute)}})
	c := s.dial(t, s.login(t, 1), roomBinding(0))
	c.until(gamev1.MsgId_MSG_ID_ROOM_SNAPSHOT)
	c.send(&gamev1.Envelope{MsgId: uint32(gamev1.MsgId_MSG_ID_MARCH_REQ), ClientSeq: 1},
		&gamev1.MarchReq{ToX: 1, Troops: 1})
	for {
		if _, err := c.tryRead(2 * time.Second); err != nil {
			return // Closed, as required.
		}
	}
}
