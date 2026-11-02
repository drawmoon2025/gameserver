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
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	host := gamehost.New(gamehost.Config{Factory: f, Grace: grace, ReplayFrames: 8})
	srv := grpc.NewServer()
	backendv1.RegisterGameBackendServer(srv, host)
	go srv.Serve(lis) //nolint:errcheck // Stops with the test.
	t.Cleanup(func() { srv.Stop(); host.Stop() })
	return lis.Addr().String()
}

func newStack(t *testing.T, nodes map[string][]string) *stack {
	t.Helper()
	s := &stack{signer: &auth.Signer{Key: []byte("0123456789abcdef0123456789abcdef"), TTL: time.Hour},
		epochs: session.NewMemory()}
	g := gateway.New(gateway.Config{Signer: s.signer, Epochs: s.epochs, Resolver: discovery.NewStatic(nodes)})
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
	sched, store, ids := worldDeps(t)
	w := slg.New(slg.Config{Store: store, Scheduler: sched, IDs: ids, Speed: 1000})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go sched.Run(ctx) //nolint:errcheck // Ends with the test.

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
	c.send(&gamev1.Envelope{MsgId: uint32(gamev1.MsgId_MSG_ID_MARCH_REQ), ClientSeq: 1, Channel: 1},
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

func worldDeps(t *testing.T) (*timer.Scheduler, *slg.MemoryStore, *idgen.Generator) {
	t.Helper()
	ids, err := idgen.New(1)
	if err != nil {
		t.Fatal(err)
	}
	sched := timer.New(timer.Config{Owner: "t", Store: timer.NewMemStore(), Poll: 10 * time.Millisecond})
	return sched, slg.NewMemoryStore(), ids
}

func TestMarchSurvivesWorldRestart(t *testing.T) {
	store := timer.NewMemStore()
	marches := slg.NewMemoryStore()
	ids, err := idgen.New(1)
	if err != nil {
		t.Fatal(err)
	}
	// First process: accept a march, then die before it arrives.
	first := timer.New(timer.Config{Owner: "a", Store: store})
	w1 := slg.New(slg.Config{Store: marches, Scheduler: first, IDs: ids, Speed: 100})
	s := newStack(t, map[string][]string{"world": {startBackend(t, w1.Factory, time.Minute)}})
	c := s.dial(t, s.login(t, 1), &gamev1.Binding{Service: "world", Key: "k1"})
	c.until(gamev1.MsgId_MSG_ID_ROOM_SNAPSHOT)
	c.send(&gamev1.Envelope{MsgId: uint32(gamev1.MsgId_MSG_ID_MARCH_REQ), ClientSeq: 1},
		&gamev1.MarchReq{ToX: 5, ToY: 5, Troops: 1})
	c.until(gamev1.MsgId_MSG_ID_MARCH_RSP)
	if store.Len(0) != 1 {
		t.Fatalf("stored jobs %d", store.Len(0))
	}

	// Second process: picks the job up from storage and completes it.
	second := timer.New(timer.Config{Owner: "b", Store: store, Poll: 10 * time.Millisecond})
	w2 := slg.New(slg.Config{Store: marches, Scheduler: second, IDs: ids})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go second.Run(ctx) //nolint:errcheck // Ends with the test.
	deadline := time.Now().Add(3 * time.Second)
	for store.Len(0) != 0 {
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
