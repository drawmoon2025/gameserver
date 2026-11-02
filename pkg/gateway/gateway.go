// Package gateway terminates client WebSockets. It verifies tokens, enforces
// per-connection limits, and multiplexes frames over one gRPC tunnel per game
// node. It keeps no game state, so losing a gateway only costs a reconnect.
package gateway

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/protobuf/proto"

	gamev1 "github.com/drawmoon2025/gameserver/gen/game/v1"
	backendv1 "github.com/drawmoon2025/gameserver/gen/backend/v1"
	"github.com/drawmoon2025/gameserver/pkg/auth"
	"github.com/drawmoon2025/gameserver/pkg/discovery"
	"github.com/drawmoon2025/gameserver/pkg/session"
)

// Routes maps a client message ID to a service name, by range. This is how one
// SLG connection reaches world, alliance and chat backends.
type Routes func(msgID uint32) (service string, ok bool)

// DefaultRoutes reflects the MsgId ranges in envelope.proto.
func DefaultRoutes(msgID uint32) (string, bool) {
	switch {
	case msgID >= 1000 && msgID < 2000:
		return "room", true
	case msgID >= 3000 && msgID < 4000:
		return "world", true
	case msgID >= 4000 && msgID < 5000:
		return "city", true
	case msgID >= 5000 && msgID < 6000:
		return "mail", true
	case msgID >= 6000 && msgID < 7000:
		return "alliance", true
	case msgID >= 7000 && msgID < 8000:
		return "chat", true
	case msgID >= 8000 && msgID < 9000:
		return "rank", true
	}
	return "", false
}

// Config tunes a Gateway.
type Config struct {
	Signer     *auth.Signer
	Epochs     session.Epochs
	Resolver   discovery.Resolver
	Routes     Routes
	MaxFrame   int64         // Default 64 KiB.
	MaxBinding int           // Default 8.
	AuthWait   time.Duration // Default 5s.
	IdleWait   time.Duration // Default 45s; clients heartbeat every 15s.
	SendQueue  int           // Default 256.
	RateLimit  rate.Limit    // Messages per second. Default 30.
	RateBurst  int           // Default 60.
	// RecheckEvery is how often an open socket re-checks its login, for
	// notifications that were missed. Default 5m.
	RecheckEvery time.Duration
	// DialTimeout bounds opening a tunnel. Default 3s.
	DialTimeout time.Duration
	// DialBackoff is how long a failed node is left alone. Default 1s.
	DialBackoff time.Duration
	Dial        func(addr string) (*grpc.ClientConn, error)
}

// Gateway serves WebSocket clients.
type Gateway struct {
	cfg      Config
	draining atomic.Bool
	online   atomic.Int64
	nextConn atomic.Uint64

	mu      sync.Mutex
	tunnels map[string]*tunnelSlot
	conns   map[*conn]struct{}
	byUID   map[int64]map[*conn]struct{}
}

// New creates a Gateway.
func New(cfg Config) *Gateway {
	if cfg.Routes == nil {
		cfg.Routes = DefaultRoutes
	}
	if cfg.MaxFrame <= 0 {
		cfg.MaxFrame = 64 << 10
	}
	if cfg.MaxBinding <= 0 {
		cfg.MaxBinding = 8
	}
	if cfg.AuthWait <= 0 {
		cfg.AuthWait = 5 * time.Second
	}
	if cfg.IdleWait <= 0 {
		cfg.IdleWait = 45 * time.Second
	}
	if cfg.SendQueue <= 0 {
		cfg.SendQueue = 256
	}
	if cfg.RateLimit <= 0 {
		cfg.RateLimit, cfg.RateBurst = 30, 60
	}
	if cfg.RecheckEvery <= 0 {
		cfg.RecheckEvery = 5 * time.Minute
	}
	if cfg.DialTimeout <= 0 {
		cfg.DialTimeout = 3 * time.Second
	}
	if cfg.DialBackoff <= 0 {
		cfg.DialBackoff = time.Second
	}
	if cfg.Dial == nil {
		cfg.Dial = func(addr string) (*grpc.ClientConn, error) {
			// Client heartbeats end at the gateway, so without pings a silently
			// dead node would look healthy until TCP gave up.
			return grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithKeepaliveParams(keepalive.ClientParameters{
					Time: 15 * time.Second, Timeout: 5 * time.Second, PermitWithoutStream: true}))
		}
	}
	return &Gateway{cfg: cfg, tunnels: map[string]*tunnelSlot{}, conns: map[*conn]struct{}{},
		byUID: map[int64]map[*conn]struct{}{}}
}

// Run closes superseded sockets as new logins are announced, until ctx ends.
func (g *Gateway) Run(ctx context.Context) {
	n, ok := g.cfg.Epochs.(session.Notifier)
	if !ok {
		return
	}
	for ctx.Err() == nil {
		if err := n.Subscribe(ctx, g.superseded); err != nil && ctx.Err() == nil {
			slog.Warn("login notifications", "err", err)
			time.Sleep(time.Second)
		}
	}
}

func (g *Gateway) superseded(uid int64, epoch uint64) {
	g.mu.Lock()
	var old []*conn
	for c := range g.byUID[uid] {
		if c.epoch < epoch {
			old = append(old, c)
		}
	}
	g.mu.Unlock()
	for _, c := range old {
		c.kick("logged in elsewhere")
	}
}

// Online returns the current connection count.
func (g *Gateway) Online() int64 { return g.online.Load() }

// conn is one client socket.
type conn struct {
	id       uint64 // Unique in this gateway, so within any tunnel it owns.
	uid      int64
	epoch    uint64
	expires  time.Time
	recheck  time.Time // Read loop only.
	ws       *websocket.Conn
	out      chan []byte
	closed   chan struct{}
	flushed  chan struct{} // Closed once writeLoop has stopped writing.
	once     sync.Once
	reason   atomic.Value // string
	bindings []bound
	limiter  *rate.Limiter
}

type bound struct {
	service string
	key     string
	tun     *tunnel
}

func (c *conn) shut(reason string) {
	c.once.Do(func() {
		c.reason.Store(reason)
		close(c.closed)
	})
}

// kick tells the client why, then closes.
func (c *conn) kick(reason string) {
	if f, err := kickFrame(reason); err == nil {
		c.push(f)
	}
	c.shut(reason)
}

func (c *conn) push(frame []byte) {
	// Checked first, and alone: after one frame is dropped no later frame may be
	// sent, or the client would report a last_seq beyond the gap and lose it.
	select {
	case <-c.closed:
		return
	default:
	}
	select {
	case c.out <- frame:
	default:
		// Slow client. Closing is safe: the game node retains its frames.
		c.shut("slow client")
	}
}

// ServeHTTP upgrades and runs one connection.
func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if g.draining.Load() {
		http.Error(w, "draining", http.StatusServiceUnavailable)
		return
	}
	ws, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		// Token auth, not cookies, so a cross-origin page gains nothing.
		InsecureSkipVerify: true,
	})
	if err != nil {
		return
	}
	ws.SetReadLimit(g.cfg.MaxFrame)
	defer ws.CloseNow() //nolint:errcheck // Best-effort cleanup.

	ctx := r.Context()
	c, err := g.handshake(ctx, ws)
	if err != nil {
		slog.Debug("handshake rejected", "err", err)
		_ = ws.Close(websocket.StatusPolicyViolation, "unauthorized")
		return
	}
	g.track(c, true)
	defer g.track(c, false)
	defer g.detach(c)

	go g.writeLoop(ctx, c)
	g.readLoop(ctx, c)
	c.shut("client gone")
	<-c.flushed // A close frame must not overtake a queued Kick.
	if reason, _ := c.reason.Load().(string); reason != "" {
		_ = ws.Close(websocket.StatusNormalClosure, reason)
	}
}

func (g *Gateway) track(c *conn, add bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if add {
		g.conns[c] = struct{}{}
		if g.byUID[c.uid] == nil {
			g.byUID[c.uid] = map[*conn]struct{}{}
		}
		g.byUID[c.uid][c] = struct{}{}
		g.online.Add(1)
		return
	}
	delete(g.conns, c)
	delete(g.byUID[c.uid], c)
	if len(g.byUID[c.uid]) == 0 {
		delete(g.byUID, c.uid)
	}
	g.online.Add(-1)
}

func (g *Gateway) handshake(ctx context.Context, ws *websocket.Conn) (*conn, error) {
	ctx, cancel := context.WithTimeout(ctx, g.cfg.AuthWait)
	defer cancel()
	_, data, err := ws.Read(ctx)
	if err != nil {
		return nil, fmt.Errorf("read connect: %w", err)
	}
	env := &gamev1.Envelope{}
	if err := proto.Unmarshal(data, env); err != nil || env.GetMsgId() != uint32(gamev1.MsgId_MSG_ID_CONNECT_REQ) {
		return nil, errors.New("first frame must be ConnectReq")
	}
	req := &gamev1.ConnectReq{}
	if err := proto.Unmarshal(env.GetPayload(), req); err != nil {
		return nil, fmt.Errorf("bad ConnectReq: %w", err)
	}
	if n := len(req.GetBindings()); n == 0 || n > g.cfg.MaxBinding {
		return nil, fmt.Errorf("binding count %d", n)
	}
	claims, err := g.cfg.Signer.Verify(req.GetToken())
	if err != nil {
		return nil, err
	}
	// Reading Redis here, once per connect and never per message, closes the
	// window where a superseded token could still attach.
	cur, err := g.cfg.Epochs.Current(ctx, claims.UID)
	if err != nil {
		return nil, err
	}
	if claims.Epoch != cur {
		return nil, fmt.Errorf("stale epoch %d, current %d", claims.Epoch, cur)
	}

	c := &conn{id: g.nextConn.Add(1), uid: claims.UID, epoch: claims.Epoch, ws: ws,
		out: make(chan []byte, g.cfg.SendQueue), closed: make(chan struct{}), flushed: make(chan struct{}),
		limiter: rate.NewLimiter(g.cfg.RateLimit, g.cfg.RateBurst)}
	if claims.ExpiresAt != nil {
		c.expires = claims.ExpiresAt.Time
	}
	c.recheck = time.Now().Add(g.jitter())
	attached := false
	defer func() {
		// A partial handshake must not leave earlier bindings looking online.
		if !attached {
			g.detach(c)
		}
	}()
	seen := make(map[string]bool, len(req.GetBindings()))
	for i, b := range req.GetBindings() {
		// A host seats a uid once per scene, so a duplicate would silently
		// overwrite the first binding's channel.
		id := b.GetService() + "\x00" + b.GetKey()
		if seen[id] {
			return nil, fmt.Errorf("duplicate binding %s/%s", b.GetService(), b.GetKey())
		}
		seen[id] = true
		addr, err := g.cfg.Resolver.Resolve(b.GetService(), b.GetKey())
		if err != nil {
			return nil, err
		}
		t, err := g.tunnel(addr)
		if err != nil {
			return nil, err
		}
		t.add(c)
		c.bindings = append(c.bindings, bound{service: b.GetService(), key: b.GetKey(), tun: t})
		err = t.sendUp(&backendv1.Upstream{Uid: c.uid, Key: b.GetKey(), Conn: c.id,
			Body: &backendv1.Upstream_Attach{Attach: &backendv1.Attach{
				SessionId: claims.Session, Epoch: c.epoch, LastSeq: b.GetLastSeq(),
				Resume: b.GetLastSeq() > 0, Key: b.GetKey(), Channel: uint32(i)}}})
		if err != nil {
			return nil, err
		}
	}
	attached = true
	return c, nil
}

// jitter spreads re-checks, so connections opened together do not hit Redis
// together.
func (g *Gateway) jitter() time.Duration {
	return g.cfg.RecheckEvery/2 + time.Duration(rand.Int64N(int64(g.cfg.RecheckEvery)))
}

// loginValid is the backstop for a missed login notification. A Redis failure
// counts as valid: it must not disconnect everyone.
func (g *Gateway) loginValid(ctx context.Context, c *conn) bool {
	c.recheck = time.Now().Add(g.jitter())
	if !c.expires.IsZero() && time.Now().After(c.expires) {
		c.kick("session expired")
		return false
	}
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	cur, err := g.cfg.Epochs.Current(ctx, c.uid)
	if err != nil {
		slog.Warn("login re-check", "uid", c.uid, "err", err)
		return true
	}
	if cur != c.epoch {
		c.kick("logged in elsewhere")
		return false
	}
	return true
}

func (g *Gateway) readLoop(ctx context.Context, c *conn) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		// Cancelling a read closes the socket, so wait for queued frames, which
		// may include the Kick explaining why this connection is closing.
		select {
		case <-c.flushed:
			cancel()
		case <-ctx.Done():
		}
	}()
	for {
		rctx, rcancel := context.WithTimeout(ctx, g.cfg.IdleWait)
		_, data, err := c.ws.Read(rctx)
		rcancel()
		if err != nil {
			return
		}
		if !c.limiter.Allow() {
			c.shut("rate limited")
			return
		}
		if time.Now().After(c.recheck) && !g.loginValid(ctx, c) {
			return
		}
		env := &gamev1.Envelope{}
		if err := proto.Unmarshal(data, env); err != nil {
			c.shut("malformed frame")
			return
		}
		if env.GetMsgId() == uint32(gamev1.MsgId_MSG_ID_HEARTBEAT) {
			if b, err := proto.Marshal(&gamev1.Envelope{MsgId: env.GetMsgId()}); err == nil {
				c.push(b)
			}
			continue
		}
		service, ok := g.cfg.Routes(env.GetMsgId())
		if !ok {
			c.shut("unroutable message")
			return
		}
		// The channel picks the binding, since two rooms share a service. The
		// service check stops a frame reaching a backend that cannot parse it.
		ch := env.GetChannel()
		if uint64(ch) >= uint64(len(c.bindings)) || c.bindings[ch].service != service {
			c.shut("service not bound on channel")
			return
		}
		b := &c.bindings[ch]
		if err := b.tun.sendUp(&backendv1.Upstream{Uid: c.uid, Key: b.key, Conn: c.id,
			Body: &backendv1.Upstream_Frame{Frame: data}}); err != nil {
			c.shut("backend lost")
			return
		}
	}
}

func (g *Gateway) writeLoop(ctx context.Context, c *conn) {
	defer close(c.flushed)
	write := func(ctx context.Context, frame []byte, limit time.Duration) bool {
		wctx, cancel := context.WithTimeout(ctx, limit)
		defer cancel()
		if err := c.ws.Write(wctx, websocket.MessageBinary, frame); err != nil {
			c.shut("write failed")
			return false
		}
		return true
	}
	for {
		select {
		case <-c.closed:
			// A slow client must not hold its socket open, so the flush is bounded.
			fctx, cancel := context.WithTimeout(ctx, time.Second)
			defer cancel()
			for {
				select {
				case frame := <-c.out:
					if !write(fctx, frame, time.Second) {
						return
					}
				default:
					return
				}
			}
		case <-ctx.Done():
			return
		case frame := <-c.out:
			if !write(ctx, frame, 10*time.Second) {
				return
			}
		}
	}
}

func (g *Gateway) detach(c *conn) {
	for _, b := range c.bindings {
		b.tun.remove(c)
		_ = b.tun.sendUp(&backendv1.Upstream{Uid: c.uid, Key: b.key, Conn: c.id,
			Body: &backendv1.Upstream_Detach{Detach: &backendv1.Detach{}}})
	}
}

// Drain stops new connections and closes current ones spread across window, so
// a rolling deploy does not trigger a synchronized reconnect storm.
func (g *Gateway) Drain(window time.Duration) {
	g.draining.Store(true)
	g.mu.Lock()
	conns := make([]*conn, 0, len(g.conns))
	for c := range g.conns {
		conns = append(conns, c)
	}
	g.mu.Unlock()
	for _, c := range conns {
		c := c
		time.AfterFunc(time.Duration(rand.Int64N(int64(window)+1)), func() {
			if b, err := kickFrame("server restarting"); err == nil {
				c.push(b)
			}
			time.AfterFunc(200*time.Millisecond, func() { c.shut("draining") })
		})
	}
}

func kickFrame(reason string) ([]byte, error) {
	p, err := proto.Marshal(&gamev1.Kick{Reason: reason})
	if err != nil {
		return nil, err
	}
	return proto.Marshal(&gamev1.Envelope{MsgId: uint32(gamev1.MsgId_MSG_ID_KICK), Payload: p})
}

// tunnel is one gRPC stream to a game node.
type tunnel struct {
	addr   string
	stream grpc.BidiStreamingClient[backendv1.Upstream, backendv1.Downstream]
	cancel context.CancelFunc

	sendMu sync.Mutex // A gRPC stream allows one concurrent sender.

	mu    sync.RWMutex
	conns map[uint64]*conn
	dead  bool
}

// tunnelSlot serializes dialing one address, so that a dead node delays only
// the connections that need it, and only once per DialBackoff.
type tunnelSlot struct {
	mu     sync.Mutex
	t      *tunnel
	failed time.Time
}

func (g *Gateway) tunnel(addr string) (*tunnel, error) {
	g.mu.Lock()
	s := g.tunnels[addr]
	if s == nil {
		s = &tunnelSlot{}
		g.tunnels[addr] = s
	}
	g.mu.Unlock()

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.t != nil && !s.t.isDead() {
		return s.t, nil
	}
	if time.Since(s.failed) < g.cfg.DialBackoff {
		return nil, fmt.Errorf("node %s recently unreachable", addr)
	}
	t, err := g.dial(addr)
	if err != nil {
		s.failed = time.Now()
		return nil, err
	}
	s.t = t
	return t, nil
}

func (g *Gateway) dial(addr string) (*tunnel, error) {
	cc, err := g.cfg.Dial(addr)
	if err != nil {
		return nil, err
	}
	// The stream outlives this call, so its context only gets a deadline here.
	ctx, cancel := context.WithCancel(context.Background())
	deadline := time.AfterFunc(g.cfg.DialTimeout, cancel)
	stream, err := backendv1.NewGameBackendClient(cc).Tunnel(ctx)
	if !deadline.Stop() && err == nil {
		err = fmt.Errorf("dial %s timed out", addr)
	}
	if err != nil {
		cancel()
		_ = cc.Close()
		return nil, err
	}
	t := &tunnel{addr: addr, stream: stream, cancel: cancel, conns: map[uint64]*conn{}}
	go func() {
		t.recvLoop()
		_ = cc.Close()
	}()
	return t, nil
}

func (t *tunnel) isDead() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.dead
}

func (t *tunnel) add(c *conn) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.conns[c.id] = c
}

func (t *tunnel) remove(c *conn) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.conns, c.id)
}

func (t *tunnel) sendUp(m *backendv1.Upstream) error {
	t.sendMu.Lock()
	defer t.sendMu.Unlock()
	return t.stream.Send(m)
}

func (t *tunnel) recvLoop() {
	defer t.cancel()
	for {
		m, err := t.stream.Recv()
		if err != nil {
			t.mu.Lock()
			t.dead = true
			all := t.conns
			t.conns = map[uint64]*conn{}
			t.mu.Unlock()
			// The game node is gone: clients reconnect and re-resolve.
			for _, c := range all {
				c.shut("backend lost")
			}
			return
		}
		t.mu.RLock()
		c := t.conns[m.GetConn()]
		t.mu.RUnlock()
		if c == nil {
			continue // The socket left while this frame was in flight.
		}
		switch b := m.GetBody().(type) {
		case *backendv1.Downstream_Frame:
			c.push(b.Frame)
		case *backendv1.Downstream_CloseReason:
			c.kick(b.CloseReason)
		}
	}
}
