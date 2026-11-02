// Command server runs one role, or every role in one process for development.
//
//	ROLE=all|account|gateway|slots|room|world|city|mail
//
// With no MYSQL_DSNS and REDIS_ADDR it uses in-memory stores. That is for local
// development only: state dies with the process and nothing is shared.
//
// With no ETCD_ENDPOINTS each stateful service must run as one node.
package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/redis/go-redis/v9"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	backendv1 "github.com/yourname/gameserver/gen/backend/v1"
	"github.com/yourname/gameserver/internal/account"
	"github.com/yourname/gameserver/internal/city"
	"github.com/yourname/gameserver/internal/gamehost"
	"github.com/yourname/gameserver/internal/gateway"
	"github.com/yourname/gameserver/internal/mail"
	"github.com/yourname/gameserver/internal/room"
	"github.com/yourname/gameserver/internal/slg"
	"github.com/yourname/gameserver/internal/slots"
	"github.com/yourname/gameserver/pkg/auth"
	"github.com/yourname/gameserver/pkg/cluster"
	"github.com/yourname/gameserver/pkg/discovery"
	"github.com/yourname/gameserver/pkg/events"
	"github.com/yourname/gameserver/pkg/idgen"
	"github.com/yourname/gameserver/pkg/session"
	"github.com/yourname/gameserver/pkg/shard"
	"github.com/yourname/gameserver/pkg/timer"
	"github.com/yourname/gameserver/pkg/wallet"
)

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// deps are the stores, shared by every role in the process.
type deps struct {
	signer   *auth.Signer
	ids      *idgen.Generator
	epochs   session.Epochs
	accounts auth.Accounts
	wallet   wallet.Wallet
	timers   timer.Store
	marches  slg.Store
	cities   city.Store
	mails    mail.Store
	outboxes []events.Store
	etcd     *clientv3.Client // Nil means a single node per stateful service.
}

func build() (*deps, error) {
	key := os.Getenv("JWT_KEY")
	if len(key) < 32 {
		return nil, errors.New("JWT_KEY must be at least 32 bytes")
	}
	d := &deps{signer: &auth.Signer{Key: []byte(key), TTL: 24 * time.Hour}}

	if addr := os.Getenv("REDIS_ADDR"); addr != "" {
		d.epochs = &session.Redis{Client: redis.NewUniversalClient(&redis.UniversalOptions{Addrs: strings.Split(addr, ",")})}
	} else {
		slog.Warn("REDIS_ADDR unset: using in-memory sessions")
		d.epochs = session.NewMemory()
	}

	if eps := os.Getenv("ETCD_ENDPOINTS"); eps != "" {
		c, err := clientv3.New(clientv3.Config{Endpoints: strings.Split(eps, ","), DialTimeout: 5 * time.Second})
		if err != nil {
			return nil, fmt.Errorf("etcd: %w", err)
		}
		d.etcd = c
	} else {
		slog.Warn("ETCD_ENDPOINTS unset: stateful services must run as one node each")
	}

	dsns := os.Getenv("MYSQL_DSNS") // Comma-separated, one per physical shard.
	if dsns == "" {
		slog.Warn("MYSQL_DSNS unset: using in-memory stores")
		outbox := &events.MemoryStore{}
		timers := timer.NewMemStore()
		d.accounts = auth.NewMemoryAccounts()
		d.wallet = wallet.NewMemory(outbox)
		d.timers = timers
		d.marches = slg.NewMemoryStore(timers)
		d.cities = city.NewMemory(timers)
		d.mails = mail.NewMemory(timers)
		d.outboxes = []events.Store{outbox}
		return d, nil
	}
	var dbs []*sql.DB
	for _, dsn := range strings.Split(dsns, ",") {
		cfg, err := mysql.ParseDSN(dsn)
		if err != nil {
			return nil, err
		}
		cfg.ParseTime = true // Timer and march rows scan into time.Time.
		db, err := sql.Open("mysql", cfg.FormatDSN())
		if err != nil {
			return nil, err
		}
		dbs = append(dbs, db)
		d.outboxes = append(d.outboxes, &events.MySQLStore{DB: db})
	}
	router, err := shard.NewEven(dbs)
	if err != nil {
		return nil, err
	}
	d.accounts = &auth.MySQLAccounts{DB: dbs[0]}
	d.wallet = &wallet.MySQL{Shards: router}
	d.timers = &timer.MySQLStore{DB: dbs[0]}
	d.marches = &slg.MySQLStore{DB: dbs[0]}
	d.cities = &city.MySQL{DB: dbs[0]}
	d.mails = &mail.MySQL{DB: dbs[0]}
	return d, nil
}

// nodeID returns NODE_ID, or leases a unique one from etcd. Two processes with
// one ID would issue duplicate IDs, and a uid is one such ID.
func (d *deps) nodeID(ctx context.Context, stop func()) (int64, error) {
	if v := os.Getenv("NODE_ID"); v != "" || d.etcd == nil {
		id, err := strconv.ParseInt(env("NODE_ID", "0"), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("NODE_ID: %w", err)
		}
		return id, nil
	}
	id, sess, err := cluster.ClaimNodeID(ctx, d.etcd, "", idgen.MaxNode+1)
	if err != nil {
		return 0, err
	}
	go func() {
		select {
		case <-sess.Done():
			slog.Error("node id lease lost")
			stop() // Another process may now take this ID.
		case <-ctx.Done():
		}
	}()
	return id, nil
}

func publisher() events.Publisher {
	if env("KAFKA_ENABLED", "false") == "true" {
		return events.NewKafka(strings.Split(env("KAFKA_BROKERS", "localhost:9092"), ","))
	}
	return events.Discard{}
}

func serveHTTP(ctx context.Context, name, addr string, h http.Handler) {
	srv := &http.Server{Addr: addr, Handler: h, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second}
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(sctx)
	}()
	go func() {
		slog.Info("listening", "role", name, "addr", addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("http server", "role", name, "err", err)
		}
	}()
}

// slotHooks are what a game node does as it gains and loses slots.
type slotHooks struct {
	acquired func(slot int)
	released func(slot int)
}

func inSlot(slot int) func(key string) bool {
	return func(key string) bool { return discovery.Slot(key) == slot }
}

// runGame starts a game node. In a cluster it takes slots as etcd assigns them;
// alone, it owns them all. Any failure to stay in the cluster ends the process,
// since a node that has lost its slots must not keep serving.
func runGame(ctx context.Context, stop func(), wg *sync.WaitGroup, d *deps, name, addr string, f gamehost.Factory, hooks slotHooks) error {
	if d.etcd == nil {
		if _, err := serveGame(ctx, name, addr, f, nil); err != nil {
			return err
		}
		for slot := range discovery.Slots {
			hooks.acquired(slot)
		}
		return nil
	}
	advertise := os.Getenv(strings.ToUpper(name) + "_ADVERTISE")
	if advertise == "" {
		return fmt.Errorf("%s_ADVERTISE is required with ETCD_ENDPOINTS", strings.ToUpper(name))
	}
	var host *gamehost.Host
	node := cluster.New(cluster.Config{Client: d.etcd, Service: name, NodeID: env("POD_NAME", "node-0"), Addr: advertise,
		Idle:     func(slot int) bool { return host.Empty(inSlot(slot)) },
		Acquired: hooks.acquired,
		Released: func(slot int) {
			host.Evict(inSlot(slot))
			hooks.released(slot)
		},
	})
	host, err := serveGame(ctx, name, addr, f, node.Owns)
	if err != nil {
		return err
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := node.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			slog.Error("left cluster", "role", name, "err", err)
			stop()
		}
	}()
	return nil
}

func serveGame(ctx context.Context, name, addr string, f gamehost.Factory, owns func(string) bool) (*gamehost.Host, error) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	host := gamehost.New(gamehost.Config{Factory: f, Owns: owns})
	// Must allow the gateways' keepalive pings.
	srv := grpc.NewServer(grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
		MinTime: 10 * time.Second, PermitWithoutStream: true}))
	backendv1.RegisterGameBackendServer(srv, host)
	go func() {
		<-ctx.Done()
		srv.GracefulStop()
		host.Stop()
	}()
	go func() {
		slog.Info("listening", "role", name, "addr", addr)
		if err := srv.Serve(lis); err != nil {
			slog.Error("grpc server", "role", name, "err", err)
		}
	}()
	return host, nil
}

func run() error {
	d, err := build()
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	id, err := d.nodeID(ctx, stop)
	if err != nil {
		return err
	}
	if d.ids, err = idgen.New(id); err != nil {
		return err
	}

	var wg sync.WaitGroup
	role := env("ROLE", "all")
	on := func(r string) bool { return role == "all" || role == r }

	if on("account") {
		local := &auth.Local{Accounts: d.accounts}
		providers := map[string]auth.Provider{local.Name(): local}
		if u := os.Getenv("THIRD_PARTY_AUTH_URL"); u != "" {
			p := &auth.HTTPProvider{ProviderName: env("THIRD_PARTY_AUTH_NAME", "partner"), URL: u}
			providers[p.Name()] = p
		}
		mux := http.NewServeMux()
		(&account.Service{Local: local, Providers: providers, Accounts: d.accounts,
			Signer: d.signer, Epochs: d.epochs, IDs: d.ids}).Routes(mux)
		serveHTTP(ctx, "account", env("ACCOUNT_ADDR", ":8081"), mux)
	}
	if on("slots") {
		mux := http.NewServeMux()
		(&slots.Service{Wallet: d.wallet, Signer: d.signer, Epochs: d.epochs, IDs: d.ids}).Routes(mux)
		serveHTTP(ctx, "slots", env("SLOTS_ADDR", ":8082"), mux)
		for _, s := range d.outboxes {
			r := &events.Relay{Store: s, Publisher: publisher()}
			go r.Run(ctx)
		}
	}
	if on("room") {
		noop := func(int) {}
		if err := runGame(ctx, stop, &wg, d, "room", env("ROOM_ADDR", ":9001"), room.Factory,
			slotHooks{acquired: noop, released: noop}); err != nil {
			return err
		}
	}
	// Each service owns its slots separately, so each has its own timer group.
	timed := func(name string, base int) (*timer.Group, slotHooks) {
		g := timer.NewGroup(timer.Config{Owner: env("POD_NAME", "node-0") + "/" + name, Store: d.timers, ShardBase: base})
		return g, slotHooks{acquired: func(slot int) { g.Start(ctx, slot) }, released: g.Stop}
	}
	if on("world") {
		timers, hooks := timed("world", 0)
		w := slg.New(slg.Config{Store: d.marches, Timers: timers, IDs: d.ids})
		if err := runGame(ctx, stop, &wg, d, "world", env("WORLD_ADDR", ":9002"), w.Factory, hooks); err != nil {
			return err
		}
	}
	if on("city") {
		timers, hooks := timed("city", city.ShardBase)
		c := city.New(city.Config{Store: d.cities, Timers: timers, IDs: d.ids})
		if err := runGame(ctx, stop, &wg, d, "city", env("CITY_ADDR", ":9003"), c.Factory, hooks); err != nil {
			return err
		}
	}
	if on("mail") {
		timers, hooks := timed("mail", mail.ShardBase)
		m := mail.New(mail.Config{Store: d.mails, Timers: timers, IDs: d.ids})
		if err := runGame(ctx, stop, &wg, d, "mail", env("MAIL_ADDR", ":9004"), m.Factory, hooks); err != nil {
			return err
		}
	}
	if on("gateway") {
		var resolver discovery.Resolver
		if d.etcd != nil {
			r := cluster.NewResolver(d.etcd, "", 0, "room", "world", "city", "mail")
			go r.Run(ctx)
			resolver = r
		} else {
			resolver = discovery.NewStatic(map[string][]string{
				"room":  strings.Split(env("ROOM_NODES", "127.0.0.1:9001"), ","),
				"world": strings.Split(env("WORLD_NODES", "127.0.0.1:9002"), ","),
				"city":  strings.Split(env("CITY_NODES", "127.0.0.1:9003"), ","),
				"mail":  strings.Split(env("MAIL_NODES", "127.0.0.1:9004"), ","),
			})
		}
		g := gateway.New(gateway.Config{Signer: d.signer, Epochs: d.epochs, Resolver: resolver})
		mux := http.NewServeMux()
		mux.Handle("GET /ws", g)
		serveHTTP(ctx, "gateway", env("GATEWAY_ADDR", ":8080"), mux)
		go g.Run(ctx)
		const drainWindow = 5 * time.Second
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-ctx.Done()
			g.Drain(drainWindow)
			time.Sleep(drainWindow + time.Second) // Exiting early would drop the rest at once.
		}()
	}
	<-ctx.Done()
	slog.Info("shutting down")
	time.Sleep(2 * time.Second) // Lets the shutdown goroutines finish.
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done: // Slots were handed back.
	case <-time.After(15 * time.Second):
		slog.Warn("shutdown timed out; leases will expire instead")
	}
	return nil
}

func main() {
	if err := run(); err != nil {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}
}
