// Command server runs one role, or every role in one process for development.
//
//	ROLE=all|account|gateway|slots|room|world
//
// With no MYSQL_DSNS and REDIS_ADDR it uses in-memory stores. That is for local
// development only: state dies with the process and nothing is shared.
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
	"syscall"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"

	backendv1 "github.com/yourname/gameserver/gen/backend/v1"
	"github.com/yourname/gameserver/internal/account"
	"github.com/yourname/gameserver/internal/gamehost"
	"github.com/yourname/gameserver/internal/gateway"
	"github.com/yourname/gameserver/internal/room"
	"github.com/yourname/gameserver/internal/slg"
	"github.com/yourname/gameserver/internal/slots"
	"github.com/yourname/gameserver/pkg/auth"
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
	outboxes []events.Store
}

func build() (*deps, error) {
	key := os.Getenv("JWT_KEY")
	if len(key) < 32 {
		return nil, errors.New("JWT_KEY must be at least 32 bytes")
	}
	node, err := strconv.ParseInt(env("NODE_ID", "0"), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("NODE_ID: %w", err)
	}
	ids, err := idgen.New(node)
	if err != nil {
		return nil, err
	}
	d := &deps{signer: &auth.Signer{Key: []byte(key), TTL: 24 * time.Hour}, ids: ids}

	if addr := os.Getenv("REDIS_ADDR"); addr != "" {
		d.epochs = &session.Redis{Client: redis.NewUniversalClient(&redis.UniversalOptions{Addrs: strings.Split(addr, ",")})}
	} else {
		slog.Warn("REDIS_ADDR unset: using in-memory sessions")
		d.epochs = session.NewMemory()
	}

	dsns := os.Getenv("MYSQL_DSNS") // Comma-separated, one per physical shard.
	if dsns == "" {
		slog.Warn("MYSQL_DSNS unset: using in-memory stores")
		outbox := &events.MemoryStore{}
		d.accounts = auth.NewMemoryAccounts()
		d.wallet = wallet.NewMemory(outbox)
		d.timers = timer.NewMemStore()
		d.marches = slg.NewMemoryStore()
		d.outboxes = []events.Store{outbox}
		return d, nil
	}
	var dbs []*sql.DB
	for _, dsn := range strings.Split(dsns, ",") {
		db, err := sql.Open("mysql", dsn)
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
	return d, nil
}

func publisher() events.Publisher {
	if env("KAFKA_ENABLED", "false") == "true" {
		return events.NewKafka(strings.Split(env("KAFKA_BROKERS", "localhost:9092"), ","))
	}
	return events.Discard{}
}

func serveHTTP(ctx context.Context, name, addr string, h http.Handler) {
	srv := &http.Server{Addr: addr, Handler: h, ReadHeaderTimeout: 5 * time.Second}
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

func serveGame(ctx context.Context, name, addr string, f gamehost.Factory) (*gamehost.Host, error) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	host := gamehost.New(gamehost.Config{Factory: f})
	srv := grpc.NewServer()
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
		if _, err := serveGame(ctx, "room", env("ROOM_ADDR", ":9001"), room.Factory); err != nil {
			return err
		}
	}
	if on("world") {
		sched := timer.New(timer.Config{Owner: env("POD_NAME", "world-0"), Shard: 0, Store: d.timers})
		w := slg.New(slg.Config{Store: d.marches, Scheduler: sched, IDs: d.ids})
		if _, err := serveGame(ctx, "world", env("WORLD_ADDR", ":9002"), w.Factory); err != nil {
			return err
		}
		go func() {
			if err := sched.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
				slog.Error("timer scheduler stopped", "err", err)
				stop() // A fenced node must not keep serving this shard.
			}
		}()
	}
	if on("gateway") {
		g := gateway.New(gateway.Config{Signer: d.signer, Epochs: d.epochs, Resolver: discovery.NewStatic(map[string][]string{
			"room":  strings.Split(env("ROOM_NODES", "127.0.0.1:9001"), ","),
			"world": strings.Split(env("WORLD_NODES", "127.0.0.1:9002"), ","),
		})})
		mux := http.NewServeMux()
		mux.Handle("GET /ws", g)
		serveHTTP(ctx, "gateway", env("GATEWAY_ADDR", ":8080"), mux)
		go func() {
			<-ctx.Done()
			g.Drain(5 * time.Second)
		}()
	}
	<-ctx.Done()
	slog.Info("shutting down")
	time.Sleep(2 * time.Second) // Lets the shutdown goroutines finish.
	return nil
}

func main() {
	if err := run(); err != nil {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}
}
