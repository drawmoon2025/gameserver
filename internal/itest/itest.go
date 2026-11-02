// Package itest connects integration tests to the stores in
// deploy/docker-compose.yml. Those tests carry the "integration" build tag:
//
//	docker compose -f deploy/docker-compose.yml --profile kafka up -d --wait
//	go test -tags integration -p 1 ./...
package itest

import (
	"database/sql"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql" // Registers the driver.
)

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// RedisAddr returns the test Redis address.
func RedisAddr() string { return env("TEST_REDIS_ADDR", "127.0.0.1:6379") }

// KafkaBroker returns the test Kafka broker.
func KafkaBroker() string { return env("TEST_KAFKA_BROKER", "127.0.0.1:9092") }

// EtcdEndpoints returns the test etcd endpoints.
func EtcdEndpoints() []string {
	return strings.Split(env("TEST_ETCD_ENDPOINTS", "127.0.0.1:2379"), ",")
}

// MySQL opens the test database with the schema already applied.
func MySQL(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("mysql", env("TEST_MYSQL_DSN", "root:dev@tcp(127.0.0.1:3306)/game?parseTime=true"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() }) //nolint:errcheck // Best-effort.
	if err := db.Ping(); err != nil {
		t.Fatal(err)
	}
	return db
}

var counter atomic.Int64

// ID returns an ID no earlier run used, so tests can share a database.
func ID() int64 { return time.Now().UnixMilli()*1000 + counter.Add(1)%1000 }
