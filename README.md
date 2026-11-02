# gameserver

Go game server framework: WebSocket room and SLG games, HTTP slots, Protobuf,
Redis, MySQL and optional Kafka.

**Status: early.** Tests use in-memory stores; nothing has been load tested.

Design and known gaps: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Verify

Install [Go](https://go.dev/dl/) 1.25+ and
[protoc](https://github.com/protocolbuffers/protobuf/releases), then:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/verify.ps1   # Windows
```

```bash
bash scripts/verify.sh                                        # Linux/macOS
```

In China, first run `go env -w GOPROXY=https://goproxy.cn,direct`.

The script generates `gen/`, then builds, vets and tests. Tests use in-memory
stores and need no Docker, so the MySQL and Kafka code is compiled but not
exercised.

Dependency and protoc plugin versions are pinned and the script uses the installed
Go (`GOTOOLCHAIN=local`). If `go.sum` is missing, the script creates it. Commit
`go.mod`, `go.sum` and `gen/` afterwards. Once `go.sum` exists, the script fails
if the module files are out of date.

## Run locally

```powershell
$env:JWT_KEY = "dev-only-key-at-least-32-bytes-long!"
go run ./cmd/server
```

With no `MYSQL_DSNS` or `REDIS_ADDR`, all roles run in one process on in-memory
stores. To use real stores, run `docker compose -f deploy/docker-compose.yml up -d`
and set those variables.

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `ROLE` | `all` | `all`, `account`, `gateway`, `slots`, `room` or `world` |
| `JWT_KEY` | required | Signing key, 32+ bytes |
| `NODE_ID` | `0` | Unique per process, 0-1023 |
| `MYSQL_DSNS` | memory | Comma-separated, one per physical shard |
| `REDIS_ADDR` | memory | Comma-separated addresses |
| `KAFKA_ENABLED` | `false` | Forward the outbox to Kafka |
| `KAFKA_BROKERS` | `localhost:9092` | Comma-separated |
| `THIRD_PARTY_AUTH_URL` | unset | Enables the third-party provider |
| `ROOM_NODES`, `WORLD_NODES` | `127.0.0.1:9001`, `:9002` | Game nodes, for gateways |
