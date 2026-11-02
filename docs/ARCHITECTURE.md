# Architecture

## Topology

```text
Client ── WSS ──> L4 LB ──> Gateway ── gRPC tunnel ──> Room / SLG services
Client ── HTTPS ─> L7 LB ──> Account (login, server list), Slots (stateless)

Stores: Redis Cluster (epochs, cache), MySQL shards (truth)
Optional: event_outbox -> Relay -> Kafka (KAFKA_ENABLED)
```

| Game type | Connection | State | Scales by |
|---|---|---|---|
| Room (cards, fishing) | WebSocket | In a Scene actor on one node | Adding nodes; rooms hash to nodes |
| Slots | HTTP | None between requests; truth in wallet | Adding instances |
| SLG | WebSocket | Kingdom actors plus durable timers | Adding service types and kingdoms |

An SLG client uses HTTP only until it has a server: `POST /v1/login`, `GET /v1/servers`,
`POST /v1/servers/enter`. It then opens the returned `ws_url`, and all play goes over that
socket. Slots is the exception by design: it is HTTP throughout, and a spin's result is in
its response, so it has no push channel.

## Rules that hold everywhere

1. **Redis is never the only copy of value.** Balances and SLG progress commit to MySQL.
2. **Anything that moves value has an idempotency key.** Slots use `request_id`; WS requests use `client_seq`; timers use the job ID.
3. **Kafka is side-channel.** It is off by default and core flows do not read from it.
4. **Actors do not share memory.** Cross-actor work uses Tell plus a reply Tell, never nested Ask.
5. **Newer epoch wins.** It is checked at connect, on attach, and on every value-moving HTTP request. A login is announced to gateways, which close older sockets at once; each socket also re-checks every few minutes in case the announcement was missed, and closes when its token expires.
6. **A durable effect needs a durable key.** `client_seq` lives in memory and dies with the seat. Anything that moves value or progress must also carry a `request_id` with a unique key, or be safe to repeat by its nature. A repeated chat line is tolerated.
7. **Rights are checked on every attach.** A client can resume a seat it held before losing the right to it, so a scene that restricts who enters implements `gamehost.Gate`. A check in `Snapshot` would be skipped by a replay.

## Reconnect

The game node holds the replay ring, so a gateway crash loses nothing.

1. The client reconnects to any gateway and sends `ConnectReq` with `last_seq` per binding.
2. The gateway checks the token and epoch, then attaches to the same node (same rendezvous hash).
3. The host replies with `ResumeRsp`, then either replays the gap or sends a snapshot.
4. `client_seq` dedupes retried requests.

A new seat's `seq` starts from the clock, so an old `last_seq` can never match it and the
client gets a snapshot. A new login resets `client_seq` dedupe, since it may be another device.

A binding's position may change between connections, so `last_seq` belongs to the service
and key, not to the channel number. Replayed frames are re-stamped with the current channel.

A player's seat in a scene has one socket: the newest. Every message between gateway and
host names its socket, so a socket only receives its own scenes' frames, and a replaced
socket cannot act or detach its successor. The replaced socket is closed.

Clients must reconnect with exponential backoff and jitter. `Gateway.Drain` spreads
disconnects over a window, so a rolling deploy does not synchronize them.

## Scene ownership

A scene key hashes to one of 256 slots, a permanent number. Nodes own slots rather than
scenes, so etcd traffic does not grow with the number of rooms. Timer shards are the same
slots, so a kingdom's timers run on the node hosting it.

- A node registers under a lease and works out which slots rendezvous hashing gives it.
  It claims a free slot with an etcd transaction, so a slot has one key and one owner.
- Dead node: its lease expires, so its slots free up and their new owners claim them.
  Clients reconnect and find the new owner. This takes up to one TTL (10s by default).
- New node: an existing owner gives up a slot once the slot is idle, or after `MaxDrain`
  (10 minutes). Live rooms are not moved unless that wait runs out.
- Partition: a node that loses etcd sees its lease end, stops serving and exits. Timers
  are also fenced by token, so a stale owner cannot complete jobs.
- Gateways poll the slot table. A stale answer is safe: a node refuses scenes it does not
  own, and the client reconnects.

A scene ends when its last player's grace period ends. Scene memory is therefore not
durable: anything that must outlive it belongs in a store.

Without etcd, `discovery.Static` works, but each stateful service must run as one node.

## SLG

SLG differs from a room game in three ways: the world runs with nobody connected,
actions take hours or days, and one player touches many services at once.

### Services

| Service | Owns | Scene key | Status |
|---|---|---|---|
| realm | Server list, which kingdom a player is in | none (HTTP) | **Implemented** |
| world | Map, marches, tile occupation | kingdom | **Implemented** |
| scheduler | Durable timers | shard | **`pkg/timer` implemented** |
| city | Buildings, gold, troops at home | uid | **Implemented** |
| alliance | Membership, leader, size limit | alliance name | **Implemented** (no rallies or territory yet) |
| battle | Stateless combat resolution | none | **Library, `internal/battle`** |
| mail | Reports, rewards | uid | **Implemented** |
| chat | Kingdom and alliance channels | channel | **Implemented** |
| rank | Leaderboards (Redis sorted sets) | board | **Implemented** |

One socket reaches all of them: `ConnectReq` lists bindings, and each binding is a channel
with its own `seq`. Clients set `channel` on every frame. The gateway sends the frame to
that binding after checking that its `msg_id` range belongs to the binding's service. Add a
service by giving it a range in `gateway.DefaultRoutes`.

Battle is a library, not a service: it is a pure function, so a network hop would add
latency and a failure mode and buy nothing. City and mail are per-player scenes keyed by
uid. The client names a binding's key, so they refuse any key that is not its own uid.

### Troops

A troop is in exactly one place: a city, a march, or a tile. Training adds to the city. A
march moves troops from the city, and a recall moves them from a tile back to it. Battle is
the only thing that removes them. The server decides every count, so a client cannot march
what it does not have.

Taking troops must be synchronous, so a march takes them from `city_state` in its own
transaction rather than through a job. That is why these tables share a database.

### Timers

Every long action follows one sequence:

```text
Validate -> write state + timer -> ack client
                ... hours later, on any node ...
Timer fires -> conditional state change (exactly once) -> notify if online
```

- Delivery is at-least-once, so the effect must be a guarded write such as
  `UPDATE ... WHERE state = 0`. A re-fire then changes nothing.
- `actor.After` is for volatile timers only (a turn countdown). It dies with the process.
- A shard has one owner. `Claim` issues a fencing token, and a stale owner's writes fail,
  which stops a paused node from deleting work its successor owns.
- After downtime, overdue jobs fire immediately on the first scan.
- A march and its timer commit in one transaction, so a march cannot exist without a
  timer. This requires `timer_job` to be in the same database as the state it completes.

### Cross-service flow: an attack

```text
world: march arrives -> tile is hostile
  -> battle: compute result (pure function, seeded by march id)
  -> world: commit march state, tile, and a delivery job per report
  -> mail: store letter once per key; player claims reward
  -> mail: commit claimed flag and a grant job
  -> city: add gold once per key
  -> rank: add score once per key (in the world's commit)
```

Services hand work over through `timer_job`, written in the sender's own transaction, so
nothing is lost between commits and Kafka stays optional. Each step is at-least-once and
made exactly-once by a guard or unique key at the receiver. There are no distributed
transactions. Each service's shards start at its own `ShardBase`, keeping their jobs apart.

### Scale notes

- A kingdom is a natural shard. A very busy kingdom splits into zone actors.
- A client asks for the map around a point, 21 by 21 tiles at a time. This is a read, not a
  subscription: nothing is pushed when another player's army moves.
- World ticks must stay short; heavy work moves out of the actor.

## Capacity

The figures below are planning assumptions, not measurements. Nothing here has been
load tested.

- Gateway: assume 200k connections per node, which gives about 50 nodes for 10M online.
- Connection memory is the first limit; `coder/websocket` runs two goroutines per socket.
  If profiling shows that dominating, swap the transport for an epoll-based one.

## Known gaps

- Node failure, or `MaxDrain` running out, loses in-memory scene state. Rooms need a
  checkpoint strategy per game.
- During a partition two nodes can host one scene for up to one lease TTL. Durable state
  is protected by guarded writes; a room's in-memory state on the losing node is discarded.
- No metrics, tracing, GM tools, or config hot reload yet.
- Rate limits exist per connection only, not per IP or per account.
- Gateway-to-backend gRPC is plaintext; use mTLS or a private network.
- The slot math is sample data, not a certified model.
- A chat channel pushes to every seated player on one actor, so a very large kingdom
  channel needs fan-out split across actors.
- A player is in one kingdom for good; there is no transfer or merge.
- A city's position is a hash, so two cities can share coordinates. Cities cannot be
  attacked; only tiles can.
- Other players see an army only once it holds a tile.
- Rank scores live only in Redis. They are derived data, but no job rebuilds them yet.
- All SLG tables are on the first database. The transactions above need `timer_job` beside
  the state they change, so sharding SLG means per-database timer tables.
- Scene backlog can delay a city or mailbox: stores are called on the actor, up to 2s each.
- A Redis failover can lose a recent epoch. Tokens from the lost login then fail, and the
  previous one works again until the next login. Avoiding this needs `WAIT` or a durable epoch.
- A clock stepped back across a process restart can repeat IDs. Node IDs are unique among
  live processes, but the high-water mark is not persisted.
- Every `slots` instance relays the outbox. Delivery stays at-least-once, but with multiple
  instances duplicates are routine and order is not preserved. Consumers must dedupe.
- A socket that closes for token expiry must log in again; there is no token refresh.
