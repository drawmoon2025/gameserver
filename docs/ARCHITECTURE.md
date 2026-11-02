# Architecture

## Topology

```text
Client ── WSS ──> L4 LB ──> Gateway ── gRPC tunnel ──> Room / SLG services
Client ── HTTPS ─> L7 LB ──> Account, Slots (stateless)

Stores: Redis Cluster (epochs, cache), MySQL shards (truth)
Optional: event_outbox -> Relay -> Kafka (KAFKA_ENABLED)
```

| Game type | Connection | State | Scales by |
|---|---|---|---|
| Room (cards, fishing) | WebSocket | In a Scene actor on one node | Adding nodes; rooms hash to nodes |
| Slots | HTTP | None between requests; truth in wallet | Adding instances |
| SLG | WebSocket | Kingdom actors plus durable timers | Adding service types and kingdoms |

## Rules that hold everywhere

1. **Redis is never the only copy of value.** Balances and SLG progress commit to MySQL.
2. **Anything that moves value has an idempotency key.** Slots use `request_id`; WS requests use `client_seq`; timers use the job ID.
3. **Kafka is side-channel.** It is off by default and core flows do not read from it.
4. **Actors do not share memory.** Cross-actor work uses Tell plus a reply Tell, never nested Ask.
5. **Newer epoch wins.** It is checked at connect, on attach, and on every value-moving HTTP request.

## Reconnect

The game node holds the replay ring, so a gateway crash loses nothing.

1. The client reconnects to any gateway and sends `ConnectReq` with `last_seq` per binding.
2. The gateway checks the token and epoch, then attaches to the same node (same rendezvous hash).
3. The host replies with `ResumeRsp`, then either replays the gap or sends a snapshot.
4. `client_seq` dedupes retried requests.

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
| world | Map, marches, tile occupation | kingdom | **Sample implemented** |
| scheduler | Durable timers | shard | **`pkg/timer` implemented** |
| city | Buildings, research, resources | uid | Planned |
| alliance | Membership, rallies, territory | alliance id | Planned |
| battle | Stateless combat resolution | none | Planned |
| mail | Reports, rewards | uid | Planned |
| chat | Kingdom and alliance channels | channel | Planned |
| rank | Leaderboards (Redis sorted sets) | board | Planned |

One socket reaches all of them: `ConnectReq` lists bindings, and each binding is a channel
with its own `seq`. Clients set `channel` on every frame. The gateway sends the frame to
that binding after checking that its `msg_id` range belongs to the binding's service. Add a
service by giving it a range in `gateway.DefaultRoutes`.

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
  -> battle: compute result (pure function, seeded, replayable)
  -> world: apply result (guarded by march id)
  -> outbox -> city (losses), mail (report), rank (score)
```

Downstream steps are at-least-once and idempotent by march ID. There are no distributed
transactions.

### Scale notes

- A kingdom is a natural shard. A very busy kingdom splits into zone actors.
- Clients subscribe to a viewport (9 grid cells), never to the whole map.
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
