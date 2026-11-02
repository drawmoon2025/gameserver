// Package rank serves leaderboards. One scene per board. Scores are derived
// data, kept in Redis sorted sets. Other services add to a score by committing
// ScoreJob in their own transaction.
package rank

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"

	gamev1 "github.com/drawmoon2025/gameserver/gen/game/v1"
	"github.com/drawmoon2025/gameserver/pkg/gamehost"
	"github.com/drawmoon2025/gameserver/pkg/discovery"
	"github.com/drawmoon2025/gameserver/pkg/timer"
)

// ShardBase places this service's timer shards after the mail service's.
const ShardBase = 3000

// KindScore is the durable timer kind for a score change.
const KindScore = "rank.score"

const (
	topSize  = 100
	topCache = time.Second
	doneTTL  = 7 * 24 * time.Hour // Longer than any job can stay undelivered.
)

// Score adds Delta to UID's score once per Key.
type Score struct {
	Board string `json:"board"`
	UID   int64  `json:"uid"`
	Delta int64  `json:"delta"`
	Key   string `json:"key"`
}

// Entry is one row of a board. Rank starts at 1; 0 means not on the board.
type Entry struct {
	UID   int64
	Score int64
	Rank  int64
}

// ScoreJob builds the durable job that applies s.
func ScoreJob(s Score) (timer.Job, error) {
	p, err := json.Marshal(s)
	if err != nil {
		return timer.Job{}, err
	}
	return timer.Job{ID: timer.KeyedID("rank:" + s.Board + ":" + s.Key), Shard: ShardBase + discovery.Slot(s.Board),
		Kind: KindScore, Payload: p, FireAt: time.Now()}, nil
}

// Store persists boards.
type Store interface {
	Add(ctx context.Context, s Score) error
	Top(ctx context.Context, board string, n int) ([]Entry, error)
	Position(ctx context.Context, board string, uid int64) (Entry, error)
}

// Memory is an in-process Store.
type Memory struct {
	mu     sync.Mutex
	boards map[string]map[int64]int64
	done   map[string]bool
}

// NewMemory creates an empty store.
func NewMemory() *Memory {
	return &Memory{boards: map[string]map[int64]int64{}, done: map[string]bool{}}
}

// Add implements Store.
func (m *Memory) Add(_ context.Context, s Score) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.done[s.Board+"\x00"+s.Key] {
		return nil
	}
	m.done[s.Board+"\x00"+s.Key] = true
	if m.boards[s.Board] == nil {
		m.boards[s.Board] = map[int64]int64{}
	}
	m.boards[s.Board][s.UID] += s.Delta
	return nil
}

func (m *Memory) sorted(board string) []Entry {
	out := make([]Entry, 0, len(m.boards[board]))
	for uid, score := range m.boards[board] {
		out = append(out, Entry{UID: uid, Score: score})
	}
	sort.Slice(out, func(a, b int) bool {
		if out[a].Score != out[b].Score {
			return out[a].Score > out[b].Score
		}
		return out[a].UID > out[b].UID
	})
	for i := range out {
		out[i].Rank = int64(i + 1)
	}
	return out
}

// Top implements Store.
func (m *Memory) Top(_ context.Context, board string, n int) ([]Entry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := m.sorted(board)
	if len(out) > n {
		out = out[:n]
	}
	return out, nil
}

// Position implements Store.
func (m *Memory) Position(_ context.Context, board string, uid int64) (Entry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, e := range m.sorted(board) {
		if e.UID == uid {
			return e, nil
		}
	}
	return Entry{UID: uid}, nil
}

// Redis keeps boards in sorted sets. The hash tag keeps a board and its dedupe
// keys on one cluster slot, which the script requires.
type Redis struct {
	Client redis.UniversalClient
}

func boardKey(board string) string { return "rank:{" + board + "}" }

// addOnce makes the dedupe mark and the increment one atomic step.
var addOnce = redis.NewScript(`
if redis.call('SET', KEYS[2], '1', 'NX', 'EX', ARGV[1]) then
  redis.call('ZINCRBY', KEYS[1], ARGV[2], ARGV[3])
end
return 1`)

// Add implements Store.
func (r *Redis) Add(ctx context.Context, s Score) error {
	keys := []string{boardKey(s.Board), boardKey(s.Board) + ":done:" + s.Key}
	if err := addOnce.Run(ctx, r.Client, keys, int(doneTTL/time.Second), s.Delta, s.UID).Err(); err != nil {
		return fmt.Errorf("rank: add: %w", err)
	}
	return nil
}

// Top implements Store.
func (r *Redis) Top(ctx context.Context, board string, n int) ([]Entry, error) {
	zs, err := r.Client.ZRevRangeWithScores(ctx, boardKey(board), 0, int64(n-1)).Result()
	if err != nil {
		return nil, fmt.Errorf("rank: top: %w", err)
	}
	out := make([]Entry, 0, len(zs))
	for i, z := range zs {
		var uid int64
		if _, err := fmt.Sscan(fmt.Sprint(z.Member), &uid); err != nil {
			return nil, fmt.Errorf("rank: bad member %v: %w", z.Member, err)
		}
		out = append(out, Entry{UID: uid, Score: int64(z.Score), Rank: int64(i + 1)})
	}
	return out, nil
}

// Position implements Store.
func (r *Redis) Position(ctx context.Context, board string, uid int64) (Entry, error) {
	member := fmt.Sprint(uid)
	pos, err := r.Client.ZRevRank(ctx, boardKey(board), member).Result()
	if errors.Is(err, redis.Nil) {
		return Entry{UID: uid}, nil
	}
	if err != nil {
		return Entry{}, fmt.Errorf("rank: position: %w", err)
	}
	score, err := r.Client.ZScore(ctx, boardKey(board), member).Result()
	if err != nil {
		return Entry{}, fmt.Errorf("rank: score: %w", err)
	}
	return Entry{UID: uid, Score: int64(score), Rank: pos + 1}, nil
}

// Config wires the service.
type Config struct {
	Store  Store
	Timers *timer.Group
	Now    func() time.Time
}

// Service creates board scenes and applies score jobs.
type Service struct {
	cfg Config
}

// New creates a Service.
func New(cfg Config) *Service {
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	s := &Service{cfg: cfg}
	cfg.Timers.Handle(KindScore, func(ctx context.Context, j timer.Job) error {
		var sc Score
		if err := json.Unmarshal(j.Payload, &sc); err != nil {
			return fmt.Errorf("rank: bad score in job %d: %w", j.ID, err)
		}
		return s.cfg.Store.Add(ctx, sc)
	})
	return s
}

// Factory is a gamehost.Factory.
func (s *Service) Factory(key string, _ *gamehost.SceneContext) gamehost.Scene {
	return &Board{key: key, svc: s}
}

// Board is one leaderboard. It caches the top rows, so a popular board costs
// Redis one read per second however many players look at it.
type Board struct {
	key     string
	svc     *Service
	top     []Entry
	topFrom time.Time
}

// OnJoin implements gamehost.Scene.
func (*Board) OnJoin(*gamehost.Player) {}

// OnDisconnect implements gamehost.Scene.
func (*Board) OnDisconnect(*gamehost.Player) {}

// OnReconnect implements gamehost.Scene.
func (*Board) OnReconnect(*gamehost.Player) {}

// OnExpire implements gamehost.Scene.
func (*Board) OnExpire(*gamehost.Player) {}

// OnRetire implements gamehost.Scene.
func (*Board) OnRetire() {}

func (b *Board) view(uid int64) (*gamev1.RankBoard, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	now := b.svc.cfg.Now()
	if b.top == nil || now.Sub(b.topFrom) >= topCache {
		top, err := b.svc.cfg.Store.Top(ctx, b.key, topSize)
		if err != nil {
			return nil, err
		}
		b.top, b.topFrom = top, now
	}
	self, err := b.svc.cfg.Store.Position(ctx, b.key, uid)
	if err != nil {
		return nil, err
	}
	msg := &gamev1.RankBoard{Board: b.key, SelfRank: self.Rank, SelfScore: self.Score}
	for _, e := range b.top {
		msg.Entries = append(msg.Entries, &gamev1.RankEntry{Uid: e.UID, Score: e.Score, Rank: e.Rank})
	}
	return msg, nil
}

// Snapshot implements gamehost.Scene.
func (b *Board) Snapshot(p *gamehost.Player) (gamev1.MsgId, proto.Message, error) {
	msg, err := b.view(p.UID)
	return gamev1.MsgId_MSG_ID_RANK_SNAPSHOT, msg, err
}

// OnMessage implements gamehost.Scene.
func (b *Board) OnMessage(p *gamehost.Player, env *gamev1.Envelope) gamehost.Reply {
	if env.GetMsgId() != uint32(gamev1.MsgId_MSG_ID_RANK_QUERY_REQ) {
		return gamehost.Reply{Code: gamev1.ErrorCode_ERROR_CODE_BAD_REQUEST}
	}
	msg, err := b.view(p.UID)
	if err != nil {
		slog.Error("rank query", "board", b.key, "err", err)
		return gamehost.Reply{Code: gamev1.ErrorCode_ERROR_CODE_INTERNAL}
	}
	return gamehost.Reply{MsgID: gamev1.MsgId_MSG_ID_RANK_QUERY_RSP, Msg: msg}
}
