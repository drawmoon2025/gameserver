// Package realm is the server list and which kingdom each player is in. It is
// HTTP, like login: a client uses it before it has anywhere to connect. All play
// after that goes over the WebSocket URL it returns.
package realm

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"sync"

	"github.com/go-sql-driver/mysql"
	"google.golang.org/protobuf/proto"

	gamev1 "github.com/yourname/gameserver/gen/game/v1"
	"github.com/yourname/gameserver/pkg/auth"
	"github.com/yourname/gameserver/pkg/session"
)

// MapSize is the width and height of a kingdom's map, in tiles.
const MapSize = 1200

// Rule errors.
var (
	ErrNoServer  = errors.New("realm: no such server")
	ErrFull      = errors.New("realm: server is full")
	ErrAlreadyIn = errors.New("realm: already in another server")
)

// Server is one kingdom on the list.
type Server struct {
	ID         string
	Name       string
	WSURL      string
	Capacity   int
	Population int
}

// Full reports whether new players are refused.
func (s Server) Full() bool { return s.Population >= s.Capacity }

// Member is a player's place in a kingdom. An empty Kingdom means none yet.
type Member struct {
	UID     int64
	Kingdom string
	X, Y    int32
}

// Base picks a player's city position. It is a pure function of its inputs, so
// a retried entry lands in the same place. Two cities may share a tile: a city
// is not on a tile, and only tiles can be fought over.
func Base(uid int64, kingdom string) (x, y int32) {
	h := fnv.New64a()
	fmt.Fprint(h, kingdom, ":", uid)
	v := h.Sum64()
	return int32(v % MapSize), int32((v >> 32) % MapSize)
}

// Store persists the list and memberships.
type Store interface {
	// Put adds a server or updates it, keeping its population.
	Put(ctx context.Context, s Server) error
	List(ctx context.Context) ([]Server, error)
	// MemberOf returns a player's membership; Kingdom is empty if they have none.
	MemberOf(ctx context.Context, uid int64) (Member, error)
	// Enter places a player in a kingdom. Repeating it is harmless, and a
	// player who is already in stays in even if the kingdom has since filled.
	Enter(ctx context.Context, uid int64, kingdom string) (Member, error)
}

// Memory is an in-process Store.
type Memory struct {
	mu      sync.Mutex
	servers map[string]Server
	members map[int64]Member
}

// NewMemory creates an empty store.
func NewMemory() *Memory {
	return &Memory{servers: map[string]Server{}, members: map[int64]Member{}}
}

// Put implements Store.
func (m *Memory) Put(_ context.Context, s Server) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	s.Population = m.servers[s.ID].Population
	m.servers[s.ID] = s
	return nil
}

// List implements Store.
func (m *Memory) List(context.Context) ([]Server, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Server, 0, len(m.servers))
	for _, s := range m.servers {
		out = append(out, s)
	}
	sort.Slice(out, func(a, b int) bool { return out[a].ID < out[b].ID })
	return out, nil
}

// MemberOf implements Store.
func (m *Memory) MemberOf(_ context.Context, uid int64) (Member, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if v, ok := m.members[uid]; ok {
		return v, nil
	}
	return Member{UID: uid}, nil
}

// Enter implements Store.
func (m *Memory) Enter(_ context.Context, uid int64, kingdom string) (Member, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if v, ok := m.members[uid]; ok {
		if v.Kingdom != kingdom {
			return Member{}, ErrAlreadyIn
		}
		return v, nil
	}
	s, ok := m.servers[kingdom]
	if !ok {
		return Member{}, ErrNoServer
	}
	if s.Full() {
		return Member{}, ErrFull
	}
	s.Population++
	m.servers[kingdom] = s
	x, y := Base(uid, kingdom)
	v := Member{UID: uid, Kingdom: kingdom, X: x, Y: y}
	m.members[uid] = v
	return v, nil
}

// MySQL is a Store on one database.
type MySQL struct {
	DB *sql.DB
}

// Put implements Store.
func (m *MySQL) Put(ctx context.Context, s Server) error {
	_, err := m.DB.ExecContext(ctx,
		`INSERT INTO kingdom (id, name, ws_url, capacity) VALUES (?, ?, ?, ?)
		 ON DUPLICATE KEY UPDATE name = VALUES(name), ws_url = VALUES(ws_url), capacity = VALUES(capacity)`,
		s.ID, s.Name, s.WSURL, s.Capacity)
	return err
}

// List implements Store.
func (m *MySQL) List(ctx context.Context) ([]Server, error) {
	rows, err := m.DB.QueryContext(ctx, `SELECT id, name, ws_url, capacity, population FROM kingdom ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Server
	for rows.Next() {
		var s Server
		if err := rows.Scan(&s.ID, &s.Name, &s.WSURL, &s.Capacity, &s.Population); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// MemberOf implements Store.
func (m *MySQL) MemberOf(ctx context.Context, uid int64) (Member, error) {
	v := Member{UID: uid}
	err := m.DB.QueryRowContext(ctx, `SELECT kingdom, x, y FROM player_kingdom WHERE uid = ?`, uid).
		Scan(&v.Kingdom, &v.X, &v.Y)
	if errors.Is(err, sql.ErrNoRows) {
		return v, nil
	}
	return v, err
}

// Enter implements Store. The membership row goes first, so a repeat costs no
// seat. The guarded UPDATE then takes the seat, and holds the kingdom's row until
// commit, so concurrent entries cannot pass the capacity.
func (m *MySQL) Enter(ctx context.Context, uid int64, kingdom string) (Member, error) {
	x, y := Base(uid, kingdom)
	v := Member{UID: uid, Kingdom: kingdom, X: x, Y: y}
	inserted, err := m.enter(ctx, v)
	if err != nil {
		return Member{}, err
	}
	if inserted {
		return v, nil
	}
	cur, err := m.MemberOf(ctx, uid)
	if err != nil {
		return Member{}, err
	}
	if cur.Kingdom != kingdom {
		return Member{}, ErrAlreadyIn
	}
	return cur, nil
}

func (m *MySQL) enter(ctx context.Context, v Member) (bool, error) {
	tx, err := m.DB.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback() //nolint:errcheck // No-op after Commit.
	res, err := tx.ExecContext(ctx, `INSERT INTO player_kingdom (uid, kingdom, x, y) VALUES (?, ?, ?, ?)`,
		v.UID, v.Kingdom, v.X, v.Y)
	var dup *mysql.MySQLError
	if errors.As(err, &dup) && dup.Number == 1062 {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	res, err = tx.ExecContext(ctx, `UPDATE kingdom SET population = population + 1 WHERE id = ? AND population < capacity`, v.Kingdom)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	if n == 0 {
		var one int
		err := tx.QueryRowContext(ctx, `SELECT 1 FROM kingdom WHERE id = ?`, v.Kingdom).Scan(&one)
		if errors.Is(err, sql.ErrNoRows) {
			return false, ErrNoServer
		}
		if err != nil {
			return false, err
		}
		return false, ErrFull
	}
	return true, tx.Commit()
}

// ParseServers reads SERVERS: "id|name|ws_url|capacity" entries separated by
// commas.
func ParseServers(v string) ([]Server, error) {
	var out []Server
	for _, entry := range strings.Split(v, ",") {
		parts := strings.Split(entry, "|")
		if len(parts) != 4 || parts[0] == "" || len(parts[0]) > 32 || parts[2] == "" {
			return nil, fmt.Errorf("realm: bad server entry %q", entry)
		}
		s := Server{ID: parts[0], Name: parts[1], WSURL: parts[2]}
		if _, err := fmt.Sscan(parts[3], &s.Capacity); err != nil || s.Capacity <= 0 {
			return nil, fmt.Errorf("realm: bad capacity in %q", entry)
		}
		out = append(out, s)
	}
	return out, nil
}

// Service is the HTTP API.
type Service struct {
	Store  Store
	Signer *auth.Signer
	Epochs session.Epochs
}

// Routes registers the handlers.
func (s *Service) Routes(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/servers", s.list)
	mux.HandleFunc("POST /v1/servers/enter", s.enter)
}

// uid checks the caller's token and that its login is the newest. It fails
// closed: with Redis down the list is unavailable.
func (s *Service) uid(w http.ResponseWriter, r *http.Request) (int64, bool) {
	token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return 0, false
	}
	claims, err := s.Signer.Verify(token)
	if err != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return 0, false
	}
	cur, err := s.Epochs.Current(r.Context(), claims.UID)
	if err != nil {
		slog.Error("realm epoch check", "uid", claims.UID, "err", err)
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
		return 0, false
	}
	if claims.Epoch != cur {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return 0, false
	}
	return claims.UID, true
}

func info(s Server) *gamev1.ServerInfo {
	out := &gamev1.ServerInfo{Id: s.ID, Name: s.Name, WsUrl: s.WSURL, Full: s.Full()}
	if s.Capacity > 0 {
		out.Load = int32(int64(s.Population) * 100 / int64(s.Capacity))
	}
	return out
}

func write(w http.ResponseWriter, m proto.Message) {
	b, err := proto.Marshal(m)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/x-protobuf")
	_, _ = w.Write(b)
}

func (s *Service) list(w http.ResponseWriter, r *http.Request) {
	uid, ok := s.uid(w, r)
	if !ok {
		return
	}
	servers, err := s.Store.List(r.Context())
	if err != nil {
		slog.Error("list servers", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	me, err := s.Store.MemberOf(r.Context(), uid)
	if err != nil {
		slog.Error("server membership", "uid", uid, "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	out := &gamev1.ServerList{Current: me.Kingdom}
	for _, v := range servers {
		out.Servers = append(out.Servers, info(v))
	}
	write(w, out)
}

func (s *Service) enter(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<10))
	req := &gamev1.EnterServerReq{}
	if err != nil || proto.Unmarshal(body, req) != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	uid, ok := s.uid(w, r)
	if !ok {
		return
	}
	v, err := s.Store.Enter(r.Context(), uid, req.GetId())
	switch {
	case err == nil:
	case errors.Is(err, ErrNoServer):
		http.Error(w, "no such server", http.StatusNotFound)
		return
	case errors.Is(err, ErrFull):
		http.Error(w, "server full", http.StatusConflict)
		return
	case errors.Is(err, ErrAlreadyIn):
		http.Error(w, "already in another server", http.StatusConflict)
		return
	default:
		slog.Error("enter server", "uid", uid, "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	servers, err := s.Store.List(r.Context())
	if err != nil {
		slog.Error("list servers", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	for _, sv := range servers {
		if sv.ID == v.Kingdom {
			write(w, &gamev1.EnterServerRsp{Server: info(sv), BaseX: v.X, BaseY: v.Y})
			return
		}
	}
	http.Error(w, "no such server", http.StatusNotFound)
}
