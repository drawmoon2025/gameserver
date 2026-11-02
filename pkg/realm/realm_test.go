package realm

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	gamev1 "github.com/drawmoon2025/gameserver/gen/game/v1"
	"github.com/drawmoon2025/gameserver/pkg/auth"
	"github.com/drawmoon2025/gameserver/pkg/session"
)

func TestBaseIsOnTheMapAndRepeatable(t *testing.T) {
	for uid := range int64(200) {
		x, y := Base(uid, "k1")
		if x < 0 || x >= MapSize || y < 0 || y >= MapSize {
			t.Fatalf("uid %d at (%d,%d)", uid, x, y)
		}
		if x2, y2 := Base(uid, "k1"); x2 != x || y2 != y {
			t.Fatalf("uid %d moved", uid)
		}
	}
}

func TestEnterRules(t *testing.T) {
	ctx := context.Background()
	m := NewMemory()
	for _, s := range []Server{{ID: "a", Capacity: 1}, {ID: "b", Capacity: 5}} {
		if err := m.Put(ctx, s); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := m.Enter(ctx, 1, "nope"); !errors.Is(err, ErrNoServer) {
		t.Fatalf("unknown server: %v", err)
	}
	first, err := m.Enter(ctx, 1, "a")
	if err != nil {
		t.Fatal(err)
	}
	// Now full, yet its member must still get back in.
	if again, err := m.Enter(ctx, 1, "a"); err != nil || again != first {
		t.Fatalf("repeat %+v: %v", again, err)
	}
	if _, err := m.Enter(ctx, 2, "a"); !errors.Is(err, ErrFull) {
		t.Fatalf("full server: %v", err)
	}
	if _, err := m.Enter(ctx, 1, "b"); !errors.Is(err, ErrAlreadyIn) {
		t.Fatalf("second server: %v", err)
	}
	list, err := m.List(ctx)
	if err != nil || list[0].Population != 1 || list[1].Population != 0 {
		t.Fatalf("list %+v: %v", list, err)
	}
	if err := m.Put(ctx, Server{ID: "a", Capacity: 9}); err != nil {
		t.Fatal(err)
	}
	if list, _ := m.List(ctx); list[0].Population != 1 {
		t.Fatalf("reseeding reset the population: %+v", list[0])
	}
}

func TestParseServers(t *testing.T) {
	got, err := ParseServers("k1|One|wss://a/ws|100,k2|Two|wss://b/ws|5")
	if err != nil || len(got) != 2 || got[1].ID != "k2" || got[1].WSURL != "wss://b/ws" || got[1].Capacity != 5 {
		t.Fatalf("%+v: %v", got, err)
	}
	for _, bad := range []string{"", "k1|One|wss://a/ws", "k1|One|wss://a/ws|0", "k1|One||5", "|One|wss://a/ws|5"} {
		if _, err := ParseServers(bad); err == nil {
			t.Errorf("accepted %q", bad)
		}
	}
}

func TestHTTPListAndEnter(t *testing.T) {
	ctx := context.Background()
	store := NewMemory()
	if err := store.Put(ctx, Server{ID: "k1", Name: "One", WSURL: "wss://a/ws", Capacity: 10}); err != nil {
		t.Fatal(err)
	}
	signer := &auth.Signer{Key: bytes.Repeat([]byte("k"), 32), TTL: time.Hour}
	epochs := session.NewMemory()
	mux := http.NewServeMux()
	(&Service{Store: store, Signer: signer, Epochs: epochs}).Routes(mux)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	do := func(method, path, token string, body proto.Message) (int, []byte) {
		t.Helper()
		var payload []byte
		if body != nil {
			var err error
			if payload, err = proto.Marshal(body); err != nil {
				t.Fatal(err)
			}
		}
		req, err := http.NewRequest(method, srv.URL+path, bytes.NewReader(payload))
		if err != nil {
			t.Fatal(err)
		}
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		rsp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer rsp.Body.Close() //nolint:errcheck // Best-effort.
		out, err := io.ReadAll(rsp.Body)
		if err != nil {
			t.Fatal(err)
		}
		return rsp.StatusCode, out
	}

	if code, _ := do(http.MethodGet, "/v1/servers", "", nil); code != http.StatusUnauthorized {
		t.Fatalf("no token: %d", code)
	}
	epoch, err := epochs.Next(ctx, 7)
	if err != nil {
		t.Fatal(err)
	}
	old, _, err := signer.Issue(7, "a", epoch)
	if err != nil {
		t.Fatal(err)
	}
	if epoch, err = epochs.Next(ctx, 7); err != nil {
		t.Fatal(err)
	}
	token, _, err := signer.Issue(7, "b", epoch)
	if err != nil {
		t.Fatal(err)
	}
	if code, _ := do(http.MethodGet, "/v1/servers", old, nil); code != http.StatusUnauthorized {
		t.Fatalf("superseded token: %d", code)
	}

	list := func() *gamev1.ServerList {
		t.Helper()
		code, body := do(http.MethodGet, "/v1/servers", token, nil)
		out := &gamev1.ServerList{}
		if err := proto.Unmarshal(body, out); code != http.StatusOK || err != nil {
			t.Fatalf("list %d: %v", code, err)
		}
		return out
	}
	if l := list(); l.GetCurrent() != "" || len(l.GetServers()) != 1 || l.GetServers()[0].GetWsUrl() != "wss://a/ws" {
		t.Fatalf("first list %v", l)
	}
	if code, _ := do(http.MethodPost, "/v1/servers/enter", token, &gamev1.EnterServerReq{Id: "nope"}); code != http.StatusNotFound {
		t.Fatalf("unknown server: %d", code)
	}
	code, body := do(http.MethodPost, "/v1/servers/enter", token, &gamev1.EnterServerReq{Id: "k1"})
	rsp := &gamev1.EnterServerRsp{}
	if err := proto.Unmarshal(body, rsp); code != http.StatusOK || err != nil {
		t.Fatalf("enter %d: %v", code, err)
	}
	x, y := Base(7, "k1")
	if rsp.GetBaseX() != x || rsp.GetBaseY() != y || rsp.GetServer().GetId() != "k1" {
		t.Fatalf("enter %v", rsp)
	}
	if l := list(); l.GetCurrent() != "k1" || l.GetServers()[0].GetLoad() != 10 {
		t.Fatalf("after entry %v", l)
	}
}
