// Package account is the HTTP login service.
package account

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	gamev1 "github.com/drawmoon2025/gameserver/gen/game/v1"
	"github.com/drawmoon2025/gameserver/pkg/auth"
	"github.com/drawmoon2025/gameserver/pkg/idgen"
	"github.com/drawmoon2025/gameserver/pkg/session"
)

const maxBody = 4 << 10

// Service issues tokens for any registered provider.
type Service struct {
	Local     *auth.Local
	Providers map[string]auth.Provider
	Accounts  auth.Accounts
	Signer    *auth.Signer
	Epochs    session.Epochs
	IDs       *idgen.Generator
}

// Routes returns the HTTP handlers.
func (s *Service) Routes(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/register", s.register)
	mux.HandleFunc("POST /v1/login", s.login)
}

func read(w http.ResponseWriter, r *http.Request, m proto.Message) bool {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBody))
	if err != nil || proto.Unmarshal(body, m) != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return false
	}
	return true
}

func (s *Service) register(w http.ResponseWriter, r *http.Request) {
	req := &gamev1.LoginReq{}
	if !read(w, r, req) {
		return
	}
	switch err := s.Local.Register(r.Context(), req.GetUsername(), req.GetCredential()); {
	case err == nil:
		w.WriteHeader(http.StatusCreated)
	case errors.Is(err, auth.ErrUsernameTaken):
		http.Error(w, "username taken", http.StatusConflict)
	case errors.Is(err, auth.ErrBadCredential):
		http.Error(w, "invalid username or password", http.StatusBadRequest)
	default:
		slog.Error("register", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
	}
}

func (s *Service) login(w http.ResponseWriter, r *http.Request) {
	req := &gamev1.LoginReq{}
	if !read(w, r, req) {
		return
	}
	rsp, err := s.Login(r.Context(), req)
	switch {
	case err == nil:
	case errors.Is(err, auth.ErrBadCredential), errors.Is(err, auth.ErrUnknownProvider):
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	default:
		slog.Error("login", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	b, err := proto.Marshal(rsp)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/x-protobuf")
	_, _ = w.Write(b)
}

// Login verifies a credential and issues a token with a new epoch.
func (s *Service) Login(ctx context.Context, req *gamev1.LoginReq) (*gamev1.LoginRsp, error) {
	p, ok := s.Providers[req.GetProvider()]
	if !ok {
		return nil, auth.ErrUnknownProvider
	}
	id, err := p.Verify(ctx, auth.Credential{Username: req.GetUsername(), Secret: req.GetCredential()})
	if err != nil {
		return nil, err
	}
	uid, err := s.Accounts.Resolve(ctx, id, s.IDs.Next())
	if err != nil {
		return nil, err
	}
	epoch, err := s.Epochs.Next(ctx, uid)
	if err != nil {
		return nil, err
	}
	token, exp, err := s.Signer.Issue(uid, uuid.NewString(), epoch)
	if err != nil {
		return nil, err
	}
	return &gamev1.LoginRsp{Token: token, Uid: uid, ExpiresAtMs: exp.UnixMilli()}, nil
}
