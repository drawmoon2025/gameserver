// Package room is a minimal room game: a shared counter. It exists to exercise
// the host. Copy it to start a real room game.
package room

import (
	"google.golang.org/protobuf/proto"

	gamev1 "github.com/drawmoon2025/gameserver/gen/game/v1"
	"github.com/drawmoon2025/gameserver/pkg/gamehost"
)

// Room is one scene. Fields are actor-only.
type Room struct {
	key   string
	ctx   *gamehost.SceneContext
	value int64
}

// Factory is a gamehost.Factory.
func Factory(key string, ctx *gamehost.SceneContext) gamehost.Scene {
	return &Room{key: key, ctx: ctx}
}

// OnJoin implements gamehost.Scene.
func (*Room) OnJoin(*gamehost.Player) {}

// OnDisconnect implements gamehost.Scene.
func (*Room) OnDisconnect(*gamehost.Player) {}

// OnReconnect implements gamehost.Scene.
func (*Room) OnReconnect(*gamehost.Player) {}

// OnExpire implements gamehost.Scene.
func (*Room) OnExpire(*gamehost.Player) {}

// OnRetire implements gamehost.Scene.
func (*Room) OnRetire() {}

// Snapshot implements gamehost.Scene.
func (r *Room) Snapshot(*gamehost.Player) (gamev1.MsgId, proto.Message, error) {
	s := &gamev1.RoomSnapshot{RoomId: r.key, Value: r.value}
	for uid := range r.ctx.Players() {
		s.Members = append(s.Members, uid)
	}
	return gamev1.MsgId_MSG_ID_ROOM_SNAPSHOT, s, nil
}

// OnMessage implements gamehost.Scene.
func (r *Room) OnMessage(p *gamehost.Player, env *gamev1.Envelope) gamehost.Reply {
	if env.GetMsgId() != uint32(gamev1.MsgId_MSG_ID_ROOM_ACTION_REQ) {
		return gamehost.Reply{Code: gamev1.ErrorCode_ERROR_CODE_BAD_REQUEST}
	}
	req := &gamev1.RoomActionReq{}
	if proto.Unmarshal(env.GetPayload(), req) != nil {
		return gamehost.Reply{Code: gamev1.ErrorCode_ERROR_CODE_BAD_REQUEST}
	}
	r.value += int64(req.GetDelta())
	rsp := &gamev1.RoomActionRsp{Value: r.value}
	for uid, other := range r.ctx.Players() {
		if uid != p.UID {
			other.Push(gamev1.MsgId_MSG_ID_ROOM_ACTION_RSP, rsp)
		}
	}
	return gamehost.Reply{MsgID: gamev1.MsgId_MSG_ID_ROOM_ACTION_RSP, Msg: rsp}
}
