package slg

import (
	"fmt"

	"github.com/yourname/gameserver/internal/battle"
	"github.com/yourname/gameserver/internal/mail"
	"github.com/yourname/gameserver/pkg/timer"
)

// tile is who holds one map cell. A zero value is empty.
type tile struct {
	Owner  int64
	Troops int64
}

// settle applies a march arriving at a tile, as a pure function so that every
// Store does exactly the same thing. The march's seed is its ID, so the fight
// replays identically on a re-fire or from its report. It returns the tile, the
// troops left of this march, and any mail to send.
func settle(m March, t tile) (tile, int64, []mail.Letter) {
	if t.Troops == 0 || t.Owner == m.UID {
		return tile{Owner: m.UID, Troops: t.Troops + m.Troops}, m.Troops, nil
	}
	r := battle.Resolve(m.ID, m.Troops, t.Troops)
	after := tile{Owner: t.Owner, Troops: r.DefenderLeft}
	if r.AttackerWon {
		after = tile{Owner: m.UID, Troops: r.AttackerLeft}
	}
	body := fmt.Sprintf("tile (%d,%d): attacker %d left of %d, defender %d left of %d, %d rounds, seed %d",
		m.ToX, m.ToY, r.AttackerLeft, m.Troops, r.DefenderLeft, t.Troops, r.Rounds, r.Seed)
	report := func(uid int64, won bool, reward int64) mail.Letter {
		title := "Defeat"
		if won {
			title = "Victory"
		}
		return mail.Letter{Key: fmt.Sprintf("battle:%d:%d", m.ID, uid), UID: uid, Kind: "battle",
			Title: title, Body: body, RewardGold: reward}
	}
	var loot int64
	if r.AttackerWon {
		loot = t.Troops // One gold for each defender beaten.
	}
	return after, r.AttackerLeft, []mail.Letter{
		report(m.UID, r.AttackerWon, loot),
		report(t.Owner, !r.AttackerWon, 0),
	}
}

func deliveries(letters []mail.Letter) ([]timer.Job, error) {
	jobs := make([]timer.Job, 0, len(letters))
	for _, l := range letters {
		j, err := mail.DeliveryJob(l)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, j)
	}
	return jobs, nil
}
