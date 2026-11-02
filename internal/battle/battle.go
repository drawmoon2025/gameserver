// Package battle resolves combat. It is a pure function of its inputs, so it is
// a library rather than a service: it needs no network hop, any node can run
// it, and a report can be replayed from its seed.
package battle

import "math/rand/v2"

// Result is the outcome of one fight.
type Result struct {
	Seed         uint64
	Rounds       int
	AttackerLeft int64
	DefenderLeft int64
	AttackerWon  bool // False on a mutual wipe-out: the attacker took nothing.
}

// Resolve fights until a side is gone. Each round both sides hit at once for 5%
// to 15% of their strength, and at least one, so every fight ends.
func Resolve(seed uint64, attacker, defender int64) Result {
	rng := rand.New(rand.NewPCG(seed, seed))
	r := Result{Seed: seed, AttackerLeft: attacker, DefenderLeft: defender}
	for r.AttackerLeft > 0 && r.DefenderLeft > 0 {
		r.Rounds++
		toDefender := hit(rng, r.AttackerLeft)
		toAttacker := hit(rng, r.DefenderLeft)
		r.DefenderLeft = max(r.DefenderLeft-toDefender, 0)
		r.AttackerLeft = max(r.AttackerLeft-toAttacker, 0)
	}
	r.AttackerWon = r.AttackerLeft > 0
	return r
}

func hit(rng *rand.Rand, strength int64) int64 {
	percent := 5 + rng.Int64N(11)
	// Dividing first keeps a huge army from overflowing the multiplication.
	return max(strength/100*percent+strength%100*percent/100, 1)
}
