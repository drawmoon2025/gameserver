package battle

import (
	"math"
	"testing"
)

func TestSameSeedSameFight(t *testing.T) {
	if a, b := Resolve(42, 1000, 900), Resolve(42, 1000, 900); a != b {
		t.Fatalf("%+v and %+v", a, b)
	}
}

func TestOutcomeIsPinned(t *testing.T) {
	// Reports are replayed from their seed, so a change here rewrites history.
	want := Result{Seed: 7, Rounds: 13, AttackerLeft: 274, AttackerWon: true}
	if got := Resolve(7, 500, 500); got != want {
		t.Fatalf("got %+v", got)
	}
}

func TestEveryFightEndsWithALoser(t *testing.T) {
	for seed := range uint64(2000) {
		r := Resolve(seed, int64(seed%50)+1, int64(seed%37)+1)
		if r.AttackerLeft > 0 && r.DefenderLeft > 0 {
			t.Fatalf("no loser: %+v", r)
		}
		if r.AttackerLeft < 0 || r.DefenderLeft < 0 {
			t.Fatalf("negative troops: %+v", r)
		}
		if r.AttackerWon != (r.AttackerLeft > 0) {
			t.Fatalf("wrong winner: %+v", r)
		}
	}
}

func TestLargerArmyUsuallyWins(t *testing.T) {
	wins := 0
	for seed := range uint64(200) {
		if Resolve(seed, 3000, 1000).AttackerWon {
			wins++
		}
	}
	if wins != 200 {
		t.Fatalf("3:1 won %d of 200", wins)
	}
}

func TestHugeArmiesDoNotOverflow(t *testing.T) {
	r := Resolve(1, math.MaxInt64, math.MaxInt64)
	if r.AttackerLeft < 0 || r.DefenderLeft < 0 || r.Rounds == 0 {
		t.Fatalf("%+v", r)
	}
}
