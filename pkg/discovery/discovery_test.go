package discovery

import (
	"fmt"
	"testing"
)

func TestSlotIsPermanent(t *testing.T) {
	// Timer jobs are stored under these numbers, so a change here strands them.
	for key, want := range map[string]int{"k1": 85, "room-1": 63, "": 38} {
		if got := Slot(key); got != want {
			t.Fatalf("Slot(%q) = %d, want %d", key, got, want)
		}
	}
}

func TestSlotsSpreadKeys(t *testing.T) {
	hits := make([]int, Slots)
	for i := range Slots * 100 {
		hits[Slot(fmt.Sprint("room-", i))]++
	}
	for slot, n := range hits {
		if n < 50 || n > 170 { // The mean is 100.
			t.Fatalf("slot %d got %d keys", slot, n)
		}
	}
}

func TestPickIgnoresOrderAndMovesOnlyRemovedNodesKeys(t *testing.T) {
	three, two := []string{"a", "b", "c"}, []string{"c", "a"}
	if Pick("x", []string{"a", "b"}) != Pick("x", []string{"b", "a"}) {
		t.Fatal("Pick depends on candidate order")
	}
	if Pick("x", nil) != "" {
		t.Fatal("no candidates must pick nothing")
	}
	for i := range 1000 {
		key := fmt.Sprint(i)
		if before := Pick(key, three); before != "b" && Pick(key, two) != before {
			t.Fatalf("key %s moved off a surviving node", key)
		}
	}
}
