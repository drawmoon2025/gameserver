package actor

import (
	"context"
	"errors"
	"testing"
)

func TestSerialExecutionWithoutLocks(t *testing.T) {
	a := Spawn("t", 16)
	defer a.Stop()
	n := 0
	for i := 0; i < 1000; i++ {
		if _, err := Ask(context.Background(), a, func() (int, error) { n++; return n, nil }); err != nil {
			t.Fatal(err)
		}
	}
	if n != 1000 {
		t.Fatalf("n=%d", n)
	}
}

func TestPanicDoesNotKillActor(t *testing.T) {
	a := Spawn("t", 16)
	defer a.Stop()
	if _, err := Ask(context.Background(), a, func() (int, error) { panic("boom") }); err == nil {
		t.Fatal("want error from panic")
	}
	v, err := Ask(context.Background(), a, func() (int, error) { return 7, nil })
	if err != nil || v != 7 {
		t.Fatalf("v=%d err=%v", v, err)
	}
}

func TestFullMailboxRejectsInsteadOfBlocking(t *testing.T) {
	a := Spawn("t", 1)
	release := make(chan struct{})
	started := make(chan struct{})
	if err := a.Tell(func() { close(started); <-release }); err != nil {
		t.Fatal(err)
	}
	<-started
	if err := a.Tell(func() {}); err != nil { // Fills the one slot.
		t.Fatal(err)
	}
	if err := a.Tell(func() {}); !errors.Is(err, ErrMailboxFull) {
		t.Fatalf("got %v", err)
	}
	close(release)
	a.Stop()
}

func TestStopRunsAcceptedWorkAndRejectsNew(t *testing.T) {
	a := Spawn("t", 8)
	ran := false
	if err := a.Tell(func() { ran = true }); err != nil {
		t.Fatal(err)
	}
	a.Stop()
	if !ran {
		t.Fatal("accepted work was dropped")
	}
	if err := a.Tell(func() {}); !errors.Is(err, ErrStopped) {
		t.Fatalf("got %v", err)
	}
}
