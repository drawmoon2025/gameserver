package resume

import (
	"fmt"
	"testing"
)

func pushN(s *Session, n int) {
	for i := 0; i < n; i++ {
		s.Push(func(seq uint64) []byte { return []byte(fmt.Sprint(seq)) })
	}
}

func TestReplayInsideWindow(t *testing.T) {
	s := New(4)
	pushN(s, 6)
	frames, ok := s.Replay(3)
	if !ok || len(frames) != 3 || string(frames[0]) != "4" || string(frames[2]) != "6" {
		t.Fatalf("ok=%v frames=%q", ok, frames)
	}
}

func TestReplayAtWindowEdge(t *testing.T) {
	s := New(4)
	pushN(s, 6)
	frames, ok := s.Replay(2) // Frames 3..6 are exactly the retained four.
	if !ok || len(frames) != 4 || string(frames[0]) != "3" {
		t.Fatalf("ok=%v frames=%q", ok, frames)
	}
	if _, ok := s.Replay(1); ok {
		t.Fatal("frame 2 was overwritten, replay must fail")
	}
}

func TestReplayNothingMissed(t *testing.T) {
	s := New(4)
	pushN(s, 6)
	if frames, ok := s.Replay(6); !ok || len(frames) != 0 {
		t.Fatalf("ok=%v frames=%q", ok, frames)
	}
}

func TestReplayClientAheadForcesSnapshot(t *testing.T) {
	s := New(4)
	pushN(s, 2)
	if _, ok := s.Replay(9); ok {
		t.Fatal("a client ahead of the server means the session was rebuilt")
	}
}

func TestClientSeqDedupe(t *testing.T) {
	s := New(4)
	if v, _ := s.CheckClientSeq(1); v != Process {
		t.Fatal("first request must process")
	}
	s.Commit(1, []byte("rsp1"))
	if v, b := s.CheckClientSeq(1); v != ResendLast || string(b) != "rsp1" {
		t.Fatalf("retry: %v %q", v, b)
	}
	if v, _ := s.CheckClientSeq(2); v != Process {
		t.Fatal("next request must process")
	}
	s.Commit(2, []byte("rsp2"))
	if v, _ := s.CheckClientSeq(1); v != DropStale {
		t.Fatal("old request must drop")
	}
}
