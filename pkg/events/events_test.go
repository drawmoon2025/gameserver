package events

import (
	"context"
	"errors"
	"testing"
)

type failing struct{}

func (failing) Publish(context.Context, []Event) error { return errors.New("down") }
func (failing) Close() error                           { return nil }

func TestDiscardDrainsOutbox(t *testing.T) {
	s := &MemoryStore{}
	s.Add(Event{Topic: "t"})
	r := &Relay{Store: s, Publisher: Discard{}}
	if n, err := r.Once(context.Background()); err != nil || n != 1 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	if rows, _ := s.Fetch(context.Background(), 10); len(rows) != 0 {
		t.Fatal("outbox grows without bound when Kafka is off")
	}
}

func TestPublishFailureKeepsRows(t *testing.T) {
	s := &MemoryStore{}
	s.Add(Event{Topic: "t"})
	r := &Relay{Store: s, Publisher: failing{}}
	if _, err := r.Once(context.Background()); err == nil {
		t.Fatal("want error")
	}
	if rows, _ := s.Fetch(context.Background(), 10); len(rows) != 1 {
		t.Fatal("rows lost on publish failure")
	}
}
