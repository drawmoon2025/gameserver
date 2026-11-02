//go:build integration

package events

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/drawmoon2025/gameserver/internal/itest"
)

func TestRelayMovesOutboxToKafka(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	db := itest.MySQL(t)
	if _, err := db.ExecContext(ctx, `DELETE FROM event_outbox`); err != nil { // Other packages' topics.
		t.Fatal(err)
	}
	topic := fmt.Sprint("itest-", itest.ID())
	conn, err := kafka.DialLeader(ctx, "tcp", itest.KafkaBroker(), topic, 0) // Creates the topic.
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	const n = 3
	for i := range n {
		if _, err := db.ExecContext(ctx,
			`INSERT INTO event_outbox (topic, partition_key, payload) VALUES (?, 'k', ?)`,
			topic, []byte(fmt.Sprint("p", i))); err != nil {
			t.Fatal(err)
		}
	}

	pub := NewKafka([]string{itest.KafkaBroker()})
	defer pub.Close()
	r := &Relay{Store: &MySQLStore{DB: db}, Publisher: pub}
	deadline := time.Now().Add(30 * time.Second)
	for {
		moved, err := r.Once(ctx)
		if err == nil && moved == n {
			break
		}
		if time.Now().After(deadline) { // A new topic takes a moment to get a leader.
			t.Fatalf("moved %d: %v", moved, err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	if moved, err := r.Once(ctx); err != nil || moved != 0 {
		t.Fatalf("second pass moved %d: %v", moved, err)
	}

	reader := kafka.NewReader(kafka.ReaderConfig{Brokers: []string{itest.KafkaBroker()}, Topic: topic, Partition: 0})
	defer reader.Close()
	for i := range n {
		m, err := reader.ReadMessage(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if string(m.Value) != fmt.Sprint("p", i) || string(m.Key) != "k" {
			t.Fatalf("message %d: %q", i, m.Value)
		}
	}
}
