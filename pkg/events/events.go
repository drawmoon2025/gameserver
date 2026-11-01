// Package events carries side-channel data: analytics, audit, activity stats.
// Core game flows never depend on it. Business code only writes to an outbox;
// the Relay forwards to Kafka when enabled and to Discard otherwise. That makes
// the Kafka switch a choice of Publisher, not a second code path.
package events

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

// Event is one outbound message. Key picks the partition, preserving per-key
// order.
type Event struct {
	Topic   string
	Key     string
	Payload []byte
}

// Row is an Event stored in an outbox.
type Row struct {
	ID int64
	Event
}

// Publisher sends a batch. It must not report success for unsent events.
type Publisher interface {
	Publish(ctx context.Context, batch []Event) error
	Close() error
}

// Discard drops events: this is what "Kafka off" means.
type Discard struct{}

// Publish implements Publisher.
func (Discard) Publish(context.Context, []Event) error { return nil }

// Close implements Publisher.
func (Discard) Close() error { return nil }

// Kafka publishes to a Kafka cluster.
type Kafka struct {
	w *kafka.Writer
}

// NewKafka creates a publisher that waits for all in-sync replicas.
func NewKafka(brokers []string) *Kafka {
	return &Kafka{w: &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
	}}
}

// Publish implements Publisher.
func (k *Kafka) Publish(ctx context.Context, batch []Event) error {
	msgs := make([]kafka.Message, len(batch))
	for i, e := range batch {
		msgs[i] = kafka.Message{Topic: e.Topic, Key: []byte(e.Key), Value: e.Payload}
	}
	return k.w.WriteMessages(ctx, msgs...)
}

// Close implements Publisher.
func (k *Kafka) Close() error { return k.w.Close() }

// Store is the outbox as seen by the Relay.
type Store interface {
	Fetch(ctx context.Context, limit int) ([]Row, error)
	Delete(ctx context.Context, ids []int64) error
}

// Relay moves outbox rows to a Publisher. Delivery is at-least-once: a crash
// between Publish and Delete resends, so consumers must dedupe.
type Relay struct {
	Store     Store
	Publisher Publisher
	Interval  time.Duration // Default 500ms.
	Batch     int           // Default 500.
}

// Run forwards until ctx ends.
func (r *Relay) Run(ctx context.Context) {
	if r.Interval <= 0 {
		r.Interval = 500 * time.Millisecond
	}
	if r.Batch <= 0 {
		r.Batch = 500
	}
	t := time.NewTicker(r.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		// Drain, so a backlog clears faster than one batch per tick.
		for ctx.Err() == nil {
			n, err := r.Once(ctx)
			if err != nil {
				slog.Error("outbox relay", "err", err)
				break
			}
			if n < r.Batch {
				break
			}
		}
	}
}

// Once forwards a single batch and returns how many rows moved.
func (r *Relay) Once(ctx context.Context) (int, error) {
	if r.Batch <= 0 {
		r.Batch = 500
	}
	rows, err := r.Store.Fetch(ctx, r.Batch)
	if err != nil || len(rows) == 0 {
		return 0, err
	}
	batch := make([]Event, len(rows))
	ids := make([]int64, len(rows))
	for i, row := range rows {
		batch[i], ids[i] = row.Event, row.ID
	}
	if err := r.Publisher.Publish(ctx, batch); err != nil {
		return 0, fmt.Errorf("publish: %w", err)
	}
	if err := r.Store.Delete(ctx, ids); err != nil {
		return 0, fmt.Errorf("delete: %w", err)
	}
	return len(rows), nil
}

// MemoryStore is an in-process outbox.
type MemoryStore struct {
	mu   sync.Mutex
	next int64
	rows []Row
}

// Add appends an event.
func (m *MemoryStore) Add(e Event) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.next++
	m.rows = append(m.rows, Row{ID: m.next, Event: e})
}

// Fetch implements Store.
func (m *MemoryStore) Fetch(_ context.Context, limit int) ([]Row, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if limit > len(m.rows) {
		limit = len(m.rows)
	}
	return append([]Row(nil), m.rows[:limit]...), nil
}

// Delete implements Store.
func (m *MemoryStore) Delete(_ context.Context, ids []int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	gone := make(map[int64]bool, len(ids))
	for _, id := range ids {
		gone[id] = true
	}
	kept := m.rows[:0]
	for _, r := range m.rows {
		if !gone[r.ID] {
			kept = append(kept, r)
		}
	}
	m.rows = kept
	return nil
}

// MySQLStore is the outbox table of one database. Run one Relay per shard.
// Only one relay per database should run at a time, or rows are sent twice.
type MySQLStore struct {
	DB *sql.DB
}

// Fetch implements Store.
func (m *MySQLStore) Fetch(ctx context.Context, limit int) ([]Row, error) {
	rows, err := m.DB.QueryContext(ctx,
		`SELECT id, topic, partition_key, payload FROM event_outbox ORDER BY id LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Row
	for rows.Next() {
		var r Row
		if err := rows.Scan(&r.ID, &r.Topic, &r.Key, &r.Payload); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// Delete implements Store.
func (m *MySQLStore) Delete(ctx context.Context, ids []int64) error {
	if len(ids) == 0 {
		return nil
	}
	args := make([]any, len(ids))
	q := []byte(`DELETE FROM event_outbox WHERE id IN (`)
	for i, id := range ids {
		if i > 0 {
			q = append(q, ',')
		}
		q = append(q, '?')
		args[i] = id
	}
	q = append(q, ')')
	_, err := m.DB.ExecContext(ctx, string(q), args...)
	return err
}
