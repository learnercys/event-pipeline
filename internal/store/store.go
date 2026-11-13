// Package store holds the Postgres access for every Go service.
package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/exaring/otelpgx"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/learnercys/event-pipeline/internal/event"
	"github.com/learnercys/event-pipeline/internal/telemetry"
)

// Store wraps a Postgres connection pool.
type Store struct {
	pool *pgxpool.Pool
}

// New connects to Postgres and verifies the connection.
func New(ctx context.Context, url string) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("parse database url: %w", err)
	}
	// One span per query, as children of whatever request or message is
	// being handled.
	cfg.ConnConfig.Tracer = otelpgx.NewTracer()
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	for {
		if err = pool.Ping(pingCtx); err == nil {
			return &Store{pool: pool}, nil
		}
		select {
		case <-pingCtx.Done():
			pool.Close()
			return nil, fmt.Errorf("ping: %w", err)
		case <-time.After(250 * time.Millisecond):
		}
	}
}

// Pool exposes the pool for tests and verification tooling.
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// Ping checks that Postgres is reachable.
func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

// Close releases all connections.
func (s *Store) Close() { s.pool.Close() }

// IngestResult says what happened to an incoming event.
type IngestResult int

const (
	// Created means the event is new and was queued for publishing.
	Created IngestResult = iota
	// Duplicate means the same event was already accepted. Nothing changed.
	Duplicate
	// Conflict means the ID was already used for an event with different data.
	Conflict
)

func (r IngestResult) String() string {
	return [...]string{"created", "duplicate", "conflict"}[r]
}

// Ingest stores a new event and its outbox row in one transaction.
//
// Because both rows commit together, an accepted event is always published
// eventually, and a rejected or failed request never publishes anything.
func (s *Store) Ingest(ctx context.Context, e event.Event) (IngestResult, error) {
	payload, err := json.Marshal(e)
	if err != nil {
		return 0, err
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op after commit

	// ON CONFLICT DO NOTHING makes concurrent retries of the same event safe:
	// the second insert waits for the first transaction, then inserts nothing.
	tag, err := tx.Exec(ctx, `
		INSERT INTO events (id, type, account_id, amount_cents, currency, occurred_at)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (id) DO NOTHING`,
		e.ID, e.Type, e.AccountID, e.AmountCents, e.Currency, e.OccurredAt)
	if err != nil {
		return 0, fmt.Errorf("insert event: %w", err)
	}

	if tag.RowsAffected() == 0 {
		existing, err := getEvent(ctx, tx, e.ID)
		if err != nil {
			return 0, err
		}
		if existing.SameAs(e) {
			return Duplicate, nil
		}
		return Conflict, nil
	}

	if _, err := tx.Exec(ctx,
		`INSERT INTO outbox (event_id, payload, trace_context) VALUES ($1, $2, $3)`,
		e.ID, payload, telemetry.Serialize(ctx)); err != nil {
		return 0, fmt.Errorf("insert outbox: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit: %w", err)
	}
	return Created, nil
}

func getEvent(ctx context.Context, q pgx.Tx, id string) (event.Event, error) {
	var e event.Event
	err := q.QueryRow(ctx, `
		SELECT id, type, account_id, amount_cents, currency, occurred_at
		FROM events WHERE id = $1`, id).
		Scan(&e.ID, &e.Type, &e.AccountID, &e.AmountCents, &e.Currency, &e.OccurredAt)
	if err != nil {
		return event.Event{}, fmt.Errorf("load existing event: %w", err)
	}
	return e, nil
}

// OutboxRecord is an event waiting to be published.
type OutboxRecord struct {
	ID      int64
	EventID string
	Payload []byte
	// TraceContext is the W3C trace context of the ingest request (JSON).
	TraceContext []byte
}

// ErrAbort can be returned by a publish callback to roll back a batch on
// purpose (for example, a simulated crash).
var ErrAbort = errors.New("batch aborted")

// RelayBatch locks up to limit unpublished outbox rows, hands them to publish,
// and marks them published only if publish succeeds.
//
// FOR UPDATE SKIP LOCKED lets several relay replicas run side by side without
// publishing the same row at the same time. If the process dies after publish
// but before commit, the rows stay unpublished and are sent again: delivery is
// at-least-once, and consumers must be idempotent.
func (s *Store) RelayBatch(ctx context.Context, limit int,
	publish func(context.Context, []OutboxRecord) error) (int, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op after commit

	rows, err := tx.Query(ctx, `
		SELECT id, event_id, payload, trace_context FROM outbox
		WHERE published_at IS NULL
		ORDER BY id
		LIMIT $1
		FOR UPDATE SKIP LOCKED`, limit)
	if err != nil {
		return 0, fmt.Errorf("select outbox: %w", err)
	}
	batch, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (OutboxRecord, error) {
		var rec OutboxRecord
		err := r.Scan(&rec.ID, &rec.EventID, &rec.Payload, &rec.TraceContext)
		return rec, err
	})
	if err != nil {
		return 0, fmt.Errorf("scan outbox: %w", err)
	}
	if len(batch) == 0 {
		return 0, nil
	}

	if err := publish(ctx, batch); err != nil {
		return 0, err
	}

	ids := make([]int64, len(batch))
	for i, r := range batch {
		ids[i] = r.ID
	}
	if _, err := tx.Exec(ctx,
		`UPDATE outbox SET published_at = now() WHERE id = ANY($1)`, ids); err != nil {
		return 0, fmt.Errorf("mark published: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit: %w", err)
	}
	return len(batch), nil
}

// ApplyToLedger adds the event to the account balance exactly once per
// consumer. It returns false if this consumer already applied the event.
//
// The dedupe insert and the balance update share one transaction, so there is
// no window where one is visible without the other.
func (s *Store) ApplyToLedger(ctx context.Context, consumer string, e event.Event) (bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op after commit

	tag, err := tx.Exec(ctx, `
		INSERT INTO processed_messages (consumer, event_id) VALUES ($1, $2)
		ON CONFLICT DO NOTHING`, consumer, e.ID)
	if err != nil {
		return false, fmt.Errorf("record processed: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return false, nil
	}

	// Balance updates are additive, so the order events arrive in does not
	// matter. See docs/adr/0003-commutative-state.md.
	if _, err := tx.Exec(ctx, `
		INSERT INTO balances (account_id, currency, amount_cents) VALUES ($1, $2, $3)
		ON CONFLICT (account_id, currency) DO UPDATE
		SET amount_cents = balances.amount_cents + EXCLUDED.amount_cents,
		    updated_at   = now()`,
		e.AccountID, e.Currency, e.SignedAmount()); err != nil {
		return false, fmt.Errorf("update balance: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit: %w", err)
	}
	return true, nil
}
