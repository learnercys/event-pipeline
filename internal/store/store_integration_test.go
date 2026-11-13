//go:build integration

package store

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/learnercys/event-pipeline/internal/event"
)

// Run with: make test-integration (needs the Postgres from docker-compose).
func newStore(t *testing.T) *Store {
	t.Helper()
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		url = "postgres://postgres:postgres@localhost:55432/pipeline?sslmode=disable"
	}
	s, err := New(context.Background(), url)
	if err != nil {
		t.Skipf("postgres not available: %v", err)
	}
	t.Cleanup(s.Close)
	return s
}

func testEvent(t *testing.T) event.Event {
	return event.Event{
		ID:          fmt.Sprintf("it_%s_%d", t.Name(), time.Now().UnixNano()),
		Type:        event.PaymentCaptured,
		AccountID:   fmt.Sprintf("it_acct_%d", time.Now().UnixNano()),
		AmountCents: 1000,
		Currency:    "USD",
		OccurredAt:  time.Now().UTC().Truncate(time.Microsecond),
	}
}

// 50 concurrent retries of the same webhook must create exactly one event and
// one outbox row.
func TestConcurrentIngestCreatesOnce(t *testing.T) {
	s, e := newStore(t), testEvent(t)
	results := make(chan IngestResult, 50)
	var wg sync.WaitGroup
	for range 50 {
		wg.Go(func() {
			r, err := s.Ingest(context.Background(), e)
			if err != nil {
				t.Error(err)
			}
			results <- r
		})
	}
	wg.Wait()
	close(results)

	counts := map[IngestResult]int{}
	for r := range results {
		counts[r]++
	}
	if counts[Created] != 1 || counts[Duplicate] != 49 {
		t.Fatalf("want 1 created / 49 duplicates, got %v", counts)
	}
	var outbox int
	if err := s.pool.QueryRow(context.Background(),
		`SELECT count(*) FROM outbox WHERE event_id = $1`, e.ID).Scan(&outbox); err != nil {
		t.Fatal(err)
	}
	if outbox != 1 {
		t.Fatalf("want 1 outbox row, got %d", outbox)
	}
}

func TestIngestConflict(t *testing.T) {
	s, e := newStore(t), testEvent(t)
	if r, _ := s.Ingest(context.Background(), e); r != Created {
		t.Fatalf("got %v", r)
	}
	e.AmountCents++
	if r, _ := s.Ingest(context.Background(), e); r != Conflict {
		t.Fatalf("reused id with new data: got %v, want conflict", r)
	}
}

// Concurrent redeliveries of the same message must move the balance once.
func TestConcurrentApplyToLedger(t *testing.T) {
	s, e := newStore(t), testEvent(t)
	var applied sync.Map
	var wg sync.WaitGroup
	for i := range 20 {
		wg.Go(func() {
			ok, err := s.ApplyToLedger(context.Background(), "it-ledger", e)
			if err != nil {
				t.Error(err)
			}
			applied.Store(i, ok)
		})
	}
	wg.Wait()

	n := 0
	applied.Range(func(_, v any) bool {
		if v.(bool) {
			n++
		}
		return true
	})
	if n != 1 {
		t.Fatalf("event applied %d times", n)
	}
	var balance int64
	if err := s.pool.QueryRow(context.Background(),
		`SELECT amount_cents FROM balances WHERE account_id = $1`, e.AccountID).Scan(&balance); err != nil {
		t.Fatal(err)
	}
	if balance != 1000 {
		t.Fatalf("balance = %d, want 1000", balance)
	}
}

// A relay that dies after publishing must leave the rows to be published again.
func TestRelayBatchAbortKeepsRowsUnpublished(t *testing.T) {
	s, e := newStore(t), testEvent(t)
	if _, err := s.Ingest(context.Background(), e); err != nil {
		t.Fatal(err)
	}

	sawEvent := func(batch []OutboxRecord) bool {
		for _, r := range batch {
			if r.EventID == e.ID {
				return true
			}
		}
		return false
	}

	// Drain until our row shows up, aborting that batch.
	var aborted bool
	for !aborted {
		_, err := s.RelayBatch(context.Background(), 500, func(_ context.Context, b []OutboxRecord) error {
			if sawEvent(b) {
				aborted = true
				return ErrAbort
			}
			return nil
		})
		if err != nil && !aborted {
			t.Fatal(err)
		}
	}

	var published bool
	for !published {
		n, err := s.RelayBatch(context.Background(), 500, func(_ context.Context, b []OutboxRecord) error {
			published = published || sawEvent(b)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 && !published {
			t.Fatal("aborted row was never offered again")
		}
	}
}
