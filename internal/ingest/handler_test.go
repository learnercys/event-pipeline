package ingest

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/learnercys/event-pipeline/internal/chaos"
	"github.com/learnercys/event-pipeline/internal/event"
	"github.com/learnercys/event-pipeline/internal/store"
)

// fakeStore mimics the Postgres semantics: first write wins, repeats are
// duplicates, and a reused ID with different data is a conflict.
type fakeStore struct {
	mu     sync.Mutex
	events map[string]event.Event
	err    error
}

func (f *fakeStore) Ingest(_ context.Context, e event.Event) (store.IngestResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return 0, f.err
	}
	if old, ok := f.events[e.ID]; ok {
		if old.SameAs(e) {
			return store.Duplicate, nil
		}
		return store.Conflict, nil
	}
	f.events[e.ID] = e
	return store.Created, nil
}

const validBody = `{"id":"evt_1","type":"payment.captured","account_id":"acct_1",
	"amount_cents":500,"currency":"USD","occurred_at":"2026-11-13T00:00:00Z"}`

func newServer(t *testing.T, st Store, inj *chaos.Injector) *httptest.Server {
	t.Helper()
	h := &Handler{Store: st, Chaos: inj, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	srv := httptest.NewServer(h.Routes())
	t.Cleanup(srv.Close)
	return srv
}

func post(t *testing.T, srv *httptest.Server, body string) int {
	t.Helper()
	resp, err := http.Post(srv.URL+"/v1/events", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	return resp.StatusCode
}

func TestPostEvent(t *testing.T) {
	st := &fakeStore{events: map[string]event.Event{}}
	srv := newServer(t, st, chaos.Disabled)

	if got := post(t, srv, validBody); got != http.StatusCreated {
		t.Fatalf("first post: got %d, want 201", got)
	}
	if got := post(t, srv, validBody); got != http.StatusOK {
		t.Fatalf("retry: got %d, want 200", got)
	}
	conflicting := strings.Replace(validBody, `"amount_cents":500`, `"amount_cents":501`, 1)
	if got := post(t, srv, conflicting); got != http.StatusConflict {
		t.Fatalf("reused id: got %d, want 409", got)
	}
	if got := post(t, srv, `{"id":""}`); got != http.StatusBadRequest {
		t.Fatalf("invalid: got %d, want 400", got)
	}
	if got := post(t, srv, strings.Repeat("x", maxBody+1)); got != http.StatusRequestEntityTooLarge {
		t.Fatalf("huge body: got %d, want 413", got)
	}
}

func TestStoreErrorIsRetryable(t *testing.T) {
	st := &fakeStore{events: map[string]event.Event{}, err: errors.New("db down")}
	srv := newServer(t, st, chaos.Disabled)
	if got := post(t, srv, validBody); got != http.StatusServiceUnavailable {
		t.Fatalf("got %d, want 503", got)
	}
}

// A lost response after commit must turn the client's retry into a duplicate,
// never a second event.
func TestLostResponseThenRetry(t *testing.T) {
	inj, _ := chaos.Parse("ingest.fail_after_commit=1")
	st := &fakeStore{events: map[string]event.Event{}}
	srv := newServer(t, st, inj)

	if got := post(t, srv, validBody); got != http.StatusServiceUnavailable {
		t.Fatalf("first post: got %d, want 503 from chaos", got)
	}
	if got := post(t, srv, validBody); got != http.StatusOK {
		t.Fatalf("retry: got %d, want 200 duplicate", got)
	}
	if len(st.events) != 1 {
		t.Fatalf("expected 1 stored event, got %d", len(st.events))
	}
}
