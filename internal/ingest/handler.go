// Package ingest is the HTTP entry point that accepts payment events.
package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"

	"github.com/learnercys/event-pipeline/internal/chaos"
	"github.com/learnercys/event-pipeline/internal/event"
	"github.com/learnercys/event-pipeline/internal/store"
)

// maxBody caps request size so a bad client cannot exhaust memory.
const maxBody = 64 << 10

// Store is the persistence the handler needs.
type Store interface {
	Ingest(ctx context.Context, e event.Event) (store.IngestResult, error)
}

// Handler serves the ingest API.
type Handler struct {
	Store Store
	Chaos *chaos.Injector
	Log   *slog.Logger
}

// Routes returns the HTTP routes.
func (h *Handler) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/events", h.postEvent)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	return mux
}

type response struct {
	ID     string `json:"id,omitempty"`
	Status string `json:"status,omitempty"`
	Error  string `json:"error,omitempty"`
}

// postEvent accepts an event.
//
//	201 Created   new event, queued for publishing
//	200 OK        exact duplicate of an accepted event (safe retry)
//	400           malformed or invalid event
//	409 Conflict  ID already used for different data
func (h *Handler) postEvent(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBody))
	if err != nil {
		writeJSON(w, http.StatusRequestEntityTooLarge, response{Error: "body too large"})
		return
	}
	e, err := event.Decode(body)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, response{Error: err.Error()})
		return
	}

	result, err := h.Store.Ingest(r.Context(), e)
	if err != nil {
		h.Log.Error("ingest failed", "event_id", e.ID, "err", err)
		writeJSON(w, http.StatusServiceUnavailable, response{Error: "temporarily unavailable, retry"})
		return
	}

	// Simulates the classic failure where the write commits but the response
	// never reaches the client. The client retries, and must get a duplicate.
	if result == store.Created && h.Chaos.Should("ingest.fail_after_commit") {
		writeJSON(w, http.StatusServiceUnavailable, response{Error: "chaos: response lost"})
		return
	}

	switch result {
	case store.Created:
		writeJSON(w, http.StatusCreated, response{ID: e.ID, Status: result.String()})
	case store.Duplicate:
		writeJSON(w, http.StatusOK, response{ID: e.ID, Status: result.String()})
	case store.Conflict:
		writeJSON(w, http.StatusConflict, response{ID: e.ID,
			Error: "event id already used with different data"})
	default:
		h.Log.Error("unknown ingest result", "result", result)
		writeJSON(w, http.StatusInternalServerError, response{Error: "internal error"})
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil && !errors.Is(err, http.ErrHandlerTimeout) {
		slog.Debug("write response", "err", err)
	}
}
