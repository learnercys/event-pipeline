// Command ingest serves the HTTP API that accepts payment events.
package main

import (
	"context"
	"errors"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/learnercys/event-pipeline/internal/chaos"
	"github.com/learnercys/event-pipeline/internal/config"
	"github.com/learnercys/event-pipeline/internal/ingest"
	"github.com/learnercys/event-pipeline/internal/store"
	"github.com/learnercys/event-pipeline/internal/telemetry"
)

func main() {
	log := config.Logger("ingest")
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	shutdown, err := telemetry.Setup(ctx, "ingest")
	if err != nil {
		config.Fatal(err)
	}
	defer func() { _ = shutdown(context.Background()) }()

	inj, err := chaos.FromEnv()
	if err != nil {
		config.Fatal(err)
	}
	st, err := store.New(ctx, config.Must("DATABASE_URL"))
	if err != nil {
		config.Fatal(err)
	}
	defer st.Close()

	srv := &http.Server{
		Addr: config.String("ADDR", ":8080"),
		// One server span per request; the store's queries become its children.
		Handler: otelhttp.NewHandler((&ingest.Handler{Store: st, Chaos: inj, Log: log}).Routes(), "ingest",
			otelhttp.WithSpanNameFormatter(func(_ string, r *http.Request) string { return r.Method + " " + r.URL.Path }),
			otelhttp.WithFilter(func(r *http.Request) bool { return r.URL.Path != "/healthz" })),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	log.Info("listening", "addr", srv.Addr, "chaos", inj.Enabled())
	if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		config.Fatal(err)
	}
	log.Info("stopped")
}
