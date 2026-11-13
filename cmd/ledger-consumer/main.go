// Command ledger-consumer applies payment events to account balances.
package main

import (
	"context"
	"os/signal"
	"syscall"
	"time"

	"github.com/learnercys/event-pipeline/internal/awsx"
	"github.com/learnercys/event-pipeline/internal/chaos"
	"github.com/learnercys/event-pipeline/internal/config"
	"github.com/learnercys/event-pipeline/internal/consumer"
	"github.com/learnercys/event-pipeline/internal/store"
	"github.com/learnercys/event-pipeline/internal/telemetry"
)

func main() {
	log := config.Logger("ledger-consumer")
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	shutdown, err := telemetry.Setup(ctx, "ledger-consumer", telemetry.OnlyContinueTraces())
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
	cfg, err := awsx.Load(ctx)
	if err != nil {
		config.Fatal(err)
	}

	c := &consumer.Consumer{
		Name:     "ledger",
		Queue:    awsx.SQS(cfg),
		QueueURL: config.Must("QUEUE_URL"),
		Ledger:   st,
		Workers:  config.Int("WORKERS", 4),
		Chaos:    inj,
		Log:      log,
	}

	go func() {
		t := time.NewTicker(10 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				log.Info("stats", "applied", c.Stats.Applied.Load(), "duplicates", c.Stats.Duplicates.Load(),
					"poison", c.Stats.Poison.Load(), "failed", c.Stats.Failed.Load())
			}
		}
	}()

	log.Info("consuming", "queue", c.QueueURL, "workers", c.Workers, "chaos", inj.Enabled())
	c.Run(ctx)
	log.Info("stopped", "applied", c.Stats.Applied.Load(), "duplicates", c.Stats.Duplicates.Load())
}
