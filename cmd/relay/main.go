// Command relay publishes outbox rows to SNS.
package main

import (
	"context"
	"os/signal"
	"syscall"
	"time"

	"github.com/learnercys/event-pipeline/internal/awsx"
	"github.com/learnercys/event-pipeline/internal/chaos"
	"github.com/learnercys/event-pipeline/internal/config"
	"github.com/learnercys/event-pipeline/internal/relay"
	"github.com/learnercys/event-pipeline/internal/store"
)

func main() {
	log := config.Logger("relay")
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

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

	r := &relay.Relay{
		Outbox:    st,
		SNS:       awsx.SNS(cfg),
		TopicARN:  config.Must("TOPIC_ARN"),
		BatchSize: config.Int("BATCH_SIZE", 100),
		Idle:      config.Duration("IDLE_INTERVAL", 200*time.Millisecond),
		Chaos:     inj,
		Log:       log,
	}
	log.Info("relaying", "topic", r.TopicARN, "chaos", inj.Enabled())
	if err := r.Run(ctx); err != nil {
		config.Fatal(err)
	}
	log.Info("stopped", "chaos_crashes", inj.Injected("relay.crash_after_publish"))
}
