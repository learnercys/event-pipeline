// Package consumer reads events from SQS and applies them to the ledger.
package consumer

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/learnercys/event-pipeline/internal/chaos"
	"github.com/learnercys/event-pipeline/internal/event"
)

// Queue is the part of the SQS API the consumer uses.
type Queue interface {
	ReceiveMessage(ctx context.Context, in *sqs.ReceiveMessageInput,
		opts ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	DeleteMessage(ctx context.Context, in *sqs.DeleteMessageInput,
		opts ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
}

// Ledger applies an event at most once per consumer name.
type Ledger interface {
	ApplyToLedger(ctx context.Context, consumer string, e event.Event) (bool, error)
	Ping(ctx context.Context) error
}

// Stats counts what the consumer did. Safe for concurrent use.
type Stats struct {
	Applied    atomic.Int64 // first time this event was seen: balance changed
	Duplicates atomic.Int64 // already applied: acknowledged without changes
	Poison     atomic.Int64 // could not be decoded: left for the DLQ
	Failed     atomic.Int64 // transient error: left for redelivery
}

// Consumer long-polls an SQS queue with a pool of workers.
type Consumer struct {
	Name     string // dedupe namespace, e.g. "ledger"
	Queue    Queue
	QueueURL string
	Ledger   Ledger
	Workers  int
	Chaos    *chaos.Injector
	Log      *slog.Logger
	Stats    Stats

	// degraded is set when the database fails. While it is set, workers stop
	// receiving: every receive counts toward the redrive limit, so polling
	// during an outage would push healthy messages into the DLQ.
	degraded atomic.Bool
}

// Run consumes until ctx is cancelled, then lets in-flight messages finish.
func (c *Consumer) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for range max(c.Workers, 1) {
		wg.Go(func() { c.worker(ctx) })
	}
	wg.Wait()
}

func (c *Consumer) worker(ctx context.Context) {
	for ctx.Err() == nil {
		if c.degraded.Load() {
			c.waitForDatabase(ctx)
			continue
		}
		out, err := c.Queue.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(c.QueueURL),
			MaxNumberOfMessages: 10,
			WaitTimeSeconds:     10, // long polling: fewer empty receives
		})
		if err != nil {
			if ctx.Err() == nil {
				c.Log.Warn("receive failed", "err", err)
				select {
				case <-ctx.Done():
				case <-time.After(time.Second):
				}
			}
			continue
		}
		for _, m := range out.Messages {
			if c.degraded.Load() {
				break // the rest come back after the visibility timeout
			}
			// Finish the message even during shutdown: a half-handled message
			// would only be redelivered, but there is no reason to waste work.
			c.Handle(context.WithoutCancel(ctx), m)
		}
	}
}

// waitForDatabase is the circuit breaker's half-open state: probe with
// backoff and only resume receiving once the database answers again.
func (c *Consumer) waitForDatabase(ctx context.Context) {
	backoff := 250 * time.Millisecond
	for ctx.Err() == nil {
		pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		err := c.Ledger.Ping(pingCtx)
		cancel()
		if err == nil {
			if c.degraded.CompareAndSwap(true, false) {
				c.Log.Info("database is back, resuming")
			}
			return
		}
		if !c.degraded.Load() {
			return // another worker already recovered
		}
		select {
		case <-ctx.Done():
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, 5*time.Second)
	}
}

// Handle processes one message. It deletes the message only after its effect
// is committed. Anything else leaves the message on the queue, where SQS
// redelivers it after the visibility timeout and, after maxReceiveCount
// attempts, moves it to the dead-letter queue.
func (c *Consumer) Handle(ctx context.Context, m types.Message) {
	e, err := event.Decode([]byte(aws.ToString(m.Body)))
	if err != nil {
		// Poison message: retrying will never fix it. We leave it alone so the
		// redrive policy moves it to the DLQ for a human to inspect.
		c.Stats.Poison.Add(1)
		c.Log.Warn("poison message", "message_id", aws.ToString(m.MessageId), "err", err)
		return
	}

	applied, err := c.Ledger.ApplyToLedger(ctx, c.Name, e)
	if err != nil {
		c.Stats.Failed.Add(1)
		if c.degraded.CompareAndSwap(false, true) {
			c.Log.Warn("database failing, pausing consumption", "event_id", e.ID, "err", err)
		}
		return
	}
	if applied {
		c.Stats.Applied.Add(1)
	} else {
		c.Stats.Duplicates.Add(1)
	}

	// Simulates dying after the commit but before acknowledging the message.
	// SQS redelivers it, and the dedupe table turns it into a no-op.
	if c.Chaos.Should(c.Name + ".crash_after_commit") {
		return
	}

	if _, err := c.Queue.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(c.QueueURL),
		ReceiptHandle: m.ReceiptHandle,
	}); err != nil && !errors.Is(err, context.Canceled) {
		// Harmless: the message comes back and is deduplicated.
		c.Log.Warn("delete failed, message will be redelivered", "event_id", e.ID, "err", err)
	}
}
