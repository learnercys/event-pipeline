// Package relay moves events from the Postgres outbox to SNS.
package relay

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sns/types"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/learnercys/event-pipeline/internal/chaos"
	"github.com/learnercys/event-pipeline/internal/store"
	"github.com/learnercys/event-pipeline/internal/telemetry"
)

// snsBatchLimit is the most entries SNS accepts in one PublishBatch call.
const snsBatchLimit = 10

// Outbox is the part of the store the relay uses.
type Outbox interface {
	RelayBatch(ctx context.Context, limit int,
		publish func(context.Context, []store.OutboxRecord) error) (int, error)
}

// Publisher sends a batch of up to 10 messages.
type Publisher interface {
	PublishBatch(ctx context.Context, in *sns.PublishBatchInput,
		opts ...func(*sns.Options)) (*sns.PublishBatchOutput, error)
}

// Relay polls the outbox and publishes new events.
type Relay struct {
	Outbox    Outbox
	SNS       Publisher
	TopicARN  string
	BatchSize int
	Idle      time.Duration // how long to sleep when the outbox is empty
	Chaos     *chaos.Injector
	Log       *slog.Logger
}

// Run publishes until ctx is cancelled.
func (r *Relay) Run(ctx context.Context) error {
	for {
		n, err := r.Outbox.RelayBatch(ctx, r.BatchSize, r.publish)
		switch {
		case ctx.Err() != nil:
			return nil
		case err != nil:
			r.Log.Warn("relay batch failed, will retry", "err", err)
			sleep(ctx, time.Second)
		case n == 0:
			sleep(ctx, r.Idle)
		default:
			r.Log.Debug("published", "count", n)
		}
	}
}

// publish sends every record to SNS. Any failure aborts the batch, which
// leaves all rows unpublished so they are retried. Records that did reach SNS
// will be sent again; consumers dedupe them.
func (r *Relay) publish(ctx context.Context, batch []store.OutboxRecord) error {
	for start := 0; start < len(batch); start += snsBatchLimit {
		end := min(start+snsBatchLimit, len(batch))
		entries := make([]types.PublishBatchRequestEntry, 0, end-start)
		spans := make([]trace.Span, 0, end-start)
		for _, rec := range batch[start:end] {
			// Continue the trace of the request that accepted this event.
			// The publish span is a producer span in that trace, and its
			// context rides along in the message attributes.
			pctx := telemetry.Deserialize(ctx, rec.TraceContext)
			pctx, span := tracer.Start(pctx, "publish "+r.topicName(), trace.WithSpanKind(trace.SpanKindProducer),
				trace.WithAttributes(
					semconv.MessagingSystemAWSSNS,
					semconv.MessagingOperationTypeSend,
					semconv.MessagingDestinationName(r.topicName()),
					attribute.String("event.id", rec.EventID),
					attribute.Int64("outbox.id", rec.ID),
				))
			spans = append(spans, span)
			attrs := map[string]types.MessageAttributeValue{
				"event_id": {DataType: aws.String("String"), StringValue: aws.String(rec.EventID)},
			}
			telemetry.InjectSNS(pctx, attrs)
			entries = append(entries, types.PublishBatchRequestEntry{
				Id:                aws.String(strconv.FormatInt(rec.ID, 10)),
				Message:           aws.String(string(rec.Payload)),
				MessageAttributes: attrs,
			})
		}
		out, err := r.SNS.PublishBatch(ctx, &sns.PublishBatchInput{
			TopicArn:                   aws.String(r.TopicARN),
			PublishBatchRequestEntries: entries,
		})
		if err == nil && len(out.Failed) > 0 {
			err = fmt.Errorf("publish batch: %d of %d entries failed", len(out.Failed), len(entries))
		}
		for _, span := range spans {
			if err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, "publish failed")
			}
			span.End()
		}
		if err != nil {
			return fmt.Errorf("publish batch: %w", err)
		}
	}

	// Simulates the relay dying after SNS accepted the messages but before
	// the outbox rows were marked published.
	if r.Chaos.Should("relay.crash_after_publish") {
		return fmt.Errorf("chaos: crashed after publish: %w", store.ErrAbort)
	}
	return nil
}

var tracer = otel.Tracer("github.com/learnercys/event-pipeline/internal/relay")

// topicName is the last segment of the topic ARN, used to name spans.
func (r *Relay) topicName() string {
	if i := strings.LastIndex(r.TopicARN, ":"); i >= 0 {
		return r.TopicARN[i+1:]
	}
	return r.TopicARN
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}
