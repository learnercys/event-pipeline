package consumer

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/learnercys/event-pipeline/internal/chaos"
	"github.com/learnercys/event-pipeline/internal/event"
)

type fakeQueue struct {
	mu      sync.Mutex
	deleted []string
}

func (q *fakeQueue) ReceiveMessage(context.Context, *sqs.ReceiveMessageInput,
	...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	return &sqs.ReceiveMessageOutput{}, nil
}

func (q *fakeQueue) DeleteMessage(_ context.Context, in *sqs.DeleteMessageInput,
	_ ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.deleted = append(q.deleted, aws.ToString(in.ReceiptHandle))
	return &sqs.DeleteMessageOutput{}, nil
}

// fakeLedger applies each (consumer, event) pair once, like processed_messages.
type fakeLedger struct {
	seen     map[string]bool
	balances map[string]int64
	err      error
	pings    int
}

func (l *fakeLedger) Ping(context.Context) error {
	l.pings++
	if l.pings >= 2 {
		l.err = nil // database comes back on the second probe
		return nil
	}
	return errors.New("still down")
}

func newLedger() *fakeLedger {
	return &fakeLedger{seen: map[string]bool{}, balances: map[string]int64{}}
}

func (l *fakeLedger) ApplyToLedger(_ context.Context, consumer string, e event.Event) (bool, error) {
	if l.err != nil {
		return false, l.err
	}
	key := consumer + "/" + e.ID
	if l.seen[key] {
		return false, nil
	}
	l.seen[key] = true
	l.balances[e.AccountID] += e.SignedAmount()
	return true, nil
}

func msg(handle, body string) types.Message {
	return types.Message{MessageId: aws.String(handle), ReceiptHandle: aws.String(handle), Body: aws.String(body)}
}

const capture = `{"id":"evt_1","type":"payment.captured","account_id":"acct_1",
	"amount_cents":700,"currency":"USD","occurred_at":"2026-11-13T00:00:00Z"}`

func newConsumer(q Queue, l Ledger, inj *chaos.Injector) *Consumer {
	return &Consumer{Name: "ledger", Queue: q, QueueURL: "q", Ledger: l, Chaos: inj,
		Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
}

func TestDuplicateDeliveryAppliesOnce(t *testing.T) {
	q, l := &fakeQueue{}, newLedger()
	c := newConsumer(q, l, chaos.Disabled)

	c.Handle(context.Background(), msg("r1", capture))
	c.Handle(context.Background(), msg("r2", capture))

	if l.balances["acct_1"] != 700 {
		t.Fatalf("balance = %d, want 700", l.balances["acct_1"])
	}
	if c.Stats.Applied.Load() != 1 || c.Stats.Duplicates.Load() != 1 {
		t.Fatalf("applied=%d duplicates=%d", c.Stats.Applied.Load(), c.Stats.Duplicates.Load())
	}
	if len(q.deleted) != 2 {
		t.Fatalf("both deliveries should be acknowledged, got %v", q.deleted)
	}
}

func TestPoisonMessageIsNotDeleted(t *testing.T) {
	q := &fakeQueue{}
	c := newConsumer(q, newLedger(), chaos.Disabled)
	c.Handle(context.Background(), msg("r1", `{"garbage":true}`))
	if len(q.deleted) != 0 || c.Stats.Poison.Load() != 1 {
		t.Fatal("poison message must stay on the queue so it reaches the DLQ")
	}
}

func TestTransientErrorIsNotDeleted(t *testing.T) {
	q, l := &fakeQueue{}, newLedger()
	l.err = errors.New("db down")
	c := newConsumer(q, l, chaos.Disabled)
	c.Handle(context.Background(), msg("r1", capture))
	if len(q.deleted) != 0 || c.Stats.Failed.Load() != 1 {
		t.Fatal("a failed apply must leave the message for redelivery")
	}
	if !c.degraded.Load() {
		t.Fatal("a database failure should open the circuit breaker")
	}
}

// While the database is down the consumer must not receive messages: each
// receive burns one of the message's attempts before it lands in the DLQ.
func TestBreakerStopsReceivingUntilDatabaseRecovers(t *testing.T) {
	q, l := &countingQueue{}, newLedger()
	l.err = errors.New("db down")
	c := newConsumer(q, l, chaos.Disabled)
	c.degraded.Store(true)

	c.waitForDatabase(context.Background())

	if q.receives != 0 {
		t.Fatalf("received %d times while degraded", q.receives)
	}
	if c.degraded.Load() || l.pings != 2 {
		t.Fatalf("breaker should close after the database answers (pings=%d)", l.pings)
	}
}

type countingQueue struct {
	fakeQueue
	receives int
}

func (q *countingQueue) ReceiveMessage(ctx context.Context, in *sqs.ReceiveMessageInput,
	opts ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	q.receives++
	return q.fakeQueue.ReceiveMessage(ctx, in, opts...)
}

func TestCrashAfterCommitThenRedelivery(t *testing.T) {
	inj, _ := chaos.Parse("ledger.crash_after_commit=1")
	q, l := &fakeQueue{}, newLedger()
	c := newConsumer(q, l, inj)

	c.Handle(context.Background(), msg("r1", capture)) // commits, "crashes" before delete
	if len(q.deleted) != 0 {
		t.Fatal("simulated crash should skip the delete")
	}

	c.Chaos = chaos.Disabled
	c.Handle(context.Background(), msg("r2", capture)) // SQS redelivers

	if l.balances["acct_1"] != 700 {
		t.Fatalf("redelivery double-applied: balance = %d", l.balances["acct_1"])
	}
	if len(q.deleted) != 1 {
		t.Fatal("the redelivered message should be acknowledged")
	}
}
