package relay

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sns/types"

	"github.com/learnercys/event-pipeline/internal/chaos"
	"github.com/learnercys/event-pipeline/internal/store"
)

type fakeSNS struct {
	calls    int
	messages []string
	failOne  bool
}

func (f *fakeSNS) PublishBatch(_ context.Context, in *sns.PublishBatchInput,
	_ ...func(*sns.Options)) (*sns.PublishBatchOutput, error) {
	f.calls++
	out := &sns.PublishBatchOutput{}
	for i, e := range in.PublishBatchRequestEntries {
		if f.failOne && i == 0 {
			out.Failed = append(out.Failed, types.BatchResultErrorEntry{Id: e.Id})
			continue
		}
		f.messages = append(f.messages, *e.Message)
	}
	return out, nil
}

func records(n int) []store.OutboxRecord {
	recs := make([]store.OutboxRecord, n)
	for i := range recs {
		recs[i] = store.OutboxRecord{ID: int64(i + 1), EventID: fmt.Sprintf("evt_%d", i),
			Payload: []byte(fmt.Sprintf(`{"id":"evt_%d"}`, i))}
	}
	return recs
}

func newRelay(pub Publisher, inj *chaos.Injector) *Relay {
	return &Relay{SNS: pub, TopicARN: "arn:test", Chaos: inj,
		Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
}

func TestPublishSplitsIntoSNSBatches(t *testing.T) {
	pub := &fakeSNS{}
	if err := newRelay(pub, chaos.Disabled).publish(context.Background(), records(25)); err != nil {
		t.Fatal(err)
	}
	if pub.calls != 3 {
		t.Fatalf("25 records should take 3 SNS calls, got %d", pub.calls)
	}
	if len(pub.messages) != 25 {
		t.Fatalf("expected 25 messages, got %d", len(pub.messages))
	}
}

func TestPartialFailureAbortsBatch(t *testing.T) {
	pub := &fakeSNS{failOne: true}
	if err := newRelay(pub, chaos.Disabled).publish(context.Background(), records(3)); err == nil {
		t.Fatal("a partially failed batch must return an error so rows stay unpublished")
	}
}

func TestCrashAfterPublishAborts(t *testing.T) {
	inj, _ := chaos.Parse("relay.crash_after_publish=1")
	pub := &fakeSNS{}
	err := newRelay(pub, inj).publish(context.Background(), records(2))
	if !errors.Is(err, store.ErrAbort) {
		t.Fatalf("expected ErrAbort, got %v", err)
	}
	if len(pub.messages) != 2 {
		t.Fatal("messages should already be published before the simulated crash")
	}
}
