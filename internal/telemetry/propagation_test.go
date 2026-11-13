package telemetry

import (
	"context"
	"testing"

	snstypes "github.com/aws/aws-sdk-go-v2/service/sns/types"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

func setup(t *testing.T) trace.Tracer {
	t.Helper()
	otel.SetTextMapPropagator(propagation.TraceContext{})
	return sdktrace.NewTracerProvider().Tracer("test")
}

// The trace context survives every hop: outbox row (JSON) -> SNS attributes
// -> SQS attributes (raw delivery copies them) -> consumer.
func TestContextSurvivesOutboxSNSAndSQS(t *testing.T) {
	tracer := setup(t)
	ctx, ingestSpan := tracer.Start(context.Background(), "POST /v1/events")
	want := ingestSpan.SpanContext()

	stored := Serialize(ctx) // written to the outbox row
	if len(stored) == 0 {
		t.Fatal("no trace context serialized")
	}

	relayCtx := Deserialize(context.Background(), stored)
	snsAttrs := map[string]snstypes.MessageAttributeValue{}
	InjectSNS(relayCtx, snsAttrs)

	// SNS raw message delivery hands the same attributes to SQS.
	sqsAttrs := map[string]sqstypes.MessageAttributeValue{}
	for k, v := range snsAttrs {
		sqsAttrs[k] = sqstypes.MessageAttributeValue{DataType: v.DataType, StringValue: v.StringValue}
	}

	got := trace.SpanContextFromContext(ExtractSQS(context.Background(), sqsAttrs))
	if got.TraceID() != want.TraceID() || got.SpanID() != want.SpanID() || !got.IsRemote() {
		t.Fatalf("consumer sees %v, want remote parent %v", got, want)
	}
}

func TestMissingContextIsHarmless(t *testing.T) {
	setup(t)
	if Serialize(context.Background()) != nil {
		t.Fatal("no span should serialize to nothing")
	}
	ctx := Deserialize(context.Background(), []byte("not json"))
	if trace.SpanContextFromContext(ctx).IsValid() {
		t.Fatal("garbage must not produce a trace context")
	}
	if trace.SpanContextFromContext(ExtractSQS(context.Background(), nil)).IsValid() {
		t.Fatal("a message without attributes must not produce a trace context")
	}
}
