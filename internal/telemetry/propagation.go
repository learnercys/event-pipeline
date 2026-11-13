package telemetry

import (
	"context"
	"encoding/json"

	"github.com/aws/aws-sdk-go-v2/aws"
	snstypes "github.com/aws/aws-sdk-go-v2/service/sns/types"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

// Trace context has to survive three hops that don't carry HTTP headers:
//
//	ingest  --(outbox row)-->  relay  --(SNS message attributes)-->
//	SQS (raw delivery keeps the attributes)  -->  consumers
//
// It travels as W3C trace context (traceparent/tracestate) in each one.

// Serialize returns the trace context in ctx as JSON, for the outbox row.
func Serialize(ctx context.Context) []byte {
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	if len(carrier) == 0 {
		return nil
	}
	b, _ := json.Marshal(carrier)
	return b
}

// Deserialize restores trace context saved by Serialize.
func Deserialize(ctx context.Context, b []byte) context.Context {
	if len(b) == 0 {
		return ctx
	}
	carrier := propagation.MapCarrier{}
	if json.Unmarshal(b, &carrier) != nil {
		return ctx
	}
	return otel.GetTextMapPropagator().Extract(ctx, carrier)
}

// InjectSNS adds the trace context in ctx to SNS message attributes.
func InjectSNS(ctx context.Context, attrs map[string]snstypes.MessageAttributeValue) {
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	for k, v := range carrier {
		attrs[k] = snstypes.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String(v)}
	}
}

// ExtractSQS returns ctx with the trace context carried by an SQS message.
func ExtractSQS(ctx context.Context, attrs map[string]sqstypes.MessageAttributeValue) context.Context {
	carrier := propagation.MapCarrier{}
	for k, v := range attrs {
		if v.StringValue != nil {
			carrier[k] = *v.StringValue
		}
	}
	return otel.GetTextMapPropagator().Extract(ctx, carrier)
}
