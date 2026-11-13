// Package telemetry sets up OpenTelemetry tracing and metrics.
//
// Everything is configured with the standard OTEL_* environment variables
// (OTEL_SERVICE_NAME, OTEL_EXPORTER_OTLP_ENDPOINT, OTEL_TRACES_SAMPLER, …),
// which the platform's Helm charts set for every pod. Without an exporter
// endpoint, nothing is exported, but trace context still propagates, so
// tests and docker-compose runs behave the same and pay almost nothing.
package telemetry

import (
	"context"
	"errors"
	"os"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
)

// Option changes how Setup configures tracing.
type Option func(*options)

type options struct{ onlyContinueTraces bool }

// OnlyContinueTraces records a span only if it continues a trace that was
// started elsewhere. Polling processes (the relay, the consumers) need it:
// their idle loop issues a database query every few hundred milliseconds,
// and each one would otherwise start a new, meaningless trace. Their real
// work always carries a parent context from the outbox or the message.
func OnlyContinueTraces() Option { return func(o *options) { o.onlyContinueTraces = true } }

// Setup installs the global tracer and meter providers. Call the returned
// function on shutdown to flush buffered telemetry.
func Setup(ctx context.Context, serviceName string, opts ...Option) (func(context.Context) error, error) {
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{}))

	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") == "" &&
		os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") == "" {
		return func(context.Context) error { return nil }, nil
	}

	if os.Getenv("OTEL_SERVICE_NAME") == "" {
		_ = os.Setenv("OTEL_SERVICE_NAME", serviceName)
	}
	res, err := resource.New(ctx,
		resource.WithFromEnv(), // OTEL_SERVICE_NAME, OTEL_RESOURCE_ATTRIBUTES
		resource.WithTelemetrySDK(),
		resource.WithHost(),
		resource.WithAttributes(semconv.ServiceName(os.Getenv("OTEL_SERVICE_NAME"))),
	)
	if err != nil {
		return nil, err
	}

	traceExp, err := otlptracehttp.New(ctx)
	if err != nil {
		return nil, err
	}
	// Without an explicit sampler, OTEL_TRACES_SAMPLER / _ARG apply.
	tpOpts := []sdktrace.TracerProviderOption{sdktrace.WithBatcher(traceExp), sdktrace.WithResource(res)}
	if o.onlyContinueTraces {
		tpOpts = append(tpOpts, sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.NeverSample())))
	}
	tp := sdktrace.NewTracerProvider(tpOpts...)
	otel.SetTracerProvider(tp)

	metricExp, err := otlpmetrichttp.New(ctx)
	if err != nil {
		return nil, err
	}
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExp, sdkmetric.WithInterval(15*time.Second))),
		sdkmetric.WithResource(res),
	)
	otel.SetMeterProvider(mp)

	return func(ctx context.Context) error {
		return errors.Join(tp.Shutdown(ctx), mp.Shutdown(ctx))
	}, nil
}
