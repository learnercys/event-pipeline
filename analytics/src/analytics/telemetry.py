"""OpenTelemetry setup for the analytics consumer, mirroring internal/telemetry.

Configured with the standard OTEL_* environment variables. With no exporter
endpoint nothing is exported, but trace context is still read from messages.
"""

from __future__ import annotations

import os
from typing import Any

from opentelemetry import propagate, trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace.sampling import ALWAYS_OFF, ParentBased

tracer = trace.get_tracer("analytics.consumer")


def setup(service_name: str = "analytics-consumer") -> TracerProvider | None:
    """Install a tracer provider that exports over OTLP/HTTP.

    Like the Go relay and ledger consumer, this process only records spans
    that continue an existing trace: its idle polling would otherwise start a
    new trace every few seconds.
    """
    if not os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT"):
        return None
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    from opentelemetry.instrumentation.psycopg import PsycopgInstrumentor
    from opentelemetry.instrumentation.redis import RedisInstrumentor

    os.environ.setdefault("OTEL_SERVICE_NAME", service_name)
    provider = TracerProvider(
        resource=Resource.create(),  # reads OTEL_SERVICE_NAME, OTEL_RESOURCE_ATTRIBUTES
        sampler=ParentBased(root=ALWAYS_OFF),
    )
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
    trace.set_tracer_provider(provider)
    PsycopgInstrumentor().instrument()  # one span per query
    RedisInstrumentor().instrument()  # one span per cache lookup/write
    return provider


def extract(message_attributes: dict[str, Any] | None):
    """Trace context carried in SQS message attributes (W3C traceparent)."""
    carrier = {
        k: v["StringValue"]
        for k, v in (message_attributes or {}).items()
        if isinstance(v, dict) and "StringValue" in v
    }
    return propagate.extract(carrier)
