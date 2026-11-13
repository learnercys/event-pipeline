# 5. Carry one trace across the outbox, SNS and SQS

**Status:** accepted

## Context

A payment event passes through four processes in two languages: the ingest
API, the outbox relay, and the ledger (Go) and analytics (Python) consumers.
Between them are a database table and two message brokers, none of which
carries HTTP headers. Without help, each process would start its own trace,
and "why did this event take 6 seconds to reach analytics?" would have no
answer.

## Decision

Propagate W3C trace context (`traceparent`/`tracestate`) by hand at each hop:

| Hop | Carrier | Code |
|---|---|---|
| ingest → relay | `outbox.trace_context` (JSONB), written in the same transaction as the event | `telemetry.Serialize` / `Deserialize` |
| relay → SNS | message attributes on each `PublishBatch` entry | `telemetry.InjectSNS` |
| SNS → SQS | raw message delivery copies the attributes | (nothing to do) |
| SQS → consumers | `MessageAttributeNames: ["All"]` on receive | `telemetry.ExtractSQS` (Go), `telemetry.extract` (Python) |

Spans follow the OpenTelemetry messaging conventions: the relay emits a
`publish payment-events` **producer** span as a child of the ingest request,
and each consumer emits a `process <queue>` **consumer** span as a child of
the publish. Database queries (`otelpgx`, psycopg), Redis calls and the SQS
delete nest under those spans.

**Polling processes only continue traces.** The relay and consumers run a
query or receive call every few hundred milliseconds while idle. With the
default sampler each would become its own one-span trace. Their tracer uses
`ParentBased(root = never sample)`: work that arrives with a trace context is
recorded, and idle loops aren't.

## Consequences

- The trace shows the pipeline's real latency structure: milliseconds in each
  process, with the outbox polling interval and queue time as the visible gaps
  between spans.
- A child-of relationship from consumer to producer is right for one message
  per trace. A batch consumer processing many events at once would use span
  *links* instead.
- Duplicates are visible: a redelivered message produces a second `process`
  span with `event.duplicate=true` in the same trace.
- Poison messages published without trace context (like the chaos test's)
  aren't traced. Their DLQ alarm is the signal for those.
- Without `OTEL_EXPORTER_OTLP_ENDPOINT` the SDK isn't installed, so tests and
  docker-compose runs pay nothing. The propagation helpers are unit-tested to
  keep one trace ID across every hop.
