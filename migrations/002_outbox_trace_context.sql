-- W3C trace context of the request that created the event, so the relay can
-- continue the same trace when it publishes (see internal/telemetry).
ALTER TABLE outbox ADD COLUMN trace_context jsonb;
