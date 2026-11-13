import json

import pytest

from analytics.chaos import Chaos
from analytics.consumer import Consumer
from analytics.event import Event

EVENT = json.dumps(
    {
        "id": "evt_1",
        "type": "payment.captured",
        "account_id": "acct_1",
        "amount_cents": 700,
        "currency": "USD",
        "occurred_at": "2026-11-13T00:00:00Z",
    }
)


class FakeQueue:
    def __init__(self):
        self.deleted = []
        self.receives = 0

    def receive_message(self, **_):
        self.receives += 1
        return {}

    def delete_message(self, *, QueueUrl, ReceiptHandle):  # noqa: N803 (boto3 casing)
        self.deleted.append(ReceiptHandle)


class FakeStore:
    """Behaves like processed_messages + account_daily_stats in one transaction."""

    def __init__(self, fail=False):
        self.seen: set[tuple[str, str]] = set()
        self.totals: dict[str, int] = {}
        self.calls = 0
        self.fail = fail
        self.pings = 0

    def ping(self):
        self.pings += 1
        if self.pings < 3:
            raise RuntimeError("still down")
        self.fail = False

    def apply(self, consumer: str, event: Event) -> bool:
        self.calls += 1
        if self.fail:
            raise RuntimeError("db down")
        if (consumer, event.id) in self.seen:
            return False
        self.seen.add((consumer, event.id))
        self.totals[event.account_id] = self.totals.get(event.account_id, 0) + event.amount_cents
        return True


class FakeCache:
    def __init__(self, broken=False):
        self.keys: set[str] = set()
        self.broken = broken

    def seen(self, event_id):
        return not self.broken and event_id in self.keys

    def mark(self, event_id):
        if not self.broken:
            self.keys.add(event_id)


def msg(handle, body=EVENT):
    return {"MessageId": handle, "ReceiptHandle": handle, "Body": body}


def make(store=None, cache=None, chaos=None):
    queue = FakeQueue()
    c = Consumer(
        name="analytics",
        queue=queue,
        queue_url="q",
        store=store or FakeStore(),
        cache=cache or FakeCache(),
        chaos=chaos,
        sleep=lambda _: None,
    )
    return c, queue


def test_duplicate_is_skipped_by_cache_without_touching_db():
    store = FakeStore()
    c, queue = make(store=store)
    c.handle(msg("r1"))
    c.handle(msg("r2"))
    assert store.totals == {"acct_1": 700}
    assert store.calls == 1, "second delivery should be answered by the cache"
    assert queue.deleted == ["r1", "r2"]


def test_cache_down_falls_back_to_postgres_dedupe():
    store = FakeStore()
    c, _ = make(store=store, cache=FakeCache(broken=True))
    c.handle(msg("r1"))
    c.handle(msg("r2"))
    assert store.totals == {"acct_1": 700}
    assert c.stats["duplicates_db"] == 1


def test_failed_commit_does_not_mark_cache_or_ack():
    cache = FakeCache()
    c, queue = make(store=FakeStore(fail=True), cache=cache)
    c.handle(msg("r1"))
    assert cache.keys == set(), "marking before commit would lose the event on retry"
    assert queue.deleted == []
    assert c.degraded, "a database failure should open the circuit breaker"


def test_breaker_stops_receiving_until_database_recovers():
    store = FakeStore(fail=True)
    c, queue = make(store=store)
    c.degraded = True

    assert c.poll_once() == 0
    assert queue.receives == 0, "must not receive while the database is down"
    assert store.pings == 3 and not c.degraded

    c.poll_once()
    assert queue.receives == 1


def test_crash_after_commit_then_redelivery():
    store = FakeStore()
    c, queue = make(store=store, chaos=Chaos("analytics.crash_after_commit=1"))
    c.handle(msg("r1"))
    assert queue.deleted == []

    c.chaos = Chaos()
    c.handle(msg("r2"))
    assert store.totals == {"acct_1": 700}
    assert queue.deleted == ["r2"]


def test_poison_message_is_left_for_dlq():
    c, queue = make()
    c.handle(msg("r1", body="{oops"))
    assert queue.deleted == []
    assert c.stats["poison"] == 1


@pytest.mark.parametrize("spec", ["a", "a=x", "a=2"])
def test_chaos_rejects_bad_spec(spec):
    with pytest.raises(ValueError):
        Chaos(spec)


def test_process_span_continues_the_message_trace(monkeypatch):
    from opentelemetry import trace
    from opentelemetry.propagate import inject
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    tracer = provider.get_tracer("test")

    with tracer.start_as_current_span("publish") as publish:
        carrier: dict[str, str] = {}
        inject(carrier)

    import analytics.telemetry as tel

    monkeypatch.setattr(tel, "tracer", tracer)
    c, _ = make()
    c.queue_url = "http://sqs/123/analytics-events"
    m = msg("r1")
    m["MessageAttributes"] = {
        "traceparent": {"DataType": "String", "StringValue": carrier["traceparent"]}
    }
    c.handle(m)

    process = exporter.get_finished_spans()[-1]
    assert process.name == "process analytics-events"
    assert process.kind == trace.SpanKind.CONSUMER
    assert process.parent.span_id == publish.get_span_context().span_id
    assert process.context.trace_id == publish.get_span_context().trace_id
    assert process.attributes["event.id"] == "evt_1"
