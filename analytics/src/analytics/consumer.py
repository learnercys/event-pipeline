"""SQS consumer that builds daily payment totals."""

from __future__ import annotations

import logging
import time
from collections import Counter
from collections.abc import Callable
from typing import Any, Protocol

from analytics.chaos import Chaos
from analytics.event import InvalidEvent, decode
from analytics.store import SeenCache, Store

log = logging.getLogger(__name__)


class Queue(Protocol):
    def receive_message(self, **kwargs: Any) -> dict[str, Any]: ...
    def delete_message(self, **kwargs: Any) -> Any: ...


class Consumer:
    def __init__(
        self,
        *,
        name: str,
        queue: Queue,
        queue_url: str,
        store: Store,
        cache: SeenCache,
        chaos: Chaos | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.name = name
        self.queue = queue
        self.queue_url = queue_url
        self.store = store
        self.cache = cache
        self.chaos = chaos or Chaos()
        self.stats: Counter[str] = Counter()
        self.sleep = sleep
        # Circuit breaker: while the database is failing we stop receiving.
        # Every receive counts toward the redrive limit, so polling during an
        # outage would push healthy messages into the dead-letter queue.
        self.degraded = False

    def poll_once(self) -> int:
        """Long-poll for up to 10 messages and handle them."""
        if self.degraded:
            self.wait_for_database()
            return 0
        out = self.queue.receive_message(
            QueueUrl=self.queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=10
        )
        messages = out.get("Messages", [])
        for message in messages:
            if self.degraded:
                break  # the rest come back after the visibility timeout
            self.handle(message)
        return len(messages)

    def wait_for_database(self, max_backoff: float = 5.0) -> None:
        """Probe with backoff; resume only once the database answers."""
        backoff = 0.25
        while True:
            try:
                self.store.ping()
            except Exception as exc:
                log.info("database still unavailable (%s), retrying in %.2fs", exc, backoff)
                self.sleep(backoff)
                backoff = min(backoff * 2, max_backoff)
                continue
            log.info("database is back, resuming")
            self.degraded = False
            return

    def handle(self, message: dict[str, Any]) -> None:
        """Process one message; delete it only once its effect is durable."""
        try:
            event = decode(message["Body"])
        except InvalidEvent as exc:
            # Leave it: the redrive policy moves it to the DLQ.
            self.stats["poison"] += 1
            log.warning("poison message %s: %s", message.get("MessageId"), exc)
            return

        if self.cache.seen(event.id):
            # Fast path: we know it was committed, skip the database round trip.
            self.stats["duplicates_cache"] += 1
            self._ack(message)
            return

        try:
            applied = self.store.apply(self.name, event)
        except Exception:
            self.stats["failed"] += 1
            log.exception("apply failed for %s, pausing consumption", event.id)
            self.degraded = True
            return

        self.stats["applied" if applied else "duplicates_db"] += 1
        # Mark only after commit. Marking first would lose the event if we
        # crashed between the mark and the commit: the retry would be skipped.
        self.cache.mark(event.id)

        if self.chaos.should(f"{self.name}.crash_after_commit"):
            return  # simulated crash before the ack: SQS will redeliver

        self._ack(message)

    def _ack(self, message: dict[str, Any]) -> None:
        try:
            self.queue.delete_message(
                QueueUrl=self.queue_url, ReceiptHandle=message["ReceiptHandle"]
            )
        except Exception as exc:  # harmless: redelivery is deduplicated
            log.warning("delete failed, message will be redelivered: %s", exc)
