"""Postgres and Redis access for the analytics consumer."""

from __future__ import annotations

import logging
from typing import Protocol

import psycopg
import redis

from analytics.event import PAYMENT_CAPTURED, Event

log = logging.getLogger(__name__)


class Store(Protocol):
    def apply(self, consumer: str, event: Event) -> bool:
        """Apply the event once per consumer. Returns False if already applied."""
        ...

    def ping(self) -> None:
        """Raise if the database is unreachable."""
        ...


class SeenCache(Protocol):
    def seen(self, event_id: str) -> bool: ...
    def mark(self, event_id: str) -> None: ...


class PostgresStore:
    """Source of truth for deduplication and for the daily totals."""

    def __init__(self, url: str) -> None:
        self._url = url
        self._conn = psycopg.connect(url, autocommit=False)

    def ping(self) -> None:
        self._reconnect_if_needed()
        self._conn.execute("SELECT 1")
        self._conn.rollback()

    def _reconnect_if_needed(self) -> None:
        if self._conn.broken or self._conn.closed:
            log.warning("postgres connection lost, reconnecting")
            self._conn = psycopg.connect(self._url, autocommit=False)

    def apply(self, consumer: str, event: Event) -> bool:
        self._reconnect_if_needed()
        # One transaction for the dedupe row and the aggregate: either both
        # are visible or neither is. A crash anywhere inside rolls back both.
        with self._conn.transaction():
            cur = self._conn.execute(
                """
                INSERT INTO processed_messages (consumer, event_id) VALUES (%s, %s)
                ON CONFLICT DO NOTHING
                """,
                (consumer, event.id),
            )
            if cur.rowcount == 0:
                return False

            captured = event.amount_cents if event.type == PAYMENT_CAPTURED else 0
            refunded = event.amount_cents - captured
            self._conn.execute(
                """
                INSERT INTO account_daily_stats
                    (account_id, day, currency, captured_cents, refunded_cents, event_count)
                VALUES (%s, %s, %s, %s, %s, 1)
                ON CONFLICT (account_id, day, currency) DO UPDATE SET
                    captured_cents = account_daily_stats.captured_cents + EXCLUDED.captured_cents,
                    refunded_cents = account_daily_stats.refunded_cents + EXCLUDED.refunded_cents,
                    event_count    = account_daily_stats.event_count + 1
                """,
                (event.account_id, event.day, event.currency, captured, refunded),
            )
            return True

    def close(self) -> None:
        self._conn.close()


class RedisSeenCache:
    """Fast-path dedupe. An optimisation only: Postgres stays authoritative.

    Redis can lose keys (eviction, TTL, failover), and a Redis write can't be
    part of the Postgres transaction. So a key is only written *after* the
    Postgres commit, and any Redis error just falls through to Postgres.
    See docs/adr/0004-redis-fast-path-dedupe.md.
    """

    def __init__(self, url: str, consumer: str, ttl_seconds: int = 24 * 3600) -> None:
        self._redis = redis.Redis.from_url(url, socket_timeout=0.5)
        self._prefix = f"seen:{consumer}:"
        self._ttl = ttl_seconds

    def seen(self, event_id: str) -> bool:
        try:
            return bool(self._redis.exists(self._prefix + event_id))
        except redis.RedisError as exc:
            log.warning("redis unavailable, falling back to postgres: %s", exc)
            return False

    def mark(self, event_id: str) -> None:
        try:
            self._redis.set(self._prefix + event_id, 1, ex=self._ttl)
        except redis.RedisError as exc:
            log.warning("redis mark failed (harmless): %s", exc)
