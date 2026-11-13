# 4. Use Redis as a dedupe *cache*, never as the source of truth

**Status:** accepted

## Context

The analytics consumer sees many duplicates: relay republishes, provider
retries, and redeliveries. Each one costs a Postgres transaction just to find
out there's nothing to do. A Redis lookup is cheaper.

The tempting design uses `SETNX seen:{id}` as the dedupe check. It is wrong:

```
SETNX seen:evt_1   → OK (first time)
… crash before the Postgres commit …
redelivery: SETNX seen:evt_1 → already set → skipped   ← event lost forever
```

Redis also loses keys through eviction, TTL expiry and failover, and a Redis
write can't join a Postgres transaction.

## Decision

- Postgres (`processed_messages`) stays the **only** authority on what was
  applied ([ADR 2](0002-idempotent-consumers.md)).
- Redis is a **read-through hint**. Check `EXISTS seen:{consumer}:{id}` first.
  On a hit, the event was committed, so delete the message and skip the
  database entirely.
- The key is written **after** the Postgres commit, never before.
- Any Redis error is treated as a miss, so the consumer falls through to
  Postgres.

## Consequences

- A lost Redis key costs one extra Postgres round trip, never correctness.
- A crash between the commit and the Redis write is harmless. The redelivery
  misses the cache and is deduplicated by Postgres.
- The tests cover each of these: cache hit skips the DB, Redis down falls back
  to Postgres, and a failed commit never marks the cache.
- In the chaos run, most duplicates are answered by Redis, which shows up as
  `duplicates_cache` in the consumer's stats log.
