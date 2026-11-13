# 1. Publish through a transactional outbox

**Status:** accepted

## Context

The ingest API has to do two things for every new event: store it in Postgres
and publish it to SNS. Those are two different systems, and there is no
transaction that spans both.

Doing them one after the other leaves a gap whichever order you pick:

| Order | Crash in between | Result |
|---|---|---|
| write DB, then publish | after the commit | event stored, **never published** |
| publish, then write DB | after the publish | consumers see an event **that doesn't exist** |

Two-phase commit isn't available for SNS, and even where 2PC exists it couples
the API's availability to the broker's.

## Decision

The API writes the event **and** an `outbox` row in the same Postgres
transaction. A separate relay process reads unpublished outbox rows, publishes
them to SNS, and then marks them published.

```
ingest:  BEGIN; INSERT events; INSERT outbox; COMMIT
relay:   BEGIN; SELECT … FOR UPDATE SKIP LOCKED; publish to SNS; UPDATE published_at; COMMIT
```

## Consequences

- An accepted event is **always** published eventually. A failed request
  publishes nothing.
- Delivery is **at-least-once**. If the relay dies after SNS accepts a batch
  but before its commit, the batch is published again. Consumers must be
  idempotent ([ADR 2](0002-idempotent-consumers.md)). The chaos test injects
  exactly this crash (`relay.crash_after_publish`).
- `FOR UPDATE SKIP LOCKED` lets several relay replicas share the work without
  publishing the same row at the same time.
- A partially failed `PublishBatch` aborts the whole batch. That is simpler
  than tracking per-entry results, and correct, because duplicates are
  harmless.
- There is a delay of one poll interval (200 ms by default). Postgres
  `LISTEN/NOTIFY` or logical replication (CDC, as in Debezium) would remove
  it. That's a reasonable next step, but more moving parts than this project
  needs.
- The outbox grows forever. In production, a periodic job deletes published
  rows older than N days. The partial index `WHERE published_at IS NULL` keeps
  the relay's query fast in the meantime.
