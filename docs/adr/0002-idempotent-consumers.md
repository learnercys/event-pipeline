# 2. Make every consumer idempotent with a dedupe table

**Status:** accepted

## Context

Duplicates reach consumers from three directions:

1. **The provider** retries webhooks. A lost HTTP response after a successful
   commit looks like a failure to the sender.
2. **The relay** republishes a batch if it dies between publishing and
   committing ([ADR 1](0001-transactional-outbox.md)).
3. **SQS** is at-least-once. It also redelivers any message that isn't deleted
   before its visibility timeout, for example when a consumer dies after doing
   the work but before deleting the message.

"Exactly-once delivery" isn't on offer. What we can get is **exactly-once
effects**.

## Decision

The provider's event ID is the idempotency key end to end:

- **Ingest:** `INSERT … ON CONFLICT (id) DO NOTHING`. A repeat of the same
  event returns `200 duplicate`. Reusing an ID with *different* data returns
  `409 Conflict`, because a provider bug like that has to surface, not get
  deduplicated away.
- **Consumers:** each consumer, in **one transaction**, inserts
  `(consumer, event_id)` into `processed_messages` with `ON CONFLICT DO
  NOTHING`, applies its effect only if the insert succeeded, and commits. The
  SQS message is deleted only **after** that commit.

```
BEGIN
  INSERT INTO processed_messages (consumer, event_id) … ON CONFLICT DO NOTHING
  -- 0 rows? already applied → COMMIT, ack, done
  UPDATE balances …
COMMIT
DeleteMessage   -- crash before this? SQS redelivers; the insert above makes it a no-op
```

## Consequences

- Because the dedupe row and the effect share one transaction, there is no
  state where one exists without the other.
- The key is namespaced by consumer, so each consumer dedupes independently on
  the same table.
- `processed_messages` grows with traffic. Production would expire rows older
  than the longest possible redelivery window (SQS keeps messages for at most
  14 days).
- The approach needs the effect to live in the same database as the dedupe
  table. If an effect is external (an email, a third-party API), the external
  call needs its own idempotency key instead.

## Poison messages and the DLQ

A message that can never be decoded is left on the queue (not deleted). After
`maxReceiveCount` receives, SQS moves it to that consumer's dead-letter queue
for a human to inspect.

That has a sharp edge: **transient** failures count toward `maxReceiveCount`
too. The first chaos run restarted Postgres under load, and consumers kept
receiving messages they couldn't process. Valid events burned all their
attempts and landed in the DLQ. The fix is a circuit breaker. After a database
error, a consumer stops receiving and probes the database with backoff, and
only resumes once it answers. The verifier now checks that **no unapplied
event ever reaches a DLQ**.
