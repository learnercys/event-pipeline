# 3. Model consumer state so that order doesn't matter

**Status:** accepted

## Context

SQS standard queues don't preserve order. Neither do retries: a refund can be
processed before the capture it refunds, if the capture's message was
redelivered.

FIFO queues with a `MessageGroupId` per account would preserve order, but:

- throughput per group is limited, and one slow message blocks its whole
  group;
- ordering only holds inside SQS. The relay, provider retries and
  redeliveries can still reorder events before they reach the queue.

## Decision

Consumer state is built from **commutative, additive updates**:

- ledger: `balance += signed_amount`
- analytics: `captured += x`, `refunded += y`, `count += 1`

Applying the same set of events in any order gives the same result. Combined
with exactly-once effects ([ADR 2](0002-idempotent-consumers.md)), the final
state depends only on *which* events arrived, not *when*.

## Consequences

- Standard queues are fine, with no head-of-line blocking.
- Balances can go briefly negative (refund before capture). For a read model
  that's acceptable. A system that must *reject* operations, like "insufficient
  funds", needs ordering or a real ledger with a sequence per account. That's
  the next portfolio project.
- Non-commutative state (for example "latest status of an order") would need
  a version or `occurred_at` comparison: `UPDATE … WHERE version < $new`.
