-- Accepted events. The provider's event ID is the idempotency key, so a
-- retried webhook can never create a second row.
CREATE TABLE events (
    id           text        PRIMARY KEY,
    type         text        NOT NULL CHECK (type IN ('payment.captured', 'refund.issued')),
    account_id   text        NOT NULL,
    amount_cents bigint      NOT NULL CHECK (amount_cents > 0),
    currency     char(3)     NOT NULL,
    occurred_at  timestamptz NOT NULL,
    received_at  timestamptz NOT NULL DEFAULT now()
);

-- Transactional outbox: written in the same transaction as the event, then
-- published to SNS by the relay. See docs/adr/0001-transactional-outbox.md.
CREATE TABLE outbox (
    id           bigserial   PRIMARY KEY,
    event_id     text        NOT NULL REFERENCES events (id),
    payload      jsonb       NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz
);

-- Keeps "what is left to publish" cheap no matter how large the table grows.
CREATE INDEX outbox_unpublished ON outbox (id) WHERE published_at IS NULL;

-- Dedupe table shared by every consumer. A consumer applies an event only if
-- it can insert (consumer, event_id) in the same transaction as its effect.
-- See docs/adr/0002-idempotent-consumers.md.
CREATE TABLE processed_messages (
    consumer     text        NOT NULL,
    event_id     text        NOT NULL,
    processed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer, event_id)
);

-- Ledger consumer (Go): running balance per account and currency.
CREATE TABLE balances (
    account_id   text        NOT NULL,
    currency     char(3)     NOT NULL,
    amount_cents bigint      NOT NULL,
    updated_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (account_id, currency)
);

-- Analytics consumer (Python): daily totals per account and currency.
CREATE TABLE account_daily_stats (
    account_id     text    NOT NULL,
    day            date    NOT NULL,
    currency       char(3) NOT NULL,
    captured_cents bigint  NOT NULL DEFAULT 0,
    refunded_cents bigint  NOT NULL DEFAULT 0,
    event_count    integer NOT NULL DEFAULT 0,
    PRIMARY KEY (account_id, day, currency)
);
