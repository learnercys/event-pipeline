import json
from datetime import UTC, date

import pytest

from analytics.event import InvalidEvent, decode

VALID = {
    "id": "evt_1",
    "type": "payment.captured",
    "account_id": "acct_1",
    "amount_cents": 1250,
    "currency": "USD",
    "occurred_at": "2026-11-13T23:30:00-05:00",
}


def test_decode_valid():
    e = decode(json.dumps(VALID))
    assert e.amount_cents == 1250
    assert e.occurred_at.tzinfo == UTC
    # 23:30 in UTC-5 is the next day in UTC; daily stats bucket by UTC day.
    assert e.day == date(2026, 11, 14)


@pytest.mark.parametrize(
    "change",
    [
        {"id": ""},
        {"type": "payment.voided"},
        {"amount_cents": 0},
        {"amount_cents": True},
        {"amount_cents": 1.5},
        {"currency": "usd"},
        {"occurred_at": "yesterday"},
        {"occurred_at": "2026-11-13T00:00:00"},  # no timezone
        {"extra": 1},
    ],
)
def test_decode_rejects(change):
    with pytest.raises(InvalidEvent):
        decode(json.dumps({**VALID, **change}))


@pytest.mark.parametrize("body", ["not json", "[]", "{}"])
def test_decode_rejects_garbage(body):
    with pytest.raises(InvalidEvent):
        decode(body)
