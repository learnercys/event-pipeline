"""Payment event model, mirroring internal/event in the Go services."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import UTC, date, datetime

PAYMENT_CAPTURED = "payment.captured"
REFUND_ISSUED = "refund.issued"

_FIELDS = {"id", "type", "account_id", "amount_cents", "currency", "occurred_at"}
_CURRENCY = re.compile(r"^[A-Z]{3}$")


class InvalidEvent(ValueError):
    """The message can never be processed, no matter how often it is retried."""


@dataclass(frozen=True)
class Event:
    id: str
    type: str
    account_id: str
    amount_cents: int
    currency: str
    occurred_at: datetime

    @property
    def day(self) -> date:
        return self.occurred_at.astimezone(UTC).date()


def decode(body: str) -> Event:
    """Parse and validate a JSON event. Unknown fields are rejected."""
    try:
        raw = json.loads(body)
    except json.JSONDecodeError as exc:
        raise InvalidEvent(f"not JSON: {exc}") from exc
    if not isinstance(raw, dict):
        raise InvalidEvent("event must be a JSON object")
    if unknown := raw.keys() - _FIELDS:
        raise InvalidEvent(f"unknown fields: {sorted(unknown)}")
    if missing := _FIELDS - raw.keys():
        raise InvalidEvent(f"missing fields: {sorted(missing)}")

    if not isinstance(raw["id"], str) or not 0 < len(raw["id"]) <= 128:
        raise InvalidEvent("id must be 1-128 characters")
    if raw["type"] not in (PAYMENT_CAPTURED, REFUND_ISSUED):
        raise InvalidEvent(f"unknown type {raw['type']!r}")
    if not isinstance(raw["account_id"], str) or not 0 < len(raw["account_id"]) <= 128:
        raise InvalidEvent("account_id must be 1-128 characters")
    amount = raw["amount_cents"]
    if not isinstance(amount, int) or isinstance(amount, bool) or amount <= 0:
        raise InvalidEvent("amount_cents must be a positive integer")
    if not isinstance(raw["currency"], str) or not _CURRENCY.match(raw["currency"]):
        raise InvalidEvent("currency must be a 3-letter ISO code")
    try:
        occurred_at = datetime.fromisoformat(raw["occurred_at"])
    except (TypeError, ValueError) as exc:
        raise InvalidEvent("occurred_at must be an RFC 3339 timestamp") from exc
    if occurred_at.tzinfo is None:
        raise InvalidEvent("occurred_at must include a timezone")

    return Event(
        id=raw["id"],
        type=raw["type"],
        account_id=raw["account_id"],
        amount_cents=amount,
        currency=raw["currency"],
        occurred_at=occurred_at.astimezone(UTC),
    )
