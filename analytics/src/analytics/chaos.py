"""Fault injection, same CHAOS format as internal/chaos in Go.

CHAOS="analytics.crash_after_commit=0.2"
"""

from __future__ import annotations

import random
from collections import Counter


class Chaos:
    def __init__(self, spec: str = "", rng: random.Random | None = None) -> None:
        self._rates: dict[str, float] = {}
        self._rng = rng or random.Random()
        self.injected: Counter[str] = Counter()
        for part in spec.split(","):
            part = part.strip()
            if not part:
                continue
            name, sep, value = part.partition("=")
            if not sep:
                raise ValueError(f"chaos: {part!r} is not name=probability")
            p = float(value)
            if not 0 <= p <= 1:
                raise ValueError(f"chaos: probability for {name!r} must be between 0 and 1")
            self._rates[name.strip()] = p

    @property
    def enabled(self) -> bool:
        return bool(self._rates)

    def should(self, point: str) -> bool:
        p = self._rates.get(point, 0.0)
        if p and self._rng.random() < p:
            self.injected[point] += 1
            return True
        return False
