"""Clocks. ``trusted`` is an explicit input, not a claim that NTP was checked."""

from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone


class SystemClock:
    """Local wall clock plus a monotonic timer.

    ``clock_trust`` stays ``unverified_local`` at the snapshot layer.
    This object returns trusted=True so bounded expiry can run; it does
    not prove the clock is synchronized.
    """

    def wall(self) -> datetime:
        return datetime.now(timezone.utc)

    def mono(self) -> float:
        return time.monotonic()

    def trusted(self) -> bool:
        return True


class FakeClock:
    def __init__(self, wall: datetime | None = None, mono: float = 1_000.0, trusted: bool = True):
        self._wall = wall or datetime(2026, 11, 8, 4, 0, 0, tzinfo=timezone.utc)
        self._mono = mono
        self._trusted = trusted

    def wall(self) -> datetime:
        return self._wall

    def mono(self) -> float:
        return self._mono

    def trusted(self) -> bool:
        return self._trusted

    def set_trusted(self, value: bool) -> None:
        self._trusted = value

    def advance(self, seconds: float) -> None:
        self._wall += timedelta(seconds=seconds)
        self._mono += seconds
