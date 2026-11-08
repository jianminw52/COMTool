"""Expiry, digest, idempotency, execution latch, and CLI exit codes.

No I/O and no framework types. The latch is the only mutable object here,
and it tracks execution progress rather than a second copy of authorization.
"""

from __future__ import annotations

import hashlib
import threading
from dataclasses import dataclass
from datetime import datetime, timezone

from rdh.domain.model import (
    DeviceOutcome,
    Effect,
    Phase,
    Receipt,
    Scope,
    TERMINAL_PHASES,
)


class PolicyError(Exception):
    pass


class IdempotencyFull(PolicyError):
    pass


class IdempotencyConflict(PolicyError):
    pass


@dataclass(frozen=True)
class DeadlineDecision:
    ok: bool
    reason: str
    deadline_mono: float | None
    canonical: str | None


def canonical_instant(text: str) -> str:
    raw = text.strip()
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    dt = datetime.fromisoformat(raw)
    if dt.tzinfo is None:
        raise ValueError("expires_at requires a timezone")
    dt = dt.astimezone(timezone.utc).replace(microsecond=0)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_canonical(canonical: str) -> datetime:
    return datetime.strptime(canonical, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


def decide_deadline(clock, expires_text: str, *, max_wait_s: float, skew_s: float) -> DeadlineDecision:
    """Convert an absolute expiry into a monotonic deadline.

    A clock that reports itself untrusted cannot authorize a write.
    Expiry farther than ``max_wait_s`` is rejected rather than clamped.
    Lateness inside ``skew_s`` gets a 50 ms grace, not a new long life.
    """

    if not clock.trusted():
        return DeadlineDecision(False, "clock_untrusted", None, None)
    try:
        canonical = canonical_instant(expires_text)
    except (ValueError, TypeError):
        return DeadlineDecision(False, "bad_expires", None, None)
    exp = _parse_canonical(canonical)
    now = clock.wall()
    if now.tzinfo is None:
        return DeadlineDecision(False, "clock_untrusted", None, canonical)
    delta = (exp - now).total_seconds()
    if delta > max_wait_s:
        return DeadlineDecision(False, "expiry_too_far", None, canonical)
    if -delta > skew_s:
        return DeadlineDecision(False, "expired", None, canonical)
    remain = 0.05 if delta <= 0 else delta
    return DeadlineDecision(True, "ok", clock.mono() + remain, canonical)


def operation_digest(action: str, target_session: str, expires_canonical: str, payload: bytes) -> str:
    hasher = hashlib.sha256()
    hasher.update(action.encode("utf-8"))
    hasher.update(b"\0")
    hasher.update(target_session.encode("utf-8"))
    hasher.update(b"\0")
    hasher.update(expires_canonical.encode("utf-8"))
    hasher.update(b"\0")
    hasher.update(payload)
    return hasher.hexdigest()


def payload_sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def uart_impact(nbytes: int) -> str:
    return (
        f"将向当前 UART 写出 {nbytes} 字节。"
        "批准只覆盖这一次、这一份字节。"
        "驱动收下字节只说明传输完成（success_scope=transport），"
        "设备是否执行未知（device_outcome=unknown），没有设备退出码。"
        "不会自动补发或重试。"
    )


def hex_preview(payload: bytes, limit: int = 64) -> str:
    shown = payload[:limit].hex()
    if len(payload) > limit:
        return f"{shown}… 共 {len(payload)} 字节"
    return shown


def is_terminal(phase: str) -> bool:
    return phase in TERMINAL_PHASES


def exit_code(phase: str) -> int:
    """Map a terminal business phase to the new CLI code.

    ``denied`` / ``expired`` are 2. ``rejected`` / ``conflict`` /
    ``unsupported`` are 4. A non-terminal phase is not success: callers
    that must stop waiting use 3.
    """

    if phase in (Phase.DENIED, Phase.EXPIRED):
        return 2
    if phase in (Phase.REJECTED, Phase.CONFLICT, Phase.UNSUPPORTED):
        return 4
    if phase in (Phase.WRITTEN, Phase.DEVICE_OK):
        return 0
    if phase in (Phase.PARTIAL, Phase.DEVICE_FAILED):
        return 5
    if phase in (Phase.UNKNOWN, Phase.DELIVERY_UNKNOWN, Phase.ACCEPTED, Phase.AWAITING):
        return 3
    if phase == Phase.BUSY:
        return 6
    raise ValueError(f"unknown phase {phase}")


def make_receipt(**kwargs) -> Receipt:
    base = dict(
        request_id="",
        operation_id=None,
        phase=Phase.REJECTED,
        effect=Effect.NONE,
        reason="",
        success_scope=Scope.NONE,
        device_outcome=DeviceOutcome.NA,
        written_bytes=None,
        requested_bytes=None,
        digest=None,
        target_session=None,
        audit_persisted=None,
        gap=None,
        snapshot=None,
        candidates=None,
        diagnostic=None,
        unfinished=False,
    )
    base.update(kwargs)
    return Receipt(**base)


class IdempotencyTable:
    """In-process table keyed by principal + operation id.

    There is no eviction. A full table rejects new keys so an executed
    record cannot be dropped and then run again as a fresh operation.
    """

    def __init__(self, capacity: int):
        if capacity < 1:
            raise ValueError("capacity")
        self._capacity = capacity
        self._rows: dict[tuple[str, str], tuple[str, Receipt]] = {}

    def __len__(self) -> int:
        return len(self._rows)

    @property
    def capacity(self) -> int:
        return self._capacity

    def resolve(self, principal_id: str, operation_id: str, digest: str) -> tuple[str, Receipt | None]:
        row = self._rows.get((principal_id, operation_id))
        if row is None:
            if len(self._rows) >= self._capacity:
                return "full", None
            return "new", None
        stored_digest, receipt = row
        if stored_digest != digest:
            return "conflict", receipt
        return "replay", receipt

    def remember(self, principal_id: str, operation_id: str, digest: str, receipt: Receipt) -> None:
        key = (principal_id, operation_id)
        row = self._rows.get(key)
        if row is None:
            if len(self._rows) >= self._capacity:
                raise IdempotencyFull()
            self._rows[key] = (digest, receipt)
            return
        stored_digest, _receipt = row
        if stored_digest != digest:
            raise IdempotencyConflict()
        self._rows[key] = (digest, receipt)


class SessionGate:
    def __init__(self):
        self._lock = threading.Lock()
        self.session_id: str | None = None

    def set_session(self, session_id: str | None) -> None:
        with self._lock:
            self.session_id = session_id

    def get(self) -> str | None:
        with self._lock:
            return self.session_id


class ExecutionLatch:
    """Minimal ticket state shared with the device worker.

    ``try_begin`` runs inside the worker, before the blocking write, and
    the write itself must happen after these locks are released.
    """

    def __init__(self, gate: SessionGate, session_id: str, deadline_mono: float, clock, expires_at: str, skew_s: float):
        self._gate = gate
        self._lock = threading.Lock()
        self.session_id = session_id
        self.deadline_mono = deadline_mono
        self._clock = clock
        self.expires_at = expires_at
        self.skew_s = skew_s
        self._state = "armed"

    @property
    def state(self) -> str:
        with self._lock:
            return self._state

    def try_begin(self) -> str:
        with self._gate._lock:
            with self._lock:
                if self._state != "armed":
                    return self._state
                if self._gate.session_id != self.session_id:
                    self._state = "cancelled"
                    return "cancelled"
                if not self._clock.trusted():
                    self._state = "cancelled"
                    return "clock_untrusted"
                if self._clock.mono() >= self.deadline_mono:
                    self._state = "cancelled"
                    return "expired"
                try:
                    exp = _parse_canonical(self.expires_at)
                    late = (self._clock.wall() - exp).total_seconds()
                except (ValueError, TypeError):
                    self._state = "cancelled"
                    return "expired"
                if late > self.skew_s:
                    self._state = "cancelled"
                    return "expired"
                self._state = "executing"
                return "began"

    def cancel(self) -> str:
        with self._gate._lock:
            with self._lock:
                if self._state == "armed":
                    self._state = "cancelled"
                    return "cancelled"
                return self._state

    def finish(self) -> None:
        with self._lock:
            if self._state == "executing":
                self._state = "finished"
