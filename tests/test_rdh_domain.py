"""Slice 1: values, expiry, digest, idempotency, latch, exit codes. No Qt or device."""

from __future__ import annotations

import threading
from datetime import datetime, timedelta, timezone

import pytest

from rdh.adapters.clock import FakeClock
from rdh.domain.model import DeviceOutcome, Effect, Phase, Scope
from rdh.domain.policy import (
    ExecutionLatch,
    IdempotencyConflict,
    IdempotencyFull,
    IdempotencyTable,
    SessionGate,
    canonical_instant,
    decide_deadline,
    exit_code,
    make_receipt,
    operation_digest,
    uart_impact,
)


def _expires(clock: FakeClock, seconds: float) -> str:
    return (clock.wall() + timedelta(seconds=seconds)).strftime("%Y-%m-%dT%H:%M:%SZ")


def test_canonical_instant_requires_timezone():
    assert canonical_instant("2026-11-08T04:00:00Z") == "2026-11-08T04:00:00Z"
    assert canonical_instant("2026-11-08T12:00:00+08:00") == "2026-11-08T04:00:00Z"
    with pytest.raises(ValueError):
        canonical_instant("2026-11-08T04:00:00")


def test_deadline_bounds_and_untrusted_clock():
    clock = FakeClock()
    ok = decide_deadline(clock, _expires(clock, 30), max_wait_s=120, skew_s=30)
    assert ok.ok and ok.deadline_mono == pytest.approx(clock.mono() + 30)
    too_far = decide_deadline(clock, _expires(clock, 121), max_wait_s=120, skew_s=30)
    assert not too_far.ok and too_far.reason == "expiry_too_far"
    late = decide_deadline(clock, _expires(clock, -31), max_wait_s=120, skew_s=30)
    assert late.reason == "expired"
    grace = decide_deadline(clock, _expires(clock, -10), max_wait_s=120, skew_s=30)
    assert grace.ok and grace.deadline_mono == pytest.approx(clock.mono() + 0.05)
    clock.set_trusted(False)
    refused = decide_deadline(clock, _expires(clock, 10), max_wait_s=120, skew_s=30)
    assert refused.reason == "clock_untrusted"


def test_digest_binds_action_session_expiry_and_bytes():
    base = operation_digest("uart.write", "t-1", "2026-11-08T04:01:00Z", b"\x01\x02")
    assert base == operation_digest("uart.write", "t-1", "2026-11-08T04:01:00Z", b"\x01\x02")
    assert base != operation_digest("uart.write", "t-1", "2026-11-08T04:01:00Z", b"\x01\x03")
    assert base != operation_digest("uart.write", "t-2", "2026-11-08T04:01:00Z", b"\x01\x02")
    assert base != operation_digest("uart.write", "t-1", "2026-11-08T04:02:00Z", b"\x01\x02")
    assert base != operation_digest("adb.exec", "t-1", "2026-11-08T04:01:00Z", b"\x01\x02")


def test_exit_codes_do_not_treat_pending_as_success():
    assert exit_code(Phase.DENIED) == 2
    assert exit_code(Phase.EXPIRED) == 2
    assert exit_code(Phase.REJECTED) == 4
    assert exit_code(Phase.CONFLICT) == 4
    assert exit_code(Phase.UNSUPPORTED) == 4
    assert exit_code(Phase.WRITTEN) == 0
    assert exit_code(Phase.DEVICE_OK) == 0
    assert exit_code(Phase.PARTIAL) == 5
    assert exit_code(Phase.DEVICE_FAILED) == 5
    assert exit_code(Phase.DELIVERY_UNKNOWN) == 3
    assert exit_code(Phase.AWAITING) == 3
    assert exit_code(Phase.ACCEPTED) == 3
    assert exit_code(Phase.BUSY) == 6
    receipt = make_receipt(phase=Phase.WRITTEN, success_scope=Scope.TRANSPORT, device_outcome=DeviceOutcome.UNKNOWN, effect=Effect.TRANSPORT)
    assert receipt.device_outcome == DeviceOutcome.UNKNOWN
    assert "设备执行成功" not in uart_impact(4)


def test_idempotency_conflict_replay_and_no_eviction():
    table = IdempotencyTable(2)
    first = make_receipt(phase=Phase.WRITTEN, operation_id="op-a", written_bytes=2)
    table.remember("eng", "op-a", "digest-a", first)
    table.remember("eng", "op-b", "digest-b", make_receipt(phase=Phase.DENIED, operation_id="op-b"))
    assert table.resolve("eng", "op-c", "digest-c") == ("full", None)
    status, receipt = table.resolve("eng", "op-a", "digest-a")
    assert status == "replay" and receipt is not None and receipt.written_bytes == 2
    status, _ = table.resolve("eng", "op-a", "digest-other")
    assert status == "conflict"
    with pytest.raises(IdempotencyFull):
        table.remember("eng", "op-c", "digest-c", first)
    with pytest.raises(IdempotencyConflict):
        table.remember("eng", "op-a", "changed", first)
    assert len(table) == 2
    assert not any(name in ("evict", "pop", "delete", "discard") for name in dir(table) if not name.startswith("_"))
    # A replay after a rejected insert is still the executed record.
    again, receipt = table.resolve("eng", "op-a", "digest-a")
    assert again == "replay" and receipt.phase == Phase.WRITTEN


def test_latch_cancel_and_begin_are_mutually_exclusive():
    clock = FakeClock()
    expires = _expires(clock, 30)
    for _ in range(300):
        gate = SessionGate()
        gate.set_session("sess")
        latch = ExecutionLatch(gate, "sess", clock.mono() + 30, clock, canonical_instant(expires), 30)
        barrier = threading.Barrier(2)
        results: dict[str, str] = {}

        def begin():
            barrier.wait()
            results["begin"] = latch.try_begin()

        def cancel():
            barrier.wait()
            results["cancel"] = latch.cancel()

        threads = (threading.Thread(target=begin), threading.Thread(target=cancel))
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        if results["begin"] == "began":
            assert results["cancel"] == "executing"
        else:
            assert results["begin"] == "cancelled"
            assert results["cancel"] == "cancelled"


def test_latch_expires_and_session_change_before_begin():
    clock = FakeClock()
    expires = canonical_instant(_expires(clock, 5))
    gate = SessionGate()
    gate.set_session("sess")
    latch = ExecutionLatch(gate, "sess", clock.mono() + 5, clock, expires, 30)
    clock.advance(6)
    assert latch.try_begin() == "expired"
    gate.set_session("sess")
    fresh = ExecutionLatch(gate, "sess", clock.mono() + 30, clock, canonical_instant(_expires(clock, 20)), 30)
    gate.set_session("other")
    assert fresh.try_begin() == "cancelled"
    clock.set_trusted(False)
    gate.set_session("sess-2")
    blocked = ExecutionLatch(gate, "sess-2", clock.mono() + 30, clock, canonical_instant(_expires(clock, 20)), 30)
    assert blocked.try_begin() == "clock_untrusted"
