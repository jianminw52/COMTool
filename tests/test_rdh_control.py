"""Slice 2: fake device, one state owner, approval, audit, queues."""

from __future__ import annotations

import json
import threading
import time
from datetime import timedelta

import pytest

from rdh.adapters.clock import FakeClock
from rdh.adapters.fake_device import FakeDevice
from rdh.app.control import Control, _reject_secrets
from rdh.domain.model import (
    LOCAL_PRINCIPAL,
    SYSTEM_PRINCIPAL,
    Connect,
    Decide,
    DeviceJob,
    DeviceOutcome,
    Disconnect,
    Discover,
    Effect,
    ExportDiagnostic,
    GrantView,
    Limits,
    Observe,
    Phase,
    Principal,
    QueryOperation,
    RelayLost,
    RequestOperation,
    Revoke,
    Scope,
    Shutdown,
    WriteOutcome,
)
from rdh.domain.policy import exit_code

REMOTE = Principal("eng", "remote", "mqtt_topic")


class MemoryAudit:
    def __init__(self):
        self.records = []
        self.fail = False
        self.block = None
        self.entered = threading.Event()

    def append_and_confirm(self, record):
        self.entered.set()
        if self.block is not None:
            self.block.wait(3)
        if self.fail:
            return False
        self.records.append(record)
        return True


def _wait(pred, timeout=2.0, detail=None):
    end = time.time() + timeout
    while time.time() < end:
        if pred():
            return
        time.sleep(0.01)
    raise AssertionError(detail() if detail else "timed out")


@pytest.fixture
def rig():
    clock = FakeClock()
    audit = MemoryAudit()
    device = FakeDevice()
    control = Control(clock=clock, audit=audit, device=device, host_id="h1", simulated=True)
    yield control, device, clock, audit
    device.resume()
    if audit.block is not None:
        audit.block.set()
    control.release_pump()
    control.shutdown_and_join(1.0)


def _expires(clock, seconds=30):
    return (clock.wall() + timedelta(seconds=seconds)).strftime("%Y-%m-%dT%H:%M:%SZ")


def _connect(control, **kwargs):
    receipt = control.submit(Connect(device="COM7", baudrate=115200, **kwargs), LOCAL_PRINCIPAL)
    assert receipt.reason == "opening", receipt
    _wait(lambda: control.local_snapshot().target_connected, detail=lambda: control.local_snapshot())
    return control.local_snapshot().target_session


def _request(control, clock, session, payload=b"\x01\x02\x03", operation_id="op-1", request_id="req-1", action="uart.write", expires_in=30):
    return control.submit(
        RequestOperation(request_id, operation_id, action, session, payload, _expires(clock, expires_in)),
        REMOTE,
    )


def _approve(control, receipt):
    return control.submit(
        Decide("decide-1", receipt.operation_id, receipt.digest, receipt.target_session, True),
        LOCAL_PRINCIPAL,
    )


def _recent(control, operation_id):
    for item in control.local_snapshot().recent:
        if item.operation_id == operation_id:
            return item
    return None


def test_discover_lists_without_opening_and_connect_is_explicit(rig):
    control, device, _clock, _audit = rig
    listed = control.submit(Discover(), LOCAL_PRINCIPAL)
    assert listed.phase == Phase.ACCEPTED
    assert [item.device for item in listed.candidates] == ["COM7"]
    assert device.opens == 0 and device.lists == 1
    refused = control.submit(Connect(device="", baudrate=115200), LOCAL_PRINCIPAL)
    assert refused.phase == Phase.REJECTED
    refused_baud = control.submit(Connect(device="COM7", baudrate=0), LOCAL_PRINCIPAL)
    assert refused_baud.reason == "baud_required"
    assert device.opens == 0
    session = _connect(control)
    snap = control.local_snapshot()
    assert snap.target_dtr is False and snap.target_rts is False
    assert snap.target_format == "8N1"
    assert snap.lines_applied is True
    assert snap.simulated is True
    assert snap.clock_trust == "unverified_local"
    assert snap.hw_loss == "unknown"
    again = control.submit(Connect(device="COM7", baudrate=9600), LOCAL_PRINCIPAL)
    assert again.phase == Phase.CONFLICT
    assert device.opens == 1
    assert session


def test_line_failure_is_reported_without_claiming_no_reset(rig):
    control, device, _clock, _audit = rig
    device.set_line_result(False)
    _connect(control)
    snap = control.local_snapshot()
    assert snap.lines_applied is False
    assert snap.target_dtr is False


def test_approval_is_exact_once_and_uart_is_transport_only(rig):
    control, device, clock, audit = rig
    session = _connect(control)
    pending = _request(control, clock, session, b"ABC")
    assert pending.phase == Phase.AWAITING
    assert device.write_calls == []
    mismatch = control.submit(Decide("d-bad", pending.operation_id, "0" * 64, session, True), LOCAL_PRINCIPAL)
    assert mismatch.reason == "approval_mismatch"
    assert _recent(control, "op-1") is None
    remote = control.submit(Decide("d-remote", pending.operation_id, pending.digest, session, True), REMOTE)
    assert remote.reason == "remote_cannot_approve"
    assert device.write_calls == []
    armed = _approve(control, pending)
    assert armed.reason == "handed_to_device"
    _wait(lambda: _recent(control, "op-1"), detail=lambda: control.local_snapshot())
    done = _recent(control, "op-1")
    assert done.phase == Phase.WRITTEN
    assert done.written_bytes == 3 and done.requested_bytes == 3
    assert done.success_scope == Scope.TRANSPORT
    assert done.device_outcome == DeviceOutcome.UNKNOWN
    assert done.reason == "uart_bytes_accepted"
    assert device.write_calls == [b"ABC"]
    assert exit_code(done.phase) == 0
    kinds = [record["record_type"] for record in audit.records]
    assert kinds.index("pre_write") < kinds.index("post_write")
    assert "payload" not in audit.records[kinds.index("pre_write")]
    replay = _request(control, clock, session, b"ABC", request_id="req-2")
    assert replay.phase == Phase.WRITTEN and replay.written_bytes == 3
    assert device.write_calls == [b"ABC"]
    conflict = _request(control, clock, session, b"XYZ", request_id="req-3")
    assert conflict.phase == Phase.CONFLICT
    assert device.write_calls == [b"ABC"]
    impact = control.local_snapshot()
    assert impact.hw_loss == "unknown"
    assert impact.rx_paused_during_write is True


def test_revoke_before_and_after_execution(rig):
    control, device, clock, _audit = rig
    session = _connect(control)
    device.pause()
    pending = _request(control, clock, session, b"Z", operation_id="op-rev")
    armed = _approve(control, pending)
    assert armed.phase == Phase.ACCEPTED
    control.submit(Revoke("rev-1"), LOCAL_PRINCIPAL)
    device.resume()
    _wait(lambda: _recent(control, "op-rev"))
    assert _recent(control, "op-rev").phase == Phase.DENIED
    assert _recent(control, "op-rev").reason == "revoked"
    assert device.write_calls == []

    started = threading.Event()
    release = threading.Event()

    def writer(data):
        started.set()
        assert release.wait(3)
        return WriteOutcome("all", len(data), None)

    device.set_writer(writer)
    live = _request(control, clock, session, b"LIVE", operation_id="op-live", request_id="req-live")
    _approve(control, live)
    assert started.wait(2)
    control.submit(Revoke("rev-2"), LOCAL_PRINCIPAL)
    release.set()
    _wait(lambda: _recent(control, "op-live"))
    done = _recent(control, "op-live")
    assert done.phase == Phase.WRITTEN
    assert done.reason == "revoked_after_executing"
    assert done.device_outcome == DeviceOutcome.UNKNOWN
    assert device.write_calls == [b"LIVE"]


def test_expiry_partial_unknown_and_audit_failure_do_not_retry(rig):
    control, device, clock, audit = rig
    session = _connect(control)
    device.pause()
    pending = _request(control, clock, session, b"OLD", operation_id="op-old", expires_in=5)
    _approve(control, pending)
    clock.advance(6)
    device.resume()
    _wait(lambda: _recent(control, "op-old"))
    assert _recent(control, "op-old").phase == Phase.EXPIRED
    assert device.write_calls == []

    device.set_writer(lambda data: WriteOutcome("partial", 1, "short"))
    partial = _request(control, clock, session, b"XYZ", operation_id="op-part", request_id="req-part")
    _approve(control, partial)
    _wait(lambda: _recent(control, "op-part"))
    assert _recent(control, "op-part").phase == Phase.PARTIAL
    assert _recent(control, "op-part").written_bytes == 1
    assert device.write_calls == [b"XYZ"]

    def explode(_data):
        raise TimeoutError("write timed out")

    device.set_writer(explode)
    unknown = _request(control, clock, session, b"U", operation_id="op-unk", request_id="req-unk")
    _approve(control, unknown)
    _wait(lambda: _recent(control, "op-unk"))
    assert _recent(control, "op-unk").phase == Phase.DELIVERY_UNKNOWN
    assert _recent(control, "op-unk").device_outcome == DeviceOutcome.UNKNOWN
    assert len(device.write_calls) == 2

    audit.fail = True
    blocked = _request(control, clock, session, b"N", operation_id="op-audit", request_id="req-audit")
    refused = _approve(control, blocked)
    assert refused.phase == Phase.DENIED and refused.reason == "audit_unconfirmed"
    assert len(device.write_calls) == 2
    assert control.local_snapshot().writes_blocked is True
    audit.fail = False
    device.set_writer(lambda data: WriteOutcome("all", len(data), None))
    again = _request(control, clock, session, b"N2", operation_id="op-audit-2", request_id="req-audit-2")
    _approve(control, again)
    _wait(lambda: _recent(control, "op-audit-2"))
    assert _recent(control, "op-audit-2").phase == Phase.WRITTEN
    assert device.write_calls[-1] == b"N2"
    assert len([call for call in device.write_calls if call == b"N"]) == 0


def test_reconnect_and_identity_change_retire_old_tickets(rig):
    control, device, clock, _audit = rig
    first = _connect(control)
    pending = _request(control, clock, first, b"Q", operation_id="op-old-sess")
    control.submit(Disconnect(), LOCAL_PRINCIPAL)
    _wait(lambda: not control.local_snapshot().target_connected)
    assert _recent(control, "op-old-sess").reason == "disconnect"
    second = _connect(control)
    assert first != second
    replay = _request(control, clock, second, b"Q", operation_id="op-old-sess", request_id="req-new")
    assert replay.phase == Phase.CONFLICT
    assert device.write_calls == []
    device.change_identity()
    _wait(lambda: not control.local_snapshot().target_connected, detail=lambda: control.local_snapshot())
    assert any("identity_changed" in item for item in control.local_snapshot().errors)


def test_idempotency_capacity_keeps_executed_record(rig):
    clock = FakeClock()
    audit = MemoryAudit()
    device = FakeDevice()
    control = Control(clock=clock, audit=audit, device=device, limits=Limits(idempotency_capacity=1), simulated=True)
    try:
        session = _connect(control)
        first = _request(control, clock, session, b"A", operation_id="kept")
        _approve(control, first)
        _wait(lambda: _recent(control, "kept"))
        blocked = _request(control, clock, session, b"B", operation_id="new", request_id="req-b")
        assert blocked.phase == Phase.BUSY and blocked.reason == "idempotency_full"
        replay = _request(control, clock, session, b"A", operation_id="kept", request_id="req-replay")
        assert replay.phase == Phase.WRITTEN
        assert device.write_calls == [b"A"]
    finally:
        control.shutdown_and_join(1)


def test_reserved_actions_have_no_fake_success(rig):
    control, device, clock, _audit = rig
    session = _connect(control)
    for action in ("adb.exec", "net.debug", "flash.write"):
        receipt = _request(control, clock, session, b"nope", operation_id="op-" + action, request_id="r-" + action[:8], action=action)
        assert receipt.phase == Phase.UNSUPPORTED and receipt.reason == "not_in_mvp"
    assert device.write_calls == []


def test_observe_hides_target_until_granted_and_relay_loss_cancels(rig):
    control, device, clock, _audit = rig
    session = _connect(control)
    hidden = control.submit(Observe("obs-1"), REMOTE)
    assert hidden.snapshot.target_connected is True
    assert hidden.snapshot.target_session is None
    assert hidden.snapshot.target_device is None
    control.submit(GrantView("g-1", "eng", ("observe", "logs")), LOCAL_PRINCIPAL)
    shown = control.submit(Observe("obs-2"), REMOTE)
    assert shown.snapshot.target_session == session
    assert shown.snapshot.target_device == "COM7"
    pending = _request(control, clock, session, b"H", operation_id="op-hide")
    assert pending.phase == Phase.AWAITING
    control.submit(RelayLost(), SYSTEM_PRINCIPAL)
    snap = control.local_snapshot()
    assert snap.relay_state == "lost"
    assert snap.grants == ()
    assert snap.target_connected is True
    assert _recent(control, "op-hide").reason == "relay_lost"
    assert device.write_calls == []
    denied = control.submit(RelayLost(), REMOTE)
    assert denied.reason == "system_only"


def test_log_overflow_is_per_consumer_and_not_zero_hardware_loss(rig):
    clock = FakeClock()
    audit = MemoryAudit()
    device = FakeDevice()
    limits = Limits(subscriber_items=1, subscriber_bytes=10000)
    control = Control(clock=clock, audit=audit, device=device, limits=limits, simulated=True)

    class Sink:
        def __init__(self):
            self.name = "file"
            self.gap = 0
            self.items = []

        def offer(self, item, nbytes=0):
            self.items.append(item)
            return True

    try:
        sink = Sink()
        control.add_log_sink(sink)
        sub = control.subscribe_logs("ui")
        for index in range(4):
            device.inject_rx(bytes([index]))
        _wait(lambda: len(sink.items) == 4, detail=lambda: sink.items)
        _items, gap = sub.poll()
        assert gap > 0
        assert sink.gap == 0
        control.shutdown_and_join(1)
        small = Control(
            clock=clock,
            audit=audit,
            device=FakeDevice(),
            limits=Limits(rx_queue_items=2, rx_queue_bytes=10000),
            simulated=True,
        )
        try:
            small.hold_pump()
            time.sleep(0.12)
            assert small.offer_rx(b"12") is True
            assert small.offer_rx(b"34") is True
            assert small.offer_rx(b"56") is False
            assert small._hw_loss == "unknown"
            small.offer_rx(b"9", hw_lost=4)
            small.release_pump()
            _wait(lambda: small.local_snapshot().overflow_chunks >= 1, detail=lambda: small.local_snapshot())
            assert small.local_snapshot().hw_loss == "4+unknown"
            assert small.local_snapshot().hw_loss != "0"
        finally:
            small.release_pump()
            small.shutdown_and_join(1)
    finally:
        control.release_pump()
        control.shutdown_and_join(1)


def test_slow_audit_does_not_block_serial_ingest(rig):
    control, device, clock, audit = rig
    session = _connect(control)
    pending = _request(control, clock, session, b"S", operation_id="op-slow", request_id="req-slow")
    audit.entered.clear()
    audit.block = threading.Event()
    box = []

    def decide():
        box.append(_approve(control, pending))

    thread = threading.Thread(target=decide)
    thread.start()
    assert audit.entered.wait(2)
    assert control.offer_rx(b"still-reading") is True
    assert control._rx_seq >= 1
    audit.block.set()
    thread.join(2)
    assert box and box[0].phase == Phase.ACCEPTED
    _wait(lambda: _recent(control, "op-slow"))


def test_blocking_write_does_not_stall_the_state_pump(rig):
    control, device, clock, _audit = rig
    session = _connect(control)
    started = threading.Event()
    release = threading.Event()

    def writer(data):
        started.set()
        assert release.wait(3)
        return WriteOutcome("all", len(data), None)

    device.set_writer(writer)
    pending = _request(control, clock, session, b"BLOCK", operation_id="op-block", request_id="req-block")
    _approve(control, pending)
    assert started.wait(2)
    began = time.time()
    observed = control.submit(Observe("obs-live"), LOCAL_PRINCIPAL, timeout=0.5)
    assert time.time() - began < 0.5
    assert observed.phase == Phase.ACCEPTED
    assert observed.snapshot.inflight
    release.set()
    _wait(lambda: _recent(control, "op-block"))


def test_control_queue_full_is_busy(rig):
    control, _device, _clock, _audit = rig
    control.hold_pump()
    time.sleep(0.12)
    receipts = []

    def one(index):
        receipts.append(control.submit(Observe(f"qq{index}"), LOCAL_PRINCIPAL, timeout=2))

    threads = [threading.Thread(target=one, args=(index,)) for index in range(80)]
    for thread in threads:
        thread.start()
    _wait(lambda: any(item.phase == Phase.BUSY for item in receipts), timeout=1.5)
    control.release_pump()
    for thread in threads:
        thread.join(3)
    assert any(item.reason == "control_queue_full" for item in receipts)
    assert any(item.phase == Phase.ACCEPTED for item in receipts)


def test_diagnostic_rejects_secret_fields(rig):
    control, _device, _clock, _audit = rig
    _connect(control)
    control.hold_pump()
    time.sleep(0.12)
    control._audit_memory.append({"password": "do-not-export", "record_type": "bad"})
    control.release_pump()
    receipt = control.submit(ExportDiagnostic(), LOCAL_PRINCIPAL)
    assert receipt.diagnostic is None
    assert receipt.phase == Phase.UNKNOWN
    with pytest.raises(RuntimeError):
        _reject_secrets({"notes": "-----BEGIN PRIVATE KEY-----\nabc"})
    clean = control.submit(Observe("obs-clean"), LOCAL_PRINCIPAL)
    encoded = json.dumps(clean.snapshot.errors)
    assert "do-not-export" not in encoded


def test_shutdown_during_write_is_not_success(rig):
    control, device, clock, _audit = rig
    session = _connect(control)
    started = threading.Event()
    release = threading.Event()

    def writer(data):
        started.set()
        release.wait(3)
        return WriteOutcome("all", len(data), None)

    device.set_writer(writer)
    pending = _request(control, clock, session, b"BYE", operation_id="op-bye", request_id="req-bye")
    _approve(control, pending)
    assert started.wait(2)
    receipt = control.shutdown_and_join(0.2)
    assert receipt.unfinished is True
    assert receipt.phase == Phase.DELIVERY_UNKNOWN
    assert receipt.reason != "shutdown"
    release.set()


def test_device_queue_full_does_not_execute_later(rig):
    control, device, clock, _audit = rig
    session = _connect(control)
    device.pause()
    for index in range(8):
        assert device.submit_job(DeviceJob(f"filler-{index}", "close")) is True
    pending = _request(control, clock, session, b"Q", operation_id="op-q", request_id="req-q")
    refused = _approve(control, pending)
    assert refused.phase == Phase.BUSY
    assert refused.reason == "device_queue_full"
    device.resume()
    time.sleep(0.2)
    assert device.write_calls == []
