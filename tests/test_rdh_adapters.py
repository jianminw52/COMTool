"""Adapter contracts: one-shot writes, audit failure, secrets, and a PTY."""

from __future__ import annotations

import json
import os
import time
from datetime import timedelta

import pytest

from rdh.adapters.clock import FakeClock
from rdh.adapters.credentials import MemorySecrets, SecretError, UnavailableSecrets, WindowsCredentialSecrets
from rdh.adapters.fake_device import FakeDevice
from rdh.adapters.journal import FileJournal
from rdh.adapters.logs import LogFileSink
from rdh.adapters.serial_device import SerialDevice, SerialHandle, _set_lines
from rdh.adapters.settings import Settings, SettingsError, load_settings, save_settings
from rdh.app.control import Control
from rdh.domain.model import LOCAL_PRINCIPAL, Connect, Decide, DeviceJob, LineSettings, Phase, PortIdentity, RequestOperation, WriteOutcome
from rdh.domain.policy import operation_digest
from tests.test_rdh_control import REMOTE, _approve, _connect, _request, _wait


class _Port:
    def __init__(self, written: int):
        self.written = written
        self.writes = []
        self.write_timeout = None

    def write(self, data):
        self.writes.append(bytes(data))
        return self.written

    def close(self):
        pass


def test_write_once_does_not_resend_the_remainder():
    port = _Port(2)
    handle = SerialHandle(port, True, PortIdentity("COM1", 1, 2, "S", "HW"), False)
    outcome = handle.write_once(b"abcdef")
    assert outcome.certainty == "partial" and outcome.written == 2
    assert port.writes == [b"abcdef"]
    assert handle.write_calls == 1


def test_dtr_ioctl_failure_is_not_a_crash_or_a_guarantee():
    class Lines:
        def __setattr__(self, name, value):
            if name in ("dtr", "rts"):
                error = OSError("Inappropriate ioctl for device")
                error.errno = 25
                raise error
            object.__setattr__(self, name, value)

    assert _set_lines(Lines(), False, False) is False


def test_scripted_serial_lists_without_opening_and_partial_write_is_once():
    opened = []

    class Handle:
        def __init__(self):
            self.calls = []
            self.lines_applied = False
            self.identity = PortIdentity("COM9", None, None, None, "")
            self.listed = False

        def write_once(self, data):
            self.calls.append(bytes(data))
            return WriteOutcome("partial", 1, "short")

        def read(self, _size):
            return b""

        def close(self):
            pass

    handle = Handle()

    def opener(device, settings):
        opened.append((device, settings.baudrate, settings.dtr, settings.rts))
        return handle

    device = SerialDevice(opener=opener, lister=lambda: ())
    captured = []
    device.on_job = captured.append
    assert device.list_ports() == ()
    assert opened == []
    device.start()
    try:
        assert device.submit_job(
            DeviceJob("open-1", "open", device="COM9", settings=LineSettings(baudrate=9600, dtr=False, rts=False), open_token="tok")
        )
        deadline = time.time() + 2
        while time.time() < deadline and not any(item.kind == "open" and item.ok for item in captured):
            time.sleep(0.01)
        assert any(item.kind == "open" and item.ok for item in captured)
        assert opened == [("COM9", 9600, False, False)]
        assert device.submit_job(DeviceJob("wr", "write", data=b"abcd"))
        deadline = time.time() + 2
        while time.time() < deadline and not handle.calls:
            time.sleep(0.01)
        assert handle.calls == [b"abcd"]
    finally:
        device.shutdown(1)


def test_journal_rejects_secrets_and_blocks_a_new_write(tmp_path):
    path = tmp_path / "audit.jsonl"
    journal = FileJournal(path)
    assert journal.append_and_confirm({"password": "hidden"}) is False
    assert path.read_text(encoding="utf-8") == ""
    clock = FakeClock()
    device = FakeDevice()
    control = Control(clock=clock, audit=journal, device=device, simulated=True)
    try:
        session = _connect(control)
        journal._fh.flush = _raise_disk
        pending = _request(control, clock, session, b"N", operation_id="op-disk", request_id="req-disk")
        refused = _approve(control, pending)
        assert refused.phase == Phase.DENIED and refused.reason == "audit_unconfirmed"
        assert device.write_calls == []
    finally:
        control.shutdown_and_join(1)
        journal.close()


def _raise_disk():
    raise OSError("disk")


def test_log_sink_gap_when_the_consumer_is_behind(tmp_path):
    sink = LogFileSink(tmp_path / "cap.log", max_items=2)
    sink.hold()
    time.sleep(0.05)

    class Record:
        def __init__(self, seq, data):
            self.seq = seq
            self.data = data

    assert sink.offer(Record(1, b"a"), 1) is True
    assert sink.offer(Record(2, b"b"), 1) is True
    assert sink.offer(Record(3, b"c"), 1) is False
    assert sink.gap >= 1
    sink.release()
    assert sink.shutdown(1) is True
    text = (tmp_path / "cap.log").read_text(encoding="utf-8")
    assert "gap" in text
    assert "3 c" not in text


def test_secrets_do_not_fall_back_to_plaintext_json(tmp_path):
    store = UnavailableSecrets()
    with pytest.raises(SecretError):
        store.put("mqtt", "pw")
    with pytest.raises(SecretError):
        store.get("mqtt")
    memory = MemorySecrets()
    memory.put("mqtt", "temp")
    assert memory.get("mqtt") == "temp"
    with pytest.raises(SecretError):
        WindowsCredentialSecrets()
    host = tmp_path / "host.json"
    host.write_text(json.dumps({"password": "s3cret", "port": "COM9"}), encoding="utf-8")
    before = host.read_bytes()
    with pytest.raises(SettingsError):
        load_settings(host)
    assert host.read_bytes() == before
    settings_path = tmp_path / "rdh-settings.json"
    settings_path.write_text(json.dumps({"host_id": "h", "mqtt_password": "nope"}), encoding="utf-8")
    raw = settings_path.read_bytes()
    with pytest.raises(SettingsError):
        load_settings(settings_path)
    assert settings_path.read_bytes() == raw
    save_settings(tmp_path / "clean.json", Settings(host_id="h", credential_name="mqtt"))
    saved = json.loads((tmp_path / "clean.json").read_text(encoding="utf-8"))
    assert "temp" not in json.dumps(saved)
    assert saved["credential_name"] == "mqtt"
    with pytest.raises(SettingsError):
        save_settings(tmp_path / "host.json", Settings())


@pytest.mark.skipif(not hasattr(os, "openpty"), reason="PTY is unavailable")
def test_pty_accepts_one_explicit_open_and_write():
    import pty

    master, slave = pty.openpty()
    import fcntl

    flags = fcntl.fcntl(master, fcntl.F_GETFL)
    fcntl.fcntl(master, fcntl.F_SETFL, flags | os.O_NONBLOCK)
    name = os.ttyname(slave)
    device = SerialDevice()
    results = []
    device.on_job = results.append
    device.start()
    try:
        assert device.list_ports() is not None
        assert device.submit_job(DeviceJob("open", "open", device=name, settings=LineSettings(115200, dtr=False, rts=False), open_token="pty"))
        deadline = time.time() + 2
        while time.time() < deadline and not any(item.kind == "open" for item in results):
            time.sleep(0.01)
        opened = [item for item in results if item.kind == "open"][-1]
        assert opened.ok, opened.error
        assert opened.lines_applied in (True, False)
        assert device.submit_job(DeviceJob("wr", "write", data=b"pty-byte"))
        deadline = time.time() + 2
        chunk = b""
        while time.time() < deadline and b"pty-byte" not in chunk:
            try:
                piece = os.read(master, 64)
            except BlockingIOError:
                piece = b""
            chunk += piece
            time.sleep(0.01)
        assert b"pty-byte" in chunk
    finally:
        device.shutdown(1)
        os.close(master)
        os.close(slave)


def test_operation_digest_still_binds_the_approved_bytes():
    clock = FakeClock()
    expires = (clock.wall() + timedelta(seconds=30)).strftime("%Y-%m-%dT%H:%M:%SZ")
    from rdh.domain.policy import canonical_instant

    canonical = canonical_instant(expires)
    assert operation_digest("uart.write", "t", canonical, b"abc") != operation_digest("uart.write", "t", canonical, b"abd")
