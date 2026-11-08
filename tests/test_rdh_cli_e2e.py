"""Local broker, new CLI, and host approval. No public broker and no field device."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
import time
from datetime import datetime, timedelta, timezone

import pytest

from rdh.adapters.clock import SystemClock
from rdh.adapters.credentials import MemorySecrets
from rdh.adapters.fake_device import FakeDevice
from rdh.adapters.serial_device import SerialDevice
from rdh.adapters.settings import Settings
from rdh.app.control import Control
from rdh.domain.model import LOCAL_PRINCIPAL, Connect, Decide, GrantView
from rdh.remote.broker import LoopbackBroker
from rdh.remote.codec import event_topic, request_topic, response_topic
from rdh.remote.mqtt import MqttClient, MqttError
from rdh.remote.service import start_remote


class Audit:
    def append_and_confirm(self, record):
        return True


def _accounts(broker, host_id="h1"):
    broker.add_account(
        f"host:{host_id}",
        "host-pw",
        publish=[f"rdh/v1/hosts/{host_id}/responses/+", f"rdh/v1/hosts/{host_id}/events/+"],
        subscribe=[f"rdh/v1/hosts/{host_id}/requests/+"],
    )
    broker.add_account(
        "eng",
        "eng-pw",
        publish=[request_topic(host_id, "eng")],
        subscribe=[response_topic(host_id, "eng"), event_topic(host_id, "eng")],
    )
    broker.add_account(
        "other",
        "other-pw",
        publish=[request_topic(host_id, "other")],
        subscribe=[response_topic(host_id, "other"), event_topic(host_id, "other")],
    )


def _expires():
    return (datetime.now(timezone.utc) + timedelta(seconds=60)).strftime("%Y-%m-%dT%H:%M:%SZ")


def _wait_snap(control, pred, timeout=2):
    end = time.time() + timeout
    while time.time() < end:
        snap = control.local_snapshot()
        if pred(snap):
            return snap
        time.sleep(0.01)
    raise AssertionError(control.local_snapshot())


_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


def _cli(port, *args, username="eng", password="eng-pw", timeout=8):
    env = os.environ.copy()
    env["PYTHONPATH"] = _ROOT + os.pathsep + env.get("PYTHONPATH", "")
    env["PYTHONIOENCODING"] = "utf-8"
    env["PYTHONUTF8"] = "1"
    env["RDH_PASSWORD"] = password
    return subprocess.run(
        [
            sys.executable,
            "-m",
            "rdh.cli",
            "--broker",
            "127.0.0.1",
            "--port",
            str(port),
            "--host-id",
            "h1",
            "--username",
            username,
            "--password-env",
            "RDH_PASSWORD",
            "--timeout",
            "3",
            *args,
        ],
        cwd=_ROOT,
        env=env,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=timeout,
        check=False,
    )


def _approve(control):
    end = time.time() + 4
    while time.time() < end:
        pending = control.local_snapshot().pending
        if pending:
            item = pending[0]
            control.submit(Decide("e2e-decide", item.operation_id, item.digest, item.target_session, True), LOCAL_PRINCIPAL)
            return
        time.sleep(0.02)


@pytest.mark.skipif(not hasattr(os, "openpty"), reason="PTY is unavailable")
def test_cli_host_approval_over_pty_and_loopback():
    import fcntl
    import pty

    master, slave = pty.openpty()
    flags = fcntl.fcntl(master, fcntl.F_GETFL)
    fcntl.fcntl(master, fcntl.F_SETFL, flags | os.O_NONBLOCK)
    name = os.ttyname(slave)
    broker = LoopbackBroker()
    _accounts(broker)
    broker.start()
    device = SerialDevice()
    control = Control(clock=SystemClock(), audit=Audit(), device=device, host_id="h1", simulated=True)
    secrets = MemorySecrets()
    secrets.put("host-broker", "host-pw")
    settings = Settings(host_id="h1", remote_mode="loopback_isolated", broker_host="127.0.0.1", broker_port=broker.port, credential_name="host-broker")
    adapter = None
    try:
        receipt = control.submit(Connect(device=name, baudrate=115200), LOCAL_PRINCIPAL)
        assert receipt.reason == "opening"
        _wait_snap(control, lambda snap: snap.target_connected or snap.errors)
        snap = control.local_snapshot()
        assert snap.target_connected, snap.errors
        adapter = start_remote(control, settings, secrets)
        assert adapter is not None
        _wait_snap(control, lambda item: item.relay_state == "loopback_isolated")
        hidden = _cli(broker.port, "observe")
        assert hidden.returncode == 0, hidden.stderr
        assert "不代表目标在线" in hidden.stderr
        hidden_body = json.loads(hidden.stdout)
        assert hidden_body["snapshot"]["target"]["connected"] is True
        assert hidden_body["snapshot"]["target"]["session"] is None
        control.submit(GrantView("e2e-grant", "eng", ("observe", "logs")), LOCAL_PRINCIPAL)
        shown = _cli(broker.port, "observe")
        session = json.loads(shown.stdout)["snapshot"]["target"]["session"]
        assert session
        approver = threading.Thread(target=_approve, args=(control,), daemon=True)
        approver.start()
        expires = _expires()
        done = _cli(
            broker.port,
            "operation",
            "uart.write",
            "--hex",
            "414243",
            "--operation-id",
            "op-pty",
            "--request-id",
            "req-pty",
            "--target-session",
            session,
            "--expires-at",
            expires,
        )
        approver.join(4)
        assert done.returncode == 0, (done.returncode, done.stdout, done.stderr)
        body = json.loads(done.stdout)
        assert body["phase"] == "written"
        assert body["success_scope"] == "transport"
        assert body["device_outcome"] == "unknown"
        assert "设备执行成功" not in done.stdout + done.stderr
        assert "未重试" in done.stderr
        chunk = b""
        end = time.time() + 2
        while time.time() < end and b"ABC" not in chunk:
            try:
                chunk += os.read(master, 64)
            except BlockingIOError:
                time.sleep(0.01)
        assert b"ABC" in chunk
        replay = _cli(
            broker.port,
            "operation",
            "uart.write",
            "--hex",
            "414243",
            "--operation-id",
            "op-pty",
            "--request-id",
            "req-replay",
            "--target-session",
            session,
            "--expires-at",
            expires,
        )
        assert replay.returncode == 0, replay.stderr
        try:
            extra = os.read(master, 64)
        except BlockingIOError:
            extra = b""
        assert extra == b""
        conflict = _cli(
            broker.port,
            "operation",
            "uart.write",
            "--hex",
            "4455",
            "--operation-id",
            "op-pty",
            "--request-id",
            "req-conflict",
            "--target-session",
            session,
            "--expires-at",
            expires,
        )
        assert conflict.returncode == 4, conflict.stdout
        queried = _cli(broker.port, "operation", "query", "--operation-id", "op-pty")
        assert queried.returncode == 0
        assert json.loads(queried.stdout)["phase"] == "written"
    finally:
        if adapter is not None:
            adapter.stop()
        broker.stop()
        control.shutdown_and_join(1)
        os.close(master)
        os.close(slave)


def test_acl_spoof_retain_and_disconnect_do_not_write():
    broker = LoopbackBroker()
    _accounts(broker)
    broker.start()
    device = FakeDevice()
    control = Control(clock=SystemClock(), audit=Audit(), device=device, host_id="h1", simulated=True)
    secrets = MemorySecrets()
    secrets.put("host-broker", "host-pw")
    settings = Settings(host_id="h1", remote_mode="loopback_isolated", broker_host="127.0.0.1", broker_port=broker.port, credential_name="host-broker")
    adapter = None
    try:
        _wait_connected = None
        control.submit(Connect(device="COM7", baudrate=115200), LOCAL_PRINCIPAL)
        _wait_snap(control, lambda snap: snap.target_connected)
        adapter = start_remote(control, settings, secrets)
        _wait_snap(control, lambda snap: snap.relay_state == "loopback_isolated")
        session = control.local_snapshot().target_session
        intruder = MqttClient()
        intruder.connect("127.0.0.1", broker.port, "root", "other", "other-pw")
        with pytest.raises(MqttError):
            intruder.publish(request_topic("h1", "eng"), b"{}", retain=False)
            time.sleep(0.2)
            if not intruder.alive:
                raise MqttError("acl closed")
        assert device.write_calls == []
        intruder.close()
        owner = MqttClient()
        owner.connect("127.0.0.1", broker.port, "not-a-principal", "eng", "eng-pw")
        owner.subscribe(response_topic("h1", "eng"))
        owner.publish(
            request_topic("h1", "eng"),
            json.dumps(
                {
                    "protocol": "rdh",
                    "version": 1,
                    "request_id": "spoof-1",
                    "operation_id": "op-spoof",
                    "action": "decide",
                    "principal": "local",
                    "target_session": session,
                }
            ).encode(),
            retain=False,
        )
        message = owner.take(1)
        assert message is not None
        body = json.loads(message["payload"])
        assert body["phase"] == "denied"
        assert device.write_calls == []
        owner.close()
        retained = MqttClient()
        retained.connect("127.0.0.1", broker.port, "eng-retain", "eng", "eng-pw")
        try:
            retained.publish(request_topic("h1", "eng"), b"{}", retain=True)
        except MqttError:
            pass
        time.sleep(0.2)
        assert retained.alive is False
        assert broker.dropped_retained >= 1
        assert device.write_calls == []
        retained.close()
    finally:
        if adapter is not None:
            adapter.stop()
        broker.stop()
        control.shutdown_and_join(1)


def test_disconnect_during_wait_does_not_replay():
    broker = LoopbackBroker()
    _accounts(broker)
    broker.start()
    device = FakeDevice()
    control = Control(clock=SystemClock(), audit=Audit(), device=device, host_id="h1", simulated=True)
    secrets = MemorySecrets()
    secrets.put("host-broker", "host-pw")
    settings = Settings(host_id="h1", remote_mode="loopback_isolated", broker_host="127.0.0.1", broker_port=broker.port, credential_name="host-broker")
    adapter = None
    try:
        control.submit(Connect(device="COM7", baudrate=115200), LOCAL_PRINCIPAL)
        _wait_snap(control, lambda snap: snap.target_connected)
        session = control.local_snapshot().target_session
        adapter = start_remote(control, settings, secrets)
        _wait_snap(control, lambda snap: snap.relay_state == "loopback_isolated")

        def drop_soon():
            end = time.time() + 2
            while time.time() < end and not control.local_snapshot().pending:
                time.sleep(0.02)
            broker.drop_all()

        threading.Thread(target=drop_soon, daemon=True).start()
        result = _cli(
            broker.port,
            "operation",
            "uart.write",
            "--hex",
            "01",
            "--operation-id",
            "op-drop",
            "--request-id",
            "req-drop",
            "--target-session",
            session,
            "--expires-at",
            _expires(),
        )
        assert result.returncode == 3, (result.returncode, result.stderr)
        assert "未重发" in result.stderr or "未自动重发" in result.stderr
        time.sleep(0.3)
        assert device.write_calls == []
        _wait_snap(control, lambda snap: snap.relay_state == "lost")
    finally:
        if adapter is not None:
            adapter.stop()
        broker.stop()
        control.shutdown_and_join(1)


def test_live_log_follow_and_password_is_not_an_argument():
    from rdh.cli.main import main

    assert main(["--password", "nope", "--broker", "127.0.0.1", "--port", "1", "--host-id", "h1", "--username", "eng", "--password-env", "RDH_PASSWORD", "observe"]) == 64
    broker = LoopbackBroker()
    _accounts(broker)
    broker.start()
    device = FakeDevice()
    control = Control(clock=SystemClock(), audit=Audit(), device=device, host_id="h1", simulated=True)
    secrets = MemorySecrets()
    secrets.put("host-broker", "host-pw")
    settings = Settings(host_id="h1", remote_mode="loopback_isolated", broker_host="127.0.0.1", broker_port=broker.port, credential_name="host-broker")
    adapter = None
    try:
        control.submit(Connect(device="COM7", baudrate=115200), LOCAL_PRINCIPAL)
        _wait_snap(control, lambda snap: snap.target_connected)
        adapter = start_remote(control, settings, secrets)
        _wait_snap(control, lambda snap: snap.relay_state == "loopback_isolated")
        control.submit(GrantView("log-grant", "eng", ("observe", "logs")), LOCAL_PRINCIPAL)

        def inject():
            for _ in range(10):
                device.inject_rx("示例日志行\n".encode())
                time.sleep(0.1)

        threading.Thread(target=inject, daemon=True).start()
        followed = _cli(broker.port, "logs", "follow", "--for-seconds", "1.2")
        assert followed.returncode == 0, followed.stderr
        assert "示例日志行" in followed.stdout
    finally:
        if adapter is not None:
            adapter.stop()
        broker.stop()
        control.shutdown_and_join(1)


def test_tls_missing_config_does_not_dial_and_loopback_rejects_public_host():
    calls = []

    def explode(*_args, **_kwargs):
        calls.append("dial")
        raise AssertionError("should not dial")

    import socket

    original = socket.create_connection
    socket.create_connection = explode
    control = Control(clock=SystemClock(), audit=Audit(), device=FakeDevice(), host_id="h1", simulated=True)
    try:
        settings = Settings(host_id="h1", remote_mode="tls", broker_host="203.0.113.10", broker_port=8883, credential_name="host-broker")
        assert start_remote(control, settings, MemorySecrets()) is None
        assert control.local_snapshot().relay_detail == "tls_or_identity_missing" or _wait_snap(control, lambda snap: snap.relay_state == "refused")
        public = Settings(host_id="h1", remote_mode="loopback_isolated", broker_host="203.0.113.10", broker_port=1883, credential_name="host-broker")
        assert start_remote(control, public, MemorySecrets()) is None
        snap = _wait_snap(control, lambda item: item.relay_detail in ("tls_or_identity_missing", "loopback_required"))
        assert snap.relay_state == "refused"
        assert calls == []
    finally:
        socket.create_connection = original
        control.shutdown_and_join(1)


def test_localhost_tls_roundtrip(tmp_path):
    key = tmp_path / "key.pem"
    cert = tmp_path / "cert.pem"
    subprocess.check_call(
        [
            "openssl",
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-keyout",
            str(key),
            "-out",
            str(cert),
            "-days",
            "1",
            "-nodes",
            "-subj",
            "/CN=localhost",
            "-addext",
            "subjectAltName=DNS:localhost",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    broker = LoopbackBroker()
    broker.enable_tls(str(cert), str(key))
    _accounts(broker)
    broker.start()
    device = FakeDevice()
    control = Control(clock=SystemClock(), audit=Audit(), device=device, host_id="h1", simulated=True)
    secrets = MemorySecrets()
    secrets.put("host-broker", "host-pw")
    settings = Settings(
        host_id="h1",
        remote_mode="tls",
        broker_host="127.0.0.1",
        broker_port=broker.port,
        credential_name="host-broker",
        tls_ca_file=str(cert),
        tls_cert_file=str(cert),
        tls_key_file=str(key),
    )
    adapter = None
    try:
        adapter = start_remote(control, settings, secrets)
        assert adapter is not None
        _wait_snap(control, lambda snap: snap.relay_state == "tls")
        import ssl

        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        context.load_verify_locations(str(cert))
        context.check_hostname = True
        client = MqttClient()
        client.connect("127.0.0.1", broker.port, "eng-tls", "eng", "eng-pw", ssl_context=context, server_hostname="localhost")
        client.subscribe(response_topic("h1", "eng"))
        client.publish(
            request_topic("h1", "eng"),
            json.dumps({"protocol": "rdh", "version": 1, "request_id": "tls-obs", "action": "observe"}).encode(),
        )
        message = client.take(2)
        client.close()
        assert message is not None
        assert json.loads(message["payload"])["reason"] == "observed"
    finally:
        if adapter is not None:
            adapter.stop()
        broker.stop()
        control.shutdown_and_join(1)
