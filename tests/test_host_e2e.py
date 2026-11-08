"""CLI to Host over a loopback broker and a PTY.

This is a Linux simulation. It is not a Windows machine and not a Rockchip board.
"""

from __future__ import annotations

import os
import pty
import re
import subprocess
import sys
import tempfile
import threading
import time
import unittest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from COMTool.host.runtime import Runtime
from COMTool.host.serial_owner import PortIdentity, SerialOwner
from tests.loopback_broker import LoopbackBroker


class EndToEnd(unittest.TestCase):
    def setUp(self):
        self.broker = LoopbackBroker()
        self.broker.start()
        time.sleep(0.05)
        self.master, self.slave = pty.openpty()
        self.slave_name = os.ttyname(self.slave)
        os.set_blocking(self.master, False)
        self.tmp = tempfile.TemporaryDirectory()
        self.owner = SerialOwner()
        self.runtime = Runtime(
            {
                "device_id": "host-sim",
                "broker_host": "127.0.0.1",
                "broker_port": self.broker.port,
                "broker_tls": False,
                "broker_username": "",
                "broker_password": "",
                "approval_seconds": 4,
                "echo_quiet_seconds": 0.2,
            },
            owner=self.owner,
            audit_path=os.path.join(self.tmp.name, "ops.jsonl"),
        )
        self.owner.open(self.slave_name, 115200, identity=PortIdentity(self.slave_name, None, None, None))
        self.runtime.start_mqtt()
        self._wait_mqtt()

    def tearDown(self):
        self.runtime.shutdown()
        self.broker.stop()
        os.close(self.master)
        os.close(self.slave)
        self.tmp.cleanup()

    def test_observe_approve_once_and_live(self):
        observed = self._cli(["--op", "observe", "--timeout", "3"])
        self.assertEqual(observed.returncode, 0, observed.stdout + observed.stderr)
        boot, mono, control, _uart = _fields(observed.stdout)
        self.assertEqual(control, 0)

        approver = threading.Thread(target=self._approve_once, daemon=True)
        approver.start()
        sent = self._cli(
            ["--text", "ping", "--boot-id", boot, "--seq", "1", "--at-ms", str(mono), "--timeout", "4"]
        )
        approver.join(5)
        self.assertEqual(sent.returncode, 0, sent.stdout + sent.stderr)
        self.assertIn("phase=sent", sent.stdout)
        self.assertIn(b"ping", self._read_master())
        self.assertEqual(self.runtime.session.writes, 1)
        audit = open(self.runtime.audit.path, encoding="utf-8").read()
        self.assertNotIn("broker_password", audit)
        self.assertIn('"result":"wrote"', audit)

        self.runtime.session.grant_view(True)
        observed = self._cli(["--op", "observe", "--timeout", "3"])
        boot, mono, control, _uart = _fields(observed.stdout)
        live = subprocess.Popen(
            [
                sys.executable,
                "bot/logbox_cli.py",
                "--device-id",
                "host-sim",
                "--op",
                "live",
                "--boot-id",
                boot,
                "--seq",
                str(control + 1),
                "--at-ms",
                str(mono),
                "--live-count",
                "1",
                "--timeout",
                "4",
            ],
            cwd=ROOT,
            env=self._env(),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        deadline = time.time() + 3
        while not self.runtime.session.live_on and time.time() < deadline:
            time.sleep(0.05)
        self.assertTrue(self.runtime.session.live_on)
        os.write(self.master, b"AB")
        out, err = live.communicate(timeout=6)
        self.assertEqual(live.returncode, 0, err.decode("utf-8", "replace"))
        self.assertIn(b"AB", out)
        self.assertFalse(self.runtime.session.live_on)

    def test_missing_approval_writes_nothing(self):
        self.runtime.session.approval_seconds = 0.3
        observed = self._cli(["--op", "observe", "--timeout", "3"])
        boot, mono, _control, uart = _fields(observed.stdout)
        sent = self._cli(
            ["--text", "nope", "--boot-id", boot, "--seq", str(uart + 1), "--at-ms", str(mono), "--timeout", "2"]
        )
        self.assertEqual(sent.returncode, 0, sent.stdout + sent.stderr)
        self.assertIn("phase=approval_timeout", sent.stdout)
        self.assertEqual(self._read_master(), b"")
        self.assertEqual(self.runtime.session.writes, 0)

    def _approve_once(self) -> None:
        deadline = time.time() + 4
        while time.time() < deadline:
            pending = self.runtime.session.pending
            if pending:
                self.runtime.session.approve(pending["request_id"], pending["sha256"])
                return
            time.sleep(0.02)

    def _cli(self, args: list[str]) -> subprocess.CompletedProcess:
        return subprocess.run(
            [sys.executable, "bot/logbox_cli.py", "--device-id", "host-sim", *args],
            cwd=ROOT,
            env=self._env(),
            text=True,
            capture_output=True,
            timeout=8,
        )

    def _env(self) -> dict:
        env = os.environ.copy()
        env.update(
            {
                "LOGBOX_MQTT_HOST": "127.0.0.1",
                "LOGBOX_MQTT_PORT": str(self.broker.port),
                "LOGBOX_MQTT_TLS": "0",
                "LOGBOX_MQTT_PASSWORD": "",
            }
        )
        return env

    def _read_master(self) -> bytes:
        data = b""
        deadline = time.time() + 1.5
        while time.time() < deadline:
            try:
                data += os.read(self.master, 128)
            except BlockingIOError:
                time.sleep(0.02)
        return data

    def _wait_mqtt(self) -> None:
        deadline = time.time() + 3
        while not self.runtime.mqtt.connected and time.time() < deadline:
            time.sleep(0.05)
        self.assertTrue(self.runtime.mqtt.connected, self.runtime.mqtt.last_error)


def _fields(text: str):
    boot = re.search(r"boot_id=(\S+)", text).group(1)
    mono = int(re.search(r"mono_ms=(\d+)", text).group(1))
    control = int(re.search(r"control_seq=(\d+)", text).group(1))
    uart = int(re.search(r"uart_seq=(\d+)", text).group(1))
    return boot, mono, control, uart


if __name__ == "__main__":
    unittest.main()
