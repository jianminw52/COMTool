"""Owner, approval, and single-open tests. No field hardware."""

from __future__ import annotations

import os
import pty
import tempfile
import threading
import time
import unittest

from COMTool.host.audit import AuditLog
from COMTool.host.serial_owner import OwnerError, PortIdentity, SerialOwner
from COMTool.host.session import HostSession, payload_sha256


class MemoryPort:
    def __init__(self, device="COM7"):
        self.is_open = True
        self.identity = PortIdentity(device, 0x1234, 0x5678, "SN1")
        self.written = bytearray()
        self._subs = {}
        self._next = 1

    def write(self, data: bytes) -> int:
        if not self.is_open:
            raise OwnerError("closed")
        self.written.extend(data)
        return len(data)

    def subscribe(self, callback):
        token = self._next
        self._next += 1
        self._subs[token] = callback
        return token

    def unsubscribe(self, token):
        self._subs.pop(token, None)

    def push(self, data: bytes) -> None:
        for callback in list(self._subs.values()):
            callback(data)


class SessionTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.port = MemoryPort()
        self.published = []
        self.session = HostSession(
            self.port,
            AuditLog(os.path.join(self.tmpdir.name, "ops.jsonl")),
            self.published.append,
            approval_seconds=0.4,
            echo_quiet_seconds=0.05,
        )

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_unsupported_does_not_write(self):
        raw = (
            '{"route":"control","op":"flash_commit","request_id":"f1","boot_id":"%s","seq":"1","at_ms":"0","confirm":"x"}'
            % self.session.boot_id
        )
        self.session.handle(raw.encode())
        self.assertEqual(self.port.written, b"")
        self.assertEqual(self.published[-1]["phase"], "unsupported")
        self.assertEqual(self.session.control_seq, 0)

    def test_unapproved_uart_writes_zero_bytes(self):
        self._uart(b"hi", request="u1")
        self.assertEqual(self.port.written, b"")
        self.session.expire_due()
        self.assertEqual(self.port.written, b"")
        time.sleep(0.5)
        self.session.expire_due()
        self.assertEqual(self.port.written, b"")
        self.assertEqual(self.published[-1]["phase"], "approval_timeout")
        self.assertIsNone(self.session.pending)
        self.assertEqual(self.session.approve("u1", payload_sha256(b"hi")), "mismatch")
        self.assertEqual(self.port.written, b"")

    def test_approve_exact_request_writes_once(self):
        self._uart(b"log\n", request="u2")
        pending = self.session.pending
        self.assertEqual(self.session.approve("other", pending["sha256"]), "mismatch")
        self.assertEqual(self.port.written, b"")
        self.assertEqual(self.session.approve(pending["request_id"], "0" * 64), "mismatch")
        self.assertEqual(self.port.written, b"")
        self.assertEqual(self.session.approve(pending["request_id"], pending["sha256"]), "wrote")
        self.assertEqual(bytes(self.port.written), b"log\n")
        self.assertEqual(self.session.approve(pending["request_id"], pending["sha256"]), "mismatch")
        self.assertEqual(self.session.writes, 1)

    def test_deny_retry_and_tamper_do_not_write(self):
        self._uart(b"Q", request="u3")
        pending = self.session.pending
        self.assertEqual(self.session.deny(pending["request_id"], pending["sha256"]), "denied")
        self.assertEqual(self.port.written, b"")
        self._uart(b"Q", request="u3", seq=1)
        self.assertEqual(self.published[-1]["phase"], "stale_seq")
        self._uart(b"NO", request="u4", seq=2)
        import base64

        other = base64.b64encode(b"NI").decode()
        self.session.handle(
            ('{"route":"uart","request_id":"u4","boot_id":"%s","seq":"2","at_ms":"0","data_b64":"%s"}' % (self.session.boot_id, other)).encode()
        )
        self.assertEqual(self.port.written, b"")

    def test_disconnect_drops_pending_before_late_approve(self):
        self._uart(b"Z", request="u5")
        pending = dict(self.session.pending)
        self.port.is_open = False
        self.assertEqual(self.session.approve(pending["request_id"], pending["sha256"]), "disconnected")
        self.assertEqual(self.port.written, b"")

    def test_view_and_live_do_not_write(self):
        self.session.handle(
            ('{"route":"control","op":"live_start","request_id":"l1","boot_id":"%s","seq":"1","at_ms":"0"}' % self.session.boot_id).encode()
        )
        self.assertEqual(self.published[-1]["phase"], "unauthorized")
        self.assertFalse(self.session.live_on)
        self.session.grant_view(True)
        self.session.handle(
            ('{"route":"control","op":"live_start","request_id":"l2","boot_id":"%s","seq":"1","at_ms":"0"}' % self.session.boot_id).encode()
        )
        self.assertTrue(self.session.live_on)
        self.session.on_bytes(b"AB")
        self.assertEqual(self.port.written, b"")
        self.assertEqual(self.published[-1]["route"], "live")
        self.session.grant_view(False)
        before = len(self.published)
        self.session.on_bytes(b"CD")
        self.assertEqual(len([item for item in self.published[before:] if item.get("route") == "live"]), 0)

    def _uart(self, raw: bytes, request: str, seq: int = 1):
        import base64

        body = (
            '{"route":"uart","request_id":"%s","boot_id":"%s","seq":"%d","at_ms":"0","data_b64":"%s"}'
            % (request, self.session.boot_id, seq, base64.b64encode(raw).decode())
        )
        self.session.handle(body.encode())


class OwnerTests(unittest.TestCase):
    def test_two_subscribers_one_open(self):
        master, slave = pty.openpty()
        name = os.ttyname(slave)
        owner = SerialOwner()
        seen = []
        owner.subscribe(seen.append)
        owner.subscribe(seen.append)
        owner.open(name, 115200, identity=PortIdentity(name, None, None, None))
        try:
            with self.assertRaises(OwnerError):
                owner.open(name, 115200, identity=PortIdentity(name, None, None, None))
            os.write(master, b"abc")
            deadline = time.time() + 2
            while len(seen) < 2 and time.time() < deadline:
                time.sleep(0.02)
            self.assertEqual(seen[0::2], seen[1::2])
            self.assertEqual(b"".join(seen[0::2]), b"abc")
            self.assertEqual(owner.bytes_read, 3)
            self.assertEqual(owner.write(b"out"), 3)
            os.set_blocking(master, False)
            echoed = b""
            deadline = time.time() + 2
            while b"out" not in echoed and time.time() < deadline:
                try:
                    echoed += os.read(master, 64)
                except BlockingIOError:
                    time.sleep(0.02)
            self.assertIn(b"out", echoed)
        finally:
            owner.close()
            os.close(master)
            os.close(slave)

    def test_missing_port_is_not_replaced(self):
        owner = SerialOwner()
        with self.assertRaises(OwnerError):
            owner.open("COM404-not-real", 115200)
        self.assertFalse(owner.is_open)


if __name__ == "__main__":
    unittest.main()
