#!/usr/bin/env python3
"""Local broker tests. They do not open a route beyond 127.0.0.1."""

# Portability: subprocess cwd is the repo root (parent of this file), not a hardcoded /workspace.

from __future__ import annotations

import base64
import io
import json
import os
import signal
import socket
import stat
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from contextlib import redirect_stdout

import logbox_cli


def _read_packet(conn: socket.socket) -> tuple[int, bytes]:
    header = conn.recv(1)
    if not header:
        raise ConnectionError("closed")
    remaining = 0
    shift = 0
    for _ in range(4):
        digit = conn.recv(1)[0]
        remaining += (digit & 0x7F) << shift
        if not digit & 0x80:
            break
        shift += 7
    body = b""
    while len(body) < remaining:
        body += conn.recv(remaining - len(body))
    return header[0], body


def _packet(header: int, body: bytes) -> bytes:
    return logbox_cli._packet(header, body)


class Broker(threading.Thread):
    def __init__(self, mode: str):
        super().__init__(daemon=True)
        self.mode = mode
        self.publishes: list[tuple[str, bytes]] = []
        self.ready = threading.Event()
        self._listener = socket.socket()
        self._listener.bind(("127.0.0.1", 0))
        self._listener.listen(1)
        self.port = self._listener.getsockname()[1]

    def _serve(self, conn: socket.socket) -> None:
        kind, _body = _read_packet(conn)
        if kind != 0x10:
            return
        conn.sendall(_packet(0x20, bytes([0, 0])))
        kind, _body = _read_packet(conn)
        if kind != 0x82:
            return
        conn.sendall(_packet(0x90, bytes([0, 1, 0])))
        kind, body = _read_packet(conn)
        if kind != 0x30:
            return
        topic_len = int.from_bytes(body[:2], "big")
        topic = body[2 : 2 + topic_len].decode()
        payload = body[2 + topic_len :]
        self.publishes.append((topic, payload))
        if self.mode == "silent":
            __import__("time").sleep(2)
            return
        if self.mode == "live":
            self._live(conn, payload)
            return
        if self.mode == "live-hold":
            self._hold(conn, payload)
            return
        if self.mode == "agent":
            self._agent(conn, payload)
            return
        data = json.loads(payload.decode())
        request_id = data["request_id"]
        state = "logbox/box1/state"
        if data.get("op") == "observe":
            reply = {
                "route": "observe",
                "ok": True,
                "request_id": request_id,
                "boot_id": "0a1b2c3d",
                "mono_ms": 5000,
                "control_seq": 0,
                "uart_seq": 0,
            }
            conn.sendall(_packet(0x30, logbox_cli._text(state) + json.dumps(reply).encode()))
            return
        wrong = json.dumps({"request_id": "other", "phase": "idle", "online": True}).encode()
        right = json.dumps(
            {
                "request_id": request_id,
                "device_id": "box1",
                "phase": "recording",
                "session_id": 3,
                "valid_bytes": 12,
                "overwritten_bytes": 0,
                "time_reliability": "monotonic_since_boot_ms",
                "remote_path": "",
                "online": True,
            }
        ).encode()
        for item in (wrong, right):
            conn.sendall(_packet(0x30, logbox_cli._text(state) + item))

    def _live(self, conn: socket.socket, first: bytes) -> None:
        state = "logbox/box1/state"
        data = json.loads(first.decode())
        if data.get("op") != "live_start":
            return
        ack = {
            "route": "control",
            "op": "live_start",
            "ok": True,
            "request_id": data["request_id"],
            "session": 1,
            "next_seq": 1,
            "already": False,
        }
        conn.sendall(_packet(0x30, logbox_cli._text(state) + json.dumps(ack).encode()))
        frames = [
            {"route": "live", "boot_id": "0a1b2c3d", "session": 1, "seq": 1, "frag_b64": base64.b64encode(b"AB").decode(), "truncated": False, "dropped": 0, "paused": False},
            {"route": "live", "boot_id": "0a1b2c3d", "session": 1, "seq": 1, "frag_b64": base64.b64encode(b"ZZ").decode(), "truncated": False, "dropped": 0, "paused": False},
            {"route": "live", "boot_id": "0a1b2c3d", "session": 1, "seq": 3, "frag_b64": base64.b64encode(b"\xe4\xb8\xad").decode(), "truncated": False, "dropped": 0, "paused": False},
            {"route": "uart", "request_id": "other", "frag_b64": base64.b64encode(b"NO").decode()},
            {"route": "live", "boot_id": "0a1b2c3d", "session": 1, "seq": 4, "frag_b64": base64.b64encode(b'{"op":"start"}').decode(), "truncated": False, "dropped": 0, "paused": False},
        ]
        for frame in frames:
            conn.sendall(_packet(0x30, logbox_cli._text(state) + json.dumps(frame).encode()))
        kind, body = _read_packet(conn)
        if kind != 0x30:
            return
        topic_len = int.from_bytes(body[:2], "big")
        stop = json.loads(body[2 + topic_len :].decode())
        self.publishes.append(("stop", json.dumps(stop).encode()))
        reply = {"route": "control", "op": "live_stop", "ok": True, "request_id": stop.get("request_id"), "session": 1, "next_seq": 1, "already": False}
        conn.sendall(_packet(0x30, logbox_cli._text(state) + json.dumps(reply).encode()))

    def _hold(self, conn: socket.socket, first: bytes) -> None:
        state = "logbox/box1/state"
        data = json.loads(first.decode())
        ack = {"route": "control", "op": "live_start", "ok": True, "request_id": data["request_id"], "session": 1, "next_seq": 1, "already": False}
        conn.sendall(_packet(0x30, logbox_cli._text(state) + json.dumps(ack).encode()))
        frame = {"route": "live", "boot_id": "0a1b2c3d", "session": 1, "seq": 1, "frag_b64": base64.b64encode(b"AB").decode(), "truncated": False, "dropped": 0, "paused": False}
        conn.sendall(_packet(0x30, logbox_cli._text(state) + json.dumps(frame).encode()))
        conn.settimeout(5)
        try:
            kind, body = _read_packet(conn)
        except (socket.timeout, TimeoutError, OSError):
            return
        if kind != 0x30:
            return
        topic_len = int.from_bytes(body[:2], "big")
        stop = json.loads(body[2 + topic_len :].decode())
        self.publishes.append(("stop", json.dumps(stop).encode()))

    def _agent(self, conn: socket.socket, first: bytes) -> None:
        state = "logbox/box1/state"
        data = json.loads(first.decode())
        ack = {"route": "control", "op": "live_start", "ok": True, "request_id": data["request_id"], "session": 1, "next_seq": 1, "already": False}
        conn.sendall(_packet(0x30, logbox_cli._text(state) + json.dumps(ack).encode()))

        def live(seq: int, raw: bytes, session: int = 1, truncated: bool = False, dropped: int | None = 0, paused: bool = False, boot: str = "aabbccdd") -> None:
            frame = {
                "route": "live",
                "boot_id": boot,
                "session": session,
                "seq": seq,
                "frag_b64": base64.b64encode(raw).decode("ascii"),
                "truncated": truncated,
                "dropped": dropped,
                "paused": paused,
            }
            conn.sendall(_packet(0x30, logbox_cli._text(state) + json.dumps(frame).encode()))

        live(1, b"AB")
        live(1, b"ZZ")
        time.sleep(5.1)
        live(2, b"\xe4\xb8")
        live(3, b"\xad\xff\x00", truncated=True, dropped=7)
        live(4, b"", paused=True, dropped=None)
        live(6, b'{"op":"start"}')
        conn.sendall(_packet(0x30, logbox_cli._text(state) + json.dumps({"route": "uart", "request_id": "nope", "frag_b64": base64.b64encode(b"NO").decode()}).encode()))
        live(1, b"N", session=2)
        live(1, b"R", boot="00112233")
        kind, body = _read_packet(conn)
        if kind != 0x30:
            return
        topic_len = int.from_bytes(body[:2], "big")
        stop = json.loads(body[2 + topic_len :].decode())
        self.publishes.append(("stop", json.dumps(stop).encode()))
        reply = {"route": "control", "op": "live_stop", "ok": True, "request_id": stop.get("request_id"), "session": 1, "next_seq": 1, "already": False}
        conn.sendall(_packet(0x30, logbox_cli._text(state) + json.dumps(reply).encode()))

    def run(self) -> None:
        self.ready.set()
        turns = 2 if self.mode == "observe" else 1
        try:
            for _ in range(turns):
                conn, _addr = self._listener.accept()
                try:
                    self._serve(conn)
                finally:
                    conn.close()
        finally:
            self._listener.close()


class CliTest(unittest.TestCase):
    def test_default_sends_nothing(self) -> None:
        buf = io.StringIO()
        with redirect_stdout(buf):
            code = logbox_cli.main([])
        self.assertEqual(code, 0)
        self.assertIn("未发送", buf.getvalue())

    def test_dry_run_does_not_connect(self) -> None:
        calls = []

        def connect(host: str, port: int, timeout: float) -> socket.socket:
            calls.append(host)
            raise AssertionError("dry-run connected")

        buf = io.StringIO()
        with redirect_stdout(buf):
            code = logbox_cli.execute(
                "box1",
                "status",
                {},
                connect=connect,
                dry_run=True,
                request_id="req1",
                boot_id="0a1b2c3d",
                seq=1,
                at_ms=1000,
            )
        self.assertEqual(code, 0)
        self.assertEqual(calls, [])
        self.assertIn("未发送", buf.getvalue())
        self.assertIn('"op":"status"', buf.getvalue())
        self.assertIn("req1", buf.getvalue())

    def test_list_and_file_id_stay_local_on_dry_run(self) -> None:
        buf = io.StringIO()
        with redirect_stdout(buf):
            code = logbox_cli.main(
                ["--dry-run", "--device-id", "box1", "--op", "upload", "--file-id", "f00000001", "--request-id", "req9",
                 "--boot-id", "0a1b2c3d", "--seq", "4", "--at-ms", "1000"]
            )
        self.assertEqual(code, 0)
        text = buf.getvalue()
        self.assertIn("未发送", text)
        self.assertIn('"op":"upload"', text)
        self.assertIn('"file_id":"f00000001"', text)
        topic, state, payload = logbox_cli.build_command("box1", "list", "req10", seq=2, at_ms=1000, boot_id="0a1b2c3d")
        self.assertEqual(topic, "logbox/box1/cmd")
        self.assertEqual(state, "logbox/box1/state")
        self.assertEqual(
            json.loads(payload),
            {"route": "control", "request_id": "req10", "op": "list", "boot_id": "0a1b2c3d", "seq": "2", "at_ms": "1000"},
        )

    def test_control_and_uart_stay_apart(self) -> None:
        with open(logbox_cli.__file__, encoding="utf-8") as source_file:
            source = source_file.read()
        self.assertNotIn("shell=True", source)
        self.assertNotIn("os.system", source)
        self.assertNotIn("subprocess", source)
        buf = io.StringIO()
        with redirect_stdout(buf):
            code = logbox_cli.main(
                ["--dry-run", "--device-id", "box1", "--op", "start", "--request-id", "c1",
                 "--boot-id", "0a1b2c3d", "--seq", "1", "--at-ms", "1000"]
            )
        self.assertEqual(code, 0)
        body = json.loads(next(line for line in buf.getvalue().splitlines() if line.startswith("{")))
        self.assertEqual(body["route"], "control")
        self.assertEqual(body["op"], "start")
        self.assertNotIn("data_b64", body)
        buf = io.StringIO()
        with redirect_stdout(buf):
            code = logbox_cli.main(
                [
                    "--dry-run",
                    "--device-id",
                    "box1",
                    "--text",
                    "echo a; b | c 'd' \"e\"",
                    "--text-lf",
                    "--boot-id",
                    "0a1b2c3d",
                    "--seq",
                    "7",
                    "--at-ms",
                    "1000",
                    "--request-id",
                    "u1",
                ]
            )
        self.assertEqual(code, 0)
        text = buf.getvalue()
        body = json.loads(next(line for line in text.splitlines() if line.startswith("{")))
        self.assertEqual(body["route"], "uart")
        self.assertNotIn("op", body)
        raw = base64.b64decode(body["data_b64"])
        self.assertEqual(raw, b"echo a; b | c 'd' \"e\"\n")
        self.assertIn("topic=logbox/box1/cmd", text)
        buf = io.StringIO()
        with redirect_stdout(buf):
            code = logbox_cli.main(["--dry-run", "--device-id", "box1", "--route", "uart", "--boot-id", "0a1b2c3d"])
        self.assertEqual(code, 2)
        self.assertIn("未发送", buf.getvalue())
        buf = io.StringIO()
        with redirect_stdout(buf):
            code = logbox_cli.main(["--dry-run", "--device-id", "box1", "--op", "start", "--text", "hi"])
        self.assertEqual(code, 2)

    def test_missing_host_sends_nothing(self) -> None:
        buf = io.StringIO()
        with redirect_stdout(buf):
            code = logbox_cli.execute("box1", "start", {}, request_id="req2", boot_id="0a1b2c3d", seq=1, at_ms=1000)
        self.assertEqual(code, 2)
        self.assertIn("未发送", buf.getvalue())

    def test_timeout_is_unknown_and_publishes_once(self) -> None:
        broker = Broker("silent")
        broker.start()
        broker.ready.wait(1)

        def connect(host: str, port: int, timeout: float) -> socket.socket:
            return socket.create_connection(("127.0.0.1", broker.port), timeout=timeout)

        buf = io.StringIO()
        with redirect_stdout(buf):
            code = logbox_cli.execute(
                "box1",
                "status",
                {"LOGBOX_MQTT_HOST": "127.0.0.1", "LOGBOX_MQTT_TLS": "0"},
                connect=connect,
                timeout=0.4,
                request_id="reqtimeout",
                boot_id="0a1b2c3d",
                seq=3,
                at_ms=1000,
            )
        broker.join(2)
        self.assertEqual(code, 3)
        self.assertIn("未知", buf.getvalue())
        self.assertNotIn("phase=", buf.getvalue())
        self.assertEqual(len(broker.publishes), 1)
        topic, payload = broker.publishes[0]
        self.assertEqual(topic, "logbox/box1/cmd")
        self.assertEqual(json.loads(payload.decode())["request_id"], "reqtimeout")

    def test_correlates_request_id(self) -> None:
        broker = Broker("reply")
        broker.start()
        broker.ready.wait(1)

        def connect(host: str, port: int, timeout: float) -> socket.socket:
            return socket.create_connection(("127.0.0.1", broker.port), timeout=timeout)

        buf = io.StringIO()
        with redirect_stdout(buf):
            code = logbox_cli.execute(
                "box1",
                "start",
                {"LOGBOX_MQTT_HOST": "127.0.0.1", "LOGBOX_MQTT_TLS": "0"},
                connect=connect,
                timeout=2,
                request_id="reqok",
                boot_id="0a1b2c3d",
                seq=4,
                at_ms=1000,
            )
        broker.join(2)
        self.assertEqual(code, 0)
        text = buf.getvalue()
        self.assertIn("phase=recording", text)
        self.assertIn("session_id=3", text)
        self.assertIn("valid_bytes=12", text)
        self.assertIn("request_id=reqok", text)
        self.assertEqual(len(broker.publishes), 1)

    def test_recover_keeps_the_next_seq(self) -> None:
        state = logbox_cli.recover_state(
            {"boot_id": "0a1b2c3d", "mono_ms": 5000, "control_seq": 20, "uart_seq": 10000}
        )
        self.assertEqual(state["control_seq"], 21)
        self.assertEqual(state["uart_seq"], 10001)
        topic, _, payload = logbox_cli.build_command(
            "box1", "stop", "newstop", seq=state["control_seq"], at_ms=state["at_ms"], boot_id=state["boot_id"]
        )
        body = json.loads(payload)
        self.assertEqual(topic, "logbox/box1/cmd")
        self.assertEqual(body["seq"], "21")
        self.assertEqual(body["at_ms"], "5000")
        retry = logbox_cli.build_command(
            "box1", "stop", "newstop", seq=state["control_seq"], at_ms=state["at_ms"], boot_id=state["boot_id"]
        )[2]
        self.assertEqual(retry, payload)
        buf = io.StringIO()
        with redirect_stdout(buf):
            code = logbox_cli.main(["--dry-run", "--device-id", "box1", "--op", "stop", "--request-id", "s"])
        self.assertEqual(code, 2)
        self.assertIn("未发送", buf.getvalue())


    def test_observe_then_command_uses_one_seq(self) -> None:
        broker = Broker("observe")
        broker.start()
        broker.ready.wait(1)

        def connect(host: str, port: int, timeout: float) -> socket.socket:
            return socket.create_connection(("127.0.0.1", broker.port), timeout=timeout)

        state: dict = {}
        buf = io.StringIO()
        with redirect_stdout(buf):
            code = logbox_cli.execute(
                "box1",
                "start",
                {"LOGBOX_MQTT_HOST": "127.0.0.1", "LOGBOX_MQTT_TLS": "0"},
                connect=connect,
                timeout=2,
                request_id="ob",
                state=state,
            )
        broker.join(2)
        self.assertEqual(code, 0, buf.getvalue())
        self.assertEqual(state["control_seq"], 1)
        cmds = [json.loads(payload.decode()) for _topic, payload in broker.publishes]
        self.assertEqual(cmds[0]["op"], "observe")
        self.assertNotIn("seq", cmds[0])
        self.assertEqual(cmds[1]["seq"], "1")
        self.assertEqual(cmds[1]["boot_id"], "0a1b2c3d")

    def test_fragment_reassembly_reports_gaps(self) -> None:
        data, missing, dups = logbox_cli.ingest_fragments(
            [
                {"frag": 2, "frag_b64": base64.b64encode(b"BB").decode()},
                {"frag": 2, "frag_b64": base64.b64encode(b"BB").decode()},
                {"frag": 1, "frag_b64": base64.b64encode(b"AA").decode()},
            ]
        )
        self.assertEqual(data, b"AABB")
        self.assertEqual(missing, [])
        self.assertEqual(dups, [2])
        _data, missing, _dups = logbox_cli.ingest_fragments(
            [{"frag": 1, "frag_b64": base64.b64encode(b"A").decode()}, {"frag": 3, "frag_b64": base64.b64encode(b"C").decode()}]
        )
        self.assertEqual(missing, [2])

    def test_pending_retry_does_not_take_a_new_seq(self) -> None:
        state = {
            "boot_id": "0a1b2c3d",
            "mono_ms": 1000,
            "control_seq": 3,
            "uart_seq": 0,
            "pending": {
                "op": "start",
                "request_id": "same",
                "file_id": None,
                "route": "control",
                "raw": None,
                "boot_id": "0a1b2c3d",
                "seq": 4,
                "at_ms": 1000,
            },
        }
        buf = io.StringIO()
        with redirect_stdout(buf):
            code = logbox_cli.execute("box1", "stop", {}, dry_run=True, state=state)
        self.assertEqual(code, 2)
        self.assertIn("冲突", buf.getvalue())
        buf = io.StringIO()
        with redirect_stdout(buf):
            code = logbox_cli.execute("box1", "start", {}, dry_run=True, state=state, retry=True)
        self.assertEqual(code, 0)
        body = json.loads(next(line for line in buf.getvalue().splitlines() if line.startswith("{")))
        self.assertEqual(body["seq"], "4")
        self.assertEqual(body["at_ms"], "1000")
        self.assertEqual(body["request_id"], "same")


    def test_flash_prepare_does_not_confirm_itself(self) -> None:
        buf = io.StringIO()
        with redirect_stdout(buf):
            code = logbox_cli.main(
                ["--dry-run", "--device-id", "box1", "--op", "flash_commit", "--boot-id", "0a1b2c3d", "--seq", "2", "--at-ms", "1000"]
            )
        self.assertEqual(code, 2)
        self.assertIn("未发送", buf.getvalue())
        self.assertNotIn("confirm", buf.getvalue())
        buf = io.StringIO()
        with redirect_stdout(buf):
            code = logbox_cli.main(
                [
                    "--dry-run",
                    "--device-id",
                    "box1",
                    "--op",
                    "flash_prepare",
                    "--boot-id",
                    "0a1b2c3d",
                    "--seq",
                    "1",
                    "--at-ms",
                    "1000",
                    "--manifest",
                    "target esp32s3;secure none;image 0x10000 4100 abcd https://fixture.test/a.bin",
                ]
            )
        self.assertEqual(code, 0)
        text = buf.getvalue()
        self.assertIn("flash_prepare", text)
        self.assertIn("manifest", text)
        self.assertNotIn('"confirm"', text)
        buf = io.StringIO()
        with redirect_stdout(buf):
            code = logbox_cli.main(
                [
                    "--dry-run",
                    "--device-id",
                    "box1",
                    "--op",
                    "flash_commit",
                    "--boot-id",
                    "0a1b2c3d",
                    "--seq",
                    "2",
                    "--at-ms",
                    "1000",
                    "--confirm",
                    "001122",
                ]
            )
        self.assertEqual(code, 0)
        self.assertEqual(buf.getvalue().count("flash_commit"), 1)

    def test_observe_does_not_resend_pending_uart(self) -> None:
        state = {
            "boot_id": "0a1b2c3d",
            "mono_ms": 1000,
            "control_seq": 1,
            "uart_seq": 4,
            "pending": {
                "op": None,
                "request_id": "same",
                "file_id": None,
                "route": "uart",
                "raw": b"Q",
                "boot_id": "0a1b2c3d",
                "seq": 5,
                "at_ms": 1000,
            },
        }
        buf = io.StringIO()
        with redirect_stdout(buf):
            code = logbox_cli.execute("box1", "observe", {}, dry_run=True, request_id="ob", state=state)
        self.assertEqual(code, 0)
        self.assertIn("observe", buf.getvalue())
        self.assertNotIn("data_b64", buf.getvalue())


    def test_live_follow_prints_bytes_and_does_not_run_them(self) -> None:
        broker = Broker("live")
        broker.start()
        self.assertTrue(broker.ready.wait(1))
        out = io.BytesIO()
        err = io.StringIO()
        code = logbox_cli.follow_live(
            "box1",
            {
                "LOGBOX_MQTT_HOST": "127.0.0.1",
                "LOGBOX_MQTT_PORT": str(broker.port),
                "LOGBOX_MQTT_TLS": "0",
                "LOGBOX_MQTT_PASSWORD": "SECRETTOKEN",
            },
            connect=lambda host, port, timeout: socket.create_connection((host, port), timeout),
            timeout=2,
            boot_id="0a1b2c3d",
            seq=1,
            at_ms=1000,
            count=3,
            data_out=out,
            err=err,
        )
        broker.join(2)
        self.assertEqual(code, 0)
        self.assertEqual(out.getvalue(), b"AB\xe4\xb8\xad{\"op\":\"start\"}")
        self.assertNotIn(b"ZZ", out.getvalue())
        self.assertNotIn(b"NO", out.getvalue())
        notes = err.getvalue()
        self.assertIn("旧帧", notes)
        self.assertIn("缺片", notes)
        self.assertNotIn("SECRETTOKEN", notes)
        self.assertNotIn("SECRETTOKEN", out.getvalue().decode("utf-8", "replace"))
        sent = b"".join(body for _topic, body in broker.publishes)
        self.assertIn(b"live_start", sent)
        self.assertIn(b"live_stop", sent)
        self.assertNotIn(b'"op":"start"', sent)


    def test_agent_jsonl_from_cli_process(self) -> None:
        from datetime import datetime

        broker = Broker("agent")
        broker.start()
        self.assertTrue(broker.ready.wait(1))
        tmp = tempfile.mkdtemp(prefix="logbox-live-")
        path = os.path.join(tmp, "box.jsonl")
        other = os.path.join(tmp, "keep-me.txt")
        with open(other, "w", encoding="utf-8") as fh:
            fh.write("leave this\n")
        env = os.environ.copy()
        env.update(
            {
                "LOGBOX_MQTT_HOST": "127.0.0.1",
                "LOGBOX_MQTT_PORT": str(broker.port),
                "LOGBOX_MQTT_TLS": "0",
                "LOGBOX_MQTT_PASSWORD": "SECRETTOKEN",
            }
        )
        proc = subprocess.run(
            [
                sys.executable,
                "bot/logbox_cli.py",
                "--device-id",
                "box1",
                "--op",
                "live",
                "--boot-id",
                "aabbccdd",
                "--seq",
                "1",
                "--at-ms",
                "1000",
                "--live-count",
                "6",
                "--timeout",
                "12",
                "--live-log",
                path,
            ],
            cwd=os.path.abspath(os.path.join(os.path.dirname(__file__), "..")),
            env=env,
            capture_output=True,
            timeout=20,
        )
        broker.join(2)
        self.assertEqual(proc.returncode, 0, proc.stderr.decode("utf-8", "replace"))
        expected = b"AB\xe4\xb8\xad\xff\x00{\"op\":\"start\"}NR"
        self.assertEqual(proc.stdout, expected)
        self.assertNotIn(b"ZZ", proc.stdout)
        self.assertNotIn(b"NO", proc.stdout)
        self.assertNotIn(b"SECRETTOKEN", proc.stdout + proc.stderr)
        self.assertIn("旧帧", proc.stderr.decode())
        self.assertIn("缺片", proc.stderr.decode())
        self.assertIn("暂停", proc.stderr.decode())
        self.assertIn("重启", proc.stderr.decode())
        self.assertIn("会话", proc.stderr.decode())
        self.assertTrue(os.path.isfile(path))
        self.assertFalse(os.path.exists(path + ".1"))
        self.assertEqual(open(other, encoding="utf-8").read(), "leave this\n")
        self.assertEqual(stat.S_IMODE(os.stat(path).st_mode), 0o600)
        rows = []
        text = open(path, encoding="utf-8").read()
        self.assertNotIn("SECRETTOKEN", text)
        for line in text.splitlines():
            rows.append(json.loads(line))
        rows.sort(key=lambda row: row["rec"])
        self.assertEqual([row["rec"] for row in rows], list(range(1, len(rows) + 1)))
        raw = b"".join(base64.b64decode(row["raw_b64"]) for row in rows if row["kind"] == "data")
        self.assertEqual(raw, expected)
        data_rows = [row for row in rows if row["kind"] == "data"]
        self.assertTrue(all(row["encoding"] == "base64" and row["nbytes"] == len(base64.b64decode(row["raw_b64"])) for row in data_rows))
        self.assertTrue(all(row["device_time"] is None and row["recv_utc"].endswith("Z") for row in rows))
        self.assertEqual(data_rows[0]["text"], "AB")
        self.assertIsNone(next(row["text"] for row in data_rows if row["nbytes"] == 3))
        gap = next(row for row in rows if row["kind"] == "diag" and row["event"] == "gap")
        self.assertIsNone(gap["dropped"])
        overflow = next(row for row in rows if row["kind"] == "diag" and row["event"] == "overflow")
        self.assertEqual(overflow["dropped"], 7)
        pause = next(row for row in rows if row["kind"] == "diag" and row["event"] == "pause")
        self.assertIsNone(pause["dropped"])
        first_mono = data_rows[0]["recv_mono_ms"]
        later = next(row for row in data_rows if row["frame_seq"] == 2)
        self.assertGreaterEqual(later["recv_mono_ms"] - first_mono, 5000)
        sent = b"".join(body for _topic, body in broker.publishes)
        self.assertIn(b"live_stop", sent)
        self.assertNotIn(b'"op":"start"', sent)
        datetime.fromisoformat(data_rows[0]["recv_utc"].replace("Z", "+00:00"))
        print("PASS live_jsonl_bytes_gap_pause failed_asserts=0")

    def test_live_log_refuses_existing_and_dry_run(self) -> None:
        path = os.path.join(tempfile.mkdtemp(prefix="logbox-live-"), "box.jsonl")
        with open(path, "w", encoding="utf-8") as fh:
            fh.write("mine\n")

        def fail_connect(*_args):
            raise AssertionError("should not connect")

        err = io.StringIO()
        code = logbox_cli.follow_live(
            "box1",
            {"LOGBOX_MQTT_HOST": "127.0.0.1", "LOGBOX_MQTT_TLS": "0"},
            connect=fail_connect,
            boot_id="aabbccdd",
            seq=1,
            at_ms=1000,
            log_path=path,
            err=err,
        )
        self.assertEqual(code, 4)
        self.assertIn("日志失败", err.getvalue())
        self.assertEqual(open(path, encoding="utf-8").read(), "mine\n")
        fresh = path + ".new"
        code = logbox_cli.main(
            ["--dry-run", "--device-id", "box1", "--op", "live", "--boot-id", "BOOT", "--seq", "1", "--at-ms", "1", "--live-log", fresh]
        )
        self.assertEqual(code, 0)
        self.assertFalse(os.path.exists(fresh))
        print("PASS live_jsonl_refuses_existing failed_asserts=0")
        from live_log import LiveLog

        rot = os.path.join(tempfile.mkdtemp(prefix="logbox-live-"), "rot.jsonl")
        untouched = rot + ".notes"
        with open(untouched, "w", encoding="utf-8") as fh:
            fh.write("other\n")
        book = LiveLog(rot, max_bytes=300, keep=2)
        for i in range(8):
            book.write_record("diag", {"device_id": "box1", "boot_id": "aa", "session": 1, "frame_seq": i, "event": "gap", "dropped": None})
        book.close()
        self.assertTrue(os.path.isfile(rot))
        self.assertTrue(os.path.isfile(rot + ".1"))
        self.assertFalse(os.path.exists(rot + ".2"))
        self.assertEqual(open(untouched, encoding="utf-8").read(), "other\n")
        kept = []
        for name in (rot + ".1", rot):
            for line in open(name, encoding="utf-8"):
                kept.append(json.loads(line)["rec"])
        self.assertGreater(kept[0], 1)
        self.assertEqual(kept, list(range(kept[0], kept[-1] + 1)))
        print("PASS live_jsonl_rotate_limit failed_asserts=0")

    def test_live_log_disk_full_stops_without_uart(self) -> None:
        from live_log import LiveLog

        broker = Broker("live-hold")
        broker.start()
        self.assertTrue(broker.ready.wait(1))
        path = os.path.join(tempfile.mkdtemp(prefix="logbox-live-"), "box.jsonl")
        err = io.StringIO()
        code = logbox_cli.follow_live(
            "box1",
            {"LOGBOX_MQTT_HOST": "127.0.0.1", "LOGBOX_MQTT_PORT": str(broker.port), "LOGBOX_MQTT_TLS": "0", "LOGBOX_MQTT_PASSWORD": "SECRETTOKEN"},
            connect=lambda host, port, timeout: socket.create_connection((host, port), timeout),
            timeout=2,
            boot_id="0a1b2c3d",
            seq=1,
            at_ms=1000,
            count=1,
            err=err,
            log=LiveLog(path, fail_after=1),
        )
        broker.join(2)
        self.assertEqual(code, 4)
        self.assertIn("日志失败", err.getvalue())
        self.assertNotIn("SECRETTOKEN", err.getvalue())
        sent = b"".join(body for _topic, body in broker.publishes)
        self.assertIn(b"live_start", sent)
        self.assertNotIn(b"data_b64", sent)
        print("PASS live_jsonl_disk_full failed_asserts=0")

    def test_live_interrupt_sends_stop(self) -> None:
        broker = Broker("live-hold")
        broker.start()
        self.assertTrue(broker.ready.wait(1))
        path = os.path.join(tempfile.mkdtemp(prefix="logbox-live-"), "box.jsonl")
        env = os.environ.copy()
        env.update(
            {
                "LOGBOX_MQTT_HOST": "127.0.0.1",
                "LOGBOX_MQTT_PORT": str(broker.port),
                "LOGBOX_MQTT_TLS": "0",
                "LOGBOX_MQTT_PASSWORD": "SECRETTOKEN",
            }
        )
        proc = subprocess.Popen(
            [
                sys.executable,
                "bot/logbox_cli.py",
                "--device-id",
                "box1",
                "--op",
                "live",
                "--boot-id",
                "0a1b2c3d",
                "--seq",
                "1",
                "--at-ms",
                "1000",
                "--timeout",
                "8",
                "--live-log",
                path,
            ],
            cwd=os.path.abspath(os.path.join(os.path.dirname(__file__), "..")),
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        for _ in range(50):
            if os.path.isfile(path) and os.path.getsize(path) > 0:
                break
            time.sleep(0.05)
        proc.send_signal(signal.SIGINT)
        stdout, stderr = proc.communicate(timeout=8)
        broker.join(2)
        self.assertIn(proc.returncode, (130, -2))
        self.assertEqual(stdout, b"AB")
        text = open(path, encoding="utf-8").read()
        self.assertIn("interrupt", text)
        self.assertNotIn("SECRETTOKEN", text + stderr.decode("utf-8", "replace"))
        print("PASS live_jsonl_interrupt failed_asserts=0")


if __name__ == "__main__":
    raise SystemExit(unittest.main())
