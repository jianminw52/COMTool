#!/usr/bin/env python3
"""Operator commands for one log box.

The process sends nothing unless --device-id and --op are both present.
MQTT settings come from the environment. A timeout prints 未知 and does not
publish the command again.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import socket
import ssl
import struct
import sys
import time
from typing import Callable

OPS = ("status", "start", "stop", "upload", "list", "observe", "flash_prepare", "flash_commit", "live_start", "live_stop")


def _remaining(value: int) -> bytes:
    out = bytearray()
    while True:
        digit = value % 128
        value //= 128
        if value:
            digit |= 0x80
        out.append(digit)
        if not value:
            return bytes(out)


def _text(value: str) -> bytes:
    raw = value.encode("utf-8")
    return struct.pack("!H", len(raw)) + raw


def _packet(header: int, body: bytes) -> bytes:
    return bytes([header]) + _remaining(len(body)) + body


def encode_connect(client_id: str, username: str, password: str) -> bytes:
    flags = 0x02
    body = _text("MQTT") + bytes([4])
    tail = b""
    if username:
        flags |= 0x80
        tail += _text(username)
    if password:
        flags |= 0x40
        tail += _text(password)
    body += bytes([flags]) + struct.pack("!H", 15) + _text(client_id) + tail
    return _packet(0x10, body)


def encode_subscribe(topic: str, packet_id: int = 1) -> bytes:
    body = struct.pack("!H", packet_id) + _text(topic) + bytes([0])
    return _packet(0x82, body)


def encode_publish(topic: str, payload: bytes) -> bytes:
    return _packet(0x30, _text(topic) + payload)


def build_command(
    device_id: str,
    op: str,
    request_id: str,
    file_id: str | None = None,
    seq: int = 0,
    at_ms: int = 0,
    boot_id: str = "",
    manifest: str | None = None,
    confirm: str | None = None,
) -> tuple[str, str, bytes]:
    if op not in OPS or seq <= 0 or at_ms < 0 or not boot_id:
        raise ValueError("bad_control")
    if op == "flash_prepare" and not manifest:
        raise ValueError("manifest required")
    if op == "flash_commit" and not confirm:
        raise ValueError("confirm required")
    topic = f"logbox/{device_id}/cmd"
    payload: dict[str, str] = {
        "route": "control",
        "request_id": request_id,
        "op": op,
        "boot_id": boot_id,
        "seq": str(seq),
        "at_ms": str(at_ms),
    }
    if file_id:
        payload["file_id"] = file_id
    if manifest:
        payload["manifest"] = manifest
    if confirm:
        payload["confirm"] = confirm
    body = json.dumps(payload, separators=(",", ":"))
    return topic, f"logbox/{device_id}/state", body.encode("utf-8")


def build_uart(
    device_id: str, request_id: str, boot_id: str, raw: bytes, seq: int, at_ms: int
) -> tuple[str, str, bytes]:
    if not boot_id or seq <= 0 or at_ms < 0:
        raise ValueError("bad_uart")
    payload = {
        "route": "uart",
        "request_id": request_id,
        "boot_id": boot_id,
        "seq": str(seq),
        "at_ms": str(at_ms),
        "data_b64": base64.b64encode(raw).decode("ascii"),
    }
    body = json.dumps(payload, separators=(",", ":"))
    return f"logbox/{device_id}/cmd", f"logbox/{device_id}/state", body.encode("utf-8")


def build_observe(device_id: str, request_id: str) -> tuple[str, str, bytes]:
    payload = {"route": "control", "op": "observe", "request_id": request_id}
    body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    return f"logbox/{device_id}/cmd", f"logbox/{device_id}/state", body


def gate_send(state: dict | None, retry: bool) -> str:
    if state is None:
        return "manual"
    if state.get("pending") and not retry:
        return "conflict"
    if state.get("pending") and retry:
        return "retry"
    if "control_seq" not in state or "uart_seq" not in state or "boot_id" not in state:
        return "observe"
    return "ready"


def merge_observe(state: dict, remote: dict) -> str:
    if state.get("pending"):
        return "conflict"
    if "control_seq" in state and int(state["control_seq"]) != int(remote["control_seq"]):
        return "conflict"
    if "uart_seq" in state and int(state["uart_seq"]) != int(remote["uart_seq"]):
        return "conflict"
    state["boot_id"] = str(remote["boot_id"])
    state["mono_ms"] = int(remote["mono_ms"])
    state["control_seq"] = int(remote["control_seq"])
    state["uart_seq"] = int(remote["uart_seq"])
    return "ok"


def ingest_fragments(parts: list[dict]) -> tuple[bytes, list[int], list[int]]:
    seen: dict[int, bytes] = {}
    dups: list[int] = []
    for part in parts:
        index = int(part["frag"])
        raw = base64.b64decode(part.get("frag_b64") or "")
        if index in seen:
            dups.append(index)
            continue
        seen[index] = raw
    missing = [index for index in range(1, max(seen) + 1) if index not in seen] if seen else []
    data = b"".join(seen[index] for index in sorted(seen))
    return data, missing, dups


def recover_state(status: dict) -> dict[str, int | str]:
    control_seq = int(status["control_seq"])
    uart_seq = int(status["uart_seq"])
    if control_seq < 0 or uart_seq < 0 or control_seq == 2**32 - 1 or uart_seq == 2**32 - 1:
        raise ValueError("seq_exhausted")
    return {
        "boot_id": str(status["boot_id"]),
        "at_ms": int(status["mono_ms"]),
        "control_seq": control_seq + 1,
        "uart_seq": uart_seq + 1,
    }


class Session:
    def __init__(self, sock: socket.socket):
        self.sock = sock
        self.buf = b""

    def send(self, packet: bytes) -> None:
        self.sock.sendall(packet)

    def _take(self, n: int, deadline: float) -> bytes:
        while len(self.buf) < n:
            self.sock.settimeout(max(0.05, deadline - time.monotonic()))
            chunk = self.sock.recv(4096)
            if not chunk:
                raise TimeoutError("closed")
            self.buf += chunk
        out, self.buf = self.buf[:n], self.buf[n:]
        return out

    def read_packet(self, deadline: float) -> tuple[int, bytes]:
        first = self._take(1, deadline)
        remaining = 0
        shift = 0
        for _ in range(4):
            digit = self._take(1, deadline)[0]
            remaining += (digit & 0x7F) << shift
            if not digit & 0x80:
                break
            shift += 7
        else:
            raise TimeoutError("bad remaining length")
        return first[0], self._take(remaining, deadline)


def _parse_publish(body: bytes) -> tuple[str, bytes]:
    if len(body) < 2:
        raise ValueError("short publish")
    size = struct.unpack("!H", body[:2])[0]
    topic = body[2 : 2 + size].decode("utf-8")
    return topic, body[2 + size :]


def execute(
    device_id: str | None,
    op: str | None,
    env: dict[str, str],
    connect: Callable[[str, int, float], socket.socket] | None = None,
    timeout: float = 8.0,
    dry_run: bool = False,
    request_id: str | None = None,
    file_id: str | None = None,
    manifest: str | None = None,
    confirm: str | None = None,
    route: str | None = None,
    raw: bytes | None = None,
    boot_id: str | None = None,
    seq: int | None = None,
    at_ms: int | None = None,
    now: Callable[[], float] = time.monotonic,
    state: dict | None = None,
    retry: bool = False,
) -> int:
    if not device_id or (not op and route != "uart"):
        print("未发送")
        return 0
    if state is not None and op != "observe" and (seq is None or at_ms is None or not boot_id):
        gate = gate_send(state, retry)
        if gate == "conflict":
            print("冲突")
            return 2
        if gate == "retry":
            pending = state["pending"]
            op = pending["op"]
            request_id = pending["request_id"]
            file_id = pending.get("file_id")
            route = pending.get("route")
            raw = pending.get("raw")
            boot_id = pending["boot_id"]
            seq = pending["seq"]
            at_ms = pending["at_ms"]
        elif gate == "observe":
            code = execute(
                device_id, "observe", env, connect, timeout, dry_run, request_id or "observe", now=now, state=state
            )
            if code != 0 or "control_seq" not in state:
                return 2 if code == 0 else code
            seq = int(state["uart_seq"] if route == "uart" else state["control_seq"]) + 1
            at_ms = int(state["mono_ms"])
            boot_id = str(state["boot_id"])
            if seq >= 2**32:
                print("冲突")
                return 2
    if op != "observe" and (seq is None or at_ms is None or not boot_id):
        print("未发送")
        return 2
    request_id = request_id or os.urandom(8).hex()
    if op == "observe":
        cmd_topic, state_topic, payload = build_observe(device_id, request_id)
    elif route == "uart":
        if op or raw is None:
            print("未发送")
            return 2
        cmd_topic, state_topic, payload = build_uart(device_id, request_id, boot_id, raw, seq, at_ms)
    else:
        if not op or op not in OPS:
            print("未发送")
            return 2
        if op == "flash_commit" and not confirm:
            print("未发送")
            return 2
        if op == "flash_prepare" and not manifest:
            print("未发送")
            return 2
        cmd_topic, state_topic, payload = build_command(
            device_id, op, request_id, file_id, seq, at_ms, boot_id, manifest, confirm
        )
    host = env.get("LOGBOX_MQTT_HOST", "")
    if dry_run or not host:
        print(f"topic={cmd_topic}")
        print(payload.decode("utf-8"))
        if dry_run:
            print("未发送")
            return 0
        print("缺少 LOGBOX_MQTT_HOST，未发送")
        return 2
    port = int(env.get("LOGBOX_MQTT_PORT", "8883"))
    tls = env.get("LOGBOX_MQTT_TLS", "1") != "0"
    username = env.get("LOGBOX_MQTT_USERNAME", "")
    password = env.get("LOGBOX_MQTT_PASSWORD", "")
    client_id = env.get("LOGBOX_MQTT_CLIENT_ID", "") or f"logbox-operator-{request_id[:8]}"
    opener = connect or _open_socket
    published = 0
    sock = None
    try:
        sock = opener(host, port, timeout)
        if tls and connect is None:
            context = ssl.create_default_context()
            sock = context.wrap_socket(sock, server_hostname=host)
        session = Session(sock)
        deadline = now() + timeout
        session.send(encode_connect(client_id, username, password))
        kind, body = session.read_packet(deadline)
        if kind != 0x20 or len(body) < 2 or body[1] != 0:
            print(f"未知 request_id={request_id}")
            return 3
        session.send(encode_subscribe(state_topic))
        kind, _body = session.read_packet(deadline)
        if kind != 0x90:
            print(f"未知 request_id={request_id}")
            return 3
        session.send(encode_publish(cmd_topic, payload))
        published = 1
        while now() < deadline:
            kind, body = session.read_packet(deadline)
            if kind & 0xF0 != 0x30:
                continue
            topic, message = _parse_publish(body)
            if topic != state_topic:
                continue
            try:
                data = json.loads(message.decode("utf-8"))
            except (UnicodeError, json.JSONDecodeError):
                continue
            if data.get("request_id") != request_id:
                continue
            if data.get("device_id") not in (None, device_id):
                continue
            if data.get("online") is False or data.get("phase") == "offline":
                print(f"离线 request_id={request_id}")
                return 0
            if data.get("route") == "observe":
                if state is not None and merge_observe(state, data) == "conflict":
                    print("冲突")
                    return 2
                print(
                    f"boot_id={data.get('boot_id')} mono_ms={data.get('mono_ms')} "
                    f"control_seq={data.get('control_seq')} uart_seq={data.get('uart_seq')}"
                )
                return 0
            if state is not None and op != "observe":
                state.pop("pending", None)
                if route == "uart":
                    state["uart_seq"] = seq
                else:
                    state["control_seq"] = seq
            print(
                "phase={phase} session_id={session_id} valid_bytes={valid_bytes} "
                "overwritten_bytes={overwritten_bytes} time_reliability={time_reliability} "
                "remote_path={remote_path} request_id={request_id} online={online}".format(
                    phase=data.get("phase", ""),
                    session_id=data.get("session_id", ""),
                    valid_bytes=data.get("valid_bytes", ""),
                    overwritten_bytes=data.get("overwritten_bytes", ""),
                    time_reliability=data.get("time_reliability", ""),
                    remote_path=data.get("remote_path", ""),
                    request_id=request_id,
                    online=data.get("online", ""),
                )
            )
            return 0
    except (TimeoutError, socket.timeout, OSError):
        if state is not None and published and op != "observe":
            state["pending"] = {
                "op": op,
                "request_id": request_id,
                "file_id": file_id,
                "route": route,
                "raw": raw,
                "boot_id": boot_id,
                "seq": seq,
                "at_ms": at_ms,
            }
        print(f"未知 request_id={request_id}")
        return 3
    finally:
        if sock is not None:
            try:
                sock.close()
            except OSError:
                pass
    print(f"未知 request_id={request_id}")
    return 3


def accept_live_frame(cursor: dict, payload: dict, out, err, log=None, device_id: str | None = None) -> str:
    from live_log import interpret_live

    events = interpret_live(cursor, payload)
    if payload.get("route") != "live":
        return "skip"
    wrote_data = False
    for event in events:
        event["device_id"] = device_id
        note = event.get("stderr")
        if note:
            err.write(note)
            err.flush()
        raw = event.get("raw")
        if raw:
            out.write(raw)
            out.flush()
            wrote_data = True
        if log is not None:
            _record_live(log, event)
    return "data" if wrote_data else ("old" if any(item.get("event") == "old" for item in events) else "note")


def _record_live(log, event: dict) -> None:
    from live_log import text_view

    common = {
        "device_id": event.get("device_id"),
        "boot_id": event.get("boot_id"),
        "session": event.get("session"),
        "frame_seq": event.get("frame_seq"),
    }
    if event.get("event") == "data":
        raw = event["raw"]
        log.write_record(
            "data",
            {
                **common,
                "encoding": "base64",
                "nbytes": len(raw),
                "raw_b64": base64.b64encode(raw).decode("ascii"),
                "text": text_view(raw),
            },
        )
        return
    log.write_record(
        "diag",
        {
            **common,
            "event": event.get("event"),
            "dropped": event.get("dropped"),
            "truncated": event.get("truncated"),
            "gap_from": event.get("gap_from"),
        },
    )


def follow_live(
    device_id: str | None,
    env: dict[str, str],
    connect: Callable[[str, int, float], socket.socket] | None = None,
    timeout: float = 8.0,
    dry_run: bool = False,
    boot_id: str | None = None,
    seq: int | None = None,
    at_ms: int | None = None,
    count: int = 0,
    now: Callable[[], float] = time.monotonic,
    data_out=None,
    err=None,
    log_path: str | None = None,
    log_bytes: int = 1024 * 1024,
    log_keep: int = 3,
    log=None,
) -> int:
    data_out = data_out if data_out is not None else sys.stdout.buffer
    err = err if err is not None else sys.stderr
    if not device_id:
        print("未发送")
        return 0
    request_id = os.urandom(4).hex()
    start_id = request_id + "s"
    stop_id = request_id + "t"
    if boot_id is None or seq is None or at_ms is None:
        boot_id, seq, at_ms = "BOOT", 1, 0
    _topic, _state, start_payload = build_command(device_id, "live_start", start_id, None, seq, at_ms, boot_id)
    _topic, _state, stop_payload = build_command(device_id, "live_stop", stop_id, None, seq + 1, at_ms, boot_id)
    if dry_run or not env.get("LOGBOX_MQTT_HOST"):
        print(start_payload.decode("utf-8"))
        print(stop_payload.decode("utf-8"))
        print("未发送")
        return 0 if dry_run else 2
    if log is None and log_path:
        from live_log import LiveLog, LiveLogError

        try:
            log = LiveLog(log_path, max_bytes=log_bytes, keep=log_keep)
        except LiveLogError:
            err.write("日志失败\n")
            print("未发送")
            return 4
    host = env["LOGBOX_MQTT_HOST"]
    port = int(env.get("LOGBOX_MQTT_PORT", "8883"))
    tls = env.get("LOGBOX_MQTT_TLS", "1") != "0"
    username = env.get("LOGBOX_MQTT_USERNAME", "")
    password = env.get("LOGBOX_MQTT_PASSWORD", "")
    opener = connect or _open_socket
    sock = None
    started = False
    stopped = False
    session = None
    cmd_topic = f"logbox/{device_id}/cmd"
    try:
        sock = opener(host, port, timeout)
        if tls and connect is None:
            sock = ssl.create_default_context().wrap_socket(sock, server_hostname=host)
        session = Session(sock)
        deadline = now() + timeout
        session.send(encode_connect(f"logbox-live-{request_id}", username, password))
        kind, body = session.read_packet(deadline)
        if kind != 0x20 or len(body) < 2 or body[1] != 0:
            err.write("停止未确认\n")
            return 3
        state_topic = f"logbox/{device_id}/state"
        session.send(encode_subscribe(state_topic))
        kind, _body = session.read_packet(deadline)
        if kind != 0x90:
            err.write("停止未确认\n")
            return 3
        session.send(encode_publish(cmd_topic, start_payload))
        started = True
        cursor: dict = {"expect": 1}
        got = 0
        while True:
            if count and got >= count and not stopped:
                session.send(encode_publish(cmd_topic, stop_payload))
                stopped = True
            try:
                kind, body = session.read_packet(now() + timeout)
            except TimeoutError:
                if not stopped:
                    session.send(encode_publish(cmd_topic, stop_payload))
                    stopped = True
                    continue
                err.write("停止未确认\n")
                if log is not None:
                    try:
                        log.write_record(
                            "diag",
                            {
                                "device_id": device_id,
                                "boot_id": None,
                                "session": None,
                                "frame_seq": None,
                                "event": "stop_unconfirmed",
                                "dropped": None,
                            },
                        )
                    except OSError:
                        err.write("日志失败\n")
                return 3
            if kind & 0xF0 != 0x30:
                continue
            topic, message = _parse_publish(body)
            if topic != state_topic:
                continue
            try:
                data = json.loads(message.decode("utf-8"))
            except (UnicodeError, json.JSONDecodeError):
                err.write("坏帧\n")
                continue
            if data.get("op") == "live_start" and data.get("request_id") == start_id:
                cursor["expect"] = int(data.get("next_seq") or 1)
                continue
            if data.get("op") == "live_stop" and data.get("request_id") == stop_id:
                return 0
            try:
                kind_name = accept_live_frame(cursor, data, data_out, err, log=log, device_id=device_id)
            except OSError:
                err.write("日志失败\n")
                if not stopped:
                    try:
                        session.send(encode_publish(cmd_topic, stop_payload))
                    except OSError:
                        pass
                return 4
            if kind_name == "data":
                got += 1
    except KeyboardInterrupt:
        err.write("中断\n")
        if started and session is not None and not stopped:
            try:
                session.send(encode_publish(cmd_topic, stop_payload))
            except OSError:
                err.write("停止未确认\n")
        if log is not None:
            try:
                log.write_record(
                    "diag",
                    {"device_id": device_id, "boot_id": None, "session": None, "frame_seq": None, "event": "interrupt", "dropped": None},
                )
            except OSError:
                err.write("日志失败\n")
        return 130
    finally:
        if sock is not None:
            try:
                sock.close()
            except OSError:
                pass
        if log is not None:
            log.close()


def _open_socket(host: str, port: int, timeout: float) -> socket.socket:
    return socket.create_connection((host, port), timeout=timeout)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="向一台日志盒发送一条命令")
    parser.add_argument("--device-id", default="")
    parser.add_argument("--op", choices=OPS + ("live",))
    parser.add_argument("--live-count", type=int, default=0)
    parser.add_argument("--live-log", default="")
    parser.add_argument("--live-log-bytes", type=int, default=1024 * 1024)
    parser.add_argument("--live-log-keep", type=int, default=3)
    parser.add_argument("--timeout", type=float, default=8.0)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--request-id", default="")
    parser.add_argument("--file-id", default="")
    parser.add_argument("--manifest", default="")
    parser.add_argument("--confirm", default="")
    parser.add_argument("--route", choices=("control", "uart"), default="control")
    parser.add_argument("--text", default="")
    parser.add_argument("--text-lf", action="store_true", help="append one LF after --text")
    parser.add_argument("--data-file", default="")
    parser.add_argument("--boot-id", default="")
    parser.add_argument("--seq", type=int, default=0)
    parser.add_argument("--at-ms", type=int, default=-1)
    args = parser.parse_args(argv)
    raw = None
    route = args.route
    if args.text or args.data_file or args.text_lf:
        route = "uart"
    if route == "uart":
        if args.op:
            print("未发送")
            return 2
        if bool(args.text or args.text_lf) == bool(args.data_file):
            print("未发送")
            return 2
        if args.data_file:
            raw = open(args.data_file, "rb").read()
        else:
            raw = args.text.encode("utf-8")
            if args.text_lf:
                raw += b"\n"
    if args.op == "live":
        return follow_live(
            args.device_id or None,
            dict(os.environ),
            timeout=args.timeout,
            dry_run=args.dry_run,
            boot_id=args.boot_id or None,
            seq=args.seq if args.seq > 0 else None,
            at_ms=args.at_ms if args.at_ms >= 0 else None,
            count=args.live_count,
            log_path=args.live_log or None,
            log_bytes=args.live_log_bytes,
            log_keep=args.live_log_keep,
        )
    return execute(
        args.device_id or None,
        args.op,
        dict(os.environ),
        timeout=args.timeout,
        dry_run=args.dry_run,
        request_id=args.request_id or None,
        file_id=args.file_id or None,
        manifest=args.manifest or None,
        confirm=args.confirm or None,
        route=route,
        raw=raw,
        boot_id=args.boot_id or None,
        seq=args.seq if args.seq > 0 else None,
        at_ms=args.at_ms if args.at_ms >= 0 else None,
    )


if __name__ == "__main__":
    sys.exit(main())
