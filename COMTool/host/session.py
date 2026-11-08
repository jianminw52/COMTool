"""Device-side route, sequence, and approval gate.

The operator CLI is unchanged: it publishes logbox/<id>/cmd and waits on
logbox/<id>/state. A matching request_id makes that CLI return 0 even when
phase is denied or unsupported. The phase text is the result; this session
never treats a denied write as bytes on the wire.
"""

from __future__ import annotations

import base64
import hashlib
import json
import threading
import time
import uuid
from typing import Callable

UNSUPPORTED = {"status", "start", "stop", "upload", "list", "flash_prepare", "flash_commit"}
READ_OPS = {"observe", "live_start", "live_stop"}


def payload_sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class HostSession:
    def __init__(self, owner, audit, publisher: Callable[[dict], None], *, approval_seconds: float = 7.0, echo_quiet_seconds: float = 5.0, clock=time.monotonic):
        self.owner = owner
        self.audit = audit
        self.publisher = publisher
        self.approval_seconds = float(approval_seconds)
        self.echo_quiet_seconds = float(echo_quiet_seconds)
        self.clock = clock
        self.boot_id = uuid.uuid4().hex[:8]
        self._t0 = clock()
        self.control_seq = 0
        self.uart_seq = 0
        self.view_granted = False
        self.live_on = False
        self.live_session = 0
        self.live_seq = 0
        self.pending = None
        self.executed: dict[str, str] = {}
        self.denied: set[str] = set()
        self._lock = threading.Lock()
        self._echo_thread: threading.Thread | None = None
        self.echo_ready = threading.Event()
        self.writes = 0
        self.write_bytes = 0

    def mono_ms(self) -> int:
        return int((self.clock() - self._t0) * 1000)

    def grant_view(self, allowed: bool) -> None:
        with self._lock:
            self.view_granted = bool(allowed)
            if not allowed:
                self.live_on = False
        self.audit.write("view", allowed=bool(allowed), boot_id=self.boot_id)

    def expire_due(self) -> None:
        with self._lock:
            pending = self.pending
            if pending is None or self.clock() <= pending["approve_deadline"]:
                return
            self.pending = None
            request_id = pending["request_id"]
            sha = pending["sha256"]
            port = pending.get("port")
        self.audit.write("uart_expired", request_id=request_id, sha256=sha, port=port, result="approval_timeout", nbytes=0)
        self._reply(request_id, {"route": "uart", "phase": "approval_timeout", "uart": "not_sent", "completion": "unknown", "nbytes": 0})

    def drop_pending(self, reason: str) -> None:
        with self._lock:
            pending = self.pending
            self.pending = None
        if pending is not None:
            self.audit.write(
                "uart_dropped",
                reason=reason,
                request_id=pending["request_id"],
                sha256=pending["sha256"],
                port=pending.get("port"),
            )

    def handle(self, raw: bytes) -> None:
        try:
            message = json.loads(raw.decode("utf-8"))
        except (UnicodeError, json.JSONDecodeError):
            return
        if not isinstance(message, dict):
            return
        route = message.get("route")
        if route == "control" and message.get("op") == "observe":
            self._observe(message)
            return
        if route == "control":
            self._control(message)
            return
        if route == "uart":
            self._uart(message)
            return
        request_id = str(message.get("request_id") or "")
        self._reply(request_id, {"route": "control", "phase": "unsupported", "op": message.get("op") or ""})

    def approve(self, request_id: str, sha256: str) -> str:
        with self._lock:
            pending = self.pending
            if pending is None or pending["request_id"] != request_id or pending["sha256"] != sha256:
                return "mismatch"
            if request_id in self.executed:
                return "already"
            if request_id in self.denied:
                return "denied"
            if self.clock() > pending["approve_deadline"] or self._expired(int(pending["at_ms"])):
                self.pending = None
                self.audit.write("uart_expired", request_id=request_id, sha256=sha256, port=pending.get("port"))
                return "expired"
            identity = self.owner.identity
            if not self.owner.is_open or identity is None or identity.device != pending["port"]:
                self.pending = None
                self.audit.write("uart_dropped", reason="disconnected", request_id=request_id, sha256=sha256, port=pending.get("port"))
                return "disconnected"
            data = pending["raw"]
            seq = pending["seq"]
            port = pending["port"]
            self.pending = None
            self.executed[request_id] = sha256
            self.uart_seq = seq
        try:
            written = self.owner.write(data)
        except Exception as exc:
            self.audit.write("uart_result", request_id=request_id, sha256=sha256, port=port, result="error", error=str(exc), nbytes=0)
            self._reply(request_id, {"route": "uart", "phase": "error", "uart": "not_sent", "completion": "unknown", "error": str(exc)})
            return "error"
        self.writes += 1
        self.write_bytes += written
        self.audit.write(
            "uart_result",
            request_id=request_id,
            sha256=sha256,
            port=port,
            result="wrote",
            nbytes=written,
            note="bytes_accepted_by_port",
            seq=seq,
        )
        self._reply(
            request_id,
            {
                "route": "uart",
                "phase": "sent",
                "uart": "sent",
                "completion": "unknown",
                "nbytes": written,
                "seq": str(seq),
            },
        )
        self._start_echo(request_id, seq)
        return "wrote"

    def deny(self, request_id: str, sha256: str) -> str:
        with self._lock:
            pending = self.pending
            if pending is None or pending["request_id"] != request_id or pending["sha256"] != sha256:
                return "mismatch"
            seq = pending["seq"]
            port = pending["port"]
            self.pending = None
            self.denied.add(request_id)
            self.uart_seq = seq
        self.audit.write("uart_result", request_id=request_id, sha256=sha256, port=port, result="denied", nbytes=0, seq=seq)
        self._reply(request_id, {"route": "uart", "phase": "denied", "uart": "not_sent", "completion": "unknown", "nbytes": 0, "seq": str(seq)})
        return "denied"

    def on_bytes(self, data: bytes) -> None:
        with self._lock:
            live = self.live_on and self.view_granted
            if live:
                self.live_seq += 1
                seq = self.live_seq
                session = self.live_session
                boot = self.boot_id
        if not live:
            return
        self.publisher(
            {
                "route": "live",
                "boot_id": boot,
                "session": session,
                "seq": seq,
                "frag_b64": base64.b64encode(data).decode("ascii"),
                "truncated": False,
                "dropped": 0,
                "paused": False,
                "device_id": None,
            }
        )

    def _observe(self, message: dict) -> None:
        request_id = str(message.get("request_id") or "")
        body = {
            "route": "observe",
            "ok": True,
            "boot_id": self.boot_id,
            "mono_ms": self.mono_ms(),
            "control_seq": self.control_seq,
            "uart_seq": self.uart_seq,
        }
        self._reply(request_id, body)
        self.audit.write("observe", request_id=request_id, boot_id=self.boot_id)

    def _control(self, message: dict) -> None:
        op = str(message.get("op") or "")
        request_id = str(message.get("request_id") or "")
        if op in UNSUPPORTED or op not in READ_OPS:
            self._reply(request_id, {"route": "control", "phase": "unsupported", "op": op})
            self.audit.write("unsupported", request_id=request_id, op=op)
            return
        checked = self._check_control_seq(message)
        if checked != "ok":
            self._reply(request_id, {"route": "control", "phase": checked, "op": op})
            return
        if op == "live_start":
            if not self.view_granted:
                self._reply(request_id, {"route": "control", "phase": "unauthorized", "op": op})
                self.audit.write("live_refused", request_id=request_id)
                return
            with self._lock:
                if not self.live_on:
                    self.live_session += 1
                    self.live_seq = 0
                self.live_on = True
                session = self.live_session
                nxt = self.live_seq + 1
                seq = int(message["seq"])
                self.control_seq = seq
            self._reply(request_id, {"route": "control", "op": "live_start", "ok": True, "session": session, "next_seq": nxt, "already": False})
            self.audit.write("live_start", request_id=request_id, session=session)
            return
        if op == "live_stop":
            with self._lock:
                self.live_on = False
                session = self.live_session
                seq = int(message["seq"])
                self.control_seq = seq
            self._reply(request_id, {"route": "control", "op": "live_stop", "ok": True, "session": session, "next_seq": 1, "already": False})
            self.audit.write("live_stop", request_id=request_id, session=session)

    def _uart(self, message: dict) -> None:
        request_id = str(message.get("request_id") or "")
        try:
            raw = base64.b64decode(message.get("data_b64") or "", validate=True)
        except Exception:
            self._reply(request_id, {"route": "uart", "phase": "rejected", "uart": "not_sent"})
            return
        if not raw or len(raw) > 192:
            self._reply(request_id, {"route": "uart", "phase": "rejected", "uart": "not_sent"})
            return
        checked = self._check_uart_seq(message, raw)
        if checked != "ok":
            self._reply(request_id, {"route": "uart", "phase": checked, "uart": "not_sent"})
            self.audit.write("uart_result", request_id=request_id, result=checked, nbytes=0, sha256=payload_sha256(raw))
            return
        if not self.owner.is_open or self.owner.identity is None:
            self._reply(request_id, {"route": "uart", "phase": "not_connected", "uart": "not_sent"})
            self.audit.write("uart_result", request_id=request_id, result="not_connected", nbytes=0, sha256=payload_sha256(raw))
            return
        sha = payload_sha256(raw)
        if request_id in self.executed:
            self._reply(request_id, {"route": "uart", "phase": "already", "uart": "not_sent", "completion": "unknown"})
            return
        if request_id in self.denied:
            self._reply(request_id, {"route": "uart", "phase": "denied", "uart": "not_sent"})
            return
        with self._lock:
            current = self.pending
            if current is not None and (current["request_id"] != request_id or current["sha256"] != sha):
                self._reply(request_id, {"route": "uart", "phase": "rejected", "uart": "not_sent"})
                self.audit.write("uart_result", request_id=request_id, result="tamper", nbytes=0, sha256=sha)
                return
            if current is None:
                self.pending = {
                    "request_id": request_id,
                    "sha256": sha,
                    "raw": raw,
                    "seq": int(message["seq"]),
                    "at_ms": int(message["at_ms"]),
                    "port": self.owner.identity.device,
                    "approve_deadline": self.clock() + self.approval_seconds,
                    "preview": _preview(raw),
                }
        self.audit.write(
            "uart_pending",
            request_id=request_id,
            sha256=sha,
            port=self.owner.identity.device,
            nbytes=len(raw),
            preview=_preview(raw),
            seq=int(message["seq"]),
        )

    def _check_control_seq(self, message: dict) -> str:
        try:
            seq = int(message["seq"])
            at_ms = int(message["at_ms"])
            boot = str(message.get("boot_id") or "")
        except (KeyError, TypeError, ValueError):
            return "rejected"
        if boot != self.boot_id:
            return "rejected"
        timing = self._timing(at_ms)
        if timing != "ok":
            return timing
        with self._lock:
            if seq != self.control_seq + 1:
                return "stale_seq" if seq <= self.control_seq else "rejected"
        return "ok"

    def _check_uart_seq(self, message: dict, raw: bytes) -> str:
        try:
            seq = int(message["seq"])
            at_ms = int(message["at_ms"])
            boot = str(message.get("boot_id") or "")
        except (KeyError, TypeError, ValueError):
            return "rejected"
        if boot != self.boot_id or message.get("op"):
            return "rejected"
        timing = self._timing(at_ms)
        if timing != "ok":
            return timing
        with self._lock:
            if seq != self.uart_seq + 1:
                return "stale_seq" if seq <= self.uart_seq else "rejected"
            if self.pending is not None and self.pending["seq"] == seq and self.pending["sha256"] != payload_sha256(raw):
                return "rejected"
        return "ok"

    def _timing(self, at_ms: int) -> str:
        delta = self.mono_ms() - int(at_ms)
        if delta > 30000:
            return "expired"
        if delta < -2000:
            return "future_time"
        return "ok"

    def _expired(self, at_ms: int) -> bool:
        return self._timing(at_ms) != "ok"

    def _reply(self, request_id: str, body: dict) -> None:
        body = dict(body)
        body["request_id"] = request_id
        body.setdefault("device_id", None)
        self.publisher(body)

    def _start_echo(self, request_id: str, seq: int) -> None:
        def run() -> None:
            quiet = self.echo_quiet_seconds
            end = time.monotonic() + quiet
            last = time.monotonic()
            frag = 0
            token_buffer = bytearray()

            def take(chunk: bytes) -> None:
                token_buffer.extend(chunk)

            token = self.owner.subscribe(take)
            self.echo_ready.set()
            try:
                while time.monotonic() < end:
                    time.sleep(0.02)
                    if token_buffer:
                        data = bytes(token_buffer)
                        del token_buffer[:]
                        last = time.monotonic()
                        while data:
                            piece, data = data[:256], data[256:]
                            frag += 1
                            self.publisher(
                                {
                                    "route": "uart",
                                    "request_id": request_id,
                                    "seq": str(seq),
                                    "frag": frag,
                                    "frag_b64": base64.b64encode(piece).decode("ascii"),
                                    "window": "open",
                                    "completion": "unknown",
                                }
                            )
                    elif time.monotonic() - last >= quiet:
                        break
            finally:
                self.owner.unsubscribe(token)
            self.publisher(
                {
                    "route": "uart",
                    "request_id": request_id,
                    "seq": str(seq),
                    "frag": frag,
                    "window": "closed",
                    "completion": "unknown",
                }
            )

        threading.Thread(target=run, name="host-echo", daemon=True).start()


def _preview(raw: bytes) -> str:
    try:
        text = raw.decode("utf-8")
    except UnicodeError:
        return raw.hex()
    return text if text.isprintable() or "\n" in text or "\r" in text else raw.hex()
