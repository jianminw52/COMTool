"""Single state owner for sessions, grants, and operation tickets.

The pump thread is the only writer of that state. Device I/O posts
results onto a bounded queue and never runs while the pump holds a lock.
Qt and MQTT are not imported here.
"""

from __future__ import annotations

import itertools
import threading
import uuid
from dataclasses import replace

from rdh import __version__
from rdh.app.queues import BoundedQueue, Subscriber
from rdh.domain.model import (
    LOCAL_PRINCIPAL,
    RESERVED_ACTIONS,
    UART_WRITE,
    Connect,
    ControlEvent,
    Decide,
    DeviceJob,
    DeviceOutcome,
    Disconnect,
    Discover,
    Effect,
    ExportDiagnostic,
    GrantState,
    GrantView,
    HostSnapshot,
    InflightView,
    JobResult,
    Limits,
    LineSettings,
    LogRecord,
    Observe,
    PendingView,
    Phase,
    Principal,
    QueryOperation,
    RecentResult,
    RelayLost,
    RequestOperation,
    Revoke,
    Scope,
    SetRelay,
    Shutdown,
    TargetSession,
    WriteOutcome,
)
from rdh.domain.policy import (
    ExecutionLatch,
    IdempotencyTable,
    SessionGate,
    decide_deadline,
    hex_preview,
    is_terminal,
    make_receipt,
    operation_digest,
    payload_sha256,
    uart_impact,
)

_TOKEN = __import__("re").compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,79}$")
_BAUD_OK = range(1, 4_000_001)
_BYTESIZE = {5, 6, 7, 8}
_PARITY = {"N", "E", "O", "M", "S"}
_STOP = {1, 1.5, 2, 1.0, 2.0}


def _new_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:16]}"


class _Holder:
    def __init__(self):
        self.cv = threading.Condition()
        self.done = False
        self.receipt = None

    def set(self, receipt) -> None:
        with self.cv:
            self.receipt = receipt
            self.done = True
            self.cv.notify_all()

    def wait(self, timeout: float):
        with self.cv:
            if not self.done:
                self.cv.wait(timeout)
            return self.receipt if self.done else None


class _Op:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class Control:
    def __init__(self, *, clock, audit, device, host_id: str = "host", simulated: bool = False, limits: Limits | None = None, clock_trust: str = "unverified_local"):
        self._clock = clock
        self._audit_port = audit
        self._device = device
        self._limits = limits or Limits()
        self.host_id = host_id
        self.simulated = simulated
        self._clock_trust = clock_trust
        self._gate = SessionGate()
        self._table = IdempotencyTable(self._limits.idempotency_capacity)
        self._ops: dict[str, _Op] = {}
        self._grants: dict[str, tuple[str, ...]] = {}
        self._session: TargetSession | None = None
        self._opening = False
        self._open_token = ""
        self._generation = 0
        self._pending_settings: LineSettings | None = None
        self._relay_state = "disabled"
        self._relay_detail = ""
        self._writes_blocked = False
        self._audit_fault = ""
        self._program = "running"
        self._shutting = False
        self._unfinished = False
        self._errors: list[str] = []
        self._recent: list[RecentResult] = []
        self._audit_memory: list[dict] = []
        self._log_ring: list[LogRecord] = []
        self._rx_seq = 0
        self._overflow_chunks = 0
        self._overflow_bytes = 0
        self._hw_loss = "unknown"
        self._rx_paused = False
        self._completion_overflow = 0
        self._pub_lock = threading.Lock()
        self._sub_lock = threading.Lock()
        self._log_subs: list[Subscriber] = []
        self._ctrl_subs: list[Subscriber] = []
        self._log_sinks: list = []
        self._ctrl_sinks: list = []
        self._snap = self._build_snapshot()
        self._commands = BoundedQueue(self._limits.control_queue)
        self._jobs = BoundedQueue(self._limits.completion_queue)
        self._rx = BoundedQueue(self._limits.rx_queue_items, self._limits.rx_queue_bytes)
        self._rx_lock = threading.Lock()
        self._ids = itertools.count(1)
        self._stop = threading.Event()
        self._hold = threading.Event()
        self._wake = threading.Event()
        self._device.on_rx = self.offer_rx
        self._device.on_job = self.offer_job
        self._device.start()
        # Daemon so a stuck pump cannot freeze process exit after a timed-out join.
        # shutdown_and_join still reports that case as unfinished and does not replay.
        self._thread = threading.Thread(target=self._loop, name="rdh-pump", daemon=True)
        self._thread.start()

    # --- threads and queues -------------------------------------------------

    def hold_pump(self) -> None:
        self._hold.set()

    def release_pump(self) -> None:
        self._hold.clear()
        self._wake.set()

    def _loop(self) -> None:
        while not self._stop.is_set():
            if self._hold.is_set():
                self._wake.wait(0.05)
                self._wake.clear()
                continue
            worked = self._drain()
            if not worked:
                self._wake.wait(0.05)
                self._wake.clear()

    def _drain(self) -> int:
        count = 0
        for result in self._jobs.get_all():
            self._on_job(result)
            count += 1
        for command, principal, holder in self._commands.get_all():
            try:
                receipt = self._dispatch(command, principal)
            except Exception as exc:  # noqa: BLE001 - keep the pump alive
                self._note_error(f"command failed: {exc}")
                receipt = make_receipt(request_id=getattr(command, "request_id", ""), phase=Phase.UNKNOWN, effect=Effect.POSSIBLE, reason="internal_error", device_outcome=DeviceOutcome.UNKNOWN)
            self._publish()
            if holder is not None:
                holder.set(receipt)
            count += 1
        for record in self._rx.get_all(limit=128):
            self._fanout_log(record)
            count += 1
        self._publish()
        return count

    def submit(self, command, principal: Principal, timeout: float = 5.0):
        if self._shutting and not isinstance(command, Shutdown):
            return make_receipt(request_id=getattr(command, "request_id", ""), phase=Phase.DENIED, reason="shutting_down")
        holder = _Holder()
        if not self._commands.try_put((command, principal, holder)):
            return make_receipt(
                request_id=getattr(command, "request_id", ""),
                phase=Phase.BUSY,
                effect=Effect.NONE,
                reason="control_queue_full",
            )
        self._wake.set()
        receipt = holder.wait(timeout)
        if receipt is None:
            return make_receipt(
                request_id=getattr(command, "request_id", ""),
                phase=Phase.UNKNOWN,
                effect=Effect.POSSIBLE,
                reason="submit_wait_timeout",
                device_outcome=DeviceOutcome.UNKNOWN,
            )
        return receipt

    def offer_rx(self, data: bytes, hw_lost: int | None = None) -> bool:
        if not data and hw_lost is None:
            return True
        with self._rx_lock:
            seq = self._rx_seq
            self._rx_seq += 1
            # Hardware loss that the driver did not report stays unknown.
            # A reported count is stored beside that fact, never as a fake zero.
            if hw_lost is not None:
                self._hw_loss = "unknown" if self._hw_loss == "unknown" else self._hw_loss
                previous = 0 if self._hw_loss == "unknown" else int(self._hw_loss.split("+", 1)[0])
                self._hw_loss = f"{previous + hw_lost}+unknown"
            record = LogRecord(seq=seq, session_id=None, data=data, mono=self._clock.mono())
            ok = self._rx.try_put(record, len(data))
            if not ok:
                self._overflow_chunks += 1
                self._overflow_bytes += len(data)
        self._wake.set()
        return ok

    def offer_job(self, result: JobResult) -> bool:
        ok = self._jobs.try_put(result)
        if not ok:
            self._completion_overflow += 1
        self._wake.set()
        return ok

    def subscribe_logs(self, name: str = "ui") -> Subscriber:
        sub = Subscriber(name, self._limits.subscriber_items, self._limits.subscriber_bytes)
        with self._sub_lock:
            self._log_subs.append(sub)
        return sub

    def subscribe_control(self, name: str = "ui") -> Subscriber:
        sub = Subscriber(name, self._limits.subscriber_items, self._limits.subscriber_bytes)
        with self._sub_lock:
            self._ctrl_subs.append(sub)
        return sub

    def add_log_sink(self, sink) -> None:
        with self._sub_lock:
            self._log_sinks.append(sink)

    def add_control_sink(self, sink) -> None:
        with self._sub_lock:
            self._ctrl_sinks.append(sink)

    def local_snapshot(self) -> HostSnapshot:
        with self._pub_lock:
            return self._snap

    def shutdown_and_join(self, timeout: float | None = None):
        timeout = self._limits.shutdown_join_s if timeout is None else timeout
        if self._stop.is_set():
            return make_receipt(
                request_id="shutdown",
                phase=Phase.DELIVERY_UNKNOWN if self._unfinished else Phase.ACCEPTED,
                effect=Effect.POSSIBLE if self._unfinished else Effect.NONE,
                reason="already_stopped",
                unfinished=self._unfinished,
                snapshot=self.local_snapshot(),
                device_outcome=DeviceOutcome.UNKNOWN if self._unfinished else DeviceOutcome.NA,
            )
        self.release_pump()
        receipt = self.submit(Shutdown(), LOCAL_PRINCIPAL, timeout=max(timeout, 1.0))
        device_report = self._device.shutdown(timeout)
        self._stop.set()
        self._wake.set()
        self._thread.join(timeout)
        pump_alive = self._thread.is_alive()
        unfinished = bool(device_report.unfinished or pump_alive or receipt.unfinished)
        detail = " ".join(
            part
            for part in (
                receipt.reason,
                device_report.detail,
                "pump abandoned" if pump_alive else "",
            )
            if part
        )
        self._unfinished = unfinished
        self._program = "stopped" if not unfinished else "stop_incomplete"
        # Publish a last snapshot from this thread. The pump is stopped.
        self._publish()
        return make_receipt(
            request_id="shutdown",
            phase=Phase.DELIVERY_UNKNOWN if unfinished else Phase.ACCEPTED,
            effect=Effect.POSSIBLE if unfinished else Effect.NONE,
            reason=detail or ("shutdown_incomplete" if unfinished else "shutdown"),
            unfinished=unfinished,
            snapshot=self.local_snapshot(),
            device_outcome=DeviceOutcome.UNKNOWN if unfinished else DeviceOutcome.NA,
        )

    # --- dispatch -----------------------------------------------------------

    def _dispatch(self, command, principal: Principal):
        if isinstance(command, Discover):
            return self._discover(command, principal)
        if isinstance(command, Connect):
            return self._connect(command, principal)
        if isinstance(command, Disconnect):
            return self._disconnect(command, principal)
        if isinstance(command, Shutdown):
            return self._shutdown(command, principal)
        if isinstance(command, Observe):
            return self._observe(command, principal)
        if isinstance(command, RequestOperation):
            return self._request(command, principal)
        if isinstance(command, Decide):
            return self._decide(command, principal)
        if isinstance(command, GrantView):
            return self._grant(command, principal)
        if isinstance(command, Revoke):
            return self._revoke_command(command, principal)
        if isinstance(command, QueryOperation):
            return self._query(command, principal)
        if isinstance(command, RelayLost):
            return self._relay_lost(command, principal)
        if isinstance(command, SetRelay):
            return self._set_relay(command, principal)
        if isinstance(command, ExportDiagnostic):
            return self._export(command, principal)
        return make_receipt(phase=Phase.UNSUPPORTED, reason="unknown_command", success_scope=Scope.NONE)

    def _local_only(self, principal: Principal, request_id: str):
        if principal.kind != "local" or principal.source != "local_ui":
            return make_receipt(request_id=request_id, phase=Phase.DENIED, reason="local_principal_required")
        return None

    def _discover(self, command: Discover, principal: Principal):
        denied = self._local_only(principal, command.request_id)
        if denied:
            return denied
        ports = tuple(self._device.list_ports())
        return make_receipt(request_id=command.request_id, phase=Phase.ACCEPTED, reason="ports_listed", candidates=ports, snapshot=self._build_snapshot())

    def _connect(self, command: Connect, principal: Principal):
        denied = self._local_only(principal, command.request_id)
        if denied:
            return denied
        if self._session is not None or self._opening:
            return make_receipt(request_id=command.request_id, phase=Phase.CONFLICT, reason="already_connected")
        if not command.device or not str(command.device).strip():
            return make_receipt(request_id=command.request_id, phase=Phase.REJECTED, reason="port_required")
        if command.baudrate not in _BAUD_OK:
            return make_receipt(request_id=command.request_id, phase=Phase.REJECTED, reason="baud_required")
        if command.bytesize not in _BYTESIZE or command.parity not in _PARITY or float(command.stopbits) not in _STOP:
            return make_receipt(request_id=command.request_id, phase=Phase.REJECTED, reason="bad_line_settings")
        settings = LineSettings(
            baudrate=int(command.baudrate),
            bytesize=int(command.bytesize),
            parity=command.parity,
            stopbits=float(command.stopbits),
            dtr=bool(command.dtr),
            rts=bool(command.rts),
        )
        self._opening = True
        self._pending_settings = settings
        self._open_token = _new_id("open")
        queued = self._device.submit_job(
            DeviceJob(job_id=self._open_token, kind="open", device=command.device, settings=settings, open_token=self._open_token)
        )
        if not queued:
            self._opening = False
            self._open_token = ""
            return make_receipt(request_id=command.request_id, phase=Phase.BUSY, reason="device_queue_full")
        return make_receipt(request_id=command.request_id, phase=Phase.ACCEPTED, reason="opening", effect=Effect.NONE)

    def _disconnect(self, command: Disconnect, principal: Principal):
        denied = self._local_only(principal, command.request_id)
        if denied:
            return denied
        close_token = str(self._generation)
        self._invalidate("disconnect", deny_phase=Phase.DENIED)
        self._device.submit_job(DeviceJob(job_id=_new_id("close"), kind="close", open_token=close_token))
        return make_receipt(request_id=command.request_id, phase=Phase.ACCEPTED, reason="disconnecting", snapshot=self._build_snapshot())

    def _shutdown(self, command: Shutdown, principal: Principal):
        denied = self._local_only(principal, command.request_id)
        if denied:
            return denied
        self._shutting = True
        self._program = "stopping"
        close_token = str(self._generation)
        executing = self._invalidate("shutdown", deny_phase=Phase.DENIED)
        self._device.submit_job(DeviceJob(job_id=_new_id("close"), kind="close", open_token=close_token))
        unfinished = executing or self._completion_overflow > 0
        self._unfinished = unfinished
        return make_receipt(
            request_id=command.request_id,
            phase=Phase.DELIVERY_UNKNOWN if unfinished else Phase.ACCEPTED,
            effect=Effect.POSSIBLE if unfinished else Effect.NONE,
            reason="shutdown_with_executing" if unfinished else "shutdown",
            unfinished=unfinished,
            device_outcome=DeviceOutcome.UNKNOWN if unfinished else DeviceOutcome.NA,
            snapshot=self._build_snapshot(),
        )

    def _observe(self, command: Observe, principal: Principal):
        if not _TOKEN.match(command.request_id):
            return make_receipt(request_id=command.request_id, phase=Phase.REJECTED, reason="bad_request_id")
        snap = self._snapshot_for(principal)
        return make_receipt(request_id=command.request_id, phase=Phase.ACCEPTED, reason="observed", effect=Effect.NONE, snapshot=snap, device_outcome=DeviceOutcome.NA)

    def _request(self, command: RequestOperation, principal: Principal):
        if principal.kind not in ("remote", "local"):
            return make_receipt(request_id=command.request_id, phase=Phase.DENIED, reason="principal_rejected")
        if not _TOKEN.match(command.request_id) or not _TOKEN.match(command.operation_id):
            return make_receipt(request_id=command.request_id, operation_id=command.operation_id, phase=Phase.REJECTED, reason="bad_id")
        if command.action in RESERVED_ACTIONS:
            return make_receipt(
                request_id=command.request_id,
                operation_id=command.operation_id,
                phase=Phase.UNSUPPORTED,
                reason="not_in_mvp",
                effect=Effect.NONE,
            )
        if command.action != UART_WRITE:
            return make_receipt(request_id=command.request_id, operation_id=command.operation_id, phase=Phase.UNSUPPORTED, reason="unknown_action")
        if not isinstance(command.payload, (bytes, bytearray)) or len(command.payload) == 0:
            return make_receipt(request_id=command.request_id, operation_id=command.operation_id, phase=Phase.REJECTED, reason="empty_payload")
        payload = bytes(command.payload)
        if len(payload) > self._limits.max_write_bytes:
            return make_receipt(request_id=command.request_id, operation_id=command.operation_id, phase=Phase.REJECTED, reason="payload_too_large")
        if not command.target_session:
            return make_receipt(request_id=command.request_id, operation_id=command.operation_id, phase=Phase.REJECTED, reason="target_required")
        decision = decide_deadline(self._clock, command.expires_at, max_wait_s=self._limits.max_wait_s, skew_s=self._limits.skew_s)
        if not decision.ok:
            phase = Phase.EXPIRED if decision.reason == "expired" else Phase.DENIED if decision.reason == "clock_untrusted" else Phase.REJECTED
            return make_receipt(
                request_id=command.request_id,
                operation_id=command.operation_id,
                phase=phase,
                reason=decision.reason,
                target_session=command.target_session,
            )
        digest = operation_digest(command.action, command.target_session, decision.canonical or "", payload)
        status, known = self._table.resolve(principal.principal_id, command.operation_id, digest)
        if status == "full":
            return make_receipt(request_id=command.request_id, operation_id=command.operation_id, phase=Phase.BUSY, reason="idempotency_full")
        if status == "conflict":
            return make_receipt(
                request_id=command.request_id,
                operation_id=command.operation_id,
                phase=Phase.CONFLICT,
                reason="idempotency_conflict",
                digest=digest,
                target_session=command.target_session,
            )
        if status == "replay" and known is not None:
            return replace(known, request_id=command.request_id)
        if self._session is None or self._session.session_id != command.target_session:
            return make_receipt(
                request_id=command.request_id,
                operation_id=command.operation_id,
                phase=Phase.DENIED,
                reason="session_mismatch",
                target_session=command.target_session,
                digest=digest,
            )
        receipt = make_receipt(
            request_id=command.request_id,
            operation_id=command.operation_id,
            phase=Phase.AWAITING,
            effect=Effect.NONE,
            reason="awaiting_local_approval",
            digest=digest,
            target_session=command.target_session,
            requested_bytes=len(payload),
            device_outcome=DeviceOutcome.NA,
        )
        op = _Op(
            operation_id=command.operation_id,
            request_id=command.request_id,
            principal_id=principal.principal_id,
            action=command.action,
            payload=payload,
            target_session=command.target_session,
            expires_at=decision.canonical,
            deadline_mono=decision.deadline_mono,
            digest=digest,
            phase=Phase.AWAITING,
            receipt=receipt,
            latch=None,
            terminal=False,
            uncertain=False,
        )
        self._ops[command.operation_id] = op
        self._table.remember(principal.principal_id, command.operation_id, digest, receipt)
        self._audit_best_effort({"record_type": "operation_requested", "operation_id": command.operation_id, "digest": digest, "payload_len": len(payload), "payload_sha256": payload_sha256(payload), "principal_id": principal.principal_id, "target_session": command.target_session})
        self._emit_operation(op)
        return receipt

    def _decide(self, command: Decide, principal: Principal):
        if principal.kind != "local" or principal.source != "local_ui":
            return make_receipt(request_id=command.request_id, operation_id=command.operation_id, phase=Phase.DENIED, reason="remote_cannot_approve", effect=Effect.NONE)
        op = self._ops.get(command.operation_id)
        if op is None:
            return make_receipt(request_id=command.request_id, operation_id=command.operation_id, phase=Phase.REJECTED, reason="not_found")
        if op.terminal:
            return replace(op.receipt, request_id=command.request_id)
        if op.phase != Phase.AWAITING:
            return make_receipt(request_id=command.request_id, operation_id=command.operation_id, phase=Phase.CONFLICT, reason="not_awaiting", digest=op.digest)
        if command.digest != op.digest or command.target_session != op.target_session:
            return make_receipt(request_id=command.request_id, operation_id=command.operation_id, phase=Phase.DENIED, reason="approval_mismatch", digest=op.digest, target_session=op.target_session)
        if not command.approve:
            return self._fail(op, Phase.DENIED, "local_denied")
        if self._shutting:
            return self._fail(op, Phase.DENIED, "shutting_down")
        if self._session is None or self._session.session_id != op.target_session:
            return self._fail(op, Phase.DENIED, "session_mismatch")
        if not self._clock.trusted():
            return self._fail(op, Phase.DENIED, "clock_untrusted")
        if self._clock.mono() >= op.deadline_mono:
            return self._fail(op, Phase.EXPIRED, "expired")
        pre = self._audit_record("pre_write", op)
        if not self._audit_required(pre):
            return self._fail(op, Phase.DENIED, "audit_unconfirmed", audit_persisted=False)
        latch = ExecutionLatch(self._gate, op.target_session, op.deadline_mono, self._clock, op.expires_at, self._limits.skew_s)
        op.latch = latch
        receipt = make_receipt(
            request_id=op.request_id,
            operation_id=op.operation_id,
            phase=Phase.ACCEPTED,
            effect=Effect.NONE,
            reason="handed_to_device",
            digest=op.digest,
            target_session=op.target_session,
            requested_bytes=len(op.payload),
            audit_persisted=True,
        )
        op.phase = Phase.ACCEPTED
        op.receipt = receipt
        self._table.remember(op.principal_id, op.operation_id, op.digest, receipt)
        queued = self._device.submit_job(DeviceJob(job_id=op.operation_id, kind="write", data=op.payload, latch=latch))
        if not queued:
            begun = latch.cancel()
            if begun == "cancelled":
                return self._fail(op, Phase.BUSY, "device_queue_full")
            op.uncertain = True
            return self._fail(op, Phase.DELIVERY_UNKNOWN, "queue_race", effect=Effect.POSSIBLE, device_outcome=DeviceOutcome.UNKNOWN)
        self._emit_operation(op)
        return receipt

    def _grant(self, command: GrantView, principal: Principal):
        denied = self._local_only(principal, command.request_id)
        if denied:
            return denied
        if not _TOKEN.match(command.principal_id) or command.principal_id in ("local", "system"):
            return make_receipt(request_id=command.request_id, phase=Phase.REJECTED, reason="bad_principal")
        scopes = tuple(scope for scope in command.scopes if scope in ("observe", "logs"))
        if not scopes:
            return make_receipt(request_id=command.request_id, phase=Phase.REJECTED, reason="empty_scopes")
        self._grants[command.principal_id] = scopes
        self._audit_best_effort({"record_type": "grant", "principal_id": command.principal_id, "scopes": list(scopes)})
        return make_receipt(request_id=command.request_id, phase=Phase.ACCEPTED, reason="granted", snapshot=self._build_snapshot())

    def _revoke_command(self, command: Revoke, principal: Principal):
        denied = self._local_only(principal, command.request_id)
        if denied:
            return denied
        self._drop_grants(command.principal_id)
        self._cancel_remote(command.principal_id, "revoked")
        self._audit_best_effort({"record_type": "revoke", "principal_id": command.principal_id or "*"})
        return make_receipt(request_id=command.request_id, phase=Phase.ACCEPTED, reason="revoked", snapshot=self._build_snapshot())

    def _query(self, command: QueryOperation, principal: Principal):
        if not _TOKEN.match(command.request_id) or not _TOKEN.match(command.operation_id):
            return make_receipt(request_id=command.request_id, phase=Phase.REJECTED, reason="bad_id")
        op = self._ops.get(command.operation_id)
        if op is None or (principal.kind == "remote" and op.principal_id != principal.principal_id):
            return make_receipt(request_id=command.request_id, operation_id=command.operation_id, phase=Phase.REJECTED, reason="not_found")
        return replace(op.receipt, request_id=command.request_id)

    def _relay_lost(self, command: RelayLost, principal: Principal):
        if principal.kind != "system":
            return make_receipt(request_id=command.request_id, phase=Phase.DENIED, reason="system_only")
        self._relay_state = "lost"
        self._relay_detail = "broker_disconnected"
        self._drop_grants(None)
        self._cancel_remote(None, "relay_lost")
        self._note_error("relay lost; remote grants cleared; capture continues")
        return make_receipt(request_id=command.request_id, phase=Phase.ACCEPTED, reason="relay_lost", snapshot=self._build_snapshot())

    def _set_relay(self, command: SetRelay, principal: Principal):
        if principal.kind != "system":
            return make_receipt(request_id=command.request_id, phase=Phase.DENIED, reason="system_only")
        if command.state not in ("disabled", "loopback_isolated", "tls", "lost", "refused"):
            return make_receipt(request_id=command.request_id, phase=Phase.REJECTED, reason="bad_relay_state")
        self._relay_state = command.state
        self._relay_detail = command.detail
        return make_receipt(request_id=command.request_id, phase=Phase.ACCEPTED, reason="relay_set", snapshot=self._build_snapshot())

    def _export(self, command: ExportDiagnostic, principal: Principal):
        denied = self._local_only(principal, command.request_id)
        if denied:
            return denied
        body = self._diagnostic(command.log_limit, command.audit_limit)
        return make_receipt(request_id=command.request_id, phase=Phase.ACCEPTED, reason="diagnostic", diagnostic=body, snapshot=self._build_snapshot())

    # --- jobs and tickets ---------------------------------------------------

    def _on_job(self, result: JobResult) -> None:
        if result.kind == "open":
            self._on_open(result)
        elif result.kind == "write":
            self._on_write(result)
        elif result.kind == "close":
            if result.open_token and result.open_token != str(self._generation):
                return
            if self._opening:
                return
            self._session = None
            self._gate.set_session(None)
        elif result.kind == "link":
            self._invalidate(result.error or "link_lost", deny_phase=Phase.DENIED)
            self._note_error(result.error or "link_lost")

    def _on_open(self, result: JobResult) -> None:
        if result.open_token != self._open_token:
            return
        self._opening = False
        self._generation += 1
        if not result.ok or result.identity is None or self._pending_settings is None:
            self._open_token = ""
            self._note_error(result.error or "open_failed")
            self._emit_simple("link", None, Phase.DENIED, result.error or "open_failed")
            return
        session = TargetSession(
            session_id=_new_id("t"),
            identity=result.identity,
            settings=self._pending_settings,
            capabilities=(UART_WRITE,),
            opened_mono=self._clock.mono(),
            lines_applied=bool(result.lines_applied),
        )
        self._session = session
        self._gate.set_session(session.session_id)
        self._audit_best_effort({"record_type": "connect", "target_session": session.session_id, "device": result.identity.device, "hwid": result.identity.hwid})
        self._emit_simple("link", None, Phase.ACCEPTED, "connected")

    def _on_write(self, result: JobResult) -> None:
        op = self._ops.get(result.job_id)
        if op is None or op.terminal:
            return
        if result.read_paused:
            self._rx_paused = True
        outcome = result.outcome or WriteOutcome("unknown", 0, "missing_outcome")
        if outcome.certainty == "none":
            reason = outcome.error or "not_started"
            phase = Phase.EXPIRED if reason == "expired" else Phase.DENIED
            if reason == "clock_untrusted":
                phase = Phase.DENIED
            self._fail(op, phase, reason)
            return
        if outcome.certainty == "all":
            reason = "revoked_after_executing" if op.uncertain else "uart_bytes_accepted"
            self._complete_transport(op, Phase.WRITTEN, reason, outcome.written)
            return
        if outcome.certainty == "partial":
            reason = "partial_write_after_revoke_no_retry" if op.uncertain else "partial_write_no_retry"
            self._complete_transport(op, Phase.PARTIAL, reason, outcome.written)
            return
        written = outcome.written if outcome.written > 0 else None
        self._complete_unknown(op, outcome.error or "delivery_unknown", written)

    def _fail(self, op: _Op, phase: str, reason: str, effect: str = Effect.NONE, device_outcome: str = DeviceOutcome.NA, audit_persisted: bool | None = None):
        if op.terminal:
            return op.receipt
        persisted = audit_persisted
        if persisted is None:
            persisted = self._audit_best_effort(self._audit_record("cancel", op, extra={"phase": phase, "reason": reason}))
        receipt = make_receipt(
            request_id=op.request_id,
            operation_id=op.operation_id,
            phase=phase,
            effect=effect,
            reason=reason,
            success_scope=Scope.NONE,
            device_outcome=device_outcome,
            requested_bytes=len(op.payload) if op.payload is not None else None,
            digest=op.digest,
            target_session=op.target_session,
            audit_persisted=persisted,
        )
        self._store(op, receipt)
        return receipt

    def _complete_transport(self, op: _Op, phase: str, reason: str, written: int) -> None:
        record = self._audit_record("post_write", op, extra={"phase": phase, "written": written, "certainty": "transport"})
        persisted = self._audit_required(record)
        receipt = make_receipt(
            request_id=op.request_id,
            operation_id=op.operation_id,
            phase=phase,
            effect=Effect.TRANSPORT,
            reason=reason,
            success_scope=Scope.TRANSPORT,
            device_outcome=DeviceOutcome.UNKNOWN,
            written_bytes=written,
            requested_bytes=len(op.payload),
            digest=op.digest,
            target_session=op.target_session,
            audit_persisted=persisted,
        )
        self._store(op, receipt)

    def _complete_unknown(self, op: _Op, reason: str, written: int | None) -> None:
        record = self._audit_record("post_write", op, extra={"phase": Phase.DELIVERY_UNKNOWN, "reason": reason})
        persisted = self._audit_best_effort(record)
        receipt = make_receipt(
            request_id=op.request_id,
            operation_id=op.operation_id,
            phase=Phase.DELIVERY_UNKNOWN,
            effect=Effect.POSSIBLE,
            reason=reason,
            success_scope=Scope.NONE,
            device_outcome=DeviceOutcome.UNKNOWN,
            written_bytes=written,
            requested_bytes=len(op.payload),
            digest=op.digest,
            target_session=op.target_session,
            audit_persisted=persisted,
        )
        self._store(op, receipt)

    def _store(self, op: _Op, receipt) -> None:
        op.terminal = is_terminal(receipt.phase)
        op.phase = receipt.phase
        op.receipt = receipt
        self._table.remember(op.principal_id, op.operation_id, op.digest, receipt)
        if op.terminal:
            self._recent.append(
                RecentResult(
                    operation_id=op.operation_id,
                    phase=receipt.phase,
                    reason=receipt.reason,
                    written_bytes=receipt.written_bytes,
                    requested_bytes=receipt.requested_bytes,
                    success_scope=receipt.success_scope,
                    device_outcome=receipt.device_outcome,
                )
            )
            del self._recent[:-8]
        self._emit_operation(op)

    def _invalidate(self, reason: str, deny_phase: str) -> bool:
        """Drop the current session. Returns True if a write had already started."""

        self._gate.set_session(None)
        self._session = None
        self._opening = False
        self._open_token = ""
        executing = False
        for op in list(self._ops.values()):
            if op.terminal:
                continue
            if op.phase == Phase.AWAITING or op.latch is None:
                self._fail(op, deny_phase, reason)
                continue
            state = op.latch.cancel()
            if state == "cancelled":
                self._fail(op, deny_phase, reason)
            elif state == "executing":
                op.uncertain = True
                executing = True
            elif state == "finished":
                executing = True
        self._audit_best_effort({"record_type": "session_invalid", "reason": reason})
        return executing

    def _drop_grants(self, principal_id: str | None) -> None:
        if principal_id is None:
            self._grants.clear()
            return
        self._grants.pop(principal_id, None)

    def _cancel_remote(self, principal_id: str | None, reason: str) -> None:
        for op in list(self._ops.values()):
            if op.terminal:
                continue
            if op.principal_id in ("local", "system"):
                continue
            if principal_id is not None and op.principal_id != principal_id:
                continue
            if op.phase == Phase.AWAITING or op.latch is None:
                self._fail(op, Phase.DENIED, reason)
                continue
            state = op.latch.cancel()
            if state == "cancelled":
                self._fail(op, Phase.DENIED, reason)
            elif state == "executing":
                op.uncertain = True

    # --- audit, snapshot, events --------------------------------------------

    def _audit_record(self, record_type: str, op: _Op, extra: dict | None = None) -> dict:
        body = {
            "record_type": record_type,
            "operation_id": op.operation_id,
            "request_id": op.request_id,
            "principal_id": op.principal_id,
            "action": op.action,
            "target_session": op.target_session,
            "digest": op.digest,
            "payload_len": len(op.payload),
            "payload_sha256": payload_sha256(op.payload),
            "expires_at": op.expires_at,
        }
        if extra:
            body.update(extra)
        return body

    def _stamp(self, record: dict) -> dict:
        stamped = dict(record)
        stamped["host_id"] = self.host_id
        stamped["wall"] = self._clock.wall().strftime("%Y-%m-%dT%H:%M:%SZ")
        stamped["mono"] = self._clock.mono()
        return stamped

    def _audit_required(self, record: dict) -> bool:
        stamped = self._stamp(record)
        try:
            ok = bool(self._audit_port.append_and_confirm(stamped))
        except Exception as exc:  # noqa: BLE001
            ok = False
            self._audit_fault = str(exc)
        self._audit_memory.append({**stamped, "persisted": ok})
        del self._audit_memory[: max(0, len(self._audit_memory) - self._limits.diagnostic_ring)]
        if ok:
            self._writes_blocked = False
            self._audit_fault = ""
        else:
            self._writes_blocked = True
            self._audit_fault = self._audit_fault or "audit_failed"
        return ok

    def _audit_best_effort(self, record: dict) -> bool:
        return self._audit_required(record)

    def _note_error(self, text: str) -> None:
        self._errors.append(text)
        del self._errors[:-20]

    def _fanout_log(self, record: LogRecord) -> None:
        session_id = self._session.session_id if self._session else None
        stamped = LogRecord(record.seq, session_id, record.data, record.mono)
        self._log_ring.append(stamped)
        del self._log_ring[: max(0, len(self._log_ring) - self._limits.diagnostic_ring)]
        with self._sub_lock:
            subs = list(self._log_subs)
            sinks = list(self._log_sinks)
        for sub in subs:
            sub.offer(stamped, len(stamped.data))
        for sink in sinks:
            sink.offer(stamped, len(stamped.data))

    def _emit_operation(self, op: _Op) -> None:
        self._emit_simple("operation", op.principal_id, op.phase, op.receipt.reason, op.operation_id)

    def _emit_simple(self, kind: str, principal_id: str | None, phase: str | None, reason: str, operation_id: str | None = None) -> None:
        event = ControlEvent(
            event_id=f"e-{next(self._ids)}",
            kind=kind,
            principal_id=principal_id,
            operation_id=operation_id,
            phase=phase,
            payload=(("reason", reason),),
        )
        with self._sub_lock:
            subs = list(self._ctrl_subs)
            sinks = list(self._ctrl_sinks)
        for sub in subs:
            sub.offer(event, 0)
        for sink in sinks:
            sink.offer(event, 0)

    def _publish(self) -> None:
        snap = self._build_snapshot()
        with self._pub_lock:
            self._snap = snap

    def _build_snapshot(self) -> HostSnapshot:
        session = self._session
        identity = session.identity if session else None
        identity_text = None
        if identity is not None:
            identity_text = f"{identity.device} vid={identity.vid} pid={identity.pid} serial={identity.serial_number} hwid={identity.hwid}"
        pending = []
        inflight = []
        for op in self._ops.values():
            if op.phase == Phase.AWAITING and not op.terminal:
                pending.append(
                    PendingView(
                        operation_id=op.operation_id,
                        request_id=op.request_id,
                        principal_id=op.principal_id,
                        action=op.action,
                        target_session=op.target_session,
                        digest=op.digest,
                        expires_at=op.expires_at,
                        nbytes=len(op.payload),
                        hex_preview=hex_preview(op.payload),
                        payload_sha256=payload_sha256(op.payload),
                        impact=uart_impact(len(op.payload)),
                    )
                )
            elif not op.terminal:
                inflight.append(InflightView(op.operation_id, op.phase, op.principal_id, len(op.payload), op.receipt.reason))
        gaps = tuple((getattr(sink, "name", "sink"), int(getattr(sink, "gap", 0))) for sink in self._log_sinks)
        return HostSnapshot(
            version=__version__,
            simulated=self.simulated,
            clock_trust=self._clock_trust if self._clock.trusted() else "untrusted",
            program_state=self._program,
            host_id=self.host_id,
            target_connected=session is not None,
            target_opening=self._opening,
            target_session=session.session_id if session else None,
            target_device=identity.device if identity else None,
            target_baud=session.settings.baudrate if session else None,
            target_format=session.settings.format_label() if session else None,
            target_dtr=session.settings.dtr if session else None,
            target_rts=session.settings.rts if session else None,
            lines_applied=session.lines_applied if session else None,
            identity_text=identity_text,
            relay_state=self._relay_state,
            relay_detail=self._relay_detail,
            grants=tuple(GrantState(pid, scopes) for pid, scopes in sorted(self._grants.items())),
            pending=tuple(pending),
            inflight=tuple(inflight),
            recent=tuple(self._recent),
            capture_seq=self._rx_seq,
            overflow_chunks=self._overflow_chunks,
            overflow_bytes=self._overflow_bytes,
            hw_loss=self._hw_loss,
            rx_paused_during_write=self._rx_paused,
            writes_blocked=self._writes_blocked,
            audit_fault=self._audit_fault,
            log_sink_gaps=gaps,
            errors=tuple(self._errors),
            unfinished=self._unfinished,
        )

    def _snapshot_for(self, principal: Principal) -> HostSnapshot:
        full = self._build_snapshot()
        if principal.kind == "local":
            return full
        scopes = self._grants.get(principal.principal_id, ())
        if "observe" not in scopes:
            return replace(
                full,
                target_session=None,
                target_device=None,
                target_baud=None,
                target_format=None,
                target_dtr=None,
                target_rts=None,
                lines_applied=None,
                identity_text=None,
                pending=tuple(item for item in full.pending if item.principal_id == principal.principal_id),
                inflight=tuple(item for item in full.inflight if item.principal_id == principal.principal_id),
                recent=tuple(item for item in full.recent if self._ops.get(item.operation_id) and self._ops[item.operation_id].principal_id == principal.principal_id),
                grants=tuple(grant for grant in full.grants if grant.principal_id == principal.principal_id),
                errors=(),
            )
        return replace(
            full,
            pending=tuple(item for item in full.pending if item.principal_id == principal.principal_id),
            inflight=tuple(item for item in full.inflight if item.principal_id == principal.principal_id),
            recent=tuple(item for item in full.recent if self._ops.get(item.operation_id) and self._ops[item.operation_id].principal_id == principal.principal_id),
            grants=tuple(grant for grant in full.grants if grant.principal_id == principal.principal_id),
            errors=(),
        )

    def _diagnostic(self, log_limit: int, audit_limit: int) -> dict:
        snap = self._build_snapshot()
        logs = [
            {
                "seq": item.seq,
                "session_id": item.session_id,
                "mono": item.mono,
                "text": item.data.decode("utf-8", errors="replace"),
            }
            for item in self._log_ring[-max(0, log_limit) :]
        ]
        audit = [dict(item) for item in self._audit_memory[-max(0, audit_limit) :]]
        body = {
            "protocol": "rdh",
            "version": 1,
            "program": {"version": snap.version, "state": snap.program_state, "simulated": snap.simulated, "clock_trust": snap.clock_trust},
            "target": {
                "connected": snap.target_connected,
                "session": snap.target_session,
                "device": snap.target_device,
                "baud": snap.target_baud,
                "format": snap.target_format,
            },
            "relay": {"state": snap.relay_state, "detail": snap.relay_detail},
            "permission": {"grants": [{"principal_id": g.principal_id, "scopes": list(g.scopes)} for g in snap.grants]},
            "capture": {"seq": snap.capture_seq, "overflow_chunks": snap.overflow_chunks, "hw_loss": snap.hw_loss},
            "audit_fault": snap.audit_fault,
            "errors": list(snap.errors),
            "logs": logs,
            "audit": audit,
            "unimplemented": ["adb.exec", "net.debug", "flash.write"],
        }
        _reject_secrets(body)
        return body


_SECRET_PARTS = ("password", "secret", "token", "private_key", "passwd", "authorization")


def _reject_secrets(value) -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            lowered = str(key).lower()
            if any(part in lowered for part in _SECRET_PARTS):
                raise RuntimeError(f"diagnostic refused secret field {key}")
            _reject_secrets(item)
            if isinstance(item, str) and "BEGIN PRIVATE KEY" in item:
                raise RuntimeError("diagnostic refused private key")
    elif isinstance(value, (list, tuple)):
        for item in value:
            _reject_secrets(item)
            if isinstance(item, str) and "BEGIN PRIVATE KEY" in item:
                raise RuntimeError("diagnostic refused private key")
