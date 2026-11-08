"""Immutable values shared by the GUI, the CLI, and the application core."""

from __future__ import annotations

from dataclasses import dataclass


PROTOCOL = "rdh"
VERSION = 1


class Phase:
    ACCEPTED = "accepted"
    AWAITING = "awaiting_approval"
    DENIED = "denied"
    REJECTED = "rejected"
    EXPIRED = "expired"
    CONFLICT = "conflict"
    UNSUPPORTED = "unsupported"
    WRITTEN = "written"
    PARTIAL = "partially_written"
    DEVICE_OK = "device_succeeded"
    DEVICE_FAILED = "device_failed"
    UNKNOWN = "unknown"
    DELIVERY_UNKNOWN = "delivery_unknown"
    BUSY = "busy"


class Effect:
    NONE = "none"
    TRANSPORT = "transport"
    POSSIBLE = "possible"
    DEVICE = "device"


class Scope:
    NONE = "none"
    TRANSPORT = "transport"
    DEVICE = "device"


class DeviceOutcome:
    UNKNOWN = "unknown"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    NA = "not_applicable"


TERMINAL_PHASES = frozenset(
    {
        Phase.DENIED,
        Phase.REJECTED,
        Phase.EXPIRED,
        Phase.CONFLICT,
        Phase.UNSUPPORTED,
        Phase.WRITTEN,
        Phase.PARTIAL,
        Phase.DEVICE_OK,
        Phase.DEVICE_FAILED,
        Phase.UNKNOWN,
        Phase.DELIVERY_UNKNOWN,
        Phase.BUSY,
    }
)

# Present in the product vocabulary, not executed by this MVP.
RESERVED_ACTIONS = frozenset({"adb.exec", "net.debug", "flash.write"})
UART_WRITE = "uart.write"


@dataclass(frozen=True)
class Principal:
    principal_id: str
    kind: str
    source: str


LOCAL_PRINCIPAL = Principal("local", "local", "local_ui")
SYSTEM_PRINCIPAL = Principal("system", "system", "system")


@dataclass(frozen=True)
class Limits:
    max_wait_s: float = 120.0
    skew_s: float = 30.0
    idempotency_capacity: int = 256
    control_queue: int = 64
    completion_queue: int = 32
    rx_queue_items: int = 256
    rx_queue_bytes: int = 256 * 1024
    subscriber_items: int = 256
    subscriber_bytes: int = 256 * 1024
    max_write_bytes: int = 64 * 1024
    device_queue: int = 8
    shutdown_join_s: float = 3.0
    diagnostic_ring: int = 200


@dataclass(frozen=True)
class PortCandidate:
    device: str
    description: str
    hwid: str
    vid: int | None
    pid: int | None
    serial_number: str | None


@dataclass(frozen=True)
class PortIdentity:
    device: str
    vid: int | None
    pid: int | None
    serial_number: str | None
    hwid: str

    def same_as(self, other: "PortIdentity") -> bool:
        return (
            self.device == other.device
            and self.vid == other.vid
            and self.pid == other.pid
            and self.serial_number == other.serial_number
            and self.hwid == other.hwid
        )


@dataclass(frozen=True)
class LineSettings:
    baudrate: int
    bytesize: int = 8
    parity: str = "N"
    stopbits: float = 1.0
    dtr: bool = False
    rts: bool = False

    def format_label(self) -> str:
        stop = self.stopbits
        stop_text = str(int(stop)) if float(stop).is_integer() else str(stop)
        return f"{self.bytesize}{self.parity}{stop_text}"


@dataclass(frozen=True)
class TargetSession:
    session_id: str
    identity: PortIdentity
    settings: LineSettings
    capabilities: tuple[str, ...]
    opened_mono: float
    lines_applied: bool


@dataclass(frozen=True)
class WriteOutcome:
    """What the device worker observed. ``none`` means the write call did not start."""

    certainty: str  # none | all | partial | unknown
    written: int
    error: str | None = None


@dataclass(frozen=True)
class DeviceShutdown:
    unfinished: bool
    detail: str = ""
    abandoned: bool = False


@dataclass(frozen=True)
class PendingView:
    operation_id: str
    request_id: str
    principal_id: str
    action: str
    target_session: str
    digest: str
    expires_at: str
    nbytes: int
    hex_preview: str
    payload_sha256: str
    impact: str


@dataclass(frozen=True)
class InflightView:
    operation_id: str
    phase: str
    principal_id: str
    nbytes: int
    reason: str


@dataclass(frozen=True)
class RecentResult:
    operation_id: str
    phase: str
    reason: str
    written_bytes: int | None
    requested_bytes: int | None
    success_scope: str
    device_outcome: str


@dataclass(frozen=True)
class GrantState:
    principal_id: str
    scopes: tuple[str, ...]


@dataclass(frozen=True)
class HostSnapshot:
    version: str
    simulated: bool
    clock_trust: str
    program_state: str
    host_id: str
    target_connected: bool
    target_opening: bool
    target_session: str | None
    target_device: str | None
    target_baud: int | None
    target_format: str | None
    target_dtr: bool | None
    target_rts: bool | None
    lines_applied: bool | None
    identity_text: str | None
    relay_state: str
    relay_detail: str
    grants: tuple[GrantState, ...]
    pending: tuple[PendingView, ...]
    inflight: tuple[InflightView, ...]
    recent: tuple[RecentResult, ...]
    capture_seq: int
    overflow_chunks: int
    overflow_bytes: int
    hw_loss: str
    rx_paused_during_write: bool
    writes_blocked: bool
    audit_fault: str
    log_sink_gaps: tuple[tuple[str, int], ...]
    errors: tuple[str, ...]
    unfinished: bool


@dataclass(frozen=True)
class Receipt:
    request_id: str
    operation_id: str | None
    phase: str
    effect: str
    reason: str
    success_scope: str
    device_outcome: str
    written_bytes: int | None = None
    requested_bytes: int | None = None
    digest: str | None = None
    target_session: str | None = None
    audit_persisted: bool | None = None
    gap: int | None = None
    snapshot: HostSnapshot | None = None
    candidates: tuple[PortCandidate, ...] | None = None
    diagnostic: dict | None = None
    unfinished: bool = False


@dataclass(frozen=True)
class LogRecord:
    seq: int
    session_id: str | None
    data: bytes
    mono: float


@dataclass(frozen=True)
class ControlEvent:
    event_id: str
    kind: str
    principal_id: str | None
    operation_id: str | None
    phase: str | None
    payload: tuple[tuple[str, str], ...]


@dataclass(frozen=True)
class Discover:
    request_id: str = "discover"


@dataclass(frozen=True)
class Connect:
    device: str
    baudrate: int
    bytesize: int = 8
    parity: str = "N"
    stopbits: float = 1.0
    dtr: bool = False
    rts: bool = False
    request_id: str = "connect"


@dataclass(frozen=True)
class Disconnect:
    request_id: str = "disconnect"


@dataclass(frozen=True)
class Shutdown:
    request_id: str = "shutdown"


@dataclass(frozen=True)
class Observe:
    request_id: str


@dataclass(frozen=True)
class RequestOperation:
    request_id: str
    operation_id: str
    action: str
    target_session: str
    payload: bytes
    expires_at: str


@dataclass(frozen=True)
class Decide:
    request_id: str
    operation_id: str
    digest: str
    target_session: str
    approve: bool


@dataclass(frozen=True)
class GrantView:
    request_id: str
    principal_id: str
    scopes: tuple[str, ...] = ("observe", "logs")


@dataclass(frozen=True)
class Revoke:
    request_id: str
    principal_id: str | None = None


@dataclass(frozen=True)
class QueryOperation:
    request_id: str
    operation_id: str


@dataclass(frozen=True)
class RelayLost:
    request_id: str = "relay-lost"


@dataclass(frozen=True)
class SetRelay:
    state: str
    detail: str = ""
    request_id: str = "set-relay"


@dataclass(frozen=True)
class ExportDiagnostic:
    request_id: str = "export"
    log_limit: int = 50
    audit_limit: int = 50


@dataclass(frozen=True)
class DeviceJob:
    job_id: str
    kind: str
    device: str = ""
    settings: LineSettings | None = None
    data: bytes = b""
    latch: object | None = None
    open_token: str = ""


@dataclass(frozen=True)
class JobResult:
    job_id: str
    kind: str
    ok: bool
    identity: PortIdentity | None = None
    outcome: WriteOutcome | None = None
    error: str = ""
    open_token: str = ""
    lines_applied: bool | None = None
    read_paused: bool = False
    listed: bool = False
