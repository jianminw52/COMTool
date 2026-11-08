"""One pyserial owner. Listing ports does not open them.

Writes are a single driver call. A short write is returned as partial and
is not completed by a second call. DTR/RTS are requested, not guaranteed,
and a PTY that rejects the ioctl is recorded instead of treated as success.
"""

from __future__ import annotations

import os
import queue
import threading
import time

from rdh.domain.model import (
    DeviceJob,
    DeviceShutdown,
    JobResult,
    LineSettings,
    PortCandidate,
    PortIdentity,
    WriteOutcome,
)


def list_serial_ports() -> tuple[PortCandidate, ...]:
    import serial.tools.list_ports

    found = []
    for port in serial.tools.list_ports.comports():
        found.append(
            PortCandidate(
                device=port.device,
                description=port.description or "",
                hwid=port.hwid or "",
                vid=port.vid,
                pid=port.pid,
                serial_number=port.serial_number,
            )
        )
    return tuple(found)


def lookup_identity(device: str, ports: tuple[PortCandidate, ...] | None = None) -> tuple[PortIdentity, bool]:
    ports = list_serial_ports() if ports is None else ports
    for port in ports:
        if port.device == device:
            return PortIdentity(port.device, port.vid, port.pid, port.serial_number, port.hwid), True
    return PortIdentity(device, None, None, None, ""), False


def _set_lines(handle, dtr: bool, rts: bool) -> bool:
    applied = True
    for name, value in (("dtr", dtr), ("rts", rts)):
        try:
            setattr(handle, name, bool(value))
        except OSError:
            applied = False
    return applied


class SerialHandle:
    def __init__(self, port, lines_applied: bool, identity: PortIdentity, listed: bool):
        self.port = port
        self.lines_applied = lines_applied
        self.identity = identity
        self.listed = listed
        self.write_calls = 0

    def write_once(self, data: bytes) -> WriteOutcome:
        self.write_calls += 1
        self.port.write_timeout = 0
        try:
            written = self.port.write(data)
        except Exception as exc:  # noqa: BLE001 - driver failures are delivery outcomes
            return WriteOutcome("unknown", 0, f"{type(exc).__name__}: {exc}")
        if written == len(data):
            return WriteOutcome("all", int(written), None)
        if written and written > 0:
            return WriteOutcome("partial", int(written), "short_write")
        return WriteOutcome("none", 0, "zero_write")

    def read(self, size: int) -> bytes:
        data = self.port.read(size)
        return bytes(data or b"")

    def close(self) -> None:
        self.port.close()


def open_serial_port(device: str, settings: LineSettings) -> SerialHandle:
    import serial

    port = serial.Serial()
    port.port = device
    port.baudrate = int(settings.baudrate)
    port.bytesize = int(settings.bytesize)
    port.parity = settings.parity
    port.stopbits = float(settings.stopbits)
    port.timeout = 0.05
    port.write_timeout = 0
    port.xonxoff = False
    port.rtscts = False
    port.dsrdtr = False
    if os.name == "posix":
        port.exclusive = True
    try:
        port.dtr = False
        port.rts = False
    except OSError:
        pass
    port.open()
    applied = _set_lines(port, settings.dtr, settings.rts)
    identity, listed = lookup_identity(device)
    return SerialHandle(port, applied, identity, listed)


class SerialDevice:
    def __init__(self, opener=None, lister=None, max_jobs: int = 8):
        self._opener = opener or open_serial_port
        self._lister = lister or list_serial_ports
        self._q: queue.Queue = queue.Queue(maxsize=max_jobs)
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._handle: SerialHandle | None = None
        self._device_name = ""
        self._last_ident = 0.0
        self.on_rx = lambda data, hw_lost=None: None
        self.on_job = lambda result: None

    def list_ports(self) -> tuple[PortCandidate, ...]:
        return tuple(self._lister())

    def submit_job(self, job: DeviceJob) -> bool:
        try:
            self._q.put_nowait(job)
            return True
        except queue.Full:
            return False

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, name="rdh-serial", daemon=True)
        self._thread.start()

    def shutdown(self, timeout: float) -> DeviceShutdown:
        self._stop.set()
        try:
            self._q.put_nowait(None)
        except queue.Full:
            pass
        if self._thread is not None:
            self._thread.join(timeout)
            if self._thread.is_alive():
                return DeviceShutdown(True, "serial thread still in a write", abandoned=True)
        self._close_handle()
        return DeviceShutdown(False, "")

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                job = self._q.get(timeout=0.05)
            except queue.Empty:
                self._idle()
                continue
            if job is None or self._stop.is_set():
                if job is not None and getattr(job, "kind", "") == "write" and job.latch is not None:
                    job.latch.cancel()
                    self.on_job(JobResult(job.job_id, "write", False, outcome=WriteOutcome("none", 0, "cancelled")))
                break
            self._handle_job(job)
            self._idle()

    def _idle(self) -> None:
        handle = self._handle
        if handle is None:
            return
        try:
            data = handle.read(4096)
        except Exception as exc:  # noqa: BLE001
            self._handle = None
            try:
                handle.close()
            except Exception:
                pass
            self.on_job(JobResult("link", "link", False, error=f"read_failed:{exc}"))
            return
        if data:
            self.on_rx(data, None)
        self._poll_identity(handle)

    def _poll_identity(self, handle: SerialHandle) -> None:
        if not handle.listed:
            return
        now = time.monotonic()
        if now - self._last_ident < 1.0:
            return
        self._last_ident = now
        current, found = lookup_identity(self._device_name, self.list_ports())
        if not found:
            self._close_handle()
            self.on_job(JobResult("link", "link", False, error="unplugged"))
            return
        if not current.same_as(handle.identity):
            self._close_handle()
            self.on_job(JobResult("link", "link", False, error="identity_changed"))

    def _handle_job(self, job: DeviceJob) -> None:
        if job.kind == "open":
            self._open(job)
        elif job.kind == "close":
            self._close_handle()
            self.on_job(JobResult(job.job_id, "close", True, open_token=job.open_token))
        elif job.kind == "write":
            self._write(job)

    def _open(self, job: DeviceJob) -> None:
        if self._handle is not None:
            self.on_job(JobResult(job.job_id, "open", False, error="already_open", open_token=job.open_token))
            return
        try:
            handle = self._opener(job.device, job.settings)
        except Exception as exc:  # noqa: BLE001
            self.on_job(JobResult(job.job_id, "open", False, error=f"{type(exc).__name__}: {exc}", open_token=job.open_token))
            return
        self._handle = handle
        self._device_name = job.device
        self._last_ident = time.monotonic()
        self.on_job(
            JobResult(
                job.job_id,
                "open",
                True,
                identity=handle.identity,
                open_token=job.open_token,
                lines_applied=handle.lines_applied,
                listed=handle.listed,
            )
        )

    def _write(self, job: DeviceJob) -> None:
        handle = self._handle
        if handle is None:
            if job.latch is not None:
                job.latch.cancel()
            self.on_job(JobResult(job.job_id, "write", False, outcome=WriteOutcome("none", 0, "not_open")))
            return
        begun = job.latch.try_begin() if job.latch is not None else "began"
        if begun != "began":
            self.on_job(JobResult(job.job_id, "write", False, outcome=WriteOutcome("none", 0, begun)))
            return
        try:
            outcome = handle.write_once(job.data)
        except Exception as exc:  # noqa: BLE001
            outcome = WriteOutcome("unknown", 0, str(exc))
        finally:
            if job.latch is not None:
                job.latch.finish()
        self.on_job(JobResult(job.job_id, "write", True, outcome=outcome, read_paused=True))

    def _close_handle(self) -> None:
        handle = self._handle
        self._handle = None
        if handle is not None:
            try:
                handle.close()
            except Exception:
                pass
