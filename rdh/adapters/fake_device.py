"""Scripted device owner used by tests and the explicit simulate mode.

It never opens a real COM port. The worker performs at most one write call
per ticket, and that call sits outside the execution latch.
"""

from __future__ import annotations

import queue
import threading
import time

from rdh.domain.model import (
    DeviceJob,
    DeviceShutdown,
    JobResult,
    PortCandidate,
    PortIdentity,
    WriteOutcome,
)


class FakeDevice:
    def __init__(self, max_jobs: int = 8):
        self.opens = 0
        self.lists = 0
        self.write_calls: list[bytes] = []
        self._ports = (
            PortCandidate("COM7", "示例串口（模拟）", "USB VID:PID=1234:5678 SER=SN1", 0x1234, 0x5678, "SN1"),
        )
        self._identity = PortIdentity("COM7", 0x1234, 0x5678, "SN1", "USB VID:PID=1234:5678 SER=SN1")
        self._known = {"COM7"}
        self._opened = False
        self._writer = self._default_writer
        self._line_ok = True
        self._paused = threading.Event()
        self._stop = threading.Event()
        self._q: queue.Queue = queue.Queue(maxsize=max_jobs)
        self._thread: threading.Thread | None = None
        self._abandoned = False
        self.on_rx = lambda data, hw_lost=None: None
        self.on_job = lambda result: None

    def set_writer(self, writer) -> None:
        self._writer = writer

    def set_line_result(self, applied: bool) -> None:
        self._line_ok = applied

    def pause(self) -> None:
        self._paused.set()

    def resume(self) -> None:
        self._paused.clear()

    def list_ports(self) -> tuple[PortCandidate, ...]:
        self.lists += 1
        return self._ports

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
        # Daemon so a stuck write cannot freeze the interpreter after a timed-out join.
        # shutdown() still reports that case as unfinished and does not replay it.
        self._thread = threading.Thread(target=self._loop, name="rdh-fake-device", daemon=True)
        self._thread.start()

    def shutdown(self, timeout: float) -> DeviceShutdown:
        self._stop.set()
        self.resume()
        try:
            self._q.put_nowait(None)
        except queue.Full:
            pass
        if self._thread is not None:
            self._thread.join(timeout)
            if self._thread.is_alive():
                self._abandoned = True
                return DeviceShutdown(True, "fake io thread still in a write", abandoned=True)
        self._opened = False
        return DeviceShutdown(False, "")

    def inject_rx(self, data: bytes, hw_lost: int | None = None) -> None:
        self.on_rx(data, hw_lost)

    def report_link(self, error: str) -> None:
        self._opened = False
        self.on_job(JobResult(job_id="link", kind="link", ok=False, error=error))

    def change_identity(self) -> None:
        self._identity = PortIdentity("COM7", 0x1234, 0x9999, "SN2", "USB VID:PID=1234:9999 SER=SN2")
        self.report_link("identity_changed")

    def _default_writer(self, data: bytes) -> WriteOutcome:
        return WriteOutcome("all", len(data), None)

    def _take(self):
        """Return the next job, or None. A paused worker does not take work."""

        deadline = time.time() + 0.05
        while not self._stop.is_set():
            if self._paused.is_set():
                if self._stop.wait(0.01):
                    return None
                continue
            try:
                return self._q.get_nowait()
            except queue.Empty:
                if time.time() >= deadline:
                    return None
                if self._stop.wait(0.01):
                    return None
        return None

    def _loop(self) -> None:
        while not self._stop.is_set():
            job = self._take()
            if job is None:
                continue
            if self._stop.is_set():
                if job.kind == "write" and job.latch is not None:
                    job.latch.cancel()
                    self.on_job(JobResult(job.job_id, "write", False, outcome=WriteOutcome("none", 0, "cancelled")))
                break
            self._handle(job)

    def _handle(self, job: DeviceJob) -> None:
        if job.kind == "open":
            self.opens += 1
            if job.device not in self._known or job.settings is None:
                self.on_job(JobResult(job.job_id, "open", False, error="port_not_available", open_token=job.open_token))
                return
            self._opened = True
            self.on_job(
                JobResult(
                    job.job_id,
                    "open",
                    True,
                    identity=self._identity,
                    open_token=job.open_token,
                    lines_applied=self._line_ok,
                    listed=True,
                )
            )
            return
        if job.kind == "close":
            self._opened = False
            self.on_job(JobResult(job.job_id, "close", True, open_token=job.open_token))
            return
        if job.kind == "write":
            self._write(job)

    def _write(self, job: DeviceJob) -> None:
        if not self._opened:
            if job.latch is not None:
                job.latch.cancel()
            self.on_job(JobResult(job.job_id, "write", False, outcome=WriteOutcome("none", 0, "not_open")))
            return
        begun = job.latch.try_begin() if job.latch is not None else "began"
        if begun != "began":
            self.on_job(JobResult(job.job_id, "write", False, outcome=WriteOutcome("none", 0, begun)))
            return
        try:
            outcome = self._writer(job.data)
        except Exception as exc:  # noqa: BLE001
            outcome = WriteOutcome("unknown", 0, str(exc))
        finally:
            if job.latch is not None:
                job.latch.finish()
        self.write_calls.append(bytes(job.data))
        self.on_job(JobResult(job.job_id, "write", True, outcome=outcome, read_paused=True))
