"""Narrow ports the application core calls. Adapters implement them."""

from __future__ import annotations

from typing import Protocol

from rdh.domain.model import DeviceJob, DeviceShutdown, PortCandidate


class ClockPort(Protocol):
    def wall(self): ...

    def mono(self) -> float: ...

    def trusted(self) -> bool: ...


class AuditPort(Protocol):
    def append_and_confirm(self, record: dict) -> bool: ...


class DevicePort(Protocol):
    def list_ports(self) -> tuple[PortCandidate, ...]: ...

    def submit_job(self, job: DeviceJob) -> bool: ...

    def start(self) -> None: ...

    def shutdown(self, timeout: float) -> DeviceShutdown: ...
