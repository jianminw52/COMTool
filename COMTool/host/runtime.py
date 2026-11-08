"""Wires the single serial owner, the approval session, and MQTT."""

from __future__ import annotations

import os
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from COMTool.host.audit import AuditLog
from COMTool.host.config import config_dir, load
from COMTool.host.mqtt_device import MqttDevice
from COMTool.host.serial_owner import SerialOwner
from COMTool.host.session import HostSession


class Runtime:
    def __init__(self, settings: dict | None = None, *, owner: SerialOwner | None = None, audit_path: str | None = None):
        self.settings = dict(settings or load())
        self.owner = owner or SerialOwner()
        path = audit_path or os.path.join(config_dir(), "host-operations.jsonl")
        self.audit = AuditLog(path)
        self.session = HostSession(
            self.owner,
            self.audit,
            self.publish,
            approval_seconds=float(self.settings.get("approval_seconds") or 7),
            echo_quiet_seconds=float(self.settings.get("echo_quiet_seconds") or 5),
        )
        self.mqtt = MqttDevice(self.session, self.settings)
        self.owner.subscribe(self._on_bytes)
        self.gui_listeners = []
        self._serial_log = None

    def publish(self, body: dict) -> None:
        self.mqtt.publish(body)

    def start_mqtt(self) -> None:
        self.mqtt.update(self.settings)
        self.session.approval_seconds = float(self.settings.get("approval_seconds") or 7)
        self.session.echo_quiet_seconds = float(self.settings.get("echo_quiet_seconds") or 5)
        self.mqtt.start()

    def apply_settings(self, settings: dict) -> None:
        self.settings = dict(settings)
        self.mqtt.shutdown()
        self.session.drop_pending("broker")
        self.mqtt = MqttDevice(self.session, self.settings)
        self.start_mqtt()

    def shutdown(self) -> None:
        self.session.drop_pending("shutdown")
        self.mqtt.shutdown()
        self.owner.close()
        self._close_serial_log()

    def _on_bytes(self, data: bytes) -> None:
        self._append_serial_log(data)
        self.session.on_bytes(data)
        for listener in list(self.gui_listeners):
            try:
                listener(data)
            except Exception:
                continue

    def set_serial_log(self, path: str) -> None:
        self._close_serial_log()
        self.settings["serial_log"] = path
        if path:
            os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
            self._serial_log = open(path, "a", encoding="utf-8", newline="\n")

    def _append_serial_log(self, data: bytes) -> None:
        handle = self._serial_log
        if handle is None:
            return
        text = data.decode("utf-8", errors="replace")
        handle.write(text)
        handle.flush()

    def _close_serial_log(self) -> None:
        if self._serial_log is not None:
            try:
                self._serial_log.close()
            except OSError:
                pass
            self._serial_log = None
