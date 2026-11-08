"""MQTT 3.1.1 device client.

Packets are the same codec the operator CLI already uses. paho is not
imported: the contract client speaks QoS 0 with the standard library, and
this process has to accept those packets.
"""

from __future__ import annotations

import json
import queue
import socket
import ssl
import threading
import time

from bot import logbox_cli


class MqttDevice:
    def __init__(self, session, settings: dict, on_change=None):
        self.session = session
        self.settings = dict(settings)
        self.on_change = on_change or (lambda: None)
        self.stop = threading.Event()
        self.out: queue.Queue[dict] = queue.Queue()
        self.connected = False
        self.last_error = ""
        self._thread: threading.Thread | None = None

    def update(self, settings: dict) -> None:
        self.settings = dict(settings)

    def publish(self, body: dict) -> None:
        item = dict(body)
        item["device_id"] = self.settings.get("device_id") or ""
        self.out.put(item)

    def start(self) -> None:
        self.stop.clear()
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self._run, name="host-mqtt", daemon=True)
        self._thread.start()

    def shutdown(self) -> None:
        self.stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)
        self.connected = False

    def _run(self) -> None:
        while not self.stop.is_set():
            host = (self.settings.get("broker_host") or "").strip()
            device = (self.settings.get("device_id") or "").strip()
            if not host or not device:
                self._set_connected(False, "未配置 broker 或 device id")
                if self.stop.wait(0.5):
                    return
                continue
            try:
                self._session(host, device)
            except Exception as exc:
                self._set_connected(False, str(exc))
                self.session.drop_pending("broker")
            if self.stop.wait(1.0):
                return

    def _session(self, host: str, device: str) -> None:
        port = int(self.settings.get("broker_port") or 8883)
        use_tls = bool(self.settings.get("broker_tls", True))
        username = self.settings.get("broker_username") or ""
        password = self.settings.get("broker_password") or ""
        cmd = f"logbox/{device}/cmd"
        sock = socket.create_connection((host, port), timeout=5)
        try:
            if use_tls:
                sock = ssl.create_default_context().wrap_socket(sock, server_hostname=host)
            link = logbox_cli.Session(sock)
            link.send(logbox_cli.encode_connect(f"logbox-host-{device}", username, password))
            deadline = time.monotonic() + 5
            kind, body = link.read_packet(deadline)
            if kind != 0x20 or len(body) < 2 or body[1] != 0:
                raise ConnectionError("broker 拒绝连接")
            link.send(logbox_cli.encode_subscribe(cmd))
            kind, _body = link.read_packet(deadline)
            if kind != 0x90:
                raise ConnectionError("无法订阅命令主题")
            self._set_connected(True, "")
            while not self.stop.is_set():
                self.session.expire_due()
                self._flush(link, cmd.replace("/cmd", "/state"))
                try:
                    kind, packet = link.read_packet(time.monotonic() + 0.3)
                except (TimeoutError, socket.timeout):
                    continue
                if kind & 0xF0 != 0x30:
                    continue
                topic, payload = logbox_cli._parse_publish(packet)
                if topic != cmd:
                    continue
                self.session.handle(payload)
                self._flush(link, f"logbox/{device}/state")
        finally:
            self._set_connected(False, self.last_error)
            try:
                sock.close()
            except OSError:
                pass

    def _flush(self, link, state_topic: str) -> None:
        while True:
            try:
                body = self.out.get_nowait()
            except queue.Empty:
                return
            raw = json.dumps(body, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
            link.send(logbox_cli.encode_publish(state_topic, raw))

    def _set_connected(self, value: bool, error: str) -> None:
        self.connected = value
        if error and "password" not in error.lower():
            self.last_error = error
        elif not error:
            self.last_error = ""
        try:
            self.on_change()
        except Exception:
            pass
