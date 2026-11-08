"""Host-side remote adapter.

It maps an ACL-constrained topic to a principal and submits that command
through the same use cases as the window. It never calls Decide.
"""

from __future__ import annotations

import json
import queue
import threading

from rdh.domain.model import SYSTEM_PRINCIPAL, Principal, SetRelay
from rdh.remote.codec import (
    WireError,
    decode_request,
    encode_body,
    error_receipt,
    event_topic,
    principal_from_request_topic,
    receipt_dict,
    response_topic,
)
from rdh.remote.mqtt import MqttClient, MqttError


class RemoteLogSink:
    def __init__(self, adapter):
        self.name = "remote-log"
        self.adapter = adapter

    @property
    def gap(self) -> int:
        return self.adapter.log_gap

    def offer(self, record, nbytes: int = 0) -> bool:
        del nbytes
        return self.adapter.offer_log(record)


class RemoteControlSink:
    def __init__(self, adapter):
        self.name = "remote-control"
        self.adapter = adapter

    @property
    def gap(self) -> int:
        return self.adapter.control_gap

    def offer(self, event, nbytes: int = 0) -> bool:
        del nbytes
        return self.adapter.offer_control(event)


class RemoteAdapter:
    def __init__(self, control, host_id: str, broker_host: str, broker_port: int, username: str, password: str, mode: str, ssl_context=None, server_hostname: str | None = None):
        self.control = control
        self.host_id = host_id
        self.mode = mode
        self._username = username
        self._password = password
        self._broker_host = broker_host
        self._broker_port = broker_port
        self._ssl = ssl_context
        self._server_hostname = server_hostname
        self.client = MqttClient()
        self._logs: queue.Queue = queue.Queue(maxsize=256)
        self._events: queue.Queue = queue.Queue(maxsize=128)
        self.log_gap = 0
        self.control_gap = 0
        self._stop = threading.Event()
        self._thread = None
        self._lost_sent = False

    def start(self) -> None:
        client_id = f"rdh-host-{self.host_id}"
        self.client.connect(
            self._broker_host,
            self._broker_port,
            client_id,
            self._username,
            self._password,
            ssl_context=self._ssl,
            server_hostname=self._server_hostname,
        )
        self.client.subscribe(f"rdh/v1/hosts/{self.host_id}/requests/+")
        self.control.add_log_sink(RemoteLogSink(self))
        self.control.add_control_sink(RemoteControlSink(self))
        self.control.submit(SetRelay(self.mode, "up"), SYSTEM_PRINCIPAL)
        self._thread = threading.Thread(target=self._loop, name="rdh-remote", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self.client.close()
        if self._thread is not None:
            self._thread.join(1)

    def offer_log(self, record) -> bool:
        snapshot = self.control.local_snapshot()
        recipients = tuple(item.principal_id for item in snapshot.grants if "logs" in item.scopes)
        if not recipients:
            return True
        try:
            self._logs.put_nowait((record, recipients))
            return True
        except queue.Full:
            self.log_gap += 1
            return False

    def offer_control(self, event) -> bool:
        if not event.principal_id or event.principal_id in ("local", "system"):
            return True
        try:
            self._events.put_nowait(event)
            return True
        except queue.Full:
            self.control_gap += 1
            return False

    def _loop(self) -> None:
        while not self._stop.is_set():
            self._flush()
            message = self.client.take(0.05)
            if message is None:
                if not self.client.alive:
                    self._lost()
                    return
                continue
            try:
                self._on_publish(message)
            except MqttError:
                self._lost()
                return

    def _on_publish(self, message) -> None:
        principal_id = principal_from_request_topic(message["topic"], self.host_id)
        if principal_id is None:
            return
        principal = Principal(principal_id, "remote", "mqtt_topic")
        try:
            command = decode_request(message["payload"], principal)
        except WireError as exc:
            self._send(response_topic(self.host_id, principal_id), receipt_dict(error_receipt(exc)))
            return
        receipt = self.control.submit(command, principal, timeout=5)
        self._send(response_topic(self.host_id, principal_id), receipt_dict(receipt))

    def _flush(self) -> None:
        while True:
            try:
                event = self._events.get_nowait()
            except queue.Empty:
                break
            body = {
                "protocol": "rdh",
                "version": 1,
                "kind": "operation",
                "event_id": event.event_id,
                "operation_id": event.operation_id,
                "phase": event.phase,
                "gap": self.control_gap,
            }
            body.update(dict(event.payload))
            self._send(event_topic(self.host_id, event.principal_id), body)
        while True:
            try:
                record, recipients = self._logs.get_nowait()
            except queue.Empty:
                break
            text = record.data.decode("utf-8", errors="replace")
            for principal_id in recipients:
                self._send(
                    event_topic(self.host_id, principal_id),
                    {
                        "protocol": "rdh",
                        "version": 1,
                        "kind": "log",
                        "seq": record.seq,
                        "text": text,
                        "gap": self.log_gap,
                    },
                )

    def _send(self, topic: str, body: dict) -> None:
        self.client.publish(topic, encode_body(body), retain=False)

    def _lost(self) -> None:
        if self._lost_sent:
            return
        self._lost_sent = True
        while True:
            try:
                self._events.get_nowait()
            except queue.Empty:
                break
        while True:
            try:
                self._logs.get_nowait()
            except queue.Empty:
                break
        from rdh.domain.model import RelayLost

        try:
            self.control.submit(RelayLost(), SYSTEM_PRINCIPAL, timeout=2)
        except Exception:
            pass
