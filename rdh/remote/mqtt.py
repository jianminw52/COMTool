"""Minimal MQTT 3.1.1 for QoS 0. No retain and no offline queue."""

from __future__ import annotations

import socket
import threading
import time
from collections import deque


class MqttError(Exception):
    pass


def _enc_len(value: int) -> bytes:
    out = bytearray()
    while True:
        digit = value % 128
        value //= 128
        if value:
            digit |= 0x80
        out.append(digit)
        if not value:
            return bytes(out)


def _enc_str(text: str) -> bytes:
    raw = text.encode("utf-8")
    return len(raw).to_bytes(2, "big") + raw


def _take_str(body: bytes, index: int) -> tuple[str, int]:
    if index + 2 > len(body):
        raise MqttError("short string")
    size = int.from_bytes(body[index : index + 2], "big")
    index += 2
    if index + size > len(body):
        raise MqttError("short string")
    return body[index : index + size].decode("utf-8"), index + size


def build_connect(client_id: str, username: str, password: str, keepalive: int = 30) -> bytes:
    flags = 0x02 | 0x80 | 0x40
    payload = _enc_str(client_id) + _enc_str(username) + _enc_str(password)
    variable = _enc_str("MQTT") + bytes([4, flags, (keepalive >> 8) & 0xFF, keepalive & 0xFF])
    body = variable + payload
    return bytes([0x10]) + _enc_len(len(body)) + body


def build_connack(code: int) -> bytes:
    return bytes([0x20, 0x02, 0x00, code & 0xFF])


def build_publish(topic: str, payload: bytes, retain: bool = False) -> bytes:
    body = _enc_str(topic) + payload
    return bytes([0x30 | (0x01 if retain else 0x00)]) + _enc_len(len(body)) + body


def build_subscribe(packet_id: int, topic: str) -> bytes:
    body = packet_id.to_bytes(2, "big") + _enc_str(topic) + bytes([0])
    return bytes([0x82]) + _enc_len(len(body)) + body


def build_suback(packet_id: int, code: int) -> bytes:
    return bytes([0x90, 0x03]) + packet_id.to_bytes(2, "big") + bytes([code & 0xFF])


def build_pingresp() -> bytes:
    return bytes([0xD0, 0x00])


def build_disconnect() -> bytes:
    return bytes([0xE0, 0x00])


def decode_packet(raw: bytes):
    if not raw:
        raise MqttError("empty")
    kind = raw[0] >> 4
    flags = raw[0] & 0x0F
    index = 1
    value = 0
    multiplier = 1
    while True:
        if index >= len(raw):
            raise MqttError("bad length")
        digit = raw[index]
        index += 1
        value += (digit & 127) * multiplier
        if not digit & 128:
            break
        multiplier *= 128
        if multiplier > 128 ** 3:
            raise MqttError("bad length")
    body = raw[index : index + value]
    if kind == 1:
        proto, cursor = _take_str(body, 0)
        level = body[cursor]
        connect_flags = body[cursor + 1]
        cursor += 4
        client_id, cursor = _take_str(body, cursor)
        username = ""
        password = ""
        if connect_flags & 0x80:
            username, cursor = _take_str(body, cursor)
        if connect_flags & 0x40:
            password, cursor = _take_str(body, cursor)
        return ("connect", {"protocol": proto, "level": level, "clean": bool(connect_flags & 0x02), "client_id": client_id, "username": username, "password": password})
    if kind == 2:
        return ("connack", body[1] if len(body) > 1 else 255)
    if kind == 3:
        topic, cursor = _take_str(body, 0)
        qos = (flags >> 1) & 0x03
        return ("publish", {"topic": topic, "payload": body[cursor:], "retain": bool(flags & 0x01), "qos": qos})
    if kind == 8:
        packet_id = int.from_bytes(body[:2], "big")
        topic, _cursor = _take_str(body, 2)
        return ("subscribe", {"id": packet_id, "topic": topic})
    if kind == 9:
        return ("suback", {"id": int.from_bytes(body[:2], "big"), "code": body[2]})
    if kind == 12:
        return ("pingreq", None)
    if kind == 13:
        return ("pingresp", None)
    if kind == 14:
        return ("disconnect", None)
    return ("unknown", kind)


class Parser:
    def __init__(self):
        self._buf = b""

    def feed(self, data: bytes) -> list:
        self._buf += data
        out = []
        while True:
            packet = self._next()
            if packet is None:
                return out
            out.append(decode_packet(packet))

    def _next(self):
        if len(self._buf) < 2:
            return None
        index = 1
        value = 0
        multiplier = 1
        while True:
            if index >= len(self._buf):
                return None
            digit = self._buf[index]
            index += 1
            value += (digit & 127) * multiplier
            if not digit & 128:
                break
            multiplier *= 128
            if index > 5:
                raise MqttError("bad length")
        total = index + value
        if len(self._buf) < total:
            return None
        packet = self._buf[:total]
        self._buf = self._buf[total:]
        return packet


def topic_match(pattern: str, topic: str) -> bool:
    left = pattern.split("/")
    right = topic.split("/")
    if len(left) != len(right):
        return False
    for pattern_part, topic_part in zip(left, right):
        if pattern_part == "+":
            if topic_part == "":
                return False
            continue
        if pattern_part != topic_part:
            return False
    return True


class MqttClient:
    def __init__(self):
        self._sock = None
        self._send_lock = threading.Lock()
        self._stop = threading.Event()
        self._thread = None
        self._parser = Parser()
        self.incoming = deque()
        self._incoming_lock = threading.Lock()
        self._subacks = deque()
        self.alive = False
        self.disconnect_reason = ""

    def connect(self, host: str, port: int, client_id: str, username: str, password: str, ssl_context=None, server_hostname: str | None = None) -> None:
        sock = socket.create_connection((host, port), timeout=3)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        if ssl_context is not None:
            sock = ssl_context.wrap_socket(sock, server_hostname=server_hostname or host)
        self._sock = sock
        self._send(build_connect(client_id, username, password))
        packet = self._read_blocking(3)
        if packet[0] != "connack" or packet[1] != 0:
            self.close()
            raise MqttError(f"connack {packet[1] if packet else 'none'}")
        self.alive = True
        self._sock.settimeout(0.2)
        self._thread = threading.Thread(target=self._loop, name="rdh-mqtt", daemon=True)
        self._thread.start()

    def subscribe(self, topic: str, timeout: float = 2) -> None:
        self._send(build_subscribe(1, topic))
        end = time.time() + timeout
        while time.time() < end:
            with self._incoming_lock:
                if self._subacks:
                    ack = self._subacks.popleft()
                    if ack["code"] == 0x80:
                        raise MqttError("subscribe rejected")
                    return
            if not self.alive:
                raise MqttError(self.disconnect_reason or "closed")
            time.sleep(0.01)
        raise MqttError("suback timeout")

    def publish(self, topic: str, payload: bytes, retain: bool = False) -> None:
        if not self.alive:
            raise MqttError("not connected")
        self._send(build_publish(topic, payload, retain=retain))

    def take(self, timeout: float):
        end = time.time() + timeout
        while time.time() < end:
            with self._incoming_lock:
                if self.incoming:
                    return self.incoming.popleft()
            if not self.alive:
                return None
            time.sleep(0.01)
        return None

    def close(self) -> None:
        self._stop.set()
        self.alive = False
        sock = self._sock
        if sock is not None:
            try:
                sock.close()
            except OSError:
                pass

    def _send(self, data: bytes) -> None:
        with self._send_lock:
            if self._sock is None:
                raise MqttError("no socket")
            self._sock.sendall(data)

    def _read_blocking(self, timeout: float):
        end = time.time() + timeout
        self._sock.settimeout(0.2)
        while time.time() < end:
            try:
                data = self._sock.recv(4096)
            except socket.timeout:
                continue
            if not data:
                raise MqttError("closed")
            packets = self._parser.feed(data)
            if packets:
                return packets[0]
        raise MqttError("timeout")

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                data = self._sock.recv(4096)
            except socket.timeout:
                continue
            except OSError as exc:
                self._fail(str(exc))
                return
            if not data:
                self._fail("eof")
                return
            try:
                packets = self._parser.feed(data)
            except MqttError as exc:
                self._fail(str(exc))
                return
            for packet in packets:
                kind, body = packet
                if kind == "publish":
                    with self._incoming_lock:
                        self.incoming.append(body)
                elif kind == "suback":
                    with self._incoming_lock:
                        self._subacks.append(body)
                elif kind == "pingreq":
                    try:
                        self._send(build_pingresp())
                    except OSError:
                        self._fail("ping")
                        return

    def _fail(self, reason: str) -> None:
        self.disconnect_reason = reason
        self.alive = False
        self._stop.set()
