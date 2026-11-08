"""Tiny QoS-0 broker for 127.0.0.1 tests. It is not a field broker."""

from __future__ import annotations

import socket
import threading

from bot import logbox_cli


class LoopbackBroker(threading.Thread):
    def __init__(self):
        super().__init__(daemon=True)
        self.published: list[tuple[str, bytes]] = []
        self._subs: list[tuple[socket.socket, str]] = []
        self._lock = threading.Lock()
        self._listener = socket.socket()
        self._listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._listener.bind(("127.0.0.1", 0))
        self._listener.listen(8)
        self.port = self._listener.getsockname()[1]
        self._stop = threading.Event()

    def run(self) -> None:
        self._listener.settimeout(0.2)
        while not self._stop.is_set():
            try:
                conn, _addr = self._listener.accept()
            except socket.timeout:
                continue
            threading.Thread(target=self._client, args=(conn,), daemon=True).start()
        self._listener.close()

    def stop(self) -> None:
        self._stop.set()
        try:
            self._listener.close()
        except OSError:
            pass

    def _client(self, conn: socket.socket) -> None:
        conn.settimeout(0.5)
        try:
            kind, _body = _read(conn)
            if kind != 0x10:
                return
            conn.sendall(logbox_cli._packet(0x20, bytes([0, 0])))
            while not self._stop.is_set():
                try:
                    kind, body = _read(conn)
                except (TimeoutError, socket.timeout):
                    continue
                if kind == 0x82:
                    size = int.from_bytes(body[2:4], "big")
                    topic = body[4 : 4 + size].decode()
                    with self._lock:
                        self._subs.append((conn, topic))
                    conn.sendall(logbox_cli._packet(0x90, bytes([0, 1, 0])))
                elif kind & 0xF0 == 0x30:
                    topic, payload = logbox_cli._parse_publish(body)
                    with self._lock:
                        self.published.append((topic, payload))
                        targets = [sock for sock, sub in self._subs if sub == topic and sock is not conn]
                    packet = logbox_cli.encode_publish(topic, payload)
                    for sock in targets:
                        try:
                            sock.sendall(packet)
                        except OSError:
                            continue
                elif kind == 0xC0:
                    conn.sendall(bytes([0xD0, 0]))
                elif kind == 0xE0:
                    return
        except Exception:
            return
        finally:
            with self._lock:
                self._subs = [(sock, topic) for sock, topic in self._subs if sock is not conn]
            try:
                conn.close()
            except OSError:
                pass


def _read(conn: socket.socket):
    header = conn.recv(1)
    if not header:
        raise ConnectionError("closed")
    remaining = 0
    shift = 0
    for _ in range(4):
        digit = conn.recv(1)
        if not digit:
            raise ConnectionError("closed")
        value = digit[0]
        remaining += (value & 0x7F) << shift
        if not value & 0x80:
            break
        shift += 7
    body = b""
    while len(body) < remaining:
        chunk = conn.recv(remaining - len(body))
        if not chunk:
            raise ConnectionError("closed")
        body += chunk
    return header[0], body
