"""Loopback MQTT broker.

It checks username, password, and topic ACL. It does not keep retained
messages or an offline queue, and it has no API that approves a write.
"""

from __future__ import annotations

import hmac
import socket
import threading

from rdh.remote.mqtt import (
    MqttError,
    Parser,
    build_connack,
    build_publish,
    build_suback,
    build_pingresp,
    topic_match,
)


class LoopbackBroker:
    def __init__(self):
        self._accounts: dict[str, tuple[str, tuple[str, ...], tuple[str, ...]]] = {}
        self._clients = []
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind(("127.0.0.1", 0))
        self.port = self._sock.getsockname()[1]
        self._server_ctx = None
        self.dropped_retained = 0

    def add_account(self, username: str, password: str, publish: list[str], subscribe: list[str]) -> None:
        self._accounts[username] = (password, tuple(publish), tuple(subscribe))

    def enable_tls(self, certfile: str, keyfile: str) -> None:
        import ssl

        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        context.load_cert_chain(certfile, keyfile)
        self._server_ctx = context

    def start(self) -> None:
        self._sock.listen(32)
        self._sock.settimeout(0.2)
        threading.Thread(target=self._accept, name="rdh-broker", daemon=True).start()

    def stop(self) -> None:
        self._stop.set()
        self.drop_all()
        try:
            self._sock.close()
        except OSError:
            pass

    def drop_all(self) -> None:
        with self._lock:
            clients = list(self._clients)
            self._clients.clear()
        for client in clients:
            client.close()

    def _accept(self) -> None:
        while not self._stop.is_set():
            try:
                conn, _addr = self._sock.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            if self._server_ctx is not None:
                try:
                    conn = self._server_ctx.wrap_socket(conn, server_side=True)
                except Exception:
                    conn.close()
                    continue
            threading.Thread(target=self._serve, args=(conn,), name="rdh-broker-client", daemon=True).start()

    def _serve(self, conn) -> None:
        session = _Session(conn)
        parser = Parser()
        conn.settimeout(0.2)
        try:
            while not self._stop.is_set() and not session.closed:
                try:
                    data = conn.recv(4096)
                except socket.timeout:
                    continue
                if not data:
                    break
                for packet in parser.feed(data):
                    if not self._handle(session, packet):
                        return
        except (MqttError, OSError, UnicodeError):
            pass
        finally:
            self._forget(session)
            session.close()

    def _handle(self, session, packet) -> bool:
        kind, body = packet
        if kind == "connect":
            return self._connect(session, body)
        if not session.username:
            session.close()
            return False
        if kind == "subscribe":
            allowed = self._allowed(session.username, body["topic"], subscribe=True)
            code = 0 if allowed else 0x80
            if allowed:
                session.subs.append(body["topic"])
            session.send(build_suback(body["id"], code))
            return True
        if kind == "publish":
            if body["retain"] or body["qos"] != 0:
                self.dropped_retained += 1
                session.close()
                return False
            if not self._allowed(session.username, body["topic"], subscribe=False):
                session.close()
                return False
            self._fanout(body["topic"], body["payload"])
            return True
        if kind == "pingreq":
            session.send(build_pingresp())
            return True
        if kind == "disconnect":
            session.close()
            return False
        return True

    def _connect(self, session, body) -> bool:
        if body["protocol"] != "MQTT" or body["level"] != 4 or not body["clean"]:
            session.send(build_connack(5))
            session.close()
            return False
        account = self._accounts.get(body["username"])
        if account is None or not hmac.compare_digest(account[0], body["password"]):
            session.send(build_connack(4))
            session.close()
            return False
        session.username = body["username"]
        session.client_id = body["client_id"]
        session.send(build_connack(0))
        with self._lock:
            self._clients.append(session)
        return True

    def _allowed(self, username: str, topic: str, subscribe: bool) -> bool:
        account = self._accounts.get(username)
        if account is None:
            return False
        patterns = account[2] if subscribe else account[1]
        return any(topic_match(pattern, topic) for pattern in patterns)

    def _fanout(self, topic: str, payload: bytes) -> None:
        packet = build_publish(topic, payload, retain=False)
        with self._lock:
            clients = list(self._clients)
        for client in clients:
            if any(topic_match(pattern, topic) for pattern in client.subs):
                try:
                    client.send(packet)
                except OSError:
                    client.close()

    def _forget(self, session) -> None:
        with self._lock:
            self._clients = [client for client in self._clients if client is not session]


class _Session:
    def __init__(self, conn):
        self._conn = conn
        self._lock = threading.Lock()
        self.username = ""
        self.client_id = ""
        self.subs: list[str] = []
        self.closed = False

    def send(self, data: bytes) -> None:
        with self._lock:
            if self.closed:
                raise OSError("closed")
            self._conn.sendall(data)

    def close(self) -> None:
        self.closed = True
        try:
            self._conn.close()
        except OSError:
            pass
