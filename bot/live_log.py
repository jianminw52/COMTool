"""Receiver-side JSONL for one live subscription.

The device is not involved. This object owns only the path it created
and the numbered siblings it rotates itself.
"""

from __future__ import annotations

import base64
import json
import os
import time
from datetime import datetime, timezone

SCHEMA_VERSION = 1
DEFAULT_MAX_BYTES = 1024 * 1024
DEFAULT_KEEP = 3


class LiveLogError(OSError):
    pass


class LiveLog:
    def __init__(self, path: str, max_bytes: int = DEFAULT_MAX_BYTES, keep: int = DEFAULT_KEEP, mono=time.monotonic, fail_after: int | None = None):
        if not path or max_bytes < 256 or keep < 1 or keep > 20:
            raise LiveLogError("bad_log_config")
        self.path = os.path.abspath(path)
        self.max_bytes = max_bytes
        self.keep = keep
        self._mono = mono
        self._t0 = mono()
        self.rec = 0
        self._fh = None
        self._size = 0
        self._owned: list[str] = []
        self._fail_after = fail_after
        self._open_new()

    def slots(self) -> list[str]:
        names = [self.path]
        for i in range(1, self.keep):
            names.append(f"{self.path}.{i}")
        return names

    def _claim_parent(self) -> None:
        parent = os.path.dirname(self.path)
        if os.path.isdir(parent):
            return
        if os.path.exists(parent):
            raise LiveLogError("log_parent_not_dir")
        grand = os.path.dirname(parent)
        if not os.path.isdir(grand):
            raise LiveLogError("log_parent_missing")
        os.mkdir(parent, 0o700)
        os.chmod(parent, 0o700)

    def _open_new(self) -> None:
        self._claim_parent()
        for name in self.slots():
            if os.path.exists(name):
                raise LiveLogError("log_exists")
        fd = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_APPEND | os.O_WRONLY, 0o600)
        os.chmod(self.path, 0o600)
        self._fh = os.fdopen(fd, "w", encoding="utf-8", newline="\n")
        self._size = 0
        self._owned.append(self.path)

    def close(self) -> None:
        if self._fh is not None:
            self._fh.flush()
            self._fh.close()
            self._fh = None

    def _rotate(self) -> None:
        self.close()
        if self.keep == 1:
            os.unlink(self.path)
            self._owned = [name for name in self._owned if name != self.path]
        else:
            self._shift()
        fd = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_APPEND | os.O_WRONLY, 0o600)
        os.chmod(self.path, 0o600)
        self._fh = os.fdopen(fd, "w", encoding="utf-8", newline="\n")
        self._size = 0
        self._owned.append(self.path)

    def _shift(self) -> None:
        oldest = f"{self.path}.{self.keep - 1}"
        if self.keep > 1 and os.path.exists(oldest):
            if oldest not in self._owned:
                raise LiveLogError("log_foreign")
            os.unlink(oldest)
        for i in range(self.keep - 2, 0, -1):
            src = f"{self.path}.{i}"
            dst = f"{self.path}.{i + 1}"
            if os.path.exists(src):
                if src not in self._owned:
                    raise LiveLogError("log_foreign")
                os.rename(src, dst)
                self._owned = [dst if name == src else name for name in self._owned]
        os.rename(self.path, f"{self.path}.1")
        self._owned = [f"{self.path}.1" if name == self.path else name for name in self._owned]

    def write_record(self, kind: str, fields: dict) -> dict:
        if self._fh is None:
            raise LiveLogError("log_closed")
        if self._fail_after is not None and self.rec + 1 >= self._fail_after:
            raise LiveLogError(28)
        self.rec += 1
        now = datetime.now(timezone.utc)
        record = {
            "schema_version": SCHEMA_VERSION,
            "kind": kind,
            "rec": self.rec,
            "recv_utc": now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{now.microsecond:06d}Z",
            "recv_mono_ms": int((self._mono() - self._t0) * 1000),
            "device_time": None,
        }
        record.update(fields)
        line = json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n"
        encoded = line.encode("utf-8")
        if self._size > 0 and self._size + len(encoded) > self.max_bytes:
            self._rotate()
        try:
            self._fh.write(line)
            self._fh.flush()
        except OSError as exc:
            raise LiveLogError(exc.errno or "log_write") from exc
        self._size += len(encoded)
        return record


def text_view(raw: bytes) -> str | None:
    try:
        return raw.decode("utf-8")
    except UnicodeError:
        return None


def interpret_live(cursor: dict, payload: dict) -> list[dict]:
    if payload.get("route") != "live":
        return []
    events: list[dict] = []
    boot = str(payload.get("boot_id") or "")
    session = payload.get("session")
    seq = int(payload.get("seq") or 0)
    if cursor.get("boot") not in (None, boot):
        events.append({"event": "reboot", "stderr": f"重启 {boot}\n", "boot_id": boot, "session": session, "frame_seq": seq, "dropped": None})
        cursor["expect"] = 1
    cursor["boot"] = boot
    if cursor.get("session") not in (None, session):
        events.append({"event": "session", "stderr": f"会话 {session}\n", "boot_id": boot, "session": session, "frame_seq": seq, "dropped": None})
        cursor["expect"] = 1
    cursor["session"] = session
    expect = int(cursor.get("expect") or 1)
    if seq < expect:
        events.append({"event": "old", "stderr": f"旧帧 {seq}\n", "boot_id": boot, "session": session, "frame_seq": seq, "dropped": None})
        return events
    if seq > expect:
        events.append(
            {
                "event": "gap",
                "stderr": f"缺片 {expect}\n",
                "boot_id": boot,
                "session": session,
                "frame_seq": seq,
                "gap_from": expect,
                "dropped": None,
            }
        )
    paused = bool(payload.get("paused"))
    if paused != bool(cursor.get("paused")):
        events.append(
            {
                "event": "resume" if cursor.get("paused") else "pause",
                "stderr": "恢复\n" if cursor.get("paused") else "暂停\n",
                "boot_id": boot,
                "session": session,
                "frame_seq": seq,
                "dropped": None,
            }
        )
        cursor["paused"] = paused
    if payload.get("truncated"):
        dropped = payload.get("dropped")
        known = dropped if isinstance(dropped, int) else None
        events.append(
            {
                "event": "overflow",
                "stderr": f"溢出 {dropped}\n",
                "boot_id": boot,
                "session": session,
                "frame_seq": seq,
                "dropped": known,
                "truncated": True,
            }
        )
    raw = b""
    frag = payload.get("frag_b64") or ""
    if frag:
        raw = base64.b64decode(frag)
    if raw:
        events.append(
            {
                "event": "data",
                "raw": raw,
                "boot_id": boot,
                "session": session,
                "frame_seq": seq,
                "dropped": None,
                "truncated": bool(payload.get("truncated")),
            }
        )
    cursor["expect"] = seq + 1
    return events
