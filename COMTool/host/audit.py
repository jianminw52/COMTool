"""Append-only operation records. MQTT passwords are never written."""

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone


class AuditLog:
    def __init__(self, path: str):
        self.path = path
        parent = os.path.dirname(path)
        if parent:
            os.makedirs(parent, exist_ok=True)

    def write(self, kind: str, **fields) -> dict:
        clean = {}
        for key, value in fields.items():
            if "password" in key.lower() or "secret" in key.lower() or key.lower() == "token":
                continue
            clean[key] = value
        record = {
            "ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "mono_ms": int(time.monotonic() * 1000),
            "kind": kind,
        }
        record.update(clean)
        line = json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n"
        with open(self.path, "a", encoding="utf-8") as handle:
            handle.write(line)
            handle.flush()
        return record
