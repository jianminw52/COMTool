"""Append-only audit journal.

Durability is flush to the OS, not fsync. A failed confirm refuses the
write; this file is not a power-fail guarantee and it never stores secrets.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

_SECRET_PARTS = ("password", "secret", "token", "private_key", "passwd", "authorization")


class FileJournal:
    def __init__(self, path: str | os.PathLike):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = self.path.open("a", encoding="utf-8")

    def append_and_confirm(self, record: dict) -> bool:
        if _has_secret(record):
            return False
        try:
            self._fh.write(json.dumps(record, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n")
            self._fh.flush()
        except OSError:
            return False
        return True

    def close(self) -> None:
        try:
            self._fh.close()
        except OSError:
            pass


def _has_secret(value) -> bool:
    if isinstance(value, dict):
        for key, item in value.items():
            if any(part in str(key).lower() for part in _SECRET_PARTS):
                return True
            if _has_secret(item):
                return True
    elif isinstance(value, (list, tuple)):
        return any(_has_secret(item) for item in value)
    elif isinstance(value, str) and "BEGIN PRIVATE KEY" in value:
        return True
    return False
