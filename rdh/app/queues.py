"""Bounded queues. A full put fails immediately and counts a drop."""

from __future__ import annotations

import threading
from collections import deque


class BoundedQueue:
    def __init__(self, max_items: int, max_bytes: int | None = None):
        if max_items < 1:
            raise ValueError("max_items")
        self.max_items = max_items
        self.max_bytes = max_bytes
        self._items: deque = deque()
        self._bytes = 0
        self.dropped = 0
        self._lock = threading.Lock()

    def __len__(self) -> int:
        with self._lock:
            return len(self._items)

    def try_put(self, item, nbytes: int = 0) -> bool:
        with self._lock:
            if len(self._items) >= self.max_items:
                self.dropped += 1
                return False
            if self.max_bytes is not None and self._bytes + max(0, nbytes) > self.max_bytes:
                self.dropped += 1
                return False
            self._items.append((item, max(0, nbytes)))
            self._bytes += max(0, nbytes)
            return True

    def get_all(self, limit: int | None = None) -> list:
        with self._lock:
            out = []
            while self._items and (limit is None or len(out) < limit):
                item, nbytes = self._items.popleft()
                self._bytes -= nbytes
                out.append(item)
            return out


class Subscriber:
    """One consumer. Overflow increments this consumer's gap only."""

    def __init__(self, name: str, max_items: int, max_bytes: int | None = None):
        self.name = name
        self.queue = BoundedQueue(max_items, max_bytes)
        self.gap = 0
        self._lock = threading.Lock()

    def offer(self, item, nbytes: int = 0) -> bool:
        ok = self.queue.try_put(item, nbytes)
        if not ok:
            with self._lock:
                self.gap += 1
        return ok

    def poll(self) -> tuple[list, int]:
        items = self.queue.get_all()
        with self._lock:
            gap = self.gap
        return items, gap
