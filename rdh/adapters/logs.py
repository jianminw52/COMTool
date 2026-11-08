"""Bounded log-file consumer. A full queue drops this consumer only."""

from __future__ import annotations

import queue
import threading
from pathlib import Path


class LogFileSink:
    def __init__(self, path, max_items: int = 256):
        self.name = "logfile"
        self.gap = 0
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = self.path.open("a", encoding="utf-8")
        self._q: queue.Queue = queue.Queue(maxsize=max_items)
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._hold = threading.Event()
        self._thread = threading.Thread(target=self._loop, name="rdh-logfile", daemon=True)
        self._thread.start()

    def hold(self) -> None:
        self._hold.set()

    def release(self) -> None:
        self._hold.clear()

    def offer(self, record, nbytes: int = 0) -> bool:
        del nbytes
        try:
            self._q.put_nowait(record)
            return True
        except queue.Full:
            with self._lock:
                self.gap += 1
            return False

    def _loop(self) -> None:
        while not self._stop.is_set():
            if self._hold.is_set():
                if self._stop.wait(0.01):
                    break
                continue
            try:
                record = self._q.get(timeout=0.05)
            except queue.Empty:
                continue
            if self._hold.is_set():
                try:
                    self._q.put_nowait(record)
                except queue.Full:
                    with self._lock:
                        self.gap += 1
                continue
            self._write(record)

    def _write(self, record) -> None:
        text = record.data.decode("utf-8", errors="replace").rstrip("\n")
        try:
            self._fh.write(f"{record.seq} {text}\n")
            self._fh.flush()
        except OSError:
            with self._lock:
                self.gap += 1

    def shutdown(self, timeout: float = 1.0) -> bool:
        self.release()
        self._stop.set()
        self._thread.join(timeout)
        while True:
            try:
                record = self._q.get_nowait()
            except queue.Empty:
                break
            self._write(record)
        if self.gap:
            try:
                self._fh.write(f"# gap {self.gap}\n")
                self._fh.flush()
            except OSError:
                pass
        try:
            self._fh.close()
        except OSError:
            pass
        return not self._thread.is_alive()
