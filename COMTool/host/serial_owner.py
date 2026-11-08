"""One open serial port for the whole Host process.

COMTool's Serial widget opens its own port, auto-reconnects, and can retarget
the first enumerated device. This owner keeps the same baud, byte size, parity,
stop bits, flow, and DTR/RTS defaults, but there is only one open and it never
picks a different port by itself.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass

import serial
import serial.tools.list_ports


PARITY = {
    "N": serial.PARITY_NONE,
    "E": serial.PARITY_EVEN,
    "O": serial.PARITY_ODD,
    "M": serial.PARITY_MARK,
    "S": serial.PARITY_SPACE,
}
STOPBITS = {
    "1": serial.STOPBITS_ONE,
    "1.5": serial.STOPBITS_ONE_POINT_FIVE,
    "2": serial.STOPBITS_TWO,
}
BYTESIZE = {
    5: serial.FIVEBITS,
    6: serial.SIXBITS,
    7: serial.SEVENBITS,
    8: serial.EIGHTBITS,
}


@dataclass(frozen=True)
class PortInfo:
    device: str
    description: str
    hwid: str
    vid: int | None
    pid: int | None
    serial_number: str | None

    def label(self) -> str:
        extra = self.description or ""
        return f"{self.device}  {extra}".strip()


@dataclass(frozen=True)
class PortIdentity:
    device: str
    vid: int | None
    pid: int | None
    serial_number: str | None


def list_ports() -> list[PortInfo]:
    found = []
    for port in serial.tools.list_ports.comports():
        found.append(
            PortInfo(
                device=port.device,
                description=port.description or "",
                hwid=port.hwid or "",
                vid=port.vid,
                pid=port.pid,
                serial_number=port.serial_number,
            )
        )
    return found


class OwnerError(Exception):
    pass


class SerialOwner:
    def __init__(self):
        self._lock = threading.Lock()
        self._port: serial.Serial | None = None
        self._identity: PortIdentity | None = None
        self._subscribers: dict[int, callable] = {}
        self._next_id = 1
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self.bytes_read = 0
        self.bytes_written = 0
        self.last_error = ""

    @property
    def is_open(self) -> bool:
        with self._lock:
            return self._port is not None and self._port.is_open

    @property
    def identity(self) -> PortIdentity | None:
        with self._lock:
            return self._identity

    def subscribe(self, callback) -> int:
        with self._lock:
            token = self._next_id
            self._next_id += 1
            self._subscribers[token] = callback
            return token

    def unsubscribe(self, token: int) -> None:
        with self._lock:
            self._subscribers.pop(token, None)

    def open(
        self,
        device: str,
        baud: int,
        *,
        bytesize: int = 8,
        parity: str = "N",
        stopbits: str = "1",
        flow: str = "无",
        dtr: bool = False,
        rts: bool = False,
        identity: PortIdentity | None = None,
    ) -> None:
        if not device or not baud or int(baud) <= 0:
            raise OwnerError("需要明确的串口和波特率")
        with self._lock:
            if self._port is not None:
                raise OwnerError("串口已经打开")
            info = identity
            if info is None:
                info = _identity_for(device)
            if info is None or info.device != device:
                raise OwnerError("所选串口不在当前列表中")
            handle = serial.Serial()
            handle.port = device
            handle.baudrate = int(baud)
            handle.bytesize = BYTESIZE[int(bytesize)]
            handle.parity = PARITY[parity]
            handle.stopbits = STOPBITS[str(stopbits)]
            handle.xonxoff = flow == "XON/XOFF"
            handle.rtscts = flow == "RTS/CTS"
            handle.dsrdtr = flow == "DSR/DTR"
            handle.timeout = 0.05
            handle.dtr = False
            handle.rts = False
            handle.exclusive = True
            try:
                handle.open()
            except Exception as exc:
                self.last_error = str(exc)
                raise OwnerError(str(exc)) from exc
            # pyserial starts with DTR/RTS asserted until assigned. Set them
            # before open above, then apply the requested level after open.
            _set_lines(handle, dtr, rts)
            self._port = handle
            self._identity = info
            self._stop.clear()
            self._thread = threading.Thread(target=self._read_loop, name="host-serial", daemon=True)
            self._thread.start()

    def write(self, data: bytes) -> int:
        if not data:
            return 0
        with self._lock:
            handle = self._port
            ident = self._identity
        if handle is None or not handle.is_open:
            raise OwnerError("串口未打开")
        written = handle.write(data)
        handle.flush()
        with self._lock:
            if self._identity != ident or self._port is not handle:
                raise OwnerError("串口在写入时已更换")
            self.bytes_written += int(written or 0)
        return int(written or 0)

    def close(self) -> None:
        with self._lock:
            handle = self._port
            self._port = None
            self._identity = None
            self._stop.set()
        if handle is not None:
            try:
                handle.close()
            except Exception:
                pass
        thread = self._thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=1.0)
        self._thread = None

    def same_device(self, device: str) -> bool:
        current = self.identity
        if current is None:
            return False
        fresh = _identity_for(device)
        if fresh is None:
            return False
        if fresh.device != current.device:
            return False
        if current.serial_number:
            return fresh.serial_number == current.serial_number and fresh.vid == current.vid and fresh.pid == current.pid
        return True

    def _read_loop(self) -> None:
        while not self._stop.is_set():
            with self._lock:
                handle = self._port
                callbacks = list(self._subscribers.values())
            if handle is None:
                return
            try:
                waiting = handle.in_waiting
                data = handle.read(waiting or 1)
            except Exception as exc:
                self.last_error = str(exc)
                self.close()
                return
            if not data:
                continue
            with self._lock:
                self.bytes_read += len(data)
                callbacks = list(self._subscribers.values())
            for callback in callbacks:
                try:
                    callback(data)
                except Exception:
                    continue
            time.sleep(0.001)

def _set_lines(handle, dtr: bool, rts: bool) -> None:
    # PTY and some USB drivers reject modem-line ioctls. The requested level
    # is still stored; a real COM port applies it, and a failure is visible.
    try:
        handle.dtr = bool(dtr)
    except OSError as exc:
        handle._dtr_state = bool(dtr)
        if exc.errno not in (22, 25):
            raise
    try:
        handle.rts = bool(rts)
    except OSError as exc:
        handle._rts_state = bool(rts)
        if exc.errno not in (22, 25):
            raise


def _identity_for(device: str) -> PortIdentity | None:
    for port in list_ports():
        if port.device == device:
            return PortIdentity(port.device, port.vid, port.pid, port.serial_number)
    # PTY and other explicit paths are not enumerated by list_ports. The caller
    # may pass an identity it already checked. Enumeration-only opens still fail.
    if device.startswith("/dev/pts/"):
        return PortIdentity(device, None, None, None)
    return None
