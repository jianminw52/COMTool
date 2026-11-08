"""Render the real Host window against a PTY. Not a field device."""

import os
import sys

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)

import pty
import time

from PyQt5.QtWidgets import QApplication

from tests.loopback_broker import LoopbackBroker
from COMTool.host.runtime import Runtime
from COMTool.host.serial_owner import PortIdentity
from COMTool.host.window import ApprovalDialog, HostWindow

OUT = "/opt/cursor/artifacts/host-eval/qt-host"
os.makedirs(OUT, exist_ok=True)
BANNER = "测试模式 · 模拟串口，不是现场板卡"


def grab(window, name):
    app.processEvents()
    window.repaint()
    app.processEvents()
    pix = window.grab()
    path = os.path.join(OUT, name)
    pix.save(path)
    print(path, pix.width(), pix.height())


app = QApplication([])
runtime = Runtime({"device_id": "host-sim", "broker_host": "", "broker_tls": False, "approval_seconds": 7})
window = HostWindow(runtime)
window.mark_test_mode(BANNER)
window.resize(1440, 900)
window.show()
grab(window, "01-idle.png")

master, slave = pty.openpty()
name = os.ttyname(slave)
window.port_box.addItem(name, name)
window.port_box.setCurrentIndex(window.port_box.findData(name))
window.baud_box.setCurrentText("115200")
app.processEvents()
window._toggle_connection()
runtime.owner.write(b"")  # touch
sample = (
    "0.00  示例板启动\n"
    "0.45  调试口  模拟口\n"
    "1.02  格式  8N1\n"
    "2.10  温度  26.4\n"
    "4.00  心跳\n"
    "8.00  存储剩余  充足\n"
    "12.2  等待指令\n"
)
os.write(master, sample.encode())
deadline = time.time() + 2
while runtime.owner.bytes_read < len(sample.encode()) and time.time() < deadline:
    app.processEvents()
    time.sleep(0.02)
app.processEvents()
grab(window, "02-log.png")

broker = LoopbackBroker()
broker.start()
runtime.settings.update({"device_id": "host-sim", "broker_host": "127.0.0.1", "broker_port": broker.port, "broker_tls": False})
runtime.apply_settings(runtime.settings)
deadline = time.time() + 3
while not runtime.mqtt.connected and time.time() < deadline:
    app.processEvents()
    time.sleep(0.05)
window._toggle_drawer()
runtime.session.grant_view(True)
window._refresh_drawer()
app.processEvents()
grab(window, "03-assist.png")

pending = {
    "request_id": "req-demo",
    "sha256": "abc123",
    "raw": b"ping\n",
    "preview": "ping\n",
    "port": name,
}
# dialog checks hash; use the real hash
from COMTool.host.session import payload_sha256

pending["sha256"] = payload_sha256(pending["raw"])
dialog = ApprovalDialog(window, pending)
dialog.show()
app.processEvents()
pix = dialog.grab()
path = os.path.join(OUT, "04-approve.png")
pix.save(path)
print(path, pix.width(), pix.height())
dialog.close()
window.close()
broker.stop()
os.close(master)
os.close(slave)
