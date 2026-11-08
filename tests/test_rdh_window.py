"""Offscreen Qt station. Screenshots are labeled simulated captures."""

from __future__ import annotations

import os
import time
from datetime import timedelta
from pathlib import Path

import pytest

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PyQt5.QtCore import Qt
from PyQt5.QtTest import QTest
from PyQt5.QtWidgets import QApplication, QLabel, QPushButton

from rdh.adapters.clock import FakeClock
from rdh.adapters.fake_device import FakeDevice
from rdh.app.control import Control
from rdh.domain.model import LOCAL_PRINCIPAL, RequestOperation
from rdh.ui.window import StationWindow
from tests.test_rdh_control import REMOTE


@pytest.fixture(scope="module")
def qapp():
    return QApplication.instance() or QApplication([])


@pytest.fixture
def station(qapp):
    clock = FakeClock()
    device = FakeDevice()
    control = Control(clock=clock, audit=_Audit(), device=device, simulated=True, host_id="shot-host")
    window = StationWindow(control)
    window.show()
    qapp.processEvents()
    yield window, control, device, clock, qapp
    window.close()
    qapp.processEvents()


class _Audit:
    def __init__(self):
        self.records = []

    def append_and_confirm(self, record):
        self.records.append(record)
        return True


def _pump(app, window, predicate, timeout=2.0):
    end = time.time() + timeout
    while time.time() < end:
        app.processEvents()
        window._tick()
        if predicate():
            return
        time.sleep(0.01)
    raise AssertionError(window.control.local_snapshot())


def _save(window, name: str):
    image = window.grab()
    directory = os.environ.get("RDH_SHOT_DIR")
    if directory:
        path = Path(directory)
        path.mkdir(parents=True, exist_ok=True)
        target = path / name
        assert image.save(str(target), "PNG")
        assert target.stat().st_size > 8000
    return image


def _inside(window, widget):
    if widget is None or not widget.isVisible():
        return
    top = widget.mapTo(window, widget.rect().topLeft())
    bottom = widget.mapTo(window, widget.rect().bottomRight())
    assert top.x() >= -2 and top.y() >= -2
    assert bottom.x() <= window.width() + 2
    assert bottom.y() <= window.height() + 2


def test_station_requires_an_explicit_port_and_keeps_one_owner(station):
    window, control, device, clock, app = station
    assert window.port.currentData() is None
    assert window.baud.currentData() is None
    assert window.connect_button.isEnabled() is False
    assert device.opens == 0
    assert "模拟" in window.banner.text()
    assert "未实现" in window.adb.text()
    assert "未实现" in window.net.text()
    window.port.setCurrentIndex(1)
    window.baud.setCurrentIndex(window.baud.findData(115200))
    app.processEvents()
    assert window.connect_button.isEnabled() is True
    QTest.mouseClick(window.connect_button, Qt.LeftButton)
    _pump(app, window, lambda: control.local_snapshot().target_connected)
    assert device.opens == 1
    assert window.card.isVisible()
    assert window.selectors.isVisible() is False
    assert window.disconnect_button.isVisible()
    assert "COM7" in window.card_name.text()
    device.inject_rx("示例回显 模拟数据，不是现场板".encode())
    _pump(app, window, lambda: window._captured >= 1)
    window._toggle_pause()
    device.inject_rx("第二行仍然采集".encode())
    _pump(app, window, lambda: window._captured >= 2)
    assert window._follow is False
    assert "已暂停滚动" in window.capture.text()
    assert "仍在记录" in window.capture.text()
    expires = (clock.wall() + timedelta(seconds=60)).strftime("%Y-%m-%dT%H:%M:%SZ")
    pending = control.submit(
        RequestOperation("ui-req", "ui-op", "uart.write", control.local_snapshot().target_session, b"ABC", expires),
        REMOTE,
    )
    assert pending.phase == "awaiting_approval"
    window._toggle_drawer()
    _pump(app, window, lambda: window.findChild(QPushButton, "approveButton") is not None)
    text = window.findChild(QLabel, "pendingText").text()
    assert "414243" in text
    assert "success_scope=transport" in text
    assert "设备执行成功" not in text
    assert "下一条任意写" not in text
    QTest.mouseClick(window.findChild(QPushButton, "approveButton"), Qt.LeftButton)
    _pump(app, window, lambda: any(item.operation_id == "ui-op" for item in control.local_snapshot().recent))
    assert device.write_calls == [b"ABC"]
    assert control.local_snapshot().recent[-1].device_outcome == "unknown"


def test_screenshots_at_both_workstation_sizes(station):
    window, control, device, clock, app = station
    for width, height in ((1440, 900), (1280, 800)):
        window.resize(width, height)
        app.processEvents()
        image = _save(window, f"idle-{width}x{height}.png")
        assert image.width() == width and image.height() == height
        _inside(window, window.findChild(type(window.log.parent()), "dock") or window.centralWidget())
    window.resize(1440, 900)
    window.port.setCurrentIndex(1)
    window.baud.setCurrentIndex(window.baud.findData(115200))
    QTest.mouseClick(window.connect_button, Qt.LeftButton)
    _pump(app, window, lambda: control.local_snapshot().target_connected)
    for index, line in enumerate(("示例板启动", "型号  RK-DEMO（模拟）", "调试输出  ABC 这是示例回显，不是现场实机")):
        device.inject_rx(line.encode())
    _pump(app, window, lambda: window._captured >= 3)
    window._toggle_drawer()
    expires = (clock.wall() + timedelta(seconds=60)).strftime("%Y-%m-%dT%H:%M:%SZ")
    control.submit(
        RequestOperation("shot-req", "shot-op", "uart.write", control.local_snapshot().target_session, b"ABC", expires),
        REMOTE,
    )
    _pump(app, window, lambda: window.findChild(QPushButton, "approveButton") is not None)
    for width, height in ((1440, 900), (1280, 800)):
        window.resize(width, height)
        app.processEvents()
        window._tick()
        image = _save(window, f"assist-{width}x{height}.png")
        assert image.width() == width and image.height() == height
        for widget in (window.log, window.drawer, window.card):
            _inside(window, widget)
