"""On-site window. Layout follows the run4 workstation, built with PyQt5."""

from __future__ import annotations

import html

from PyQt5.QtCore import Qt, QTimer, pyqtSignal
from PyQt5.QtGui import QFont, QTextCursor
from PyQt5.QtWidgets import (
    QCheckBox,
    QComboBox,
    QDialog,
    QFormLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMessageBox,
    QPlainTextEdit,
    QPushButton,
    QSpinBox,
    QVBoxLayout,
    QWidget,
)

from COMTool.host.config import load, save
from COMTool.host.serial_owner import PortIdentity, list_ports
from COMTool.host.session import payload_sha256


STYLE = """
QMainWindow, QWidget { background: #1b1f24; color: #e8edf2; font-size: 14px; }
QLabel { color: #e8edf2; font-size: 14px; }
QComboBox, QLineEdit, QPlainTextEdit, QSpinBox {
  background: #1a1f25; color: #e8edf2; border: 1px solid #4c5866;
  border-radius: 2px; padding: 6px; font-size: 14px; min-height: 28px;
}
QPushButton {
  background: #2c333c; color: #e8edf2; border: 1px solid #4c5866;
  border-radius: 2px; padding: 8px 12px; font-size: 15px;
}
QPushButton#connectButton { background: #1d6f9a; color: #f5fbff; border: 1px solid #2c86b4; min-height: 40px; font-size: 16px; }
QPushButton#connectButton:disabled { background: #2e353d; color: #c5ced6; border: 1px solid #4a545f; }
QPushButton#disconnectButton { background: #2a2426; color: #e7b4b0; border: 1px solid #6a4442; min-height: 40px; font-size: 16px; }
QPushButton#approveButton { background: #1d6f9a; color: #f5fbff; }
QPlainTextEdit#logView { font-family: "WenQuanYi Micro Hei Mono", monospace; font-size: 15px; background: #14181c; }
#dock { background: #222830; border-right: 1px solid #3a434e; }
#testBanner { background: #3a3224; color: #f0d3a0; border: 1px solid #6d5a32; padding: 6px 10px; }
"""


class HostWindow(QMainWindow):
    bytes_arrived = pyqtSignal(bytes)

    def __init__(self, runtime):
        super().__init__()
        self.runtime = runtime
        self.follow = True
        self._selected_identity: PortIdentity | None = None
        self._approval = None
        self.setWindowTitle("现场调试台")
        self.resize(1280, 800)
        self.setStyleSheet(STYLE)
        self._build()
        self.bytes_arrived.connect(self._append_log)
        self.runtime.gui_listeners.append(self.bytes_arrived.emit)
        self._timer = QTimer(self)
        self._timer.timeout.connect(self._poll)
        self._timer.start(200)
        self.refresh_ports()
        self._load_settings_into_status()

    def _build(self) -> None:
        root = QWidget()
        self.setCentralWidget(root)
        outer = QVBoxLayout(root)
        outer.setContentsMargins(0, 0, 0, 0)
        outer.setSpacing(0)
        outer.addWidget(self._header())
        self.test_banner = QLabel("")
        self.test_banner.setObjectName("testBanner")
        self.test_banner.setVisible(False)
        outer.addWidget(self.test_banner)
        body = QHBoxLayout()
        body.setContentsMargins(0, 0, 0, 0)
        body.setSpacing(0)
        body.addWidget(self._dock(), 0)
        body.addWidget(self._terminal(), 1)
        self.drawer = self._drawer()
        self.drawer.setVisible(False)
        body.addWidget(self.drawer, 0)
        wrap = QWidget()
        wrap.setLayout(body)
        outer.addWidget(wrap, 1)

    def _header(self) -> QWidget:
        bar = QWidget()
        row = QHBoxLayout(bar)
        title = QLabel("现场调试台")
        title.setStyleSheet("font-size: 16px; font-weight: 600;")
        row.addWidget(title)
        row.addWidget(QLabel("  "))
        row.addStretch(1)
        self.target_state = QLabel("目标未连接")
        self.relay_state = QLabel("中继未连接")
        self.auth_state = QLabel("远程未授权")
        for label in (self.target_state, self.relay_state, self.auth_state):
            row.addWidget(label)
        return bar

    def _dock(self) -> QWidget:
        dock = QWidget()
        dock.setObjectName("dock")
        dock.setFixedWidth(300)
        layout = QVBoxLayout(dock)
        layout.addWidget(QLabel("唯一目标"))
        heading = QLabel("当前设备")
        heading.setFont(QFont(heading.font().family(), 14))
        layout.addWidget(heading)
        self.card = QLabel("尚未选择串口")
        self.card.setWordWrap(True)
        self.card.setVisible(False)
        layout.addWidget(self.card)
        self.port_box = QComboBox()
        self.port_box.setObjectName("portBox")
        self.baud_box = QComboBox()
        self.baud_box.setObjectName("baudBox")
        self.baud_box.addItem("选择波特率")
        for baud in (9600, 19200, 38400, 57600, 115200, 230400, 460800, 921600):
            self.baud_box.addItem(str(baud))
        self.baud_box.setCurrentIndex(0)
        self.form = QWidget()
        form = QVBoxLayout(self.form)
        form.setContentsMargins(0, 0, 0, 0)
        form.addWidget(QLabel("串口"))
        form.addWidget(self.port_box)
        form.addWidget(QLabel("波特率"))
        form.addWidget(self.baud_box)
        self.refresh_button = QPushButton("刷新列表")
        self.refresh_button.clicked.connect(self.refresh_ports)
        form.addWidget(self.refresh_button)
        layout.addWidget(self.form)
        self.dtr_box = QCheckBox("DTR")
        self.rts_box = QCheckBox("RTS")
        self.dtr_box.setChecked(False)
        self.rts_box.setChecked(False)
        layout.addWidget(QLabel("电平，默认关闭"))
        layout.addWidget(self.dtr_box)
        layout.addWidget(self.rts_box)
        note = QLabel("默认关闭。部分驱动在打开端口时仍可能产生瞬态，不能保证目标板绝不复位。")
        note.setWordWrap(True)
        layout.addWidget(note)
        self.assist_button = QPushButton("打开远程协助")
        self.assist_button.setEnabled(False)
        self.assist_button.clicked.connect(self._toggle_drawer)
        layout.addWidget(self.assist_button)
        layout.addWidget(QLabel("ADB  未实现"))
        layout.addWidget(QLabel("网络  未实现"))
        layout.addWidget(QLabel("刷写 / 百度上传  未实现"))
        layout.addStretch(1)
        self.connect_button = QPushButton("连接")
        self.connect_button.setObjectName("connectButton")
        self.connect_button.setEnabled(False)
        self.connect_button.clicked.connect(self._toggle_connection)
        layout.addWidget(self.connect_button)
        self.hint = QLabel("先选择串口")
        self.hint.setWordWrap(True)
        layout.addWidget(self.hint)
        self.settings_button = QPushButton("连接设置")
        self.settings_button.clicked.connect(self._edit_settings)
        layout.addWidget(self.settings_button)
        self.audit_button = QPushButton("操作记录")
        self.audit_button.clicked.connect(self._show_audit)
        layout.addWidget(self.audit_button)
        self.port_box.currentIndexChanged.connect(self._form_changed)
        self.baud_box.currentIndexChanged.connect(self._form_changed)
        return dock

    def _terminal(self) -> QWidget:
        panel = QWidget()
        layout = QVBoxLayout(panel)
        layout.setContentsMargins(8, 8, 8, 8)
        row = QHBoxLayout()
        row.addWidget(QLabel("日志"))
        row.addStretch(1)
        self.record_label = QLabel("尚未记录")
        row.addWidget(self.record_label)
        self.pause_button = QPushButton("暂停滚动")
        self.pause_button.setObjectName("pauseButton")
        self.pause_button.setEnabled(False)
        self.pause_button.clicked.connect(self._toggle_follow)
        row.addWidget(self.pause_button)
        layout.addLayout(row)
        self.guide = QLabel("尚未接入\n在左侧选择串口和波特率，再点连接。不会自动扫描，也不会试波特率。")
        self.guide.setWordWrap(True)
        layout.addWidget(self.guide)
        self.log = QPlainTextEdit()
        self.log.setObjectName("logView")
        self.log.setReadOnly(True)
        self.log.setLineWrapMode(QPlainTextEdit.WidgetWidth)
        self.log.setVisible(False)
        layout.addWidget(self.log, 1)
        return panel

    def _drawer(self) -> QWidget:
        drawer = QWidget()
        drawer.setFixedWidth(360)
        layout = QVBoxLayout(drawer)
        head = QHBoxLayout()
        head.addWidget(QLabel("远程协助"))
        head.addStretch(1)
        close = QPushButton("关闭")
        close.clicked.connect(self._close_drawer)
        head.addWidget(close)
        layout.addLayout(head)
        self.drawer_lead = QLabel("")
        self.drawer_lead.setWordWrap(True)
        layout.addWidget(self.drawer_lead)
        self.view_button = QPushButton("允许查看日志")
        self.view_button.clicked.connect(self._toggle_view)
        layout.addWidget(self.view_button)
        self.view_note = QLabel("查看和写命令是两件事。收回查看后，新的远程日志请求会被拒绝。已经送出的内容不会撤回。")
        self.view_note.setWordWrap(True)
        layout.addWidget(self.view_note)
        self.write_state = QLabel("写命令：关闭")
        layout.addWidget(self.write_state)
        layout.addStretch(1)
        return drawer

    def mark_test_mode(self, text: str) -> None:
        self.test_banner.setText(text)
        self.test_banner.setVisible(True)

    def refresh_ports(self) -> None:
        current = self.port_box.currentData()
        self.port_box.blockSignals(True)
        self.port_box.clear()
        self.port_box.addItem("选择串口", None)
        for port in list_ports():
            self.port_box.addItem(port.label(), port.device)
        self.port_box.setCurrentIndex(0)
        if current:
            index = self.port_box.findData(current)
            if index > 0:
                self.port_box.setCurrentIndex(index)
        self.port_box.blockSignals(False)
        self._form_changed()

    def _selected_port(self) -> str | None:
        value = self.port_box.currentData()
        return value or None

    def _selected_baud(self) -> int | None:
        text = self.baud_box.currentText()
        if text.isdigit():
            return int(text)
        return None

    def _restyle_connect(self, primary: bool) -> None:
        self.connect_button.setObjectName("connectButton" if primary else "disconnectButton")
        self.connect_button.style().unpolish(self.connect_button)
        self.connect_button.style().polish(self.connect_button)

    def _form_changed(self) -> None:
        if self.runtime.owner.is_open:
            return
        port = self._selected_port()
        baud = self._selected_baud()
        self.connect_button.setEnabled(bool(port and baud))
        if not port:
            self.hint.setText("先选择串口")
        elif not baud:
            self.hint.setText("再选择波特率")
        else:
            self.hint.setText("核对这只口和波特率，然后连接")

    def _toggle_connection(self) -> None:
        if self.runtime.owner.is_open:
            self.runtime.session.drop_pending("disconnect")
            self.runtime.owner.close()
            self._show_disconnected()
            return
        port = self._selected_port()
        baud = self._selected_baud()
        if not port or not baud:
            return
        try:
            self.runtime.owner.open(
                port,
                baud,
                dtr=self.dtr_box.isChecked(),
                rts=self.rts_box.isChecked(),
            )
        except Exception as exc:
            QMessageBox.warning(self, "没有打开", str(exc))
            return
        self._selected_identity = self.runtime.owner.identity
        self.runtime.audit.write("serial_open", port=port, baud=baud, dtr=self.dtr_box.isChecked(), rts=self.rts_box.isChecked())
        self._show_connected(port, baud)

    def reconnect_same_device(self) -> str:
        identity = self._selected_identity
        if identity is None:
            return "没有这次选择过的设备"
        if not self.runtime.owner.same_device(identity.device):
            return "这只设备现在不在列表里，没有改接别的口"
        baud = self._selected_baud()
        if not baud:
            return "没有波特率"
        self.runtime.owner.open(identity.device, baud, identity=identity, dtr=False, rts=False)
        return "已重新打开同一只口"

    def _show_connected(self, port: str, baud: int) -> None:
        self.card.setText(f"{port}\n{baud} · 8N1 · 无流控\nDTR {'开' if self.dtr_box.isChecked() else '关'} · RTS {'开' if self.rts_box.isChecked() else '关'}")
        self.card.setVisible(True)
        self.form.setVisible(False)
        self.connect_button.setText("断开连接")
        self._restyle_connect(False)
        self.connect_button.setEnabled(True)
        self.assist_button.setEnabled(True)
        self.hint.setText("已接入。日志在中间。")
        self.guide.setVisible(False)
        self.log.setVisible(True)
        self.target_state.setText("目标已连接")

    def _show_disconnected(self) -> None:
        self.runtime.session.grant_view(False)
        self.card.setVisible(False)
        self.form.setVisible(True)
        self.connect_button.setText("连接")
        self._restyle_connect(True)
        self.assist_button.setEnabled(False)
        self.drawer.setVisible(False)
        self.guide.setVisible(self.log.document().isEmpty())
        self.target_state.setText("目标未连接")
        self._form_changed()
        if not self.log.document().isEmpty():
            self.record_label.setText("已停止记录新数据")

    def _append_log(self, data: bytes) -> None:
        text = data.decode("utf-8", errors="replace")
        self.log.setVisible(True)
        self.guide.setVisible(False)
        self.pause_button.setEnabled(True)
        cursor = self.log.textCursor()
        cursor.movePosition(QTextCursor.End)
        cursor.insertText(text)
        if self.follow:
            self.log.setTextCursor(cursor)
            self.log.ensureCursorVisible()
        count = self.runtime.owner.bytes_read
        if self.follow:
            self.record_label.setText(f"正在记录 · {count} 字节")
        else:
            self.record_label.setText(f"正在记录 · 滚动已暂停 · {count} 字节")

    def _toggle_follow(self) -> None:
        self.follow = not self.follow
        self.pause_button.setText("暂停滚动" if self.follow else "恢复滚动")
        if self.runtime.owner.is_open:
            count = self.runtime.owner.bytes_read
            self.record_label.setText("正在记录 · {} 字节".format(count) if self.follow else f"正在记录 · 滚动已暂停 · {count} 字节")

    def _toggle_drawer(self) -> None:
        if not self.runtime.owner.is_open:
            return
        self.drawer.setVisible(not self.drawer.isVisible())
        self.assist_button.setText("协助已打开" if self.drawer.isVisible() else "打开远程协助")
        self._refresh_drawer()

    def _close_drawer(self) -> None:
        self.drawer.setVisible(False)
        self.assist_button.setText("打开远程协助")

    def _toggle_view(self) -> None:
        allowed = not self.runtime.session.view_granted
        self.runtime.session.grant_view(allowed)
        self._refresh_drawer()

    def _refresh_drawer(self) -> None:
        allowed = self.runtime.session.view_granted
        linked = self.runtime.mqtt.connected
        self.view_button.setText("收回查看" if allowed else "允许查看日志")
        self.write_state.setText("写命令：关闭")
        self.relay_state.setText("中继已连接" if linked else "中继未连接")
        self.auth_state.setText("可查看 · 写命令关" if allowed else "远程未授权")
        if not linked:
            self.drawer_lead.setText("broker 未连接。填写连接设置后才能远程查看。")
        elif not allowed:
            self.drawer_lead.setText("远程查看还没打开。写命令保持关闭。")
        else:
            self.drawer_lead.setText("对方可以看日志。每一条写命令都要在这里单独批准。")

    def _poll(self) -> None:
        self.runtime.session.expire_due()
        pending = self.runtime.session.pending
        if pending and (self._approval is None or not self._approval.isVisible()):
            self._approval = ApprovalDialog(self, pending)
            choice = self._approval.exec_()
            current = self.runtime.session.pending
            if current is None or current["request_id"] != pending["request_id"]:
                return
            if choice == QDialog.Accepted:
                result = self.runtime.session.approve(pending["request_id"], pending["sha256"])
                if result != "wrote":
                    QMessageBox.information(self, "没有写入", f"这条请求没有写到串口（{result}）。")
            else:
                self.runtime.session.deny(pending["request_id"], pending["sha256"])
        if self.runtime.owner.is_open:
            self.target_state.setText("目标已连接")
        self._refresh_drawer()

    def _load_settings_into_status(self) -> None:
        self.runtime.start_mqtt()

    def _edit_settings(self) -> None:
        dialog = SettingsDialog(self, self.runtime.settings)
        if dialog.exec_() != QDialog.Accepted:
            return
        data = dialog.values()
        save(data)
        self.runtime.apply_settings(data)

    def _show_audit(self) -> None:
        path = self.runtime.audit.path
        text = ""
        try:
            with open(path, encoding="utf-8") as handle:
                text = handle.read()[-4000:]
        except FileNotFoundError:
            text = "还没有操作记录。"
        box = QMessageBox(self)
        box.setWindowTitle("操作记录")
        box.setText("下面是最近的操作记录。已提交只表示端口收下了字节，不是目标执行成功。")
        box.setDetailedText(text)
        box.exec_()

    def closeEvent(self, event) -> None:
        self.runtime.shutdown()
        event.accept()


class ApprovalDialog(QDialog):
    def __init__(self, parent, pending: dict):
        super().__init__(parent)
        self.setWindowTitle("批准这一条写命令")
        layout = QVBoxLayout(self)
        layout.addWidget(QLabel(f"对象：{pending['port']}"))
        layout.addWidget(QLabel(f"请求：{pending['request_id']}"))
        shown = html.escape(pending["preview"])
        view = QPlainTextEdit(shown)
        view.setReadOnly(True)
        view.setFont(QFont("WenQuanYi Micro Hei Mono", 12))
        layout.addWidget(view)
        layout.addWidget(QLabel("这些字节会原样写到上面的串口。目标可能回复，也可能改变状态。只批准这一条，不会开放下一条任意写。"))
        layout.addWidget(QLabel(f"校验 {pending['sha256'][:16]}"))
        row = QHBoxLayout()
        deny = QPushButton("拒绝")
        allow = QPushButton("批准这一条")
        allow.setObjectName("approveButton")
        deny.clicked.connect(self.reject)
        allow.clicked.connect(self.accept)
        row.addWidget(deny)
        row.addWidget(allow)
        layout.addLayout(row)
        assert payload_sha256(pending["raw"]) == pending["sha256"]


class SettingsDialog(QDialog):
    def __init__(self, parent, settings: dict):
        super().__init__(parent)
        self.setWindowTitle("连接设置")
        form = QFormLayout(self)
        self.device = QLineEdit(settings.get("device_id") or "")
        self.host = QLineEdit(settings.get("broker_host") or "")
        self.port = QSpinBox()
        self.port.setRange(1, 65535)
        self.port.setValue(int(settings.get("broker_port") or 8883))
        self.tls = QCheckBox("使用 TLS")
        self.tls.setChecked(bool(settings.get("broker_tls", True)))
        self.user = QLineEdit(settings.get("broker_username") or "")
        self.password = QLineEdit(settings.get("broker_password") or "")
        self.password.setEchoMode(QLineEdit.Password)
        self.approval = QSpinBox()
        self.approval.setRange(1, 30)
        self.approval.setValue(int(settings.get("approval_seconds") or 7))
        form.addRow("device id", self.device)
        form.addRow("broker 主机", self.host)
        form.addRow("端口", self.port)
        form.addRow("", self.tls)
        form.addRow("用户名", self.user)
        form.addRow("密码", self.password)
        form.addRow("批准等待秒", self.approval)
        note = QLabel("留空 broker 或 device id 时保持离线。密码只保存在本机配置，不写入操作记录。")
        note.setWordWrap(True)
        form.addRow(note)
        row = QHBoxLayout()
        cancel = QPushButton("取消")
        ok = QPushButton("保存")
        cancel.clicked.connect(self.reject)
        ok.clicked.connect(self.accept)
        row.addWidget(cancel)
        row.addWidget(ok)
        form.addRow(row)

    def values(self) -> dict:
        current = load()
        current.update(
            {
                "device_id": self.device.text().strip(),
                "broker_host": self.host.text().strip(),
                "broker_port": int(self.port.value()),
                "broker_tls": self.tls.isChecked(),
                "broker_username": self.user.text(),
                "broker_password": self.password.text(),
                "approval_seconds": float(self.approval.value()),
            }
        )
        return current
