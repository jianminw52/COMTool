"""Industrial on-site window: device dock, log, and an on-demand assist drawer.

The main thread only renders immutable snapshots and log records. Pause
scroll does not pause capture. Approval always names the exact bytes.
"""

from __future__ import annotations

import os

from PyQt5.QtCore import Qt
from PyQt5.QtGui import QFont
from PyQt5.QtWidgets import (
    QCheckBox,
    QComboBox,
    QFileDialog,
    QFrame,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMessageBox,
    QPlainTextEdit,
    QPushButton,
    QScrollArea,
    QVBoxLayout,
    QWidget,
)

from rdh.domain.model import LOCAL_PRINCIPAL, Connect, Decide, Disconnect, Discover, ExportDiagnostic, GrantView, Revoke

_BAUD_RATES = (9600, 19200, 38400, 57600, 115200, 230400, 460800, 921600)
_STYLE = """
QWidget { background: #14181e; color: #e6edf3; font-family: "WenQuanYi Micro Hei", "Droid Sans Fallback", sans-serif; font-size: 13px; }
QFrame#dock, QFrame#drawer, QFrame#card, QFrame#guide { background: #1c222b; border: 1px solid #3c4a59; border-radius: 8px; }
QFrame#topbar { background: #171c22; border-bottom: 1px solid #31404c; }
QLabel#title { font-size: 16px; font-weight: 600; }
QLabel#muted, QLabel#hint { color: #93a1b0; }
QLabel#banner { color: #e0b15a; background: #2a2418; border: 1px solid #6d5a2e; border-radius: 8px; padding: 2px 8px; }
QLabel#section { color: #8fd9d2; font-size: 12px; }
QComboBox, QPlainTextEdit { background: #12171d; border: 1px solid #3c4a59; border-radius: 6px; padding: 4px 6px; }
QPlainTextEdit { font-family: "WenQuanYi Micro Hei Mono", "DejaVu Sans Mono", monospace; font-size: 13px; }
QPushButton { background: #24303a; border: 1px solid #3c4a59; border-radius: 6px; padding: 8px 12px; }
QPushButton:disabled { color: #6d7884; background: #1a2028; }
QPushButton#primary { background: #1e6f6a; border-color: #3cbfb6; }
QPushButton#disconnectButton { background: #5c2d2d; border-color: #a24b4b; }
QPushButton#approve { background: #1e6f6a; border-color: #3cbfb6; }
QCheckBox { spacing: 8px; }
QScrollArea { border: none; }
"""


class StationWindow(QMainWindow):
    def __init__(self, control):
        super().__init__()
        self.control = control
        self.logs = control.subscribe_logs("ui")
        self._follow = True
        self._captured = 0
        self._connected = False
        self._pending_sig = None
        self._shut = False
        self.last_shutdown = None
        self.setWindowTitle("现场调试台")
        self.resize(1440, 900)
        self.setMinimumSize(1100, 700)
        self._build()
        self.setStyleSheet(_STYLE)
        from PyQt5.QtCore import QTimer

        self._timer = QTimer(self)
        self._timer.timeout.connect(self._tick)
        self._timer.start(40)
        self._refresh_ports(keep=False)

    def _build(self) -> None:
        root = QWidget()
        self.setCentralWidget(root)
        outer = QVBoxLayout(root)
        outer.setContentsMargins(0, 0, 0, 0)
        outer.setSpacing(0)
        outer.addWidget(self._topbar())
        body = QHBoxLayout()
        body.setContentsMargins(12, 12, 12, 12)
        body.setSpacing(12)
        outer.addLayout(body, 1)
        body.addWidget(self._dock(), 0)
        body.addWidget(self._center(), 1)
        self.drawer = self._drawer()
        body.addWidget(self.drawer, 0)
        self.drawer.setVisible(False)

    def _topbar(self) -> QWidget:
        bar = QFrame()
        bar.setObjectName("topbar")
        layout = QHBoxLayout(bar)
        title = QLabel("现场调试台")
        title.setObjectName("title")
        self.banner = QLabel("模拟 · 非现场实机" if self.control.simulated else "本地串口 · 未做现场验收")
        self.banner.setObjectName("banner")
        self.target_state = QLabel("目标未连接")
        self.relay_state = QLabel("中继未连接")
        self.grant_state = QLabel("远程未授权")
        for label in (self.target_state, self.relay_state, self.grant_state):
            label.setObjectName("muted")
        layout.addWidget(title)
        layout.addWidget(self.banner)
        layout.addStretch(1)
        layout.addWidget(self.target_state)
        layout.addWidget(self.relay_state)
        layout.addWidget(self.grant_state)
        return bar

    def _dock(self) -> QWidget:
        dock = QFrame()
        dock.setObjectName("dock")
        dock.setFixedWidth(300)
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        inner = QWidget()
        layout = QVBoxLayout(inner)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(8)
        section = QLabel("唯一目标")
        section.setObjectName("section")
        layout.addWidget(section)
        self.device_title = QLabel("当前设备")
        layout.addWidget(self.device_title)
        self.selectors = QWidget()
        form = QVBoxLayout(self.selectors)
        form.setContentsMargins(0, 0, 0, 0)
        form.addWidget(QLabel("串口"))
        self.port = QComboBox()
        self.port.setObjectName("portCombo")
        self.port.currentIndexChanged.connect(self._sync_connect)
        form.addWidget(self.port)
        form.addWidget(QLabel("波特率"))
        self.baud = QComboBox()
        self.baud.setObjectName("baudCombo")
        self.baud.addItem("选择波特率", None)
        for rate in _BAUD_RATES:
            self.baud.addItem(str(rate), rate)
        self.baud.setCurrentIndex(0)
        self.baud.currentIndexChanged.connect(self._sync_connect)
        form.addWidget(self.baud)
        refresh = QPushButton("刷新端口")
        refresh.setObjectName("refreshButton")
        refresh.clicked.connect(lambda: self._refresh_ports(keep=True))
        form.addWidget(refresh)
        layout.addWidget(self.selectors)
        self.card = QFrame()
        self.card.setObjectName("card")
        self.card.setVisible(False)
        card_layout = QVBoxLayout(self.card)
        self.card_name = QLabel("—")
        self.card_name.setObjectName("deviceCard")
        self.card_meta = QLabel("")
        self.card_meta.setWordWrap(True)
        self.card_meta.setMaximumWidth(260)
        self.card_meta.setObjectName("muted")
        card_layout.addWidget(self.card_name)
        card_layout.addWidget(self.card_meta)
        layout.addWidget(self.card)
        layout.addWidget(QLabel("线路"))
        self.line = QComboBox()
        self.line.setObjectName("lineCombo")
        self.line.addItem("8N1 · 无流控", (8, "N", 1.0))
        layout.addWidget(self.line)
        self.dtr = QCheckBox("DTR · 关")
        self.rts = QCheckBox("RTS · 关")
        self.dtr.setObjectName("dtrBox")
        self.rts.setObjectName("rtsBox")
        self.dtr.stateChanged.connect(lambda state: self.dtr.setText("DTR · 开" if state else "DTR · 关"))
        self.rts.stateChanged.connect(lambda state: self.rts.setText("RTS · 开" if state else "RTS · 关"))
        layout.addWidget(self.dtr)
        layout.addWidget(self.rts)
        hint = QLabel("默认关闭。打开可能让板子复位。驱动不保证开口瞬间绝不复位。本程序不会自动试波特率，也不会在刷新时打开串口。")
        hint.setWordWrap(True)
        hint.setObjectName("hint")
        layout.addWidget(hint)
        self.assist_button = QPushButton("打开远程协助")
        self.assist_button.setObjectName("assistButton")
        self.assist_button.clicked.connect(self._toggle_drawer)
        layout.addWidget(self.assist_button)
        assist_hint = QLabel("先完成本地连接，再进入远程协助。")
        assist_hint.setObjectName("hint")
        assist_hint.setWordWrap(True)
        layout.addWidget(assist_hint)
        self.adb = QLabel("ADB          未实现")
        self.adb.setObjectName("adbLabel")
        self.adb.setEnabled(False)
        self.net = QLabel("网络          未实现")
        self.net.setObjectName("netLabel")
        self.net.setEnabled(False)
        layout.addWidget(self.adb)
        layout.addWidget(self.net)
        layout.addStretch(1)
        self.connect_button = QPushButton("连接")
        self.connect_button.setObjectName("connectButton")
        self.connect_button.setEnabled(False)
        self.connect_button.clicked.connect(self._connect)
        self.disconnect_button = QPushButton("断开连接")
        self.disconnect_button.setObjectName("disconnectButton")
        self.disconnect_button.clicked.connect(self._disconnect)
        self.disconnect_button.setVisible(False)
        layout.addWidget(self.connect_button)
        layout.addWidget(self.disconnect_button)
        self.connect_hint = QLabel("先选择串口和波特率")
        self.connect_hint.setObjectName("hint")
        layout.addWidget(self.connect_hint)
        scroll.setWidget(inner)
        dock_layout = QVBoxLayout(dock)
        dock_layout.setContentsMargins(0, 0, 0, 0)
        dock_layout.addWidget(scroll)
        return dock

    def _center(self) -> QWidget:
        center = QWidget()
        layout = QVBoxLayout(center)
        layout.setContentsMargins(0, 0, 0, 0)
        header = QHBoxLayout()
        header.addWidget(QLabel("日志"))
        header.addStretch(1)
        self.capture = QLabel("尚未记录")
        self.capture.setObjectName("captureLabel")
        self.pause_button = QPushButton("暂停滚动")
        self.pause_button.setObjectName("pauseButton")
        self.pause_button.clicked.connect(self._toggle_pause)
        header.addWidget(self.capture)
        header.addWidget(self.pause_button)
        layout.addLayout(header)
        self.guide = QFrame()
        self.guide.setObjectName("guide")
        guide_layout = QVBoxLayout(self.guide)
        self.guide_text = QLabel("尚未接入\n在左侧核对这只串口和波特率，再点「连接」。不会自动扫描，也不会试波特率。")
        self.guide_text.setWordWrap(True)
        guide_layout.addWidget(self.guide_text)
        steps = QHBoxLayout()
        for title, body in (("01  选择串口", "未选择"), ("02  选择波特率", "未选择"), ("03  连接", "还不能连接")):
            card = QFrame()
            card.setObjectName("card")
            inner = QVBoxLayout(card)
            inner.addWidget(QLabel(title))
            inner.addWidget(QLabel(body))
            steps.addWidget(card)
        guide_layout.addLayout(steps)
        layout.addWidget(self.guide)
        self.log = QPlainTextEdit()
        self.log.setObjectName("logView")
        self.log.setReadOnly(True)
        self.log.setMaximumBlockCount(2000)
        self.log.setFont(QFont("WenQuanYi Micro Hei Mono", 12))
        self.log.setPlaceholderText("输出区 · 连接之后，日志从这里往下排。")
        layout.addWidget(self.log, 1)
        self.log_hint = QLabel("硬件丢失：未知。暂停滚动只停视图，采集继续。")
        self.log_hint.setObjectName("hint")
        self.log_hint.setWordWrap(True)
        layout.addWidget(self.log_hint)
        return center

    def _drawer(self) -> QWidget:
        drawer = QFrame()
        drawer.setObjectName("drawer")
        drawer.setFixedWidth(340)
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        inner = QWidget()
        layout = QVBoxLayout(inner)
        layout.setContentsMargins(12, 12, 12, 12)
        head = QHBoxLayout()
        head.addWidget(QLabel("远程协助"))
        head.addStretch(1)
        close = QPushButton("关闭")
        close.clicked.connect(self._toggle_drawer)
        head.addWidget(close)
        layout.addLayout(head)
        intro = QLabel("对方可以看已授权的日志。每一次写都要单独批准，没有「下一条任意写」。")
        intro.setWordWrap(True)
        intro.setMaximumWidth(300)
        layout.addWidget(intro)
        layout.addWidget(QLabel("协助身份"))
        self.principal_edit = QLineEdit("eng")
        self.principal_edit.setObjectName("principalEdit")
        layout.addWidget(self.principal_edit)
        self.drawer_relay = QLabel("中继  未连接")
        layout.addWidget(self.drawer_relay)
        self.grant_button = QPushButton("允许查看日志")
        self.grant_button.setObjectName("grantButton")
        self.grant_button.clicked.connect(self._grant)
        self.revoke_button = QPushButton("收回查看")
        self.revoke_button.setObjectName("revokeButton")
        self.revoke_button.clicked.connect(self._revoke)
        layout.addWidget(self.grant_button)
        layout.addWidget(self.revoke_button)
        self.grant_label = QLabel("查看日志  未允许")
        self.grant_label.setWordWrap(True)
        self.grant_label.setMaximumWidth(300)
        layout.addWidget(self.grant_label)
        self.pending_box = QVBoxLayout()
        layout.addLayout(self.pending_box)
        self.pending_empty = QLabel("没有待批准的写。")
        self.pending_empty.setObjectName("hint")
        self.pending_empty.setWordWrap(True)
        layout.addWidget(self.pending_empty)
        self.recent_label = QLabel("")
        self.recent_label.setWordWrap(True)
        self.recent_label.setMaximumWidth(300)
        layout.addWidget(self.recent_label)
        export = QPushButton("导出诊断包")
        export.setObjectName("exportButton")
        export.clicked.connect(self._export)
        layout.addWidget(export)
        note = QLabel("诊断包不含密码。ADB、网络和刷写未实现。")
        note.setWordWrap(True)
        note.setObjectName("hint")
        layout.addWidget(note)
        layout.addStretch(1)
        scroll.setWidget(inner)
        outer = QVBoxLayout(drawer)
        outer.setContentsMargins(0, 0, 0, 0)
        outer.addWidget(scroll)
        return drawer

    def _refresh_ports(self, keep: bool) -> None:
        previous = self.port.currentData() if keep else None
        receipt = self.control.submit(Discover(), LOCAL_PRINCIPAL)
        self.port.blockSignals(True)
        self.port.clear()
        self.port.addItem("选择串口", None)
        for candidate in receipt.candidates or ():
            self.port.addItem(candidate.device, candidate.device)
        restore = 0
        if previous:
            index = self.port.findData(previous)
            restore = index if index >= 0 else 0
        self.port.setCurrentIndex(restore)
        self.port.blockSignals(False)
        self._sync_connect()

    def _sync_connect(self) -> None:
        ready = bool(self.port.currentData() and self.baud.currentData()) and not self._connected
        self.connect_button.setEnabled(ready)
        if self._connected:
            self.connect_hint.setText("已接入。新输出记在右侧日志。")
        elif ready:
            self.connect_hint.setText("参数已选定。点连接才会打开串口。")
        else:
            self.connect_hint.setText("先选择串口和波特率")

    def _connect(self) -> None:
        device = self.port.currentData()
        baud = self.baud.currentData()
        if not device or not baud:
            return
        bytesize, parity, stopbits = self.line.currentData()
        self.control.submit(
            Connect(
                device=device,
                baudrate=int(baud),
                bytesize=bytesize,
                parity=parity,
                stopbits=stopbits,
                dtr=self.dtr.isChecked(),
                rts=self.rts.isChecked(),
            ),
            LOCAL_PRINCIPAL,
        )

    def _disconnect(self) -> None:
        self.control.submit(Disconnect(), LOCAL_PRINCIPAL)

    def _toggle_drawer(self) -> None:
        visible = not self.drawer.isVisible()
        self.drawer.setVisible(visible)
        self.assist_button.setText("协助已打开" if visible else "打开远程协助")

    def _toggle_pause(self) -> None:
        self._follow = not self._follow
        self.pause_button.setText("继续滚动" if not self._follow else "暂停滚动")

    def _grant(self) -> None:
        principal_id = self.principal_edit.text().strip()
        self.control.submit(GrantView("ui-grant", principal_id, ("observe", "logs")), LOCAL_PRINCIPAL)

    def _revoke(self) -> None:
        self.control.submit(Revoke("ui-revoke"), LOCAL_PRINCIPAL)

    def _decide(self, operation_id: str, digest: str, session: str, approve: bool) -> None:
        self.control.submit(Decide(f"ui-{operation_id}", operation_id, digest, session, approve), LOCAL_PRINCIPAL)

    def _export(self) -> None:
        directory = os.environ.get("RDH_DIAG_DIR", "")
        if directory:
            path = os.path.join(directory, "diagnostic.json")
        else:
            path, _selected = QFileDialog.getSaveFileName(self, "导出诊断包", "rdh-diagnostic.json", "JSON (*.json)")
        if not path:
            return
        receipt = self.control.submit(ExportDiagnostic(), LOCAL_PRINCIPAL)
        if receipt.diagnostic is None:
            QMessageBox.warning(self, "诊断包", "没有写出诊断包。")
            return
        with open(path, "w", encoding="utf-8") as handle:
            import json

            json.dump(receipt.diagnostic, handle, ensure_ascii=False, indent=2)
            handle.write("\n")

    def _tick(self) -> None:
        records, gap = self.logs.poll()
        for record in records:
            self._append_log(record)
        if gap:
            self.capture.setText(f"已记录 {self._captured} 行 · 本视图缺口 {gap}")
        snap = self.control.local_snapshot()
        self._apply(snap)

    def _append_log(self, record) -> None:
        self._captured += 1
        text = record.data.decode("utf-8", errors="replace").rstrip("\n")
        bar = self.log.verticalScrollBar()
        position = bar.value()
        self.log.appendPlainText(f"{record.seq:>6}  {text}")
        if self._follow:
            bar.setValue(bar.maximum())
        else:
            bar.setValue(position)
        self.capture.setText(("已暂停滚动 · 仍在记录 " if not self._follow else "正在记录 ") + str(self._captured) + " 行")

    def _apply(self, snap) -> None:
        self._connected = snap.target_connected
        self.banner.setText("模拟 · 非现场实机" if snap.simulated else "本地串口 · 未做现场验收")
        self.target_state.setText("目标已连接" if snap.target_connected else ("正在打开" if snap.target_opening else "目标未连接"))
        relay = {
            "disabled": "中继未连接",
            "loopback_isolated": "中继已连接 · 本地隔离",
            "tls": "中继已连接",
            "lost": "中继已断开",
            "refused": "远程未启动",
        }.get(snap.relay_state, snap.relay_state)
        self.relay_state.setText(relay)
        self.grant_state.setText("可查看" if snap.grants else "远程未授权")
        self.selectors.setVisible(not snap.target_connected)
        self.card.setVisible(snap.target_connected)
        self.connect_button.setVisible(not snap.target_connected)
        self.disconnect_button.setVisible(snap.target_connected)
        self.guide.setVisible(not snap.target_connected and self._captured == 0)
        self.line.setEnabled(not snap.target_connected)
        self.dtr.setEnabled(not snap.target_connected)
        self.rts.setEnabled(not snap.target_connected)
        if snap.target_connected:
            self.card_name.setText(snap.target_device or "—")
            applied = "已按请求设置" if snap.lines_applied else "驱动未确认线路电平"
            self.card_meta.setText(
                "\n".join(
                    (
                        snap.identity_text or "",
                        f"{snap.target_baud} · {snap.target_format} · 无流控",
                        f"DTR {'开' if snap.target_dtr else '关'} · RTS {'开' if snap.target_rts else '关'} · {applied}",
                        "连接中不改线路。要改请断开后重设再连接。",
                    )
                )
            )
        self._sync_connect()
        self.drawer_relay.setText(f"中继  {relay}  {snap.relay_detail}".strip())
        if snap.grants:
            shown = ", ".join(f"{item.principal_id}:{'/'.join(item.scopes)}" for item in snap.grants)
            self.grant_label.setText(f"查看日志  已允许  {shown}")
        else:
            self.grant_label.setText("查看日志  未允许。收回后，未执行的远程写会被取消。")
        loss = "未知" if snap.hw_loss == "unknown" else snap.hw_loss
        extra = "写期间读取会暂停。" if snap.rx_paused_during_write else ""
        self.log_hint.setText(f"硬件丢失：{loss}。队列溢出 {snap.overflow_chunks} 块。{extra}暂停滚动只停视图。")
        signature = tuple((item.operation_id, item.digest) for item in snap.pending)
        if signature != self._pending_sig:
            self._pending_sig = signature
            self._render_pending(snap.pending)
        if snap.recent:
            last = snap.recent[-1]
            self.recent_label.setText(
                f"最近结果 {last.operation_id} {last.phase} {last.reason} "
                f"写出 {last.written_bytes}/{last.requested_bytes} "
                f"scope={last.success_scope} device={last.device_outcome}"
            )

    def _render_pending(self, pending) -> None:
        while self.pending_box.count():
            item = self.pending_box.takeAt(0)
            widget = item.widget()
            if widget is not None:
                widget.deleteLater()
        self.pending_empty.setVisible(not pending)
        for item in pending:
            card = QFrame()
            card.setObjectName("card")
            layout = QVBoxLayout(card)
            lines = QLabel(
                "\n".join(
                    (
                        f"调用方  {item.principal_id}",
                        f"目标会话  {item.target_session}",
                        f"动作  {item.action}",
                        f"精确字节  {item.nbytes}",
                        f"十六进制  {item.hex_preview}",
                        f"摘要  {item.payload_sha256}",
                        f"有效期  {item.expires_at}",
                        item.impact,
                    )
                )
            )
            lines.setWordWrap(True)
            lines.setMaximumWidth(300)
            lines.setObjectName("pendingText")
            layout.addWidget(lines)
            row = QHBoxLayout()
            deny = QPushButton("拒绝")
            deny.clicked.connect(lambda _checked=False, op=item: self._decide(op.operation_id, op.digest, op.target_session, False))
            approve = QPushButton("批准这一次")
            approve.setObjectName("approveButton")
            approve.clicked.connect(lambda _checked=False, op=item: self._decide(op.operation_id, op.digest, op.target_session, True))
            row.addWidget(deny)
            row.addWidget(approve)
            layout.addLayout(row)
            self.pending_box.addWidget(card)

    def closeEvent(self, event) -> None:
        if not self._shut:
            self._shut = True
            self._timer.stop()
            self.last_shutdown = self.control.shutdown_and_join(1.0)
            if self.last_shutdown.unfinished:
                QMessageBox.warning(
                    self,
                    "未干净退出",
                    (self.last_shutdown.reason or "仍有未结束的写出") + "\n这次关闭没有当成成功，也没有重放。",
                )
        event.accept()
