"""Composition root. No approval policy lives here."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from rdh.adapters.clock import SystemClock
from rdh.adapters.fake_device import FakeDevice
from rdh.adapters.journal import FileJournal
from rdh.adapters.logs import LogFileSink
from rdh.adapters.serial_device import SerialDevice
from rdh.adapters.settings import SettingsError, load_settings
from rdh.app.control import Control
from rdh.domain.model import SYSTEM_PRINCIPAL, SetRelay


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="rdh", description="现场 Remote Debug Host")
    parser.add_argument("--simulate", action="store_true", help="使用模拟串口，界面会标明不是现场实机")
    parser.add_argument("--settings", default="", help="非秘密设置文件，不能是 host.json")
    args = parser.parse_args(argv)
    try:
        settings = load_settings(args.settings) if args.settings else load_settings(Path("rdh-settings.json"))
    except SettingsError as exc:
        print(str(exc), file=sys.stderr)
        return 64
    data_dir = Path.cwd() / "rdh-data"
    journal_path = settings.journal_path or str(data_dir / "audit.jsonl")
    log_path = settings.log_path or str(data_dir / "capture.log")
    device = FakeDevice() if args.simulate else SerialDevice()
    journal = FileJournal(journal_path)
    logs = LogFileSink(log_path)
    control = Control(
        clock=SystemClock(),
        audit=journal,
        device=device,
        host_id=settings.host_id,
        simulated=args.simulate,
        clock_trust="unverified_local",
    )
    control.add_log_sink(logs)
    # Remote transport is intentionally off until a qualified mode is selected.
    control.submit(SetRelay("disabled", ""), SYSTEM_PRINCIPAL)
    from PyQt5.QtWidgets import QApplication

    from rdh.ui.window import StationWindow

    app = QApplication.instance() or QApplication(sys.argv)
    window = StationWindow(control)
    window.show()
    app.exec_()
    logs.shutdown(1.0)
    journal.close()
    report = window.last_shutdown
    if report is not None and report.unfinished:
        print(report.reason, file=sys.stderr)
        return 6
    return 0
