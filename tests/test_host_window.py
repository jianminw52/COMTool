"""Offscreen checks for the real Qt window. Not a Windows desktop session."""

from __future__ import annotations

import os
import unittest

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PyQt5.QtWidgets import QApplication

from COMTool.host.runtime import Runtime
from COMTool.host.window import HostWindow

_APP = QApplication.instance() or QApplication([])


class WindowTests(unittest.TestCase):
    def test_initial_form_does_not_pick_a_port_or_baud(self):
        window = HostWindow(Runtime({"device_id": "", "broker_host": "", "broker_tls": False}))
        window.show()
        _APP.processEvents()
        self.assertEqual(window.port_box.currentText(), "选择串口")
        self.assertEqual(window.baud_box.currentText(), "选择波特率")
        self.assertFalse(window.connect_button.isEnabled())
        self.assertFalse(window.dtr_box.isChecked())
        self.assertFalse(window.rts_box.isChecked())
        self.assertFalse(window.runtime.owner.is_open)
        window.close()


if __name__ == "__main__":
    unittest.main()
