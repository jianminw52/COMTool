"""Windows and desktop entry for the on-site host.

Run: python COMTool/host_main.py
The process is a Windows field program. This Linux checkout can launch the
same PyQt5 window; it does not turn the tool into a Linux-only build.
"""

import os
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from PyQt5.QtWidgets import QApplication

from COMTool.host.runtime import Runtime
from COMTool.host.window import HostWindow


def main(argv=None) -> int:
    app = QApplication(argv if argv is not None else sys.argv)
    window = HostWindow(Runtime())
    window.show()
    return app.exec_()


if __name__ == "__main__":
    raise SystemExit(main())
