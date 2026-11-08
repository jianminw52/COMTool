"""Import boundaries. The core does not import Qt, serial, sockets, or adapters."""

from __future__ import annotations

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1] / "rdh"

FORBIDDEN_CORE = ("PyQt5", "PyQt6", "PySide2", "PySide6", "serial", "socket", "paho", "rdh.adapters", "rdh.ui", "rdh.remote", "rdh.cli", "rdh.bootstrap")
FORBIDDEN_UI = ("serial", "socket", "rdh.adapters", "rdh.remote", "rdh.cli")


def _imports(path: Path) -> list[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    found = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            found.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            found.append(node.module)
    return found


def _forbidden(modules: list[str], banned: tuple[str, ...]) -> list[str]:
    hits = []
    for module in modules:
        for item in banned:
            if module == item or module.startswith(item + "."):
                hits.append(module)
    return hits


def test_domain_and_app_stay_free_of_frameworks():
    for folder in ("domain", "app"):
        for path in (ROOT / folder).glob("*.py"):
            hits = _forbidden(_imports(path), FORBIDDEN_CORE)
            assert hits == [], (path, hits)


def test_ui_does_not_touch_the_device_or_the_wire():
    for path in (ROOT / "ui").glob("*.py"):
        hits = _forbidden(_imports(path), FORBIDDEN_UI)
        assert hits == [], (path, hits)


def test_remote_and_cli_do_not_own_the_device():
    for folder in ("remote", "cli"):
        for path in (ROOT / folder).glob("*.py"):
            hits = _forbidden(_imports(path), ("PyQt5", "serial", "rdh.adapters", "rdh.ui", "rdh.app"))
            assert hits == [], (path, hits)


def test_adapters_do_not_import_qt_or_the_control_owner():
    for path in (ROOT / "adapters").glob("*.py"):
        modules = _imports(path)
        hits = _forbidden(modules, ("PyQt5", "rdh.ui", "rdh.app.control", "rdh.remote", "rdh.cli"))
        assert hits == [], (path, hits)
