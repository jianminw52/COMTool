"""Host settings. The broker password is kept for the local process only."""

from __future__ import annotations

import json
import os
import sys


def config_dir() -> str:
    if sys.platform.startswith("win"):
        root = os.environ.get("APPDATA") or os.path.expanduser(r"~\AppData\Roaming")
        return os.path.join(root, "COMTool")
    home = os.environ.get("HOME") or os.path.expanduser("~")
    return os.path.join(home, ".config", "comtool")


def config_path() -> str:
    return os.path.join(config_dir(), "host.json")


DEFAULTS = {
    "device_id": "",
    "broker_host": "",
    "broker_port": 8883,
    "broker_tls": True,
    "broker_username": "",
    "broker_password": "",
    "approval_seconds": 7.0,
    "echo_quiet_seconds": 5.0,
    "serial_log": "",
}


def load() -> dict:
    data = dict(DEFAULTS)
    path = config_path()
    if os.path.exists(path):
        with open(path, encoding="utf-8") as handle:
            stored = json.load(handle)
        if isinstance(stored, dict):
            for key in DEFAULTS:
                if key in stored:
                    data[key] = stored[key]
    return data


def save(data: dict) -> None:
    path = config_path()
    os.makedirs(os.path.dirname(path), exist_ok=True)
    out = {key: data.get(key, DEFAULTS[key]) for key in DEFAULTS}
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(out, handle, indent=2, ensure_ascii=False)
        handle.write("\n")
