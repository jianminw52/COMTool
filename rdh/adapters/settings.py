"""Non-secret settings.

``host.json`` is never read or rewritten. Secret-looking keys are refused
and the file is left untouched.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

_ALLOWED = {
    "host_id",
    "remote_mode",
    "broker_host",
    "broker_port",
    "tls_ca_file",
    "tls_cert_file",
    "tls_key_file",
    "credential_name",
    "journal_path",
    "log_path",
}
_SECRET_PARTS = ("password", "secret", "token", "private_key", "passwd", "authorization")


class SettingsError(Exception):
    pass


@dataclass(frozen=True)
class Settings:
    host_id: str = "host"
    remote_mode: str = "disabled"
    broker_host: str = "127.0.0.1"
    broker_port: int = 0
    tls_ca_file: str = ""
    tls_cert_file: str = ""
    tls_key_file: str = ""
    credential_name: str = ""
    journal_path: str = ""
    log_path: str = ""


def load_settings(path: str | Path) -> Settings:
    file = Path(path)
    if file.name == "host.json":
        raise SettingsError("refusing to read or migrate host.json")
    if not file.exists():
        return Settings()
    raw = file.read_text(encoding="utf-8")
    data = json.loads(raw)
    if not isinstance(data, dict):
        raise SettingsError("settings must be an object")
    _reject(data)
    unknown = set(data) - _ALLOWED
    if unknown:
        raise SettingsError("unknown settings fields: " + ", ".join(sorted(unknown)))
    if data.get("remote_mode", "disabled") not in ("disabled", "loopback_isolated", "tls"):
        raise SettingsError("bad remote_mode")
    return Settings(
        host_id=str(data.get("host_id") or "host"),
        remote_mode=str(data.get("remote_mode") or "disabled"),
        broker_host=str(data.get("broker_host") or "127.0.0.1"),
        broker_port=int(data.get("broker_port") or 0),
        tls_ca_file=str(data.get("tls_ca_file") or ""),
        tls_cert_file=str(data.get("tls_cert_file") or ""),
        tls_key_file=str(data.get("tls_key_file") or ""),
        credential_name=str(data.get("credential_name") or ""),
        journal_path=str(data.get("journal_path") or ""),
        log_path=str(data.get("log_path") or ""),
    )


def save_settings(path: str | Path, settings: Settings) -> None:
    file = Path(path)
    if file.name == "host.json":
        raise SettingsError("refusing to write host.json")
    body = {
        "host_id": settings.host_id,
        "remote_mode": settings.remote_mode,
        "broker_host": settings.broker_host,
        "broker_port": settings.broker_port,
        "tls_ca_file": settings.tls_ca_file,
        "tls_cert_file": settings.tls_cert_file,
        "tls_key_file": settings.tls_key_file,
        "credential_name": settings.credential_name,
        "journal_path": settings.journal_path,
        "log_path": settings.log_path,
    }
    _reject(body)
    file.write_text(json.dumps(body, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _reject(data: dict) -> None:
    for key, value in data.items():
        lowered = str(key).lower()
        if any(part in lowered for part in _SECRET_PARTS):
            raise SettingsError(f"refusing secret field {key}")
        if isinstance(value, str) and "BEGIN PRIVATE KEY" in value:
            raise SettingsError("refusing inline private key")
