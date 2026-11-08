"""Start or refuse the remote adapter.

Loopback without TLS is allowed only for an explicit isolated mode aimed
at 127.0.0.1. Anything else needs a complete TLS identity. Passwords are
read from the secret port, never from the settings file.
"""

from __future__ import annotations

import ssl
from pathlib import Path

from rdh.domain.model import SYSTEM_PRINCIPAL, SetRelay
from rdh.remote.adapter import RemoteAdapter
from rdh.remote.mqtt import MqttError

_LOOPBACK = {"127.0.0.1", "localhost"}


def start_remote(control, settings, secrets):
    mode = settings.remote_mode
    if mode == "disabled":
        control.submit(SetRelay("disabled", ""), SYSTEM_PRINCIPAL)
        return None
    if mode == "loopback_isolated":
        if settings.broker_host not in _LOOPBACK or not settings.broker_port:
            control.submit(SetRelay("refused", "loopback_required"), SYSTEM_PRINCIPAL)
            return None
        return _connect(control, settings, secrets, mode, ssl_context=None, server_hostname=None)
    if mode == "tls":
        if not _tls_ready(settings):
            control.submit(SetRelay("refused", "tls_or_identity_missing"), SYSTEM_PRINCIPAL)
            return None
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        context.load_verify_locations(settings.tls_ca_file)
        context.load_cert_chain(settings.tls_cert_file, settings.tls_key_file)
        context.check_hostname = True
        return _connect(control, settings, secrets, mode, ssl_context=context, server_hostname="localhost")
    control.submit(SetRelay("refused", "unknown_remote_mode"), SYSTEM_PRINCIPAL)
    return None


def _tls_ready(settings) -> bool:
    if not settings.credential_name:
        return False
    paths = (settings.tls_ca_file, settings.tls_cert_file, settings.tls_key_file)
    return all(path and Path(path).is_file() for path in paths)


def _connect(control, settings, secrets, mode: str, ssl_context, server_hostname):
    if not settings.credential_name:
        control.submit(SetRelay("refused", "secret_unavailable"), SYSTEM_PRINCIPAL)
        return None
    try:
        password = secrets.get(settings.credential_name)
    except Exception:
        control.submit(SetRelay("refused", "secret_unavailable"), SYSTEM_PRINCIPAL)
        return None
    adapter = RemoteAdapter(
        control,
        settings.host_id,
        settings.broker_host,
        int(settings.broker_port),
        f"host:{settings.host_id}",
        password,
        mode,
        ssl_context=ssl_context,
        server_hostname=server_hostname,
    )
    try:
        adapter.start()
    except (MqttError, OSError, ssl.SSLError) as exc:
        control.submit(SetRelay("refused", type(exc).__name__), SYSTEM_PRINCIPAL)
        return None
    return adapter
