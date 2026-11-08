"""Secret ports.

Plain JSON is not a backend. An unavailable store fails the call.
The Windows bridge is present for a later on-site build and is not
treated as tested on other operating systems.
"""

from __future__ import annotations

import os


class SecretError(Exception):
    pass


class UnavailableSecrets:
    def status(self) -> str:
        return "unavailable"

    def put(self, name: str, secret: str) -> None:
        del name, secret
        raise SecretError("secret store unavailable: refusing plaintext fallback")

    def get(self, name: str) -> str:
        del name
        raise SecretError("secret store unavailable: refusing plaintext fallback")


class MemorySecrets:
    """Process-local double for tests. Callers must opt in."""

    def __init__(self):
        self._items: dict[str, str] = {}

    def status(self) -> str:
        return "memory"

    def put(self, name: str, secret: str) -> None:
        if not name or secret is None or secret == "":
            raise SecretError("empty secret")
        self._items[name] = secret

    def get(self, name: str) -> str:
        try:
            return self._items[name]
        except KeyError as exc:
            raise SecretError("secret not found") from exc


class WindowsCredentialSecrets:
    def __init__(self):
        if os.name != "nt":
            raise SecretError("Windows credential store is not available on this OS")
        try:
            import win32cred
        except ImportError as exc:
            raise SecretError("win32cred is not installed") from exc
        self._win32cred = win32cred

    def status(self) -> str:
        return "windows"

    def put(self, name: str, secret: str) -> None:
        if not name or not secret:
            raise SecretError("empty secret")
        payload = {
            "Type": self._win32cred.CRED_TYPE_GENERIC,
            "TargetName": f"rdh/{name}",
            "CredentialBlob": secret,
            "Persist": self._win32cred.CRED_PERSIST_LOCAL_MACHINE,
            "UserName": "rdh",
        }
        self._win32cred.CredWrite(payload, 0)

    def get(self, name: str) -> str:
        try:
            cred = self._win32cred.CredRead(f"rdh/{name}", self._win32cred.CRED_TYPE_GENERIC)
        except Exception as exc:  # noqa: BLE001 - win32 raises its own errors
            raise SecretError("secret not found") from exc
        blob = cred.get("CredentialBlob")
        if isinstance(blob, bytes):
            return blob.decode("utf-16-le") if b"\x00" in blob else blob.decode()
        return str(blob)
