"""JSON codec for rdh/v1.

The trusted principal comes from the topic the broker ACL allowed, never
from the payload or the MQTT client id.
"""

from __future__ import annotations

import base64
import json

from rdh.domain.model import (
    PROTOCOL,
    VERSION,
    DeviceOutcome,
    Effect,
    Observe,
    Phase,
    Principal,
    QueryOperation,
    RequestOperation,
    Scope,
    UART_WRITE,
)
from rdh.domain.policy import make_receipt

_REMOTE_FORBIDDEN = {"decide", "approve", "grant", "revoke", "connect", "disconnect", "shutdown"}


class WireError(Exception):
    def __init__(self, phase: str, reason: str, request_id: str = "", operation_id: str | None = None):
        super().__init__(reason)
        self.phase = phase
        self.reason = reason
        self.request_id = request_id
        self.operation_id = operation_id


def request_topic(host_id: str, principal_id: str) -> str:
    return f"rdh/v1/hosts/{host_id}/requests/{principal_id}"


def response_topic(host_id: str, principal_id: str) -> str:
    return f"rdh/v1/hosts/{host_id}/responses/{principal_id}"


def event_topic(host_id: str, principal_id: str) -> str:
    return f"rdh/v1/hosts/{host_id}/events/{principal_id}"


def principal_from_request_topic(topic: str, host_id: str) -> str | None:
    prefix = f"rdh/v1/hosts/{host_id}/requests/"
    if not topic.startswith(prefix):
        return None
    principal_id = topic[len(prefix) :]
    if not principal_id or "/" in principal_id or principal_id in ("local", "system", "+", "#"):
        return None
    return principal_id


def encode_body(body: dict) -> bytes:
    if "password" in body or "secret" in body:
        raise WireError(Phase.REJECTED, "secret_in_payload")
    return json.dumps(body, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")


def decode_request(payload: bytes, principal: Principal):
    try:
        data = json.loads(payload.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise WireError(Phase.REJECTED, "bad_json") from exc
    if not isinstance(data, dict):
        raise WireError(Phase.REJECTED, "bad_json")
    request_id = str(data.get("request_id") or "")
    operation_id = data.get("operation_id")
    if data.get("protocol") != PROTOCOL or data.get("version") != VERSION:
        raise WireError(Phase.REJECTED, "bad_protocol", request_id, operation_id if isinstance(operation_id, str) else None)
    if "client_id" in data:
        raise WireError(Phase.DENIED, "client_id_is_not_principal", request_id)
    claimed = data.get("principal")
    if claimed is not None:
        if claimed != principal.principal_id:
            raise WireError(Phase.DENIED, "payload_principal_mismatch", request_id)
        raise WireError(Phase.DENIED, "payload_principal_not_accepted", request_id)
    action = data.get("action")
    if action in _REMOTE_FORBIDDEN:
        reason = "remote_cannot_approve" if action in ("decide", "approve") else "remote_forbidden"
        raise WireError(Phase.DENIED, reason, request_id, operation_id if isinstance(operation_id, str) else None)
    if action == "observe":
        return Observe(request_id)
    if action == "operation.query":
        if not isinstance(operation_id, str):
            raise WireError(Phase.REJECTED, "bad_id", request_id)
        return QueryOperation(request_id, operation_id)
    if action == UART_WRITE:
        raw = data.get("parameters") or {}
        encoded = raw.get("bytes_b64")
        if not isinstance(encoded, str):
            raise WireError(Phase.REJECTED, "bad_payload", request_id, operation_id if isinstance(operation_id, str) else None)
        try:
            blob = base64.b64decode(encoded, validate=True)
        except (ValueError, TypeError) as exc:
            raise WireError(Phase.REJECTED, "bad_base64", request_id) from exc
        if not isinstance(operation_id, str) or not isinstance(data.get("target_session"), str) or not isinstance(data.get("expires_at"), str):
            raise WireError(Phase.REJECTED, "bad_fields", request_id)
        return RequestOperation(request_id, operation_id, UART_WRITE, data["target_session"], blob, data["expires_at"])
    if action in ("adb.exec", "net.debug", "flash.write"):
        raise WireError(Phase.UNSUPPORTED, "not_in_mvp", request_id, operation_id if isinstance(operation_id, str) else None)
    raise WireError(Phase.UNSUPPORTED, "unknown_action", request_id, operation_id if isinstance(operation_id, str) else None)


def error_receipt(error: WireError):
    return make_receipt(
        request_id=error.request_id,
        operation_id=error.operation_id,
        phase=error.phase,
        effect=Effect.NONE,
        reason=error.reason,
        success_scope=Scope.NONE,
        device_outcome=DeviceOutcome.NA,
    )


def _snapshot_dict(snapshot) -> dict:
    return {
        "program": {
            "version": snapshot.version,
            "state": snapshot.program_state,
            "simulated": snapshot.simulated,
            "clock_trust": snapshot.clock_trust,
        },
        "target": {
            "connected": snapshot.target_connected,
            "session": snapshot.target_session,
            "device": snapshot.target_device,
            "baud": snapshot.target_baud,
            "format": snapshot.target_format,
        },
        "relay": {"state": snapshot.relay_state, "detail": snapshot.relay_detail},
        "permission": {
            "grants": [{"principal_id": item.principal_id, "scopes": list(item.scopes)} for item in snapshot.grants],
            "writes": "per_operation_approval",
        },
        "capture": {"seq": snapshot.capture_seq, "overflow_chunks": snapshot.overflow_chunks, "hw_loss": snapshot.hw_loss},
    }


def receipt_dict(receipt) -> dict:
    body = {
        "protocol": PROTOCOL,
        "version": VERSION,
        "request_id": receipt.request_id,
        "operation_id": receipt.operation_id,
        "phase": receipt.phase,
        "effect": receipt.effect,
        "reason": receipt.reason,
        "success_scope": receipt.success_scope,
        "device_outcome": receipt.device_outcome,
        "written_bytes": receipt.written_bytes,
        "requested_bytes": receipt.requested_bytes,
        "digest": receipt.digest,
        "target_session": receipt.target_session,
        "audit_persisted": receipt.audit_persisted,
        "unfinished": receipt.unfinished,
    }
    if receipt.snapshot is not None:
        body["snapshot"] = _snapshot_dict(receipt.snapshot)
    return body
