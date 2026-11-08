"""One CLI for engineers and bots.

Passwords come from an environment variable. A dropped connection exits
as unknown and does not publish the operation again.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import signal
import sys
import time
import uuid

from rdh.domain.model import PROTOCOL, VERSION, Phase
from rdh.domain.policy import exit_code, is_terminal
from rdh.remote.codec import encode_body, event_topic, request_topic, response_topic
from rdh.remote.mqtt import MqttClient, MqttError


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if "--password" in argv:
        print("密码只能从 --password-env 指定的环境变量读取。", file=sys.stderr)
        return 64
    parser = argparse.ArgumentParser(prog="rdh")
    parser.add_argument("--broker", required=True)
    parser.add_argument("--port", required=True, type=int)
    parser.add_argument("--host-id", required=True)
    parser.add_argument("--username", required=True)
    parser.add_argument("--password-env", required=True)
    parser.add_argument("--timeout", type=float, default=5.0)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("observe")
    logs = sub.add_parser("logs")
    logs_sub = logs.add_subparsers(dest="logs_command", required=True)
    follow = logs_sub.add_parser("follow")
    follow.add_argument("--for-seconds", type=float, default=0.0)
    operation = sub.add_parser("operation")
    operation_sub = operation.add_subparsers(dest="operation_command", required=True)
    write = operation_sub.add_parser("uart.write")
    write.add_argument("--hex")
    write.add_argument("--b64")
    write.add_argument("--operation-id", required=True)
    write.add_argument("--request-id", required=True)
    write.add_argument("--target-session", required=True)
    write.add_argument("--expires-at", required=True)
    query = operation_sub.add_parser("query")
    query.add_argument("--operation-id", required=True)
    query.add_argument("--request-id", default="")
    try:
        args = parser.parse_args(argv)
    except SystemExit as exc:
        return int(exc.code) if exc.code else 64
    password = os.environ.get(args.password_env)
    if not password:
        print("缺少密码环境变量。", file=sys.stderr)
        return 64
    client = MqttClient()
    try:
        client.connect(args.broker, args.port, f"rdh-cli-{args.username}-{uuid.uuid4().hex[:8]}", args.username, password)
        client.subscribe(response_topic(args.host_id, args.username))
        client.subscribe(event_topic(args.host_id, args.username))
        if args.command == "observe":
            return _observe(client, args)
        if args.command == "logs":
            return _follow(client, args.for_seconds)
        if args.operation_command == "query":
            request_id = args.request_id or "q-" + uuid.uuid4().hex[:8]
            return _roundtrip(client, args, _query_body(request_id, args.operation_id), args.operation_id, query=True)
        return _write(client, args)
    except MqttError as exc:
        print(f"本地传输失败：{exc}。未重发。", file=sys.stderr)
        return 6
    except KeyboardInterrupt:
        print("本地中断。这不代表远端操作已取消。", file=sys.stderr)
        return 130
    finally:
        client.close()


def _observe(client, args) -> int:
    request_id = "obs-" + uuid.uuid4().hex[:8]
    body = {"protocol": PROTOCOL, "version": VERSION, "request_id": request_id, "action": "observe"}
    found, status = _wait(client, args, body, request_id, None, until_terminal=False)
    if status == "disconnected":
        print("连接中断。未重发。", file=sys.stderr)
        return 3
    if found is None:
        print("观察没有返回。不代表目标在线，也没有重发。", file=sys.stderr)
        return 3
    _emit(found)
    print("观察完成，不代表目标在线。", file=sys.stderr)
    return 0


def _write(client, args) -> int:
    if bool(args.hex) == bool(args.b64):
        print("请只提供 --hex 或 --b64 其中之一。", file=sys.stderr)
        return 64
    try:
        payload = bytes.fromhex(args.hex) if args.hex is not None else base64.b64decode(args.b64, validate=True)
    except (ValueError, TypeError):
        print("字节参数无法解析。", file=sys.stderr)
        return 64
    body = {
        "protocol": PROTOCOL,
        "version": VERSION,
        "request_id": args.request_id,
        "operation_id": args.operation_id,
        "target_session": args.target_session,
        "action": "uart.write",
        "parameters": {"bytes_b64": base64.b64encode(payload).decode("ascii")},
        "expires_at": args.expires_at,
    }
    return _roundtrip(client, args, body, args.operation_id, query=False)


def _roundtrip(client, args, body: dict, operation_id: str, query: bool) -> int:
    found, status = _wait(client, args, body, body["request_id"], operation_id, query=query)
    if status == "disconnected":
        print("连接中断，结果未知。未重发。", file=sys.stderr)
        return 3
    if found is None or not is_terminal(str(found.get("phase"))):
        print("等待结束，结果未知。未自动重发。", file=sys.stderr)
        return 3
    _emit(found)
    print(_summary(found), file=sys.stderr)
    return exit_code(str(found["phase"]))


def _wait(client, args, body: dict, request_id: str, operation_id: str | None, query: bool = False, until_terminal: bool = True):
    client.publish(request_topic(args.host_id, args.username), encode_body(body), retain=False)
    deadline = time.time() + args.timeout
    seen = None
    next_query = time.time() + 0.12
    while time.time() < deadline:
        if not client.alive:
            return seen, "disconnected"
        message = client.take(0.05)
        if message is not None:
            try:
                decoded = json.loads(message["payload"].decode("utf-8"))
            except (UnicodeError, json.JSONDecodeError, AttributeError):
                decoded = None
            if isinstance(decoded, dict) and not decoded.get("kind") and (
                decoded.get("request_id") == request_id or (operation_id and decoded.get("operation_id") == operation_id)
            ):
                seen = decoded
                if (not until_terminal) or is_terminal(str(decoded.get("phase"))):
                    return decoded, ""
        if seen is not None and operation_id and not query and time.time() >= next_query and client.alive:
            query_id = "q-" + uuid.uuid4().hex[:8]
            client.publish(
                request_topic(args.host_id, args.username),
                encode_body(_query_body(query_id, operation_id)),
                retain=False,
            )
            request_id = query_id
            next_query = time.time() + 0.15
    return seen, "timeout"


def _query_body(request_id: str, operation_id: str) -> dict:
    return {"protocol": PROTOCOL, "version": VERSION, "request_id": request_id, "action": "operation.query", "operation_id": operation_id}


def _follow(client, seconds: float) -> int:
    if seconds < 0:
        print("--for-seconds 不能为负。", file=sys.stderr)
        return 64
    end = time.time() + seconds if seconds else None
    previous = signal.getsignal(signal.SIGINT)

    def _interrupt(_signum, _frame):
        raise KeyboardInterrupt

    signal.signal(signal.SIGINT, _interrupt)
    try:
        while True:
            if end is not None and time.time() >= end:
                return 0
            if not client.alive:
                print("事件流中断。未把缺口记成 0，也没有重发操作。", file=sys.stderr)
                return 3
            message = client.take(0.1)
            if message is None:
                continue
            try:
                decoded = json.loads(message["payload"].decode("utf-8"))
            except (UnicodeError, json.JSONDecodeError, AttributeError):
                continue
            if decoded.get("kind") != "log":
                continue
            gap = int(decoded.get("gap") or 0)
            if gap:
                print(f"# gap {gap}", flush=True)
            text = str(decoded.get("text") or "")
            print(text if text.endswith("\n") else text + "\n", end="", flush=True)
    finally:
        signal.signal(signal.SIGINT, previous)


def _emit(body: dict) -> None:
    print(json.dumps(body, ensure_ascii=False, sort_keys=True), flush=True)


def _summary(body: dict) -> str:
    phase = body.get("phase")
    written = body.get("written_bytes")
    requested = body.get("requested_bytes")
    if phase == Phase.WRITTEN:
        return f"传输完成：写出 {written}/{requested} 字节。设备结果未知。未重试。"
    if phase == Phase.PARTIAL:
        return f"部分写出 {written}/{requested} 字节。未补发。"
    if phase in (Phase.UNKNOWN, Phase.DELIVERY_UNKNOWN):
        return "结果未知。未自动重发。"
    return f"{phase} {body.get('reason')}"
