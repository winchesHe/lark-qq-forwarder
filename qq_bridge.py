#!/usr/bin/env python3

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import subprocess
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import httpx
from qqbot_agent_sdk import EventParser, QQApiClient, QQWebSocket, WSCallbacks


APP_ID = "1905539559"
KEYCHAIN_SERVICE = "codex.lark-qq-forwarder.qqbot-secret"
PROJECT_DIR = Path(__file__).resolve().parent
DEFAULT_INPUT = PROJECT_DIR / "lark-notifications.jsonl"
DEFAULT_STATE = PROJECT_DIR / ".qq-forwarder-state.json"


class BridgeError(RuntimeError):
    pass


def read_client_secret() -> str:
    result = subprocess.run(
        [
            "/usr/bin/security",
            "find-generic-password",
            "-w",
            "-a",
            APP_ID,
            "-s",
            KEYCHAIN_SERVICE,
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    secret = result.stdout.strip()
    if result.returncode != 0 or not secret:
        raise BridgeError("未在 macOS 钥匙串中找到 QQ 机器人密钥")
    return secret


@dataclass
class StateStore:
    path: Path
    data: dict[str, Any]

    @classmethod
    def load(cls, path: Path) -> "StateStore":
        if not path.exists():
            return cls(path=path, data={"schema_version": 1, "app_id": APP_ID})
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise BridgeError(f"无法读取转发状态文件：{path}") from exc
        if data.get("app_id") not in (None, APP_ID):
            raise BridgeError("状态文件属于另一个 QQ 机器人")
        data["app_id"] = APP_ID
        return cls(path=path, data=data)

    @property
    def group_openid(self) -> Optional[str]:
        value = self.data.get("group_openid")
        return value if isinstance(value, str) and value else None

    def bind_group(self, group_openid: str) -> None:
        self.data["group_openid"] = group_openid
        self.save()

    def prime(self, input_path: Path, *, force_end: bool = False) -> int:
        absolute_path = str(input_path.resolve())
        current_size = input_path.stat().st_size if input_path.exists() else 0
        stored_path = self.data.get("input_path")
        stored_offset = self.data.get("offset")

        if (
            force_end
            or stored_path != absolute_path
            or not isinstance(stored_offset, int)
            or stored_offset < 0
            or stored_offset > current_size
        ):
            self.data["offset"] = current_size
        self.data["input_path"] = absolute_path
        self.save()
        return int(self.data["offset"])

    def advance(self, offset: int, fingerprint: Optional[str] = None) -> None:
        self.data["offset"] = offset
        if fingerprint:
            recent = self.data.get("recent_fingerprints", [])
            if not isinstance(recent, list):
                recent = []
            recent = [value for value in recent if value != fingerprint]
            recent.append(fingerprint)
            self.data["recent_fingerprints"] = recent[-100:]
        self.save()

    def has_sent(self, fingerprint: str) -> bool:
        recent = self.data.get("recent_fingerprints", [])
        return isinstance(recent, list) and fingerprint in recent

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = self.path.with_suffix(self.path.suffix + ".tmp")
        temp_path.write_text(
            json.dumps(self.data, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        temp_path.replace(self.path)


@dataclass(frozen=True)
class JSONLRecord:
    next_offset: int
    payload: Optional[dict[str, Any]]


def read_next_record(input_path: Path, offset: int) -> Optional[JSONLRecord]:
    if not input_path.exists():
        return None
    with input_path.open("rb") as handle:
        handle.seek(offset)
        line = handle.readline()
        if not line or not line.endswith(b"\n"):
            return None
        next_offset = handle.tell()
    try:
        payload = json.loads(line.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        payload = None
    if not isinstance(payload, dict):
        payload = None
    return JSONLRecord(next_offset=next_offset, payload=payload)


def format_notification(payload: dict[str, Any]) -> str:
    title = str(payload.get("title") or "飞书通知").strip()
    subtitle = str(payload.get("subtitle") or "").strip()
    body = str(payload.get("body") or "").strip()
    lines = ["【飞书通知】", title]
    if subtitle and subtitle != title:
        lines.append(subtitle)
    if body:
        lines.append(body)
    return "\n".join(lines)


class GatewaySession:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._session_id: Optional[str] = None
        self._last_seq: Optional[int] = None

    def get(self) -> tuple[Optional[str], Optional[int]]:
        with self._lock:
            return self._session_id, self._last_seq

    def set(self, session_id: Optional[str], last_seq: Optional[int]) -> None:
        with self._lock:
            self._session_id = session_id
            self._last_seq = last_seq


async def create_api() -> tuple[QQApiClient, httpx.AsyncClient]:
    client = httpx.AsyncClient()
    api = QQApiClient(app_id=APP_ID, client_secret=read_client_secret())
    api.setup(client)
    await api.ensure_token()
    return api, client


async def send_group_text(
    api: QQApiClient,
    group_openid: str,
    content: str,
    *,
    reply_to: Optional[str] = None,
) -> dict[str, Any]:
    try:
        response = await api.send_text(
            "group",
            group_openid,
            content,
            reply_to=reply_to,
            markdown=False,
        )
    except RuntimeError as exc:
        raise BridgeError(
            "QQ 消息发送失败，请检查目标群是否允许机器人主动发言"
        ) from exc
    if not response.get("id"):
        raise BridgeError("QQ API 未返回消息 ID，无法确认消息已发送")
    return response


async def bind_group(state: StateStore, *, rebind: bool = False) -> str:
    if state.group_openid and not rebind:
        print("群聊已经绑定；如需更换目标群，请使用 bind --rebind")
        return state.group_openid

    api, http_client = await create_api()
    main_loop = asyncio.get_running_loop()
    bound = asyncio.Event()
    fatal_error: asyncio.Queue[str] = asyncio.Queue(maxsize=1)
    session = GatewaySession()

    async def on_message(event_type: str, raw: dict[str, Any]) -> None:
        event = EventParser().parse(event_type, raw)
        if not event or event.chat_scope != "group":
            return
        state.bind_group(event.chat_id)
        await send_group_text(
            api,
            event.chat_id,
            "绑定成功。接下来会把本机捕获到的新飞书通知转发到这个群。",
            reply_to=event.message_id,
        )
        print("已收到群聊 @ 消息并完成绑定。")
        bound.set()

    def on_fatal_error(code: str, message: str) -> None:
        main_loop.call_soon_threadsafe(
            fatal_error.put_nowait, f"QQ 网关连接失败（{code}）：{message}"
        )

    callbacks = WSCallbacks(
        on_message_event=on_message,
        on_connected=lambda: print("QQ 网关已连接，请在目标群发送：@qclaw 绑定测试"),
        on_disconnected=lambda: print("QQ 网关连接已断开，正在尝试恢复……"),
        on_fatal_error=on_fatal_error,
        get_token=api.ensure_token_sync,
        get_session=session.get,
        set_session=session.set,
        set_heartbeat_interval=lambda _: None,
        clear_token=api.clear_token,
        fail_pending=lambda _: None,
        get_gateway_url=api.get_gateway_url_sync,
    )
    websocket = QQWebSocket(callbacks=callbacks, log_tag=f"QQBot:{APP_ID}")

    try:
        gateway_url = await asyncio.to_thread(api.get_gateway_url_sync)
        websocket.start(gateway_url, main_loop)
        bind_task = asyncio.create_task(bound.wait())
        error_task = asyncio.create_task(fatal_error.get())
        done, pending = await asyncio.wait(
            {bind_task, error_task}, return_when=asyncio.FIRST_COMPLETED
        )
        for task in pending:
            task.cancel()
        if error_task in done:
            raise BridgeError(error_task.result())
        if not state.group_openid:
            raise BridgeError("收到了群消息，但未能保存群标识")
        return state.group_openid
    finally:
        await websocket.async_stop()
        await http_client.aclose()


async def send_test(state: StateStore) -> None:
    group_openid = state.group_openid
    if not group_openid:
        raise BridgeError("尚未绑定 QQ 群，请先运行 bind")
    api, http_client = await create_api()
    try:
        await send_group_text(
            api,
            group_openid,
            "QQ 主动消息测试成功。下一条新飞书通知将由本机自动转发。",
        )
        print("QQ 主动测试消息发送成功。")
    finally:
        await http_client.aclose()


async def forward_forever(
    state: StateStore,
    input_path: Path,
    *,
    poll_interval: float = 0.25,
) -> None:
    group_openid = state.group_openid
    if not group_openid:
        raise BridgeError("尚未绑定 QQ 群，请先运行 bind")
    offset = state.prime(input_path)
    api, http_client = await create_api()
    print(f"飞书 → QQ 转发已启动；从字节位置 {offset} 继续监听。")

    try:
        while True:
            record = read_next_record(input_path, offset)
            if record is None:
                await asyncio.sleep(poll_interval)
                continue

            offset = record.next_offset
            payload = record.payload
            if payload is None:
                state.advance(offset)
                print("跳过一行无法解析的 JSONL 记录。")
                continue

            fingerprint = str(payload.get("fingerprint") or "")
            if fingerprint and state.has_sent(fingerprint):
                state.advance(offset)
                continue

            await send_group_text(
                api,
                group_openid,
                format_notification(payload),
            )
            state.advance(offset, fingerprint or None)
            title = str(payload.get("title") or "飞书通知")
            print(f"已转发：{title}")
    finally:
        await http_client.aclose()


async def check_connection() -> None:
    api, http_client = await create_api()
    try:
        await asyncio.to_thread(api.get_gateway_url_sync)
        print("QQ 凭证和网关连接检查通过。")
    finally:
        await http_client.aclose()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="飞书本地通知到 QQ 群转发器")
    parser.add_argument(
        "command", choices=["check", "prime", "bind", "test", "run"]
    )
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--state", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--rebind", action="store_true")
    parser.add_argument("--force-end", action="store_true")
    return parser.parse_args()


async def async_main() -> None:
    args = parse_args()
    state = StateStore.load(args.state)
    if args.command == "check":
        await check_connection()
    elif args.command == "prime":
        offset = state.prime(args.input, force_end=args.force_end)
        print(f"转发起点已设为字节位置 {offset}。")
    elif args.command == "bind":
        await bind_group(state, rebind=args.rebind)
    elif args.command == "test":
        await send_test(state)
    elif args.command == "run":
        await forward_forever(state, args.input)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    try:
        asyncio.run(async_main())
    except KeyboardInterrupt:
        print("已停止。")
    except BridgeError as exc:
        raise SystemExit(f"错误：{exc}") from exc


if __name__ == "__main__":
    main()
