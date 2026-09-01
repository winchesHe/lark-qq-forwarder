#!/usr/bin/env python3

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import re
import shutil
import subprocess
import tempfile
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import httpx
from qqbot_agent_sdk import (
    MEDIA_TYPE_IMAGE,
    EventParser,
    MediaInfo,
    MediaUploader,
    MessageToCreate,
    QQApiClient,
    QQMessageType,
    QQWebSocket,
    WSCallbacks,
)


APP_ID = "1905539559"
KEYCHAIN_SERVICE = "codex.lark-qq-forwarder.qqbot-secret"
PROJECT_DIR = Path(__file__).resolve().parent
DEFAULT_INPUT = PROJECT_DIR / "lark-notifications.jsonl"
DEFAULT_STATE = PROJECT_DIR / ".qq-forwarder-state.json"
DEFAULT_LARK_PROFILE = "tenant-105183"
DEFAULT_LARK_CONTACT = "Perfecto"
IMAGE_KEY_PATTERN = re.compile(r"\b(img_[A-Za-z0-9_-]{10,})\b")


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
            return cls(
                path=path,
                data={"schema_version": 2, "app_id": APP_ID},
            )
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise BridgeError(f"无法读取转发状态文件：{path}") from exc
        if data.get("app_id") not in (None, APP_ID):
            raise BridgeError("状态文件属于另一个 QQ 机器人")
        data.pop("recent_fingerprints", None)
        data["schema_version"] = 2
        data["app_id"] = APP_ID
        return cls(path=path, data=data)

    @property
    def group_openid(self) -> Optional[str]:
        value = self.data.get("group_openid")
        return value if isinstance(value, str) and value else None

    @property
    def message_position(self) -> Optional[int]:
        value = self.data.get("lark_message_position")
        return value if isinstance(value, int) and value >= 0 else None

    def bind_group(self, group_openid: str) -> None:
        self.data["group_openid"] = group_openid
        self.save()

    def prime_input(self, input_path: Path, *, force_end: bool = False) -> int:
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

    def prime_lark(
        self,
        *,
        chat_id: str,
        sender_id: str,
        latest_position: int,
        force_end: bool = False,
    ) -> int:
        target_changed = (
            self.data.get("lark_chat_id") != chat_id
            or self.data.get("lark_sender_id") != sender_id
        )
        if target_changed or force_end or self.message_position is None:
            self.data["lark_message_position"] = latest_position
            self.data["recent_message_ids"] = []
        self.data["lark_chat_id"] = chat_id
        self.data["lark_sender_id"] = sender_id
        self.save()
        return int(self.data["lark_message_position"])

    def assert_lark_target(self, *, chat_id: str, sender_id: str) -> None:
        if (
            self.data.get("lark_chat_id") != chat_id
            or self.data.get("lark_sender_id") != sender_id
            or self.message_position is None
        ):
            raise BridgeError("飞书目标或消息游标尚未初始化，请先运行 prime")

    def advance_input(self, offset: int) -> None:
        self.data["offset"] = offset
        self.save()

    def advance_message(self, position: int, message_id: str) -> None:
        current = self.message_position or 0
        self.data["lark_message_position"] = max(current, position)
        recent = self.data.get("recent_message_ids", [])
        if not isinstance(recent, list):
            recent = []
        recent = [value for value in recent if value != message_id]
        recent.append(message_id)
        self.data["recent_message_ids"] = recent[-200:]
        self.save()

    def has_processed_message(self, message_id: str) -> bool:
        recent = self.data.get("recent_message_ids", [])
        return isinstance(recent, list) and message_id in recent

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


def notification_matches_contact(
    payload: dict[str, Any], contact_name: str
) -> bool:
    clue = contact_name.strip().casefold()
    if not clue:
        return False
    values = [payload.get("title")]
    raw_texts = payload.get("raw_texts")
    if isinstance(raw_texts, list):
        values.extend(raw_texts)
    return any(
        clue in value.strip().casefold()
        for value in values
        if isinstance(value, str)
    )


def format_lark_text(contact_name: str, content: str) -> str:
    return f"【飞书·{contact_name}】\n{content.strip()}"


@dataclass(frozen=True)
class LarkTarget:
    name: str
    sender_id: str
    chat_id: str


@dataclass(frozen=True)
class LarkMessage:
    message_id: str
    position: int
    msg_type: str
    sender_id: str
    content: str


def _walk_objects(root: Any) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    queue = [root]
    seen: set[int] = set()
    while queue:
        value = queue.pop(0)
        if not isinstance(value, (dict, list)):
            continue
        identity = id(value)
        if identity in seen:
            continue
        seen.add(identity)
        if isinstance(value, dict):
            result.append(value)
            queue.extend(value.values())
        else:
            queue.extend(value)
    return result


def extract_lark_messages(payload: Any) -> list[LarkMessage]:
    messages: list[LarkMessage] = []
    seen_ids: set[str] = set()
    for value in _walk_objects(payload):
        message_id = value.get("message_id")
        position = value.get("message_position")
        msg_type = value.get("msg_type")
        sender = value.get("sender")
        if not (
            isinstance(message_id, str)
            and isinstance(msg_type, str)
            and isinstance(sender, dict)
        ):
            continue
        try:
            numeric_position = int(position)
        except (TypeError, ValueError):
            continue
        sender_id = sender.get("id")
        if not isinstance(sender_id, str) or message_id in seen_ids:
            continue
        content = value.get("content")
        messages.append(
            LarkMessage(
                message_id=message_id,
                position=numeric_position,
                msg_type=msg_type,
                sender_id=sender_id,
                content=content if isinstance(content, str) else "",
            )
        )
        seen_ids.add(message_id)
    return messages


def pending_messages(
    messages: list[LarkMessage], cursor: int
) -> list[LarkMessage]:
    return sorted(
        (message for message in messages if message.position > cursor),
        key=lambda message: message.position,
    )


def extract_image_key(content: str) -> Optional[str]:
    match = IMAGE_KEY_PATTERN.search(content)
    return match.group(1) if match else None


class LarkClient:
    def __init__(self, *, profile: str, binary: Optional[str] = None) -> None:
        self.profile = profile
        self.binary = binary or shutil.which("lark-cli") or "lark-cli"

    def _run(self, arguments: list[str], *, cwd: Optional[Path] = None) -> Any:
        try:
            result = subprocess.run(
                [self.binary, *arguments, "--profile", self.profile],
                cwd=cwd,
                capture_output=True,
                text=True,
                check=False,
            )
        except OSError as exc:
            raise BridgeError("无法启动 lark-cli") from exc
        try:
            payload = json.loads(result.stdout)
        except json.JSONDecodeError as exc:
            raise BridgeError("lark-cli 未返回有效 JSON") from exc
        if not isinstance(payload, dict):
            raise BridgeError("lark-cli 返回了不支持的 JSON 结构")
        if result.returncode != 0 or payload.get("ok") is False:
            error = payload.get("error")
            message = error.get("message") if isinstance(error, dict) else None
            raise BridgeError(f"飞书 API 调用失败：{message or '未知错误'}")
        return payload

    def resolve_target(self, contact_name: str) -> LarkTarget:
        payload = self._run(
            [
                "contact",
                "+search-user",
                "--query",
                contact_name,
                "--has-chatted",
                "--as",
                "user",
                "--format",
                "json",
            ]
        )
        candidates: dict[str, dict[str, Any]] = {}
        for value in _walk_objects(payload):
            open_id = value.get("open_id")
            p2p_chat_id = value.get("p2p_chat_id")
            name = value.get("name") or value.get("localized_name")
            if not (
                isinstance(open_id, str)
                and isinstance(p2p_chat_id, str)
                and isinstance(name, str)
            ):
                continue
            if contact_name.casefold() not in name.casefold():
                continue
            candidates[open_id] = value
        if len(candidates) != 1:
            raise BridgeError(
                f"联系人 {contact_name} 必须唯一匹配，当前匹配 {len(candidates)} 个"
            )
        value = next(iter(candidates.values()))
        return LarkTarget(
            name=str(value.get("name") or value.get("localized_name")),
            sender_id=str(value["open_id"]),
            chat_id=str(value["p2p_chat_id"]),
        )

    def list_messages(self, chat_id: str) -> list[LarkMessage]:
        payload = self._run(
            [
                "im",
                "+chat-messages-list",
                "--chat-id",
                chat_id,
                "--order",
                "desc",
                "--page-size",
                "50",
                "--page-all",
                "--page-limit",
                "100",
                "--no-reactions",
                "--as",
                "user",
                "--format",
                "json",
            ]
        )
        return extract_lark_messages(payload)

    def download_image(
        self,
        *,
        message_id: str,
        image_key: str,
        output_directory: Path,
    ) -> Path:
        self._run(
            [
                "im",
                "+messages-resources-download",
                "--message-id",
                message_id,
                "--file-key",
                image_key,
                "--type",
                "image",
                "--output",
                "lark-image",
                "--as",
                "user",
                "--format",
                "json",
            ],
            cwd=output_directory,
        )
        files = [path for path in output_directory.iterdir() if path.is_file()]
        if len(files) != 1 or files[0].stat().st_size <= 0:
            raise BridgeError("飞书图片下载结果不完整")
        return files[0]


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


async def send_group_image(
    api: QQApiClient,
    http_client: httpx.AsyncClient,
    group_openid: str,
    image_path: Path,
) -> dict[str, Any]:
    try:
        uploader = MediaUploader(api, http_client, log_tag="LarkQQForwarder")
        file_info = await uploader.upload(
            "group",
            group_openid,
            str(image_path),
            MEDIA_TYPE_IMAGE,
            file_name="lark-image.jpg",
        )
        if not file_info:
            raise BridgeError("QQ 图片上传未返回 file_info")
        response = await api.post_group_message(
            group_openid,
            MessageToCreate(
                msg_type=QQMessageType.RICH_MEDIA,
                msg_seq=api.next_msg_seq(),
                media=MediaInfo(file_info=file_info),
            ),
        )
    except RuntimeError as exc:
        raise BridgeError("QQ 图片上传或发送失败") from exc
    if not response.get("id"):
        raise BridgeError("QQ 图片发送未返回消息 ID")
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
            "绑定成功。接下来会把 Perfecto 的新飞书消息转发到这个群。",
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
            "QQ 主动消息测试成功。下一条 Perfecto 飞书消息将由本机自动转发。",
        )
        print("QQ 主动测试消息发送成功。")
    finally:
        await http_client.aclose()


async def process_pending_messages(
    *,
    state: StateStore,
    lark: LarkClient,
    target: LarkTarget,
    api: QQApiClient,
    http_client: httpx.AsyncClient,
    group_openid: str,
) -> tuple[int, int]:
    cursor = state.message_position
    if cursor is None:
        raise BridgeError("飞书消息游标尚未初始化")
    messages = await asyncio.to_thread(lark.list_messages, target.chat_id)
    pending = pending_messages(messages, cursor)
    forwarded = 0

    for message in pending:
        if state.has_processed_message(message.message_id):
            state.advance_message(message.position, message.message_id)
            continue
        if message.sender_id != target.sender_id:
            state.advance_message(message.position, message.message_id)
            continue

        if message.msg_type == "text":
            if message.content.strip():
                await send_group_text(
                    api,
                    group_openid,
                    format_lark_text(target.name, message.content),
                )
                forwarded += 1
        elif message.msg_type == "image":
            image_key = extract_image_key(message.content)
            if not image_key:
                raise BridgeError("飞书图片消息缺少 image_key")
            with tempfile.TemporaryDirectory(prefix="lark-qq-image-") as directory:
                image_path = await asyncio.to_thread(
                    lark.download_image,
                    message_id=message.message_id,
                    image_key=image_key,
                    output_directory=Path(directory),
                )
                await send_group_image(
                    api,
                    http_client,
                    group_openid,
                    image_path,
                )
                forwarded += 1
        else:
            logging.info("跳过暂不支持的飞书消息类型：%s", message.msg_type)

        state.advance_message(message.position, message.message_id)

    return len(pending), forwarded


async def forward_forever(
    state: StateStore,
    input_path: Path,
    *,
    lark: LarkClient,
    target: LarkTarget,
    contact_name: str,
    poll_interval: float = 0.25,
) -> None:
    group_openid = state.group_openid
    if not group_openid:
        raise BridgeError("尚未绑定 QQ 群，请先运行 bind")
    state.assert_lark_target(
        chat_id=target.chat_id,
        sender_id=target.sender_id,
    )
    offset = state.prime_input(input_path)
    api, http_client = await create_api()
    print("飞书 → QQ 转发已启动；通知仅作唤醒，正文由飞书 API 获取。")

    try:
        while True:
            record = read_next_record(input_path, offset)
            if record is None:
                await asyncio.sleep(poll_interval)
                continue

            payload = record.payload
            if payload is None:
                offset = record.next_offset
                state.advance_input(offset)
                print("跳过一行无法解析的通知唤醒记录。")
                continue
            if not notification_matches_contact(payload, contact_name):
                offset = record.next_offset
                state.advance_input(offset)
                continue

            pending_count, forwarded_count = await process_pending_messages(
                state=state,
                lark=lark,
                target=target,
                api=api,
                http_client=http_client,
                group_openid=group_openid,
            )
            offset = record.next_offset
            state.advance_input(offset)
            print(
                f"飞书 API 检查完成：未处理 {pending_count} 条，"
                f"已转发 {forwarded_count} 条。"
            )
    finally:
        await http_client.aclose()


async def check_connections(lark: LarkClient, contact_name: str) -> None:
    target = await asyncio.to_thread(lark.resolve_target, contact_name)
    await asyncio.to_thread(lark.list_messages, target.chat_id)
    api, http_client = await create_api()
    try:
        await asyncio.to_thread(api.get_gateway_url_sync)
        print("飞书用户授权、目标会话、QQ 凭证和网关检查通过。")
    finally:
        await http_client.aclose()


async def prime_forwarder(
    *,
    state: StateStore,
    input_path: Path,
    lark: LarkClient,
    contact_name: str,
    force_end: bool,
) -> None:
    target = await asyncio.to_thread(lark.resolve_target, contact_name)
    messages = await asyncio.to_thread(lark.list_messages, target.chat_id)
    latest_position = max((message.position for message in messages), default=0)
    offset = state.prime_input(input_path, force_end=force_end)
    position = state.prime_lark(
        chat_id=target.chat_id,
        sender_id=target.sender_id,
        latest_position=latest_position,
        force_end=force_end,
    )
    print(f"转发起点已设置：通知字节位置 {offset}，飞书消息位置 {position}。")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="飞书 Perfecto 消息到 QQ 群转发器")
    parser.add_argument(
        "command", choices=["check", "prime", "bind", "test", "run"]
    )
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--state", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--rebind", action="store_true")
    parser.add_argument("--force-end", action="store_true")
    parser.add_argument("--lark-profile", default=DEFAULT_LARK_PROFILE)
    parser.add_argument("--lark-contact", default=DEFAULT_LARK_CONTACT)
    parser.add_argument("--lark-cli")
    return parser.parse_args()


async def async_main() -> None:
    args = parse_args()
    state = StateStore.load(args.state)
    lark = LarkClient(profile=args.lark_profile, binary=args.lark_cli)

    if args.command == "check":
        await check_connections(lark, args.lark_contact)
    elif args.command == "prime":
        await prime_forwarder(
            state=state,
            input_path=args.input,
            lark=lark,
            contact_name=args.lark_contact,
            force_end=args.force_end,
        )
    elif args.command == "bind":
        await bind_group(state, rebind=args.rebind)
    elif args.command == "test":
        await send_test(state)
    elif args.command == "run":
        target = await asyncio.to_thread(lark.resolve_target, args.lark_contact)
        await forward_forever(
            state,
            args.input,
            lark=lark,
            target=target,
            contact_name=args.lark_contact,
        )


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
