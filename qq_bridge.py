#!/usr/bin/env python3

from __future__ import annotations

import argparse
import asyncio
from datetime import datetime
import fcntl
import json
import logging
from logging.handlers import RotatingFileHandler
import os
import re
import shutil
import subprocess
import tempfile
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Optional

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
DEFAULT_CHANNEL_STATE = PROJECT_DIR / ".lark-channel-cursors.json"
DEFAULT_LARK_PROFILE = "tenant-105183"
DEFAULT_LARK_CONTACT = "Perfecto"
DEFAULT_LISTENERS = PROJECT_DIR / ".lark-listeners.json"
DEFAULT_LISTENER_CURSORS = PROJECT_DIR / ".lark-listener-cursors.json"
DEFAULT_PROCESS_LOCK = PROJECT_DIR / ".qq-forwarder.lock"
DEFAULT_METRICS = PROJECT_DIR / ".qq-forwarder-metrics.json"
DEFAULT_LOG = PROJECT_DIR / ".qq-forwarder.log"
STATE_SCHEMA_VERSION = 3
IMAGE_KEY_PATTERN = re.compile(r"\b(img_[A-Za-z0-9_-]{10,})\b")
LARK_COMMAND_TIMEOUT_SECONDS = 30.0
QQ_REQUEST_TIMEOUT_SECONDS = 30.0
MAX_SEND_ATTEMPTS = 3
SOURCE_QUEUE_SIZE = 4
DEFAULT_POLL_INTERVAL = 0.1
LOGGER = logging.getLogger("lark_qq_forwarder")


class BridgeError(RuntimeError):
    pass


async def run_bounded_source_tasks(tasks: list[Any], *, max_workers: int = SOURCE_QUEUE_SIZE) -> None:
    """用有界队列调度来源任务；单个来源异常不会取消其他来源。"""
    if not tasks:
        return
    worker_count = max(1, min(max_workers, len(tasks)))
    queue: asyncio.Queue[Any] = asyncio.Queue(maxsize=worker_count)

    async def worker() -> None:
        while True:
            task = await queue.get()
            try:
                await task
            except Exception as exc:
                logging.exception("来源同步任务出现未预期异常，已隔离该来源：%s", exc)
            finally:
                queue.task_done()

    workers = [asyncio.create_task(worker()) for _ in range(worker_count)]
    try:
        for task in tasks:
            await queue.put(task)
        await queue.join()
    finally:
        for worker_task in workers:
            worker_task.cancel()
        await asyncio.gather(*workers, return_exceptions=True)


class ForwarderProcessLock:
    """防止多个独立转发进程同时修改同一份游标和发送重复消息。"""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._handle: Optional[Any] = None

    def acquire(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        handle = self.path.open("a+", encoding="utf-8")
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            handle.close()
            raise BridgeError("已有另一个飞书到 QQ 转发进程正在运行") from exc
        handle.seek(0)
        handle.truncate()
        handle.write(f"pid={os.getpid()}\n")
        handle.flush()
        self._handle = handle

    def release(self) -> None:
        if self._handle is None:
            return
        try:
            fcntl.flock(self._handle.fileno(), fcntl.LOCK_UN)
        finally:
            self._handle.close()
            self._handle = None

    def __enter__(self) -> "ForwarderProcessLock":
        self.acquire()
        return self

    def __exit__(self, *_args: object) -> None:
        self.release()


def _save_json_atomically(path: Path, data: dict[str, Any]) -> None:
    """在进程锁内原子写入状态，避免并发实例互相覆盖游标。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = path.with_suffix(path.suffix + ".lock")
    with lock_path.open("a+") as lock_handle:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
        temp_path = path.with_suffix(path.suffix + ".tmp")
        with temp_path.open("w", encoding="utf-8") as temp_handle:
            temp_handle.write(json.dumps(data, ensure_ascii=False, indent=2) + "\n")
            temp_handle.flush()
            os.fsync(temp_handle.fileno())
        temp_path.replace(path)


class ForwarderMetrics:
    """保存不含正文和敏感标识的来源级延迟、吞吐和失败摘要。"""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.data: dict[str, Any] = {
            "schema_version": 1,
            "updated_at": None,
            "sources": {},
            "recent_syncs": [],
        }

    def record_sync(
        self,
        source: str,
        *,
        pending: int,
        forwarded: int,
        elapsed_ms: float,
        success: bool,
        error_class: Optional[str] = None,
    ) -> None:
        now = datetime.now().astimezone().isoformat(timespec="milliseconds")
        summary = self.data["sources"].setdefault(
            source,
            {"sync_count": 0, "success_count": 0, "failure_count": 0, "forwarded_count": 0, "last_pending": 0, "last_elapsed_ms": 0.0},
        )
        summary["sync_count"] += 1
        summary["success_count" if success else "failure_count"] += 1
        summary["forwarded_count"] += forwarded
        summary["last_pending"] = pending
        summary["last_elapsed_ms"] = round(elapsed_ms, 3)
        event = {
            "source": source,
            "at": now,
            "pending": pending,
            "forwarded": forwarded,
            "elapsed_ms": round(elapsed_ms, 3),
            "success": success,
        }
        if error_class:
            event["error_class"] = error_class
        recent = self.data["recent_syncs"]
        recent.append(event)
        self.data["recent_syncs"] = recent[-200:]
        self.data["updated_at"] = now
        try:
            _save_json_atomically(self.path, self.data)
        except OSError:
            logging.warning("转发指标写入失败，不影响消息转发：%s", self.path)


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
                data={
                    "schema_version": STATE_SCHEMA_VERSION,
                    "app_id": APP_ID,
                    "qq_groups": [],
                },
            )
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise BridgeError(f"无法读取转发状态文件：{path}") from exc
        if data.get("app_id") not in (None, APP_ID):
            raise BridgeError("状态文件属于另一个 QQ 机器人")
        data.pop("recent_fingerprints", None)
        # schema v2 只有一个 group_openid；迁移时保留旧字段，避免旧版本
        # 进程在回滚期间无法读取状态，同时建立可扩展的群绑定列表。
        groups = data.get("qq_groups")
        if not isinstance(groups, list):
            groups = []
        normalized_groups: list[dict[str, Any]] = []
        for group in groups:
            if not isinstance(group, dict):
                continue
            group_openid = group.get("group_openid")
            if not isinstance(group_openid, str) or not group_openid:
                continue
            normalized = dict(group)
            normalized.setdefault("binding_id", f"binding-{uuid.uuid4().hex}")
            normalized.setdefault("label", "QQ 群")
            normalized.setdefault("status", "active")
            normalized.setdefault("verification_state", "legacy")
            normalized_groups.append(normalized)

        legacy_group = data.get("group_openid")
        if (
            isinstance(legacy_group, str)
            and legacy_group
            and not any(
                group.get("group_openid") == legacy_group
                for group in normalized_groups
            )
        ):
            normalized_groups.insert(
                0,
                {
                    "binding_id": "binding-legacy-default",
                    "group_openid": legacy_group,
                    "label": "当前绑定群",
                    "status": "active",
                    "verification_state": "legacy",
                },
            )
        data["qq_groups"] = normalized_groups
        data["schema_version"] = STATE_SCHEMA_VERSION
        data["app_id"] = APP_ID
        return cls(path=path, data=data)

    @property
    def group_openid(self) -> Optional[str]:
        value = self.data.get("group_openid")
        if isinstance(value, str) and value:
            return value
        for group in self.group_bindings:
            if group.get("status") == "active":
                return group["group_openid"]
        return None

    @property
    def group_bindings(self) -> list[dict[str, Any]]:
        groups = self.data.get("qq_groups", [])
        return groups if isinstance(groups, list) else []

    def active_group_openids(self) -> list[str]:
        active = [
            group["group_openid"]
            for group in self.group_bindings
            if group.get("status") == "active"
            and isinstance(group.get("group_openid"), str)
            and group["group_openid"]
        ]
        if active:
            return active
        legacy = self.data.get("group_openid")
        return [legacy] if isinstance(legacy, str) and legacy else []

    def mark_delivery(self, source_name: str, group_openid: str, message_id: str) -> None:
        deliveries = self.data.setdefault("deliveries", {})
        if not isinstance(deliveries, dict):
            deliveries = {}
            self.data["deliveries"] = deliveries
        key = f"{source_name}:{group_openid}"
        ids = deliveries.get(key, [])
        if not isinstance(ids, list):
            ids = []
        if message_id not in ids:
            ids.append(message_id)
        deliveries[key] = ids[-500:]
        self.save()

    def has_delivery(self, source_name: str, group_openid: str, message_id: str) -> bool:
        deliveries = self.data.get("deliveries", {})
        if not isinstance(deliveries, dict):
            return False
        ids = deliveries.get(f"{source_name}:{group_openid}", [])
        return isinstance(ids, list) and message_id in ids

    @property
    def message_position(self) -> Optional[int]:
        value = self.data.get("lark_message_position")
        return value if isinstance(value, int) and value >= 0 else None

    def bind_group(
        self,
        group_openid: str,
        *,
        label: str = "QQ 群",
        binding_id: Optional[str] = None,
    ) -> None:
        if not isinstance(group_openid, str) or not group_openid:
            raise BridgeError("QQ 群标识不能为空")
        binding_id = binding_id or f"binding-{uuid.uuid4().hex}"
        groups = [
            group
            for group in self.group_bindings
            if group.get("group_openid") != group_openid
        ]
        groups.insert(
            0,
            {
                "binding_id": binding_id,
                "group_openid": group_openid,
                "label": label,
                "status": "active",
                "verification_state": "verified",
            },
        )
        # 当前阶段仍保持单目标兼容语义：新绑定成为唯一 active 群。
        for group in groups[1:]:
            if group.get("status") == "active":
                group["status"] = "disabled"
        self.data["qq_groups"] = groups
        self.data["group_openid"] = group_openid
        self.data["active_binding_id"] = binding_id
        self.save()

    def add_group_binding(self, group_openid: str, *, label: str = "QQ 群") -> None:
        if not isinstance(group_openid, str) or not group_openid:
            raise BridgeError("QQ 群标识不能为空")
        if any(group.get("group_openid") == group_openid for group in self.group_bindings):
            for group in self.group_bindings:
                if group.get("group_openid") == group_openid:
                    group["status"] = "active"
                    group["verification_state"] = "verified"
            self.save()
            return
        self.group_bindings.append(
            {
                "binding_id": f"binding-{uuid.uuid4().hex}",
                "group_openid": group_openid,
                "label": label,
                "status": "active",
                "verification_state": "verified",
            }
        )
        self.data["group_openid"] = group_openid
        self.save()

    def mark_group_verified(self, group_openid: str, *, ok: bool, error: str | None = None) -> None:
        for group in self.group_bindings:
            if group.get("group_openid") != group_openid:
                continue
            group["verification_state"] = "verified" if ok else "failed"
            group["status"] = "active" if ok else "failed"
            if ok:
                group.pop("last_failure", None)
            elif error:
                group["last_failure"] = error[:300]
            self.save()
            return

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
        _save_json_atomically(self.path, self.data)


def _valid_channel_position(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


class ListenerCursorStore:
    """为新增监听人员保存独立的飞书消息游标。"""

    def __init__(self, path: Path, data: dict[str, Any]) -> None:
        self.path, self.data = path, data

    @classmethod
    def load(cls, path: Path) -> "ListenerCursorStore":
        if not path.exists():
            return cls(path, {"schema_version": 1, "listeners": {}})
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise BridgeError("无法读取监听人员游标文件") from exc
        listeners = data.get("listeners") if isinstance(data, dict) else None
        return cls(path, {"schema_version": 1, "listeners": listeners if isinstance(listeners, dict) else {}})

    def get(self, name: str) -> dict[str, Any]:
        value = self.data["listeners"].setdefault(name, {"cursor": 0, "recent_message_ids": []})
        return value

    def initialize(self, target: LarkTarget, latest_position: int) -> None:
        value = self.get(target.name)
        if value.get("chat_id") != target.chat_id or value.get("sender_id") != target.sender_id:
            value.update({"chat_id": target.chat_id, "sender_id": target.sender_id, "cursor": latest_position, "recent_message_ids": []})
            self.save()

    def cursor(self, name: str) -> int:
        return int(self.get(name).get("cursor", 0))

    def has_processed(self, name: str, message_id: str) -> bool:
        return message_id in self.get(name).get("recent_message_ids", [])

    def advance(self, name: str, message: LarkMessage) -> None:
        value = self.get(name)
        value["cursor"] = max(int(value.get("cursor", 0)), message.position)
        recent = [item for item in value.get("recent_message_ids", []) if item != message.message_id]
        recent.append(message.message_id)
        value["recent_message_ids"] = recent[-200:]
        self.save()

    def save(self) -> None:
        _save_json_atomically(self.path, self.data)


@dataclass(frozen=True)
class ChannelCursor:
    name: str
    chat_id: str
    cursor_position: int
    initial_cursor_position: int
    recent_message_ids: tuple[str, ...]


class ChannelCursorStore:
    """保存每个飞书频道独立的消息位置，不保存消息正文。"""

    def __init__(self, path: Path, data: dict[str, Any]) -> None:
        self.path = path
        self.data = data

    @classmethod
    def load(cls, path: Path) -> "ChannelCursorStore":
        if not path.exists():
            raise BridgeError("多频道游标文件不存在，请先建立频道游标")
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise BridgeError("无法读取多频道游标文件") from exc
        if not isinstance(data, dict):
            raise BridgeError("多频道游标文件格式无效")

        channels = data.get("channels")
        if not isinstance(channels, list) or not channels:
            raise BridgeError("多频道游标文件没有可用频道")

        normalized: list[dict[str, Any]] = []
        names: set[str] = set()
        chat_ids: set[str] = set()
        for value in channels:
            if not isinstance(value, dict):
                raise BridgeError("多频道游标文件包含无效频道")
            name = value.get("name")
            chat_id = value.get("chat_id")
            cursor_position = value.get("cursor_position")
            initial_position = value.get(
                "initial_cursor_position", cursor_position
            )
            if (
                not isinstance(name, str)
                or not name.strip()
                or not isinstance(chat_id, str)
                or not chat_id.strip()
                or not _valid_channel_position(cursor_position)
                or not _valid_channel_position(initial_position)
                or name in names
                or chat_id in chat_ids
            ):
                raise BridgeError("多频道游标文件包含重复或无效频道")
            recent_ids = value.get("recent_message_ids", [])
            if not isinstance(recent_ids, list) or not all(
                isinstance(item, str) for item in recent_ids
            ):
                raise BridgeError("多频道游标文件包含无效消息记录")
            item = dict(value)
            item["initial_cursor_position"] = initial_position
            item["recent_message_ids"] = recent_ids[-200:]
            normalized.append(item)
            names.add(name)
            chat_ids.add(chat_id)

        data = dict(data)
        data["channels"] = normalized
        return cls(path=path, data=data)

    def names(self) -> list[str]:
        return [item["name"] for item in self.data["channels"]]

    def get(self, name: str) -> ChannelCursor:
        for value in self.data["channels"]:
            if value["name"] != name:
                continue
            recent_ids = value.get("recent_message_ids", [])
            return ChannelCursor(
                name=value["name"],
                chat_id=value["chat_id"],
                cursor_position=value["cursor_position"],
                initial_cursor_position=value["initial_cursor_position"],
                recent_message_ids=tuple(recent_ids),
            )
        raise BridgeError("指定频道未在游标配置中初始化")

    def has_processed_message(self, name: str, message_id: str) -> bool:
        return message_id in self.get(name).recent_message_ids

    def advance(self, name: str, position: int, message_id: str) -> None:
        for value in self.data["channels"]:
            if value["name"] != name:
                continue
            current = value["cursor_position"]
            value["cursor_position"] = max(current, position)
            recent_ids = value.get("recent_message_ids", [])
            recent_ids = [item for item in recent_ids if item != message_id]
            recent_ids.append(message_id)
            value["recent_message_ids"] = recent_ids[-200:]
            self.save()
            return
        raise BridgeError("指定频道未在游标配置中初始化")

    def save(self) -> None:
        _save_json_atomically(self.path, self.data)


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


def _normalize_notification_text(value: str) -> str:
    return " ".join(value.replace("～", "~").strip().casefold().split())


def notification_matches_contact(
    payload: dict[str, Any], contact_name: str
) -> bool:
    clue = _normalize_notification_text(contact_name)
    if not clue:
        return False
    values = [payload.get("title")]
    raw_texts = payload.get("raw_texts")
    if isinstance(raw_texts, list):
        values.extend(raw_texts)
    return any(
        clue in _normalize_notification_text(value)
        for value in values
        if isinstance(value, str)
    )


def format_lark_text(contact_name: str, content: str) -> str:
    """仅在原消息没有时间信息时补充时间和转发标识，避免重复套壳。"""
    text = content.strip()
    # 飞书历史消息常见的日期、时间格式：2026-09-02 10:15:53、10:15:53。
    has_timestamp = bool(
        re.search(r"\b(?:20\d{2}[-/]\d{1,2}[-/]\d{1,2}\s+)?\d{1,2}:\d{2}(?::\d{2})?\b", text)
        or re.search(r"\b20\d{2}[-/]\d{1,2}[-/]\d{1,2}\b", text)
    )
    if has_timestamp:
        return text
    timestamp = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S")
    return f"【飞书·{contact_name}】 {timestamp}\n{text}"


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
        started = time.perf_counter()
        operation = " ".join(arguments[:2])
        try:
            result = subprocess.run(
                [self.binary, *arguments, "--profile", self.profile],
                cwd=cwd,
                capture_output=True,
                text=True,
                check=False,
                timeout=LARK_COMMAND_TIMEOUT_SECONDS,
            )
        except subprocess.TimeoutExpired as exc:
            LOGGER.error("飞书 CLI 超时 operation=%s elapsed_ms=%.1f", operation, (time.perf_counter() - started) * 1000)
            raise BridgeError("飞书 CLI 请求超时") from exc
        except OSError as exc:
            LOGGER.error("飞书 CLI 启动失败 operation=%s elapsed_ms=%.1f error=%s", operation, (time.perf_counter() - started) * 1000, type(exc).__name__)
            raise BridgeError("无法启动 lark-cli") from exc
        try:
            payload = json.loads(result.stdout)
        except json.JSONDecodeError as exc:
            LOGGER.error("飞书 CLI 返回无效 JSON operation=%s elapsed_ms=%.1f", operation, (time.perf_counter() - started) * 1000)
            raise BridgeError("lark-cli 未返回有效 JSON") from exc
        if not isinstance(payload, dict):
            raise BridgeError("lark-cli 返回了不支持的 JSON 结构")
        if result.returncode != 0 or payload.get("ok") is False:
            error = payload.get("error")
            message = error.get("message") if isinstance(error, dict) else None
            LOGGER.error("飞书 CLI 失败 operation=%s elapsed_ms=%.1f error=%s", operation, (time.perf_counter() - started) * 1000, type(error).__name__ if error else "unknown")
            raise BridgeError(f"飞书 API 调用失败：{message or '未知错误'}")
        LOGGER.info("飞书 CLI 完成 operation=%s elapsed_ms=%.1f", operation, (time.perf_counter() - started) * 1000)
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

    @staticmethod
    def _pagination(payload: Any) -> tuple[bool, Optional[str]]:
        """兼容 lark-cli 顶层和 meta.pagination 两种分页返回结构。"""
        candidates = [payload]
        if isinstance(payload, dict):
            candidates.append(payload.get("meta"))
            candidates.append(payload.get("data"))
        for value in candidates:
            if not isinstance(value, dict):
                continue
            pagination = value.get("pagination") if isinstance(value.get("pagination"), dict) else value
            has_more = pagination.get("has_more")
            token = pagination.get("page_token") or pagination.get("next_token")
            if isinstance(has_more, bool):
                return has_more, token if isinstance(token, str) and token else None
        return False, None

    def list_messages_since(self, chat_id: str, cursor: int) -> list[LarkMessage]:
        """按分页游标倒序读取，读到本地高水位后立即停止。"""
        messages: list[LarkMessage] = []
        page_token: Optional[str] = None
        for _ in range(1000):
            arguments = [
                "im", "+chat-messages-list", "--chat-id", chat_id,
                "--order", "desc", "--page-size", "50",
                "--no-reactions", "--as", "user", "--format", "json",
            ]
            if page_token:
                arguments.extend(["--page-token", page_token])
            payload = self._run(arguments)
            page_messages = extract_lark_messages(payload)
            messages.extend(page_messages)
            if any(item.position <= cursor for item in page_messages):
                break
            has_more, next_token = self._pagination(payload)
            if not has_more or not next_token or next_token == page_token:
                break
            page_token = next_token
        return messages

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
        # lark-cli 可能同时落地元数据 JSON 和图片文件，不能用“目录恰好一个文件”判断成功。
        files = [
            path for path in output_directory.iterdir()
            if path.is_file() and path.stat().st_size > 0 and path.suffix.lower() not in {".json", ".jsonl"}
        ]
        if not files:
            raise BridgeError("飞书图片下载结果不完整")
        return max(files, key=lambda path: path.stat().st_size)


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
    client = httpx.AsyncClient(timeout=QQ_REQUEST_TIMEOUT_SECONDS)
    api = QQApiClient(app_id=APP_ID, client_secret=read_client_secret())
    api.setup(client)
    await api.ensure_token()
    return api, client


async def _retry_send(action: Callable[[], Any], *, description: str) -> Any:
    """对网络型发送做有限重试，避免瞬时故障直接拖垮整个转发进程。"""
    delay = 0.5
    for attempt in range(1, MAX_SEND_ATTEMPTS + 1):
        started = time.perf_counter()
        try:
            result = await action()
            LOGGER.info("QQ 操作成功 operation=%s attempt=%d elapsed_ms=%.1f", description, attempt, (time.perf_counter() - started) * 1000)
            return result
        except BridgeError as exc:
            LOGGER.warning("QQ 操作失败 operation=%s attempt=%d elapsed_ms=%.1f error=%s", description, attempt, (time.perf_counter() - started) * 1000, type(exc).__name__)
            if attempt >= MAX_SEND_ATTEMPTS:
                raise
            logging.warning(
                "%s失败，将在 %.1f 秒后重试（%d/%d）",
                description,
                delay,
                attempt,
                MAX_SEND_ATTEMPTS - 1,
            )
            await asyncio.sleep(delay)
            delay *= 2
    raise BridgeError(f"{description}失败")


async def send_group_text(
    api: QQApiClient,
    group_openid: str,
    content: str,
    *,
    reply_to: Optional[str] = None,
) -> dict[str, Any]:
    async def send_once() -> dict[str, Any]:
        try:
            response = await api.send_text(
                "group",
                group_openid,
                content,
                reply_to=reply_to,
                markdown=False,
            )
        except Exception as exc:
            raise BridgeError(
                f"QQ 消息发送失败，请检查目标群是否允许机器人主动发言：{exc}"
            ) from exc
        if not response.get("id"):
            raise BridgeError("QQ API 未返回消息 ID，无法确认消息已发送")
        return response

    return await _retry_send(send_once, description="QQ 文本消息发送")


async def send_group_image(
    api: QQApiClient,
    http_client: httpx.AsyncClient,
    group_openid: str,
    image_path: Path,
) -> dict[str, Any]:
    async def send_once() -> dict[str, Any]:
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
        except BridgeError:
            raise
        except Exception as exc:
            raise BridgeError(f"QQ 图片上传或发送失败：{exc}") from exc
        if not response.get("id"):
            raise BridgeError("QQ 图片发送未返回消息 ID")
        return response

    return await _retry_send(send_once, description="QQ 图片上传或发送")


async def bind_group(
    state: StateStore, *, rebind: bool = False, keep_existing: bool = False
) -> str:
    if state.group_openid and not rebind and not keep_existing:
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
        try:
            await send_group_text(
                api,
                event.chat_id,
                "绑定成功。接下来会把 Perfecto 的新飞书消息转发到这个群。",
                reply_to=event.message_id,
            )
        except BridgeError as exc:
            main_loop.call_soon_threadsafe(
                fatal_error.put_nowait, f"已收到 QQ 群消息，但确认回复失败：{exc}"
            )
            return
        if keep_existing:
            state.add_group_binding(event.chat_id)
        else:
            state.bind_group(event.chat_id)
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
        timeout_task = asyncio.create_task(asyncio.sleep(90))
        done, pending = await asyncio.wait(
            {bind_task, error_task, timeout_task}, return_when=asyncio.FIRST_COMPLETED
        )
        for task in pending:
            task.cancel()
        if error_task in done:
            raise BridgeError(error_task.result())
        if timeout_task in done:
            raise BridgeError("90 秒内未收到可识别的 QQ 群 @ 消息，请确认机器人已入群并开启群消息权限")
        if not state.group_openid:
            raise BridgeError("收到了群消息，但未能保存群标识")
        return state.group_openid
    finally:
        await websocket.async_stop()
        await http_client.aclose()


async def send_test(state: StateStore) -> None:
    group_openids = state.active_group_openids()
    if not group_openids:
        raise BridgeError("尚未绑定 QQ 群，请先运行 bind")
    api, http_client = await create_api()
    try:
        for group_openid in group_openids:
            try:
                await send_group_text(api, group_openid, "QQ 主动消息测试成功。下一条 Perfecto 飞书消息将由本机自动转发。")
            except BridgeError as exc:
                state.mark_group_verified(group_openid, ok=False, error=str(exc))
                raise
            state.mark_group_verified(group_openid, ok=True)
        print("QQ 主动测试消息发送成功。")
    finally:
        await http_client.aclose()


async def process_source_pending_messages(
    *,
    source_name: str,
    source_chat_id: str,
    source_sender_id: Optional[str],
    cursor: int,
    lark: LarkClient,
    api: QQApiClient,
    http_client: httpx.AsyncClient,
    group_openid: str | list[str],
    has_processed: Callable[[str], bool],
    advance: Callable[[LarkMessage], None],
    has_delivery: Optional[Callable[[str, str], bool]] = None,
    mark_delivery: Optional[Callable[[str, str], None]] = None,
) -> tuple[int, int]:
    group_openids = [group_openid] if isinstance(group_openid, str) else list(group_openid)
    if not group_openids:
        raise BridgeError("没有可用的 QQ 群绑定")
    list_since = getattr(lark, "list_messages_since", None)
    fetch_started = time.perf_counter()
    try:
        if callable(list_since):
            messages = await asyncio.to_thread(list_since, source_chat_id, cursor)
        else:
            messages = await asyncio.to_thread(lark.list_messages, source_chat_id)
        LOGGER.info("来源读取完成 source=%s message_count=%d elapsed_ms=%.1f", source_name, len(messages), (time.perf_counter() - fetch_started) * 1000)
    except BridgeError as exc:
        LOGGER.error("来源读取失败 source=%s elapsed_ms=%.1f error=%s", source_name, (time.perf_counter() - fetch_started) * 1000, type(exc).__name__)
        raise
    pending = pending_messages(messages, cursor)
    forwarded = 0

    for message in pending:
        if has_processed(message.message_id):
            advance(message)
            continue
        if source_sender_id is not None and message.sender_id != source_sender_id:
            advance(message)
            continue

        # 图片只需从飞书下载一次；QQ 的 file_info 仍按目标群分别上传。
        if message.msg_type == "image":
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
                for target_group in group_openids:
                    if has_delivery and has_delivery(target_group, message.message_id):
                        continue
                    await send_group_image(api, http_client, target_group, image_path)
                    forwarded += 1
                    if mark_delivery:
                        mark_delivery(target_group, message.message_id)
            advance(message)
            continue

        for target_group in group_openids:
            if has_delivery and has_delivery(target_group, message.message_id):
                continue
            if message.msg_type == "text":
                if message.content.strip():
                    await send_group_text(api, target_group, format_lark_text(source_name, message.content))
                    forwarded += 1
            else:
                logging.info("跳过暂不支持的飞书消息类型：%s", message.msg_type)
            if mark_delivery:
                mark_delivery(target_group, message.message_id)

        advance(message)

    return len(pending), forwarded


async def process_pending_messages(
    *,
    state: StateStore,
    lark: LarkClient,
    target: LarkTarget,
    api: QQApiClient,
    http_client: httpx.AsyncClient,
    group_openid: str | list[str],
) -> tuple[int, int]:
    cursor = state.message_position
    if cursor is None:
        raise BridgeError("飞书消息游标尚未初始化")
    return await process_source_pending_messages(
        source_name=target.name,
        source_chat_id=target.chat_id,
        source_sender_id=target.sender_id,
        cursor=cursor,
        lark=lark,
        api=api,
        http_client=http_client,
        group_openid=group_openid,
        has_processed=state.has_processed_message,
        has_delivery=lambda group, message_id: state.has_delivery(
            target.name, group, message_id
        ),
        mark_delivery=lambda group, message_id: state.mark_delivery(
            target.name, group, message_id
        ),
        advance=lambda message: state.advance_message(
            message.position, message.message_id
        ),
    )


async def process_channel_pending_messages(
    *,
    state: Optional[StateStore] = None,
    cursors: ChannelCursorStore,
    channel_name: str,
    lark: LarkClient,
    api: QQApiClient,
    http_client: httpx.AsyncClient,
    group_openid: str | list[str],
) -> tuple[int, int]:
    channel = cursors.get(channel_name)
    return await process_source_pending_messages(
        source_name=channel.name,
        source_chat_id=channel.chat_id,
        source_sender_id=None,
        cursor=channel.cursor_position,
        lark=lark,
        api=api,
        http_client=http_client,
        group_openid=group_openid,
        has_processed=lambda message_id: cursors.has_processed_message(
            channel.name, message_id
        ),
        advance=lambda message: cursors.advance(
            channel.name, message.position, message.message_id
        ),
        has_delivery=(
            (lambda group, message_id: state.has_delivery(channel.name, group, message_id))
            if state else None
        ),
        mark_delivery=(
            (lambda group, message_id: state.mark_delivery(channel.name, group, message_id))
            if state else None
        ),
    )


async def _forward_forever_impl(
    state: StateStore,
    input_path: Path,
    *,
    lark: LarkClient,
    target: LarkTarget,
    contact_name: str,
    poll_interval: float = DEFAULT_POLL_INTERVAL,
    channel_state_path: Path = DEFAULT_CHANNEL_STATE,
    listeners_path: Path = DEFAULT_LISTENERS,
    listener_cursors_path: Path = DEFAULT_LISTENER_CURSORS,
    metrics_path: Path = DEFAULT_METRICS,
) -> None:
    group_openids = state.active_group_openids()
    if not group_openids:
        raise BridgeError("尚未绑定 QQ 群，请先运行 bind")
    state.assert_lark_target(
        chat_id=target.chat_id,
        sender_id=target.sender_id,
    )
    listener_names = [contact_name]
    if listeners_path.exists():
        try:
            payload = json.loads(listeners_path.read_text(encoding="utf-8"))
            values = payload.get("listeners") if isinstance(payload, dict) else None
            if isinstance(values, list):
                listener_names = list(dict.fromkeys([value.strip() for value in values if isinstance(value, str) and value.strip()])) or [contact_name]
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            logging.warning("监听人员配置读取失败，将继续使用默认监听人员")
    extra_targets: dict[str, LarkTarget] = {}
    listener_store = ListenerCursorStore.load(listener_cursors_path)
    for name in listener_names:
        if name.casefold() == contact_name.casefold():
            continue
        if not hasattr(lark, "resolve_target"):
            logging.warning("当前飞书客户端不支持新增监听人员解析，跳过：%s", name)
            continue
        try:
            extra_targets[name] = await asyncio.to_thread(lark.resolve_target, name)
            messages = await asyncio.to_thread(lark.list_messages, extra_targets[name].chat_id)
            listener_store.initialize(extra_targets[name], max((item.position for item in messages), default=0))
        except BridgeError as exc:
            logging.error("监听人员 %s 初始化失败：%s", name, exc)
    channel_cursors = ChannelCursorStore.load(channel_state_path)
    offset = state.prime_input(input_path)
    api, http_client = await create_api()
    metrics = ForwarderMetrics(metrics_path)
    print(
        "飞书 → QQ 转发已启动；自动来源：Perfecto 和 "
        f"{len(channel_cursors.names())} 个频道；通知仅作唤醒。"
    )

    try:
        last_fallback_sync = time.monotonic()

        async def fallback_sync() -> None:
            """通知缺失时低频巡检所有来源，避免依赖 macOS 通知不丢消息。"""
            nonlocal last_fallback_sync
            for name, target_for_listener in extra_targets.items():
                try:
                    await process_source_pending_messages(
                        source_name=target_for_listener.name,
                        source_chat_id=target_for_listener.chat_id,
                        source_sender_id=target_for_listener.sender_id,
                        cursor=listener_store.cursor(name),
                        lark=lark, api=api, http_client=http_client,
                        group_openid=group_openids,
                        has_processed=lambda message_id, listener=name: listener_store.has_processed(listener, message_id),
                        advance=lambda message, listener=name: listener_store.advance(listener, message),
                        has_delivery=lambda group, message_id, listener=name: state.has_delivery(listener, group, message_id),
                        mark_delivery=lambda group, message_id, listener=name: state.mark_delivery(listener, group, message_id),
                    )
                except BridgeError as exc:
                    logging.error("兜底同步 %s 失败：%s", name, exc)
            for channel_name in channel_cursors.names():
                try:
                    await process_channel_pending_messages(
                        state=state, cursors=channel_cursors, channel_name=channel_name,
                        lark=lark, api=api, http_client=http_client,
                        group_openid=group_openids,
                    )
                except BridgeError as exc:
                    logging.error("兜底同步 %s 失败：%s", channel_name, exc)
            try:
                await process_pending_messages(
                    state=state, lark=lark, target=target, api=api,
                    http_client=http_client, group_openid=group_openids,
                )
            except BridgeError as exc:
                logging.error("兜底同步 Perfecto 失败：%s", exc)
            last_fallback_sync = time.monotonic()

        # 启动时先补扫新增监听人员的未处理消息，避免通知在旧进程中被消费后永久丢失。
        for name, target_for_listener in extra_targets.items():
            await process_source_pending_messages(
                source_name=target_for_listener.name,
                source_chat_id=target_for_listener.chat_id,
                source_sender_id=target_for_listener.sender_id,
                cursor=listener_store.cursor(name),
                lark=lark,
                api=api,
                http_client=http_client,
                group_openid=group_openids,
                has_processed=lambda message_id, listener=name: listener_store.has_processed(listener, message_id),
                advance=lambda message, listener=name: listener_store.advance(listener, message),
                has_delivery=lambda group, message_id, listener=name: state.has_delivery(listener, group, message_id),
                mark_delivery=lambda group, message_id, listener=name: state.mark_delivery(listener, group, message_id),
            )

        source_semaphore = asyncio.Semaphore(4)

        async def sync_perfecto() -> None:
            started = time.perf_counter()
            pending_count = forwarded_count = 0
            try:
                async with source_semaphore:
                    pending_count, forwarded_count = await process_pending_messages(
                        state=state, lark=lark, target=target, api=api,
                        http_client=http_client, group_openid=group_openids,
                    )
                print(f"Perfecto API 检查完成：未处理 {pending_count} 条，已转发 {forwarded_count} 条。")
                metrics.record_sync("Perfecto", pending=pending_count, forwarded=forwarded_count, elapsed_ms=(time.perf_counter() - started) * 1000, success=True)
            except BridgeError as exc:
                metrics.record_sync("Perfecto", pending=pending_count, forwarded=forwarded_count, elapsed_ms=(time.perf_counter() - started) * 1000, success=False, error_class=type(exc).__name__)
                logging.error("Perfecto 转发失败，本次唤醒稍后由兜底同步重试：%s", exc)

        async def sync_listener(name: str, target_for_listener: LarkTarget) -> None:
            started = time.perf_counter()
            pending_count = forwarded_count = 0
            try:
                async with source_semaphore:
                    pending_count, forwarded_count = await process_source_pending_messages(
                        source_name=target_for_listener.name,
                        source_chat_id=target_for_listener.chat_id,
                        source_sender_id=target_for_listener.sender_id,
                        cursor=listener_store.cursor(name), lark=lark, api=api,
                        http_client=http_client, group_openid=group_openids,
                        has_processed=lambda message_id, listener=name: listener_store.has_processed(listener, message_id),
                        advance=lambda message, listener=name: listener_store.advance(listener, message),
                        has_delivery=lambda group, message_id, listener=name: state.has_delivery(listener, group, message_id),
                        mark_delivery=lambda group, message_id, listener=name: state.mark_delivery(listener, group, message_id),
                    )
                print(f"{name} API 检查完成：未处理 {pending_count} 条，已转发 {forwarded_count} 条。")
                metrics.record_sync(name, pending=pending_count, forwarded=forwarded_count, elapsed_ms=(time.perf_counter() - started) * 1000, success=True)
            except BridgeError as exc:
                metrics.record_sync(name, pending=pending_count, forwarded=forwarded_count, elapsed_ms=(time.perf_counter() - started) * 1000, success=False, error_class=type(exc).__name__)
                logging.error("%s 转发失败，本次唤醒稍后由兜底同步重试：%s", name, exc)

        async def sync_channel(channel_name: str) -> None:
            started = time.perf_counter()
            pending_count = forwarded_count = 0
            try:
                async with source_semaphore:
                    pending_count, forwarded_count = await process_channel_pending_messages(
                        state=state, cursors=channel_cursors, channel_name=channel_name,
                        lark=lark, api=api, http_client=http_client,
                        group_openid=group_openids,
                    )
                print(f"{channel_name} API 检查完成：未处理 {pending_count} 条，已转发 {forwarded_count} 条。")
                metrics.record_sync(channel_name, pending=pending_count, forwarded=forwarded_count, elapsed_ms=(time.perf_counter() - started) * 1000, success=True)
            except BridgeError as exc:
                metrics.record_sync(channel_name, pending=pending_count, forwarded=forwarded_count, elapsed_ms=(time.perf_counter() - started) * 1000, success=False, error_class=type(exc).__name__)
                logging.error("%s 转发失败，本次唤醒稍后由兜底同步重试：%s", channel_name, exc)

        while True:
            record = read_next_record(input_path, offset)
            if record is None:
                if time.monotonic() - last_fallback_sync >= 30.0:
                    await fallback_sync()
                await asyncio.sleep(poll_interval)
                continue

            payload = record.payload
            if payload is None:
                offset = record.next_offset
                state.advance_input(offset)
                print("跳过一行无法解析的通知唤醒记录。")
                continue
            tasks: list[Any] = []
            if notification_matches_contact(payload, contact_name):
                tasks.append(sync_perfecto())
            for name, target_for_listener in extra_targets.items():
                if notification_matches_contact(payload, name):
                    tasks.append(sync_listener(name, target_for_listener))
            for channel_name in channel_cursors.names():
                if notification_matches_contact(payload, channel_name):
                    tasks.append(sync_channel(channel_name))
            if tasks:
                await run_bounded_source_tasks(tasks, max_workers=SOURCE_QUEUE_SIZE)
            offset = record.next_offset
            state.advance_input(offset)
    finally:
        await http_client.aclose()


async def forward_forever(*args: Any, **kwargs: Any) -> None:
    """在独占进程锁内运行转发器，避免独立会话并发发送和写游标。"""
    lock_path = kwargs.pop("process_lock_path", DEFAULT_PROCESS_LOCK)
    with ForwarderProcessLock(Path(lock_path)):
        await _forward_forever_impl(*args, **kwargs)


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
    parser.add_argument("--add", action="store_true", help="保留现有群并新增绑定")
    parser.add_argument("--force-end", action="store_true")
    parser.add_argument("--channel-state", type=Path, default=DEFAULT_CHANNEL_STATE)
    parser.add_argument("--lark-profile", default=DEFAULT_LARK_PROFILE)
    parser.add_argument("--lark-contact", default=DEFAULT_LARK_CONTACT)
    parser.add_argument("--listeners-file", type=Path, default=DEFAULT_LISTENERS)
    parser.add_argument("--listener-cursors", type=Path, default=DEFAULT_LISTENER_CURSORS)
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
        await bind_group(state, rebind=args.rebind, keep_existing=args.add)
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
            channel_state_path=args.channel_state,
            listeners_path=args.listeners_file,
            listener_cursors_path=args.listener_cursors,
        )


def main() -> None:
    log_format = "%(asctime)s %(levelname)s %(name)s: %(message)s"
    file_handler = RotatingFileHandler(
        DEFAULT_LOG, maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8"
    )
    file_handler.setFormatter(logging.Formatter(log_format))
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(logging.Formatter(log_format))
    logging.basicConfig(level=logging.INFO, handlers=[file_handler, stream_handler])
    LOGGER.info("转发进程启动 log_path=%s", DEFAULT_LOG)
    try:
        asyncio.run(async_main())
    except KeyboardInterrupt:
        print("已停止。")
    except BridgeError as exc:
        raise SystemExit(f"错误：{exc}") from exc


if __name__ == "__main__":
    main()
