#!/usr/bin/env python3

"""飞书到 QQ 转发器的本机控制面。

这个模块只负责进程生命周期、脱敏状态和安全事件，不参与 QQ WebSocket、
飞书消息读取或群绑定逻辑。所有子进程输出都丢弃，前端只消费结构化 JSON。
"""

from __future__ import annotations

import argparse
import asyncio
import collections
import datetime as dt
import hmac
import http.server
import json
import logging
import os
import secrets
import signal
import subprocess
import sys
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Optional
from urllib.parse import parse_qs, urlsplit


PROJECT_DIR = Path(__file__).resolve().parent
STORAGE_DIR_ENV = "LARK_QQ_STORAGE_DIR"
LOCAL_HOST = "127.0.0.1"
DEFAULT_PORT = 8765
EVENT_LIMIT = 80
LISTENER_FILE_NAME = ".lark-listeners.json"

ROLE_PROBE = "notification_probe"
ROLE_FORWARDER = "forwarder"
ROLE_PRIME = "prime"
ROLE_CHECK = "check"
ROLE_BIND = "bind"
ROLE_TEST = "test"
ROLE_REPLAY = "replay"

OP_BINDING = "binding"
OP_TEST = "test"
OP_PRIME = "prime"
OP_REPLAY = "replay"

ROLE_LABELS = {
    ROLE_PROBE: "飞书通知监听",
    ROLE_FORWARDER: "Python 转发任务",
}

STATE_LABELS = {
    "starting": "启动中",
    "running": "运行中",
    "stopping": "停止中",
    "stopped": "已停止",
    "degraded": "部分运行",
    "failed": "启动失败",
}

BINDING_STATE_LABELS = {
    "unbound": "未绑定",
    "bound": "已绑定",
    "binding": "绑定中",
    "cancelling": "取消中",
}

OPERATION_STATE_LABELS = {
    "idle": "尚未执行",
    "running": "进行中",
    "cancelling": "取消中",
    "succeeded": "成功",
    "failed": "失败",
    "cancelled": "已取消",
}


class ControlPlaneError(RuntimeError):
    """控制面可以安全展示给 UI 的错误。"""


class ListenerStore:
    """保存可由通知唤醒的飞书监听人员名称。"""

    def __init__(self, path: Path) -> None:
        self.path = path

    def names(self) -> list[str]:
        if not self.path.exists():
            return ["Perfecto"]
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            return ["Perfecto"]
        values = data.get("listeners") if isinstance(data, dict) else None
        names = [value.strip() for value in values if isinstance(value, str) and value.strip()] if isinstance(values, list) else []
        return list(dict.fromkeys(names)) or ["Perfecto"]

    def add(self, name: str) -> list[str]:
        name = name.strip()
        if not name or len(name) > 80:
            raise ControlPlaneError("监听人员名称不能为空且不能超过 80 个字符")
        names = self.names()
        if name.casefold() in {value.casefold() for value in names}:
            raise ControlPlaneError("该监听人员已存在")
        names.append(name)
        self.path.write_text(json.dumps({"listeners": names}, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        return names


class ActionConflict(ControlPlaneError):
    """当前状态不允许执行该动作。"""


class StartupFailure(ControlPlaneError):
    """启动流程失败，但不携带子进程原始输出。"""


class StartAborted(ControlPlaneError):
    """启动流程在完成前被停止请求取消。"""


class ConfirmationRequired(ControlPlaneError):
    """高风险控制面操作缺少明确确认。"""


class InvalidAction(ControlPlaneError):
    """请求参数不符合控制面允许的动作范围。"""


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds").replace(
        "+00:00", "Z"
    )


@dataclass(frozen=True)
class ControlPlaneConfig:
    project_dir: Path
    python_executable: Path
    probe_executable: Path
    bridge_script: Path
    replay_script: Path
    input_path: Path
    state_path: Path
    channel_state_path: Path
    listener_path: Optional[Path] = None
    profile: str = "tenant-105183"
    contact: str = "Perfecto"
    host: str = LOCAL_HOST
    port: int = DEFAULT_PORT

    def __post_init__(self) -> None:
        if self.listener_path is None:
            object.__setattr__(self, "listener_path", self.project_dir / LISTENER_FILE_NAME)
        if self.host != LOCAL_HOST:
            raise ValueError("控制面只能监听 127.0.0.1")
        if not 0 <= self.port <= 65535:
            raise ValueError("控制面端口无效")

    @classmethod
    def for_project(
        cls,
        project_dir: Path = PROJECT_DIR,
        *,
        port: int = DEFAULT_PORT,
    ) -> "ControlPlaneConfig":
        project_dir = project_dir.resolve()
        storage_dir = Path(os.environ.get(STORAGE_DIR_ENV, str(Path.home() / "Library/Application Support/lark-qq-forwarder"))).expanduser().resolve()
        venv_python = project_dir / ".venv" / "bin" / "python"
        python_executable = (
            venv_python if venv_python.is_file() else Path(sys.executable)
        )
        release_probe = project_dir / ".build" / "release" / "lark-notification-probe"
        debug_probe = project_dir / ".build" / "debug" / "lark-notification-probe"
        probe_executable = (
            release_probe if release_probe.exists() else debug_probe
        )
        return cls(
            project_dir=project_dir,
            python_executable=python_executable,
            probe_executable=probe_executable,
            bridge_script=project_dir / "qq_bridge.py",
            replay_script=project_dir / "channel_replay.py",
            input_path=storage_dir / "lark-notifications.jsonl",
            state_path=storage_dir / ".qq-forwarder-state.json",
            channel_state_path=storage_dir / ".lark-channel-cursors.json",
            listener_path=storage_dir / LISTENER_FILE_NAME,
            port=port,
        )

    def command(
        self,
        action: str,
        *,
        rebind: bool = False,
        keep_existing: bool = False,
        force_end: bool = False,
        channel: Optional[str] = None,
        message_ids: Optional[list[str]] = None,
    ) -> list[str]:
        common = [
            "--input",
            str(self.input_path),
            "--state",
            str(self.state_path),
            "--lark-profile",
            self.profile,
            "--lark-contact",
            self.contact,
        ]
        if action == "prime":
            command = [str(self.python_executable), str(self.bridge_script), "prime", *common]
            if force_end:
                command.append("--force-end")
            return command
        if action == "bind":
            command = [str(self.python_executable), str(self.bridge_script), "bind", *common]
            if rebind:
                command.append("--rebind")
            if keep_existing:
                command.append("--add")
            return command
        if action == "rename":
            if not channel:
                raise ValueError("群绑定 ID 不能为空")
            return [str(self.python_executable), str(self.bridge_script), "rename", *common, "--binding-id", channel]
        if action == "test":
            command = [str(self.python_executable), str(self.bridge_script), "test", *common]
            if channel:
                command.extend(["--binding-id", channel])
            return command
        if action == "replay":
            if not isinstance(channel, str) or not channel.strip():
                raise ValueError("补发频道不能为空")
            command = [
                str(self.python_executable),
                str(self.replay_script),
                "--channel",
                channel,
                "--channel-state",
                str(self.channel_state_path),
                "--state",
                str(self.state_path),
                "--lark-profile",
                self.profile,
            ]
            if message_ids:
                for message_id in message_ids:
                    command.extend(["--message-id", message_id])
            command.extend(["--progress-path", str(self.state_path.with_name(".replay-progress.json"))])
            return command
        if action == "run":
            return [
                str(self.python_executable),
                str(self.bridge_script),
                "run",
                *common,
                "--channel-state",
                str(self.channel_state_path),
                "--listeners-file",
                str(self.listener_path),
                "--listener-cursors",
                str(self.state_path.with_name(".lark-listener-cursors.json")),
            ]
        if action == "check":
            return [str(self.python_executable), str(self.bridge_script), "check", *common]
        if action == "probe":
            return [
                str(self.probe_executable),
                "--prompt-permission",
                "--output",
                str(self.input_path),
            ]
        raise ValueError(f"未知控制面动作：{action}")


@dataclass
class ProcessRecord:
    role: str
    process: Any
    started_at: str
    exit_code: Optional[int] = None
    finished_at: Optional[str] = None
    expected_exit: bool = False
    cancel_requested: bool = False


def _state_summary(path: Path) -> dict[str, Any]:
    """读取状态文件中的非敏感摘要，不把原始字段带到 API。"""

    if not path.exists():
        return {
            "state_file_available": False,
            "qq_group_bound": False,
            "lark_session_initialized": False,
            "message_cursor_initialized": False,
        }

    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return {
            "state_file_available": False,
            "state_file_valid": False,
            "qq_group_bound": False,
            "lark_session_initialized": False,
            "message_cursor_initialized": False,
        }

    if not isinstance(data, dict):
        return {
            "state_file_available": True,
            "state_file_valid": False,
            "qq_group_bound": False,
            "lark_session_initialized": False,
            "message_cursor_initialized": False,
        }

    group_openid = data.get("group_openid")
    groups = data.get("qq_groups", [])
    chat_id = data.get("lark_chat_id")
    sender_id = data.get("lark_sender_id")
    position = data.get("lark_message_position")
    recent_ids = data.get("recent_message_ids")
    return {
        "state_file_available": True,
        "state_file_valid": True,
        "qq_group_bound": isinstance(group_openid, str) and bool(group_openid),
        "qq_group_count": len(groups) if isinstance(groups, list) else (1 if group_openid else 0),
        "qq_groups": [
            {
                "binding_id": group.get("binding_id"),
                "label": group.get("label", "QQ 群"),
                "status": group.get("status", "unknown"),
                "verification_state": group.get("verification_state", "unknown"),
                # 仅展示短尾号，方便区分群，不泄露完整 openid。
                "display_id": group.get("group_openid", "")[-6:].upper(),
            }
            for group in groups if isinstance(group, dict) and isinstance(group.get("group_openid"), str)
        ] if isinstance(groups, list) else [],
        "lark_session_initialized": (
            isinstance(chat_id, str)
            and bool(chat_id)
            and isinstance(sender_id, str)
            and bool(sender_id)
        ),
        "message_cursor_initialized": isinstance(position, int) and position >= 0,
        "processed_message_count": (
            len(recent_ids) if isinstance(recent_ids, list) else 0
        ),
    }


def _metrics_summary(path: Path) -> dict[str, Any]:
    """读取转发指标摘要，不把消息正文或原始标识暴露给控制面。"""
    if not path.exists():
        return {"available": False, "updated_at": None, "sources": {}}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return {"available": False, "updated_at": None, "sources": {}}
    sources = data.get("sources") if isinstance(data, dict) else None
    if not isinstance(sources, dict):
        sources = {}
    safe_sources = {
        str(name): {
            "sync_count": int(value.get("sync_count", 0)),
            "success_count": int(value.get("success_count", 0)),
            "failure_count": int(value.get("failure_count", 0)),
            "forwarded_count": int(value.get("forwarded_count", 0)),
            "last_pending": int(value.get("last_pending", 0)),
            "last_elapsed_ms": float(value.get("last_elapsed_ms", 0.0)),
        }
        for name, value in sources.items()
        if isinstance(value, dict)
    }
    return {
        "available": True,
        "updated_at": data.get("updated_at") if isinstance(data.get("updated_at"), str) else None,
        "sources": safe_sources,
    }


def _channel_config_summary(path: Path) -> dict[str, Any]:
    """读取多频道配置摘要，只把名称和可用状态带到页面。"""

    empty = {
        "available": False,
        "baseline_at": None,
        "comparison": None,
        "channels": [],
    }
    if not path.exists():
        return empty
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return empty
    if not isinstance(data, dict) or not isinstance(data.get("channels"), list):
        return empty

    channels: list[dict[str, Any]] = []
    seen_names: set[str] = set()
    for value in data["channels"]:
        if not isinstance(value, dict):
            continue
        name = value.get("name")
        position = value.get("cursor_position")
        if (
            not isinstance(name, str)
            or not name.strip()
            or name in seen_names
        ):
            continue
        ready = isinstance(position, int) and not isinstance(position, bool) and position >= 0
        channels.append(
            {
                "name": name,
                "state": "ready" if ready else "invalid",
                "label": "游标已建立" if ready else "游标不可用",
            }
        )
        seen_names.add(name)

    return {
        "available": any(channel["state"] == "ready" for channel in channels),
        "baseline_at": data.get("baseline_at")
        if isinstance(data.get("baseline_at"), str)
        else None,
        "comparison": data.get("comparison")
        if isinstance(data.get("comparison"), str)
        else None,
        "channels": channels,
    }


class ProcessSupervisor:
    """只管理由当前控制面启动的子进程。"""

    def __init__(
        self,
        config: ControlPlaneConfig,
        *,
        process_factory: Optional[Callable[..., Any]] = None,
        clock: Callable[[], str] = utc_now,
    ) -> None:
        self.config = config
        self._process_factory = process_factory or subprocess.Popen
        self._clock = clock
        self._lock = threading.RLock()
        self._server_port = config.port
        self._listener_store = ListenerStore(config.listener_path)
        self._records: dict[str, ProcessRecord] = {}
        self._operation_records: dict[str, ProcessRecord] = {}
        self._operation_threads: dict[str, threading.Thread] = {}
        self._check_record: Optional[ProcessRecord] = None
        self._operation_state = "stopped"
        self._failure_message: Optional[str] = None
        self._generation = 0
        self._start_thread: Optional[threading.Thread] = None
        self._check_thread: Optional[threading.Thread] = None
        self._code_watch_paths = (self.config.bridge_script, self.config.replay_script)
        self._code_signature = self._code_signature_now()
        initial_runtime = _state_summary(config.state_path)
        initial_binding_state = (
            "bound" if initial_runtime.get("qq_group_bound") else "unbound"
        )
        self._operations: dict[str, dict[str, Any]] = {
            OP_BINDING: {
                "state": initial_binding_state,
                "label": BINDING_STATE_LABELS[initial_binding_state],
                "mode": None,
                "started_at": None,
                "completed_at": None,
                "failure_message": None,
                "effect": None,
            },
            OP_TEST: {
                "state": "idle",
                "label": OPERATION_STATE_LABELS["idle"],
                "mode": None,
                "started_at": None,
                "completed_at": None,
                "failure_message": None,
                "effect": None,
            },
            OP_PRIME: {
                "state": "idle",
                "label": OPERATION_STATE_LABELS["idle"],
                "mode": None,
                "started_at": None,
                "completed_at": None,
                "failure_message": None,
                "effect": None,
            },
            OP_REPLAY: {
                "state": "idle",
                "label": OPERATION_STATE_LABELS["idle"],
                "mode": None,
                "started_at": None,
                "completed_at": None,
                "failure_message": None,
                "effect": None,
            },
        }
        self._event_id = 0
        self._events: collections.deque[dict[str, Any]] = collections.deque(
            maxlen=EVENT_LIMIT
        )
        self._last_check: dict[str, Any] = {
            "state": "not_run",
            "label": "尚未执行",
            "completed_at": None,
        }
        self._input_offset = self._initial_input_offset()
        self._watch_stop = threading.Event()
        self._watch_thread: Optional[threading.Thread] = None
        self._record_event_locked(
            "control_plane_ready",
            "本机控制面已就绪",
        )

    def _initial_input_offset(self) -> int:
        try:
            return self.config.input_path.stat().st_size
        except OSError:
            return 0

    def _record_event_locked(
        self,
        event_type: str,
        message: str,
        *,
        level: str = "info",
        role: Optional[str] = None,
    ) -> None:
        self._event_id += 1
        event: dict[str, Any] = {
            "id": self._event_id,
            "occurred_at": self._clock(),
            "type": event_type,
            "level": level,
            "message": message,
        }
        if role is not None:
            event["role"] = role
        self._events.append(event)

    def _refresh_processes_locked(self) -> None:
        for record in self._records.values():
            if record.exit_code is not None:
                continue
            try:
                exit_code = record.process.poll()
            except Exception:
                exit_code = None
            if exit_code is None:
                continue
            record.exit_code = int(exit_code)
            record.finished_at = self._clock()
            if (
                record.role in ROLE_LABELS
                and not record.expected_exit
                and self._operation_state not in {"stopped", "stopping"}
            ):
                level = "error" if exit_code != 0 else "warning"
                if record.role == ROLE_PROBE:
                    message = "飞书通知监听已退出，请检查辅助功能权限后重启"
                    failure_message = "通知监听已退出，请检查 macOS 辅助功能权限后重启"
                else:
                    message = "转发任务已退出；发送失败不会推进游标，请检查后重启"
                    failure_message = (
                        "转发任务已退出；发送失败不会推进游标，请运行只读检查后重启"
                    )
                self._record_event_locked(
                    "process_exited",
                    message,
                    level=level,
                    role=record.role,
                )
                self._failure_message = failure_message
                if not self._active_public_records_locked():
                    self._operation_state = "failed"

    def _is_alive(self, record: Optional[ProcessRecord]) -> bool:
        if record is None or record.exit_code is not None:
            return False
        try:
            return record.process.poll() is None
        except Exception:
            return False

    def _active_public_records_locked(self) -> list[ProcessRecord]:
        return [
            record
            for role in (ROLE_PROBE, ROLE_FORWARDER)
            if (record := self._records.get(role)) is not None
            and self._is_alive(record)
        ]

    def _active_operation_records_locked(self) -> list[ProcessRecord]:
        return [
            record
            for record in self._operation_records.values()
            if self._is_alive(record)
        ]

    def _has_active_operations_locked(self) -> bool:
        return bool(self._active_operation_records_locked())

    def _has_active_records_locked(self, *, include_check: bool = True) -> bool:
        if self._active_public_records_locked():
            return True
        if self._is_alive(self._records.get(ROLE_PRIME)):
            return True
        if self._has_active_operations_locked():
            return True
        return include_check and self._is_alive(self._check_record)

    def _overall_state_locked(self) -> str:
        if self._operation_state in {"starting", "stopping"}:
            return self._operation_state

        public_records = self._active_public_records_locked()
        if len(public_records) == 2:
            return "running"
        if len(public_records) == 1:
            return "degraded"
        if self._operation_state == "failed":
            return "failed"
        return "stopped"

    def _process_snapshot_locked(self, role: str) -> dict[str, Any]:
        record = self._records.get(role)
        state = "stopped"
        pid: Optional[int] = None
        exit_code: Optional[int] = None
        started_at: Optional[str] = None
        finished_at: Optional[str] = None

        if self._operation_state == "starting" and record is None:
            state = "starting"
        if record is not None:
            started_at = record.started_at
            exit_code = record.exit_code
            finished_at = record.finished_at
            if self._is_alive(record):
                state = "running"
                try:
                    pid = int(record.process.pid)
                except (AttributeError, TypeError, ValueError):
                    pid = None
            elif record.exit_code is not None:
                state = (
                    "stopped"
                    if record.expected_exit
                    else ("failed" if record.exit_code != 0 else "stopped")
                )
        if self._operation_state == "stopping" and state == "running":
            state = "stopping"

        return {
            "role": role,
            "label": ROLE_LABELS[role],
            "state": state,
            "label_for_state": STATE_LABELS.get(state, state),
            "pid": pid,
            "started_at": started_at,
            "finished_at": finished_at,
            "exit_code": exit_code,
        }

    def _sync_binding_state_locked(self) -> None:
        # 绑定结果以状态文件为准；绑定进程仍在运行时保留“绑定中”，避免把
        # 尚未完成的 WebSocket 交互误报成已完成。
        if self._operation_records.get(OP_BINDING) is not None:
            return
        runtime = _state_summary(self.config.state_path)
        state = "bound" if runtime.get("qq_group_bound") else "unbound"
        operation = self._operations[OP_BINDING]
        operation["state"] = state
        operation["label"] = BINDING_STATE_LABELS[state]

    def _begin_operation_locked(self, operation_name: str, mode: str) -> None:
        operation = self._operations[operation_name]
        state = "binding" if operation_name == OP_BINDING else "running"
        operation.update(
            {
                "state": state,
                "label": (
                    BINDING_STATE_LABELS[state]
                    if operation_name == OP_BINDING
                    else OPERATION_STATE_LABELS[state]
                ),
                "mode": mode,
                "started_at": self._clock(),
                "completed_at": None,
                "failure_message": None,
                "effect": None,
            }
        )

    def _finish_operation_locked(
        self,
        operation_name: str,
        state: str,
        *,
        failure_message: Optional[str] = None,
        effect: Optional[str] = None,
    ) -> None:
        operation = self._operations[operation_name]
        labels = BINDING_STATE_LABELS if operation_name == OP_BINDING else OPERATION_STATE_LABELS
        operation.update(
            {
                "state": state,
                "label": labels[state],
                "completed_at": self._clock(),
                "failure_message": failure_message,
                "effect": effect,
            }
        )

    def _operation_snapshot_locked(self, operation_name: str) -> dict[str, Any]:
        return dict(self._operations[operation_name])

    def _recovery_snapshot_locked(
        self,
        overall_state: str,
        runtime: dict[str, Any],
    ) -> dict[str, Any]:
        process_states = {
            role: self._process_snapshot_locked(role)["state"]
            for role in (ROLE_PROBE, ROLE_FORWARDER)
        }
        binding = self._operations[OP_BINDING]
        test = self._operations[OP_TEST]
        prime = self._operations[OP_PRIME]
        replay = self._operations[OP_REPLAY]
        hints: list[str] = []

        if overall_state == "failed":
            hints.append("启动流程未完成：先运行只读检查，再重试启动或重启。")
        if process_states[ROLE_PROBE] == "failed":
            hints.append("通知监听未运行：请检查 macOS 辅助功能权限，然后重启。")
        if process_states[ROLE_FORWARDER] == "failed":
            hints.append("转发任务已退出；发送失败不会推进游标，请检查后重启。")
        if overall_state == "degraded" and self._failure_message:
            hints.append(self._failure_message)
        if binding["state"] == "unbound":
            hints.append("尚未绑定 QQ 群：开始绑定后，在目标群发送 @qclaw 绑定测试。")
        if binding.get("failure_message"):
            hints.append("绑定未完成：请检查 QQ 凭证和群权限后重试。")
        if test["state"] == "failed":
            hints.append("主动消息测试失败：请检查绑定和 QQ 群主动发言权限后重试。")
        if prime["state"] == "failed":
            hints.append("prime 未完成：请运行只读检查确认飞书授权和目标会话后重试。")
        if replay["state"] == "failed":
            hints.append("指定频道补发失败：已成功发送的消息保持记录，请检查后重试。")
        if not runtime.get("message_cursor_initialized"):
            hints.append("飞书消息游标尚未初始化：停止转发任务后执行默认 prime。")
        if not hints:
            hints.append("暂无需要处理的异常；发送失败时游标不会前进。")

        return {
            "hints": hints[:6],
            "actions": [
                {
                    "id": "check",
                    "label": "运行只读检查",
                    "available": self._check_record is None
                    or not self._is_alive(self._check_record),
                },
                {
                    "id": "restart",
                    "label": "重启转发",
                    "available": overall_state not in {"starting", "stopping"},
                },
            ],
        }

    def _status_locked(self) -> dict[str, Any]:
        self._sync_binding_state_locked()
        overall_state = self._overall_state_locked()
        runtime = _state_summary(self.config.state_path)
        runtime.update(
            {
                "notification_log_available": self.config.input_path.exists(),
                "probe_available": self.config.probe_executable.is_file(),
                "python_available": self.config.python_executable.is_file(),
                "bridge_available": self.config.bridge_script.is_file(),
            }
        )
        binding = self._operation_snapshot_locked(OP_BINDING)
        test = self._operation_snapshot_locked(OP_TEST)
        prime = self._operation_snapshot_locked(OP_PRIME)
        replay = self._operation_snapshot_locked(OP_REPLAY)
        channel_replay = _channel_config_summary(self.config.channel_state_path)
        progress_path = self.config.state_path.with_name(".replay-progress.json")
        replay_progress: dict[str, Any] = {}
        if progress_path.exists():
            try:
                value = json.loads(progress_path.read_text(encoding="utf-8"))
                if isinstance(value, dict):
                    replay_progress = value
            except (OSError, UnicodeDecodeError, json.JSONDecodeError):
                replay_progress = {}
        ready_channel_count = sum(
            channel["state"] == "ready" for channel in channel_replay["channels"]
        )
        runtime.update(
            {
                "channel_forwarding_available": channel_replay["available"],
                "channel_forwarding_count": ready_channel_count,
                "forwarder_metrics": _metrics_summary(
                    self.config.state_path.with_name(".qq-forwarder-metrics.json")
                ),
            }
        )
        return {
            # 只增量扩展字段，保留版本号以兼容现有读取方。
            "schema_version": 1,
            "service": {
                "state": "ready",
                "host": LOCAL_HOST,
                "port": self._server_port,
            },
            "overall": {
                "state": overall_state,
                "label": STATE_LABELS[overall_state],
                "failure_message": self._failure_message,
            },
            "processes": [
                self._process_snapshot_locked(ROLE_PROBE),
                self._process_snapshot_locked(ROLE_FORWARDER),
            ],
            "runtime": runtime,
            "check": dict(self._last_check),
            "operations": {
                "binding": binding,
                "test": test,
                "prime": prime,
                "replay": replay,
            },
            # 顶层别名让页面和本机脚本都能直接读取核心操作状态；内容与
            # operations 完全一致，避免任何一方自行解析终端输出。
            "binding": binding,
            "test": test,
            "prime": prime,
            "replay": replay,
            "channel_replay": channel_replay,
            "replay_progress": replay_progress,
            "listeners": self._listener_store.names(),
            "recovery": self._recovery_snapshot_locked(overall_state, runtime),
            "events": list(self._events),
        }

    def status(self) -> dict[str, Any]:
        with self._lock:
            self._refresh_processes_locked()
            return self._status_locked()

    def listeners(self) -> list[str]:
        with self._lock:
            return self._listener_store.names()

    def add_listener(self, name: str) -> list[str]:
        with self._lock:
            names = self._listener_store.add(name)
            self._record_event_locked("listener_added", f"已新增监听人员：{name.strip()}")
            return names

    def set_server_port(self, port: int) -> None:
        with self._lock:
            self._server_port = port

    def _spawn(self, role: str, command: list[str]) -> ProcessRecord:
        try:
            process = self._process_factory(
                command,
                cwd=str(self.config.project_dir),
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                start_new_session=True,
            )
        except FileNotFoundError as exc:
            raise StartupFailure("启动所需的本地程序不存在") from exc
        except PermissionError as exc:
            raise StartupFailure("启动所需的本地程序没有执行权限") from exc
        except OSError as exc:
            raise StartupFailure("无法启动本地子进程") from exc
        return ProcessRecord(role=role, process=process, started_at=self._clock())

    def _generation_active_locked(self, generation: int) -> bool:
        return generation == self._generation and self._operation_state == "starting"

    def _launch_public(self, role: str, command: list[str], generation: int) -> None:
        with self._lock:
            if not self._generation_active_locked(generation):
                raise StartAborted("启动已取消")
            record = self._spawn(role, command)
            self._records[role] = record
            self._record_event_locked(
                "process_started",
                f"{ROLE_LABELS[role]}已启动",
                role=role,
            )

    def _ensure_action_available_locked(self, *, requires_stopped: bool = False) -> None:
        if self._operation_state in {"starting", "stopping"}:
            raise ActionConflict("当前已有启动或停止操作正在进行")
        if self._has_active_operations_locked():
            raise ActionConflict("当前已有绑定、测试、prime 或频道补发操作正在进行")
        if requires_stopped and self._active_public_records_locked():
            raise ActionConflict("请先停止转发任务，再执行此操作")

    def _start_operation_locked(
        self,
        *,
        operation_name: str,
        role: str,
        mode: str,
        command: list[str],
        requested_event: str,
        requested_message: str,
        start_failure_message: str,
    ) -> dict[str, Any]:
        self._begin_operation_locked(operation_name, mode)
        self._record_event_locked(requested_event, requested_message)
        try:
            record = self._spawn(role, command)
        except ControlPlaneError:
            self._finish_operation_locked(
                operation_name,
                "failed",
                failure_message=start_failure_message,
            )
            self._record_event_locked(
                f"{operation_name}_failed",
                start_failure_message,
                level="error",
            )
            return self._status_locked()

        self._operation_records[operation_name] = record
        thread = threading.Thread(
            target=self._operation_worker,
            args=(operation_name, record, mode),
            name=f"lark-qq-{operation_name}",
            daemon=True,
        )
        self._operation_threads[operation_name] = thread
        thread.start()
        return self._status_locked()

    def _operation_worker(
        self,
        operation_name: str,
        record: ProcessRecord,
        mode: str,
    ) -> None:
        try:
            raw_exit_code = record.process.wait()
            exit_code = int(raw_exit_code) if raw_exit_code is not None else 1
        except Exception:
            exit_code = 1

        with self._lock:
            if self._operation_records.get(operation_name) is not record:
                return
            record.exit_code = exit_code
            record.finished_at = self._clock()
            self._operation_records.pop(operation_name, None)
            self._operation_threads.pop(operation_name, None)
            cancelled = record.expected_exit or record.cancel_requested

            if operation_name == OP_BINDING:
                is_bound = bool(_state_summary(self.config.state_path).get("qq_group_bound"))
                if cancelled:
                    state = "bound" if is_bound else "unbound"
                    self._finish_operation_locked(OP_BINDING, state)
                    self._record_event_locked(
                        "bind_cancelled",
                        "绑定已取消，当前绑定保持不变",
                        level="warning",
                    )
                elif exit_code == 0 and is_bound:
                    self._finish_operation_locked(
                        OP_BINDING,
                        "bound",
                        effect="QQ 群绑定已保存",
                    )
                    self._record_event_locked("bind_succeeded", "QQ 群绑定完成")
                else:
                    self._finish_operation_locked(
                        OP_BINDING,
                        "bound" if is_bound else "unbound",
                        failure_message="绑定未完成，请检查 QQ 凭证和群权限后重试",
                    )
                    self._record_event_locked(
                        "bind_failed",
                        "绑定未完成，请检查 QQ 凭证和群权限后重试",
                        level="error",
                    )
                return

            if operation_name == OP_TEST:
                if cancelled:
                    self._finish_operation_locked(OP_TEST, "cancelled")
                    self._record_event_locked(
                        "test_cancelled",
                        "QQ 主动消息测试已取消",
                        level="warning",
                    )
                elif exit_code == 0:
                    self._finish_operation_locked(
                        OP_TEST,
                        "succeeded",
                        effect="测试消息已发送到已绑定 QQ 群",
                    )
                    self._record_event_locked("test_succeeded", "QQ 主动消息测试成功")
                else:
                    message = (
                        "QQ 主动消息测试失败，请检查绑定和群主动发言权限后重试"
                    )
                    self._finish_operation_locked(
                        OP_TEST,
                        "failed",
                        failure_message=message,
                    )
                    self._record_event_locked("test_failed", message, level="error")
                return

            if operation_name == OP_PRIME:
                if cancelled:
                    self._finish_operation_locked(OP_PRIME, "cancelled")
                    self._record_event_locked(
                        "prime_cancelled",
                        "prime 已取消，游标未被本次操作改动",
                        level="warning",
                    )
                elif exit_code == 0:
                    effect = (
                        "已放弃当前未处理消息，并从最新位置开始；不会补发历史消息"
                        if mode == "force_end"
                        else "默认 prime 已完成；保留已有游标，首次初始化不补发历史消息"
                    )
                    self._finish_operation_locked(
                        OP_PRIME,
                        "succeeded",
                        effect=effect,
                    )
                    self._record_event_locked("prime_succeeded", effect)
                else:
                    message = (
                        "prime 未完成，请运行只读检查确认飞书授权和目标会话后重试"
                    )
                    self._finish_operation_locked(
                        OP_PRIME,
                        "failed",
                        failure_message=message,
                    )
                    self._record_event_locked("prime_failed", message, level="error")

            if operation_name == OP_REPLAY:
                if cancelled:
                    self._finish_operation_locked(OP_REPLAY, "cancelled")
                    self._record_event_locked(
                        "replay_cancelled",
                        "指定频道补发已取消，已成功发送的消息保持记录",
                        level="warning",
                    )
                elif exit_code == 0:
                    self._finish_operation_locked(
                        OP_REPLAY,
                        "succeeded",
                        effect="指定频道补发已完成",
                    )
                    self._record_event_locked(
                        "replay_succeeded",
                        f"频道「{mode}」补发已完成",
                    )
                else:
                    message = "指定频道补发失败，已成功发送的消息保持记录，请检查后重试"
                    self._finish_operation_locked(
                        OP_REPLAY,
                        "failed",
                        failure_message=message,
                    )
                    self._record_event_locked(
                        "replay_failed",
                        message,
                        level="error",
                    )

    def bind(
        self,
        *,
        rebind: bool = False,
        keep_existing: bool = False,
        confirmed: bool = False,
    ) -> dict[str, Any]:
        if rebind and not confirmed:
            raise ConfirmationRequired("重新绑定前必须明确确认")
        with self._lock:
            self._refresh_processes_locked()
            self._sync_binding_state_locked()
            if not rebind and not keep_existing and self._operations[OP_BINDING]["state"] == "bound":
                self._record_event_locked("bind_skipped", "当前 QQ 群已经绑定")
                return self._status_locked()
            self._ensure_action_available_locked(requires_stopped=True)
            mode = "add" if keep_existing else ("rebind" if rebind else "bind")
            return self._start_operation_locked(
                operation_name=OP_BINDING,
                role=ROLE_BIND,
                mode=mode,
                command=self.config.command("bind", rebind=rebind, keep_existing=keep_existing),
                requested_event="bind_requested",
                requested_message=(
                    "已接受新增群绑定请求；请在目标 QQ 群发送 @qclaw 绑定测试"
                    if keep_existing
                    else "已接受重新绑定请求；请在目标 QQ 群发送 @qclaw 绑定测试"
                    if rebind
                    else "已接受绑定请求；请在目标 QQ 群发送 @qclaw 绑定测试"
                ),
                start_failure_message="绑定进程未能启动，请检查本机 Python 环境后重试",
            )

    def remove_group(self, binding_id: str, *, confirmed: bool = False) -> dict[str, Any]:
        if not confirmed:
            raise ConfirmationRequired("删除 QQ 群绑定前必须明确确认")
        if not isinstance(binding_id, str) or not binding_id.strip():
            raise InvalidAction("QQ 群绑定参数无效")
        with self._lock:
            self._refresh_processes_locked()
            self._ensure_action_available_locked(requires_stopped=True)
            from qq_bridge import StateStore
            state = StateStore.load(self.config.state_path)
            state.remove_group_binding(binding_id.strip())
            self._record_event_locked("group_removed", "已删除一个 QQ 群绑定")
            self._sync_binding_state_locked()
            return self._status_locked()

    def update_group_label(self, binding_id: str, label: str) -> dict[str, Any]:
        if not isinstance(binding_id, str) or not binding_id.strip() or not isinstance(label, str):
            raise InvalidAction("QQ 群备注参数无效")
        with self._lock:
            self._refresh_processes_locked()
            self._ensure_action_available_locked(requires_stopped=True)
            from qq_bridge import StateStore
            state = StateStore.load(self.config.state_path)
            state.update_group_label(binding_id.strip(), label)
            self._record_event_locked("group_label_updated", "已更新 QQ 群备注")
            self._sync_binding_state_locked()
            return self._status_locked()

    def rename_group(self, binding_id: str) -> dict[str, Any]:
        if not isinstance(binding_id, str) or not binding_id.strip():
            raise InvalidAction("QQ 群绑定参数无效")
        with self._lock:
            self._refresh_processes_locked()
            self._ensure_action_available_locked(requires_stopped=True)
            return self._start_operation_locked(
                operation_name=OP_BINDING,
                role=ROLE_BIND,
                mode="rename",
                command=self.config.command("rename", channel=binding_id.strip()),
                requested_event="group_rename_requested",
                requested_message="已进入群备注等待状态，请在目标 QQ 群 @Bot 发送群备注",
                start_failure_message="群备注等待进程未能启动，请检查本机 Python 环境后重试",
            )

    def cancel_bind(self) -> dict[str, Any]:
        with self._lock:
            record = self._operation_records.get(OP_BINDING)
            if record is None or not self._is_alive(record):
                raise ActionConflict("当前没有进行中的绑定操作")
            record.cancel_requested = True
            record.expected_exit = True
            operation = self._operations[OP_BINDING]
            operation["state"] = "cancelling"
            operation["label"] = BINDING_STATE_LABELS["cancelling"]
            self._record_event_locked("bind_cancel_requested", "已接受取消绑定请求")
            threading.Thread(
                target=self._terminate,
                args=(record,),
                name="lark-qq-bind-cancel",
                daemon=True,
            ).start()
            return self._status_locked()

    def test(self, binding_id: Optional[str] = None) -> dict[str, Any]:
        with self._lock:
            self._refresh_processes_locked()
            self._ensure_action_available_locked()
            return self._start_operation_locked(
                operation_name=OP_TEST,
                role=ROLE_TEST,
                mode="message",
                command=self.config.command("test", channel=binding_id),
                requested_event="test_requested",
                requested_message=("已接受 QQ 主动消息测试请求，将向指定 QQ 群发送测试消息" if binding_id else "已接受 QQ 主动消息测试请求，将向活跃 QQ 群发送测试消息"),
                start_failure_message="测试进程未能启动，请检查本机 Python 环境后重试",
            )

    def prime(
        self,
        *,
        force_end: bool = False,
        confirmed: bool = False,
    ) -> dict[str, Any]:
        if force_end and not confirmed:
            raise ConfirmationRequired("force-end 前必须明确确认")
        with self._lock:
            self._refresh_processes_locked()
            self._ensure_action_available_locked(requires_stopped=True)
            mode = "force_end" if force_end else "normal"
            return self._start_operation_locked(
                operation_name=OP_PRIME,
                role=ROLE_PRIME,
                mode=mode,
                command=self.config.command("prime", force_end=force_end),
                requested_event="prime_requested",
                requested_message=(
                    "已接受 force-end 请求，将放弃当前未处理消息并从最新位置开始"
                    if force_end
                    else "已接受默认 prime 请求，不补发历史消息"
                ),
                start_failure_message="prime 进程未能启动，请检查本机 Python 环境后重试",
            )

    def replay(self, channel_name: str, message_ids: Optional[list[str]] = None) -> dict[str, Any]:
        if not isinstance(channel_name, str) or not channel_name.strip():
            raise InvalidAction("请选择要补发的频道")
        with self._lock:
            self._refresh_processes_locked()
            channel_summary = _channel_config_summary(self.config.channel_state_path)
            available_names = {
                channel["name"]
                for channel in channel_summary["channels"]
                if channel["state"] == "ready"
            }
            if channel_name not in available_names:
                raise InvalidAction("指定频道未配置或游标不可用")
            self._ensure_action_available_locked(requires_stopped=True)
            return self._start_operation_locked(
                operation_name=OP_REPLAY,
                role=ROLE_REPLAY,
                mode=channel_name,
                command=self.config.command("replay", channel=channel_name, message_ids=message_ids),
                requested_event="replay_requested",
                requested_message=f"已接受频道「{channel_name}」补发请求",
                start_failure_message="补发进程未能启动，请检查本机 Python 环境后重试",
            )

    def preview_replay(self, channel_name: str) -> dict[str, Any]:
        channel_summary = _channel_config_summary(self.config.channel_state_path)
        if channel_name not in {item["name"] for item in channel_summary["channels"] if item["state"] == "ready"}:
            raise InvalidAction("指定频道未配置或游标不可用")
        from channel_replay import preview_channel
        items = asyncio.run(preview_channel(channel_name=channel_name, channel_state_path=self.config.channel_state_path, lark_profile=self.config.profile))
        return {"channel": channel_name, "items": items, "count": len(items)}

    def cancel_replay(self) -> dict[str, Any]:
        with self._lock:
            record = self._operation_records.get(OP_REPLAY)
            if record is None or not self._is_alive(record):
                raise ActionConflict("当前没有进行中的频道补发操作")
            record.cancel_requested = True
            record.expected_exit = True
            operation = self._operations[OP_REPLAY]
            operation["state"] = "cancelling"
            operation["label"] = OPERATION_STATE_LABELS["cancelling"]
            self._record_event_locked("replay_cancel_requested", "已接受取消补发请求")
            threading.Thread(
                target=self._terminate,
                args=(record,),
                name="lark-qq-replay-cancel",
                daemon=True,
            ).start()
            return self._status_locked()

    def _run_prime(self, generation: int) -> None:
        with self._lock:
            if not self._generation_active_locked(generation):
                raise StartAborted("启动已取消")
            self._begin_operation_locked(OP_PRIME, "normal")
            try:
                record = self._spawn(ROLE_PRIME, self.config.command("prime"))
            except ControlPlaneError:
                message = "prime 进程未能启动，请检查本机 Python 环境后重试"
                self._finish_operation_locked(
                    OP_PRIME,
                    "failed",
                    failure_message=message,
                )
                self._record_event_locked("prime_failed", message, level="error")
                raise
            self._records[ROLE_PRIME] = record

        exit_code: Optional[int] = None
        try:
            exit_code = int(record.process.wait())
        except Exception as exc:
            with self._lock:
                message = "启动前 prime 未完成，请检查飞书授权和目标会话"
                self._finish_operation_locked(
                    OP_PRIME,
                    "failed",
                    failure_message=message,
                )
                self._record_event_locked("prime_failed", message, level="error")
            raise StartupFailure("启动前检查未完成") from exc
        finally:
            with self._lock:
                if self._records.get(ROLE_PRIME) is record:
                    record.exit_code = exit_code
                    record.finished_at = self._clock()
                    self._records.pop(ROLE_PRIME, None)

        with self._lock:
            if not self._generation_active_locked(generation):
                self._finish_operation_locked(OP_PRIME, "cancelled")
                raise StartAborted("启动已取消")
            if exit_code != 0:
                message = "启动前 prime 未通过，请运行只读检查后重试"
                self._finish_operation_locked(
                    OP_PRIME,
                    "failed",
                    failure_message=message,
                )
                self._record_event_locked("prime_failed", message, level="error")
                raise StartupFailure("启动前检查未通过，未启动转发任务")
            probe_record = self._records.get(ROLE_PROBE)
            if not self._is_alive(probe_record):
                self._finish_operation_locked(
                    OP_PRIME,
                    "succeeded",
                    effect="默认 prime 已完成；未补发历史消息",
                )
                raise StartupFailure("飞书通知监听未能保持运行")
            self._finish_operation_locked(
                OP_PRIME,
                "succeeded",
                effect="默认 prime 已完成；未补发历史消息",
            )
            self._record_event_locked("prime_completed", "启动前检查已完成")

    def _start_worker(self, generation: int) -> None:
        try:
            self._launch_public(
                ROLE_PROBE,
                self.config.command("probe"),
                generation,
            )
            self._run_prime(generation)
            self._launch_public(
                ROLE_FORWARDER,
                self.config.command("run"),
                generation,
            )
            with self._lock:
                if not self._generation_active_locked(generation):
                    raise StartAborted("启动已取消")
                self._operation_state = "running"
                self._failure_message = None
                self._record_event_locked("start_completed", "飞书到 QQ 转发已运行")
        except StartAborted:
            return
        except StartupFailure as exc:
            self._fail_start(generation, str(exc))
        except Exception:
            self._fail_start(generation, "本地启动流程失败")
        finally:
            with self._lock:
                if self._start_thread is threading.current_thread():
                    self._start_thread = None

    def _fail_start(self, generation: int, message: str) -> None:
        to_stop: list[ProcessRecord] = []
        with self._lock:
            if generation != self._generation:
                return
            if message == "飞书通知监听未能保持运行":
                message = "通知监听未能保持运行，请检查 macOS 辅助功能权限后重试"
            self._operation_state = "failed"
            self._failure_message = message
            self._record_event_locked("start_failed", message, level="error")
            to_stop = [
                record
                for record in self._records.values()
                if self._is_alive(record)
            ]
            for record in to_stop:
                record.expected_exit = True
        for record in reversed(to_stop):
            self._terminate(record)

    def start(self) -> dict[str, Any]:
        with self._lock:
            self._refresh_processes_locked()
            if self._operation_state in {"starting", "stopping"}:
                raise ActionConflict("当前已有启动或停止操作正在进行")
            if self._has_active_operations_locked():
                raise ActionConflict("当前已有绑定、测试、prime 或频道补发操作正在进行")
            if self._has_active_records_locked(include_check=False):
                raise ActionConflict("转发任务已经在运行")
            self._generation += 1
            generation = self._generation
            self._operation_state = "starting"
            self._failure_message = None
            self._record_event_locked("start_requested", "已接受启动请求")
            thread = threading.Thread(
                target=self._start_worker,
                args=(generation,),
                name="lark-qq-start",
                daemon=True,
            )
            self._start_thread = thread
            thread.start()
            return self._status_locked()

    def _terminate(self, record: ProcessRecord) -> None:
        record.expected_exit = True
        try:
            if record.process.poll() is None:
                record.process.terminate()
        except Exception:
            return
        try:
            record.process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            try:
                record.process.kill()
                record.process.wait(timeout=1)
            except Exception:
                pass
        except Exception:
            pass

    def stop(self) -> dict[str, Any]:
        with self._lock:
            self._refresh_processes_locked()
            if not self._has_active_records_locked() and self._operation_state == "stopped":
                return self._status_locked()
            self._generation += 1
            self._operation_state = "stopping"
            self._failure_message = None
            self._record_event_locked("stop_requested", "已接受停止请求")
            records = [
                record
                for record in self._records.values()
                if self._is_alive(record)
            ]
            records.extend(
                record
                for record in self._operation_records.values()
                if self._is_alive(record)
            )
            if self._check_record is not None and self._is_alive(self._check_record):
                records.append(self._check_record)
            for record in records:
                record.expected_exit = True
                if record.role == ROLE_BIND:
                    record.cancel_requested = True
            start_thread = self._start_thread

        for record in reversed(records):
            self._terminate(record)
        if start_thread is not None and start_thread is not threading.current_thread():
            start_thread.join(timeout=3)

        with self._lock:
            self._refresh_processes_locked()
            if self._has_active_records_locked():
                self._operation_state = "degraded"
                self._failure_message = "部分本地子进程未能停止"
                self._record_event_locked(
                    "stop_failed",
                    self._failure_message,
                    level="error",
                )
            else:
                self._operation_state = "stopped"
                self._record_event_locked("stop_completed", "飞书到 QQ 转发已停止")
            return self._status_locked()

    def restart(self) -> dict[str, Any]:
        self.stop()
        return self.start()

    def _check_worker(self, record: ProcessRecord) -> None:
        try:
            exit_code = int(record.process.wait())
        except Exception:
            exit_code = 1
        with self._lock:
            if self._check_record is not record:
                return
            record.exit_code = exit_code
            record.finished_at = self._clock()
            self._check_record = None
            if exit_code == 0:
                self._last_check = {
                    "state": "passed",
                    "label": "检查通过",
                    "completed_at": record.finished_at,
                }
                self._record_event_locked("check_passed", "只读检查通过")
            else:
                self._last_check = {
                    "state": "failed",
                    "label": "检查未通过",
                    "completed_at": record.finished_at,
                }
                self._record_event_locked(
                    "check_failed",
                    "只读检查未通过",
                    level="warning",
                )
            self._check_thread = None

    def check(self) -> dict[str, Any]:
        with self._lock:
            if self._check_record is not None and self._is_alive(self._check_record):
                raise ActionConflict("只读检查正在进行")
            try:
                record = self._spawn(ROLE_CHECK, self.config.command("check"))
            except ControlPlaneError:
                self._last_check = {
                    "state": "failed",
                    "label": "检查未通过",
                    "completed_at": self._clock(),
                }
                self._record_event_locked(
                    "check_failed",
                    "无法启动只读检查",
                    level="warning",
                )
                return self._status_locked()
            self._check_record = record
            self._last_check = {
                "state": "running",
                "label": "检查中",
                "completed_at": None,
            }
            self._record_event_locked("check_requested", "已接受只读检查请求")
            thread = threading.Thread(
                target=self._check_worker,
                args=(record,),
                name="lark-qq-check",
                daemon=True,
            )
            self._check_thread = thread
            thread.start()
            return self._status_locked()

    def poll_input_events(self) -> None:
        """只从新增完整 JSONL 行识别唤醒类型，不读取任何正文字段。"""

        try:
            size = self.config.input_path.stat().st_size
        except OSError:
            return
        if size < self._input_offset:
            self._input_offset = size

        try:
            with self.config.input_path.open("rb") as handle:
                handle.seek(self._input_offset)
                while True:
                    line = handle.readline()
                    if not line or not line.endswith(b"\n"):
                        break
                    self._input_offset = handle.tell()
                    try:
                        payload = json.loads(line.decode("utf-8"))
                    except (UnicodeDecodeError, json.JSONDecodeError):
                        continue
                    if not isinstance(payload, dict):
                        continue
                    if payload.get("type") != "notification_wakeup":
                        continue
                    with self._lock:
                        self._record_event_locked(
                            "notification_wakeup",
                            "收到通知唤醒信号，等待转发器同步",
                        )
        except OSError:
            return

    def start_input_watcher(self) -> None:
        with self._lock:
            if self._watch_thread is not None and self._watch_thread.is_alive():
                return
            self._watch_stop.clear()
            thread = threading.Thread(
                target=self._input_watcher,
                name="lark-qq-events",
                daemon=True,
            )
            self._watch_thread = thread
            thread.start()

    def _input_watcher(self) -> None:
        while not self._watch_stop.is_set():
            self.poll_input_events()
            self._reload_changed_code()
            self._watch_stop.wait(0.5)

    def _code_signature_now(self) -> tuple[Optional[int], ...]:
        values: list[Optional[int]] = []
        for path in self._code_watch_paths:
            try:
                values.append(path.stat().st_mtime_ns)
            except OSError:
                values.append(None)
        return tuple(values)

    def _reload_changed_code(self) -> None:
        signature = self._code_signature_now()
        if signature == self._code_signature:
            return
        self._code_signature = signature
        with self._lock:
            running = bool(self._active_public_records_locked())
        if not running:
            return
        try:
            with self._lock:
                self._record_event_locked("code_reload_requested", "检测到转发代码变化，准备自动重载")
            self.restart()
        except ControlPlaneError as exc:
            logging.warning("代码自动重载未完成：%s", exc)

    def close(self) -> None:
        self._watch_stop.set()
        self.stop()


class LocalControlPlaneServer(http.server.ThreadingHTTPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(
        self,
        config: ControlPlaneConfig,
        supervisor: ProcessSupervisor,
        static_dir: Path,
    ) -> None:
        if config.host != LOCAL_HOST:
            raise ValueError("控制面只能监听 127.0.0.1")
        self.config = config
        self.supervisor = supervisor
        self.static_dir = static_dir
        self.control_token = secrets.token_urlsafe(32)
        super().__init__((LOCAL_HOST, config.port), ControlPlaneRequestHandler)
        self.supervisor.set_server_port(self.server_port)

    @property
    def allowed_origins(self) -> set[str]:
        return {
            f"http://{LOCAL_HOST}:{self.server_port}",
            f"http://localhost:{self.server_port}",
        }

    def server_close(self) -> None:
        self.supervisor.close()
        super().server_close()


class ControlPlaneRequestHandler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server: LocalControlPlaneServer

    def log_message(self, _format: str, *_args: Any) -> None:
        # 控制面不把请求内容写到终端，避免误记录用户数据。
        return

    def _write_bytes(
        self,
        payload: bytes,
        content_type: str,
        *,
        status: int = 200,
        no_store: bool = False,
    ) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header(
            "Content-Security-Policy",
            "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; font-src 'self'; base-uri 'none'; form-action 'self'",
        )
        self.send_header("Referrer-Policy", "no-referrer")
        if no_store:
            self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(payload)

    def _write_json(self, payload: dict[str, Any], *, status: int = 200) -> None:
        body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode(
            "utf-8"
        )
        self._write_bytes(body, "application/json; charset=utf-8", status=status, no_store=True)

    def _error(self, message: str, *, status: int) -> None:
        self._write_json({"ok": False, "error": message}, status=status)

    def _valid_host(self) -> bool:
        host = self.headers.get("Host")
        if not host:
            return True
        host_name = host.rsplit(":", 1)[0] if ":" in host else host
        return host_name in {LOCAL_HOST, "localhost"}

    def _authorized_write(self) -> bool:
        origin = self.headers.get("Origin")
        if origin and origin not in self.server.allowed_origins:
            self.close_connection = True
            self._error("只允许本机同源操作", status=403)
            return False
        token = self.headers.get("X-Control-Token", "")
        if not hmac.compare_digest(token, self.server.control_token):
            self.close_connection = True
            self._error("缺少有效的本机操作令牌", status=403)
            return False
        return True

    def _read_small_body(self) -> Optional[dict[str, Any]]:
        value = self.headers.get("Content-Length", "0")
        try:
            length = int(value)
        except ValueError:
            self._error("请求体无效", status=400)
            self.close_connection = True
            return None
        if length < 0 or length > 1024:
            self._error("请求体过大", status=413)
            self.close_connection = True
            return None
        raw_body = self.rfile.read(length) if length else b""
        if not raw_body:
            return {}
        try:
            body = json.loads(raw_body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            self._error("请求体必须是有效 JSON", status=400)
            return None
        if not isinstance(body, dict):
            self._error("请求体必须是 JSON 对象", status=400)
            return None
        return body

    def do_GET(self) -> None:
        if not self._valid_host():
            self._error("控制面只接受本机请求", status=400)
            return
        path = urlsplit(self.path).path
        if path == "/api/status":
            self._write_json({"ok": True, "data": self.server.supervisor.status()})
            return
        if path == "/api/listeners":
            self._write_json({"ok": True, "data": {"listeners": self.server.supervisor.listeners()}})
            return
        if path == "/api/replay/preview":
            channel = parse_qs(urlsplit(self.path).query).get("channel", [""])[0]
            try:
                data = self.server.supervisor.preview_replay(channel)
            except ControlPlaneError as exc:
                self._error(str(exc), status=400)
                return
            self._write_json({"ok": True, "data": data})
            return
        if path == "/api/session":
            self._write_json(
                {
                    "ok": True,
                    "data": {
                        "control_token": self.server.control_token,
                        "write_protection": "same-origin-and-token",
                    },
                }
            )
            return
        static_files = {
            "/": ("index.html", "text/html; charset=utf-8"),
            "/index.html": ("index.html", "text/html; charset=utf-8"),
            "/static/app.js": ("app.js", "text/javascript; charset=utf-8"),
            "/static/styles.css": ("styles.css", "text/css; charset=utf-8"),
            "/static/favicon.svg": ("favicon.svg", "image/svg+xml"),
        }
        file_info = static_files.get(path)
        if file_info is None:
            self._error("页面不存在", status=404)
            return
        file_name, content_type = file_info
        try:
            payload = (self.server.static_dir / file_name).read_bytes()
        except OSError:
            self._error("页面资源不可用", status=500)
            return
        self._write_bytes(payload, content_type)

    def do_POST(self) -> None:
        if not self._valid_host():
            self._error("控制面只接受本机请求", status=400)
            return
        if not self._authorized_write():
            return
        body = self._read_small_body()
        if body is None:
            return

        path = urlsplit(self.path).path
        try:
            if path == "/api/listeners":
                name = body.get("name")
                if not isinstance(name, str):
                    self._error("监听人员名称无效", status=400)
                    return
                self._write_json({"ok": True, "data": {"listeners": self.server.supervisor.add_listener(name)}}, status=201)
                return
            if path == "/api/actions/start":
                status = self.server.supervisor.start()
            elif path == "/api/actions/stop":
                status = self.server.supervisor.stop()
            elif path == "/api/actions/restart":
                status = self.server.supervisor.restart()
            elif path == "/api/actions/check":
                status = self.server.supervisor.check()
            elif path == "/api/actions/bind":
                rebind = body.get("rebind", False)
                keep_existing = body.get("add", False)
                confirmed = body.get("confirm", False)
                if not isinstance(rebind, bool) or not isinstance(keep_existing, bool) or not isinstance(confirmed, bool):
                    self._error("绑定确认参数无效", status=400)
                    return
                status = self.server.supervisor.bind(
                    rebind=rebind,
                    keep_existing=keep_existing,
                    confirmed=confirmed,
                )
            elif path == "/api/actions/bind/cancel":
                status = self.server.supervisor.cancel_bind()
            elif path == "/api/actions/groups/remove":
                binding_id = body.get("binding_id")
                confirmed = body.get("confirm", False)
                if not isinstance(binding_id, str) or not isinstance(confirmed, bool):
                    self._error("删除 QQ 群参数无效", status=400)
                    return
                status = self.server.supervisor.remove_group(binding_id, confirmed=confirmed)
            elif path == "/api/actions/groups/label":
                binding_id = body.get("binding_id")
                label = body.get("label")
                if not isinstance(binding_id, str) or not isinstance(label, str):
                    self._error("QQ 群备注参数无效", status=400)
                    return
                status = self.server.supervisor.update_group_label(binding_id, label)
            elif path == "/api/actions/groups/rename":
                binding_id = body.get("binding_id")
                if not isinstance(binding_id, str):
                    self._error("QQ 群绑定参数无效", status=400)
                    return
                status = self.server.supervisor.rename_group(binding_id)
            elif path == "/api/actions/test":
                binding_id = body.get("binding_id")
                if binding_id is not None and not isinstance(binding_id, str):
                    self._error("QQ 群绑定参数无效", status=400)
                    return
                status = self.server.supervisor.test(binding_id=binding_id)
            elif path == "/api/actions/replay":
                channel_name = body.get("channel")
                message_ids = body.get("message_ids")
                if not isinstance(channel_name, str):
                    self._error("补发频道参数无效", status=400)
                    return
                if message_ids is not None and (not isinstance(message_ids, list) or not all(isinstance(item, str) for item in message_ids)):
                    self._error("补发消息选择参数无效", status=400)
                    return
                status = self.server.supervisor.replay(channel_name, message_ids=message_ids)
            elif path == "/api/actions/replay/cancel":
                status = self.server.supervisor.cancel_replay()
            elif path in {"/api/actions/prime", "/api/actions/prime/force-end"}:
                path_force_end = path.endswith("/force-end")
                force_end = body.get("force_end", path_force_end)
                confirmed = body.get("confirm", False)
                if (
                    not isinstance(force_end, bool)
                    or not isinstance(confirmed, bool)
                    or (path_force_end and not force_end)
                ):
                    self._error("prime 参数无效", status=400)
                    return
                status = self.server.supervisor.prime(
                    force_end=force_end,
                    confirmed=confirmed,
                )
            else:
                self._error("操作不存在", status=404)
                return
        except ActionConflict as exc:
            self._error(str(exc), status=409)
            return
        except ConfirmationRequired as exc:
            self._error(str(exc), status=400)
            return
        except InvalidAction as exc:
            self._error(str(exc), status=400)
            return
        except ControlPlaneError:
            self._error("操作未完成", status=500)
            return
        self._write_json({"ok": True, "data": status}, status=202)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="飞书到 QQ 转发器本机控制面")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument(
        "--project-dir",
        type=Path,
        default=PROJECT_DIR,
        help="转发器项目目录，默认使用当前脚本所在目录",
    )
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.WARNING, format="%(levelname)s: %(message)s")
    args = parse_args()
    config = ControlPlaneConfig.for_project(args.project_dir, port=args.port)
    supervisor = ProcessSupervisor(config)
    server = LocalControlPlaneServer(
        config,
        supervisor,
        static_dir=Path(__file__).resolve().parent / "web",
    )
    supervisor.start_input_watcher()

    def stop_on_signal(_signum: int, _frame: Any) -> None:
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, stop_on_signal)
    print(
        f"本机控制面已启动：http://{LOCAL_HOST}:{server.server_port}（仅本机）",
        flush=True,
    )
    try:
        server.serve_forever(poll_interval=0.25)
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
