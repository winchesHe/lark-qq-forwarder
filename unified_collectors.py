"""飞书 API 与 QQ 通知分别采集，只向持久化队列提交任务。"""

import asyncio
import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path

from qq_bridge import (
    BridgeError, ChannelCursorStore, LarkMessage, ListenerCursorStore, StateStore,
    extract_image_keys, extract_post_text, format_lark_text, pending_messages,
    read_next_record, routed_group_openids, notification_matches_contact, load_source_title_settings,
)
from qq_sources import QQSourceStore
from delivery_dispatcher import QueueFull


@dataclass(frozen=True)
class LarkSource:
    name: str
    chat_id: str
    sender_id: str | None
    initial: int
    kind: str

    @property
    def key(self) -> str:
        return "lark:" + self.chat_id + ":" + (self.sender_id or "*")

    def metadata(self) -> dict:
        return {"kind": self.kind, "name": self.name, "chat_id": self.chat_id, "sender_id": self.sender_id}


def lark_parts(source: LarkSource, message: LarkMessage, include_title: bool | None = None) -> list[dict]:
    if source.sender_id is not None and source.sender_id != message.sender_id:
        return []
    if message.msg_type == "text":
        return [{"type": "text", "text": format_lark_text(source.name, message.content, include_title=include_title)}] if message.content.strip() else []
    if message.msg_type not in {"image", "post"}:
        return []
    keys = extract_image_keys(message.content)
    if message.msg_type == "image" and not keys:
        raise BridgeError("飞书图片消息缺少资源引用，采集进度已保留")
    parts = []
    text = extract_post_text(message.content) if message.msg_type == "post" else ""
    if text:
        parts.append({"type": "text", "text": format_lark_text(source.name, text, include_title=include_title)})
    parts.extend({"type": "image", "message_id": message.message_id, "image_key": key} for key in keys)
    return parts


async def collect_lark_source(store, source: LarkSource, lark, state_path: Path, routing_path: Path,
                              *, messages: list[LarkMessage] | None = None, targets: list[str] | None = None,
                              channels_path: Path | None = None, listener_cursors: Path | None = None) -> int:
    cursor = store.position(source.key, source.initial)
    if messages is None:
        if callable(getattr(lark, "list_messages_since", None)):
            messages = await asyncio.to_thread(lark.list_messages_since, source.chat_id, cursor)
        else:
            messages = await asyncio.to_thread(lark.list_messages, source.chat_id)
        messages = pending_messages(messages, cursor)
    state = StateStore.load(state_path)
    destination = targets if targets is not None else routed_group_openids(state, source.name, routing_path)
    captured = 0
    title_settings = load_source_title_settings(state_path.with_name(".lark-source-settings.json"))
    if source.kind == "channel":
        cursors = ChannelCursorStore.load(channels_path or state_path.with_name(".lark-channel-cursors.json"))
        processed = lambda message: source.name in cursors.names() and cursors.has_processed_message(source.name, message)
    elif source.kind == "listener":
        cursors = ListenerCursorStore.load(listener_cursors or state_path.with_name(".lark-listener-cursors.json"))
        processed = lambda message: cursors.has_processed(source.name, message)
    else:
        processed = state.has_processed_message
    for message in messages:
        parts = lark_parts(source, message, title_settings.get(source.name, True))
        groups = [group for group in destination if not state.has_delivery(source.name, group, message.message_id)] if parts and not processed(message.message_id) else []
        store.capture(source=source.key, message=message.message_id, position=message.position,
                      targets=groups, payload={"parts": parts, "next_part": 0}, metadata=source.metadata())
        captured += 1
        if captured % 10 == 0:
            await asyncio.sleep(0)
    return captured


def publish_lark_event(metadata: dict, targets: list[str], position: int, message: str,
                       *, state_path: Path, channels_path: Path, listeners_path: Path) -> None:
    kind = metadata.get("kind")
    if kind not in {"contact", "listener", "channel"}:
        return
    state = StateStore.load(state_path)
    name = metadata["name"]
    for target in targets:
        state.mark_delivery(name, target, message)
    if kind == "contact":
        if state.data.get("lark_chat_id") == metadata["chat_id"] and state.data.get("lark_sender_id") == metadata["sender_id"]:
            state.advance_message(position, message)
    elif kind == "channel":
        channels = ChannelCursorStore.load(channels_path)
        if name in channels.names() and channels.get(name).chat_id == metadata["chat_id"]:
            channels.advance(name, position, message)
    else:
        listeners = ListenerCursorStore.load(listeners_path)
        value = listeners.get(name)
        if value.get("chat_id") == metadata["chat_id"] and value.get("sender_id") == metadata["sender_id"]:
            listeners.advance(name, LarkMessage(message, position, "", "", ""))


class LarkCollector:
    def __init__(self, store, lark, *, state_path: Path, channels_path: Path, listeners_path: Path,
                 listener_cursors: Path, routing_path: Path, input_path: Path, contact: str) -> None:
        self.store, self.lark = store, lark
        self.state_path, self.channels_path = state_path, channels_path
        self.listeners_path, self.listener_cursors = listeners_path, listener_cursors
        self.routing_path, self.input_path, self.contact = routing_path, input_path, contact
        self.workers, self.events, self.states = {}, {}, {}
        self.fetch_slots = asyncio.Semaphore(4)

    async def discover(self) -> dict[str, LarkSource]:
        sources = {}
        state = StateStore.load(self.state_path)
        names = [self.contact]
        if self.listeners_path.exists():
            data = json.loads(self.listeners_path.read_text())
            names = list(dict.fromkeys([self.contact, *data.get("listeners", [])]))
        wanted = {"resolve:" + name for name in names}
        for key in list(self.states):
            if key.startswith("resolve:") and key not in wanted:
                del self.states[key]
        for name in names:
            try:
                target = await asyncio.to_thread(self.lark.resolve_target, name)
                if name.casefold() == self.contact.casefold():
                    state.assert_lark_target(chat_id=target.chat_id, sender_id=target.sender_id)
                    initial, kind = state.message_position, "contact"
                else:
                    cursors = ListenerCursorStore.load(self.listener_cursors)
                    existing = cursors.get(name)
                    if existing.get("chat_id") != target.chat_id or existing.get("sender_id") != target.sender_id:
                        latest = await asyncio.to_thread(self.lark.list_messages, target.chat_id)
                        cursors.initialize(target, max((m.position for m in latest), default=0))
                    initial, kind = cursors.cursor(name), "listener"
                source = LarkSource(name, target.chat_id, target.sender_id, initial, kind)
                sources[source.key] = source
            except Exception:
                self.states["resolve:" + name] = "failed"
                continue
            self.states.pop("resolve:" + name, None)
        if self.channels_path.exists():
            channels = ChannelCursorStore.load(self.channels_path)
            for name in channels.names():
                channel = channels.get(name)
                source = LarkSource(name, channel.chat_id, None, channel.cursor_position, "channel")
                sources[source.key] = source
        return sources

    async def _worker(self, source: LarkSource, wake: asyncio.Event) -> None:
        while True:
            wake.clear()
            try:
                async with self.fetch_slots:
                    await collect_lark_source(self.store, source, self.lark, self.state_path, self.routing_path,
                                              channels_path=self.channels_path, listener_cursors=self.listener_cursors)
                self.states[source.key] = "running"
            except QueueFull:
                self.states[source.key] = "backpressure"
            except Exception:
                self.states[source.key] = "failed"
            try:
                await asyncio.wait_for(wake.wait(), 30)
            except TimeoutError:
                pass

    async def run(self, stop: asyncio.Event) -> None:
        offset = self.input_path.stat().st_size if self.input_path.exists() else 0
        last_discovery = -30.0
        sources = {}
        try:
            while not stop.is_set():
                now = asyncio.get_running_loop().time()
                if now - last_discovery >= 30:
                    try:
                        discovered = await self.discover()
                        changed = {key for key in sources.keys() & discovered.keys()
                                   if sources[key].metadata() != discovered[key].metadata()}
                        for key in (set(self.workers) - set(discovered)) | changed:
                            worker = self.workers.pop(key)
                            worker.cancel()
                            await asyncio.gather(worker, return_exceptions=True)
                            self.events.pop(key, None)
                            self.states.pop(key, None)
                        sources = discovered
                        for key, source in sources.items():
                            if key not in self.workers:
                                self.states[key] = "starting"
                                self.events[key] = asyncio.Event()
                                self.workers[key] = asyncio.create_task(self._worker(source, self.events[key]))
                    except Exception:
                        self.states["configuration"] = "failed"
                    else:
                        self.states.pop("configuration", None)
                    last_discovery = now
                for _ in range(100):
                    record = read_next_record(self.input_path, offset)
                    if record is None:
                        break
                    for key, source in sources.items():
                        if record.payload and notification_matches_contact(record.payload, source.name):
                            self.events[key].set()
                    offset = record.next_offset
                state = "degraded" if any(value != "running" for value in self.states.values()) else "running"
                self.store.set_status("lark", state, "来源读取异常或等待队列容量" if state == "degraded" else "")
                try:
                    await asyncio.wait_for(stop.wait(), 0.25)
                except TimeoutError:
                    pass
        finally:
            for worker in self.workers.values():
                worker.cancel()
            await asyncio.gather(*self.workers.values(), return_exceptions=True)
            self.store.set_status("lark", "stopped")


class QQNotificationCollector:
    def __init__(self, store, spool: Path, rules_path: Path, state_path: Path) -> None:
        self.store, self.spool, self.rules_path, self.state_path = store, spool, rules_path, state_path

    def collect_file(self, path: Path) -> str:
        if path.stat().st_size > 32768:
            raise ValueError("通知记录超过大小限制")
        notification = json.loads(path.read_text())
        if (notification.get("type") != "qq_notification" or notification.get("schema_version") != 1
                or notification.get("bundle_id") != "com.tencent.qq"
                or not re.fullmatch(r"[a-fA-F0-9-]{36}", notification.get("event_id", ""))):
            raise ValueError("QQ 通知记录格式无效")
        for field in ("title", "body", "subtitle"):
            if not isinstance(notification.get(field), str):
                raise ValueError("QQ 通知文本无效")
        title, body = notification["title"].strip(), notification["body"].strip()
        rules = QQSourceStore(self.rules_path, lambda *_: None).read()
        # 不猜测折叠汇总背后的消息，也不把其当成完整正文转发。
        summary = not body or bool(re.fullmatch(r"(?:你有)?\s*\d+\s*条(?:新)?消息[。.!！]?", body))
        matching = [] if summary else [rule for rule in rules if rule["enabled"] and rule["group_name"] == title
            and (not rule["sender"] or notification["subtitle"].strip() == rule["sender"]
                 or body.startswith(rule["sender"] + "：") or body.startswith(rule["sender"] + ":"))]
        bindings = StateStore.load(self.state_path).group_bindings
        selected = {value for rule in matching for value in rule["binding_ids"]}
        targets = list(dict.fromkeys(group["group_openid"] for group in bindings
                                    if group.get("status") == "active" and group["binding_id"] in selected))
        source = "qq:" + hashlib.sha256(title.encode()).hexdigest()
        self.store.capture(source=source, message=notification["event_id"], position=0, targets=targets,
                           payload={"parts": [{"type": "text", "text": "[QQ · " + title + "]\n" + body}], "next_part": 0},
                           metadata={"kind": "qq", "spool": str(path.resolve())})
        path.unlink()
        return "captured" if targets else "summary" if summary else "unmatched"

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            state, detail = "running", ""
            try:
                self.spool.mkdir(parents=True, exist_ok=True, mode=0o700)
                for path in sorted(self.spool.glob("*.json"))[:100]:
                    try:
                        result = self.collect_file(path)
                        if result != "captured":
                            detail = "部分通知为汇总、未匹配来源或未配置目标"
                    except QueueFull:
                        state, detail = "backpressure", "队列容量不足，通知已保留等待处理"
                        break
                    except (ValueError, UnicodeError):
                        path.rename(path.with_suffix(".invalid"))
                        state, detail = "degraded", "存在无法解析的通知，原记录已保留"
                if any(self.spool.glob("*.invalid")):
                    state, detail = "degraded", "存在无法解析的通知，原记录已保留"
            except Exception:
                state, detail = "failed", "QQ 通知或规则无法读取，原记录已保留"
            self.store.set_status("qq", state, detail)
            try:
                await asyncio.wait_for(stop.wait(), 0.25)
            except TimeoutError:
                pass
        self.store.set_status("qq", "stopped")
