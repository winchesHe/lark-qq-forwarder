"""频道补发使用同一持久化队列，只调度用户已选择的投递任务。"""

import asyncio
import json
from pathlib import Path

from channel_replay import ReplaySummary
from qq_bridge import BridgeError, ChannelCursorStore, ForwarderProcessLock, LarkClient, StateStore, pending_messages
from unified_collectors import LarkSource, collect_lark_source
from unified_service import dispatch_selected, publisher
from unified_store import QUEUE_FILE, UnifiedStore


async def replay_unified(*, channel_name: str, channel_state_path: Path, state_path: Path,
                         lark_profile: str = "tenant-105183", lark_client=None,
                         message_ids: set[str] | None = None, binding_ids: set[str] | None = None,
                         progress_path: Path | None = None, api_factory=None) -> ReplaySummary:
    with ForwarderProcessLock(state_path.with_name(".qq-forwarder.lock")):
        store = UnifiedStore(state_path.with_name(QUEUE_FILE))
        try:
            state = StateStore.load(state_path)
            groups = [group["group_openid"] for group in state.group_bindings if group.get("status") == "active"
                      and (not binding_ids or group["binding_id"] in binding_ids)]
            if not groups:
                raise BridgeError("没有可用于补发的目标群")
            if binding_ids and len(groups) != len(binding_ids):
                raise BridgeError("所选目标包含不存在或未启用的群")
            cursors = ChannelCursorStore.load(channel_state_path)
            channel = cursors.get(channel_name)
            lark = lark_client or LarkClient(profile=lark_profile)
            if callable(getattr(lark, "list_messages_since", None)):
                messages = await asyncio.to_thread(lark.list_messages_since, channel.chat_id, channel.cursor_position)
            else:
                messages = await asyncio.to_thread(lark.list_messages, channel.chat_id)
            selected = [message for message in pending_messages(messages, channel.cursor_position)
                        if message_ids is None or message.message_id in message_ids]
            source = LarkSource(channel.name, channel.chat_id, None, channel.cursor_position, "channel")
            publish = publisher(state_path, channel_state_path, state_path.with_name(".lark-listener-cursors.json"))
            forwarded = skipped = 0
            processed = []
            ids = []
            # 先校验旧队列，拒绝时不能先保存新消息或推进采集位置。
            selected_messages = {message.message_id for message in selected}
            queued = store.db.execute("SELECT id,message,target FROM deliveries WHERE source=? AND status!='done' ORDER BY id", (source.key,)).fetchall()
            selected_existing = {row["id"] for row in queued if row["message"] in selected_messages and row["target"] in groups}
            for message in selected:
                existing = store.db.execute("SELECT id,target FROM deliveries WHERE source=? AND message=?", (source.key, message.message_id)).fetchall()
                candidates = [(row["id"], row["target"]) for row in existing if row["target"] in groups]
                if not existing:
                    candidates = [(float("inf"), group) for group in groups]
                if any(row["target"] == target and row["id"] < task_id and row["id"] not in selected_existing
                       for task_id, target in candidates for row in queued):
                    raise BridgeError("该来源存在更早的待投递任务，请先选择这些消息完成补发")

            def selected_event_ids(message):
                return [row[0] for row in store.db.execute("SELECT id,target FROM deliveries WHERE source=? AND message=?", (source.key, message.message_id)) if row[1] in groups]

            def write_progress(status: str, error=None):
                nonlocal forwarded, skipped, processed
                processed = []
                forwarded = skipped = 0
                for message in selected:
                    if not store.db.execute("SELECT 1 FROM source_events WHERE source=? AND message=?", (source.key, message.message_id)).fetchone():
                        continue
                    outcomes = store.outcomes(selected_event_ids(message))
                    forwarded += sum(item["sent_parts"] for item in outcomes)
                    if all(item["status"] == "done" for item in outcomes):
                        processed.append(message.message_id)
                        if not outcomes or all(item["outcome"] != "sent" for item in outcomes):
                            skipped += 1
                if progress_path:
                    from qq_bridge import _save_json_atomically
                    _save_json_atomically(progress_path, {"channel": channel.name, "state": status,
                        "total": len(selected), "current": len(processed), "forwarded": forwarded,
                        "skipped": skipped, "processed_ids": processed, "error": error})

            try:
                await collect_lark_source(store, source, lark, state_path, state_path.with_name(".lark-routing.json"),
                                          messages=selected, targets=groups, channels_path=channel_state_path)
                for message in selected:
                    ids.extend(selected_event_ids(message))
                for task_id in ids:
                    store.resume_delivery(task_id)
                write_progress("running")
                if ids:
                    await dispatch_selected(store, ids, state_path=state_path, lark=lark, publish=publish, publish_source=source.key,
                                            progress=lambda: write_progress("running"), api_factory=api_factory)
                store.export_completed(publish, source=source.key)
                write_progress("succeeded")
                store.prune()
            except BaseException:
                write_progress("failed", "补发未全部确认，已保存完成部分和待投递任务")
                raise
            return ReplaySummary(channel.name, len(selected), forwarded, skipped,
                                 ChannelCursorStore.load(channel_state_path).get(channel.name).cursor_position)
        finally:
            store.close()
