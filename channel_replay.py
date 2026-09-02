#!/usr/bin/env python3

"""按频道选择性补发飞书历史消息到当前已绑定的 QQ 群。"""

from __future__ import annotations

import argparse
import asyncio
import logging
import json
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from qq_bridge import (
    BridgeError,
    ChannelCursorStore,
    DEFAULT_PROCESS_LOCK,
    ForwarderProcessLock,
    LarkClient,
    StateStore,
    create_api,
    extract_image_key,
    extract_image_keys,
    extract_post_text,
    format_lark_text,
    pending_messages,
    send_group_image,
    send_group_text,
)


PROJECT_DIR = Path(__file__).resolve().parent
DEFAULT_STATE = PROJECT_DIR / ".qq-forwarder-state.json"
DEFAULT_CHANNEL_STATE = PROJECT_DIR / ".lark-channel-cursors.json"
DEFAULT_LARK_PROFILE = "tenant-105183"


@dataclass(frozen=True)
class ReplaySummary:
    channel_name: str
    pending_count: int
    forwarded_count: int
    skipped_count: int
    cursor_position: int


async def preview_channel(*, channel_name: str, channel_state_path: Path, lark_profile: str = DEFAULT_LARK_PROFILE, lark_client: Optional[LarkClient] = None) -> list[dict[str, object]]:
    cursors = ChannelCursorStore.load(channel_state_path)
    channel = cursors.get(channel_name)
    lark = lark_client or LarkClient(profile=lark_profile)
    list_since = getattr(lark, "list_messages_since", None)
    if callable(list_since):
        messages = await asyncio.to_thread(list_since, channel.chat_id, channel.cursor_position)
    else:
        messages = await asyncio.to_thread(lark.list_messages, channel.chat_id)
    pending = pending_messages(messages, channel.cursor_position)
    return [
        {"message_id": item.message_id, "position": item.position, "type": item.msg_type,
         "preview": item.content[:240] if item.msg_type == "text" else "图片消息",
         "content": item.content if item.msg_type == "text" else "",
         "image_key": extract_image_key(item.content) if item.msg_type == "image" else None}
        for item in pending
    ]


async def _replay_channel_impl(
    *,
    channel_name: str,
    channel_state_path: Path,
    state_path: Path,
    lark_profile: str = DEFAULT_LARK_PROFILE,
    lark_client: Optional[LarkClient] = None,
    message_ids: Optional[set[str]] = None,
    binding_ids: Optional[set[str]] = None,
    progress_path: Optional[Path] = None,
) -> ReplaySummary:
    qq_state = StateStore.load(state_path)
    selected = binding_ids or set()
    groups = [group for group in qq_state.group_bindings if group.get("status") == "active"]
    if selected:
        groups = [group for group in groups if group.get("binding_id") in selected]
    group_openids = [group["group_openid"] for group in groups if isinstance(group.get("group_openid"), str)]
    if not selected and not group_openids:
        group_openids = qq_state.active_group_openids()
    if not group_openids:
        raise BridgeError("尚未绑定 QQ 群，请先运行 bind")

    cursors = ChannelCursorStore.load(channel_state_path)
    channel = cursors.get(channel_name)
    lark = lark_client or LarkClient(profile=lark_profile)
    list_since = getattr(lark, "list_messages_since", None)
    if callable(list_since):
        messages = await asyncio.to_thread(list_since, channel.chat_id, channel.cursor_position)
    else:
        messages = await asyncio.to_thread(lark.list_messages, channel.chat_id)
    pending = pending_messages(messages, channel.cursor_position)
    if message_ids is not None:
        pending = [message for message in pending if message.message_id in message_ids]
    if not pending:
        return ReplaySummary(
            channel_name=channel.name,
            pending_count=0,
            forwarded_count=0,
            skipped_count=0,
            cursor_position=channel.cursor_position,
        )

    api, http_client = await create_api()
    forwarded = 0
    skipped = 0
    try:
        total = len(pending)
        processed_ids: list[str] = []
        def write_progress(state: str, current: int, *, error: str | None = None) -> None:
            if not progress_path:
                return
            progress_path.write_text(json.dumps({"channel": channel.name, "state": state, "total": total, "current": current, "forwarded": forwarded, "skipped": skipped, "processed_ids": processed_ids, "error": error}, ensure_ascii=False), encoding="utf-8")
        write_progress("running", 0)
        for message in pending:
            if cursors.has_processed_message(channel.name, message.message_id):
                cursors.advance(channel.name, message.position, message.message_id)
                skipped += 1
                processed_ids.append(message.message_id)
                write_progress("running", len(processed_ids))
                continue

            if message.msg_type == "text":
                if message.content.strip():
                    for group_openid in group_openids:
                        if not qq_state.has_delivery(channel.name, group_openid, message.message_id):
                            await send_group_text(api, group_openid, format_lark_text(channel.name, message.content))
                            qq_state.mark_delivery(channel.name, group_openid, message.message_id)
                            forwarded += 1
                else:
                    skipped += 1
            elif message.msg_type in {"image", "post"}:
                image_keys = extract_image_keys(message.content)
                if not image_keys and message.msg_type == "image":
                    raise BridgeError("飞书图片消息缺少 image_key")
                post_text = extract_post_text(message.content) if message.msg_type == "post" else ""
                if post_text:
                    for group_openid in group_openids:
                        if not qq_state.has_delivery(channel.name, group_openid, message.message_id):
                            await send_group_text(api, group_openid, format_lark_text(channel.name, post_text))
                            forwarded += 1
                with tempfile.TemporaryDirectory(
                    prefix="lark-qq-channel-image-"
                ) as directory:
                    for image_key in image_keys:
                        image_path = await asyncio.to_thread(lark.download_image, message_id=message.message_id, image_key=image_key, output_directory=Path(directory))
                        for group_openid in group_openids:
                            if not qq_state.has_delivery(channel.name, group_openid, message.message_id):
                                await send_group_image(api, http_client, group_openid, image_path)
                                qq_state.mark_delivery(channel.name, group_openid, message.message_id)
                                forwarded += 1
            else:
                logging.info("跳过暂不支持的飞书消息类型：%s", message.msg_type)
                skipped += 1

            cursors.advance(channel.name, message.position, message.message_id)
            processed_ids.append(message.message_id)
            write_progress("running", len(processed_ids))
    finally:
        await http_client.aclose()

    if progress_path:
        progress_path.write_text(json.dumps({"channel": channel.name, "state": "succeeded", "total": len(pending), "current": len(pending), "forwarded": forwarded, "skipped": skipped, "processed_ids": processed_ids}, ensure_ascii=False), encoding="utf-8")
    return ReplaySummary(
        channel_name=channel.name,
        pending_count=len(pending),
        forwarded_count=forwarded,
        skipped_count=skipped,
        cursor_position=cursors.get(channel.name).cursor_position,
    )


async def replay_channel(
    *,
    channel_name: str,
    channel_state_path: Path,
    state_path: Path,
    lark_profile: str = DEFAULT_LARK_PROFILE,
    lark_client: Optional[LarkClient] = None,
    message_ids: Optional[set[str]] = None,
    binding_ids: Optional[set[str]] = None,
    progress_path: Optional[Path] = None,
    process_lock_path: Path = DEFAULT_PROCESS_LOCK,
) -> ReplaySummary:
    """频道补发与自动转发共享独占锁，避免并发修改游标或重复发送。"""
    with ForwarderProcessLock(process_lock_path):
        return await _replay_channel_impl(
            channel_name=channel_name,
            channel_state_path=channel_state_path,
            state_path=state_path,
            lark_profile=lark_profile,
            lark_client=lark_client,
            message_ids=message_ids,
            binding_ids=binding_ids,
            progress_path=progress_path,
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="按频道补发飞书消息到 QQ 群")
    parser.add_argument("--channel", required=True)
    parser.add_argument("--channel-state", type=Path, default=DEFAULT_CHANNEL_STATE)
    parser.add_argument("--state", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--lark-profile", default=DEFAULT_LARK_PROFILE)
    parser.add_argument("--message-id", action="append", dest="message_ids")
    parser.add_argument("--binding-id", action="append", dest="binding_ids")
    parser.add_argument("--progress-path", type=Path)
    return parser.parse_args()


async def async_main() -> None:
    args = parse_args()
    summary = await replay_channel(
        channel_name=args.channel,
        channel_state_path=args.channel_state,
        state_path=args.state,
        lark_profile=args.lark_profile,
        message_ids=set(args.message_ids) if args.message_ids else None,
        binding_ids=set(args.binding_ids) if args.binding_ids else None,
        progress_path=args.progress_path,
    )
    print(
        f"频道补发完成：待处理 {summary.pending_count} 条，"
        f"已发送 {summary.forwarded_count} 条，"
        f"已跳过 {summary.skipped_count} 条。"
    )


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    try:
        asyncio.run(async_main())
    except BridgeError as exc:
        raise SystemExit(f"错误：{exc}") from exc


if __name__ == "__main__":
    main()
