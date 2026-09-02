#!/usr/bin/env python3

"""按频道选择性补发飞书历史消息到当前已绑定的 QQ 群。"""

from __future__ import annotations

import argparse
import asyncio
import logging
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from qq_bridge import (
    BridgeError,
    ChannelCursorStore,
    LarkClient,
    StateStore,
    create_api,
    extract_image_key,
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


async def replay_channel(
    *,
    channel_name: str,
    channel_state_path: Path,
    state_path: Path,
    lark_profile: str = DEFAULT_LARK_PROFILE,
    lark_client: Optional[LarkClient] = None,
) -> ReplaySummary:
    qq_state = StateStore.load(state_path)
    group_openid = qq_state.group_openid
    if not group_openid:
        raise BridgeError("尚未绑定 QQ 群，请先运行 bind")

    cursors = ChannelCursorStore.load(channel_state_path)
    channel = cursors.get(channel_name)
    lark = lark_client or LarkClient(profile=lark_profile)
    messages = await asyncio.to_thread(lark.list_messages, channel.chat_id)
    pending = pending_messages(messages, channel.cursor_position)
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
        for message in pending:
            if cursors.has_processed_message(channel.name, message.message_id):
                cursors.advance(channel.name, message.position, message.message_id)
                skipped += 1
                continue

            if message.msg_type == "text":
                if message.content.strip():
                    await send_group_text(
                        api,
                        group_openid,
                        format_lark_text(channel.name, message.content),
                    )
                    forwarded += 1
                else:
                    skipped += 1
            elif message.msg_type == "image":
                image_key = extract_image_key(message.content)
                if not image_key:
                    raise BridgeError("飞书图片消息缺少 image_key")
                with tempfile.TemporaryDirectory(
                    prefix="lark-qq-channel-image-"
                ) as directory:
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
                skipped += 1

            cursors.advance(channel.name, message.position, message.message_id)
    finally:
        await http_client.aclose()

    return ReplaySummary(
        channel_name=channel.name,
        pending_count=len(pending),
        forwarded_count=forwarded,
        skipped_count=skipped,
        cursor_position=cursors.get(channel.name).cursor_position,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="按频道补发飞书消息到 QQ 群")
    parser.add_argument("--channel", required=True)
    parser.add_argument("--channel-state", type=Path, default=DEFAULT_CHANNEL_STATE)
    parser.add_argument("--state", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--lark-profile", default=DEFAULT_LARK_PROFILE)
    return parser.parse_args()


async def async_main() -> None:
    args = parse_args()
    summary = await replay_channel(
        channel_name=args.channel,
        channel_state_path=args.channel_state,
        state_path=args.state,
        lark_profile=args.lark_profile,
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
