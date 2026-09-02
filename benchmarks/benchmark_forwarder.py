#!/usr/bin/env python3
"""在不连接真实飞书/QQ 的前提下测量转发基线。"""

from __future__ import annotations

import argparse
import asyncio
import json
import statistics
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import qq_bridge
from qq_bridge import LarkMessage, LarkTarget, StateStore, format_lark_text, process_pending_messages


class FixtureLarkClient:
    def __init__(self, messages: list[LarkMessage], *, download_delay_ms: float) -> None:
        self.messages = messages
        self.download_count = 0
        self.download_delay_ms = download_delay_ms

    def list_messages(self, _chat_id: str) -> list[LarkMessage]:
        return self.messages

    def download_image(
        self, *, message_id: str, image_key: str, output_directory: Path
    ) -> Path:
        del message_id, image_key
        time.sleep(self.download_delay_ms / 1000)
        self.download_count += 1
        path = output_directory / "fixture.jpg"
        path.write_bytes(b"fixture-image")
        return path


async def run_legacy_once(
    *, state: StateStore, lark: FixtureLarkClient, groups: list[str], messages: list[LarkMessage]
) -> None:
    """复现优化前主链路的图片行为，用于同机对比。"""
    pending = sorted(messages, key=lambda item: item.position)
    for message in pending:
        for group in groups:
            if message.msg_type == "image":
                with tempfile.TemporaryDirectory(prefix="lark-qq-legacy-") as directory:
                    image_path = lark.download_image(
                        message_id=message.message_id,
                        image_key="img_fixture_1234567890",
                        output_directory=Path(directory),
                    )
                    await qq_bridge.send_group_image(object(), object(), group, image_path)
                    state.mark_delivery("fixture", group, message.message_id)
            elif message.content.strip():
                await qq_bridge.send_group_text(object(), group, format_lark_text("fixture", message.content))
                state.mark_delivery("fixture", group, message.message_id)
        state.advance_message(message.position, message.message_id)


async def run_once(*, messages: int, groups: int, images_every: int, mode: str, download_delay_ms: float, qq_delay_ms: float) -> dict[str, Any]:
    fixture_messages = [
        LarkMessage(
            message_id=f"fixture-{index}",
            position=index,
            msg_type="image" if images_every and index % images_every == 0 else "text",
            sender_id="fixture-sender",
            content=("[图片] img_fixture_1234567890" if images_every and index % images_every == 0 else f"消息 {index}"),
        )
        for index in range(1, messages + 1)
    ]
    lark = FixtureLarkClient(fixture_messages, download_delay_ms=download_delay_ms)
    sent = 0

    async def fake_text(*_args: Any, **_kwargs: Any) -> dict[str, str]:
        nonlocal sent
        await asyncio.sleep(qq_delay_ms / 1000)
        sent += 1
        return {"id": f"qq-text-{sent}"}

    async def fake_image(*_args: Any, **_kwargs: Any) -> dict[str, str]:
        nonlocal sent
        await asyncio.sleep(qq_delay_ms / 1000)
        sent += 1
        return {"id": f"qq-image-{sent}"}

    old_text, old_image = qq_bridge.send_group_text, qq_bridge.send_group_image
    qq_bridge.send_group_text, qq_bridge.send_group_image = fake_text, fake_image
    started = time.perf_counter()
    try:
        with tempfile.TemporaryDirectory(prefix="lark-qq-benchmark-") as directory:
            state = StateStore.load(Path(directory) / "state.json")
            state.prime_lark(chat_id="fixture-chat", sender_id="fixture-sender", latest_position=0)
            target_groups = [f"group-{index}" for index in range(groups)]
            if mode == "legacy":
                await run_legacy_once(state=state, lark=lark, groups=target_groups, messages=fixture_messages)
            else:
                await process_pending_messages(
                    state=state,
                    lark=lark,
                    target=LarkTarget("fixture", "fixture-sender", "fixture-chat"),
                    api=object(),
                    http_client=object(),
                    group_openid=target_groups,
                )
    finally:
        qq_bridge.send_group_text, qq_bridge.send_group_image = old_text, old_image
    elapsed_ms = (time.perf_counter() - started) * 1000
    return {
        "elapsed_ms": round(elapsed_ms, 3),
        "throughput_messages_per_second": round(messages / max(elapsed_ms / 1000, 1e-9), 3),
        "sent_count": sent,
        "image_download_count": lark.download_count,
    }


async def main() -> None:
    parser = argparse.ArgumentParser(description="运行飞书到 QQ 转发基准")
    parser.add_argument("--messages", type=int, default=20)
    parser.add_argument("--groups", type=int, default=1)
    parser.add_argument("--images-every", type=int, default=5)
    parser.add_argument("--runs", type=int, default=10)
    parser.add_argument("--mode", choices=("legacy", "optimized"), default="optimized")
    parser.add_argument("--download-delay-ms", type=float, default=50.0)
    parser.add_argument("--qq-delay-ms", type=float, default=5.0)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if min(args.messages, args.groups, args.runs) <= 0 or args.images_every < 0 or min(args.download_delay_ms, args.qq_delay_ms) < 0:
        raise SystemExit("messages、groups、runs 必须大于 0，images-every 和延迟不能小于 0")

    results = [
        await run_once(
            messages=args.messages,
            groups=args.groups,
            images_every=args.images_every,
            mode=args.mode,
            download_delay_ms=args.download_delay_ms,
            qq_delay_ms=args.qq_delay_ms,
        )
        for _ in range(args.runs)
    ]
    elapsed = [item["elapsed_ms"] for item in results]
    summary = {
        "schema_version": 1,
        "baseline_kind": "local-fixture",
        "mode": args.mode,
        "parameters": {
            "messages": args.messages,
            "groups": args.groups,
            "images_every": args.images_every,
            "runs": args.runs,
            "download_delay_ms": args.download_delay_ms,
            "qq_delay_ms": args.qq_delay_ms,
        },
        "metrics": {
            "elapsed_ms_p50": round(statistics.median(elapsed), 3),
            "elapsed_ms_p95": round(sorted(elapsed)[max(0, int(len(elapsed) * 0.95) - 1)], 3),
            "throughput_messages_per_second_median": round(statistics.median(item["throughput_messages_per_second"] for item in results), 3),
            "image_download_count_per_run": results[0]["image_download_count"],
            "sent_count_per_run": results[0]["sent_count"],
        },
        "runs": results,
    }
    rendered = json.dumps(summary, ensure_ascii=False, indent=2) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")
    print(rendered, end="")


if __name__ == "__main__":
    asyncio.run(main())
