import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from channel_replay import ChannelCursorStore, replay_channel
from qq_bridge import BridgeError, LarkMessage


class FakeLarkClient:
    def __init__(self, messages: list[LarkMessage]) -> None:
        self.messages = messages
        self.requested_chat_id: str | None = None

    def list_messages(self, chat_id: str) -> list[LarkMessage]:
        self.requested_chat_id = chat_id
        return self.messages


def write_qq_state(root: Path) -> None:
    (root / ".qq-forwarder-state.json").write_text(
        json.dumps(
            {
                "schema_version": 2,
                "app_id": "1905539559",
                "group_openid": "group-sensitive",
            }
        ),
        encoding="utf-8",
    )


def write_channel_state(root: Path, *, cursor: int = 100) -> None:
    (root / ".lark-channel-cursors.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "baseline_at": "2026-09-01T15:00:00+08:00",
                "comparison": "strictly_after",
                "channels": [
                    {
                        "name": "指定频道",
                        "chat_id": "oc_channel",
                        "chat_type": "group",
                        "cursor_position": cursor,
                        "initial_cursor_position": 100,
                    },
                    {
                        "name": "另一个频道",
                        "chat_id": "oc_other",
                        "chat_type": "group",
                        "cursor_position": 200,
                        "initial_cursor_position": 200,
                    },
                ],
            }
        ),
        encoding="utf-8",
    )


class ChannelCursorStoreTests(unittest.TestCase):
    def test_advances_only_the_selected_channel_without_message_body(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_channel_state(root)
            store = ChannelCursorStore.load(root / ".lark-channel-cursors.json")

            store.advance("指定频道", 101, "om_sensitive")

            saved = json.loads(
                (root / ".lark-channel-cursors.json").read_text(encoding="utf-8")
            )
            self.assertEqual(store.names(), ["指定频道", "另一个频道"])
            self.assertEqual(store.get("指定频道").cursor_position, 101)
            self.assertEqual(store.get("另一个频道").cursor_position, 200)
            self.assertIn("om_sensitive", saved["channels"][0]["recent_message_ids"])
            self.assertNotIn("正文", json.dumps(saved, ensure_ascii=False))


class ChannelReplayTests(unittest.IsolatedAsyncioTestCase):
    async def test_replays_only_selected_channel_and_advances_after_each_message(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_qq_state(root)
            write_channel_state(root)
            lark = FakeLarkClient(
                [
                    LarkMessage("om-101", 101, "text", "sender-a", "第一条"),
                    LarkMessage("om-102", 102, "system", "sender-b", "系统事件"),
                    LarkMessage("om-103", 103, "text", "sender-c", "第二条"),
                ]
            )
            api = object()
            http_client = AsyncMock()
            sender = AsyncMock(return_value={"id": "qq-message"})

            async def fake_create_api() -> tuple[object, AsyncMock]:
                return api, http_client

            with (
                patch("channel_replay.create_api", new=fake_create_api),
                patch("channel_replay.send_group_text", new=sender),
            ):
                summary = await replay_channel(
                    channel_name="指定频道",
                    channel_state_path=root / ".lark-channel-cursors.json",
                    state_path=root / ".qq-forwarder-state.json",
                    lark_client=lark,
                )

            self.assertEqual(lark.requested_chat_id, "oc_channel")
            self.assertEqual(summary.pending_count, 3)
            self.assertEqual(summary.forwarded_count, 2)
            self.assertEqual(summary.skipped_count, 1)
            self.assertEqual(summary.cursor_position, 103)
            self.assertEqual(sender.await_count, 2)
            self.assertIn("【飞书·指定频道】", sender.await_args_list[0].args[2])
            self.assertNotIn("系统事件", sender.await_args_list[0].args[2])
            http_client.aclose.assert_awaited_once()

    async def test_send_failure_keeps_cursor_before_failed_message(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_qq_state(root)
            write_channel_state(root)
            lark = FakeLarkClient(
                [LarkMessage("om-101", 101, "text", "sender-a", "不会成功")]
            )
            http_client = AsyncMock()

            async def fake_create_api() -> tuple[object, AsyncMock]:
                return object(), http_client

            with (
                patch("channel_replay.create_api", new=fake_create_api),
                patch(
                    "channel_replay.send_group_text",
                    new=AsyncMock(side_effect=BridgeError("发送失败")),
                ),
            ):
                with self.assertRaises(BridgeError):
                    await replay_channel(
                        channel_name="指定频道",
                        channel_state_path=root / ".lark-channel-cursors.json",
                        state_path=root / ".qq-forwarder-state.json",
                        lark_client=lark,
                    )

            store = ChannelCursorStore.load(root / ".lark-channel-cursors.json")
            self.assertEqual(store.get("指定频道").cursor_position, 100)
            http_client.aclose.assert_awaited_once()

    async def test_unknown_channel_is_rejected_before_lark_or_qq_access(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_qq_state(root)
            write_channel_state(root)
            lark = FakeLarkClient([])

            with self.assertRaises(BridgeError):
                await replay_channel(
                    channel_name="未配置频道",
                    channel_state_path=root / ".lark-channel-cursors.json",
                    state_path=root / ".qq-forwarder-state.json",
                    lark_client=lark,
                )

            self.assertIsNone(lark.requested_chat_id)


if __name__ == "__main__":
    unittest.main()
