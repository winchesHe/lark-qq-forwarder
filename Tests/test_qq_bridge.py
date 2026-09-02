import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from qq_bridge import (
    BridgeError,
    ChannelCursorStore,
    LarkMessage,
    LarkTarget,
    StateStore,
    extract_image_key,
    extract_lark_messages,
    format_lark_text,
    forward_forever,
    notification_matches_contact,
    pending_messages,
    process_pending_messages,
    read_next_record,
)


class StateStoreTests(unittest.TestCase):
    def test_migrates_legacy_notification_fingerprints(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state_path = Path(directory) / "state.json"
            state_path.write_text(
                '{"schema_version":1,"recent_fingerprints":["legacy"]}',
                encoding="utf-8",
            )

            state = StateStore.load(state_path)

            self.assertEqual(state.data["schema_version"], 2)
            self.assertNotIn("recent_fingerprints", state.data)

    def test_first_prime_starts_at_end(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            input_path = root / "notifications.jsonl"
            input_path.write_text('{"title":"历史消息"}\n', encoding="utf-8")
            state = StateStore.load(root / "state.json")

            offset = state.prime_input(input_path)

            self.assertEqual(offset, input_path.stat().st_size)

    def test_lark_prime_preserves_existing_cursor_for_same_target(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state = StateStore.load(Path(directory) / "state.json")
            state.prime_lark(
                chat_id="chat-a",
                sender_id="user-a",
                latest_position=10,
            )
            state.advance_message(12, "message-12")

            cursor = state.prime_lark(
                chat_id="chat-a",
                sender_id="user-a",
                latest_position=20,
            )

            self.assertEqual(cursor, 12)
            self.assertTrue(state.has_processed_message("message-12"))

    def test_lark_prime_resets_cursor_when_target_changes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state = StateStore.load(Path(directory) / "state.json")
            state.prime_lark(
                chat_id="chat-a",
                sender_id="user-a",
                latest_position=10,
            )
            state.advance_message(12, "message-12")

            cursor = state.prime_lark(
                chat_id="chat-b",
                sender_id="user-b",
                latest_position=30,
            )

            self.assertEqual(cursor, 30)
            self.assertFalse(state.has_processed_message("message-12"))

    def test_force_end_moves_cursor_to_latest_and_clears_recent_ids(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state = StateStore.load(Path(directory) / "state.json")
            state.prime_lark(
                chat_id="chat-a",
                sender_id="user-a",
                latest_position=10,
            )
            state.advance_message(12, "message-12")

            cursor = state.prime_lark(
                chat_id="chat-a",
                sender_id="user-a",
                latest_position=30,
                force_end=True,
            )

            self.assertEqual(cursor, 30)
            self.assertFalse(state.has_processed_message("message-12"))


class JSONLTests(unittest.TestCase):
    def test_reads_only_complete_record_after_offset(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            input_path = Path(directory) / "notifications.jsonl"
            input_path.write_text('{"title":"旧记录"}\n', encoding="utf-8")
            offset = input_path.stat().st_size
            with input_path.open("a", encoding="utf-8") as handle:
                handle.write('{"title":"Perfecto"}\n')

            record = read_next_record(input_path, offset)

            self.assertIsNotNone(record)
            assert record is not None
            self.assertEqual(record.payload, {"title": "Perfecto"})

    def test_ignores_incomplete_last_line(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            input_path = Path(directory) / "notifications.jsonl"
            input_path.write_text('{"title":"Perfecto"}', encoding="utf-8")

            self.assertIsNone(read_next_record(input_path, 0))


class NotificationTriggerTests(unittest.TestCase):
    def test_matches_contact_from_minimal_wakeup_title(self) -> None:
        self.assertTrue(
            notification_matches_contact(
                {"type": "notification_wakeup", "title": "Perfecto"},
                "Perfecto",
            )
        )

    def test_rejects_other_contact(self) -> None:
        self.assertFalse(
            notification_matches_contact(
                {"type": "notification_wakeup", "title": "其他联系人"},
                "Perfecto",
            )
        )

    def test_matches_fullwidth_tilde_in_channel_name(self) -> None:
        self.assertTrue(
            notification_matches_contact(
                {"type": "notification_wakeup", "title": "骚神复盘～"},
                "骚神复盘~",
            )
        )


class LarkMessageTests(unittest.TestCase):
    def test_extracts_nested_messages_and_deduplicates_ids(self) -> None:
        payload = {
            "ok": True,
            "data": {
                "items": [
                    {
                        "message_id": "message-1",
                        "message_position": "11",
                        "msg_type": "text",
                        "content": "同文",
                        "sender": {"id": "perfecto"},
                    },
                    {
                        "message_id": "message-2",
                        "message_position": "12",
                        "msg_type": "text",
                        "content": "同文",
                        "sender": {"id": "perfecto"},
                    },
                ]
            },
        }
        payload["duplicate"] = payload["data"]["items"][0]

        messages = extract_lark_messages(payload)
        pending = pending_messages(messages, 10)

        self.assertEqual([message.message_id for message in pending], [
            "message-1",
            "message-2",
        ])
        self.assertEqual([message.content for message in pending], ["同文", "同文"])

    def test_extracts_image_key_from_formatted_content(self) -> None:
        self.assertEqual(
            extract_image_key("[图片] img_v3_abcdef1234567890"),
            "img_v3_abcdef1234567890",
        )
        self.assertIsNone(extract_image_key("[图片] 无资源键"))

    def test_formats_authoritative_text(self) -> None:
        self.assertEqual(
            format_lark_text("Perfecto", "后台文本\n"),
            "【飞书·Perfecto】\n后台文本",
        )


class FakeLarkClient:
    def __init__(self, messages: list[LarkMessage]) -> None:
        self.messages = messages

    def list_messages(self, _chat_id: str) -> list[LarkMessage]:
        return self.messages


class MappingLarkClient:
    def __init__(self, messages_by_chat: dict[str, list[LarkMessage]]) -> None:
        self.messages_by_chat = messages_by_chat
        self.requested_chat_ids: list[str] = []

    def list_messages(self, chat_id: str) -> list[LarkMessage]:
        self.requested_chat_ids.append(chat_id)
        return self.messages_by_chat[chat_id]


class ForwardingTests(unittest.IsolatedAsyncioTestCase):
    async def test_forwards_all_configured_channels_from_notification_wakeups(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            input_path = root / "notifications.jsonl"
            channel_names = [
                "骚神复盘~",
                "三川吴岳8-11",
                "新生代柚子(屏蔽问答)",
                "聪明小阿姨",
            ]
            chat_ids = ["chat-shaoshen", "chat-sanchuan", "chat-youzi", "chat-ayi"]
            input_path.write_text(
                "".join(
                    json.dumps(
                        {
                            "type": "notification_wakeup",
                            "title": "骚神复盘～" if index == 0 else name,
                        },
                        ensure_ascii=False,
                    )
                    + "\n"
                    for index, name in enumerate(channel_names)
                ),
                encoding="utf-8",
            )

            state = StateStore.load(root / "state.json")
            state.data.update(
                {
                    "group_openid": "group-a",
                    "input_path": str(input_path.resolve()),
                    "offset": 0,
                }
            )
            state.prime_lark(
                chat_id="chat-perfecto",
                sender_id="perfecto",
                latest_position=10,
            )

            channel_state_path = root / "channels.json"
            channel_state_path.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "channels": [
                            {
                                "name": name,
                                "chat_id": chat_id,
                                "cursor_position": 100,
                                "initial_cursor_position": 100,
                            }
                            for name, chat_id in zip(channel_names, chat_ids)
                        ],
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            lark = MappingLarkClient(
                {
                    chat_id: [
                        LarkMessage(
                            f"message-{index}",
                            101,
                            "text",
                            f"member-{index}",
                            f"频道消息-{index}",
                        )
                    ]
                    for index, chat_id in enumerate(chat_ids)
                }
            )
            http_client = AsyncMock()
            sender = AsyncMock(return_value={"id": "qq-message"})

            async def fake_create_api() -> tuple[object, AsyncMock]:
                return object(), http_client

            class ForwardingFinished(Exception):
                pass

            async def stop_after_input(_interval: float) -> None:
                raise ForwardingFinished()

            with (
                patch("qq_bridge.create_api", new=fake_create_api),
                patch("qq_bridge.send_group_text", new=sender),
                patch("qq_bridge.asyncio.sleep", new=stop_after_input),
            ):
                with self.assertRaises(ForwardingFinished):
                    await forward_forever(
                        state,
                        input_path,
                        lark=lark,
                        target=LarkTarget(
                            "Perfecto", "perfecto", "chat-perfecto"
                        ),
                        contact_name="Perfecto",
                        channel_state_path=channel_state_path,
                    )

            self.assertEqual(lark.requested_chat_ids, chat_ids)
            self.assertEqual(sender.await_count, 4)
            sent_texts = [call.args[2] for call in sender.await_args_list]
            for name in channel_names:
                self.assertTrue(any(f"【飞书·{name}】" in text for text in sent_texts))
            cursors = ChannelCursorStore.load(channel_state_path)
            self.assertTrue(all(cursor.cursor_position == 101 for cursor in (
                cursors.get(name) for name in channel_names
            )))
            http_client.aclose.assert_awaited_once()

    async def test_forwards_consecutive_same_text_as_distinct_messages(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state = StateStore.load(Path(directory) / "state.json")
            state.prime_lark(
                chat_id="chat-a",
                sender_id="perfecto",
                latest_position=10,
            )
            messages = [
                LarkMessage("message-11", 11, "text", "perfecto", "重复验证"),
                LarkMessage("message-12", 12, "text", "perfecto", "重复验证"),
            ]

            with patch("qq_bridge.send_group_text", new=AsyncMock(return_value={"id": "qq"})) as send:
                pending_count, forwarded_count = await process_pending_messages(
                    state=state,
                    lark=FakeLarkClient(messages),
                    target=LarkTarget("Perfecto", "perfecto", "chat-a"),
                    api=object(),
                    http_client=object(),
                    group_openid="group-a",
                )

            self.assertEqual((pending_count, forwarded_count), (2, 2))
            self.assertEqual(send.await_count, 2)
            self.assertEqual(state.message_position, 12)
            self.assertTrue(state.has_processed_message("message-11"))
            self.assertTrue(state.has_processed_message("message-12"))

    async def test_send_failure_does_not_advance_cursor(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state = StateStore.load(Path(directory) / "state.json")
            state.prime_lark(
                chat_id="chat-a",
                sender_id="perfecto",
                latest_position=10,
            )
            messages = [
                LarkMessage("message-11", 11, "text", "perfecto", "会失败"),
            ]

            with patch(
                "qq_bridge.send_group_text",
                new=AsyncMock(side_effect=BridgeError("发送失败")),
            ):
                with self.assertRaises(BridgeError):
                    await process_pending_messages(
                        state=state,
                        lark=FakeLarkClient(messages),
                        target=LarkTarget("Perfecto", "perfecto", "chat-a"),
                        api=object(),
                        http_client=object(),
                        group_openid="group-a",
                    )

            self.assertEqual(state.message_position, 10)
            self.assertFalse(state.has_processed_message("message-11"))

    async def test_replayed_trigger_has_no_pending_messages(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state = StateStore.load(Path(directory) / "state.json")
            state.prime_lark(
                chat_id="chat-a",
                sender_id="perfecto",
                latest_position=10,
            )
            messages = [
                LarkMessage("message-11", 11, "text", "perfecto", "只发一次"),
            ]
            sender = AsyncMock(return_value={"id": "qq"})

            with patch("qq_bridge.send_group_text", new=sender):
                await process_pending_messages(
                    state=state,
                    lark=FakeLarkClient(messages),
                    target=LarkTarget("Perfecto", "perfecto", "chat-a"),
                    api=object(),
                    http_client=object(),
                    group_openid="group-a",
                )
                second = await process_pending_messages(
                    state=state,
                    lark=FakeLarkClient(messages),
                    target=LarkTarget("Perfecto", "perfecto", "chat-a"),
                    api=object(),
                    http_client=object(),
                    group_openid="group-a",
                )

            self.assertEqual(second, (0, 0))
            self.assertEqual(sender.await_count, 1)


if __name__ == "__main__":
    unittest.main()
