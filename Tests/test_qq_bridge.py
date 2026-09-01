import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from qq_bridge import (
    BridgeError,
    LarkMessage,
    LarkTarget,
    StateStore,
    extract_image_key,
    extract_lark_messages,
    format_lark_text,
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


class ForwardingTests(unittest.IsolatedAsyncioTestCase):
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
