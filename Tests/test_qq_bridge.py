import json
import tempfile
import unittest
from pathlib import Path

from qq_bridge import StateStore, format_notification, read_next_record


class StateStoreTests(unittest.TestCase):
    def test_first_prime_starts_at_end(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            input_path = root / "notifications.jsonl"
            input_path.write_text('{"title":"历史消息"}\n', encoding="utf-8")
            state = StateStore.load(root / "state.json")

            offset = state.prime(input_path)

            self.assertEqual(offset, input_path.stat().st_size)

    def test_reads_only_record_appended_after_prime(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            input_path = root / "notifications.jsonl"
            input_path.write_text('{"title":"历史消息"}\n', encoding="utf-8")
            state = StateStore.load(root / "state.json")
            offset = state.prime(input_path)
            with input_path.open("a", encoding="utf-8") as handle:
                handle.write('{"title":"新消息","body":"正文"}\n')

            record = read_next_record(input_path, offset)

            self.assertIsNotNone(record)
            assert record is not None
            self.assertEqual(record.payload["title"], "新消息")


class MessageFormattingTests(unittest.TestCase):
    def test_formats_title_subtitle_and_body(self) -> None:
        message = format_notification(
            {"title": "项目群", "subtitle": "小明", "body": "收到请回复"}
        )

        self.assertEqual(message, "【飞书通知】\n项目群\n小明\n收到请回复")

    def test_ignores_duplicate_subtitle(self) -> None:
        message = format_notification(
            {"title": "小明", "subtitle": "小明", "body": "测试"}
        )

        self.assertEqual(message, "【飞书通知】\n小明\n测试")


if __name__ == "__main__":
    unittest.main()
