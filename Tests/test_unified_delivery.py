import asyncio
import json
import tempfile
import time
import unittest
import uuid
from pathlib import Path
from unittest.mock import AsyncMock, patch

from delivery_dispatcher import DeliveryDispatcher, QueueFull, RetryDelivery, SkipDelivery
from qq_bridge import BridgeError, ChannelCursorStore, ForwarderProcessLock, LarkMessage, StateStore
from unified_collectors import LarkSource, QQNotificationCollector, collect_lark_source, publish_lark_event
from unified_replay import replay_unified
from unified_sender import QQDeliverySender, translate_failure
from unified_service import read_unified_status, run_unified, submit_text, wait_for_deliveries
from unified_store import QUEUE_FILE, UnifiedStore


class FakeAPI:
    def __init__(self):
        self.calls = []
        self.sequence = 0

    def next_msg_seq(self):
        self.sequence += 1
        return self.sequence

    async def post_group_message(self, target, message):
        self.calls.append((target, message))
        await asyncio.sleep(0)
        return {"id": str(len(self.calls))}


class FakeLark:
    def __init__(self, messages=()):
        self.messages = list(messages)
        self.downloads = []

    def list_messages_since(self, chat, cursor):
        return [message for message in self.messages if message.position > cursor]

    def download_image(self, *, message_id, image_key, output_directory):
        self.downloads.append((message_id, image_key))
        path = output_directory / "image.jpg"
        path.write_bytes(b"image-fixture")
        return path

    def resolve_target(self, name):
        from qq_bridge import LarkTarget
        return LarkTarget(name=name, chat_id="chat", sender_id="sender")


class UnifiedTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.state_path = self.root / "state.json"
        self.state = StateStore.load(self.state_path)
        self.state.bind_group("A")
        self.state.add_group_binding("B")
        self.state.prime_lark(chat_id="chat", sender_id="sender", latest_position=0)
        self.queue_path = self.root / QUEUE_FILE
        self.store = UnifiedStore(self.queue_path)
        self.addCleanup(lambda: self.store.close() if self.store else None)
        self.source = LarkSource("演示", "chat", "sender", 0, "contact")
        self.api, self.client = FakeAPI(), AsyncMock()
        self.channels = self.root / "channels.json"
        self.channels.write_text(json.dumps({"channels": [{"name": "演示频道", "chat_id": "channel-chat", "cursor_position": 0, "initial_cursor_position": 0, "recent_message_ids": []}]}))

    def capture(self, message="1", targets=None, payload=None, source="a", position=1, metadata=None):
        return self.store.capture(source=source, message=message, position=position,
            targets=targets if targets is not None else ["A"],
            payload=payload or {"parts": [{"type": "text", "text": "演示正文"}], "next_part": 0},
            metadata=metadata or {"kind": "manual"})

    async def drain(self, ids, lark=None, **options):
        sender = QQDeliverySender(self.store, self.state_path, self.api, self.client, lark or FakeLark())
        stop = asyncio.Event()
        runner = asyncio.create_task(DeliveryDispatcher(self.store, sender, interval=0,
            poll_interval=0.001, retry_base=0.001, ids=ids, **options).run(stop))
        try:
            async with asyncio.timeout(3):
                while not all(row["status"] == "done" for row in self.store.outcomes(ids)):
                    if runner.done():
                        runner.result()
                    await asyncio.sleep(0.005)
        finally:
            stop.set()
            await runner
            await sender.close()

    def test_event_and_highwater_are_atomic_and_route_snapshot_is_immutable(self):
        self.store.max_pending = 1
        with self.assertRaises(QueueFull):
            self.capture(targets=["A", "B"])
        self.assertEqual(self.store.position("a"), 0)
        self.assertEqual(self.store.counts(), {})
        ids = self.capture(targets=["A"])
        self.assertEqual(self.capture(targets=["A", "B"]), ids)
        self.assertEqual(self.store.position("a"), 1)

    def test_producer_does_not_reset_active_and_cannot_dispatch(self):
        self.capture()
        claimed = self.store.claim(time.time(), 0)
        producer = UnifiedStore(self.queue_path, owner=False)
        try:
            self.assertEqual(producer.counts(), {"active": 1})
            with self.assertRaises(RuntimeError):
                producer.claim(time.time(), 0)
            producer.capture(source="b", message="1", position=1, targets=["B"], payload={}, metadata={})
            self.assertEqual(producer.counts(), {"active": 1, "pending": 1})
        finally:
            producer.close()
        self.store.complete(claimed)

    def test_confirmed_cursor_waits_for_earlier_targets_and_export_is_repeatable(self):
        self.capture("1", targets=["A", "B"], position=1)
        self.capture("2", targets=["A"], position=2)
        task = self.store.claim(0, 0)
        self.store.complete(task)
        stalled = self.store.claim(0, 0)
        self.store.retry(stalled, 100)
        task = self.store.claim(0, 0)
        self.store.complete(task)
        published = []
        self.assertEqual(self.store.export_completed(lambda *args: published.append(args)), 0)
        self.store.complete(self.store.claim(100, 0))
        self.assertEqual(self.store.export_completed(lambda *args: published.append(args)), 2)
        self.assertEqual([args[2] for args in published], [1, 2])
        self.assertEqual(self.store.export_completed(lambda *args: published.append(args)), 0)

    def test_gc_keeps_pending_and_existing_spool_receipt(self):
        spool = self.root / "notification.json"
        spool.write_text("fixture")
        ids = self.capture(metadata={"kind": "qq", "spool": str(spool)})
        self.store.complete(self.store.claim(0, 0))
        self.store.export_completed(lambda *_: None)
        self.store.prune(retention_seconds=-1)
        self.assertEqual(len(self.store.outcomes(ids)), 1)
        spool.unlink()
        self.store.prune(retention_seconds=-1)
        self.assertEqual(self.store.outcomes(ids), [])
        self.capture("2", position=2)
        self.store.prune(retention_seconds=-1)
        self.assertEqual(self.store.counts(), {"pending": 1})

    async def test_lark_capture_decouples_progress_and_migrates_partial_delivery(self):
        self.state.mark_delivery("演示", "A", "1")
        lark = FakeLark([LarkMessage("1", 1, "text", "sender", "第一条")])
        await collect_lark_source(self.store, self.source, lark, self.state_path, self.root / "routing.json")
        self.assertEqual(StateStore.load(self.state_path).message_position, 0)
        self.assertEqual(self.store.position(self.source.key), 1)
        ids = self.store.event_ids(self.source.key, "1")
        self.assertEqual(len(ids), 1)
        await self.drain(ids)
        self.assertEqual([target for target, _ in self.api.calls], ["B"])
        self.store.export_completed(lambda *args: publish_lark_event(*args, state_path=self.state_path,
            channels_path=self.channels, listeners_path=self.root / "listener-cursors.json"))
        self.assertEqual(StateStore.load(self.state_path).message_position, 1)
        await collect_lark_source(self.store, self.source, lark, self.state_path, self.root / "routing.json")
        self.assertEqual(len(self.api.calls), 1)

    async def test_lark_read_failure_and_oversize_keep_capture_position(self):
        lark = FakeLark()
        lark.list_messages_since = lambda *_: (_ for _ in ()).throw(BridgeError("读取失败"))
        with self.assertRaises(BridgeError):
            await collect_lark_source(self.store, self.source, lark, self.state_path, self.root / "routing.json")
        self.assertEqual(self.store.position(self.source.key), 0)
        self.store.max_payload_bytes = 10
        with self.assertRaises(QueueFull):
            await collect_lark_source(self.store, self.source, FakeLark([LarkMessage("1", 1, "text", "sender", "正文")]), self.state_path, self.root / "routing.json")
        self.assertEqual(self.store.position(self.source.key), 0)

    async def test_image_preparation_is_shared_and_parts_resume_without_repeating_text(self):
        image = {"type": "image", "message_id": "1", "image_key": "img_1234567890"}
        ids = self.capture(targets=["A", "B"], payload={"parts": [{"type": "text", "text": "图文"}, image], "next_part": 0})
        lark = FakeLark()
        upload = AsyncMock(return_value="file-token")
        original = self.api.post_group_message
        failed = False
        async def send(target, message):
            nonlocal failed
            if target == "A" and message.media and not failed:
                failed = True
                raise RuntimeError("temporary")
            return await original(target, message)
        self.api.post_group_message = send
        with patch("unified_sender.MediaUploader.upload", upload):
            await self.drain(ids, lark)
        self.assertEqual(len(lark.downloads), 1)
        self.assertEqual(upload.await_count, 2)
        self.assertEqual(sum(message.content == "图文" for _, message in self.api.calls), 2)
        self.assertEqual([row["sent_parts"] for row in self.store.outcomes(ids)], [2, 2])

    async def test_text_preserved_and_rejection_is_not_success(self):
        content = "精确正文\n第二行 "
        ids = self.capture(payload={"parts": [{"type": "text", "text": content}], "next_part": 0})
        await self.drain(ids)
        self.assertEqual(self.api.calls[0][1].content, content)
        rejected = self.capture("2", position=2)
        self.api.post_group_message = AsyncMock(side_effect=RuntimeError("消息内容违规"))
        await self.drain(rejected)
        self.assertEqual(self.store.outcomes(rejected)[0]["outcome"], "skipped")

    def test_error_classification_does_not_depend_on_unverified_business_codes(self):
        self.assertIsInstance(translate_failure(RuntimeError("消息内容违规")), SkipDelivery)
        failure = translate_failure(RuntimeError("QQ Bot API error [429] /messages: limited"))
        self.assertIsInstance(failure, RetryDelivery)
        self.assertEqual(failure.scope, "account")
        self.assertIsInstance(translate_failure(RuntimeError("unknown 400 error")), BridgeError)

    def test_timeout_cancellation_during_preparation_cannot_send_later(self):
        ids = self.capture()
        task = self.store.claim(0, 0)
        producer = UnifiedStore(self.queue_path, owner=False)
        producer.cancel_pending(ids)
        producer.close()
        self.store.defer(task, 1)
        self.assertEqual(self.store.outcomes(ids)[0]["outcome"], "cancelled")
        self.assertIsNone(self.store.claim(2, 0))

    def qq_fixture(self, *, event_id=None, title="演示源群", body="张三：正文", subtitle="", filename="0001.json"):
        path = self.root / filename
        path.write_text(json.dumps({"type": "qq_notification", "schema_version": 1,
            "event_id": event_id or str(uuid.uuid4()), "bundle_id": "com.tencent.qq",
            "title": title, "body": body, "subtitle": subtitle}))
        return path

    def qq_collector(self):
        path = self.root / "qq-rules.json"
        binding = self.state.group_bindings[0]["binding_id"]
        path.write_text(json.dumps({"schema_version": 1, "rules": [
            {"id": "all", "group_name": "演示源群", "sender": "", "enabled": True, "binding_ids": [binding]},
            {"id": "sender", "group_name": "演示源群", "sender": "张三", "enabled": True, "binding_ids": [binding]},
        ]}))
        return QQNotificationCollector(self.store, self.root, path, self.state_path)

    def test_qq_overlap_rules_dedup_occurrence_but_keep_identical_messages(self):
        collector = self.qq_collector()
        event = str(uuid.uuid4())
        self.assertEqual(collector.collect_file(self.qq_fixture(event_id=event)), "captured")
        collector.collect_file(self.qq_fixture(event_id=event))
        self.assertEqual(self.store.counts(), {"pending": 1})
        collector.collect_file(self.qq_fixture())
        self.assertEqual(self.store.counts(), {"pending": 2})

    def test_qq_summary_and_unmatched_are_not_sent_and_full_queue_keeps_file(self):
        collector = self.qq_collector()
        self.assertEqual(collector.collect_file(self.qq_fixture(body="你有3条新消息")), "summary")
        self.assertEqual(collector.collect_file(self.qq_fixture(title="其他源群")), "unmatched")
        self.assertEqual(self.store.counts(), {})
        self.store.max_pending = 1
        collector.collect_file(self.qq_fixture())
        path = self.qq_fixture()
        with self.assertRaises(QueueFull):
            collector.collect_file(path)
        self.assertTrue(path.exists())

    async def test_manual_when_stopped_dispatches_only_requested_message(self):
        self.capture("old", source="lark:other", targets=["A"])
        self.store.close()
        self.store = None
        binding = self.state.group_bindings[1]["binding_id"]
        await submit_text(self.state_path, [binding], "手动消息", api_factory=AsyncMock(return_value=(self.api, self.client)))
        self.assertEqual([target for target, _ in self.api.calls], ["B"])
        self.store = UnifiedStore(self.queue_path)
        self.assertEqual(self.store.counts(), {"done": 1, "pending": 1})
        self.assertEqual(self.store.db.execute("SELECT exported FROM source_events WHERE message='old'").fetchone()[0], 0)

    async def test_manual_while_running_uses_existing_sender(self):
        binding = self.state.group_bindings[0]["binding_id"]
        stop = asyncio.Event()
        sender = QQDeliverySender(self.store, self.state_path, self.api, self.client, FakeLark())
        runner = asyncio.create_task(DeliveryDispatcher(self.store, sender, interval=0, poll_interval=0.001).run(stop))
        connect = AsyncMock(side_effect=AssertionError("不能另起发送连接"))
        try:
            with ForwarderProcessLock(self.state_path.with_name(".qq-forwarder.lock")):
                await submit_text(self.state_path, [binding], "经现有发送器", api_factory=connect)
        finally:
            stop.set()
            await runner
            await sender.close()
        connect.assert_not_awaited()
        self.assertEqual(len(self.api.calls), 1)

    async def test_old_running_service_rejects_new_sender_without_touching_queue(self):
        self.store.close()
        self.store = None
        with ForwarderProcessLock(self.state_path.with_name(".qq-forwarder.lock")):
            with self.assertRaisesRegex(BridgeError, "旧转发服务"):
                await submit_text(self.state_path, [self.state.group_bindings[0]["binding_id"]], "测试")

    async def test_replay_selection_uses_queue_and_preserves_cursor_and_progress(self):
        self.store.close()
        self.store = None
        lark = FakeLark([LarkMessage("one", 1, "text", "any", "一"), LarkMessage("two", 2, "text", "any", "二")])
        result = await replay_unified(channel_name="演示频道", channel_state_path=self.channels,
            state_path=self.state_path, lark_client=lark, message_ids={"two"},
            binding_ids={self.state.group_bindings[0]["binding_id"]}, progress_path=self.root / "progress.json",
            api_factory=AsyncMock(return_value=(self.api, self.client)))
        self.assertEqual((result.pending_count, result.forwarded_count, result.cursor_position), (1, 1, 2))
        self.assertEqual(len(self.api.calls), 1)
        self.assertEqual(json.loads((self.root / "progress.json").read_text())["processed_ids"], ["two"])

    async def test_running_service_collects_both_sources_and_stops_cleanly(self):
        binding = self.state.group_bindings[0]["binding_id"]
        (self.root / ".qq-notification-sources.json").write_text(json.dumps({"schema_version": 1, "rules": [
            {"id": "qq", "group_name": "演示源群", "sender": "", "enabled": True, "binding_ids": [binding]}]}))
        spool = self.root / "qq-notifications"
        spool.mkdir()
        path = self.qq_fixture()
        path.rename(spool / path.name)
        self.store.close()
        self.store = None
        stop = asyncio.Event()
        runner = asyncio.create_task(run_unified(state_path=self.state_path, input_path=self.root / "wakeups.jsonl",
            channels_path=self.root / "no-channels.json", listeners_path=self.root / "listeners.json",
            listener_cursors=self.root / "listener-cursors.json", routing_path=self.root / "routing.json",
            profile="test", contact="演示", stop=stop,
            lark_client=FakeLark([LarkMessage("one", 1, "text", "sender", "飞书正文")]),
            api_factory=AsyncMock(return_value=(self.api, self.client))))
        try:
            async with asyncio.timeout(5):
                while len(self.api.calls) < 3:
                    if runner.done():
                        runner.result()
                    await asyncio.sleep(0.01)
        finally:
            stop.set()
            await runner
        self.assertEqual(len(self.api.calls), 3)
        self.assertEqual(StateStore.load(self.state_path).message_position, 1)
        self.assertEqual(read_unified_status(self.queue_path)["state"], "stopped")
        self.client.aclose.assert_awaited_once()

    def test_export_failure_does_not_block_other_sources(self):
        from unified_store import ExportFailure
        self.capture("bad", targets=[], source="bad", metadata={"kind": "bad"})
        self.capture("good", targets=[], source="good", metadata={"kind": "good"})
        published = []
        def publish(metadata, *args):
            if metadata["kind"] == "bad":
                raise OSError("fixture")
            published.append(metadata["kind"])
        with self.assertRaises(ExportFailure):
            self.store.export_completed(publish)
        self.assertEqual(published, ["good"])
        self.assertEqual(self.store.db.execute("SELECT exported FROM source_events WHERE source='bad'").fetchone()[0], 0)

    async def test_force_end_discards_only_contact_backlog_and_default_prime_preserves_it(self):
        from qq_bridge import prime_forwarder
        contact_ids = self.capture("one", source=self.source.key)
        other_ids = self.capture("other", source="qq:other")
        self.store.close()
        self.store = None
        lark = FakeLark([LarkMessage("latest", 10, "text", "sender", "正文")])
        lark.list_messages = lambda *_: lark.messages
        args = dict(state=self.state, input_path=self.root / "wakeups.jsonl", lark=lark, contact_name="演示")
        await prime_forwarder(**args, force_end=False)
        self.store = UnifiedStore(self.queue_path)
        self.assertEqual(self.store.outcomes(contact_ids)[0]["status"], "pending")
        self.store.close()
        self.store = None
        await prime_forwarder(**args, force_end=True)
        self.store = UnifiedStore(self.queue_path)
        self.assertEqual(self.store.outcomes(contact_ids)[0]["outcome"], "cancelled")
        self.assertEqual(self.store.outcomes(other_ids)[0]["status"], "pending")
        self.assertEqual(self.store.position(self.source.key), 10)
        self.assertEqual(StateStore.load(self.state_path).message_position, 10)
        with ForwarderProcessLock(self.state_path.with_name(".qq-forwarder.lock")):
            with self.assertRaises(BridgeError):
                await prime_forwarder(**args, force_end=True)

    async def test_replay_preflight_rejects_without_capturing_and_filters_existing_targets(self):
        key = "lark:channel-chat:*"
        self.capture("old", source=key)
        self.store.close()
        self.store = None
        lark = FakeLark([LarkMessage("new", 2, "text", "any", "正文")])
        kwargs = dict(channel_name="演示频道", channel_state_path=self.channels, state_path=self.state_path,
                      lark_client=lark, binding_ids={self.state.group_bindings[0]["binding_id"]},
                      api_factory=AsyncMock(return_value=(self.api, self.client)))
        with self.assertRaisesRegex(BridgeError, "更早"):
            await replay_unified(**kwargs)
        self.store = UnifiedStore(self.queue_path)
        self.assertEqual(self.store.event_ids(key, "new"), [])
        self.assertEqual(self.store.position(key), 1)
        self.store.discard_through(key, 1)
        ids = self.capture("new", source=key, position=2, targets=["A", "B"],
                           metadata=LarkSource("演示频道", "channel-chat", None, 0, "channel").metadata())
        self.store.close()
        self.store = None
        await replay_unified(**kwargs)
        self.store = UnifiedStore(self.queue_path)
        self.assertEqual([target for target, _ in self.api.calls], ["A"])
        self.assertEqual([row["status"] for row in self.store.outcomes(ids)], ["done", "pending"])
        self.assertEqual(ChannelCursorStore.load(self.channels).get("演示频道").cursor_position, 0)

    def test_recovery_is_atomic_and_status_excludes_payload_and_stale_activity(self):
        from unified_service import queue_tasks, retry_queued
        ids = self.capture()
        task = self.store.claim(0, 0)
        self.store.retry(task, 100, blocked=True)
        with self.assertRaises(BridgeError):
            retry_queued(self.state_path, [ids[0], 9999])
        self.assertEqual(self.store.outcomes(ids)[0]["status"], "blocked")
        listing = queue_tasks(self.state_path)
        self.assertEqual(listing[0]["id"], ids[0])
        self.assertNotIn("演示正文", json.dumps(listing, ensure_ascii=False))
        retry_queued(self.state_path, ids)
        self.assertEqual(self.store.outcomes(ids)[0]["status"], "pending")
        self.store.set_status("service", "running")
        self.store.set_status("qq", "failed", "通知无法读取")
        self.store.set_status("lark", "running")
        self.assertEqual(read_unified_status(self.queue_path)["state"], "degraded")
        self.assertNotIn("演示正文", json.dumps(read_unified_status(self.queue_path), ensure_ascii=False))
        self.store.close()
        self.store = None
        self.assertEqual(read_unified_status(self.queue_path)["state"], "stopped")

    async def test_replay_migrates_recent_confirmation_without_resending(self):
        data = json.loads(self.channels.read_text())
        data["channels"][0]["recent_message_ids"] = ["one"]
        self.channels.write_text(json.dumps(data))
        self.store.close()
        self.store = None
        result = await replay_unified(channel_name="演示频道", channel_state_path=self.channels,
            state_path=self.state_path, lark_client=FakeLark([LarkMessage("one", 1, "text", "any", "正文")]),
            api_factory=AsyncMock(return_value=(self.api, self.client)))
        self.assertEqual((result.forwarded_count, result.skipped_count), (0, 1))
        self.assertEqual(self.api.calls, [])

    async def test_replay_failed_capture_does_not_mark_uncaptured_messages_processed(self):
        self.store.close()
        self.store = None
        progress = self.root / "progress.json"
        with patch("unified_replay.collect_lark_source", side_effect=QueueFull("fixture")):
            with self.assertRaises(QueueFull):
                await replay_unified(channel_name="演示频道", channel_state_path=self.channels,
                    state_path=self.state_path, lark_client=FakeLark([LarkMessage("one", 1, "text", "any", "正文")]),
                    progress_path=progress, api_factory=AsyncMock(return_value=(self.api, self.client)))
        self.assertEqual(json.loads(progress.read_text())["processed_ids"], [])
        self.assertEqual(self.api.calls, [])


if __name__ == "__main__":
    unittest.main()
