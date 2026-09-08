import asyncio
import os
import tempfile
import unittest
from pathlib import Path

from delivery_dispatcher import (
    DeliveryDispatcher, DeliveryStore, PauseTarget, QueueFull, RetryDelivery,
)


class DeliveryStoreTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "queue.sqlite3"
        self.store = DeliveryStore(self.path, max_pending=4, max_records=6)
        self.addCleanup(lambda: self.store.close())

    def put(self, source, message, targets):
        return self.store.enqueue(source, message, targets, {"text": "演示正文"})

    def test_atomic_fanout_dedup_capacity_and_payload_cleanup(self):
        self.assertEqual(self.put("lark:a", "1", ["A", "B", "B"]), 2)
        self.assertEqual(self.put("lark:a", "1", ["A", "B"]), 0)
        self.put("qq:b", "1", ["A"])
        with self.assertRaises(QueueFull):
            self.put("qq:b", "2", ["A", "B"])
        self.assertEqual(self.store.counts(), {"pending": 3})
        task = self.store.claim(0, 0)
        self.store.complete(task)
        self.assertEqual(self.put("lark:a", "1", ["A"]), 0)
        row = self.store.db.execute("SELECT payload FROM deliveries WHERE id=?", (task.id,)).fetchone()
        self.assertIsNone(row[0])
        self.assertEqual(os.stat(self.path).st_mode & 0o777, 0o600)

    def test_per_source_order_fairness_and_single_target_inflight(self):
        self.put("lark:a", "1", ["A"])
        self.put("lark:a", "2", ["A"])
        self.put("qq:b", "1", ["A"])
        first = self.store.claim(0, 0)
        self.assertEqual((first.source, first.message), ("lark:a", "1"))
        self.assertIsNone(self.store.claim(0, 0))
        self.store.complete(first)
        second = self.store.claim(0, 0)
        self.assertEqual(second.source, "qq:b")
        self.store.complete(second)
        self.assertEqual(self.store.claim(0, 0).message, "2")

    def test_retry_blocks_only_own_lane_and_preserves_order(self):
        self.put("lark:a", "1", ["A"])
        self.put("lark:a", "2", ["A"])
        self.put("qq:b", "1", ["A"])
        failed = self.store.claim(0, 0)
        self.store.retry(failed, 10)
        other = self.store.claim(0, 0)
        self.assertEqual(other.source, "qq:b")
        self.store.complete(other)
        self.assertIsNone(self.store.claim(9, 0))
        self.assertEqual(self.store.claim(10, 0).id, failed.id)

    def test_target_and_account_backoff_and_manual_recovery(self):
        self.put("lark:a", "1", ["A", "B"])
        first = self.store.claim(0, 0)
        self.store.retry(first, 5, "target")
        second = self.store.claim(0, 0)
        self.assertEqual(second.target, "B")
        self.store.retry(second, 10, "account")
        self.assertIsNone(self.store.claim(9, 0))
        recovered = self.store.claim(10, 0)
        self.store.pause_target(recovered)
        other = self.store.claim(10, 0)
        self.assertNotEqual(other.target, recovered.target)
        self.store.complete(other)
        self.assertIsNone(self.store.claim(10, 0))
        self.store.resume_target(recovered.target)
        task = self.store.claim(10, 0)
        self.store.retry(task, 10, blocked=True)
        self.assertIsNone(self.store.claim(10, 0))
        self.store.resume_delivery(task.id)
        self.assertEqual(self.store.claim(10, 0).attempts, 1)

    def test_restart_recovers_inflight_and_keeps_success_dedup(self):
        self.put("lark:a", "1", ["A", "B"])
        done = self.store.claim(0, 0)
        self.store.complete(done)
        inflight = self.store.claim(0, 0)
        with self.assertRaisesRegex(RuntimeError, "占用"):
            DeliveryStore(self.path)
        self.store.close()
        self.store = DeliveryStore(self.path)
        self.assertEqual(self.put("lark:a", "1", ["A", "B"]), 0)
        self.assertEqual(self.store.claim(0, 0).id, inflight.id)

    def test_rate_limit_and_invalid_payload_and_record_bound(self):
        with self.assertRaises(ValueError):
            self.put("", "1", ["A"])
        with self.assertRaises(ValueError):
            self.store.enqueue("a", "1", ["A"], {"text": "x" * 65536})
        for i in range(6):
            self.put("a", str(i), ["A"])
            self.store.complete(self.store.claim(i, 1))
            self.assertIsNone(self.store.claim(i + 0.5, 1))
        with self.assertRaises(QueueFull):
            self.put("a", "7", ["A"])
        self.assertEqual(self.store.counts(), {"done": 6})


class DispatcherTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.store = DeliveryStore(Path(self.temp.name) / "queue.sqlite3")
        self.addCleanup(self.store.close)

    async def wait_until(self, predicate):
        async def wait():
            while not predicate():
                await asyncio.sleep(0.001)
        await asyncio.wait_for(wait(), 2)

    def dispatcher(self, send, **kwargs):
        return DeliveryDispatcher(self.store, send, interval=0, poll_interval=0.001, **kwargs)

    async def test_parallel_targets_bounded_concurrency_and_graceful_stop(self):
        for i in range(3):
            self.store.enqueue("lark:a", str(i), ["A", "B", "C"], {"text": "演示"})
        started, active = [], set()
        release, stop = asyncio.Event(), asyncio.Event()

        async def send(task):
            self.assertNotIn(task.target, active)
            active.add(task.target)
            started.append(task)
            await release.wait()
            active.remove(task.target)

        runner = asyncio.create_task(self.dispatcher(send, concurrency=2).run(stop))
        await self.wait_until(lambda: len(started) == 2)
        self.assertEqual(len(active), 2)
        stop.set()
        await asyncio.sleep(0.01)
        self.assertFalse(runner.done())
        release.set()
        await runner
        self.assertEqual(len(started), 2)
        self.assertEqual(self.store.counts(), {"done": 2, "pending": 7})

    async def test_retry_releases_worker_and_other_source_overtakes(self):
        self.store.enqueue("lark:a", "1", ["A"], {})
        self.store.enqueue("lark:a", "2", ["A"], {})
        self.store.enqueue("qq:b", "1", ["A"], {})
        calls, stop = [], asyncio.Event()

        async def send(task):
            calls.append((task.source, task.message))
            if task.source == "lark:a" and task.message == "1" and task.attempts == 1:
                raise RetryDelivery(0.03)

        runner = asyncio.create_task(self.dispatcher(send, concurrency=1).run(stop))
        await self.wait_until(lambda: self.store.counts().get("done") == 3)
        stop.set()
        await runner
        self.assertEqual(calls, [("lark:a", "1"), ("qq:b", "1"), ("lark:a", "1"), ("lark:a", "2")])

    async def test_timeout_skips_after_budget_and_next_message_completes(self):
        self.store.enqueue("a", "1", ["A", "B"], {})
        self.store.enqueue("a", "2", ["A"], {})
        stop = asyncio.Event()

        async def send(task):
            if task.target == "A" and task.message == "1":
                await asyncio.Event().wait()

        runner = asyncio.create_task(self.dispatcher(send, timeout=0.01, max_attempts=2, retry_base=0).run(stop))
        await self.wait_until(lambda: self.store.counts().get("done") == 3)
        stop.set()
        await runner
        self.assertEqual(self.store.counts(), {"done": 3})
        failed = self.store.db.execute("SELECT id,attempts,payload FROM deliveries WHERE target='A' AND message='1'").fetchone()
        self.assertEqual((failed["attempts"], failed["payload"]), (2, None))
        self.assertEqual(self.store.db.execute("SELECT outcome FROM delivery_results WHERE id=?", (failed["id"],)).fetchone()[0], "retry_exhausted")

    async def test_exhausted_account_retry_keeps_backoff_for_other_messages(self):
        self.store.enqueue("a", "1", ["A"], {})
        self.store.enqueue("a", "2", ["A"], {})
        task = self.store.claim(0, 0)
        async def send(_task):
            raise RetryDelivery(60, "account")
        await self.dispatcher(send, max_attempts=1)._deliver(task)
        self.assertEqual(self.store.counts(), {"done": 1, "pending": 1})
        due = self.store.db.execute("SELECT due FROM scheduler").fetchone()[0]
        self.assertIsNone(self.store.claim(due - 1, 0))
        self.assertEqual(self.store.claim(due, 0).message, "2")

    async def test_restart_skips_old_exhausted_tasks_only_in_selected_scope(self):
        self.store.enqueue("a", "1", ["A"], {})
        self.store.enqueue("b", "1", ["B"], {})
        with self.store.db:
            self.store.db.execute("UPDATE deliveries SET status='blocked',attempts=5")
        ids = [row[0] for row in self.store.db.execute("SELECT id FROM deliveries ORDER BY id")]
        stop = asyncio.Event()
        stop.set()
        async def send(_task):
            self.fail("已耗尽任务不能再次发送")
        await self.dispatcher(send, ids=[ids[0]]).run(stop)
        self.assertEqual(self.store.counts(), {"done": 1, "blocked": 1})
        await self.dispatcher(send).run(stop)
        self.assertEqual(self.store.counts(), {"done": 2})

    async def test_cancellation_releases_claim_for_next_start(self):
        self.store.enqueue("qq:a", "1", ["A"], {})
        entered = asyncio.Event()

        async def send(task):
            entered.set()
            await asyncio.Event().wait()

        dispatcher = self.dispatcher(send)
        runner = asyncio.create_task(dispatcher.run(asyncio.Event()))
        await entered.wait()
        runner.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await runner
        self.assertEqual(self.store.counts(), {"pending": 1})

    async def test_target_pause_leaves_other_group_running(self):
        self.store.enqueue("qq:a", "1", ["A", "B"], {})
        stop = asyncio.Event()

        async def send(task):
            if task.target == "A":
                raise PauseTarget()

        runner = asyncio.create_task(self.dispatcher(send).run(stop))
        await self.wait_until(lambda: self.store.counts().get("done") == 1)
        stop.set()
        await runner
        self.assertEqual(self.store.counts(), {"done": 1, "pending": 1})


if __name__ == "__main__":
    unittest.main()
