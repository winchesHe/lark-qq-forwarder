"""双来源投递演示，只使用临时数据和模拟发送端，不连接真实账号。"""

import asyncio
import json
import tempfile
from pathlib import Path

from delivery_dispatcher import DeliveryDispatcher, DeliveryStore, RetryDelivery


async def demo() -> dict:
    with tempfile.TemporaryDirectory(prefix="delivery-demo-") as directory:
        store = DeliveryStore(Path(directory) / "queue.sqlite3")
        stop = asyncio.Event()
        delivered, attempts = [], []
        active_targets = set()
        max_parallel = 0

        async def collect(source):
            for position in range(1, 4):
                store.enqueue(source, str(position), ["演示群A", "演示群B"], {"text": "模拟文本"})
                # 实际适配器在这里持久化采集进度；重放相同消息入队不会重复投递。
                await asyncio.sleep(0)

        async def send(task):
            nonlocal max_parallel
            if task.target in active_targets:
                raise AssertionError("同一群发生并发发送")
            active_targets.add(task.target)
            max_parallel = max(max_parallel, len(active_targets))
            attempts.append((task.source, task.message, task.target))
            try:
                await asyncio.sleep(0.005)
                if (task.source, task.message, task.target, task.attempts) == ("飞书:演示源", "1", "演示群A", 1):
                    raise RetryDelivery(0.02)
                delivered.append((task.source, task.message, task.target))
            finally:
                active_targets.remove(task.target)

        dispatcher = DeliveryDispatcher(store, send, concurrency=2, interval=0, poll_interval=0.001)
        runner = asyncio.create_task(dispatcher.run(stop))
        try:
            await asyncio.gather(collect("飞书:演示源"), collect("QQ:演示源"))
            async with asyncio.timeout(5):
                while store.counts().get("done") != 12:
                    if runner.done():
                        runner.result()
                    await asyncio.sleep(0.001)
            for source in ("飞书:演示源", "QQ:演示源"):
                for target in ("演示群A", "演示群B"):
                    assert [message for s, message, t in delivered if (s, t) == (source, target)] == ["1", "2", "3"]
            assert len(set(delivered)) == 12 and max_parallel == 2
            return {"模式": "模拟发送，未连接真实账号", "来源数": 2, "目标群数": 2,
                    "成功投递": len(delivered), "发送尝试": len(attempts),
                    "最大并行群数": max_parallel, "来源内顺序": "通过"}
        finally:
            stop.set()
            await runner
            store.close()


if __name__ == "__main__":
    print(json.dumps(asyncio.run(demo()), ensure_ascii=False, indent=2))
