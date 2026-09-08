"""统一投递骨架：单进程拥有队列，采集与发送通过持久化任务解耦。"""

import asyncio
import fcntl
import json
import logging
import math
import os
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Awaitable, Callable


class QueueFull(RuntimeError):
    """采集方必须保留原进度，稍后重试整个入队事务。"""


class RetryDelivery(Exception):
    def __init__(self, delay: float, scope: str = "message") -> None:
        if scope not in {"message", "target", "account"} or not math.isfinite(delay) or delay < 0:
            raise ValueError("重试范围或延迟无效")
        self.delay, self.scope = delay, scope


class ContinueDelivery(Exception):
    """当前分段已确认，持久化剩余任务后重新参与公平调度。"""

    def __init__(self, payload: dict) -> None:
        self.payload = payload


class DeferDelivery(Exception):
    """等待图片准备等前置工作，不消耗发送重试预算。"""

    def __init__(self, delay: float = 0.25) -> None:
        self.delay = delay


class SkipDelivery(Exception):
    """内容被明确拒绝，不再自动重试。"""


class PauseTarget(Exception):
    """目标群失效；保留任务并等待显式恢复。"""


@dataclass(frozen=True)
class Delivery:
    id: int
    source: str
    message: str
    target: str
    payload: dict
    attempts: int


class DeliveryStore:
    """所有方法由拥有者事件循环调用；不允许跨线程或多个服务同时调度。"""

    def __init__(self, path: Path, *, max_pending: int = 10000,
                 max_records: int = 100000, max_payload_bytes: int = 65536,
                 owner: bool = True) -> None:
        if min(max_pending, max_records, max_payload_bytes) < 1:
            raise ValueError("队列容量必须为正数")
        self.max_pending, self.max_records = max_pending, max_records
        self.max_payload_bytes = max_payload_bytes
        path.parent.mkdir(parents=True, exist_ok=True)
        self.owner = owner
        self._lock = None
        if owner:
            self._lock = os.open(str(path) + ".lock", os.O_CREAT | os.O_RDWR, 0o600)
            try:
                fcntl.flock(self._lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                os.close(self._lock)
                raise RuntimeError("投递队列已被另一个服务占用") from None
        try:
            fd = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)
            os.close(fd)
            os.chmod(path, 0o600)
            self.db = sqlite3.connect(path)
            self.db.row_factory = sqlite3.Row
            self.db.execute("PRAGMA secure_delete=ON")
            self.db.executescript("""
                CREATE TABLE IF NOT EXISTS deliveries (
                    id INTEGER PRIMARY KEY, source TEXT NOT NULL, message TEXT NOT NULL,
                    target TEXT NOT NULL, payload TEXT, status TEXT NOT NULL DEFAULT 'pending',
                    attempts INTEGER NOT NULL DEFAULT 0, due REAL NOT NULL DEFAULT 0,
                    UNIQUE(source, message, target)
                );
                CREATE INDEX IF NOT EXISTS delivery_lane ON deliveries(source, target, status, id);
                CREATE INDEX IF NOT EXISTS delivery_status ON deliveries(status, target);
                CREATE TABLE IF NOT EXISTS lanes (
                    source TEXT, target TEXT, turn INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY(source, target)
                );
                CREATE TABLE IF NOT EXISTS targets (
                    target TEXT PRIMARY KEY, turn INTEGER NOT NULL DEFAULT 0,
                    due REAL NOT NULL DEFAULT 0, paused INTEGER NOT NULL DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS delivery_results (
                    id INTEGER PRIMARY KEY, outcome TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS delivery_parts (
                    id INTEGER NOT NULL, part INTEGER NOT NULL, PRIMARY KEY(id,part)
                );
                CREATE TABLE IF NOT EXISTS scheduler (
                    id INTEGER PRIMARY KEY CHECK(id=1), turn INTEGER NOT NULL, due REAL NOT NULL
                );
                INSERT OR IGNORE INTO scheduler VALUES(1, 0, 0);
            """)
            # 进程退出后无法确认在途请求结果，恢复为待发送；不宣称恰好一次投递。
            if owner:
                with self.db:
                    self.db.execute("UPDATE deliveries SET status='pending' WHERE status='active'")
        except BaseException:
            if hasattr(self, "db"):
                self.db.close()
            if self._lock is not None:
                os.close(self._lock)
            raise

    def close(self) -> None:
        self.db.close()
        if self._lock is not None:
            os.close(self._lock)

    def enqueue(self, source: str, message: str, targets: list[str], payload: dict) -> int:
        """整条消息的所有目标原子入队；返回后采集方才可提交自己的进度。"""
        if (not isinstance(source, str) or not source or not isinstance(message, str) or not message
                or not isinstance(targets, list) or not targets
                or any(not isinstance(target, str) or not target for target in targets)
                or not isinstance(payload, dict)):
            raise ValueError("来源、消息、目标与负载不能为空或类型错误")
        encoded = json.dumps(payload, ensure_ascii=False, allow_nan=False)
        if len(encoded.encode("utf-8")) > self.max_payload_bytes:
            raise ValueError("单条投递负载超过限制")
        with self.db:
            self.db.execute("BEGIN IMMEDIATE")
            return self._enqueue(source, message, targets, encoded)

    def _enqueue(self, source: str, message: str, targets: list[str], encoded: str) -> int:
        missing = [target for target in dict.fromkeys(targets) if not self.db.execute(
            "SELECT 1 FROM deliveries WHERE source=? AND message=? AND target=?",
            (source, message, target)).fetchone()]
        counts = self.db.execute(
            "SELECT COUNT(*), COALESCE(SUM(status != 'done'), 0) FROM deliveries").fetchone()
        if missing and (counts[0] + len(missing) > self.max_records
                        or counts[1] + len(missing) > self.max_pending):
            raise QueueFull("投递队列已满，采集进度不能推进")
        for target in missing:
            self.db.execute("INSERT OR IGNORE INTO targets(target) VALUES(?)", (target,))
            self.db.execute("INSERT OR IGNORE INTO lanes(source,target) VALUES(?,?)", (source, target))
            self.db.execute(
                "INSERT INTO deliveries(source,message,target,payload) VALUES(?,?,?,?)",
                (source, message, target, encoded))
        return len(missing)

    def claim(self, now: float, interval: float, ids: list[int] | None = None) -> Delivery | None:
        if not self.owner:
            raise RuntimeError("生产者不能领取投递任务")
        if ids == []:
            return None
        only_ids = "" if ids is None else " AND d.id IN (" + ",".join("?" for _ in ids) + ")"
        with self.db:
            self.db.execute("BEGIN IMMEDIATE")
            scheduler = self.db.execute("SELECT * FROM scheduler WHERE id=1").fetchone()
            if scheduler["due"] > now:
                return None
            row = self.db.execute(f"""
                SELECT d.* FROM deliveries d
                JOIN lanes l ON l.source=d.source AND l.target=d.target
                JOIN targets t ON t.target=d.target
                WHERE d.status='pending' {only_ids} AND d.due<=? AND t.due<=? AND t.paused=0
                  AND NOT EXISTS (SELECT 1 FROM deliveries a
                      WHERE a.target=d.target AND a.status='active')
                  AND NOT EXISTS (SELECT 1 FROM deliveries p
                      WHERE p.source=d.source AND p.target=d.target
                        AND p.id<d.id AND p.status!='done')
                ORDER BY t.turn, l.turn, d.id LIMIT 1
            """, (*(ids or []), now, now)).fetchone()
            if row is None:
                return None
            turn = scheduler["turn"] + 1
            self.db.execute("UPDATE scheduler SET turn=?, due=? WHERE id=1", (turn, now + interval))
            self.db.execute("UPDATE targets SET turn=? WHERE target=?", (turn, row["target"]))
            self.db.execute("UPDATE lanes SET turn=? WHERE source=? AND target=?",
                            (turn, row["source"], row["target"]))
            self.db.execute("UPDATE deliveries SET status='active', attempts=attempts+1 WHERE id=?", (row["id"],))
            return Delivery(row["id"], row["source"], row["message"], row["target"],
                            json.loads(row["payload"]), row["attempts"] + 1)

    def complete(self, task: Delivery, outcome: str = "sent") -> None:
        with self.db:
            if outcome == "sent" and task.payload.get("parts"):
                self.db.execute("INSERT OR IGNORE INTO delivery_parts VALUES(?,?)", (task.id, task.payload.get("next_part", 0)))
            self.db.execute("UPDATE deliveries SET status='done', payload=NULL WHERE id=?", (task.id,))
            self.db.execute("INSERT OR REPLACE INTO delivery_results VALUES(?,?)", (task.id, outcome))

    def continue_delivery(self, task: Delivery, payload: dict) -> None:
        with self.db:
            self.db.execute("INSERT OR IGNORE INTO delivery_parts VALUES(?,?)", (task.id, payload["next_part"] - 1))
            self.db.execute("UPDATE deliveries SET status='pending', attempts=0, due=0, payload=? WHERE id=?",
                            (json.dumps(payload, ensure_ascii=False), task.id))

    def defer(self, task: Delivery, due: float) -> None:
        with self.db:
            self.db.execute("UPDATE deliveries SET status='pending', attempts=MAX(attempts-1,0), due=? WHERE id=?", (due, task.id))

    def release(self, task: Delivery) -> None:
        with self.db:
            self.db.execute("UPDATE deliveries SET status='pending' WHERE id=? AND status='active'", (task.id,))

    def retry(self, task: Delivery, due: float, scope: str = "message", *, blocked: bool = False) -> None:
        with self.db:
            self.db.execute("UPDATE deliveries SET status=?, due=? WHERE id=?",
                            ("blocked" if blocked else "pending", due, task.id))
            if scope == "target":
                self.db.execute("UPDATE targets SET due=MAX(due,?) WHERE target=?", (due, task.target))
            elif scope == "account":
                self.db.execute("UPDATE scheduler SET due=MAX(due,?) WHERE id=1", (due,))

    def pause_target(self, task: Delivery) -> None:
        with self.db:
            self.db.execute("UPDATE targets SET paused=1 WHERE target=?", (task.target,))
            self.db.execute("UPDATE deliveries SET status='pending' WHERE id=?", (task.id,))

    def resume_target(self, target: str) -> None:
        with self.db:
            self.db.execute("UPDATE targets SET paused=0, due=0 WHERE target=?", (target,))

    def resume_delivery(self, task_id: int) -> None:
        with self.db:
            self.db.execute("UPDATE deliveries SET status='pending', due=0, attempts=0 WHERE id=? AND status='blocked'", (task_id,))

    def counts(self) -> dict[str, int]:
        return {row[0]: row[1] for row in self.db.execute("SELECT status,COUNT(*) FROM deliveries GROUP BY status")}

    def skip_exhausted(self, max_attempts: int, ids: list[int] | None = None) -> int:
        """重试耗尽的消息结束投递，不继续挡住同线路后续消息。"""
        if not self.owner:
            raise RuntimeError("只有队列拥有者可以处理重试耗尽的任务")
        if ids == []:
            return 0
        selected = "" if ids is None else " AND id IN (" + ",".join("?" for _ in ids) + ")"
        with self.db:
            self.db.execute("BEGIN IMMEDIATE")
            rows = self.db.execute("SELECT id FROM deliveries WHERE status='blocked' AND attempts>=?" + selected,
                                   (max_attempts, *(ids or []))).fetchall()
            for row in rows:
                self.db.execute("UPDATE deliveries SET status='done',payload=NULL WHERE id=?", (row[0],))
                self.db.execute("INSERT OR REPLACE INTO delivery_results VALUES(?, 'retry_exhausted')", (row[0],))
        for row in rows:
            logging.getLogger(__name__).warning("投递重试耗尽，已跳过 task_id=%s max_attempts=%s", row[0], max_attempts)
        return len(rows)


class DeliveryDispatcher:
    def __init__(self, store: DeliveryStore, send: Callable[[Delivery], Awaitable[None]], *,
                 concurrency: int = 4, interval: float = 0.1, timeout: float = 30,
                 max_attempts: int = 5, retry_base: float = 1, poll_interval: float = 0.05,
                 ids: list[int] | None = None) -> None:
        if concurrency < 1 or max_attempts < 1 or any(not math.isfinite(v) or v < 0 for v in
                (interval, timeout, retry_base, poll_interval)) or min(timeout, poll_interval) <= 0:
            raise ValueError("调度参数无效")
        self.store, self.send = store, send
        self.concurrency, self.interval, self.timeout = concurrency, interval, timeout
        self.max_attempts, self.retry_base, self.poll_interval = max_attempts, retry_base, poll_interval
        self.ids = ids
        self._running = False

    async def _deliver(self, task: Delivery) -> None:
        try:
            await asyncio.wait_for(self.send(task), timeout=self.timeout)
        except asyncio.CancelledError:
            self.store.retry(task, time.time())
            raise
        except ContinueDelivery as progress:
            self.store.continue_delivery(task, progress.payload)
        except DeferDelivery as waiting:
            self.store.defer(task, time.time() + waiting.delay)
        except SkipDelivery:
            self.store.complete(task, outcome="skipped")
        except PauseTarget:
            self.store.pause_target(task)
        except RetryDelivery as failure:
            self.store.retry(task, time.time() + failure.delay, failure.scope,
                             blocked=task.attempts >= self.max_attempts)
            self.store.skip_exhausted(self.max_attempts, [task.id])
        except Exception:
            # 不把 SDK 异常文本写入队列，避免异常携带正文、凭证等信息。
            delay = min(300, self.retry_base * 2 ** min(task.attempts - 1, 16))
            self.store.retry(task, time.time() + delay, blocked=task.attempts >= self.max_attempts)
            self.store.skip_exhausted(self.max_attempts, [task.id])
        else:
            self.store.complete(task)

    async def run(self, stop: asyncio.Event) -> None:
        """停止领取新任务后等在途任务收尾；发送端必须支持异步取消。"""
        if self._running:
            raise RuntimeError("调度器已运行")
        self._running = True
        active: dict[asyncio.Task, Delivery] = {}
        try:
            # 升级时处理旧版本留下的耗尽任务；补发仍只影响本次选择。
            self.store.skip_exhausted(self.max_attempts, self.ids)
            while not stop.is_set():
                finished = {task for task in active if task.done()}
                for task in finished:
                    del active[task]
                    task.result()
                while len(active) < self.concurrency and not stop.is_set():
                    delivery = self.store.claim(time.time(), self.interval, self.ids)
                    if delivery is None:
                        break
                    active[asyncio.create_task(self._deliver(delivery))] = delivery
                try:
                    await asyncio.wait_for(stop.wait(), self.poll_interval)
                except TimeoutError:
                    pass
            if active:
                await asyncio.gather(*active)
        finally:
            for task in active:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*active, return_exceptions=True)
            # 任务可能在协程开始前就被取消，此时发送协程的 finally 无法执行。
            for delivery in active.values():
                self.store.release(delivery)
            self._running = False
