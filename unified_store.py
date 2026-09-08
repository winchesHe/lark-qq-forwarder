"""两路采集共用的持久化事件、采集高水位和投递确认。"""

import json
import time
from pathlib import Path
from typing import Callable

from delivery_dispatcher import DeliveryStore, QueueFull


QUEUE_FILE = ".delivery-queue.sqlite3"


class ExportFailure(RuntimeError):
    """部分来源的确认发布失败，其余来源已继续处理。"""


class UnifiedStore(DeliveryStore):
    def __init__(self, path: Path, **options) -> None:
        super().__init__(path, **options)
        try:
            self.db.executescript("""
                CREATE TABLE IF NOT EXISTS source_progress (
                    source TEXT PRIMARY KEY, position INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS source_events (
                    id INTEGER PRIMARY KEY, source TEXT NOT NULL, message TEXT NOT NULL,
                    position INTEGER NOT NULL, metadata TEXT NOT NULL,
                    created REAL NOT NULL, exported INTEGER NOT NULL DEFAULT 0,
                    UNIQUE(source, message)
                );
                CREATE INDEX IF NOT EXISTS event_order ON source_events(source, exported, id);
                CREATE TABLE IF NOT EXISTS runtime_status (
                    name TEXT PRIMARY KEY, state TEXT NOT NULL, updated REAL NOT NULL,
                    detail TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS cancellation_requests (id INTEGER PRIMARY KEY);
            """)
            if self.owner:
                self.cancel_pending([row[0] for row in self.db.execute("SELECT id FROM cancellation_requests")])
        except BaseException:
            self.close()
            raise

    def position(self, source: str, initial: int = 0) -> int:
        row = self.db.execute("SELECT position FROM source_progress WHERE source=?", (source,)).fetchone()
        return max(row[0], initial) if row else initial

    def capture(self, *, source: str, message: str, position: int,
                targets: list[str], payload: dict, metadata: dict) -> list[int]:
        """事件、全部目标和采集位置一起提交；重复事件不会按新路由补投。"""
        if (not isinstance(position, int) or isinstance(position, bool) or position < 0
                or not source or not message or len(source) > 512 or len(message) > 512
                or len(targets) > 100 or any(not isinstance(t, str) or not t or len(t) > 512 for t in targets)):
            raise ValueError("采集事件标识或位置无效")
        encoded = json.dumps(payload, ensure_ascii=False, allow_nan=False)
        context = json.dumps(metadata, ensure_ascii=False, allow_nan=False)
        if len(encoded.encode()) + len(context.encode()) > self.max_payload_bytes:
            raise QueueFull("采集负载超过容量限制，保留原进度等待处理")
        with self.db:
            self.db.execute("BEGIN IMMEDIATE")
            known = self.db.execute("SELECT 1 FROM source_events WHERE source=? AND message=?", (source, message)).fetchone()
            if not known:
                if self.db.execute("SELECT COUNT(*) FROM source_events").fetchone()[0] >= self.max_records:
                    raise QueueFull("采集事件容量已满，不能推进进度")
                self._enqueue(source, message, targets, encoded)
                self.db.execute("INSERT INTO source_events(source,message,position,metadata,created) VALUES(?,?,?,?,?)",
                                (source, message, position, context, time.time()))
            self.db.execute("""INSERT INTO source_progress VALUES(?,?)
                ON CONFLICT(source) DO UPDATE SET position=MAX(position, excluded.position)""", (source, position))
        return self.event_ids(source, message)

    def event_ids(self, source: str, message: str) -> list[int]:
        return [row[0] for row in self.db.execute(
            "SELECT id FROM deliveries WHERE source=? AND message=? ORDER BY id", (source, message))]

    def export_completed(self, publish: Callable[[dict, list[str], int, str], None], limit: int = 100,
                         *, source: str | None = None) -> int:
        """只推进连续完成的旧游标；写后退出时可安全重复发布确认。"""
        completed = 0
        failed_sources = set()
        for _ in range(limit):
            excluded = "" if not failed_sources else " AND e.source NOT IN (" + ",".join("?" for _ in failed_sources) + ")"
            event = self.db.execute(f"""
                SELECT e.* FROM source_events e WHERE e.exported=0
                AND (? IS NULL OR e.source=?) {excluded}
                AND NOT EXISTS (SELECT 1 FROM source_events p
                    WHERE p.source=e.source AND p.exported=0 AND p.id<e.id)
                AND NOT EXISTS (SELECT 1 FROM deliveries d
                    WHERE d.source=e.source AND d.message=e.message AND d.status!='done')
                ORDER BY e.id LIMIT 1
            """, (source, source, *failed_sources)).fetchone()
            if event is None:
                break
            targets = [row[0] for row in self.db.execute(
                "SELECT target FROM deliveries WHERE source=? AND message=?", (event["source"], event["message"]))]
            try:
                publish(json.loads(event["metadata"]), targets, event["position"], event["message"])
            except Exception:
                failed_sources.add(event["source"])
                continue
            with self.db:
                self.db.execute("UPDATE source_events SET exported=1 WHERE id=?", (event["id"],))
            completed += 1
        if failed_sources:
            raise ExportFailure("部分来源确认同步失败，已保留进度等待恢复")
        return completed

    def discard_through(self, source: str, position: int) -> None:
        """仅在服务已停且持有进程锁时，将明确放弃的来源任务和高水位一起保存。"""
        if not self.owner:
            raise RuntimeError("放弃进度需要独占队列")
        with self.db:
            self.db.execute("BEGIN IMMEDIATE")
            ids = [row[0] for row in self.db.execute("""SELECT d.id FROM deliveries d
                JOIN source_events e ON e.source=d.source AND e.message=d.message
                WHERE e.source=? AND e.position<=? AND d.status!='done'""", (source, position))]
            for task_id in ids:
                self.db.execute("UPDATE deliveries SET status='done',payload=NULL WHERE id=?", (task_id,))
                self.db.execute("INSERT OR REPLACE INTO delivery_results VALUES(?, 'cancelled')", (task_id,))
            self.db.execute("UPDATE source_events SET exported=1 WHERE source=? AND position<=?", (source, position))
            self.db.execute("""INSERT INTO source_progress VALUES(?,?)
                ON CONFLICT(source) DO UPDATE SET position=MAX(position, excluded.position)""", (source, position))

    def outcomes(self, ids: list[int]) -> list[dict]:
        if not ids:
            return []
        marks = ",".join("?" for _ in ids)
        return [dict(row) for row in self.db.execute(f"""
            SELECT d.id,d.status,r.outcome,t.paused,
              (SELECT COUNT(*) FROM delivery_parts p WHERE p.id=d.id) AS sent_parts,
              EXISTS(SELECT 1 FROM deliveries p WHERE p.source=d.source AND p.target=d.target AND p.id<d.id AND p.status='blocked') AS blocked_by
            FROM deliveries d
            JOIN targets t ON t.target=d.target LEFT JOIN delivery_results r ON r.id=d.id
            WHERE d.id IN ({marks}) ORDER BY d.id
        """, ids)]

    def cancel_pending(self, ids: list[int]) -> None:
        """取消请求尚未开始的部分；已发出的网络请求结果不能猜测。"""
        with self.db:
            self.db.execute("BEGIN IMMEDIATE")
            for task_id in ids:
                self.db.execute("INSERT OR IGNORE INTO cancellation_requests SELECT id FROM deliveries WHERE id=? AND status!='done'", (task_id,))
                changed = self.db.execute("UPDATE deliveries SET status='done',payload=NULL WHERE id=? AND status IN ('pending','blocked')", (task_id,))
                if changed.rowcount:
                    self.db.execute("INSERT OR REPLACE INTO delivery_results VALUES(?, 'cancelled')", (task_id,))

    def _cancelled(self, task) -> bool:
        return self.db.execute("SELECT 1 FROM cancellation_requests WHERE id=?", (task.id,)).fetchone() is not None

    def retry(self, task, due, scope="message", *, blocked=False) -> None:
        if self._cancelled(task):
            self.complete(task, "cancelled")
        else:
            super().retry(task, due, scope, blocked=blocked)

    def defer(self, task, due) -> None:
        if self._cancelled(task):
            self.complete(task, "cancelled")
        else:
            super().defer(task, due)

    def continue_delivery(self, task, payload) -> None:
        super().continue_delivery(task, payload)
        if self._cancelled(task):
            self.cancel_pending([task.id])

    def release(self, task) -> None:
        super().release(task)
        if self._cancelled(task):
            self.cancel_pending([task.id])

    def set_status(self, name: str, state: str, detail: str = "") -> None:
        with self.db:
            self.db.execute("INSERT OR REPLACE INTO runtime_status VALUES(?,?,?,?)", (name, state, time.time(), detail))

    def prune(self, *, retention_seconds: float = 86400, limit: int = 500) -> int:
        """保留近期回执；只回收已导出确认的事件，不删除积压和受阻任务。"""
        with self.db:
            self.db.execute("BEGIN IMMEDIATE")
            rows = self.db.execute("SELECT * FROM source_events WHERE exported=1 AND created<? ORDER BY id LIMIT ?",
                                   (time.time() - retention_seconds, limit)).fetchall()
            for row in rows:
                context = json.loads(row["metadata"])
                # 崩溃可能发生在队列提交后、通知文件删除前，文件仍在时保留去重凭据。
                if context.get("spool") and Path(context["spool"]).exists():
                    continue
                self.db.execute("DELETE FROM delivery_results WHERE id IN (SELECT id FROM deliveries WHERE source=? AND message=?)", (row["source"], row["message"]))
                self.db.execute("DELETE FROM delivery_parts WHERE id IN (SELECT id FROM deliveries WHERE source=? AND message=?)", (row["source"], row["message"]))
                self.db.execute("DELETE FROM cancellation_requests WHERE id IN (SELECT id FROM deliveries WHERE source=? AND message=?)", (row["source"], row["message"]))
                self.db.execute("DELETE FROM deliveries WHERE source=? AND message=?", (row["source"], row["message"]))
                self.db.execute("DELETE FROM source_events WHERE id=?", (row["id"],))
            self.db.execute("DELETE FROM lanes WHERE NOT EXISTS (SELECT 1 FROM deliveries d WHERE d.source=lanes.source AND d.target=lanes.target)")
        return len(rows)
