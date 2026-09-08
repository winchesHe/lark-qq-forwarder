"""统一采集与投递运行时；所有生产启动均复用现有进程锁。"""

import asyncio
import fcntl
import functools
import logging
import signal
import sqlite3
import time
import uuid
from pathlib import Path

from delivery_dispatcher import DeliveryDispatcher
from qq_bridge import BridgeError, ForwarderProcessLock, LarkClient, StateStore, create_api
from unified_collectors import LarkCollector, QQNotificationCollector, publish_lark_event
from unified_sender import QQDeliverySender
from unified_store import QUEUE_FILE, UnifiedStore


def queue_is_owned(path: Path) -> bool:
    lock_path = Path(str(path) + ".lock")
    if not lock_path.exists():
        return False
    with lock_path.open("a+") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return True
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        return False


def read_unified_status(path: Path) -> dict:
    if not path.exists():
        return {"state": "not_initialized", "sources": {}, "queue": {}}
    try:
        db = sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=0.2)
        try:
            rows = db.execute("SELECT name,state,updated,detail FROM runtime_status").fetchall()
            counts = {row[0]: row[1] for row in db.execute("SELECT status,COUNT(*) FROM deliveries GROUP BY status")}
            counts["retry_exhausted"] = db.execute("SELECT COUNT(*) FROM delivery_results WHERE outcome='retry_exhausted'").fetchone()[0]
            counts["paused_targets"] = db.execute("SELECT COUNT(*) FROM targets t WHERE paused=1 AND EXISTS (SELECT 1 FROM deliveries d WHERE d.target=t.target AND d.status!='done')").fetchone()[0]
            statuses = {row[0]: {"state": row[1], "updated": row[2], "detail": row[3]} for row in rows}
            service = statuses.get("service", {})
            alive = time.time() - service.get("updated", 0) < 5 and queue_is_owned(path)
            state = service.get("state", "stopped") if alive else "stopped"
            for source in ("lark", "qq"):
                if not alive and source in statuses:
                    statuses[source]["state"] = "stopped"
            if alive and (counts.get("blocked") or counts["paused_targets"] or any(statuses.get(source, {}).get("state") in {"failed", "degraded", "backpressure"} for source in ("lark", "qq"))):
                state = "degraded"
            return {"state": state, "sources": {key: value for key, value in statuses.items() if key != "service"}, "queue": counts}
        finally:
            db.close()
    except (sqlite3.Error, OSError):
        return {"state": "unavailable", "sources": {}, "queue": {}}


def publisher(state_path: Path, channels_path: Path, listener_cursors: Path):
    return functools.partial(publish_lark_event, state_path=state_path, channels_path=channels_path, listeners_path=listener_cursors)


def quiet_sdk_logs() -> None:
    # SDK 的异常可能携带服务端原文；统一运行时只记录自己的脱敏状态。
    logger = logging.getLogger("qqbot_agent_sdk")
    logger.handlers = [logging.NullHandler()]
    logger.propagate = False


async def run_unified(*, state_path: Path, input_path: Path, channels_path: Path,
                      listeners_path: Path, listener_cursors: Path, routing_path: Path,
                      profile: str, contact: str, stop: asyncio.Event | None = None,
                      lark_client=None, api_factory=None) -> None:
    install_signals = stop is None
    stop = stop or asyncio.Event()
    loop = asyncio.get_running_loop()
    installed = []
    for signum in (signal.SIGTERM, signal.SIGINT):
        if not install_signals:
            break
        try:
            loop.add_signal_handler(signum, stop.set)
            installed.append(signum)
        except (NotImplementedError, RuntimeError):
            pass
    quiet_sdk_logs()
    try:
        with ForwarderProcessLock(state_path.with_name(".qq-forwarder.lock")):
            store = UnifiedStore(state_path.with_name(QUEUE_FILE))
            client = sender = None
            tasks = []
            try:
                store.set_status("service", "starting")
                store.set_status("lark", "starting")
                store.set_status("qq", "starting")
                api, client = await (api_factory or create_api)()
                lark = lark_client or LarkClient(profile=profile)
                sender = QQDeliverySender(store, state_path, api, client, lark)
                publish = publisher(state_path, channels_path, listener_cursors)
                lark_collector = LarkCollector(store, lark, state_path=state_path, channels_path=channels_path,
                    listeners_path=listeners_path, listener_cursors=listener_cursors,
                    routing_path=routing_path, input_path=input_path, contact=contact)
                qq_collector = QQNotificationCollector(store, state_path.with_name("qq-notifications"),
                    state_path.with_name(".qq-notification-sources.json"), state_path)

                async def maintain():
                    while not stop.is_set():
                        problem = ""
                        try:
                            store.export_completed(publish)
                            store.prune()
                            sender.images.trim(store)
                        except Exception:
                            problem = "投递确认同步失败，已保留队列等待恢复"
                        counts = store.counts()
                        source_problem = store.db.execute("SELECT 1 FROM runtime_status WHERE name IN ('lark','qq') AND state IN ('failed','degraded','backpressure')").fetchone()
                        if source_problem:
                            problem = "部分来源采集异常，请查看各路状态"
                        if sum(counts.get(key, 0) for key in ("pending", "active", "blocked")) >= store.max_pending * 0.8:
                            problem = "待投递队列超过容量的 80%，请检查积压"
                        if counts.get("blocked"):
                            problem = "存在超过重试预算的任务，等待恢复"
                        if store.db.execute("SELECT 1 FROM targets t WHERE paused=1 AND EXISTS (SELECT 1 FROM deliveries d WHERE d.target=t.target AND d.status!='done') LIMIT 1").fetchone():
                            problem = "部分目标群已暂停，积压保留等待恢复"
                        store.set_status("service", "degraded" if problem else "running", problem)
                        try:
                            await asyncio.wait_for(stop.wait(), 1)
                        except TimeoutError:
                            pass

                tasks = [asyncio.create_task(DeliveryDispatcher(store, sender).run(stop)),
                         asyncio.create_task(lark_collector.run(stop)),
                         asyncio.create_task(qq_collector.run(stop)), asyncio.create_task(maintain())]
                stopper = asyncio.create_task(stop.wait())
                try:
                    finished, _ = await asyncio.wait([*tasks, stopper], return_when=asyncio.FIRST_COMPLETED)
                    if stopper not in finished:
                        for task in finished:
                            task.result()
                        raise BridgeError("统一转发任务意外退出")
                    for collector in tasks[1:3]:
                        collector.cancel()
                    await asyncio.gather(*tasks[1:3], return_exceptions=True)
                    await asyncio.gather(tasks[0], tasks[3])
                    store.export_completed(publish)
                finally:
                    stopper.cancel()
                    await asyncio.gather(stopper, return_exceptions=True)
            finally:
                stop.set()
                for task in tasks:
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                if sender is not None:
                    await sender.close()
                if client is not None:
                    await client.aclose()
                store.set_status("service", "stopped")
                store.close()
    finally:
        for signum in installed:
            loop.remove_signal_handler(signum)


async def wait_for_deliveries(store, ids: list[int], *, timeout: float = 120, tick=None) -> list[dict]:
    async with asyncio.timeout(timeout):
        while True:
            if tick:
                tick()
            outcomes = store.outcomes(ids)
            if len(outcomes) != len(ids):
                raise BridgeError("投递回执不完整，请检查队列")
            if any(row["status"] == "blocked" or row["paused"] or row["blocked_by"] for row in outcomes):
                raise BridgeError("投递任务受阻，请检查目标群或恢复队列")
            if all(row["status"] == "done" for row in outcomes):
                if any(row["outcome"] == "cancelled" for row in outcomes):
                    raise BridgeError("投递已取消")
                return outcomes
            await asyncio.sleep(0.05)


async def dispatch_selected(store, ids: list[int], *, state_path: Path, lark,
                            publish=None, publish_source=None, progress=None, api_factory=None) -> list[dict]:
    quiet_sdk_logs()
    api, client = await (api_factory or create_api)()
    sender = QQDeliverySender(store, state_path, api, client, lark)
    stop = asyncio.Event()
    dispatcher = asyncio.create_task(DeliveryDispatcher(store, sender, ids=ids).run(stop))
    def tick():
        if dispatcher.done():
            dispatcher.result()
            raise BridgeError("投递调度器已停止")
        if publish:
            store.export_completed(publish, source=publish_source)
        if progress:
            progress()
    try:
        return await wait_for_deliveries(store, ids, tick=tick)
    finally:
        stop.set()
        dispatcher.cancel()
        await asyncio.gather(dispatcher, return_exceptions=True)
        await sender.close()
        await client.aclose()


async def submit_text(state_path: Path, binding_ids: list[str], text: str, *, request_id: str | None = None,
                      api_factory=None) -> None:
    if not isinstance(text, str) or not text.strip() or len(text.encode()) > 3000:
        raise BridgeError("消息不能为空且不能超过 3000 字节")
    state = StateStore.load(state_path)
    groups = [group for group in state.group_bindings if group.get("status") == "active" and group["binding_id"] in binding_ids]
    if len({group["binding_id"] for group in groups}) != len(set(binding_ids)) or not groups:
        raise BridgeError("指定 QQ 群不存在或未启用")
    lock = ForwarderProcessLock(state_path.with_name(".qq-forwarder.lock"))
    owner = False
    try:
        lock.acquire()
        owner = True
    except BridgeError:
        if not queue_is_owned(state_path.with_name(QUEUE_FILE)):
            raise BridgeError("旧转发服务仍在运行，请切换新版后再使用统一发送") from None
    store = None
    ids = []
    try:
        store = UnifiedStore(state_path.with_name(QUEUE_FILE), owner=owner)
        ids = store.capture(source="manual", message=request_id or str(uuid.uuid4()), position=0,
            targets=[group["group_openid"] for group in groups],
            payload={"parts": [{"type": "text", "text": text}], "next_part": 0}, metadata={"kind": "manual"})
        if owner:
            outcomes = await dispatch_selected(store, ids, state_path=state_path, lark=None, api_factory=api_factory)
            store.export_completed(lambda *_: None, source="manual")
            store.prune()
        else:
            outcomes = await wait_for_deliveries(store, ids)
        if any(row["outcome"] != "sent" for row in outcomes):
            raise BridgeError("消息未被 QQ 接受，未完成投递")
    except TimeoutError:
        if store:
            store.cancel_pending(ids)
        raise BridgeError("等待投递超时，已取消尚未开始的任务；在途结果需核对") from None
    except BaseException:
        if store:
            store.cancel_pending(ids)
        raise
    finally:
        if store:
            store.close()
        if owner:
            lock.release()


def queue_tasks(state_path: Path) -> list[dict]:
    """恢复清单只返回任务编号和目标备注，不返回正文、来源 ID 或群 OpenID。"""
    path = state_path.with_name(QUEUE_FILE)
    if not path.exists():
        return []
    groups = {group["group_openid"]: group for group in StateStore.load(state_path).group_bindings}
    db = sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)
    try:
        return [{"id": row[0], "source": row[1].split(":", 1)[0], "state": row[2],
                 "attempts": row[3], "target_paused": bool(row[5]),
                 "binding_id": groups.get(row[4], {}).get("binding_id"),
                 "label": groups.get(row[4], {}).get("label", "已移除目标")}
                for row in db.execute("""SELECT d.id,d.source,d.status,d.attempts,d.target,t.paused
                    FROM deliveries d JOIN targets t ON t.target=d.target
                    WHERE d.status!='done' ORDER BY (d.status='blocked' OR t.paused=1) DESC,d.id LIMIT 1000""")]
    finally:
        db.close()


def retry_queued(state_path: Path, ids: list[int], binding_id: str | None = None) -> None:
    """显式恢复所选受阻任务或已暂停目标；保持单一发送出口。"""
    if not ids and not binding_id:
        raise BridgeError("请指定 --delivery-id 或 --binding-id")
    if len(ids) > 1000 or any(type(value) is not int or value < 1 for value in ids):
        raise BridgeError("投递任务编号无效")
    path = state_path.with_name(QUEUE_FILE)
    if not path.exists():
        raise BridgeError("投递队列尚未初始化")
    group = None
    if binding_id:
        group = next((value for value in StateStore.load(state_path).group_bindings
                      if value["binding_id"] == binding_id and value.get("status") == "active"), None)
        if group is None:
            raise BridgeError("目标群不存在或未启用")
    store = UnifiedStore(path, owner=False)
    try:
        with store.db:
            store.db.execute("BEGIN IMMEDIATE")
            for task_id in ids:
                row = store.db.execute("SELECT status FROM deliveries WHERE id=?", (task_id,)).fetchone()
                if row is None or row[0] != "blocked":
                    raise BridgeError("仅可恢复清单中受阻的任务，未修改队列")
            for task_id in ids:
                store.db.execute("UPDATE deliveries SET status='pending',attempts=0,due=0 WHERE id=?", (task_id,))
            if group:
                store.db.execute("UPDATE targets SET paused=0,due=0 WHERE target=?", (group["group_openid"],))
    finally:
        store.close()
