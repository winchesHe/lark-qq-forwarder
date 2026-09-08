"""共用 QQ 单次发送出口；图片准备独立限流，不占消息发送名额。"""

import asyncio
import json
import re
import tempfile
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

from qqbot_agent_sdk import MEDIA_TYPE_IMAGE, MediaInfo, MediaUploader, MessageToCreate, QQMessageType

from delivery_dispatcher import ContinueDelivery, DeferDelivery, PauseTarget, RetryDelivery, SkipDelivery
from qq_bridge import BridgeError, StateStore, is_content_violation_error


def translate_failure(failure: Exception) -> Exception:
    """仅识别本机 SDK 确实提供的字段；未知错误交由有限重试处理。"""
    chain = []
    current = failure
    while current is not None and len(chain) < 8:
        chain.append(current)
        current = current.__cause__
    if any(is_content_violation_error(item) for item in chain):
        return SkipDelivery()
    statuses = [re.search(r"QQ Bot API error \[(\d{3})\]", str(item)) for item in chain]
    if any(match and match[1] in {"429", "401"} for match in statuses):
        return RetryDelivery(60, "account")
    if any(type(item).__name__ == "UploadDailyLimitExceededError" for item in chain):
        return RetryDelivery(3600, "account")
    return BridgeError("QQ 投递未确认")


class ImageCache:
    def __init__(self, lark, *, limit: int = 8) -> None:
        self.lark, self.limit = lark, limit
        self.entries = {}

    @asynccontextmanager
    async def acquire(self, source: str, message: str, key: str):
        identity = (source, message, key)
        entry = self.entries.get(identity)
        if entry is None:
            directory = tempfile.TemporaryDirectory(prefix="unified-lark-image-")
            async def download():
                return await asyncio.to_thread(self.lark.download_image, message_id=message,
                                               image_key=key, output_directory=Path(directory.name))
            entry = {"directory": directory, "task": asyncio.create_task(download()), "users": 0}
            self.entries[identity] = entry
        entry["users"] += 1
        try:
            # 超时取消上传者时，不能在线程仍写图片期间删除目录。
            path = await asyncio.shield(entry["task"])
            yield path
        finally:
            entry["users"] -= 1
            self.trim()

    def trim(self, store=None) -> None:
        for key, entry in list(self.entries.items()):
            if entry["users"] or not entry["task"].done():
                continue
            failed = entry["task"].cancelled() or entry["task"].exception() is not None
            completed = store is not None and not store.db.execute(
                "SELECT 1 FROM deliveries WHERE source=? AND message=? AND status!='done' LIMIT 1", key[:2]).fetchone()
            if failed or completed or len(self.entries) > self.limit:
                entry["directory"].cleanup()
                del self.entries[key]

    async def close(self) -> None:
        await asyncio.gather(*(entry["task"] for entry in self.entries.values()), return_exceptions=True)
        for entry in self.entries.values():
            entry["directory"].cleanup()
        self.entries.clear()


class QQDeliverySender:
    def __init__(self, store, state_path: Path, api, http_client, lark, *, prepare_limit: int = 2) -> None:
        self.store, self.state_path = store, state_path
        self.api, self.http_client = api, http_client
        self.prepare_limit = prepare_limit
        self.images = ImageCache(lark)
        self.preparing = {}
        self.session = str(uuid.uuid4())

    async def _prepare_image(self, task, part: dict) -> str:
        async with asyncio.timeout(90):
            async with self.images.acquire(task.source, part["message_id"], part["image_key"]) as path:
                uploader = MediaUploader(self.api, self.http_client, log_tag="UnifiedForwarder")
                token = await uploader.upload("group", task.target, str(path), MEDIA_TYPE_IMAGE, file_name="lark-image.jpg")
                if not token:
                    raise BridgeError("QQ 图片上传未返回文件引用")
                return token

    async def __call__(self, task) -> None:
        state = StateStore.load(self.state_path)
        for task_id, preparation in list(self.preparing.items()):
            row = self.store.db.execute("SELECT target,status FROM deliveries WHERE id=?", (task_id,)).fetchone()
            if row is None or row["status"] in {"done", "blocked"} or row["target"] not in state.active_group_openids():
                preparation.cancel()
                await asyncio.gather(preparation, return_exceptions=True)
                del self.preparing[task_id]
        if task.target not in state.active_group_openids():
            raise PauseTarget()
        payload = task.payload
        parts = payload.get("parts", [])
        index = payload.get("next_part", 0)
        if not isinstance(parts, list) or not parts or not 0 <= index < len(parts):
            raise BridgeError("投递任务缺少有效内容")
        part = parts[index]
        if part.get("type") == "image" and (part.get("prepared_session") != self.session
                or time.time() - part.get("prepared_at", 0) > 60):
            part.pop("file_info", None)
        try:
            if part["type"] == "image" and not part.get("file_info"):
                preparation = self.preparing.get(task.id)
                if preparation is None:
                    if len(self.preparing) >= self.prepare_limit:
                        raise DeferDelivery()
                    self.preparing[task.id] = asyncio.create_task(self._prepare_image(task, part))
                    raise DeferDelivery()
                if not preparation.done():
                    raise DeferDelivery()
                del self.preparing[task.id]
                part["file_info"] = preparation.result()
                part["prepared_session"] = self.session
                part["prepared_at"] = time.time()
                # 上传成功后立即保存引用，发送失败时不重复下载或上传。
                with self.store.db:
                    self.store.db.execute("UPDATE deliveries SET payload=? WHERE id=?", (json.dumps(payload, ensure_ascii=False), task.id))

            if part["type"] == "text":
                message = MessageToCreate(content=part["text"], msg_type=QQMessageType.TEXT,
                                          msg_seq=self.api.next_msg_seq())
            elif part["type"] == "image":
                message = MessageToCreate(msg_type=QQMessageType.RICH_MEDIA, msg_seq=self.api.next_msg_seq(),
                                          media=MediaInfo(file_info=part["file_info"]))
            else:
                raise BridgeError("无法投递此消息类型")
            result = await self.api.post_group_message(task.target, message)
            if not isinstance(result, dict) or not result.get("id"):
                raise BridgeError("QQ 未返回投递确认")
        except DeferDelivery:
            raise
        except Exception as failure:
            raise translate_failure(failure) from None

        if index + 1 < len(parts):
            payload["next_part"] = index + 1
            # 已完成分段清空正文和文件引用，只保留未发送内容。
            parts[index] = {"type": "completed"}
            raise ContinueDelivery(payload)

    async def close(self) -> None:
        for task in self.preparing.values():
            task.cancel()
        await asyncio.gather(*self.preparing.values(), return_exceptions=True)
        self.preparing.clear()
        await self.images.close()
