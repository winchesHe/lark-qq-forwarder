"""QQ 系统通知监听规则；仅保存配置，不启动采集或发送消息。"""

import json
import secrets
from pathlib import Path
from typing import Callable


class QQSourceError(ValueError):
    pass


class QQSourceStore:
    def __init__(self, path: Path, save_json: Callable[[Path, dict], None]) -> None:
        self.path = path
        self.save_json = save_json

    def read(self) -> list[dict]:
        if not self.path.exists():
            return []
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
            if not isinstance(data, dict) or data.get("schema_version") != 1:
                raise ValueError()
            rules = data["rules"]
            if not isinstance(rules, list):
                raise ValueError()
            ids = set()
            for rule in rules:
                self.validate(rule)
                if not isinstance(rule.get("id"), str) or not rule["id"] or rule["id"] in ids:
                    raise ValueError()
                ids.add(rule["id"])
            return rules
        except (OSError, UnicodeError, ValueError, KeyError, TypeError) as exc:
            raise QQSourceError("QQ 监听配置无法读取，请检查配置文件；原文件已保留") from exc

    @staticmethod
    def validate(rule: dict) -> None:
        if not isinstance(rule, dict):
            raise QQSourceError("监听规则无效")
        for key, required in (("group_name", True), ("sender", False)):
            value = rule.get(key)
            if not isinstance(value, str) or len(value) > 80 or (required and not value.strip()):
                raise QQSourceError("群名不能为空，群名和发送人不能超过 80 个字符")
            if any(ord(char) < 32 for char in value):
                raise QQSourceError("群名和发送人不能包含控制字符")
        if not isinstance(rule.get("enabled"), bool):
            raise QQSourceError("规则启用状态无效")
        targets = rule.get("binding_ids")
        if (not isinstance(targets, list) or not 1 <= len(targets) <= 20
                or not all(isinstance(item, str) and item for item in targets)
                or len(set(targets)) != len(targets)):
            raise QQSourceError("请选择 1 到 20 个不重复的目标群")

    def save(self, payload: dict, active_ids: set[str]) -> list[dict]:
        self.validate(payload)
        if not set(payload["binding_ids"]).issubset(active_ids):
            raise QQSourceError("目标群已移除或未启用，请重新选择")
        rules = self.read()
        rule_id = payload.get("id")
        if rule_id is not None and (not isinstance(rule_id, str) or not any(r["id"] == rule_id for r in rules)):
            raise QQSourceError("监听规则已不存在，请刷新页面")
        rule = {
            "id": rule_id or secrets.token_hex(8),
            "group_name": payload["group_name"].strip(),
            "sender": payload["sender"].strip(),
            "binding_ids": payload["binding_ids"],
            "enabled": payload["enabled"],
        }
        if any(r["id"] != rule["id"] and (r["group_name"], r["sender"]) == (rule["group_name"], rule["sender"]) for r in rules):
            raise QQSourceError("同一群和发送人的监听规则已存在，请编辑原规则")
        if rule_id is None:
            if len(rules) >= 100:
                raise QQSourceError("最多保存 100 条监听规则")
            rules.append(rule)
        else:
            rules = [rule if r["id"] == rule_id else r for r in rules]
        self.save_json(self.path, {"schema_version": 1, "rules": rules})
        return rules

    def remove(self, rule_id: str) -> list[dict]:
        rules = self.read()
        if not isinstance(rule_id, str) or not any(r["id"] == rule_id for r in rules):
            raise QQSourceError("监听规则已不存在，请刷新页面")
        rules = [r for r in rules if r["id"] != rule_id]
        self.save_json(self.path, {"schema_version": 1, "rules": rules})
        return rules
