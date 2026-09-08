import json
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from control_plane import InvalidAction, LocalControlPlaneServer, ProcessSupervisor
from Tests.test_control_plane import FakeProcessFactory, make_config


class QQSourceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.config = make_config(self.root, port=0)
        self.config.state_path.write_text(json.dumps({
            "group_openid": "private-openid",
            "qq_groups": [{"binding_id": "target-1", "group_openid": "private-openid",
                           "label": "测试目标群", "status": "active"}],
        }))
        self.factory = FakeProcessFactory()
        self.supervisor = ProcessSupervisor(self.config, process_factory=self.factory)
        self.rule = {"group_name": "测试源群", "sender": "", "binding_ids": ["target-1"], "enabled": True}

    def test_save_edit_disable_reload_remove_without_sending(self):
        result = self.supervisor.qq_sources(self.rule)
        rule = result["rules"][0]
        self.assertEqual(result["collector_state"], "not_connected")
        self.assertNotIn("private-openid", json.dumps(result))
        restored = ProcessSupervisor(self.config, process_factory=self.factory)
        self.assertEqual(restored.qq_sources()["rules"], [rule])
        edited = dict(rule, group_name="新群名", sender="指定人员", enabled=False)
        self.assertEqual(restored.qq_sources(edited)["rules"], [edited])
        self.assertEqual(restored.qq_sources({"id": rule["id"]}, remove=True)["rules"], [])
        self.assertEqual(self.factory.processes, [])

    def test_invalid_rules_do_not_change_saved_config(self):
        saved = self.supervisor.qq_sources(self.rule)["rules"]
        invalid = [
            dict(self.rule, group_name=" "), dict(self.rule, sender="x" * 81),
            dict(self.rule, group_name="群\n名"), dict(self.rule, enabled="true"),
            dict(self.rule, binding_ids=[]), dict(self.rule, binding_ids=["missing"]),
            dict(self.rule, binding_ids=["target-1", "target-1"]),
            dict(self.rule, id="missing"), self.rule,
        ]
        for rule in invalid:
            with self.subTest(rule=rule), self.assertRaises(InvalidAction):
                self.supervisor.qq_sources(rule)
        with self.assertRaises(InvalidAction):
            self.supervisor.qq_sources({"id": "missing"}, remove=True)
        self.assertEqual(self.supervisor.qq_sources()["rules"], saved)

    def test_corrupt_file_is_preserved(self):
        path = self.root / ".qq-notification-sources.json"
        for content in ("broken", "[]", '{"schema_version":1,"rules":[{}]}'):
            path.write_text(content)
            with self.subTest(content=content), self.assertRaises(InvalidAction):
                self.supervisor.qq_sources(self.rule)
            self.assertEqual(path.read_text(), content)

    def test_write_failure_retains_existing_rules(self):
        saved = self.supervisor.qq_sources(self.rule)["rules"]
        with patch.object(self.supervisor._qq_source_store, "save_json", side_effect=OSError("disk")):
            with self.assertRaises(InvalidAction):
                self.supervisor.qq_sources(dict(saved[0], enabled=False))
        self.assertEqual(self.supervisor.qq_sources()["rules"], saved)

    def test_http_authorization_validation_and_reload(self):
        server = LocalControlPlaneServer(self.config, self.supervisor, static_dir=Path(__file__).resolve().parents[1] / "web")
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        base = "http://127.0.0.1:" + str(server.server_address[1])
        try:
            for headers in ({}, {"X-Control-Token": server.control_token, "Origin": "https://foreign.invalid"}):
                with self.assertRaises(HTTPError) as failure:
                    urlopen(Request(base + "/api/qq/sources", data=json.dumps(self.rule).encode(), headers=headers))
                self.assertEqual(failure.exception.code, 403)
            headers = {"X-Control-Token": server.control_token, "Content-Type": "application/json"}
            with urlopen(Request(base + "/api/qq/sources", data=json.dumps(self.rule).encode(), headers=headers)) as response:
                self.assertTrue(json.load(response)["ok"])
            with urlopen(base + "/api/qq/sources") as response:
                self.assertEqual(len(json.load(response)["data"]["rules"]), 1)
            with self.assertRaises(HTTPError) as failure:
                urlopen(Request(base + "/api/qq/sources", data=b'{"group_name":1}', headers=headers))
            self.assertEqual(failure.exception.code, 400)
            with urlopen(base + "/static/qq.js") as response:
                self.assertEqual(response.status, 200)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)
