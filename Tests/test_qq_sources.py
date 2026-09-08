import json
import io
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from control_plane import InvalidAction, LocalControlPlaneServer, ProcessSupervisor
from Tests.test_control_plane import FakeProcess, FakeProcessFactory, make_config


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
            dict(self.rule, binding_ids=["missing"]),
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

    def test_source_can_exist_without_targets_and_group_routing_is_independent(self):
        other_group = {"binding_id": "target-2", "group_openid": "second", "status": "active", "label": "另一个群"}
        state = json.loads(self.config.state_path.read_text())
        state["qq_groups"].append(other_group)
        self.config.state_path.write_text(json.dumps(state))
        first = self.supervisor.qq_sources(dict(self.rule, binding_ids=[]))["rules"][0]
        second = self.supervisor.qq_sources(dict(self.rule, group_name="另一个源", binding_ids=["target-2"]))["rules"][1]
        routed = self.supervisor.qq_sources({"binding_id": "target-1", "source_ids": [first["id"], second["id"]]}, routing=True)["rules"]
        self.assertEqual(routed[0]["binding_ids"], ["target-1"])
        self.assertEqual(set(routed[1]["binding_ids"]), {"target-1", "target-2"})
        cleared = self.supervisor.qq_sources({"binding_id": "target-1", "source_ids": []}, routing=True)["rules"]
        self.assertEqual(cleared[0]["binding_ids"], [])
        self.assertEqual(cleared[1]["binding_ids"], ["target-2"])
        for payload in ({"binding_id": "missing", "source_ids": []},
                        {"binding_id": "target-1", "source_ids": ["missing"]},
                        {"binding_id": "target-1", "source_ids": [first["id"], first["id"]]}):
            with self.subTest(payload=payload), self.assertRaises(InvalidAction):
                self.supervisor.qq_sources(payload, routing=True)
        self.assertEqual(self.supervisor.qq_sources()["rules"], cleared)

    def test_manual_send_requires_confirmation_target_and_valid_text(self):
        from control_plane import ConfirmationRequired
        with self.assertRaises(ConfirmationRequired):
            self.supervisor.send_qq_message("target-1", "测试", False)
        for binding_id, text in (("", "测试"), ("missing", "测试"), ("target-1", ""), ("target-1", "字" * 1001)):
            with self.subTest(binding_id=binding_id, text_length=len(text)), self.assertRaises(InvalidAction):
                self.supervisor.send_qq_message(binding_id, text, True)
        self.assertEqual(self.factory.processes, [])

    def test_manual_send_passes_text_via_stdin_and_records_result(self):
        class MessagePipe(io.BytesIO):
            def close(self):
                self.saved = self.getvalue()
                super().close()

        for exit_code, expected in ((0, "succeeded"), (1, "failed")):
            process = FakeProcess([], exit_code=exit_code)
            process.stdin = MessagePipe()
            with patch.object(self.supervisor, "_process_factory", return_value=process) as factory:
                self.supervisor.send_qq_message("target-1", "专用正文\n第二行", True)
                for thread in list(self.supervisor._operation_threads.values()):
                    thread.join(timeout=2)
                args, kwargs = factory.call_args
                self.assertIn("send", args[0])
                self.assertNotIn("专用正文", str(args))
                self.assertEqual(process.stdin.saved.decode(), "专用正文\n第二行")
                status = self.supervisor.status()
                self.assertEqual(status["operations"]["test"]["state"], expected)
                self.assertNotIn("专用正文", json.dumps(status, ensure_ascii=False))

    def test_http_authorization_validation_and_reload(self):
        server = LocalControlPlaneServer(self.config, self.supervisor, static_dir=Path(__file__).resolve().parents[1] / "web")
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        base = "http://127.0.0.1:" + str(server.server_address[1])
        try:
            for path in ("/api/qq/sources", "/api/qq/routing", "/api/qq/send"):
                for headers in ({}, {"X-Control-Token": server.control_token, "Origin": "https://foreign.invalid"}):
                    with self.subTest(path=path), self.assertRaises(HTTPError) as failure:
                        urlopen(Request(base + path, data=json.dumps(self.rule).encode(), headers=headers))
                    self.assertEqual(failure.exception.code, 403)
            headers = {"X-Control-Token": server.control_token, "Content-Type": "application/json"}
            with urlopen(Request(base + "/api/qq/sources", data=json.dumps(self.rule).encode(), headers=headers)) as response:
                self.assertTrue(json.load(response)["ok"])
            with urlopen(base + "/api/qq/sources") as response:
                rules = json.load(response)["data"]["rules"]
                self.assertEqual(len(rules), 1)
            with urlopen(Request(base + "/api/qq/routing", data=json.dumps({"binding_id": "target-1", "source_ids": []}).encode(), headers=headers)) as response:
                self.assertEqual(json.load(response)["data"]["rules"][0]["binding_ids"], [])
            with self.assertRaises(HTTPError) as failure:
                urlopen(Request(base + "/api/qq/send", data=json.dumps({"binding_id": "target-1", "text": "测试", "confirmed": False}).encode(), headers=headers))
            self.assertEqual(failure.exception.code, 400)
            self.assertEqual(self.factory.processes, [])
            with self.assertRaises(HTTPError) as failure:
                urlopen(Request(base + "/api/qq/sources", data=b'{"group_name":1}', headers=headers))
            self.assertEqual(failure.exception.code, 400)
            with urlopen(base + "/static/qq.js") as response:
                self.assertEqual(response.status, 200)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)
