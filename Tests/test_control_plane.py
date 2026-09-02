import json
import socket
import tempfile
import threading
import time
import unittest
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from control_plane import (
    ActionConflict,
    ControlPlaneConfig,
    ConfirmationRequired,
    InvalidAction,
    LocalControlPlaneServer,
    ProcessSupervisor,
)


class FakeProcess:
    _next_pid = 4100

    def __init__(self, command: list[str], *, exit_code: int | None = None) -> None:
        self.command = command
        self.pid = FakeProcess._next_pid
        FakeProcess._next_pid += 1
        self._exit_code = exit_code
        self.terminate_count = 0

    def poll(self) -> int | None:
        return self._exit_code

    def wait(self, timeout: float | None = None) -> int:
        del timeout
        if self._exit_code is None:
            return_code = 0
            while self._exit_code is None:
                time.sleep(0.001)
            return_code = self._exit_code
            return return_code
        return self._exit_code

    def terminate(self) -> None:
        self.terminate_count += 1
        self._exit_code = -15

    def kill(self) -> None:
        self._exit_code = -9


class FakeProcessFactory:
    def __init__(
        self,
        *,
        prime_exit_code: int = 0,
        check_exit_code: int = 0,
        bind_exit_code: int | None = None,
        test_exit_code: int | None = 0,
        replay_exit_code: int | None = 0,
        probe_exit_code: int | None = None,
        forwarder_exit_code: int | None = None,
    ) -> None:
        self.prime_exit_code = prime_exit_code
        self.check_exit_code = check_exit_code
        self.bind_exit_code = bind_exit_code
        self.test_exit_code = test_exit_code
        self.replay_exit_code = replay_exit_code
        self.probe_exit_code = probe_exit_code
        self.forwarder_exit_code = forwarder_exit_code
        self.processes: list[FakeProcess] = []

    def __call__(self, command: list[str], **_kwargs: object) -> FakeProcess:
        if "prime" in command:
            exit_code = self.prime_exit_code
        elif command and command[0].endswith("/probe"):
            exit_code = self.probe_exit_code
        elif "bind" in command:
            exit_code = self.bind_exit_code
        elif "test" in command:
            exit_code = self.test_exit_code
        elif "channel_replay.py" in command:
            exit_code = self.replay_exit_code
        elif "run" in command:
            exit_code = self.forwarder_exit_code
        elif "check" in command:
            exit_code = self.check_exit_code
        else:
            exit_code = None
        process = FakeProcess(command, exit_code=exit_code)
        self.processes.append(process)
        return process


def make_config(root: Path, *, port: int = 18765) -> ControlPlaneConfig:
    return ControlPlaneConfig(
        project_dir=root,
        python_executable=root / "python",
        probe_executable=root / "probe",
        bridge_script=root / "qq_bridge.py",
        replay_script=root / "channel_replay.py",
        input_path=root / "lark-notifications.jsonl",
        state_path=root / ".qq-forwarder-state.json",
        channel_state_path=root / ".lark-channel-cursors.json",
        port=port,
    )


def write_channel_state(root: Path) -> None:
    (root / ".lark-channel-cursors.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "baseline_at": "2026-09-01T15:00:00+08:00",
                "comparison": "strictly_after",
                "channels": [
                    {
                        "name": "指定频道",
                        "chat_id": "oc-channel",
                        "cursor_position": 100,
                        "initial_cursor_position": 100,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )


def wait_for(predicate: object, timeout: float = 1.0) -> None:
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if callable(predicate) and predicate():
            return
        time.sleep(0.005)
    raise AssertionError("等待控制面状态超时")


class ProcessSupervisorTests(unittest.TestCase):
    def test_replay_accepts_only_configured_channel_and_can_be_cancelled(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_channel_state(root)
            factory = FakeProcessFactory(replay_exit_code=None)
            supervisor = ProcessSupervisor(make_config(root), process_factory=factory)

            channel_summary = supervisor.status()["channel_replay"]
            self.assertTrue(channel_summary["available"])
            self.assertEqual(
                channel_summary["channels"],
                [{
                    "name": "指定频道",
                    "state": "ready",
                    "label": "游标已建立",
                }],
            )
            self.assertNotIn("chat_id", json.dumps(channel_summary, ensure_ascii=False))
            self.assertNotIn("cursor_position", json.dumps(channel_summary, ensure_ascii=False))

            with self.assertRaises(InvalidAction):
                supervisor.replay("未配置频道")

            running = supervisor.replay("指定频道")
            self.assertEqual(running["replay"]["state"], "running")
            self.assertIn("--channel", factory.processes[0].command)
            self.assertIn("指定频道", factory.processes[0].command)

            cancelling = supervisor.cancel_replay()
            self.assertEqual(cancelling["replay"]["state"], "cancelling")
            wait_for(lambda: supervisor.status()["replay"]["state"] == "cancelled")
            self.assertTrue(
                any(
                    event["type"] == "replay_cancelled"
                    for event in supervisor.status()["events"]
                )
            )
            supervisor.close()

    def test_binding_reports_running_and_succeeds_without_exposing_state(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            state_path = root / ".qq-forwarder-state.json"
            factory = FakeProcessFactory(bind_exit_code=None)
            supervisor = ProcessSupervisor(make_config(root), process_factory=factory)

            binding = supervisor.bind()
            self.assertEqual(binding["binding"]["state"], "binding")
            self.assertIn("@qclaw 绑定测试", binding["events"][-1]["message"])
            self.assertEqual(factory.processes[0].command[2], "bind")

            state_path.write_text(
                json.dumps(
                    {
                        "group_openid": "group-openid-sensitive",
                        "lark_chat_id": "chat-sensitive",
                        "lark_sender_id": "sender-sensitive",
                        "message_body": "不要出现在 API",
                    }
                ),
                encoding="utf-8",
            )
            factory.processes[0]._exit_code = 0
            wait_for(lambda: supervisor.status()["binding"]["state"] == "bound")

            serialized = json.dumps(supervisor.status(), ensure_ascii=False)
            self.assertNotIn("group-openid-sensitive", serialized)
            self.assertNotIn("chat-sensitive", serialized)
            self.assertNotIn("sender-sensitive", serialized)
            self.assertNotIn("不要出现在 API", serialized)
            supervisor.close()

    def test_binding_cancel_keeps_unbound_state(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            factory = FakeProcessFactory(bind_exit_code=None)
            supervisor = ProcessSupervisor(make_config(root), process_factory=factory)

            supervisor.bind()
            cancelling = supervisor.cancel_bind()
            self.assertEqual(cancelling["binding"]["state"], "cancelling")
            wait_for(lambda: supervisor.status()["binding"]["state"] == "unbound")
            self.assertEqual(factory.processes[0].terminate_count, 1)
            self.assertTrue(
                any(event["type"] == "bind_cancelled" for event in supervisor.status()["events"])
            )
            supervisor.close()

    def test_rebind_requires_confirmation_and_adds_rebind_cli_flag(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / ".qq-forwarder-state.json").write_text(
                '{"group_openid":"group-openid-sensitive"}',
                encoding="utf-8",
            )
            factory = FakeProcessFactory(bind_exit_code=None)
            supervisor = ProcessSupervisor(make_config(root), process_factory=factory)

            with self.assertRaises(ConfirmationRequired):
                supervisor.bind(rebind=True)

            supervisor.bind(rebind=True, confirmed=True)
            self.assertIn("--rebind", factory.processes[0].command)
            supervisor.cancel_bind()
            wait_for(lambda: supervisor.status()["binding"]["state"] == "bound")
            supervisor.close()

    def test_test_operation_reports_success_and_failure(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / ".qq-forwarder-state.json").write_text(
                '{"group_openid":"group-openid-sensitive"}',
                encoding="utf-8",
            )
            success_factory = FakeProcessFactory(test_exit_code=0)
            success = ProcessSupervisor(make_config(root), process_factory=success_factory)
            success.test()
            wait_for(lambda: success.status()["test"]["state"] == "succeeded")
            self.assertEqual(success_factory.processes[0].command[2], "test")
            self.assertIn("test_succeeded", [event["type"] for event in success.status()["events"]])
            success.close()

            failure_factory = FakeProcessFactory(test_exit_code=9)
            failure = ProcessSupervisor(make_config(root), process_factory=failure_factory)
            failure.test()
            wait_for(lambda: failure.status()["test"]["state"] == "failed")
            self.assertIn("权限", failure.status()["test"]["failure_message"])
            self.assertNotIn('"exit_code": 9', json.dumps(failure.status(), ensure_ascii=False))
            failure.close()

    def test_prime_normal_and_force_end_are_structured_and_redacted(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / ".qq-forwarder-state.json").write_text(
                json.dumps(
                    {
                        "group_openid": "group-openid-sensitive",
                        "lark_chat_id": "chat-sensitive",
                        "lark_sender_id": "sender-sensitive",
                        "lark_message_position": 42,
                        "recent_message_ids": ["message-sensitive"],
                    }
                ),
                encoding="utf-8",
            )
            factory = FakeProcessFactory(prime_exit_code=0)
            supervisor = ProcessSupervisor(make_config(root), process_factory=factory)

            supervisor.prime()
            wait_for(lambda: supervisor.status()["prime"]["state"] == "succeeded")
            self.assertNotIn("--force-end", factory.processes[0].command)
            self.assertIn("不补发历史消息", supervisor.status()["prime"]["effect"])

            supervisor.prime(force_end=True, confirmed=True)
            wait_for(lambda: supervisor.status()["prime"]["state"] == "succeeded")
            self.assertIn("--force-end", factory.processes[1].command)
            self.assertIn("从最新位置开始", supervisor.status()["prime"]["effect"])

            serialized = json.dumps(supervisor.status(), ensure_ascii=False)
            self.assertNotIn("group-openid-sensitive", serialized)
            self.assertNotIn("chat-sensitive", serialized)
            self.assertNotIn("sender-sensitive", serialized)
            self.assertNotIn("message-sensitive", serialized)
            supervisor.close()

    def test_permission_failure_and_unexpected_exit_have_recovery_guidance(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            permission_factory = FakeProcessFactory(probe_exit_code=2)
            permission_supervisor = ProcessSupervisor(
                make_config(root), process_factory=permission_factory
            )
            permission_supervisor.start()
            wait_for(
                lambda: permission_supervisor.status()["overall"]["state"] == "failed"
            )
            permission_status = permission_supervisor.status()
            self.assertIn("辅助功能", permission_status["overall"]["failure_message"])
            self.assertTrue(
                any("辅助功能" in hint for hint in permission_status["recovery"]["hints"])
            )
            permission_supervisor.close()

            exit_factory = FakeProcessFactory()
            exit_supervisor = ProcessSupervisor(make_config(root), process_factory=exit_factory)
            exit_supervisor.start()
            wait_for(lambda: exit_supervisor.status()["overall"]["state"] == "running")
            forwarder = next(process for process in exit_factory.processes if "run" in process.command)
            forwarder._exit_code = 1
            wait_for(lambda: exit_supervisor.status()["processes"][1]["state"] == "failed")
            exit_status = exit_supervisor.status()
            self.assertIn("发送失败不会推进游标", exit_status["overall"]["failure_message"])
            self.assertTrue(
                any("重启" in hint for hint in exit_status["recovery"]["hints"])
            )
            exit_supervisor.close()

    def test_start_runs_existing_sequence_and_returns_structured_status(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            state_path = root / ".qq-forwarder-state.json"
            state_path.write_text(
                json.dumps(
                    {
                        "group_openid": "group-openid-sensitive",
                        "lark_chat_id": "chat-sensitive",
                        "lark_sender_id": "sender-sensitive",
                        "lark_message_position": 12,
                        "input_path": "/private/image/path-sensitive.jpg",
                    }
                ),
                encoding="utf-8",
            )
            factory = FakeProcessFactory()
            supervisor = ProcessSupervisor(make_config(root), process_factory=factory)

            starting = supervisor.start()
            self.assertEqual(starting["overall"]["state"], "starting")
            wait_for(lambda: supervisor.status()["overall"]["state"] == "running")

            status = supervisor.status()
            commands = [process.command for process in factory.processes]
            self.assertEqual([command[2] for command in commands[1:]], ["prime", "run"])
            run_command = next(command for command in commands if "run" in command)
            self.assertIn("--channel-state", run_command)
            self.assertEqual(
                [process["state"] for process in status["processes"]],
                ["running", "running"],
            )
            serialized = json.dumps(status, ensure_ascii=False)
            self.assertNotIn("group-openid-sensitive", serialized)
            self.assertNotIn("chat-sensitive", serialized)
            self.assertNotIn("sender-sensitive", serialized)
            self.assertNotIn("image/path-sensitive.jpg", serialized)
            supervisor.close()

    def test_prime_failure_does_not_start_forwarder(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            factory = FakeProcessFactory(prime_exit_code=7)
            supervisor = ProcessSupervisor(make_config(root), process_factory=factory)

            supervisor.start()
            wait_for(lambda: supervisor.status()["overall"]["state"] == "failed")

            commands = [process.command for process in factory.processes]
            self.assertEqual(len(commands), 2)
            self.assertNotIn("run", commands[-1])
            self.assertEqual(supervisor.status()["processes"][1]["state"], "stopped")
            self.assertNotIn('"exit_code": 7', json.dumps(supervisor.status(), ensure_ascii=False))
            supervisor.close()

    def test_start_is_idempotently_protected_and_stop_only_terminates_owned_processes(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            factory = FakeProcessFactory()
            supervisor = ProcessSupervisor(make_config(root), process_factory=factory)
            supervisor.start()
            wait_for(lambda: supervisor.status()["overall"]["state"] == "running")

            with self.assertRaises(ActionConflict):
                supervisor.start()

            stopped = supervisor.stop()
            self.assertEqual(stopped["overall"]["state"], "stopped")
            self.assertTrue(all(process.terminate_count == 1 for process in factory.processes if "run" in process.command or "probe" in process.command))
            supervisor.close()

    def test_notification_event_uses_only_wakeup_type(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            input_path = root / "lark-notifications.jsonl"
            input_path.write_text('{"type":"notification_wakeup","body":"旧正文"}\n', encoding="utf-8")
            supervisor = ProcessSupervisor(make_config(root), process_factory=FakeProcessFactory())
            with input_path.open("a", encoding="utf-8") as handle:
                handle.write(
                    json.dumps(
                        {
                            "type": "notification_wakeup",
                            "title": "敏感标题",
                            "body": "敏感正文",
                            "image_path": "/private/image.jpg",
                        }
                    )
                    + "\n"
                )

            supervisor.poll_input_events()
            status = supervisor.status()
            serialized = json.dumps(status, ensure_ascii=False)
            self.assertIn("notification_wakeup", serialized)
            self.assertNotIn("敏感标题", serialized)
            self.assertNotIn("敏感正文", serialized)
            self.assertNotIn("/private/image.jpg", serialized)
            supervisor.close()

    def test_read_only_check_is_structured_and_does_not_use_message_content(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            factory = FakeProcessFactory(check_exit_code=0)
            supervisor = ProcessSupervisor(make_config(root), process_factory=factory)

            running = supervisor.check()
            self.assertEqual(running["check"]["state"], "running")
            wait_for(lambda: supervisor.status()["check"]["state"] == "passed")

            status = supervisor.status()
            self.assertEqual(status["check"]["label"], "检查通过")
            self.assertIn("check", factory.processes[0].command)
            self.assertNotIn("secret", json.dumps(status, ensure_ascii=False))
            supervisor.close()


class LocalControlPlaneHTTPTests(unittest.TestCase):
    def test_http_writes_require_token_and_server_binds_loopback(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with socket.socket() as probe_socket:
                probe_socket.bind(("127.0.0.1", 0))
                port = probe_socket.getsockname()[1]
            config = make_config(root, port=port)
            supervisor = ProcessSupervisor(config, process_factory=FakeProcessFactory())
            server = LocalControlPlaneServer(
                config,
                supervisor,
                static_dir=Path(__file__).resolve().parents[1] / "web",
            )
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            base_url = f"http://127.0.0.1:{port}"
            try:
                with urlopen(f"{base_url}/api/status") as response:
                    status_payload = json.loads(response.read())
                self.assertTrue(status_payload["ok"])
                self.assertEqual(status_payload["data"]["service"]["host"], "127.0.0.1")
                self.assertIn("binding", status_payload["data"]["operations"])
                self.assertIn("test", status_payload["data"]["operations"])
                self.assertIn("prime", status_payload["data"]["operations"])
                self.assertIn("replay", status_payload["data"]["operations"])

                with self.assertRaises(HTTPError) as missing_token:
                    urlopen(Request(f"{base_url}/api/actions/start", method="POST", data=b"{}"))
                self.assertEqual(missing_token.exception.code, 403)

                foreign_origin = Request(
                    f"{base_url}/api/actions/start",
                    method="POST",
                    data=b"{}",
                    headers={
                        "X-Control-Token": server.control_token,
                        "Origin": "http://foreign.invalid",
                    },
                )
                with self.assertRaises(HTTPError) as wrong_origin:
                    urlopen(foreign_origin)
                self.assertEqual(wrong_origin.exception.code, 403)

                for path, body in (
                    ("/api/actions/bind", {"rebind": True}),
                    ("/api/actions/prime/force-end", {"force_end": True}),
                ):
                    missing_confirmation = Request(
                        f"{base_url}{path}",
                        method="POST",
                        data=json.dumps(body).encode("utf-8"),
                        headers={
                            "X-Control-Token": server.control_token,
                            "Content-Type": "application/json",
                        },
                    )
                    with self.assertRaises(HTTPError) as confirmation_error:
                        urlopen(missing_confirmation)
                    self.assertEqual(confirmation_error.exception.code, 400)

                bind_request = Request(
                    f"{base_url}/api/actions/bind",
                    method="POST",
                    data=b"{}",
                    headers={
                        "X-Control-Token": server.control_token,
                        "Content-Type": "application/json",
                    },
                )
                with urlopen(bind_request) as response:
                    bind_payload = json.loads(response.read())
                self.assertEqual(bind_payload["data"]["binding"]["state"], "binding")

                cancel_request = Request(
                    f"{base_url}/api/actions/bind/cancel",
                    method="POST",
                    data=b"{}",
                    headers={
                        "X-Control-Token": server.control_token,
                        "Content-Type": "application/json",
                    },
                )
                with urlopen(cancel_request) as response:
                    cancel_payload = json.loads(response.read())
                self.assertEqual(cancel_payload["data"]["binding"]["state"], "cancelling")

                def read_status() -> dict[str, object]:
                    with urlopen(f"{base_url}/api/status") as response:
                        return json.loads(response.read())

                wait_for(
                    lambda: read_status()["data"]["binding"]["state"] == "unbound"
                )

                request = Request(
                    f"{base_url}/api/actions/start",
                    method="POST",
                    data=b"{}",
                    headers={"X-Control-Token": server.control_token},
                )
                with urlopen(request) as response:
                    action_payload = json.loads(response.read())
                self.assertTrue(action_payload["ok"])
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=1)


if __name__ == "__main__":
    unittest.main()
