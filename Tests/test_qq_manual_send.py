import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from qq_bridge import BridgeError, StateStore, send_manual_message


class ManualSendTests(unittest.IsolatedAsyncioTestCase):
    async def test_only_selected_group_receives_exact_text(self):
        with tempfile.TemporaryDirectory() as directory:
            state = StateStore.load(Path(directory) / "state.json")
            state.bind_group("group-a")
            state.add_group_binding("group-b")
            selected = state.group_bindings[1]["binding_id"]
            api, client = object(), AsyncMock()
            with patch("qq_bridge.create_api", AsyncMock(return_value=(api, client))), patch("qq_bridge.send_group_text", AsyncMock()) as send:
                await send_manual_message(state, selected, "正文\n第二行")
                send.assert_awaited_once_with(api, "group-b", "正文\n第二行")
                client.aclose.assert_awaited_once()

    async def test_invalid_target_does_not_connect_and_failure_closes_client(self):
        with tempfile.TemporaryDirectory() as directory:
            state = StateStore.load(Path(directory) / "state.json")
            state.bind_group("group-a")
            with patch("qq_bridge.create_api", AsyncMock()) as connect:
                with self.assertRaises(BridgeError):
                    await send_manual_message(state, "missing", "正文")
                connect.assert_not_awaited()
            api, client = object(), AsyncMock()
            with patch("qq_bridge.create_api", AsyncMock(return_value=(api, client))), patch("qq_bridge.send_group_text", AsyncMock(side_effect=BridgeError("failure"))):
                with self.assertRaises(BridgeError):
                    await send_manual_message(state, state.group_bindings[0]["binding_id"], "正文")
                client.aclose.assert_awaited_once()
