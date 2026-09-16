"""The Herdr client: a thin wrapper over the `herdr` CLI, optional everywhere."""

from __future__ import annotations

import os
import socket
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from runtime.plugin_next import herdr as herdr_module  # noqa: E402
from runtime.plugin_next.herdr import Herdr, HerdrError, agent_name_for  # noqa: E402
from tests.fakes import write_fake_herdr  # noqa: E402


class HerdrClientTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.binary = write_fake_herdr(self.root)
        os.environ["FAKE_HERDR_STATE"] = str(self.root / "state.json")
        os.environ["FAKE_HERDR_LOG"] = str(self.root / "calls.log")
        self.client = Herdr(binary=str(self.binary), session="pilot", socket_path="")

    def tearDown(self):
        for key in ("FAKE_HERDR_STATE", "FAKE_HERDR_LOG", "FAKE_HERDR_TRUST", "FAKE_HERDR_AFTER",
                    "FAKE_HERDR_SCREEN", "FAKE_HERDR_CODEX_SESSION", "FAKE_HERDR_W1_CWD"):
            os.environ.pop(key, None)
        self.temp.cleanup()

    def calls(self) -> list[str]:
        return (self.root / "calls.log").read_text().splitlines()

    def test_layout_calls_return_the_ids_to_use_next(self):
        self.assertEqual([w["label"] for w in self.client.workspaces()], ["grep.ai"])
        self.assertEqual(self.client.find_workspace("GREP.AI")["workspace_id"], "w1")
        self.assertIsNone(self.client.find_workspace("nope"))
        tab = self.client.tab_create(workspace_id="w1", cwd=self.temp.name, label="MCP")
        self.assertEqual((tab["workspace_id"], tab["tab_id"], tab["pane_id"]), ("w1", "w1:t2", "w1:p2"))
        ws = self.client.workspace_create(cwd=self.temp.name, label="tether-lab")
        self.assertEqual(ws["workspace_id"], "w3")
        self.assertEqual(ws["pane_id"], "w3:p1")
        self.client.pane_report_metadata("w1:p2", source="tether", tokens={"slack": "https://x/p1"})
        self.assertTrue(any(line.startswith("tab create --workspace w1") and "--no-focus" in line for line in self.calls()))
        self.assertTrue(any("report-metadata w1:p2 --source tether --token slack=https://x/p1" in line for line in self.calls()))

    def test_workspace_for_cwd_matches_the_deepest_checkout(self):
        os.environ["FAKE_HERDR_W1_CWD"] = self.temp.name
        inside = Path(self.temp.name) / "sub"
        inside.mkdir()
        self.assertEqual(self.client.workspace_for_cwd(str(inside))["workspace_id"], "w1")
        self.assertIsNone(self.client.workspace_for_cwd("/"))

    def test_agent_start_prompt_and_session_id(self):
        tab = self.client.tab_create(workspace_id="w1", cwd=self.temp.name, label="MCP")
        started = self.client.agent_start("mcp", kind="claude", pane_id=tab["pane_id"], args=("--dangerously-skip-permissions",))
        self.assertEqual(started["status"], "ready")
        self.assertEqual(self.client.session_id("mcp"), "claude-sess-2")
        self.assertEqual(self.client.find_agent_by_session("claude-sess-2")["name"], "mcp")
        self.assertEqual(self.client.find_agent_by_name("w1:p2")["name"], "mcp")
        self.assertIsNone(self.client.find_agent_by_session("nope"))
        after = self.client.agent_prompt("mcp", "Reply with exactly LAB-OK and nothing else.")
        self.assertEqual(after["agent_status"], "idle")
        prompt_line = next(line for line in self.calls() if line.startswith("agent prompt"))
        self.assertTrue(prompt_line.endswith("--wait"), "a Herdr turn waits with no clock")
        self.assertNotIn("--timeout", prompt_line)
        self.assertTrue(any("-- --dangerously-skip-permissions" in line for line in self.calls()))

    def test_folder_trust_dialog_is_blocked_not_failed(self):
        os.environ["FAKE_HERDR_TRUST"] = "1"
        os.environ["FAKE_HERDR_SCREEN"] = "Is this a project you created or one you trust?\n > No, exit\n   Yes, I trust this folder"
        tab = self.client.tab_create(workspace_id="w1", cwd=self.temp.name, label="MCP")
        started = self.client.agent_start("mcp", kind="claude", pane_id=tab["pane_id"])
        self.assertEqual(started["status"], "blocked")
        self.assertEqual(started["agent"]["agent_status"], "blocked")
        self.assertIn("trust this folder", self.client.agent_read("mcp"))
        with self.assertRaises(HerdrError) as refused:
            self.client.agent_prompt("mcp", "hi")
        self.assertEqual(refused.exception.code, "agent_blocked")
        self.client.send_keys("mcp", "down", "enter")
        self.assertEqual(self.client.agent_wait("mcp")["agent_status"], "idle")
        self.assertEqual(self.client.session_id("mcp"), "claude-sess-2", "the session id arrives once the agent is up")

    def test_codex_reports_no_session_until_the_hook_is_approved(self):
        tab = self.client.tab_create(workspace_id="w1", cwd=self.temp.name, label="cx")
        self.client.agent_start("cx", kind="codex", pane_id=tab["pane_id"])
        self.assertEqual(self.client.session_id("cx"), "")
        os.environ["FAKE_HERDR_CODEX_SESSION"] = "1"
        self.client.agent_start("cx2", kind="codex", pane_id=tab["pane_id"])
        self.assertEqual(self.client.session_id("cx2"), "codex-sess-3")

    def test_errors_carry_the_cli_code(self):
        with self.assertRaises(HerdrError) as missing:
            self.client.agent_get("ghost")
        self.assertEqual(missing.exception.code, "agent_not_found")
        self.assertEqual(herdr_module._error_of('{"id":"x","error":{"code":"server_not_running","message":"no herdr server"}}'),
                         ("server_not_running", "no herdr server"))
        self.assertEqual(herdr_module._error_of("boom")[0], "herdr_error")

    def test_discover_needs_a_binary_and_a_live_socket(self):
        for key in [k for k in os.environ if k.startswith("HERDR_")]:
            os.environ.pop(key)  # this test may itself run inside a Herdr pane
        home = self.root / "config"
        (home / "sessions" / "pilot").mkdir(parents=True)
        os.environ["PATH"] = f"{self.root}{os.pathsep}{os.environ.get('PATH', '')}"
        self.assertIsNone(Herdr.discover(binary="herdr", home=home), "no socket: not available")
        dead = home / "sessions" / "pilot" / "herdr.sock"
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        listener.bind(str(dead))
        listener.close()  # a socket file nobody listens on
        self.assertIsNone(Herdr.discover(binary="herdr", home=home), "dead socket: not available")
        live_path = home / "sessions" / "pilot" / "herdr.sock"
        live_path.unlink()
        server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        server.bind(str(live_path))
        server.listen(1)
        try:
            client = Herdr.discover(binary="herdr", home=home)
            self.assertIsNotNone(client)
            self.assertEqual((client.session, client.socket_path), ("pilot", str(live_path)))
            self.assertTrue(client.available())
            self.assertIsNone(client.env, "the process environment is read at call time, minus HERDR_*")
            self.assertIsNone(Herdr.discover(binary="no-such-herdr-binary", home=home))
        finally:
            server.close()

    def test_agent_names_are_valid_herdr_names(self):
        self.assertEqual(agent_name_for("MCP"), "mcp")
        self.assertEqual(agent_name_for("Debug why experts (MCP) are missing forms!"), "debug-why-experts-mcp-are-missin")
        self.assertEqual(agent_name_for("42 things"), "t-42-things")
        self.assertEqual(agent_name_for("!!!"), "tether")


if __name__ == "__main__":
    unittest.main()
