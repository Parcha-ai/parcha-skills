"""Offline example proves an artifact outcome, not simulated model intelligence."""
from contextlib import redirect_stdout
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import socket
import subprocess
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import urllib.request


PATH = Path(__file__).resolve().parents[1] / "runtime/plugin_next/demo.py"
SPEC = importlib.util.spec_from_file_location("tether_demo_tested", PATH)
demo = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(demo)


class DemoTest(unittest.TestCase):
    def test_real_artifact_review_correction_and_human_followup(self):
        result = demo.run_demo()
        self.assertTrue(result["ok"])
        self.assertTrue(result["simulated"])
        self.assertFalse(result["artifact"]["initial_review"]["passed"])
        failure = result["artifact"]["initial_review"]["cases"][2]
        self.assertEqual((failure["input"], failure["expected"], failure["actual"]), ([1, 5], 3, 5))
        self.assertTrue(result["artifact"]["final_review"]["passed"])
        self.assertEqual(result["task_outcome"]["completed_tasks"], 1)
        self.assertEqual(result["task_outcome"]["completed_transport_attempts"], 5)
        self.assertEqual([t["artifact_accepted"] for t in result["turns"]], [False, False, False, True, True])
        engineer = [t for t in result["turns"] if t["profile"] == "implementer"]
        self.assertEqual(len(engineer), 3)
        for field in ("binding_id", "endpoint_id", "native_session_id"):
            self.assertEqual(len({t[field] for t in engineer}), 1)
        self.assertTrue(engineer[-1]["actor_is_simulated_human"])
        self.assertIn("= 3.", result["deliveries"][-1]["text"])
        for turn, delivery in zip(result["turns"], result["deliveries"]):
            self.assertEqual(turn["fake_host_message_id"], delivery["message_id"])
            self.assertEqual(turn["profile"], delivery["profile"])
            self.assertEqual(turn["saved_answer_sha256"], hashlib.sha256(delivery["text"].encode()).hexdigest())
            self.assertEqual(turn["native_result_session_id"], turn["native_session_id"])
            self.assertEqual((delivery["channel"], delivery["thread"]), ("CDEMO", "100.1"))
            self.assertTrue(delivery["simulated_ack"])

    def test_no_network_process_discovery_or_credential_echo(self):
        with patch.object(subprocess, "Popen", side_effect=AssertionError("process forbidden")), \
             patch.object(subprocess, "run", side_effect=AssertionError("process forbidden")), \
             patch.object(socket, "socket", side_effect=AssertionError("network forbidden")), \
             patch.object(urllib.request, "urlopen", side_effect=AssertionError("network forbidden")), \
             patch.dict("os.environ", {"TETHER_HERDR": "on", "SLACK_BOT_TOKEN": "SENSITIVE_FIXTURE_VALUE", "ANTHROPIC_API_KEY": "SENSITIVE_FIXTURE_VALUE"}):
            result = demo.run_demo()
            self.assertTrue(result["ok"])
            self.assertNotIn("SENSITIVE_FIXTURE_VALUE", json.dumps(result))
        self.assertEqual((result["actual_model_calls"], result["actual_slack_calls"], result["native_processes"]), (0, 0, 0))
        with demo._core() as core:
            with self.assertRaises(demo.DemoFailure):
                core.herdr.Herdr.discover()

    def test_bad_correction_cannot_be_reported_complete(self):
        with patch.object(demo, "CORRECTED", demo.INITIAL):
            with self.assertRaisesRegex(demo.DemoFailure, "did not save and deliver"):
                demo.run_demo()

    def test_foreign_native_result_rejected_and_all_temp_files_removed(self):
        original_result = demo._Computer.read_result
        original_temp = tempfile.TemporaryDirectory
        roots = []
        def temporary(*args, **kwargs):
            value = original_temp(*args, **kwargs)
            roots.append(Path(value.name))
            return value
        def foreign(computer, timeout):
            return {**original_result(computer, timeout), "session_id": "foreign-simulated-native"}
        with patch.object(demo._Computer, "read_result", foreign), \
             patch.object(demo.tempfile, "TemporaryDirectory", temporary):
            with self.assertRaisesRegex(demo.DemoFailure, "original session"):
                demo.run_demo()
        self.assertEqual(len(roots), 1)
        self.assertTrue(all(not root.exists() for root in roots))

    def test_team_names_only_never_select_live_account_or_computer(self):
        team = SimpleNamespace(self_id="maker", colleagues=(
            SimpleNamespace(id="review", name="My Reviewer", slack_id="UREALREVIEW", computer="live-review"),
            SimpleNamespace(id="maker", name="My Engineer", slack_id="UREALMAKER", computer="live-shell")))
        result = demo.run_demo(team=team)
        self.assertEqual(result["colleagues"], {"implementer": "My Engineer", "reviewer": "My Reviewer"})
        text = json.dumps(result)
        self.assertNotIn("UREAL", text)
        self.assertNotIn("live-shell", text)
        self.assertTrue(result["ok"])

    def test_json_cli_success_and_failed_artifact_are_distinct(self):
        output = io.StringIO()
        with redirect_stdout(output):
            code = demo.main(["--json"])
        self.assertEqual(code, 0)
        self.assertTrue(json.loads(output.getvalue())["ok"])
        output = io.StringIO()
        with patch.object(demo, "CORRECTED", demo.INITIAL), redirect_stdout(output):
            code = demo.main(["--json"])
        self.assertEqual(code, 1)
        result = json.loads(output.getvalue())
        self.assertFalse(result["ok"])
        self.assertEqual(result["error_type"], "DemoFailure")

    def test_explicit_manifest_cli_uses_real_parser_without_live_settings(self):
        with tempfile.TemporaryDirectory() as root:
            config = Path(root) / "team.toml"
            config.write_text('version = 1\nself = "maker"\n'
                '[[colleagues]]\nid = "maker"\nname = "Custom Engineer"\nrole = "Build"\n'
                'slack_id = "UREALMAKER"\ncomputer = "live-shell"\n'
                '[[colleagues]]\nid = "review"\nname = "Custom Reviewer"\nrole = "Review"\n')
            output = io.StringIO()
            with redirect_stdout(output):
                code = demo.main(["--json", "--team-config", str(config)])
            self.assertEqual(code, 0)
            result = json.loads(output.getvalue())
            self.assertEqual(result["colleagues"]["implementer"], "Custom Engineer")
            self.assertEqual(result["colleagues"]["reviewer"], "Custom Reviewer")
            self.assertNotIn("UREALMAKER", output.getvalue())
            self.assertNotIn("live-shell", output.getvalue())


if __name__ == "__main__":
    unittest.main()
