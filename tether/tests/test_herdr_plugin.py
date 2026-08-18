import importlib.util
import json
import os
import pathlib
import stat
import tempfile
import tomllib
import types
import unittest
from unittest import mock


ROOT = pathlib.Path(__file__).resolve().parents[1]
PLUGIN_ROOT = ROOT / "herdr-plugin"


def load_plugin():
    spec = importlib.util.spec_from_file_location(
        "tether_herdr_plugin_test",
        PLUGIN_ROOT / "tether_plugin.py",
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class HerdrPluginTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.state = pathlib.Path(self.temp.name) / "state"
        self.state.mkdir(mode=0o700)
        self.plugin = load_plugin()

    def tearDown(self):
        self.temp.cleanup()

    def environment(self):
        return {
            "HERDR_PLUGIN_STATE_DIR": str(self.state),
            "HERDR_BIN_PATH": "/usr/bin/herdr",
            "HERDR_PLUGIN_CONTEXT_JSON": json.dumps({
                "focused_pane_id": "w1:p1",
                "focused_pane_cwd": "/tmp/project",
                "clicked_url": (
                    "https://example.slack.com/archives/C12345678/"
                    "p1234567890123456"
                ),
            }),
        }

    def test_manifest_declares_linux_popup_actions_and_link_handler(self):
        manifest = tomllib.loads(
            (PLUGIN_ROOT / "herdr-plugin.toml").read_text(encoding="utf-8")
        )
        self.assertEqual(manifest["id"], "parcha.tether")
        self.assertEqual(manifest["version"], "0.3.0-beta.1")
        self.assertEqual(manifest["min_herdr_version"], "0.8.0")
        self.assertEqual(manifest["platforms"], ["linux"])
        self.assertEqual(manifest["panes"][0]["placement"], "popup")
        self.assertEqual(
            {action["id"] for action in manifest["actions"]},
            {"open-cockpit", "attach-slack-thread", "rebind-current"},
        )
        self.assertEqual(manifest["link_handlers"][0]["action"], "attach-slack-thread")

    def test_action_handoff_keeps_slack_url_out_of_child_argv(self):
        completed = types.SimpleNamespace(returncode=0)
        with mock.patch.dict(os.environ, self.environment(), clear=True), mock.patch.object(
            self.plugin.subprocess,
            "run",
            return_value=completed,
        ) as run:
            result = self.plugin.open_cockpit("attach")

        self.assertEqual(result, 0)
        argv = run.call_args.args[0]
        self.assertFalse(any("slack.com" in argument for argument in argv))
        self.assertNotIn("--target-pane", argv)
        self.assertEqual(argv[-1], "--focus")
        token_argument = next(argument for argument in argv if argument.startswith("TETHER_INVOCATION_ID="))
        token = token_argument.split("=", 1)[1]
        target = self.state / f"invocation-{token}.json"
        self.assertEqual(stat.S_IMODE(target.stat().st_mode), 0o600)
        self.assertIn("slack.com", target.read_text(encoding="utf-8"))

    def test_invocation_is_single_use_and_whitelisted(self):
        context = {
            "focused_pane_id": "w1:p1",
            "clicked_url": "https://example.slack.com/archives/C12345678/p1234567890123456",
            "secret": "must-not-persist",
        }
        with mock.patch.dict(
            os.environ,
            {"HERDR_PLUGIN_STATE_DIR": str(self.state)},
            clear=True,
        ):
            token = self.plugin.save_invocation(
                {key: value for key, value in context.items() if key in self.plugin.CONTEXT_FIELDS},
                "attach",
            )
            mode, restored = self.plugin.load_invocation(token)

        self.assertEqual(mode, "attach")
        self.assertEqual(restored["focused_pane_id"], "w1:p1")
        self.assertNotIn("secret", restored)
        self.assertFalse((self.state / f"invocation-{token}.json").exists())

    def test_idempotency_is_stable_for_same_agent_and_input(self):
        status = {"terminal_id": "term_abc"}
        first = self.plugin._idempotency_key("attach", status, "thread")
        second = self.plugin._idempotency_key("attach", status, "thread")
        self.assertEqual(first, second)
        self.assertNotEqual(
            first,
            self.plugin._idempotency_key("attach", status, "other-thread"),
        )

    def test_attach_passes_slack_url_only_on_stdin(self):
        status = {"terminal_id": "term_abc", "bridge": None}
        screen = mock.Mock()
        with mock.patch.object(
            self.plugin,
            "_prompt",
            return_value="https://example.slack.com/archives/C12345678/p1234567890123456",
        ), mock.patch.object(
            self.plugin, "_confirm", return_value=True
        ), mock.patch.object(
            self.plugin, "_tether_binary", return_value="/usr/bin/tether"
        ), mock.patch.object(
            self.plugin,
            "run_json",
            return_value={"thread_ts": "1234567890.123456"},
        ) as run:
            self.plugin._run_action(
                screen,
                {"focused_pane_id": "w1:p1"},
                status,
                "a",
                "",
            )

        argv = run.call_args.args[0]
        self.assertIn("--slack-url-stdin", argv)
        self.assertFalse(any("slack.com" in argument for argument in argv))
        self.assertIn("slack.com", run.call_args.kwargs["input_text"])


if __name__ == "__main__":
    unittest.main()
