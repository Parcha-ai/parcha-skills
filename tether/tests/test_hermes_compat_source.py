from __future__ import annotations

import importlib.util
from pathlib import Path
import subprocess
import sys
import tempfile
import types
import unittest
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
COMPAT_PATH = ROOT / "runtime" / "hermes_compat.py"


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


class HermesSourceCompatibilityTest(unittest.TestCase):
    def setUp(self):
        self.compat_name = f"tether_hermes_source_test_{id(self)}"
        self.compat = load_module(self.compat_name, COMPAT_PATH)
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        files = {
            ".gitignore": "__pycache__/\n*.pyc\ntools/generated_*.py\n",
            "hermes_cli/__init__.py": '__version__ = "0.19.0"\n',
            "hermes_cli/plugins.py": "# audited hooks\n",
            "gateway/run.py": "# audited gateway\n",
            "gateway/platforms/base.py": "# audited base\n",
            "tools/runtime_helper.py": "# audited helper\n",
            "plugins/platforms/slack/adapter.py": (
                "class SlackAdapter:\n"
                "    pass\n"
            ),
        }
        for relative, content in files.items():
            target = self.root / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(content, encoding="utf-8")
        subprocess.run(
            ["git", "init", "-q"],
            cwd=self.root,
            check=True,
        )
        subprocess.run(
            ["git", "config", "user.name", "Tether Test"],
            cwd=self.root,
            check=True,
        )
        subprocess.run(
            ["git", "config", "user.email", "tether@example.invalid"],
            cwd=self.root,
            check=True,
        )
        subprocess.run(["git", "add", "."], cwd=self.root, check=True)
        subprocess.run(
            ["git", "commit", "-qm", "audited source"],
            cwd=self.root,
            check=True,
        )
        self.head = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=self.root,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()

        self.original_hermes = sys.modules.get("hermes_cli")
        self.hermes = load_module(
            "hermes_cli",
            self.root / "hermes_cli" / "__init__.py",
        )
        self.adapter_module_name = f"audited_slack_adapter_{id(self)}"
        self.adapter_module = load_module(
            self.adapter_module_name,
            self.root / "plugins" / "platforms" / "slack" / "adapter.py",
        )

    def tearDown(self):
        if self.original_hermes is None:
            sys.modules.pop("hermes_cli", None)
        else:
            sys.modules["hermes_cli"] = self.original_hermes
        sys.modules.pop(self.adapter_module_name, None)
        sys.modules.pop(self.compat_name, None)
        self.temp.cleanup()

    def test_exact_clean_checkout_is_accepted(self):
        observed = self.compat.verify_hermes_source(
            self.adapter_module.SlackAdapter,
            expected_commit=self.head,
        )
        self.assertEqual(observed, self.head)

    def test_commit_mismatch_is_rejected(self):
        with self.assertRaisesRegex(
            self.compat.HermesCompatibilityError,
            "unsupported Hermes source commit",
        ):
            self.compat.verify_hermes_source(
                self.adapter_module.SlackAdapter,
                expected_commit="0" * 40,
            )

    def test_dirty_tracked_source_outside_critical_set_is_rejected(self):
        with (self.root / "tools" / "runtime_helper.py").open(
            "a",
            encoding="utf-8",
        ) as stream:
            stream.write("# local patch\n")
        with self.assertRaisesRegex(
            self.compat.HermesCompatibilityError,
            "local changes",
        ):
            self.compat.verify_hermes_source(
                self.adapter_module.SlackAdapter,
                expected_commit=self.head,
            )

    def test_untracked_source_is_rejected(self):
        (self.root / "tools" / "runtime_shadow.py").write_text(
            "# untracked import shadow\n",
            encoding="utf-8",
        )
        with self.assertRaisesRegex(
            self.compat.HermesCompatibilityError,
            "local changes",
        ):
            self.compat.verify_hermes_source(
                self.adapter_module.SlackAdapter,
                expected_commit=self.head,
            )

    def test_ignored_python_source_overlay_is_rejected(self):
        (self.root / "tools" / "generated_shadow.py").write_text(
            "# ignored import shadow\n",
            encoding="utf-8",
        )
        with self.assertRaisesRegex(
            self.compat.HermesCompatibilityError,
            "ignored Python overlays",
        ):
            self.compat.verify_hermes_source(
                self.adapter_module.SlackAdapter,
                expected_commit=self.head,
            )

    def test_adapter_outside_checkout_is_rejected(self):
        class ForeignAdapter:
            pass

        with self.assertRaisesRegex(
            self.compat.HermesCompatibilityError,
            "not loaded from the audited checkout",
        ):
            self.compat.verify_hermes_source(
                ForeignAdapter,
                expected_commit=self.head,
            )


class HermesHookCompatibilityTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.compat_name = "tether_hermes_hook_test"
        cls.compat = load_module(cls.compat_name, COMPAT_PATH)

    @classmethod
    def tearDownClass(cls):
        sys.modules.pop(cls.compat_name, None)

    def test_authoritative_hook_is_first_and_unique(self):
        def earlier(**_kwargs):
            return {"action": "allow"}

        def later(**_kwargs):
            return {"action": "rewrite", "text": "wrong"}

        def tether(**_kwargs):
            return {"action": "skip"}

        manager = types.SimpleNamespace(
            _hooks={"pre_gateway_dispatch": [earlier]},
        )

        class Context:
            def __init__(self, active_manager):
                self._manager = active_manager

            def register_hook(self, name, callback):
                self._manager._hooks.setdefault(name, []).append(callback)

        context = Context(manager)
        self.compat.register_authoritative_gateway_hook(context, tether)
        manager._hooks["pre_gateway_dispatch"].append(later)
        self.compat.register_authoritative_gateway_hook(context, tether)

        self.assertEqual(
            manager._hooks["pre_gateway_dispatch"],
            [tether, earlier, later],
        )

    def test_incompatible_hook_registry_fails_closed(self):
        class Context:
            _manager = types.SimpleNamespace()

            def register_hook(self, _name, _callback):
                return None

        with self.assertRaisesRegex(
            self.compat.HermesCompatibilityError,
            "hook registry",
        ):
            self.compat.register_authoritative_gateway_hook(
                Context(),
                lambda **_kwargs: None,
            )


class HermesAdapterSignatureTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.compat_name = "tether_hermes_signature_test"
        cls.compat = load_module(cls.compat_name, COMPAT_PATH)

    @classmethod
    def tearDownClass(cls):
        sys.modules.pop(cls.compat_name, None)

    @staticmethod
    def compatible_adapter_type():
        class CompatibleAdapter:
            async def _handle_slack_message(self, event, payload=None):
                return None

            def _get_client(self, chat_id, team_id=None):
                return None

            async def _ensure_dm_conversation(self, chat_id, team_id=None):
                return chat_id

            def _is_ignored_channel(self, channel_id):
                return False

            async def _remove_reaction(
                self,
                channel,
                timestamp,
                emoji,
                team_id="",
            ):
                return None

            def _pop_slash_context(self, chat_id, team_id=""):
                return None

            async def _send_slash_ephemeral(self, ctx, content):
                return None

            async def _upload_file(
                self,
                chat_id,
                file_path,
                caption=None,
                reply_to=None,
                metadata=None,
            ):
                return None

            def _resolve_thread_ts(self, reply_to=None, metadata=None):
                return None

            def _maybe_blocks(self, content):
                return None

            async def edit_message(
                self,
                chat_id,
                message_id,
                content,
                *,
                finalize=False,
                metadata=None,
            ):
                return None

            def format_message(self, content):
                return content

            async def send_clarify(
                self,
                chat_id,
                question,
                choices,
                clarify_id,
                session_key,
                metadata=None,
            ):
                return None

            async def send_exec_approval(
                self,
                chat_id,
                command,
                session_key,
                description="dangerous command",
                metadata=None,
            ):
                return None

            async def send_private_notice(
                self,
                chat_id,
                user_id,
                content,
                reply_to=None,
                metadata=None,
            ):
                return None

            async def send_slash_confirm(
                self,
                chat_id,
                title,
                message,
                session_key,
                confirm_id,
                metadata=None,
            ):
                return None

            async def send_video(
                self,
                chat_id,
                video_path,
                caption=None,
                reply_to=None,
                metadata=None,
            ):
                return None

            async def send_document(
                self,
                chat_id,
                file_path,
                caption=None,
                file_name=None,
                reply_to=None,
                metadata=None,
            ):
                return None

            async def send_multiple_images(
                self,
                chat_id,
                images,
                metadata=None,
                human_delay=0.0,
            ):
                return None

            @staticmethod
            def truncate_message(content, max_length=4096):
                return [content]

            async def stop_typing(self, chat_id, metadata=None):
                return None

            async def connect(self):
                return None

            async def send(
                self,
                chat_id,
                content,
                reply_to=None,
                metadata=None,
            ):
                return None

        return CompatibleAdapter

    def test_required_wrapper_surface_is_verified(self):
        adapter_type = self.compatible_adapter_type()
        with mock.patch.object(
            self.compat,
            "verify_hermes_source",
            return_value=self.compat.TESTED_HERMES_COMMIT,
        ):
            self.assertEqual(
                self.compat.validate_adapter(adapter_type, version="0.19.0"),
                "0.19.0",
            )

    def test_missing_send_metadata_parameter_is_rejected(self):
        class Adapter(self.compatible_adapter_type()):
            async def send(self, chat_id, content):
                return None

        with mock.patch.object(
            self.compat,
            "verify_hermes_source",
            return_value=self.compat.TESTED_HERMES_COMMIT,
        ), self.assertRaisesRegex(
            self.compat.HermesCompatibilityError,
            "send is missing parameters",
        ):
            self.compat.validate_adapter(Adapter, version="0.19.0")


if __name__ == "__main__":
    unittest.main()
