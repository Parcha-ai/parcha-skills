import importlib.util
import os
import pathlib
import sys
import tempfile
import unittest
from unittest import mock


ROOT = pathlib.Path(__file__).resolve().parents[1]
RUNTIME_PATH = ROOT / "runtime" / "bridge_runtime.py"


def load_runtime(home: pathlib.Path):
    env = {
        "HOME": str(home),
        "HERMES_HOME": str(home / ".hermes"),
        "XDG_DATA_HOME": str(home / ".local" / "share"),
        "XDG_CONFIG_HOME": str(home / ".config"),
    }
    with mock.patch.dict(os.environ, env, clear=False):
        name = f"cwd_identity_test_{id(home)}"
        spec = importlib.util.spec_from_file_location(name, RUNTIME_PATH)
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
        return module


class WorkingDirectoryIdentityTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.home = pathlib.Path(self.temp.name)
        self.runtime = load_runtime(self.home)
        self.project = self.home / "project"
        self.project.mkdir()

    def tearDown(self):
        self.temp.cleanup()

    def bridge(self, *, include_identity: bool = True):
        source = {
            "session_id": "codex-session",
            "cwd": str(self.project),
        }
        if include_identity:
            source.update(
                self.runtime.working_directory_identity(str(self.project))
            )
        return self.runtime.Bridge(
            "brg_test",
            "codex_session",
            source,
            "U12345678",
            "T12345678",
            "C12345678",
            "123.456",
            "cwd-test",
            "active",
            1,
            2,
            "verified",
            "",
        )

    def test_missing_identity_requires_rebind_before_process_start(self):
        with mock.patch.object(
            self.runtime,
            "_resolve_executable",
            return_value="/usr/bin/codex",
        ), mock.patch.object(
            self.runtime.subprocess,
            "Popen",
        ) as process, self.assertRaisesRegex(
            self.runtime.NativeContinuationError,
            "identity is missing",
        ) as raised:
            self.runtime.continue_native(
                self.bridge(include_identity=False),
                "continue",
            )
        self.assertEqual(raised.exception.code, "binding_rebind_required")
        process.assert_not_called()

    def test_replaced_directory_is_rejected_before_process_start(self):
        bridge = self.bridge()
        self.project.rename(self.home / "project-original")
        self.project.mkdir()
        with mock.patch.object(
            self.runtime,
            "_resolve_executable",
            return_value="/usr/bin/codex",
        ), mock.patch.object(
            self.runtime.subprocess,
            "Popen",
        ) as process, self.assertRaisesRegex(
            self.runtime.NativeContinuationError,
            "replaced or changed identity",
        ) as raised:
            self.runtime.continue_native(bridge, "continue")
        self.assertEqual(raised.exception.code, "cwd_identity_changed")
        process.assert_not_called()

    def test_child_uses_pinned_directory_descriptor(self):
        captured = {}

        class Process:
            returncode = 0
            pid = 12345

            def __init__(self, command, **kwargs):
                captured["command"] = command
                captured.update(kwargs)

        def collect(_process, prompt, _deadline, _cancel_event):
            captured["input"] = prompt
            return b"done", b"", False

        with mock.patch.object(
            self.runtime,
            "_resolve_executable",
            return_value="/usr/bin/codex",
        ), mock.patch.object(
            self.runtime.subprocess,
            "Popen",
            Process,
        ), mock.patch.object(
            self.runtime,
            "_collect_native_output",
            side_effect=collect,
        ):
            result = self.runtime.continue_native(
                self.bridge(),
                "continue",
            )
        self.assertEqual(result, "done")
        self.assertEqual(captured["input"], "continue")
        self.assertTrue(
            captured["cwd"].startswith(("/proc/self/fd/", "/dev/fd/"))
        )
        self.assertEqual(len(captured["pass_fds"]), 1)
        self.assertNotEqual(captured["cwd"], str(self.project))


if __name__ == "__main__":
    unittest.main()
