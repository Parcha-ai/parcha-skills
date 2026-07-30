import hashlib
import importlib.util
import json
import os
import pathlib
import sys
import tempfile
import types
import unittest
from unittest import mock


ROOT = pathlib.Path(__file__).resolve().parents[1]
RUNTIME_PATH = ROOT / "runtime" / "bridge_runtime.py"
BOOT_ID = "00000000-0000-4000-8000-000000000001"


def load_runtime(home: pathlib.Path):
    environment = {
        "HOME": str(home),
        "HERMES_HOME": str(home / ".hermes"),
        "XDG_DATA_HOME": str(home / ".local" / "share"),
        "XDG_CONFIG_HOME": str(home / ".config"),
    }
    with mock.patch.dict(os.environ, environment, clear=False):
        name = f"process_identity_hardening_{id(home)}"
        spec = importlib.util.spec_from_file_location(name, RUNTIME_PATH)
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
        return module


class ProcessIdentityHardeningTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.home = pathlib.Path(self.temp.name)
        self.runtime = load_runtime(self.home)
        self.proc_root = self.home / "proc"
        boot_path = self.proc_root / "sys" / "kernel" / "random"
        boot_path.mkdir(parents=True)
        (boot_path / "boot_id").write_text(BOOT_ID, encoding="utf-8")
        self.write_process(
            pid=100,
            executable=self.home / "bin" / "bash",
            parent_pid=1,
            process_group=100,
            foreground_group=200,
        )

    def tearDown(self):
        self.temp.cleanup()

    def identity_payload(self, **overrides):
        executable = self.home / "bin" / "codex"
        payload = {
            "agent": "codex",
            "boot": BOOT_ID,
            "exe": "1:2",
            "exe_path": hashlib.sha256(str(executable).encode()).hexdigest()[:16],
            "pane": "51",
            "pid": 200,
            "session": "didactic-jellyfish",
            "start": "20000",
            "tty": "34851",
        }
        payload.update(overrides)
        return payload

    def identity(self, payload=None):
        return self.runtime.PROCESS_IDENTITY_PREFIX + json.dumps(
            payload or self.identity_payload(),
            sort_keys=True,
            separators=(",", ":"),
        )

    def source(self, identity=None, **overrides):
        source = {
            "session_id": "codex-session",
            "zellij_session": "didactic-jellyfish",
            "zellij_pane_id": "51",
            "cwd": "/tmp/project",
            "pane_agent": "codex",
            "process_identity": identity or self.identity(),
            "binding_version": "2",
            "binding_state": "verified",
            "endpoint_kind": "zellij_pane",
            "delivery_policy": "native_required",
        }
        source.update(overrides)
        return source

    def assert_identity_rejected(self, identity, **source_overrides):
        with self.assertRaises(
            (ValueError, self.runtime.NativeContinuationError)
        ) as raised:
            self.runtime.Store.validate_source(
                "codex_session",
                self.source(identity, **source_overrides),
            )
        self.assertTrue(str(raised.exception).strip())

    def write_process(
        self,
        *,
        pid,
        executable,
        argv=None,
        parent_pid=100,
        start_time=20_000,
        session="didactic-jellyfish",
        pane="51",
        process_group=None,
        foreground_group=None,
        tty_number=34851,
    ):
        executable_path = pathlib.Path(executable)
        executable_path.parent.mkdir(parents=True, exist_ok=True)
        if not executable_path.exists():
            executable_path.write_text("#!/bin/sh\n", encoding="utf-8")
            executable_path.chmod(0o700)

        process = self.proc_root / str(pid)
        process.mkdir(parents=True)
        (process / "environ").write_bytes(
            (
                f"ZELLIJ_SESSION_NAME={session}\0"
                f"ZELLIJ_PANE_ID={pane}\0"
            ).encode()
        )
        command = argv or (str(executable_path),)
        (process / "cmdline").write_bytes(
            b"\0".join(part.encode() for part in command) + b"\0"
        )
        process_group = process_group if process_group is not None else pid
        foreground_group = (
            foreground_group if foreground_group is not None else process_group
        )
        stat_fields = [
            "S",
            str(parent_pid),
            str(process_group),
            str(process_group),
            str(tty_number),
            str(foreground_group),
            *(["0"] * 13),
            str(start_time),
        ]
        (process / "stat").write_text(
            f"{pid} ({executable_path.name}) " + " ".join(stat_fields),
            encoding="utf-8",
        )
        (process / "exe").symlink_to(executable_path)
        return process

    def resolve(self, allowed=None):
        allowed_agents = allowed or {"codex", "claude"}
        trusted_paths = {
            agent: {
                str(path.resolve())
                for path in self.home.rglob("*")
                if path.is_file() and path.stem == agent
            }
            for agent in allowed_agents
        }
        return self.runtime._zellij_agent_process(
            "didactic-jellyfish",
            "51",
            allowed_agents,
            self.proc_root,
            metadata_agent="codex",
            trusted_paths=trusted_paths,
        )

    def decode(self, descriptor):
        self.assertTrue(
            descriptor.startswith(self.runtime.PROCESS_IDENTITY_PREFIX)
        )
        return json.loads(
            descriptor.removeprefix(self.runtime.PROCESS_IDENTITY_PREFIX)
        )

    def test_rejects_malformed_and_missing_linux_proc_identity_fields(self):
        malformed = (
            "linux-proc-v1:",
            "linux-proc-v1:not-json",
            "linux-proc-v1:[]",
            "linux-proc-v1:{}",
        )
        for identity in malformed:
            with self.subTest(identity=identity):
                self.assert_identity_rejected(identity)

        required = (
            "agent",
            "boot",
            "exe",
            "exe_path",
            "pane",
            "pid",
            "session",
            "start",
        )
        for field in required:
            payload = self.identity_payload()
            del payload[field]
            with self.subTest(missing=field):
                self.assert_identity_rejected(self.identity(payload))

    def test_rejects_wrong_linux_proc_identity_field_types(self):
        wrong_values = {
            "agent": 7,
            "boot": None,
            "exe": ["1:2"],
            "exe_path": {"hash": "abc"},
            "pane": 51,
            "pid": "200",
            "session": False,
            "start": 20_000,
        }
        for field, wrong_value in wrong_values.items():
            payload = self.identity_payload()
            payload[field] = wrong_value
            with self.subTest(field=field, wrong_value=wrong_value):
                self.assert_identity_rejected(self.identity(payload))

    def test_rejects_identity_cross_field_mismatches(self):
        cases = (
            (
                self.identity(self.identity_payload(agent="claude")),
                {},
                "agent",
            ),
            (
                self.identity(self.identity_payload(session="other-session")),
                {},
                "session",
            ),
            (
                self.identity(self.identity_payload(pane="99")),
                {},
                "pane",
            ),
            (
                self.identity(),
                {"pane_agent": "claude"},
                "adapter",
            ),
        )
        for identity, overrides, mismatch in cases:
            with self.subTest(mismatch=mismatch):
                self.assert_identity_rejected(identity, **overrides)

    def test_rejects_spoofed_argv0_when_executable_is_not_allowlisted(self):
        shell = self.home / "bin" / "bash"
        self.write_process(
            pid=200,
            executable=shell,
            argv=("codex", "exec", "resume", "session-1"),
        )

        with self.assertRaises(self.runtime.NativeContinuationError) as raised:
            self.resolve({"codex"})

        self.assertEqual(raised.exception.code, "process_identity_missing")

    def test_pid_reuse_with_changed_start_ticks_changes_identity(self):
        codex = self.home / "bin" / "codex"
        process = self.write_process(
            pid=200,
            executable=codex,
            start_time=20_000,
        )
        _, first = self.resolve({"codex"})
        fields = (process / "stat").read_text(encoding="utf-8").split()
        fields[-1] = "30000"
        (process / "stat").write_text(" ".join(fields), encoding="utf-8")
        _, second = self.resolve({"codex"})

        self.assertNotEqual(first, second)
        self.assertEqual(self.decode(first)["pid"], self.decode(second)["pid"])
        self.assertEqual(self.decode(first)["start"], "20000")
        self.assertEqual(self.decode(second)["start"], "30000")

    def test_executable_path_and_inode_changes_each_change_identity(self):
        first_path = self.home / "bin" / "codex"
        process = self.write_process(pid=200, executable=first_path)
        _, original = self.resolve({"codex"})

        second_path = self.home / "other" / "codex"
        second_path.parent.mkdir(parents=True)
        second_path.write_text("#!/bin/sh\n", encoding="utf-8")
        second_path.chmod(0o700)
        (process / "exe").unlink()
        (process / "exe").symlink_to(second_path)
        _, moved = self.resolve({"codex"})

        moved_payload = self.decode(moved)
        original_payload = self.decode(original)
        self.assertNotEqual(original_payload["exe_path"], moved_payload["exe_path"])
        self.assertNotEqual(original, moved)

        previous_inode = moved_payload["exe"]
        replacement = second_path.with_suffix(".replacement")
        replacement.write_text("#!/bin/sh\n", encoding="utf-8")
        replacement.chmod(0o700)
        replacement.replace(second_path)
        _, replaced = self.resolve({"codex"})
        replaced_payload = self.decode(replaced)

        self.assertEqual(moved_payload["exe_path"], replaced_payload["exe_path"])
        self.assertNotEqual(previous_inode, replaced_payload["exe"])
        self.assertNotEqual(moved, replaced)

    def test_equal_candidate_processes_are_rejected_as_ambiguous(self):
        for pid in (200, 201):
            self.write_process(
                pid=pid,
                executable=self.home / f"bin-{pid}" / "codex",
                start_time=20_000 + pid,
                process_group=700,
                foreground_group=700,
            )

        with self.assertRaises(self.runtime.NativeContinuationError) as raised:
            self.resolve({"codex"})

        self.assertEqual(raised.exception.code, "process_identity_ambiguous")

    def test_no_allowlisted_candidate_is_rejected_as_missing(self):
        self.write_process(
            pid=200,
            executable=self.home / "bin" / "bash",
            argv=(str(self.home / "bin" / "bash"), "-l"),
        )

        with self.assertRaises(self.runtime.NativeContinuationError) as raised:
            self.resolve({"codex"})

        self.assertEqual(raised.exception.code, "process_identity_missing")

    def test_zellij_metadata_adapter_mismatch_is_rejected(self):
        codex = self.home / "bin" / "codex"
        self.write_process(
            pid=199,
            executable=self.home / "bin" / "bash",
        )
        self.write_process(
            pid=200,
            executable=codex,
            parent_pid=199,
        )
        panes = [{
            "id": 51,
            "is_plugin": False,
            "exited": False,
            "terminal_command": "/opt/claude/bin/claude --resume session-1",
        }]
        completed = types.SimpleNamespace(stdout=json.dumps(panes))
        original_resolver = self.runtime._zellij_agent_process

        def resolve(
            session,
            pane,
            allowed,
            metadata_agent="",
            trusted_paths=None,
        ):
            return original_resolver(
                session,
                pane,
                allowed,
                self.proc_root,
                metadata_agent=metadata_agent,
                trusted_paths=trusted_paths,
            )

        with mock.patch.object(
            self.runtime, "_resolve_executable", return_value="/usr/bin/zellij"
        ), mock.patch.object(
            self.runtime.subprocess, "run", return_value=completed
        ), mock.patch.object(
            self.runtime,
            "_trusted_agent_paths",
            return_value={"codex": {str(codex.resolve())}},
        ), mock.patch.object(
            self.runtime, "_zellij_agent_process", side_effect=resolve
        ):
            with self.assertRaises(
                self.runtime.NativeContinuationError
            ) as raised:
                self.runtime.zellij_pane_identity(
                    "didactic-jellyfish",
                    "51",
                    "/tmp/project",
                    self.runtime.Config(
                        zellij_agent_commands=("codex", "claude")
                    ),
                )

        self.assertEqual(raised.exception.code, "adapter_pane_mismatch")

    def test_process_disappearing_during_proc_read_is_not_bound(self):
        process = self.write_process(
            pid=200,
            executable=self.home / "bin" / "codex",
        )
        cmdline = process / "cmdline"
        executable_link = process / "exe"
        original_read_bytes = pathlib.Path.read_bytes

        def racing_read_bytes(path):
            data = original_read_bytes(path)
            if path == cmdline:
                executable_link.unlink()
            return data

        with mock.patch.object(
            pathlib.Path,
            "read_bytes",
            new=racing_read_bytes,
        ):
            with self.assertRaises(
                self.runtime.NativeContinuationError
            ) as raised:
                self.resolve({"codex"})

        self.assertEqual(raised.exception.code, "process_identity_missing")

    def test_partial_proc_entries_from_read_races_are_ignored(self):
        process = self.proc_root / "200"
        process.mkdir()
        (process / "environ").write_bytes(
            b"ZELLIJ_SESSION_NAME=didactic-jellyfish\0ZELLIJ_PANE_ID=51\0"
        )
        # The process exits before stat, cmdline, and exe can be read.

        with self.assertRaises(self.runtime.NativeContinuationError) as raised:
            self.resolve({"codex"})

        self.assertEqual(raised.exception.code, "process_identity_missing")


if __name__ == "__main__":
    unittest.main()
