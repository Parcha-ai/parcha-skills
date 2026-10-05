from __future__ import annotations

import hashlib
import json
import os
import pathlib
import shutil
import socket
import subprocess
import tempfile
import threading
import time
import unittest
from collections.abc import Callable
from typing import Any


PACKAGE_ROOT = pathlib.Path(__file__).resolve().parents[1]
CLI = PACKAGE_ROOT / "bin" / "tether.js"
NOTIFIER = PACKAGE_ROOT / "skills" / "tether" / "scripts" / "tether_notify.py"
MAX_REQUEST_FRAME_BYTES = 1_048_576
MAX_RESPONSE_FRAME_BYTES = 8 * 1_048_576


class FakeBroker:
    def __init__(
        self,
        root: pathlib.Path,
        responder: Callable[[dict[str, Any]], bytes | None],
    ) -> None:
        self.path = root / "broker.sock"
        self.responder = responder
        self.requests: list[dict[str, Any]] = []
        self.ready = threading.Event()
        self.finished = threading.Event()
        self.error: BaseException | None = None
        self.thread = threading.Thread(target=self._serve, daemon=True)

    def __enter__(self) -> "FakeBroker":
        self.thread.start()
        if not self.ready.wait(2):
            self.fail_if_needed()
            raise RuntimeError("fake broker did not start")
        return self

    def __exit__(self, *args: object) -> None:
        self.finished.wait(2)
        self.thread.join(timeout=2)
        self.path.unlink(missing_ok=True)
        self.fail_if_needed()

    def fail_if_needed(self) -> None:
        if self.error is not None:
            raise self.error

    def _serve(self) -> None:
        server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            server.bind(str(self.path))
            os.chmod(self.path, 0o600)
            server.listen(1)
            server.settimeout(3)
            self.ready.set()
            connection, _ = server.accept()
            with connection:
                connection.settimeout(2)
                frame = b""
                while not frame.endswith(b"\n"):
                    chunk = connection.recv(65_536)
                    if not chunk:
                        break
                    frame += chunk
                    if len(frame) > MAX_REQUEST_FRAME_BYTES:
                        raise AssertionError("CLI request exceeded protocol limit")
                if not frame.endswith(b"\n"):
                    raise AssertionError("CLI request was not newline framed")
                request = json.loads(frame[:-1])
                if not isinstance(request, dict):
                    raise AssertionError("CLI request was not a JSON object")
                self.requests.append(request)
                response = self.responder(request)
                if response is not None:
                    try:
                        connection.sendall(response)
                    except BrokenPipeError:
                        # A deadline or size guard may intentionally close first.
                        pass
        except BaseException as exc:
            self.error = exc
            self.ready.set()
        finally:
            server.close()
            self.finished.set()


class TetherCliTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(prefix="tether-cli-")
        self.root = pathlib.Path(self.temporary.name)
        self.home = self.root / "home"
        self.home.mkdir()
        self.base_env = {
            **os.environ,
            "HOME": str(self.home),
            "XDG_DATA_HOME": str(self.root / "data"),
            "XDG_STATE_HOME": str(self.home / ".local" / "state"),
            "XDG_CONFIG_HOME": str(self.root / "config"),
            "HERMES_HOME": str(self.root / "hermes"),
            "TETHER_HERDR": "off",
            "PYTHONDONTWRITEBYTECODE": "1",
        }
        for key in (
            "TETHER_BROKER_SOCKET",
            "TETHER_SOCKET_PATH",
            "TETHER_SOCKET",
            "TETHER_BROKER_TIMEOUT_MS",
            "CODEX_THREAD_ID",
            "CLAUDE_CODE_SESSION_ID",
            "ZELLIJ_SESSION_NAME",
            "ZELLIJ_PANE_ID",
            "HERDR_ENV",
            "HERDR_SESSION",
            "HERDR_SOCKET_PATH",
            "HERDR_PANE_ID",
            "HERDR_TAB_ID",
            "HERDR_WORKSPACE_ID",
        ):
            self.base_env.pop(key, None)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    @staticmethod
    def response(payload: dict[str, Any]) -> bytes:
        return json.dumps(payload, separators=(",", ":")).encode() + b"\n"

    def run_cli(
        self,
        *arguments: str,
        socket_path: pathlib.Path | None = None,
        extra_env: dict[str, str] | None = None,
        input_text: str | None = None,
        pass_fds: tuple[int, ...] = (),
        timeout: float = 4,
        cli_path: pathlib.Path = CLI,
    ) -> subprocess.CompletedProcess[str]:
        env = dict(self.base_env)
        if socket_path is not None:
            env["TETHER_BROKER_SOCKET"] = str(socket_path)
        if extra_env:
            env.update(extra_env)
        return subprocess.run(
            ["node", str(cli_path), *arguments],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            input=input_text,
            pass_fds=pass_fds,
            env=env,
            timeout=timeout,
            check=False,
        )

    def write_managed_install(
        self,
        *,
        harness: str = "codex",
        legacy: tuple[str, ...] = (),
        omit: tuple[pathlib.Path, ...] = (),
        extra: tuple[pathlib.Path, ...] = (),
    ) -> tuple[pathlib.Path, dict[pathlib.Path, int]]:
        runtime = self.root / "data" / "tether"
        plugin = self.root / "hermes" / "plugins" / "tether"
        local_bin = self.home / ".local" / "bin"
        codex = self.home / ".codex"
        claude = self.home / ".claude"
        candidates: dict[pathlib.Path, int] = {
            runtime / "tether_notify.py": 0o700,
            runtime / "install.sh": 0o700,
            runtime / "package.json": 0o600,
            plugin / "__init__.py": 0o600,
            plugin / "active.py": 0o600,
            plugin / "admission.py": 0o600,
            plugin / "broker.py": 0o600,
            plugin / "journal.py": 0o600,
            plugin / "slack_egress.py": 0o600,
            plugin / "store.py": 0o600,
            plugin / "session_driver.py": 0o600,
            plugin / "notices.py": 0o600,
            plugin / "herdr.py": 0o600,
            plugin / "demo.py": 0o600,
            plugin / "team.py": 0o600,
            plugin / "team.md": 0o600,
            plugin / "plugin.yaml": 0o644,
            local_bin / "tether": 0o700,
        }

        def add_skill(root: pathlib.Path, include_legacy: bool) -> None:
            skill = root / "skills" / "tether"
            candidates.update({
                skill / "SKILL.md": 0o644,
                skill / "agents" / "openai.yaml": 0o644,
                skill / "references" / "setup.md": 0o644,
                skill / "references" / "contract.md": 0o644,
                skill / "scripts" / "tether_notify.py": 0o700,
            })
            if include_legacy:
                compatibility = root / "skills" / "hermes-slack-bridge"
                candidates.update({
                    compatibility / "SKILL.md": 0o644,
                    compatibility / "scripts" / "hermes_notify.py": 0o700,
                })

        if harness in ("codex", "both"):
            add_skill(codex, "codex" in legacy)
        if harness in ("claude-code", "both"):
            add_skill(claude, "claude-code" in legacy)
        for candidate in extra:
            candidates[candidate] = 0o600
        for candidate, mode in candidates.items():
            candidate.parent.mkdir(parents=True, exist_ok=True)
            candidate.write_text(f"# managed {candidate.name}\n", encoding="utf-8")
            candidate.chmod(mode)

        state = self.home / ".local" / "state" / "tether-installer"
        state.mkdir(parents=True)
        manifest = state / "current.tsv"
        metadata = (
            "# tether-manifest-v2\n"
            f"@harness\t{harness}\n"
            f"@runtime_home\t{runtime}\n"
            f"@plugin_home\t{plugin}\n"
            f"@local_bin\t{local_bin}\n"
            f"@codex_root\t{codex}\n"
            f"@claude_root\t{claude}\n"
            f"@legacy\t{','.join(legacy) or 'none'}\n"
        )
        omitted = set(omit)
        rows = "".join(
            f"{candidate}\t{mode:o}\t"
            f"{hashlib.sha256(candidate.read_bytes()).hexdigest()}\n"
            for candidate, mode in candidates.items()
            if candidate not in omitted
        )
        manifest.write_text(metadata + rows, encoding="utf-8")
        manifest.chmod(0o600)
        return manifest, candidates

    def setup_child_fixtures(self) -> tuple[pathlib.Path, pathlib.Path]:
        """Exercise the real CLI with counted offline installer/notifier children."""
        package = self.root / "source"
        copied_cli = package / "bin" / "tether.js"
        copied_cli.parent.mkdir(parents=True)
        copied_cli.write_bytes(CLI.read_bytes())
        (package / "package.json").write_text('{"version":"0.4.0"}\n')
        skill = package / "skills" / "tether" / "SKILL.md"
        skill.parent.mkdir(parents=True)
        skill.write_text("Fixture skill, no gateway.\n")
        log = self.root / "child-calls.jsonl"
        self.base_env["TETHER_TEST_CHILD_LOG"] = str(log)

        def child(label: str) -> str:
            return (
                "#!/usr/bin/env python3\nimport json, os, sys\n"
                "with open(os.environ['TETHER_TEST_CHILD_LOG'], 'a') as stream:\n"
                f"    stream.write(json.dumps([{label!r}, *sys.argv[1:]]) + '\\n')\n"
                "sys.exit(int(os.environ.get('TETHER_TEST_INSTALL_EXIT', '0')) "
                f"if {label!r} == 'installer' else 0)\n"
            )

        installer = package / "install.sh"
        installer.write_text(child("installer"))
        installer.chmod(0o700)
        notifier = self.root / "data" / "tether" / "tether_notify.py"
        notifier.parent.mkdir(parents=True)
        notifier.write_text(child("notifier"))
        return copied_cli, log

    def tree_snapshot(self) -> dict[str, str]:
        return {
            str(path.relative_to(self.root)): (
                hashlib.sha256(path.read_bytes()).hexdigest() if path.is_file() else "directory"
            )
            for path in self.root.rglob("*")
        }

    def test_setup_help_never_runs_children_or_changes_files(self) -> None:
        cli, log = self.setup_child_fixtures()
        before = self.tree_snapshot()
        for flag in ("--help", "-h"):
            with self.subTest(flag=flag):
                result = self.run_cli("setup", flag, cli_path=cli)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn("--non-interactive", result.stdout)
                self.assertIn("--no-restart", result.stdout)
                self.assertIn("--team-id T012ABCDEF", result.stdout)
                self.assertIn("existing configured IDs", result.stdout)
                self.assertNotIn("--herdr", result.stdout)
                self.assertFalse(log.exists())
                self.assertEqual(self.tree_snapshot(), before)

    def test_invalid_setup_options_never_run_children_or_change_files(self) -> None:
        cli, log = self.setup_child_fixtures()
        before = self.tree_snapshot()
        invalid = (
            ("--herdr",), ("--harness=unsupported",), ("--harness",),
            ("--harness=codex", "--both"), ("--codex", "--claude-code"),
            ("--non-interactive", "--non-interactive"), ("--no-restart=false",),
            ("--harness=codex", "--harness=both"), ("unexpected-positional",),
            ("--team-id",), ("--team-id=",), ("--team-id", "T1"),
            ("--team-id", "t012ABCDEF"), ("--team-id", "U012ABCDEF"),
            ("--team-id", "T012 ABC"), ("--team-id", "T" + "A" * 32),
            ("--team-id=T012ABCDEF", "--team-id=T012ABCDEF"),
        )
        for arguments in invalid:
            with self.subTest(arguments=arguments):
                result = self.run_cli("setup", *arguments, cli_path=cli)
                self.assertEqual(result.returncode, 2, result.stderr)
                self.assertFalse(log.exists())
                self.assertEqual(self.tree_snapshot(), before)

    @unittest.skipIf(hasattr(os, "geteuid") and os.geteuid() == 0, "setup refuses root")
    def test_setup_validates_and_forwards_harness_and_notifier_flags(self) -> None:
        cli, log = self.setup_child_fixtures()
        for selection, harness in (
            (("--codex",), "codex"), (("--claude-code",), "claude-code"),
            (("--both",), "both"), (("--harness=auto",), "auto"),
            (("--harness", "both"), "both"),
        ):
            with self.subTest(selection=selection):
                log.unlink(missing_ok=True)
                result = self.run_cli(
                    "setup", *selection, "--non-interactive", "--no-restart", cli_path=cli,
                )
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(
                    [json.loads(line) for line in log.read_text().splitlines()],
                    [["installer", "install", f"--harness={harness}"],
                     ["notifier", "setup", "--non-interactive", "--no-restart"]],
                )

    @unittest.skipIf(hasattr(os, "geteuid") and os.geteuid() == 0, "setup refuses root")
    def test_setup_forwards_workspace_only_to_notifier(self) -> None:
        cli, log = self.setup_child_fixtures()
        for arguments, workspace in (
            (("--team-id", "T012ABCDEF"), "T012ABCDEF"),
            (("--team-id=T99",), "T99"),
        ):
            with self.subTest(arguments=arguments):
                log.unlink(missing_ok=True)
                result = self.run_cli("setup", "--both", *arguments, cli_path=cli)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(
                    [json.loads(line) for line in log.read_text().splitlines()],
                    [["installer", "install", "--harness=both"],
                     ["notifier", "setup", "--team-id", workspace]],
                )
    @unittest.skipIf(hasattr(os, "geteuid") and os.geteuid() == 0, "setup refuses root")
    def test_failed_setup_install_never_runs_notifier(self) -> None:
        cli, log = self.setup_child_fixtures()
        result = self.run_cli("setup", "--both", cli_path=cli,
                              extra_env={"TETHER_TEST_INSTALL_EXIT": "7"})
        self.assertEqual(result.returncode, 7)
        self.assertEqual([json.loads(line) for line in log.read_text().splitlines()],
                         [["installer", "install", "--harness=both"]])

    def test_help_advertises_demo_and_omits_unshipped_schema(self) -> None:
        result = self.run_cli("--help")
        self.assertEqual(result.returncode, 0)
        self.assertIn("tether demo [--json]", result.stdout)
        self.assertNotIn("tether schema", result.stdout)
        for flag in ("--help", "-h"):
            result = self.run_cli("demo", flag)
            self.assertEqual(result.returncode, 0)
            self.assertIn("offline", result.stdout)

    def test_demo_runs_without_an_installed_runtime(self) -> None:
        result = self.run_cli("demo", "--json", timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)
        payload = json.loads(result.stdout)
        self.assertEqual(payload["schema"], "tether-offline-demo/v1")
        self.assertTrue(payload["ok"])
        self.assertTrue(payload["simulated"])
        self.assertEqual(payload["actual_model_calls"], 0)
        self.assertEqual(payload["actual_slack_calls"], 0)
        self.assertEqual(payload["native_processes"], 0)
        human = self.run_cli("demo", timeout=10)
        self.assertEqual(human.returncode, 0, human.stderr)
        self.assertIn("Offline simulation", human.stdout)
        self.assertFalse((self.root / "hermes").exists())
        self.assertFalse((self.root / "data").exists())

    def test_standalone_installed_cli_runs_adjacent_plugin_demo(self) -> None:
        cli = self.home / ".local" / "bin" / "tether"
        cli.parent.mkdir(parents=True)
        cli.write_bytes(CLI.read_bytes())
        plugin = self.root / "hermes" / "plugins" / "tether"
        shutil.copytree(PACKAGE_ROOT / "runtime" / "plugin_next", plugin,
                        ignore=shutil.ignore_patterns("__pycache__"))
        before = self.tree_snapshot()
        result = self.run_cli("demo", "--json", cli_path=cli, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(json.loads(result.stdout)["ok"])
        self.assertEqual(self.tree_snapshot(), before)
        self.assertFalse((self.root / "data").exists())

    def test_demo_invalid_flags_do_not_start_python(self) -> None:
        result = self.run_cli("demo", "--socket=unexpected",
                              extra_env={"PYTHON_BIN": str(self.root / "nonexistent-python")})
        self.assertEqual(result.returncode, 2)
        self.assertIn("Unknown option", result.stderr)

    def test_demo_team_config_uses_custom_names_with_simulated_computers(self) -> None:
        manifest = self.root / "custom team.toml"
        manifest.write_text(
            'version = 1\nself = "ada"\n'
            '[[colleagues]]\nid = "lin"\nname = "Lin"\nrole = "Reviewer"\n'
            'slack_id = "U11111111"\ncomputer = "never-launch-this-computer"\n'
            '[[colleagues]]\nid = "ada"\nname = "Ada"\nrole = "Engineer"\n'
            'slack_id = "U22222222"\ncomputer = "also-never-launch-this-computer"\n',
        )
        before = self.tree_snapshot()
        result = self.run_cli("demo", "--json", "--team-config", str(manifest), timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)
        payload = json.loads(result.stdout)
        self.assertTrue(payload["ok"])
        self.assertTrue(payload["simulated"])
        self.assertEqual(payload["colleagues"], {"implementer": "Ada", "reviewer": "Lin"})
        self.assertEqual(payload["actual_model_calls"], 0)
        self.assertEqual(payload["actual_slack_calls"], 0)
        self.assertEqual(payload["native_processes"], 0)
        self.assertNotIn("U11111111", result.stdout)
        self.assertNotIn("U22222222", result.stdout)
        self.assertEqual(self.tree_snapshot(), before)

    def test_missing_installed_demo_returns_a_typed_error_without_installing(self) -> None:
        cli = self.home / ".local" / "bin" / "tether"
        cli.parent.mkdir(parents=True)
        cli.write_bytes(CLI.read_bytes())
        before = self.tree_snapshot()
        result = self.run_cli("demo", "--json", cli_path=cli)
        self.assertEqual(result.returncode, 2)
        self.assertEqual(json.loads(result.stderr)["code"], "demo_unavailable")
        self.assertEqual(self.tree_snapshot(), before)

    def test_malformed_response_is_a_nonzero_protocol_error(self) -> None:
        with FakeBroker(self.root, lambda _request: b"{not-json}\n") as broker:
            result = self.run_cli("status", socket_path=broker.path)
        self.assertEqual(result.returncode, 3)
        self.assertIn("malformed JSON", result.stderr)
        self.assertEqual(result.stdout, "")

    def test_multiple_response_frames_are_rejected(self) -> None:
        response = self.response({"ok": True}) + self.response({"ok": True})
        with FakeBroker(self.root, lambda _request: response) as broker:
            result = self.run_cli("status", socket_path=broker.path)
        self.assertEqual(result.returncode, 3)
        self.assertIn("invalid JSON framing", result.stderr)

    def test_timeout_is_bounded_and_nonzero(self) -> None:
        def delayed(_request: dict[str, Any]) -> bytes:
            time.sleep(0.3)
            return self.response({"ok": True, "implementation": "tether"})

        started = time.monotonic()
        with FakeBroker(self.root, delayed) as broker:
            result = self.run_cli(
                "status",
                "--timeout-ms",
                "75",
                socket_path=broker.path,
            )
        elapsed = time.monotonic() - started
        self.assertEqual(result.returncode, 3)
        self.assertIn("broker_timeout", result.stderr)
        self.assertLess(elapsed, 2)

    def test_peer_close_without_response_is_nonzero(self) -> None:
        with FakeBroker(self.root, lambda _request: None) as broker:
            result = self.run_cli("status", socket_path=broker.path)
        self.assertEqual(result.returncode, 3)
        self.assertIn("closed without a response", result.stderr)

    def test_oversized_response_is_rejected(self) -> None:
        oversized = (
            b'{"ok":true,"padding":"'
            + b"x" * MAX_RESPONSE_FRAME_BYTES
            + b'"}\n'
        )
        with FakeBroker(self.root, lambda _request: oversized) as broker:
            result = self.run_cli("status", socket_path=broker.path)
        self.assertEqual(result.returncode, 3)
        self.assertIn("exceeds the 8 MiB", result.stderr)

    def test_large_valid_history_response_fits_the_protocol(self) -> None:
        messages = [
            {
                "ts": f"{index}.000",
                "thread_ts": "1.000",
                "text": "x" * 35_000,
                "user": "U12345678",
            }
            for index in range(100)
        ]
        response = self.response({"ok": True, "messages": messages})
        self.assertGreater(len(response), MAX_REQUEST_FRAME_BYTES)
        self.assertLess(len(response), MAX_RESPONSE_FRAME_BYTES)
        with FakeBroker(self.root, lambda _request: response) as broker:
            result = self.run_cli(
                "thread",
                "--channel",
                "C12345678",
                "--thread-ts",
                "1.000",
                "--limit",
                "100",
                socket_path=broker.path,
            )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(len(json.loads(result.stdout)), 100)

    def test_error_contract_is_rendered_without_secret_values(self) -> None:
        secret = "xox" + "b-test-secret-value-123456"

        def rejection(_request: dict[str, Any]) -> bytes:
            return self.response(
                {
                    "ok": False,
                    "code": "workspace_mismatch",
                    "message": f"wrong workspace; token={secret}",
                    "status": "rejected",
                    "retryable": False,
                    "next_action": f"remove Bearer {secret} and use the installed workspace",
                }
            )

        with FakeBroker(self.root, rejection) as broker:
            result = self.run_cli(
                "identity",
                "--json",
                socket_path=broker.path,
                extra_env={"SLACK_BOT_TOKEN": secret},
            )
        self.assertEqual(result.returncode, 4)
        payload = json.loads(result.stderr)
        self.assertEqual(payload["code"], "workspace_mismatch")
        self.assertEqual(payload["status"], "rejected")
        self.assertFalse(payload["retryable"])
        self.assertNotIn(secret, result.stderr)
        self.assertNotIn(secret, result.stdout)
        self.assertIn("[REDACTED]", result.stderr)

    @unittest.skipIf(os.geteuid() == 0, "mutating CLI commands intentionally refuse root")
    def test_reply_reads_message_from_stdin(self) -> None:
        message = "private reply from stdin"
        with FakeBroker(
            self.root,
            lambda _request: self.response(
                {"ok": True, "thread_ts": "123.456"}
            ),
        ) as broker:
            result = self.run_cli(
                "reply",
                "--bridge-id",
                "brg_example",
                "--reply-key",
                "reply-1",
                "--text-stdin",
                socket_path=broker.path,
                input_text=message,
            )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(broker.requests[0]["text"], message)
        self.assertNotIn("DEPRECATED", result.stderr)

    @unittest.skipIf(os.geteuid() == 0, "mutating CLI commands intentionally refuse root")
    def test_spawn_reads_task_from_stdin_and_needs_a_channel_with_a_thread(self) -> None:
        # 2026-09-16: the agent hit argv punctuation limits on --task and then spawned with
        # --thread-ts but no --channel; the broker fell back to #agent-hub and the session's
        # reports became stray roots. The task comes from stdin, and a thread needs its channel.
        task = "Debug why experts created via the MCP path are missing their form manifest: it's `expert_apply` (see #8622)."
        with FakeBroker(
            self.root,
            lambda _request: self.response({"ok": True, "harness": "claude", "session_id": "s-1", "cwd": "/w",
                                            "thread_ts": "100.1", "channel_id": "C07QDVCPWS1", "status": "spawned"}),
        ) as broker:
            result = self.run_cli(
                "spawn", "--task-stdin", "--channel", "C07QDVCPWS1", "--thread-ts", "100.1",
                "--herdr-workspace", "grep.ai", "--tab", "MCP",
                socket_path=broker.path, input_text=task,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(broker.requests[0]["task"], task)
            self.assertEqual((broker.requests[0]["channel_id"], broker.requests[0]["thread_ts"]), ("C07QDVCPWS1", "100.1"))
            self.assertEqual((broker.requests[0]["herdr_workspace"], broker.requests[0]["tab"]), ("grep.ai", "MCP"))
            self.assertNotIn("herdr", broker.requests[0], "herdr placement is automatic unless --no-herdr")
        with FakeBroker(self.root, lambda _request: self.response({"ok": True, "status": "spawned"})) as broker:
            plain = self.run_cli("spawn", "--task", "t", "--no-herdr", socket_path=broker.path)
            self.assertEqual(plain.returncode, 0, plain.stderr)
            self.assertIs(broker.requests[0]["herdr"], False)
        with FakeBroker(self.root, lambda _request: self.response({"ok": True, "status": "attached"})) as broker:
            attached = self.run_cli("attach", "--herdr", "hvrt", "--channel", "C1", "--thread-ts", "100.1",
                                    "--idempotency-key", "a-1", socket_path=broker.path)
            self.assertEqual(attached.returncode, 0, attached.stderr)
            self.assertEqual(broker.requests[0]["herdr_agent"], "hvrt")
            self.assertNotIn("source_kind", broker.requests[0], "attach by Herdr name needs no calling session")
            refused = self.run_cli("spawn", "--task", "t", "--thread-ts", "100.1", socket_path=broker.path)
            self.assertEqual(refused.returncode, 2, refused.stderr)
            self.assertIn("channel_required", refused.stderr + refused.stdout)
            self.assertEqual(len(broker.requests), 1, "the refusal never reached the broker")

    @unittest.skipIf(os.geteuid() == 0, "mutating CLI commands intentionally refuse root")
    def test_post_reads_message_from_private_fd(self) -> None:
        message = "private reply from inherited fd"
        read_fd, write_fd = os.pipe()
        try:
            os.write(write_fd, message.encode())
            os.close(write_fd)
            write_fd = -1
            with FakeBroker(
                self.root,
                lambda _request: self.response(
                    {"ok": True, "thread_ts": "123.456"}
                ),
            ) as broker:
                result = self.run_cli(
                    "post",
                    "--channel",
                    "C12345678",
                    "--thread-ts",
                    "123.456",
                    "--idempotency-key",
                    "post-1",
                    "--text-fd",
                    str(read_fd),
                    socket_path=broker.path,
                    pass_fds=(read_fd,),
                )
        finally:
            if write_fd >= 0:
                os.close(write_fd)
            os.close(read_fd)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(broker.requests[0]["text"], message)

    @unittest.skipIf(os.geteuid() == 0, "mutating CLI commands intentionally refuse root")
    def test_message_input_sources_are_mutually_exclusive(self) -> None:
        result = self.run_cli(
            "reply",
            "--bridge-id",
            "brg_example",
            "--reply-key",
            "reply-1",
            "--text",
            "argv text",
            "--text-stdin",
            input_text="stdin text",
        )
        self.assertEqual(result.returncode, 2)
        self.assertIn("exactly one", result.stderr)



    def test_python_notifier_reads_message_from_stdin(self) -> None:
        import socket
        import threading

        capture = self.root / "notifier-request.json"
        sock = self.root / "broker.sock"
        server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        server.bind(str(sock))
        server.listen(1)

        def serve() -> None:
            connection, _ = server.accept()
            with connection:
                data = b""
                while b"\n" not in data:
                    chunk = connection.recv(65536)
                    if not chunk:
                        break
                    data += chunk
                capture.write_text(data.split(b"\n", 1)[0].decode("utf-8"))
                connection.sendall(b'{"ok": true, "thread_ts": "123.456"}\n')

        thread = threading.Thread(target=serve, daemon=True)
        thread.start()
        message = "notifier stdin message"
        result = subprocess.run(
            ["python3", str(NOTIFIER), "reply", "--bridge-id", "brg_example",
             "--reply-key", "reply-1", "--text-stdin"],
            text=True, input=message, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            env={**self.base_env, "TETHER_BROKER_SOCKET": str(sock)},
            timeout=8, check=False,
        )
        thread.join(timeout=5)
        server.close()
        self.assertEqual(result.returncode, 0, result.stderr)
        captured = json.loads(capture.read_text())
        self.assertEqual(captured["op"], "reply")
        self.assertEqual(captured["text"], message)
        self.assertEqual(result.stdout.strip(), "123.456")

    def test_close_and_unbind_send_the_close_contract(self) -> None:
        for command in ("close", "unbind"):
            with self.subTest(command=command):
                with FakeBroker(
                    self.root,
                    lambda _request: self.response(
                        {"ok": True, "bridge_id": "brg_example", "status": "closed"}
                    ),
                ) as broker:
                    result = self.run_cli(
                        command,
                        "--bridge-id",
                        "brg_example",
                        "--team",
                        "T12345678",
                        "--expected-generation",
                        "7",
                        socket_path=broker.path,
                    )
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(
                    broker.requests,
                    [
                        {
                            "op": "close",
                            "bridge_id": "brg_example",
                            "team_id": "T12345678",
                            "channel_id": "",
                            "thread_ts": "",
                            "expected_generation": 7,
                        }
                    ],
                )

    def test_unresolved_lists_operator_recovery_items(self) -> None:
        with FakeBroker(
            self.root,
            lambda _request: self.response(
                {"ok": True, "operations": [{"kind": "ingress", "id": "evt-1"}]}
            ),
        ) as broker:
            result = self.run_cli(
                "unresolved",
                "--team",
                "T12345678",
                "--json",
                socket_path=broker.path,
            )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            broker.requests,
            [{"op": "unresolved", "team_id": "T12345678"}],
        )
        self.assertEqual(
            json.loads(result.stdout)["operations"][0]["id"],
            "evt-1",
        )

    def test_resolve_is_disabled_before_operator_isolation(self) -> None:
        result = self.run_cli(
            "resolve",
            "--kind",
            "attempt",
            "--id",
            "attempt-1",
            "--action",
            "abandon",
            "--team",
            "T12345678",
        )
        self.assertEqual(result.returncode, 2)
        self.assertIn("operator_boundary_unavailable", result.stderr)

    @unittest.skipIf(os.geteuid() == 0, "mutating CLI commands intentionally refuse root")
    def test_rebind_sends_explicit_headless_source(self) -> None:
        with FakeBroker(
            self.root,
            lambda _request: self.response(
                {
                    "ok": True,
                    "bridge_id": "brg_example",
                    "thread_ts": "123.456",
                    "source_kind": "headless_run",
                }
            ),
        ) as broker:
            result = self.run_cli(
                "rebind",
                "--team",
                "T12345678",
                "--channel",
                "C12345678",
                "--thread-ts",
                "123.456",
                "--run-id",
                "run-example",
                "--cwd",
                str(self.root),
                socket_path=broker.path,
            )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, "123.456\n")
        self.assertEqual(
            broker.requests,
            [
                {
                    "op": "rebind",
                    "team_id": "T12345678",
                    "channel_id": "C12345678",
                    "thread_ts": "123.456",
                    "source_kind": "headless_run",
                    "source": {
                        "run_id": "run-example",
                        "queue_id": "run-example",
                        "cwd": str(self.root),
                    },
                }
            ],
        )

    @unittest.skipIf(os.geteuid() == 0, "mutating CLI commands intentionally refuse root")
    def test_non_native_source_cannot_replace_ambient_native_context(self) -> None:
        for source_flag, source_id in (
            ("--run-id", "fallback-run"),
            ("--hermes-session-id", "fallback-hermes"),
        ):
            with self.subTest(source_flag=source_flag):
                result = self.run_cli(
                    "notify",
                    "--text",
                    "done",
                    source_flag,
                    source_id,
                    "--idempotency-key",
                    f"test-{source_id}",
                    extra_env={"CODEX_THREAD_ID": "native-session"},
                )
                self.assertEqual(result.returncode, 2)
                self.assertIn("cannot replace an active", result.stderr)
                self.assertIn("native_binding_required", result.stderr)

    def test_explicit_socket_precedes_environment_socket(self) -> None:
        unused = self.root / "unused.sock"
        with FakeBroker(
            self.root,
            lambda _request: self.response(
                {
                    "ok": True,
                    "implementation": "tether",
                    "protocol_version": 6,
                    "allowed_user_count": 1,
                    "owner_configured": True,
                    "slack_transport_connected": True,
                    "peer_uid_enforced": True,
                    "root_refused": True,
                }
            ),
        ) as broker:
            result = self.run_cli(
                "status",
                "--json",
                "--socket",
                str(broker.path),
                extra_env={"TETHER_BROKER_SOCKET": str(unused)},
            )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout)["implementation"], "tether")

    def test_polling_does_not_mask_missing_socket_mode_ingress(self) -> None:
        with FakeBroker(
            self.root,
            lambda _request: self.response(
                {
                    "ok": True,
                    "implementation": "tether",
                    "protocol_version": 6,
                    "allowed_user_count": 1,
                    "owner_configured": True,
                    "slack_transport_connected": None,
                    "reply_poll_healthy": True,
                    "peer_uid_enforced": True,
                    "root_refused": True,
                }
            ),
        ) as broker:
            result = self.run_cli("status", socket_path=broker.path)
        self.assertEqual(result.returncode, 1)
        self.assertIn(
            "FAIL Slack Socket Mode ingress has not connected yet",
            result.stdout,
        )
        self.assertIn("ok best-effort Slack polling worker active", result.stdout)

    def test_status_rejects_unknown_future_broker_protocol(self) -> None:
        with FakeBroker(
            self.root,
            lambda _request: self.response(
                {
                    "ok": True,
                    "implementation": "tether",
                    "protocol_version": 7,
                    "allowed_user_count": 1,
                    "owner_configured": True,
                    "slack_transport_connected": True,
                    "peer_uid_enforced": True,
                    "root_refused": True,
                }
            ),
        ) as broker:
            result = self.run_cli("status", socket_path=broker.path)

        self.assertEqual(result.returncode, 1)
        self.assertIn("FAIL unsupported broker protocol=7", result.stdout)

    def test_doctor_json_reports_local_and_broker_checks(self) -> None:
        self.write_managed_install(harness="codex")
        with FakeBroker(
            self.root,
            lambda _request: self.response(
                {
                    "ok": True,
                    "implementation": "tether",
                    "protocol_version": 6,
                    "allowed_user_count": 2,
                    "owner_configured": True,
                    "slack_transport_connected": True,
                    "peer_uid_enforced": True,
                    "root_refused": True,
                }
            ),
        ) as broker:
            result = self.run_cli(
                "doctor",
                "--json",
                socket_path=broker.path,
            )
        self.assertEqual(result.returncode, 0, result.stderr)
        payload = json.loads(result.stdout)
        self.assertTrue(payload["ok"])
        self.assertIn("ok broker socket is private", payload["checks"])
        self.assertIn(
            "ok managed install integrity verified (23 files; harness=codex)",
            payload["checks"],
        )
        self.assertEqual(payload["status"]["protocol_version"], 6)

    def test_doctor_fails_on_unresolved_delivery_blocker(self) -> None:
        self.write_managed_install(harness="codex")
        with FakeBroker(
            self.root,
            lambda _request: self.response(
                {
                    "ok": True,
                    "implementation": "tether",
                    "protocol_version": 6,
                    "allowed_user_count": 1,
                    "owner_configured": True,
                    "slack_transport_connected": True,
                    "reply_poll_healthy": True,
                    "peer_uid_enforced": True,
                    "root_refused": True,
                    "queued_delivery_count": 10,
                    "uncertain_delivery_count": 1,
                    "blocked_bridge_count": 1,
                }
            ),
        ) as broker:
            result = self.run_cli("doctor", socket_path=broker.path)

        self.assertEqual(result.returncode, 1)
        self.assertIn(
            "FAIL durable delivery blocked: unresolved=1 "
            "blocked_threads=1; run tether unresolved",
            result.stdout,
        )
        self.assertIn("WARN queued Slack follow-ups=10", result.stdout)

    def test_doctor_fails_when_a_managed_file_drifted(self) -> None:
        self.write_managed_install(harness="codex")
        runtime = self.root / "data" / "tether" / "tether_notify.py"
        runtime.write_text("# drifted\n", encoding="utf-8")

        with FakeBroker(
            self.root,
            lambda _request: self.response(
                {
                    "ok": True,
                    "implementation": "tether",
                    "protocol_version": 6,
                    "allowed_user_count": 1,
                    "owner_configured": True,
                    "slack_transport_connected": True,
                    "peer_uid_enforced": True,
                    "root_refused": True,
                }
            ),
        ) as broker:
            result = self.run_cli("doctor", socket_path=broker.path)

        self.assertEqual(result.returncode, 1)
        self.assertIn(
            "FAIL managed install drift detected (1 file; harness=codex)",
            result.stdout,
        )

    def test_doctor_accepts_more_restrictive_managed_modes(self) -> None:
        _, candidates = self.write_managed_install(harness="codex")
        plugin_manifest = self.root / "hermes" / "plugins" / "tether" / "plugin.yaml"
        skill = self.home / ".codex" / "skills" / "tether" / "SKILL.md"
        self.assertEqual(candidates[plugin_manifest], 0o644)
        self.assertEqual(candidates[skill], 0o644)
        plugin_manifest.chmod(0o600)
        skill.chmod(0o400)

        with FakeBroker(
            self.root,
            lambda _request: self.response(
                {
                    "ok": True,
                    "implementation": "tether",
                    "protocol_version": 6,
                    "allowed_user_count": 1,
                    "owner_configured": True,
                    "slack_transport_connected": True,
                    "peer_uid_enforced": True,
                    "root_refused": True,
                }
            ),
        ) as broker:
            result = self.run_cli("doctor", socket_path=broker.path)

        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertIn("ok managed install integrity verified", result.stdout)

    def test_doctor_rejects_removed_owner_execute_permission(self) -> None:
        self.write_managed_install(harness="codex")
        notifier = self.root / "data" / "tether" / "tether_notify.py"
        notifier.chmod(0o600)

        with FakeBroker(
            self.root,
            lambda _request: self.response(
                {
                    "ok": True,
                    "implementation": "tether",
                    "protocol_version": 6,
                    "allowed_user_count": 1,
                    "owner_configured": True,
                    "slack_transport_connected": True,
                    "peer_uid_enforced": True,
                    "root_refused": True,
                }
            ),
        ) as broker:
            result = self.run_cli("doctor", socket_path=broker.path)

        self.assertEqual(result.returncode, 1)
        self.assertIn("FAIL managed install drift detected", result.stdout)

    def test_doctor_validates_both_harnesses_and_declared_legacy_shim(self) -> None:
        self.write_managed_install(harness="both", legacy=("codex",))
        with FakeBroker(
            self.root,
            lambda _request: self.response(
                {
                    "ok": True,
                    "implementation": "tether",
                    "protocol_version": 6,
                    "allowed_user_count": 1,
                    "owner_configured": True,
                    "slack_transport_connected": True,
                    "peer_uid_enforced": True,
                    "root_refused": True,
                }
            ),
        ) as broker:
            result = self.run_cli("doctor", socket_path=broker.path)
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertIn(
            "ok managed install integrity verified (30 files; harness=both)",
            result.stdout,
        )

    def test_doctor_rejects_missing_and_unexpected_manifest_records(self) -> None:
        expected = self.home / ".codex" / "skills" / "tether" / "SKILL.md"
        unexpected = self.root / "data" / "tether" / "unexpected.py"
        self.write_managed_install(
            harness="codex",
            omit=(expected,),
            extra=(unexpected,),
        )
        with FakeBroker(
            self.root,
            lambda _request: self.response(
                {
                    "ok": True,
                    "implementation": "tether",
                    "protocol_version": 6,
                    "allowed_user_count": 1,
                    "owner_configured": True,
                    "slack_transport_connected": True,
                    "peer_uid_enforced": True,
                    "root_refused": True,
                }
            ),
        ) as broker:
            result = self.run_cli("doctor", socket_path=broker.path)
        self.assertEqual(result.returncode, 1)
        self.assertIn(
            "FAIL managed target set mismatch (1 missing, 1 unexpected; harness=codex)",
            result.stdout,
        )

    def test_doctor_rejects_legacy_manifest_without_harness_metadata(self) -> None:
        runtime = self.root / "data" / "tether"
        runtime.mkdir(parents=True)
        candidates = (
            runtime / "tether_team.py",
            runtime / "tether_notify.py",
            runtime / "install.sh",
        )
        for candidate in candidates:
            candidate.write_text(f"# {candidate.name}\n", encoding="utf-8")
        state = self.home / ".local" / "state" / "tether-installer"
        state.mkdir(parents=True)
        manifest = state / "current.tsv"
        manifest.write_text(
            "".join(
                f"{candidate}\t{candidate.stat().st_mode & 0o777:o}\t"
                f"{hashlib.sha256(candidate.read_bytes()).hexdigest()}\n"
                for candidate in candidates
            ),
            encoding="utf-8",
        )
        manifest.chmod(0o600)
        with FakeBroker(
            self.root,
            lambda _request: self.response(
                {
                    "ok": True,
                    "implementation": "tether",
                    "protocol_version": 6,
                    "allowed_user_count": 1,
                    "owner_configured": True,
                    "slack_transport_connected": True,
                    "peer_uid_enforced": True,
                    "root_refused": True,
                }
            ),
        ) as broker:
            result = self.run_cli("doctor", socket_path=broker.path)
        self.assertEqual(result.returncode, 1)
        self.assertIn(
            "FAIL installer manifest metadata missing; upgrade Tether to regenerate it",
            result.stdout,
        )

    def test_success_payload_redacts_sensitive_keys(self) -> None:
        secret = "not-safe-for-output"
        with FakeBroker(
            self.root,
            lambda _request: self.response(
                {
                    "ok": True,
                    "team_id": "T12345678",
                    "access_token": secret,
                    "message": f"Authorization: Bearer {secret}",
                }
            ),
        ) as broker:
            result = self.run_cli(
                "identity",
                "--json",
                socket_path=broker.path,
                extra_env={"TEST_API_KEY": secret},
            )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertNotIn(secret, result.stdout)
        payload = json.loads(result.stdout)
        self.assertEqual(payload["access_token"], "[REDACTED]")


if __name__ == "__main__":
    unittest.main()
