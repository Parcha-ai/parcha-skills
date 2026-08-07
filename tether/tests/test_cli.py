from __future__ import annotations

import hashlib
import json
import os
import pathlib
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
            "HERMES_HOME": str(self.root / "hermes"),
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
    ) -> subprocess.CompletedProcess[str]:
        env = dict(self.base_env)
        if socket_path is not None:
            env["TETHER_BROKER_SOCKET"] = str(socket_path)
        if extra_env:
            env.update(extra_env)
        return subprocess.run(
            ["node", str(CLI), *arguments],
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
            runtime / "bridge_runtime.py": 0o600,
            runtime / "hermes_compat.py": 0o600,
            runtime / "routing.py": 0o600,
            runtime / "security.py": 0o600,
            runtime / "slack_protocol.py": 0o600,
            runtime / "tether_notify.py": 0o700,
            runtime / "install.sh": 0o700,
            runtime / "package.json": 0o600,
            runtime / "herdr-plugin" / "herdr-plugin.toml": 0o644,
            runtime / "herdr-plugin" / "tether_plugin.py": 0o700,
            runtime / "herdr-plugin" / "README.md": 0o644,
            plugin / "__init__.py": 0o600,
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

    @unittest.skipIf(os.geteuid() == 0, "mutating CLI commands intentionally refuse root")
    def test_js_forwards_message_to_python_only_through_stdin(self) -> None:
        runtime = self.root / "data" / "tether"
        runtime.mkdir(parents=True)
        (runtime / "tether_notify.py").write_text("# notifier placeholder\n")
        capture = self.root / "child-capture.json"
        fake_python = self.root / "fake-python"
        fake_python.write_text(
            "#!/usr/bin/env python3\n"
            "import json, os, pathlib, sys\n"
            "pathlib.Path(os.environ['CAPTURE_PATH']).write_text(json.dumps({\n"
            "    'argv': sys.argv[1:], 'stdin': sys.stdin.read(),\n"
            "}))\n"
            "print('123.456')\n",
            encoding="utf-8",
        )
        fake_python.chmod(0o700)
        message = "must never appear in Python argv"
        result = self.run_cli(
            "notify",
            "--text",
            message,
            "--idempotency-key",
            "notify-1",
            extra_env={
                "PYTHON_BIN": str(fake_python),
                "CAPTURE_PATH": str(capture),
                "TETHER_BROKER_SOCKET": str(self.root / "bridge.sock"),
                "ZELLIJ_SESSION_NAME": "work",
                "ZELLIJ_PANE_ID": "7",
            },
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        captured = json.loads(capture.read_text())
        self.assertNotIn(message, captured["argv"])
        self.assertIn("--text-stdin", captured["argv"])
        self.assertEqual(captured["stdin"], message)
        self.assertIn("DEPRECATED", result.stderr)

    def test_python_notifier_reads_message_from_stdin(self) -> None:
        runtime = self.root / "data" / "tether"
        runtime.mkdir(parents=True)
        capture = self.root / "notifier-request.json"
        (runtime / "bridge_runtime.py").write_text(
            "import json, os, pathlib\n"
            "def broker_call(request):\n"
            "    pathlib.Path(os.environ['CAPTURE_PATH']).write_text(json.dumps(request))\n"
            "    return {'thread_ts': '123.456'}\n"
            "def doctor(): return (True, ['ok'])\n"
            "def herdr_agent_identity(*args): return {}\n"
            "def zellij_pane_identity(*args): return {}\n"
            "def working_directory_identity(cwd): return {'cwd': cwd}\n",
            encoding="utf-8",
        )
        message = "notifier stdin message"
        result = subprocess.run(
            [
                "python3",
                str(NOTIFIER),
                "reply",
                "--bridge-id",
                "brg_example",
                "--reply-key",
                "reply-1",
                "--text-stdin",
            ],
            text=True,
            input=message,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env={**self.base_env, "CAPTURE_PATH": str(capture)},
            timeout=4,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(capture.read_text())["text"], message)

    @unittest.skipIf(os.geteuid() == 0, "mutating CLI commands intentionally refuse root")
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

    @unittest.skipIf(os.geteuid() == 0, "mutating CLI commands intentionally refuse root")
    def test_resolve_requires_explicit_fields_and_confirms_action(self) -> None:
        with FakeBroker(
            self.root,
            lambda _request: self.response(
                {"ok": True, "status": "resolved"}
            ),
        ) as broker:
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
                socket_path=broker.path,
            )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            broker.requests,
            [{
                "op": "resolve",
                "kind": "attempt",
                "id": "attempt-1",
                "action": "abandon",
                "team_id": "T12345678",
            }],
        )
        self.assertEqual(
            result.stdout,
            'resolved attempt "attempt-1" with action=abandon\n',
        )

    @unittest.skipIf(os.geteuid() == 0, "mutating CLI commands intentionally refuse root")
    def test_resolve_rejects_unknown_actions_before_broker_call(self) -> None:
        result = self.run_cli(
            "resolve",
            "--kind",
            "ingress",
            "--id",
            "evt-1",
            "--action",
            "force",
        )
        self.assertEqual(result.returncode, 2)
        self.assertIn("--action must be one of", result.stderr)

    @unittest.skipIf(os.geteuid() == 0, "mutating CLI commands intentionally refuse root")
    def test_resolve_accepts_reconciliation_kind(self) -> None:
        with FakeBroker(
            self.root,
            lambda _request: self.response(
                {"ok": True, "status": "resolved"}
            ),
        ) as broker:
            result = self.run_cli(
                "resolve",
                "--kind",
                "reconciliation",
                "--id",
                "rec_0123456789abcdef0123456789abcdef",
                "--action",
                "retry",
                "--team",
                "T12345678",
                socket_path=broker.path,
            )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            broker.requests[0]["kind"],
            "reconciliation",
        )

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
            "ok managed install integrity verified (19 files; harness=codex)",
            payload["checks"],
        )
        self.assertEqual(payload["status"]["protocol_version"], 6)

    def test_doctor_fails_when_a_managed_file_drifted(self) -> None:
        self.write_managed_install(harness="codex")
        runtime = self.root / "data" / "tether" / "bridge_runtime.py"
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
            "ok managed install integrity verified (26 files; harness=both)",
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
            runtime / "bridge_runtime.py",
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
