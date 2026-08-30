"""Subprocess-level tests: cmd_run against fake harness binaries, and the
node installer. No network, no real codex/pi."""

import hashlib
import json
import os
import secrets
import shutil
import signal
import socket
import stat
import subprocess
import tempfile
import threading
import time
import unittest
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

# Subprocess fixtures inherit this process environment. Isolate them from an
# enclosing Parable session and from the user's shared live-usage cache.
for key in (
    "PARABLE_AGENT_STATE_JSON",
    "PARABLE_CONTEXT_RECOVERY_FILE",
    "PARABLE_WELCOME_MESSAGE",
    "CLAUDE_AUTOCOMPACT_PCT_OVERRIDE",
    "CLAUDE_CODE_AUTO_COMPACT_WINDOW",
    "CLAUDE_CODE_MAX_CONTEXT_TOKENS",
):
    os.environ.pop(key, None)
_TEST_USAGE_CACHE_DIR = tempfile.TemporaryDirectory(prefix="parable-integration-usage-")
os.environ["PARABLE_USAGE_CACHE"] = str(
    Path(_TEST_USAGE_CACHE_DIR.name) / "usage-cache.json"
)

REPO = Path(__file__).resolve().parent.parent
SCRIPT = REPO / "skills" / "parable" / "scripts" / "parable.py"
NODE = shutil.which("node") or "node"
PROXY_COMMIT = "323b7276bc5bd251e5497699e42c556d6316b30c"
PROXY_PATCH_SHA256 = "89f1cbe8b274c114b94bd1f5146658046e1124f1510a402359f26e2a87f38b4a"

FAKE_CODEX = """#!/usr/bin/env bash
cat > /dev/null   # drain the plan from stdin like the real binary
echo '{"type":"thread.started","thread_id":"fake-thread-1"}'
echo '{"type":"turn.started"}'
echo '{"type":"item.completed","item":{"id":"i1","type":"agent_message","text":"done from fake codex"}}'
echo '{"type":"turn.completed","usage":{"input_tokens":10,"cached_input_tokens":2,"output_tokens":3}}'
"""

FAKE_PI = """#!/usr/bin/env bash
echo '{"type":"session","version":3,"id":"fake-pi-session"}'
echo '{"type":"turn_start"}'
echo '{"type":"tool_execution_start","toolName":"bash"}'
echo '{"type":"message_end","message":{"role":"assistant","stopReason":"stop","content":[{"type":"text","text":"done from fake pi"}],"usage":{"input":7,"output":2,"cacheRead":1,"cacheWrite":0,"cost":{"total":0.0005}}}}'
echo '{"type":"agent_end"}'
"""

FAKE_CLAUDE = """#!/usr/bin/env python3
import json
import os
import signal
import sys
import time

capture = {
    "argv": sys.argv[1:],
    "base_url": os.environ.get("ANTHROPIC_BASE_URL"),
    "welcome_message": os.environ.get("PARABLE_WELCOME_MESSAGE"),
    "agent_state": os.environ.get("PARABLE_AGENT_STATE_JSON"),
    "auth_token_present": bool(os.environ.get("ANTHROPIC_AUTH_TOKEN")),
    "source_token_present": any(
        key in os.environ for key in ("PARABLE_PROXY_TOKEN", "CLIPROXY_API_KEY")
    ),
    "inherited": {
        key: key in os.environ
        for key in (
            "ANTHROPIC_API_KEY",
            "CLAUDE_CODE_OAUTH_TOKEN",
            "CLAUDE_CODE_SUBAGENT_MODEL",
            "CLAUDE_CODE_EXPERIMENTAL_AGENT_TEAMS",
        )
    },
    "max_context_tokens": os.environ.get("CLAUDE_CODE_MAX_CONTEXT_TOKENS"),
    "auto_compact_window": os.environ.get("CLAUDE_CODE_AUTO_COMPACT_WINDOW"),
    "auto_compact_pct": os.environ.get("CLAUDE_AUTOCOMPACT_PCT_OVERRIDE"),
    "context_recovery_enabled": bool(
        os.environ.get("PARABLE_CONTEXT_RECOVERY_FILE")
    ),
    "resume_picker_recovery": os.environ.get("PARABLE_CONTEXT_RESUME_PICKER") == "1",
    "teammate_recovery_active": os.environ.get("PARABLE_TEAMMATE_RECOVERY_ACTIVE") == "1",
}
with open(os.environ["FAKE_CLAUDE_CAPTURE"], "w") as handle:
    json.dump(capture, handle)
calls_path = os.environ.get("FAKE_CLAUDE_CALLS")
if calls_path:
    with open(calls_path, "a") as handle:
        handle.write(json.dumps(capture) + "\\n")
recovery_path = os.environ.get("PARABLE_CONTEXT_RECOVERY_FILE")
picker_session = os.environ.get("FAKE_CLAUDE_RESUME_PICKER_SESSION")
arguments = sys.argv[1:]
bare_resume = any(
    argument == "--resume="
    or (
        argument in {"-r", "--resume"}
        and (index + 1 == len(arguments) or arguments[index + 1].startswith("-"))
    )
    for index, argument in enumerate(arguments)
)
selected_model = (
    sys.argv[sys.argv.index("--model") + 1] if "--model" in sys.argv else None
)
failure_once = os.environ.get("FAKE_CLAUDE_CONTEXT_FAILURE_ONCE")
failure_state = os.environ.get("FAKE_CLAUDE_CONTEXT_FAILURE_STATE")
if failure_once and failure_state:
    context_failure = not os.path.exists(failure_state)
    if context_failure:
        descriptor = os.open(
            failure_state, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600
        )
        os.close(descriptor)
else:
    context_failure = bool(failure_once and "--resume" not in sys.argv)
context_failure = context_failure or (
    os.environ.get("FAKE_CLAUDE_CONTEXT_FAILURE_ALWAYS")
    and selected_model != "claude-sonnet-5[1m]"
)
picker_recovery = bool(
    recovery_path
    and picker_session
    and bare_resume
    and os.environ.get("PARABLE_CONTEXT_RESUME_PICKER") == "1"
)
teammate_once = os.environ.get("FAKE_CLAUDE_TEAMMATE_INTERRUPT_ONCE")
teammate_state = os.environ.get("FAKE_CLAUDE_TEAMMATE_INTERRUPT_STATE")
teammate_recovery = bool(
    recovery_path
    and teammate_once
    and teammate_state
    and not os.path.exists(teammate_state)
)
if teammate_recovery:
    descriptor = os.open(
        teammate_state, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600
    )
    os.close(descriptor)
if recovery_path and (context_failure or picker_recovery or teammate_recovery):
    request = {
        "version": 1,
        "reason": (
            "resume_picker" if picker_recovery
            else "teammate_interrupt" if teammate_recovery
            else "context_failure"
        ),
        "session_id": picker_session if picker_recovery else "12345678-1234-4234-9234-123456789abc",
    }
    descriptor = os.open(recovery_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(descriptor, "w") as handle:
        json.dump(request, handle)
    if os.environ.get("FAKE_CLAUDE_CONTEXT_FAILURE_ALWAYS") and "--resume" in sys.argv:
        raise SystemExit(int(os.environ.get("FAKE_CLAUDE_EXIT", "44")))
    def recover_stop(signum, _frame):
        raise SystemExit(128 + signum)
    signal.signal(signal.SIGTERM, recover_stop)
    while True:
        time.sleep(0.05)
if sys.argv[-1:] == ["/context"]:
    requested_session = (
        sys.argv[sys.argv.index("--resume") + 1]
        if "--resume" in sys.argv else "resolved-resume-session"
    )
    resumed_exactly = (
        "--resume" in sys.argv
        and requested_session in {"resolved-resume-session", "12345678-1234-4234-9234-123456789abc"}
    )
    compact_state = os.environ.get("FAKE_CLAUDE_COMPACT_STATE")
    compact_finished = bool(compact_state and os.path.exists(compact_state))
    token_env = (
        "FAKE_CLAUDE_POST_COMPACT_TOKENS"
        if resumed_exactly and (requested_session == "resolved-resume-session" or compact_finished)
        else "FAKE_CLAUDE_CONTEXT_TOKENS"
    )
    tokens = os.environ.get(token_env, "42000")
    print(json.dumps({
        "type": "result", "is_error": False,
        "session_id": requested_session,
        "result": "**Tokens:** " + tokens + " / 967k (33%)",
    }))
    raise SystemExit(0)
if sys.argv[-1:] and sys.argv[-1].startswith("/compact "):
    compact_state = os.environ.get("FAKE_CLAUDE_COMPACT_STATE")
    if compact_state:
        with open(compact_state, "w") as handle:
            handle.write("done\\n")
    print(json.dumps({
        "type": "result", "is_error": False,
        "session_id": "resolved-resume-session", "result": "",
    }))
    raise SystemExit(0)
if os.environ.get("FAKE_CLAUDE_WAIT"):
    def stop(signum, _frame):
        target = os.environ.get("FAKE_CLAUDE_SIGNAL_CAPTURE")
        if target:
            with open(target, "w") as handle:
                json.dump({"pid": os.getpid(), "signal": signum}, handle)
        raise SystemExit(128 + signum)
    for handled in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
        signal.signal(handled, stop)
    while True:
        time.sleep(0.05)
raise SystemExit(int(os.environ.get("FAKE_CLAUDE_EXIT", "0")))
"""

CONFIG = """
[parable]
version = 1
[providers.fake-codex]
type = "codex"
base_url = "https://example.test/v1"
env_key = "FAKE_KEY"
wire_api = "responses"
[providers.fake-pi]
type = "pi"
base_url = "https://example.test/v1"
env_key = "FAKE_KEY"
[executors.cx]
provider = "fake-codex"
model = "fake/model"
effort = "low"
[executors.px]
provider = "fake-pi"
model = "fake/model"
effort = "low"
"""


def claude_config(base_url: str, include_kimi: bool = True) -> str:
    config = f"""
[parable]
version = 1

[claude]
base_url = "{base_url}"
auth_token_env = "PARABLE_PROXY_TOKEN"
brain_model = "gpt-5.6-sol"

[providers.claude]
type = "subagent"
"""
    if include_kimi:
        config += """

[executors.kimi]
provider = "claude"
model = "kimi-k3"
tags = ["implementer", "third-party"]
use_for = "Implementation tasks that benefit from an independent model family."
avoid_for = "Tasks that must remain on the parent model."
"""
    return config


class _ModelHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        server = self.server
        if self.path != "/v1/models":
            self.send_error(404)
            return
        server.authorization_ok = (
            self.headers.get("Authorization") == f"Bearer {server.expected_token}"
        )
        if not server.authorization_ok:
            self.send_error(401)
            return
        body = json.dumps({"data": [{"id": model} for model in server.models]}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, _format, *_args):
        pass


@contextmanager
def model_server(models: list[str]):
    token = secrets.token_urlsafe(24)
    server = ThreadingHTTPServer(("127.0.0.1", 0), _ModelHandler)
    server.models = models
    server.expected_token = token
    server.authorization_ok = False
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server, f"http://127.0.0.1:{server.server_address[1]}", token
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def make_repo(tmp: str) -> Path:
    repo = Path(tmp) / "repo"
    (repo / ".claude").mkdir(parents=True)
    (repo / ".claude" / "parable.toml").write_text(CONFIG)
    (repo / "plan.md").write_text("Toy plan.")
    subprocess.run(["git", "init", "-q"], cwd=repo, check=True)
    return repo


def fake_bin(tmp: str) -> Path:
    bindir = Path(tmp) / "bin"
    bindir.mkdir()
    for name, body in (("codex", FAKE_CODEX), ("pi", FAKE_PI), ("claude", FAKE_CLAUDE)):
        f = bindir / name
        f.write_text(body)
        f.chmod(0o755)
    return bindir


def run_cli(repo: Path, bindir: Path, *args: str) -> subprocess.CompletedProcess:
    env = os.environ | {"PATH": f"{bindir}:{os.environ['PATH']}",
                        "FAKE_KEY": "test-key-value"}
    env.pop("PARABLE_CONFIG", None)
    return subprocess.run(["python3", str(SCRIPT), *args],
                          cwd=repo, env=env, capture_output=True, text=True, timeout=60)


class TestCmdRunEndToEnd(unittest.TestCase):
    def test_codex_run_writes_artifacts_and_summary(self):
        with tempfile.TemporaryDirectory() as tmp:
            repo, bindir = make_repo(tmp), fake_bin(tmp)
            p = run_cli(repo, bindir, "run", "cx", str(repo / "plan.md"), "--slug", "toy")
            self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
            self.assertIn("STATUS   OK", p.stdout)
            self.assertIn("SESSION  fake-thread-1", p.stdout)
            run_dir = next((repo / ".parable" / "runs").iterdir())
            meta = json.loads((run_dir / "meta.json").read_text())
            self.assertEqual(meta["harness"], "codex")
            self.assertEqual(meta["status"], "OK")
            self.assertEqual(meta["session_id"], "fake-thread-1")
            self.assertTrue((run_dir / "cmd.txt").exists())
            self.assertTrue((run_dir / "harness.jsonl").exists())

    def test_pi_run_generates_agent_dir_and_cost(self):
        with tempfile.TemporaryDirectory() as tmp:
            repo, bindir = make_repo(tmp), fake_bin(tmp)
            p = run_cli(repo, bindir, "run", "px", str(repo / "plan.md"), "--slug", "toy")
            self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
            self.assertIn("cost=$0.0005", p.stdout)
            run_dir = next((repo / ".parable" / "runs").iterdir())
            meta = json.loads((run_dir / "meta.json").read_text())
            self.assertEqual(meta["harness"], "pi")
            models = json.loads((run_dir / "pi-agent" / "models.json").read_text())
            self.assertEqual(models["providers"]["parable_fake-pi"]["apiKey"], "$FAKE_KEY")
            self.assertNotIn("test-key-value", (run_dir / "cmd.txt").read_text())

    def test_missing_env_key_fails_before_dispatch(self):
        with tempfile.TemporaryDirectory() as tmp:
            repo, bindir = make_repo(tmp), fake_bin(tmp)
            env = os.environ | {"PATH": f"{bindir}:{os.environ['PATH']}"}
            env.pop("FAKE_KEY", None)
            env.pop("PARABLE_CONFIG", None)
            p = subprocess.run(["python3", str(SCRIPT), "run", "cx", str(repo / "plan.md")],
                               cwd=repo, env=env, capture_output=True, text=True, timeout=60)
            self.assertNotEqual(p.returncode, 0)
            self.assertIn("FAKE_KEY is not set", p.stderr)


class TestClaudeSubscriptionLauncher(unittest.TestCase):
    def make_claude_repo(self, tmp: str, base_url: str, include_kimi: bool = True) -> Path:
        repo = Path(tmp) / "claude-repo"
        repo.mkdir()
        (repo / "parable.toml").write_text(claude_config(base_url, include_kimi))
        subprocess.run(["git", "init", "-q"], cwd=repo, check=True)
        return repo

    def launch_env(self, tmp: str, bindir: Path, capture: Path, token: str) -> dict[str, str]:
        home = Path(tmp) / "home"
        (home / ".claude").mkdir(parents=True)
        (home / ".claude" / "settings.json").write_text('{"theme":"unchanged"}\n')
        return os.environ | {
            "HOME": str(home),
            "PATH": f"{bindir}:{os.environ['PATH']}",
            "PARABLE_PROXY_TOKEN": token,
            "FAKE_CLAUDE_CAPTURE": str(capture),
            "ANTHROPIC_API_KEY": "must-not-survive",
            "CLAUDE_CODE_OAUTH_TOKEN": "must-not-survive",
            "CLAUDE_CODE_SUBAGENT_MODEL": "gpt-5.6-sol",
            "CLAUDE_CODE_EXPERIMENTAL_AGENT_TEAMS": "1",
        }

    def test_launcher_routes_sol_forwards_args_and_scrubs_global_override(self):
        with tempfile.TemporaryDirectory() as tmp, model_server(
            ["gpt-5.6-sol", "kimi-k3", "unrelated-model"]
        ) as (server, base_url, token):
            bindir = fake_bin(tmp)
            repo = self.make_claude_repo(tmp, base_url)
            capture = Path(tmp) / "capture.json"
            env = self.launch_env(tmp, bindir, capture, token)
            settings = Path(env["HOME"]) / ".claude" / "settings.json"
            before = settings.read_bytes()

            proc = subprocess.run(
                ["node", str(REPO / "bin" / "parable.js"), "claude", "--print", "hello"],
                cwd=repo, env=env, capture_output=True, text=True, timeout=60,
            )

            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertTrue(server.authorization_ok)
            capture_text = capture.read_text()
            self.assertNotIn(token, capture_text)
            captured = json.loads(capture_text)
            welcome_plugin = (
                REPO / "skills" / "parable" / "runtime" / "welcome-plugin"
            )
            self.assertEqual(
                captured["argv"],
                [
                    "--plugin-dir", str(welcome_plugin),
                    "--model", "gpt-5.6-sol", "--print", "hello",
                ],
            )
            self.assertEqual(captured["base_url"], base_url)
            self.assertTrue(captured["auth_token_present"])
            self.assertFalse(captured["source_token_present"])
            self.assertEqual(
                json.loads(captured["agent_state"]),
                {
                    "active": ["parable-kimi"],
                    "unavailable": [],
                    "parent": [],
                },
            )
            self.assertEqual(
                captured["inherited"],
                {
                    "ANTHROPIC_API_KEY": False,
                    "CLAUDE_CODE_OAUTH_TOKEN": False,
                    "CLAUDE_CODE_SUBAGENT_MODEL": False,
                    "CLAUDE_CODE_EXPERIMENTAL_AGENT_TEAMS": True,
                },
            )
            self.assertEqual(settings.read_bytes(), before)
            agent = repo / ".claude" / "agents" / "parable-kimi.md"
            self.assertIn('model: "kimi-k3"', agent.read_text())
            self.assertNotIn(token, agent.read_text())
            self.assertNotIn(token, (repo / "parable.toml").read_text())

    def test_launcher_keeps_resume_on_one_million_sol_window(self):
        with tempfile.TemporaryDirectory() as tmp, model_server(
            ["gpt-5.6-sol", "claude-sonnet-5", "kimi-k3"]
        ) as (_server, base_url, token):
            bindir = fake_bin(tmp)
            repo = self.make_claude_repo(tmp, base_url)
            capture = Path(tmp) / "capture.json"
            calls = Path(tmp) / "claude-calls.jsonl"
            env = self.launch_env(tmp, bindir, capture, token) | {
                "FAKE_CLAUDE_CALLS": str(calls),
                "FAKE_CLAUDE_CONTEXT_TOKENS": "321400",
            }

            proc = subprocess.run(
                [
                    "node", str(REPO / "bin" / "parable.js"),
                    "claude", "--continue", "--print", "finish",
                ],
                cwd=repo, env=env, capture_output=True, text=True, timeout=60,
            )

            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertNotIn("resume: compacted", proc.stdout)
            recorded = [json.loads(line) for line in calls.read_text().splitlines()]
            self.assertEqual(len(recorded), 1)
            self.assertEqual(
                recorded[0]["argv"][-4:],
                ["gpt-5.6-sol", "--continue", "--print", "finish"],
            )
            self.assertEqual(recorded[0]["max_context_tokens"], "1000000")
            self.assertEqual(recorded[0]["auto_compact_window"], "1000000")
            self.assertEqual(recorded[0]["auto_compact_pct"], "90")

    def test_launcher_degrades_when_an_optional_routed_model_is_absent(self):
        with tempfile.TemporaryDirectory() as tmp, model_server(
            ["gpt-5.6-sol"]
        ) as (_server, base_url, token):
            bindir = fake_bin(tmp)
            repo = self.make_claude_repo(tmp, base_url)
            capture = Path(tmp) / "capture.json"
            env = self.launch_env(tmp, bindir, capture, token)
            proc = subprocess.run(
                ["node", str(REPO / "bin" / "parable.js"), "claude", "--print", "hello"],
                cwd=repo, env=env, capture_output=True, text=True, timeout=60,
            )
            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn(
                "degraded: parable-kimi unavailable for this session", proc.stdout
            )
            captured = json.loads(capture.read_text())
            self.assertEqual(
                json.loads(captured["agent_state"]),
                {
                    "active": [],
                    "unavailable": ["parable-kimi"],
                    "parent": [],
                },
            )
            agent = repo / ".claude" / "agents" / "parable-kimi.md"
            self.assertTrue(agent.is_file())
            self.assertIn('model: "kimi-k3"', agent.read_text())

    def test_auto_falls_back_but_explicit_unavailable_parent_fails(self):
        with tempfile.TemporaryDirectory() as tmp, model_server(
            ["gpt-5.6-sol"]
        ) as (_server, base_url, token):
            bindir = fake_bin(tmp)
            repo = self.make_claude_repo(tmp, base_url)
            config = (repo / "parable.toml").read_text()
            config = config.replace(
                'brain_model = "gpt-5.6-sol"',
                'brain_model = "claude-fable-5"',
            )
            config += """

[executors.sol_exact]
provider = "claude"
model = "gpt-5.6-sol"

[executors.fable_exact]
provider = "claude"
model = "claude-fable-5"
"""
            (repo / "parable.toml").write_text(config)
            capture = Path(tmp) / "capture.json"
            env = self.launch_env(tmp, bindir, capture, token)

            automatic = subprocess.run(
                [
                    "node", str(REPO / "bin" / "parable.js"),
                    "claude", "--brain", "auto", "--print", "hello",
                ],
                cwd=repo, env=env, capture_output=True, text=True, timeout=60,
            )
            self.assertEqual(
                automatic.returncode, 0, automatic.stdout + automatic.stderr
            )
            self.assertIn(
                "brain: gpt-5.6-sol (Fable is unavailable; using Sol)",
                automatic.stdout,
            )
            self.assertEqual(
                json.loads(json.loads(capture.read_text())["agent_state"]),
                {
                    "active": [],
                    "unavailable": ["parable-fable-exact", "parable-kimi"],
                    "parent": ["parable-sol-exact"],
                },
            )
            finalized = subprocess.run(
                [
                    "python3", str(SCRIPT), "finalize", "--json",
                ],
                cwd=repo, env=env, capture_output=True, text=True, timeout=60,
            )
            self.assertEqual(
                finalized.returncode, 0, finalized.stdout + finalized.stderr
            )
            report = json.loads(finalized.stdout)
            self.assertTrue(report["degraded"])
            self.assertEqual(report["configuredParentModel"], "claude-fable-5")
            self.assertEqual(report["parentModel"], "gpt-5.6-sol")

            capture.unlink()
            explicit = subprocess.run(
                [
                    "node", str(REPO / "bin" / "parable.js"),
                    "claude", "--brain", "fable", "--print", "hello",
                ],
                cwd=repo, env=env, capture_output=True, text=True, timeout=60,
            )
            self.assertNotEqual(explicit.returncode, 0)
            self.assertIn(
                "--brain fable model 'claude-fable-5' is unavailable",
                explicit.stderr,
            )
            self.assertFalse(capture.exists())

    def test_agent_sync_is_idempotent_cleans_stale_and_preserves_unrelated(self):
        with tempfile.TemporaryDirectory() as tmp:
            bindir = fake_bin(tmp)
            repo = self.make_claude_repo(tmp, "http://127.0.0.1:8317")
            agents = repo / ".claude" / "agents"
            agents.mkdir(parents=True)
            unrelated = agents / "handwritten.md"
            unrelated.write_text("---\nname: handwritten\ndescription: mine\n---\nKeep me.\n")
            deceptive = agents / "parable-handwritten.md"
            deceptive.write_text("---\nname: parable-handwritten\ndescription: mine\n---\nKeep me too.\n")
            stale = agents / "parable-stale.md"
            stale.write_text(
                "---\nname: parable-stale\ndescription: old\nmodel: old\n---\n"
                "<!-- Generated by @parcha/parable from parable.toml. -->\n"
            )
            env = os.environ | {
                "HOME": str(Path(tmp) / "empty-home"),
                "PATH": f"{bindir}:{os.environ['PATH']}",
            }
            command = ["node", str(REPO / "bin" / "parable.js"), "agents", "sync"]

            first = subprocess.run(
                command, cwd=repo, env=env, capture_output=True, text=True, timeout=60,
            )
            self.assertEqual(first.returncode, 0, first.stdout + first.stderr)
            generated = agents / "parable-kimi.md"
            first_content = generated.read_bytes()
            first_mtime = generated.stat().st_mtime_ns
            self.assertEqual(generated.stat().st_mode & 0o777, 0o644)
            self.assertFalse(stale.exists())
            self.assertTrue(unrelated.exists())
            self.assertTrue(deceptive.exists())

            second = subprocess.run(
                command, cwd=repo, env=env, capture_output=True, text=True, timeout=60,
            )
            self.assertEqual(second.returncode, 0, second.stdout + second.stderr)
            self.assertIn("0 changed, 1 unchanged, 0 removed", second.stdout)
            self.assertEqual(generated.read_bytes(), first_content)
            self.assertEqual(generated.stat().st_mtime_ns, first_mtime)

            (repo / "parable.toml").write_text(claude_config(
                "http://127.0.0.1:8317", include_kimi=False
            ))
            third = subprocess.run(
                command, cwd=repo, env=env, capture_output=True, text=True, timeout=60,
            )
            self.assertEqual(third.returncode, 0, third.stdout + third.stderr)
            self.assertFalse(generated.exists())
            self.assertTrue(unrelated.exists())
            self.assertTrue(deceptive.exists())


class TestInstallerSmoke(unittest.TestCase):
    def make_auth_proxy(self, target: Path) -> Path:
        target.write_text("""#!/usr/bin/env sh
case " $* " in
  *" --claude-login "*)
    umask 077
    printf '%s\n' '{"type":"claude"}' > "$HOME/.cli-proxy-api/claude-test.json"
    ;;
esac
exit 0
""")
        target.chmod(0o755)
        return target

    def test_public_onboarding_surfaces_use_skill_bootstrap_and_auto_handoff(self):
        readme = (REPO / "README.md").read_text()
        guide = (REPO / "docs" / "CLIPROXYAPI_GPT_SUBSCRIPTION.md").read_text()
        skill = (REPO / "skills" / "parable" / "SKILL.md").read_text()
        providers = (REPO / "skills" / "parable" / "references" / "providers.md").read_text()
        installer = (REPO / "install.sh").read_text()

        self.assertIn("./install.sh", readme)
        self.assertIn("new-terminal `parable` command", readme)
        self.assertNotIn("# terminal 1: foreground local proxy", readme)
        self.assertNotIn('"$PARABLE" setup finalize\n"$PARABLE" claude', readme)
        self.assertLess(readme.index("## Install Parable"), readme.index("## Unscientific stats"))
        self.assertIn("Read [`skills/parable/SKILL.md`](skills/parable/SKILL.md) now", readme)
        self.assertIn("`AskUserQuestion` in Claude Code", readme)
        self.assertIn("not a file-copy task", readme)
        self.assertIn("Do not run `install.sh` as a generic skill copier", readme)
        self.assertIn("First-run succeeds only after the selected native OAuth flows", readme)

        self.assertIn("./install.sh", guide)
        self.assertIn("\nparable\n", guide)
        self.assertIn("That is the whole ordinary path.", guide)
        self.assertIn("stops only the proxy process it owns", guide)
        self.assertIn("Neither command is part of ordinary onboarding.", guide)

        for surface in (skill, providers):
            self.assertIn("parable.sh", surface)
            self.assertIn("`parable`", surface)
            self.assertIn("setup finalize", surface)
            self.assertIn("proxy start", surface)

        self.assertIn("install parable.sh", skill)
        self.assertIn("AskUserQuestion", skill)
        self.assertIn("request_user_input", skill)
        self.assertIn("do not silently infer paid subscriptions", skill)
        self.assertIn("--non-interactive", skill)
        self.assertIn("--vendors claude[,chatgpt][,xai][,kimi]", skill)
        self.assertIn("--build-proxy", skill)
        self.assertIn("Claude Code's `Bash` tool is the exception", skill)
        self.assertIn("open a new terminal and run exactly", skill)
        self.assertIn("parable auth login", skill)
        self.assertIn("Do not run it through", skill)
        self.assertIn("Do not give the user three separate `auth add` commands", skill)
        self.assertIn("should never be handed separate per-provider commands", readme)
        self.assertIn("inside Claude Code", readme)
        self.assertIn("user-only", skill)
        self.assertIn("--brain auto|fable|sol|grok|config", skill)
        self.assertIn("Grok 4.6", readme)
        self.assertIn("`parable --brain grok`", guide)
        self.assertIn('model = "grok-4.6"', providers)
        self.assertIn('"model_context_window=1000000"', providers)
        self.assertIn('"model_auto_compact_token_limit=900000"', providers)

        self.assertIn('chmod +x "$DEST"/parable.sh', installer)
        self.assertIn('exec "$DEST/parable.sh" "$@"', installer)

        help_proc = subprocess.run(
            [NODE, str(REPO / "bin" / "parable.js"), "--help"],
            capture_output=True,
            text=True,
            timeout=60,
        )
        self.assertEqual(help_proc.returncode, 0, help_proc.stdout + help_proc.stderr)
        self.assertIn("--brain auto|fable|sol|grok|config", help_proc.stdout)
        self.assertIn("auto-brain Claude Code session", help_proc.stdout)
        self.assertIn("backward-compatible explicit launcher alias", help_proc.stdout)
        self.assertIn("diagnostic foreground", help_proc.stdout)
        self.assertIn("auth login", help_proc.stdout)

    def test_install_and_error_path(self):
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "target"
            p = subprocess.run(["node", str(REPO / "bin" / "parable.js"), "install",
                                "--target", str(target)],
                               capture_output=True, text=True, timeout=60)
            self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
            self.assertTrue((target / "skills" / "parable" / "SKILL.md").exists())
            self.assertTrue((target / "parable.toml").exists())
            # error path: target dir path occupied by a file must fail loudly
            blocked = Path(tmp) / "blocked"
            blocked.write_text("a file, not a dir")
            p2 = subprocess.run(["node", str(REPO / "bin" / "parable.js"), "install",
                                 "--target", str(blocked)],
                                capture_output=True, text=True, timeout=60)
            self.assertNotEqual(p2.returncode, 0)
            self.assertIn("error", (p2.stderr + p2.stdout).lower())

    def test_skill_only_bootstrap_installs_reruns_and_hands_off_auto(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            home = root / "home"
            home.mkdir()
            (home / ".bashrc").write_text("# user config\n")
            standalone = root / "installed-skill"
            shutil.copytree(REPO / "skills" / "parable", standalone)
            proxy = self.make_auth_proxy(root / "fake-proxy")
            env = os.environ | {"HOME": str(home), "SHELL": "/bin/bash"}
            for name in ("PARABLE_CONFIG", "PARABLE_CLIPROXY_BIN", "CLIPROXY_API_KEY"):
                env.pop(name, None)
            command = [
                "bash", str(standalone / "parable.sh"),
                "--non-interactive", "--vendors", "claude",
                "--proxy-bin", str(proxy),
            ]

            first = subprocess.run(
                command, cwd=root, env=env, capture_output=True, text=True, timeout=60,
            )
            self.assertEqual(first.returncode, 0, first.stdout + first.stderr)
            handoff = "In a new terminal, open your project and run:"
            launch = "  parable\n"
            self.assertEqual(first.stdout.count(handoff), 1)
            self.assertIn(launch, first.stdout)

            installed = home / ".local" / "share" / "parable" / "0.1.33"
            durable = home / ".local" / "bin" / "parable"
            self.assertTrue((installed / "bin" / "parable.js").is_file())
            self.assertTrue((installed / "lib" / "onboarding.js").is_file())
            self.assertTrue((installed / "skills" / "parable" / "SKILL.md").is_file())
            welcome = (
                installed / "skills" / "parable" / "runtime" / "welcome-plugin"
                / "scripts" / "welcome.py"
            )
            self.assertTrue(welcome.is_file())
            rendered = subprocess.run(
                ["python3", str(welcome)],
                env=env | {"PARABLE_WELCOME_MESSAGE": "PARABLE READY"},
                capture_output=True,
                text=True,
                timeout=60,
            )
            self.assertEqual(rendered.returncode, 0, rendered.stdout + rendered.stderr)
            self.assertEqual(json.loads(rendered.stdout), {
                "systemMessage": "\nPARABLE READY",
                "suppressOutput": True,
            })
            self.assertTrue(durable.is_symlink())
            self.assertEqual(durable.resolve(), (installed / "bin" / "parable.js").resolve())
            self.assertEqual((home / ".config" / "parable").stat().st_mode & 0o777, 0o700)
            self.assertEqual(
                (home / ".config" / "parable" / "parable.toml").stat().st_mode & 0o777,
                0o600,
            )
            bashrc = home / ".bashrc"
            self.assertTrue(bashrc.read_text().startswith("# user config\n"))
            self.assertIn("# Added by Parable: user commands", bashrc.read_text())
            fresh = subprocess.run(
                ["bash", "--noprofile", "--rcfile", str(bashrc), "-i", "-c", "command -v parable"],
                env=env,
                capture_output=True,
                text=True,
                timeout=60,
            )
            self.assertEqual(fresh.returncode, 0, fresh.stdout + fresh.stderr)
            self.assertIn(str(durable), fresh.stdout)

            second = subprocess.run(
                command, cwd=root, env=env, capture_output=True, text=True, timeout=60,
            )
            self.assertEqual(second.returncode, 0, second.stdout + second.stderr)
            self.assertIn("runtime: already installed", second.stdout)
            self.assertEqual(second.stdout.count(handoff), 1)
            self.assertEqual(bashrc.read_text().count("# Added by Parable: user commands"), 1)

            config = home / ".config" / "parable" / "parable.toml"
            config.write_text(config.read_text() + "\n# user edit\n")
            edited = subprocess.run(
                command, cwd=root, env=env, capture_output=True, text=True, timeout=60,
            )
            self.assertNotEqual(edited.returncode, 0)
            self.assertNotIn(handoff, edited.stdout)
            self.assertTrue(config.read_text().endswith("# user edit\n"))

    def test_skill_bootstrap_refuses_unrelated_command_and_missing_node(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            home = root / "home"
            home.mkdir()
            standalone = root / "installed-skill"
            shutil.copytree(REPO / "skills" / "parable", standalone)
            local_bin = home / ".local" / "bin"
            local_bin.mkdir(parents=True)
            unrelated = local_bin / "parable"
            unrelated.write_text("user-owned\n")
            env = os.environ | {
                "HOME": str(home),
                "SHELL": "/bin/bash",
                "PATH": f"{local_bin}:{os.environ['PATH']}",
            }
            blocked = subprocess.run(
                ["bash", str(standalone / "parable.sh"), "--non-interactive"],
                cwd=root,
                env=env,
                capture_output=True,
                text=True,
                timeout=60,
            )
            self.assertNotEqual(blocked.returncode, 0)
            self.assertIn("not managed by Parable", blocked.stderr)
            self.assertNotIn("In a new terminal", blocked.stdout)
            self.assertEqual(unrelated.read_text(), "user-owned\n")

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            home = root / "home"
            home.mkdir()
            standalone = root / "installed-skill"
            shutil.copytree(REPO / "skills" / "parable", standalone)
            tools = root / "tools"
            tools.mkdir()
            for name in ("dirname", "tr"):
                os.symlink(shutil.which(name), tools / name)
            missing = subprocess.run(
                ["/bin/bash", str(standalone / "parable.sh")],
                cwd=root,
                env={"HOME": str(home), "SHELL": "/bin/bash", "PATH": str(tools)},
                capture_output=True,
                text=True,
                timeout=60,
            )
            self.assertNotEqual(missing.returncode, 0)
            self.assertIn("node is required", missing.stderr)
            self.assertFalse((home / ".local" / "share" / "parable").exists())

    def test_skill_bootstrap_no_auth_never_prints_ready_handoff(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            home = root / "home"
            home.mkdir()
            standalone = root / "installed-skill"
            shutil.copytree(REPO / "skills" / "parable", standalone)
            proxy = root / "fake-proxy"
            proxy.write_text("#!/usr/bin/env sh\nexit 0\n")
            proxy.chmod(0o755)
            proc = subprocess.run(
                [
                    "bash", str(standalone / "parable.sh"),
                    "--non-interactive", "--vendors", "claude",
                    "--proxy-bin", str(proxy), "--no-auth",
                ],
                cwd=root,
                env=os.environ | {"HOME": str(home), "SHELL": "/bin/bash"},
                capture_output=True,
                text=True,
                timeout=60,
            )
            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("subscriptions are not authorized", proc.stdout)
            self.assertIn("Open a new terminal and run:", proc.stdout)
            self.assertIn("parable auth login", proc.stdout)
            self.assertNotIn("! parable auth login", proc.stdout)
            self.assertNotIn("Run `parable auth add` for each selected vendor", proc.stdout)
            self.assertNotIn("In a new terminal", proc.stdout)

    def test_bundled_runtime_version_and_patch_match_package(self):
        package = json.loads((REPO / "package.json").read_text())
        version = (REPO / "skills" / "parable" / "runtime" / "VERSION").read_text().strip()
        self.assertEqual(version, package["version"])
        plugin_manifest = json.loads((
            REPO / ".claude-plugin" / "plugin.json"
        ).read_text())
        self.assertEqual(plugin_manifest["version"], package["version"])
        marketplace = json.loads((
            REPO / ".claude-plugin" / "marketplace.json"
        ).read_text())
        self.assertEqual(marketplace["plugins"][0]["version"], package["version"])
        welcome_manifest = json.loads((
            REPO / "skills" / "parable" / "runtime" / "welcome-plugin"
            / ".claude-plugin" / "plugin.json"
        ).read_text())
        self.assertEqual(welcome_manifest["version"], package["version"])
        patch = (
            REPO / "skills" / "parable" / "runtime" / "patches"
            / "cliproxyapi-v7.2.131-claude-effort.patch"
        )
        self.assertEqual(hashlib.sha256(patch.read_bytes()).hexdigest(), PROXY_PATCH_SHA256)

    def test_global_install_does_not_create_partial_onboarding_state(self):
        with tempfile.TemporaryDirectory() as tmp:
            home = Path(tmp)
            env = os.environ | {"HOME": str(home)}
            proc = subprocess.run(
                [NODE, str(REPO / "bin" / "parable.js"), "install"],
                cwd=home,
                env=env,
                capture_output=True,
                text=True,
                timeout=60,
            )
            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertTrue((home / ".claude" / "skills" / "parable" / "SKILL.md").is_file())
            self.assertFalse((home / ".config" / "parable").exists())
            self.assertIn("parable setup", proc.stdout)

    def test_source_installer_enters_the_same_skill_bootstrap(self):
        with tempfile.TemporaryDirectory() as tmp:
            home = Path(tmp)
            proxy = self.make_auth_proxy(home / "fake-proxy")
            proc = subprocess.run(
                [
                    "bash", str(REPO / "install.sh"),
                    "--non-interactive", "--vendors", "claude",
                    "--proxy-bin", str(proxy),
                ],
                cwd=home,
                env=os.environ | {"HOME": str(home), "SHELL": "/bin/bash"},
                capture_output=True,
                text=True,
                timeout=60,
            )
            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertTrue((home / ".claude" / "skills" / "parable" / "SKILL.md").is_file())
            self.assertTrue((home / ".config" / "parable" / "parable.toml").is_file())
            self.assertTrue((home / ".local" / "bin" / "parable").is_symlink())
            self.assertIn("In a new terminal", proc.stdout)


class TestClaudeAgentModelGuard(unittest.TestCase):
    def run_guard(self, payload: object,
                  state: object | None = None
                  ) -> subprocess.CompletedProcess:
        guard = (
            REPO / "skills" / "parable" / "runtime" / "welcome-plugin"
            / "scripts" / "model_guard.py"
        )
        env = dict(os.environ)
        env.pop("PARABLE_AGENT_STATE_JSON", None)
        if state is not None:
            env["PARABLE_AGENT_STATE_JSON"] = json.dumps(state)
        return subprocess.run(
            ["python3", str(guard)],
            input=json.dumps(payload),
            env=env,
            capture_output=True,
            text=True,
            timeout=10,
        )

    def test_generated_agent_override_is_removed_without_losing_other_input(self):
        payload = {
            "hook_event_name": "PreToolUse",
            "tool_name": "Agent",
            "tool_input": {
                "description": "Plan PoA visual profile",
                "prompt": "Inspect the visual profile.",
                "subagent_type": "parable-kimi",
                "model": "haiku",
                "run_in_background": True,
            },
        }
        result = self.run_guard(payload)
        self.assertEqual(result.returncode, 0, result.stderr)
        decision = json.loads(result.stdout)["hookSpecificOutput"]
        self.assertEqual(decision["hookEventName"], "PreToolUse")
        self.assertEqual(decision["permissionDecision"], "allow")
        self.assertEqual(
            decision["updatedInput"],
            {
                "description": "Plan PoA visual profile",
                "prompt": "Inspect the visual profile.",
                "subagent_type": "parable-kimi",
                "run_in_background": True,
            },
        )

    def test_unmanaged_or_already_exact_agent_input_is_untouched(self):
        for tool_input in (
            {"subagent_type": "Explore", "model": "haiku", "prompt": "look"},
            {"subagent_type": "parable-kimi", "prompt": "build"},
        ):
            with self.subTest(tool_input=tool_input):
                result = self.run_guard({
                    "hook_event_name": "PreToolUse",
                    "tool_name": "Agent",
                    "tool_input": tool_input,
                })
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(result.stdout, "")

    def test_session_snapshot_blocks_unavailable_managed_agent(self):
        payload = {
            "hook_event_name": "PreToolUse",
            "tool_name": "Agent",
            "tool_input": {
                "subagent_type": "parable-haiku-exact",
                "prompt": "inspect",
            },
        }
        result = self.run_guard(payload, state={
            "active": ["parable-kimi"],
            "unavailable": ["parable-haiku-exact"],
            "parent": [],
        })
        decision = json.loads(result.stdout)["hookSpecificOutput"]
        self.assertEqual(decision["permissionDecision"], "deny")
        self.assertIn("unavailable in this Parable session",
                      decision["permissionDecisionReason"])

    def test_active_session_agent_still_uses_frontmatter_model(self):
        payload = {
            "hook_event_name": "PreToolUse",
            "tool_name": "Agent",
            "tool_input": {
                "subagent_type": "parable-kimi",
                "model": "haiku",
                "prompt": "build",
            },
        }
        result = self.run_guard(payload, state={
            "active": ["parable-kimi"],
            "unavailable": [],
            "parent": [],
        })
        decision = json.loads(result.stdout)["hookSpecificOutput"]
        self.assertEqual(decision["permissionDecision"], "allow")
        self.assertNotIn("model", decision["updatedInput"])

    def test_malformed_session_snapshot_fails_closed_for_managed_agent(self):
        payload = {
            "hook_event_name": "PreToolUse",
            "tool_name": "Agent",
            "tool_input": {
                "subagent_type": "parable-kimi",
                "prompt": "build",
            },
        }
        result = self.run_guard(payload, state={"not": "the state schema"})
        decision = json.loads(result.stdout)["hookSpecificOutput"]
        self.assertEqual(decision["permissionDecision"], "deny")

    def test_parent_model_agent_is_blocked_with_accurate_reason(self):
        for agent in ("parable-sol-exact", "parable-grok"):
            with self.subTest(agent=agent):
                payload = {
                    "hook_event_name": "PreToolUse",
                    "tool_name": "Agent",
                    "tool_input": {
                        "subagent_type": agent,
                        "prompt": "delegate back",
                    },
                }
                result = self.run_guard(payload, state={
                    "active": [],
                    "unavailable": [],
                    "parent": [agent],
                })
                decision = json.loads(result.stdout)["hookSpecificOutput"]
                self.assertEqual(decision["permissionDecision"], "deny")
                self.assertIn("current parent model", decision["permissionDecisionReason"])


class TestClaudeContextRecoveryHook(unittest.TestCase):
    def run_hook(
        self,
        payload: object,
        target: Path,
        env_extra: dict[str, str] | None = None,
    ) -> subprocess.CompletedProcess:
        hook = (
            REPO / "skills" / "parable" / "runtime" / "welcome-plugin"
            / "scripts" / "context_recovery.py"
        )
        env = dict(os.environ)
        env["PARABLE_CONTEXT_RECOVERY_FILE"] = str(target)
        if env_extra:
            env.update(env_extra)
        result = subprocess.run(
            ["python3", str(hook)],
            input=json.dumps(payload),
            env=env,
            capture_output=True,
            text=True,
            timeout=10,
        )
        return result

    def test_main_context_failure_requests_private_exact_session_recovery(self):
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "request.json"
            result = self.run_hook({
                "hook_event_name": "StopFailure",
                "error": "invalid_request",
                "error_details": "400 Your input exceeds the context window of this model.",
                "last_assistant_message": "API Error: input exceeds the context window",
                "session_id": "12345678-1234-4234-9234-123456789abc",
            }, target)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(result.stdout, "")
            self.assertEqual(json.loads(target.read_text()), {
                "version": 1,
                "reason": "context_failure",
                "session_id": "12345678-1234-4234-9234-123456789abc",
            })
            self.assertEqual(target.stat().st_mode & 0o777, 0o600)

    def test_resume_picker_selection_requests_exact_session_preflight(self):
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "request.json"
            result = self.run_hook({
                "hook_event_name": "SessionStart",
                "source": "resume",
                "session_id": "12345678-1234-4234-9234-123456789abc",
            }, target, {"PARABLE_CONTEXT_RESUME_PICKER": "1"})
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(json.loads(target.read_text()), {
                "version": 1,
                "reason": "resume_picker",
                "session_id": "12345678-1234-4234-9234-123456789abc",
            })

    def test_ordinary_resume_session_start_does_not_request_restart(self):
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "request.json"
            result = self.run_hook({
                "hook_event_name": "SessionStart",
                "source": "resume",
                "session_id": "12345678-1234-4234-9234-123456789abc",
            }, target)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertFalse(target.exists())

    def test_unrelated_and_subagent_failures_do_not_request_restart(self):
        events = [
            {
                "hook_event_name": "StopFailure",
                "error": "server_error",
                "error_details": "503 overloaded",
                "session_id": "12345678-1234-4234-9234-123456789abc",
            },
            {
                "hook_event_name": "StopFailure",
                "error": "invalid_request",
                "error_details": "input exceeds the context window",
                "session_id": "12345678-1234-4234-9234-123456789abc",
                "agent_id": "agent-1",
            },
        ]
        for event in events:
            with self.subTest(event=event), tempfile.TemporaryDirectory() as tmp:
                target = Path(tmp) / "request.json"
                result = self.run_hook(event, target)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertFalse(target.exists())

    def write_transcript(self, target: Path, records: list[object]) -> None:
        target.write_text("".join(json.dumps(record) + "\n" for record in records))
        target.chmod(0o600)

    def notification_event(self, transcript: Path) -> dict[str, object]:
        return {
            "hook_event_name": "Notification",
            "notification_type": "idle_prompt",
            "session_id": "12345678-1234-4234-9234-123456789abc",
            "transcript_path": str(transcript),
        }

    def test_idle_after_automatic_teammate_interruption_requests_exact_resume(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            transcript = root / "session.jsonl"
            target = root / "request.json"
            self.write_transcript(transcript, [
                {
                    "type": "user",
                    "timestamp": "2026-08-02T21:50:37.659Z",
                    "message": {
                        "role": "user",
                        "content": [{"type": "text", "text": "[Request interrupted by user]"}],
                    },
                },
                {
                    "type": "user",
                    "timestamp": "2026-08-02T21:50:37.696Z",
                    "message": {
                        "role": "user",
                        "content": "Another Claude session sent a message:\n<teammate-message>done</teammate-message>",
                    },
                },
                {
                    "type": "assistant",
                    "timestamp": "2026-08-02T21:50:41.590Z",
                    "message": {
                        "role": "assistant",
                        "content": [{"type": "thinking", "thinking": "resume work"}],
                    },
                },
                {
                    "type": "user",
                    "timestamp": "2026-08-02T21:50:43.467Z",
                    "message": {
                        "role": "user",
                        "content": [{"type": "text", "text": "[Request interrupted by user]"}],
                    },
                },
                {"type": "system", "subtype": "away_summary"},
            ])
            result = self.run_hook(self.notification_event(transcript), target)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(json.loads(target.read_text()), {
                "version": 1,
                "reason": "teammate_interrupt",
                "session_id": "12345678-1234-4234-9234-123456789abc",
            })

    def test_resuming_stranded_teammate_turn_requests_immediate_reply_once(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            transcript = root / "session.jsonl"
            target = root / "request.json"
            self.write_transcript(transcript, [
                {
                    "type": "user",
                    "timestamp": "2026-08-02T21:50:37.659Z",
                    "message": {
                        "role": "user",
                        "content": "[Request interrupted by user]",
                    },
                },
                {
                    "type": "user",
                    "timestamp": "2026-08-02T21:50:37.696Z",
                    "message": {
                        "role": "user",
                        "content": "Another Claude session sent a message:\n<teammate-message />",
                    },
                },
            ])
            event = self.notification_event(transcript) | {
                "hook_event_name": "SessionStart",
                "source": "resume",
            }
            first = self.run_hook(event, target)
            self.assertEqual(first.returncode, 0, first.stderr)
            self.assertEqual(json.loads(target.read_text())["reason"], "teammate_interrupt")

            target.unlink()
            guarded = self.run_hook(
                event,
                target,
                {"PARABLE_TEAMMATE_RECOVERY_ACTIVE": "1"},
            )
            self.assertEqual(guarded.returncode, 0, guarded.stderr)
            self.assertFalse(target.exists())

    def test_idle_after_manual_interrupt_does_not_resume_against_user_intent(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            transcript = root / "session.jsonl"
            target = root / "request.json"
            self.write_transcript(transcript, [{
                "type": "user",
                "timestamp": "2026-08-02T21:50:37.659Z",
                "message": {
                    "role": "user",
                    "content": [{"type": "text", "text": "[Request interrupted by user]"}],
                },
            }])
            result = self.run_hook(self.notification_event(transcript), target)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertFalse(target.exists())

    def test_mixed_timezone_timestamps_fail_closed(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            transcript = root / "session.jsonl"
            target = root / "request.json"
            self.write_transcript(transcript, [
                {
                    "type": "user",
                    "timestamp": "2026-08-02T21:50:37.659",
                    "message": {
                        "role": "user",
                        "content": "[Request interrupted by user]",
                    },
                },
                {
                    "type": "user",
                    "timestamp": "2026-08-02T21:50:37.696Z",
                    "message": {
                        "role": "user",
                        "content": "Another Claude session sent a message:\n<teammate-message />",
                    },
                },
            ])
            result = self.run_hook(self.notification_event(transcript), target)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertFalse(target.exists())

    def test_later_user_prompt_or_assistant_progress_suppresses_teammate_recovery(self):
        endings = [
            {
                "type": "user",
                "message": {"role": "user", "content": "stop; I want to change direction"},
            },
            {
                "type": "assistant",
                "message": {
                    "role": "assistant",
                    "content": [{"type": "tool_use", "name": "Bash", "input": {}}],
                },
            },
        ]
        for ending in endings:
            with self.subTest(ending=ending), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                transcript = root / "session.jsonl"
                target = root / "request.json"
                self.write_transcript(transcript, [
                    {
                        "type": "user",
                        "timestamp": "2026-08-02T21:50:37.659Z",
                        "message": {
                            "role": "user",
                            "content": "[Request interrupted by user]",
                        },
                    },
                    {
                        "type": "user",
                        "timestamp": "2026-08-02T21:50:37.696Z",
                        "message": {
                            "role": "user",
                            "content": "Another Claude session sent a message:\n<teammate-message />",
                        },
                    },
                    ending,
                ])
                result = self.run_hook(self.notification_event(transcript), target)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertFalse(target.exists())


class TestFirstRunSetup(unittest.TestCase):
    def make_proxy(self, root: Path, name: str = "proxy") -> Path:
        target = root / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text("#!/usr/bin/env sh\nexit 0\n")
        target.chmod(0o755)
        return target

    def run_cli(
        self,
        home: Path,
        *args: str,
        env_extra: dict[str, str] | None = None,
        input_text: str | None = None,
        cli: Path | None = None,
    ) -> subprocess.CompletedProcess:
        env = os.environ | {"HOME": str(home)}
        env.pop("PARABLE_CLIPROXY_BIN", None)
        env.pop("XDG_DATA_HOME", None)
        if env_extra:
            env.update(env_extra)
        return subprocess.run(
            [NODE, str(cli or (REPO / "bin" / "parable.js")), *args],
            cwd=home,
            env=env,
            input=input_text,
            capture_output=True,
            text=True,
            timeout=60,
        )

    def assert_private(self, target: Path, mode: int) -> None:
        self.assertEqual(target.stat().st_mode & 0o777, mode, target)
        self.assertFalse(target.is_symlink(), target)

    def test_claude_baseline_setup_is_private_token_safe_valid_and_idempotent(self):
        with tempfile.TemporaryDirectory() as tmp:
            home = Path(tmp)
            proxy = self.make_proxy(home / "tools", "custom-proxy")
            proc = self.run_cli(
                home,
                "setup",
                "--non-interactive",
                "--vendors", "claude",
                "--proxy-bin", str(proxy),
                "--no-auth",
            )
            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            config_dir = home / ".config" / "parable"
            auth_dir = home / ".cli-proxy-api"
            self.assert_private(config_dir, 0o700)
            self.assert_private(auth_dir, 0o700)
            names = ("cliproxy.yaml", "cliproxy.env", "parable.toml", "setup.json")
            files = [config_dir / name for name in names]
            for target in files:
                self.assert_private(target, 0o600)

            env_text = (config_dir / "cliproxy.env").read_text()
            prefix = "export CLIPROXY_API_KEY='"
            self.assertTrue(env_text.startswith(prefix))
            token = env_text[len(prefix):-2]
            self.assertEqual(len(token), 64)
            self.assertTrue(all(character in "0123456789abcdef" for character in token))
            self.assertNotIn(token, proc.stdout + proc.stderr)

            yaml = (config_dir / "cliproxy.yaml").read_text()
            self.assertIn('host: "127.0.0.1"', yaml)
            self.assertIn("port: 8317", yaml)
            self.assertIn(f'auth-dir: "{auth_dir}"', yaml)
            self.assertIn(token, yaml)
            self.assertIn("transient-error-cooldown-seconds: -1\n", yaml)
            self.assertIn("claude-code:\n  disable-cloaking-model-list: true\n", yaml)
            config = (config_dir / "parable.toml").read_text()
            self.assertIn('brain_model = "claude-fable-5"', config)
            for present in (
                "claude-fable-5",
                "claude-sonnet-5",
                "claude-opus-4-8",
                "claude-haiku-4-5-20251001",
            ):
                self.assertIn(present, config)
            for absent in (
                "grok-4.6",
                "gpt-5.6-sol",
                "gpt-5.6-terra",
                "gpt-5.6-luna",
                "kimi",
            ):
                self.assertNotIn(absent, config)
            self.assertNotIn(token, config)
            manifest_text = (config_dir / "setup.json").read_text()
            manifest = json.loads(manifest_text)
            self.assertEqual(manifest["vendors"], ["claude"])
            self.assertEqual(manifest["proxyBinary"], str(proxy.resolve()))
            self.assertNotIn(token, manifest_text)

            before = {
                target: (target.read_bytes(), target.stat().st_mtime_ns)
                for target in files
            }
            again = self.run_cli(
                home,
                "setup",
                "--non-interactive",
                "--vendors", "claude",
                "--no-auth",
            )
            self.assertEqual(again.returncode, 0, again.stdout + again.stderr)
            self.assertIn("valid and unchanged", again.stdout)
            self.assertNotIn(token, again.stdout + again.stderr)
            for target in files:
                self.assertEqual(
                    (target.read_bytes(), target.stat().st_mtime_ns),
                    before[target],
                )

    def test_setup_migrates_only_the_previous_generated_proxy_config(self):
        for oldest in (False, True):
            with self.subTest(oldest=oldest), tempfile.TemporaryDirectory() as tmp:
                home = Path(tmp)
                proxy = self.make_proxy(home / "tools")
                first = self.run_cli(
                    home,
                    "setup", "--non-interactive", "--vendors", "claude",
                    "--proxy-bin", str(proxy), "--no-auth",
                )
                self.assertEqual(first.returncode, 0, first.stdout + first.stderr)
                config = home / ".config" / "parable" / "cliproxy.yaml"
                legacy = config.read_text().replace(
                    "transient-error-cooldown-seconds: -1\n",
                    "",
                )
                if oldest:
                    legacy = legacy.replace(
                        "claude-code:\n  disable-cloaking-model-list: true\n",
                        "",
                    )
                config.write_text(legacy)
                config.chmod(0o600)

                migrated = self.run_cli(
                    home,
                    "setup", "--non-interactive", "--vendors", "claude", "--no-auth",
                )
                self.assertEqual(migrated.returncode, 0, migrated.stdout + migrated.stderr)
                self.assertIn("updated generated proxy compatibility", migrated.stdout)
                self.assertIn(
                    "transient-error-cooldown-seconds: -1\n",
                    config.read_text(),
                )
                self.assertIn(
                    "claude-code:\n  disable-cloaking-model-list: true\n",
                    config.read_text(),
                )

    def test_setup_migrates_only_the_previous_generated_grok_config(self):
        with tempfile.TemporaryDirectory() as tmp:
            home = Path(tmp)
            proxy = self.make_proxy(home / "tools")
            first = self.run_cli(
                home,
                "setup", "--non-interactive", "--vendors", "claude,xai",
                "--proxy-bin", str(proxy), "--no-auth",
            )
            self.assertEqual(first.returncode, 0, first.stdout + first.stderr)
            config_dir = home / ".config" / "parable"
            config = config_dir / "parable.toml"
            previous = config.read_text()
            replacements = (
                ('model = "grok-4.6"', 'model = "grok-4.5"'),
                (
                    'tags = ["architect", "implementer", "systems", "third-family", "subscription"]',
                    'tags = ["implementer", "systems", "third-family", "subscription"]',
                ),
                (
                    'use_for = "Coding, agentic implementation, systems work, architectural second opinions, parent orchestration, and cross-family smoke testing."',
                    'use_for = "Bounded terminal-heavy or systems implementation, especially Rust or C++, plus cross-family smoke testing."',
                ),
                (
                    'avoid_for = "Delegation from a Grok parent, routine mechanical work, or reviewing its own diff."',
                    'avoid_for = "Sole final factual review, orchestration, ambiguous product architecture, or reviewing its own diff."',
                ),
                (
                    'architecture = ["fable_exact","opus_exact","grok"]',
                    'architecture = ["fable_exact","opus_exact"]',
                ),
            )
            for current, old in replacements:
                self.assertIn(current, previous)
                previous = previous.replace(current, old)
            config.write_text(previous)
            config.chmod(0o600)
            untouched = {
                target: (target.read_bytes(), target.stat().st_mtime_ns)
                for target in (
                    config_dir / "cliproxy.yaml",
                    config_dir / "cliproxy.env",
                    config_dir / "setup.json",
                )
            }

            migrated = self.run_cli(
                home,
                "setup", "--non-interactive", "--vendors", "claude,xai", "--no-auth",
            )
            self.assertEqual(migrated.returncode, 0, migrated.stdout + migrated.stderr)
            self.assertIn("upgraded generated Grok model", migrated.stdout)
            self.assertIn('model = "grok-4.6"', config.read_text())
            self.assertNotIn('model = "grok-4.5"', config.read_text())
            self.assertEqual(config.stat().st_mode & 0o777, 0o600)
            for target, snapshot in untouched.items():
                self.assertEqual((target.read_bytes(), target.stat().st_mtime_ns), snapshot)

            config.write_text(previous + "# user edit\n")
            config.chmod(0o600)
            refused = self.run_cli(
                home,
                "setup", "--non-interactive", "--vendors", "claude,xai", "--no-auth",
            )
            self.assertNotEqual(refused.returncode, 0)
            self.assertIn("refusing to overwrite", refused.stderr)
            self.assertEqual(config.read_text(), previous + "# user edit\n")

    def test_interactive_and_all_vendor_configs_use_exact_models(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            proxy = self.make_proxy(root / "tools")
            interactive_home = root / "interactive"
            interactive_home.mkdir()
            interactive = self.run_cli(
                interactive_home,
                "setup", "--proxy-bin", str(proxy), "--no-auth",
                input_text="n\nn\nn\n",
            )
            self.assertEqual(interactive.returncode, 0, interactive.stdout + interactive.stderr)
            self.assertIn("Add Kimi Code subscription (Kimi K3)?", interactive.stdout)
            interactive_manifest = json.loads(
                (interactive_home / ".config" / "parable" / "setup.json").read_text()
            )
            self.assertEqual(interactive_manifest["vendors"], ["claude"])

            interactive_kimi_home = root / "interactive-kimi"
            interactive_kimi_home.mkdir()
            interactive_kimi = self.run_cli(
                interactive_kimi_home,
                "setup", "--proxy-bin", str(proxy), "--no-auth",
                input_text="n\nn\ny\n",
            )
            self.assertEqual(
                interactive_kimi.returncode, 0, interactive_kimi.stdout + interactive_kimi.stderr,
            )
            interactive_kimi_manifest = json.loads(
                (interactive_kimi_home / ".config" / "parable" / "setup.json").read_text()
            )
            self.assertEqual(interactive_kimi_manifest["vendors"], ["claude", "kimi"])

            claude_xai_home = root / "claude-xai"
            claude_xai_home.mkdir()
            claude_xai = self.run_cli(
                claude_xai_home,
                "setup", "--non-interactive", "--vendors", "xai,claude",
                "--proxy-bin", str(proxy), "--no-auth",
            )
            self.assertEqual(claude_xai.returncode, 0, claude_xai.stdout + claude_xai.stderr)
            claude_xai_manifest = json.loads(
                (claude_xai_home / ".config" / "parable" / "setup.json").read_text()
            )
            self.assertEqual(claude_xai_manifest["vendors"], ["claude", "xai"])
            claude_xai_config = (
                claude_xai_home / ".config" / "parable" / "parable.toml"
            ).read_text()
            self.assertIn('brain_model = "claude-fable-5"', claude_xai_config)
            self.assertIn('model = "grok-4.6"', claude_xai_config)
            self.assertNotIn("gpt-5.6-", claude_xai_config)

            all_home = root / "all"
            all_home.mkdir()
            all_vendors = self.run_cli(
                all_home,
                "setup", "--non-interactive",
                "--vendors", "xai,chatgpt,claude,kimi",
                "--proxy-bin", str(proxy), "--port", "9123", "--no-auth",
            )
            self.assertEqual(all_vendors.returncode, 0, all_vendors.stdout + all_vendors.stderr)
            config_dir = all_home / ".config" / "parable"
            manifest = json.loads((config_dir / "setup.json").read_text())
            self.assertEqual(manifest["vendors"], ["claude", "chatgpt", "xai", "kimi"])
            self.assertEqual(manifest["port"], 9123)
            config = (config_dir / "parable.toml").read_text()
            for model in (
                "gpt-5.6-sol",
                "gpt-5.6-terra",
                "gpt-5.6-luna",
                "claude-fable-5",
                "claude-sonnet-5",
                "claude-opus-4-8",
                "claude-haiku-4-5-20251001",
                "grok-4.6",
                "kimi-k3",
            ):
                self.assertIn(model, config)
            self.assertIn('effort = "xhigh"', config)
            self.assertIn('effort = "medium"', config)
            self.assertIn('effort = "low"', config)
            self.assertIn(
                'frontend=["terra","sol_exact","sonnet_exact"]',
                config.replace(" ", ""),
            )
            self.assertIn(
                'architecture=["fable_exact","opus_exact","sol_exact","grok"]',
                config.replace(" ", ""),
            )
            self.assertNotIn("kimi", config.replace(" ", "").split("architecture=")[1].split("]")[0])
            self.assertNotIn("kimi", claude_xai_config.lower())

    def test_setup_rejects_selection_binary_and_unsafe_state_without_writes(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            empty_path = root / "empty-path"
            empty_path.mkdir()
            missing_home = root / "missing"
            missing_home.mkdir()
            missing = self.run_cli(
                missing_home,
                "setup", "--non-interactive", "--vendors", "claude", "--no-auth",
                env_extra={"PATH": str(empty_path)},
            )
            self.assertNotEqual(missing.returncode, 0)
            self.assertIn("CLIProxyAPI was not found", missing.stderr)
            self.assertFalse((missing_home / ".config" / "parable").exists())
            self.assertFalse((missing_home / ".cli-proxy-api").exists())

            proxy = self.make_proxy(root / "tools")
            for vendors, message in (("chatgpt", "must include claude"),
                                     ("claude,glm", "unsupported vendor")):
                home = root / vendors.replace(",", "-")
                home.mkdir()
                rejected = self.run_cli(
                    home,
                    "setup", "--non-interactive", "--vendors", vendors,
                    "--proxy-bin", str(proxy), "--no-auth",
                )
                self.assertNotEqual(rejected.returncode, 0)
                self.assertIn(message, rejected.stderr)
                self.assertFalse((home / ".config" / "parable").exists())

            no_vendors_home = root / "no-vendors"
            no_vendors_home.mkdir()
            no_vendors = self.run_cli(
                no_vendors_home,
                "setup", "--non-interactive", "--proxy-bin", str(proxy), "--no-auth",
            )
            self.assertNotEqual(no_vendors.returncode, 0)
            self.assertIn("requires --vendors", no_vendors.stderr)
            self.assertFalse((no_vendors_home / ".config" / "parable").exists())

            partial_home = root / "partial"
            config_dir = partial_home / ".config" / "parable"
            config_dir.mkdir(parents=True, mode=0o700)
            config_dir.chmod(0o700)
            outside = partial_home / "outside"
            outside.write_text("do not touch")
            (config_dir / "cliproxy.yaml").symlink_to(outside)
            partial = self.run_cli(
                partial_home,
                "setup", "--non-interactive", "--vendors", "claude",
                "--proxy-bin", str(proxy), "--no-auth",
            )
            self.assertNotEqual(partial.returncode, 0)
            self.assertIn("partial setup state", partial.stderr)
            self.assertEqual(outside.read_text(), "do not touch")

    def test_setup_refuses_mode_content_and_selection_drift(self):
        with tempfile.TemporaryDirectory() as tmp:
            home = Path(tmp)
            proxy = self.make_proxy(home / "tools")
            created = self.run_cli(
                home,
                "setup", "--non-interactive", "--vendors", "claude",
                "--proxy-bin", str(proxy), "--no-auth",
            )
            self.assertEqual(created.returncode, 0, created.stdout + created.stderr)
            config_dir = home / ".config" / "parable"
            env_file = config_dir / "cliproxy.env"
            original = env_file.read_bytes()
            env_file.chmod(0o644)
            bad_mode = self.run_cli(
                home,
                "setup", "--non-interactive", "--vendors", "claude", "--no-auth",
            )
            self.assertNotEqual(bad_mode.returncode, 0)
            self.assertIn("mode 0600", bad_mode.stderr)
            self.assertEqual(env_file.read_bytes(), original)
            env_file.chmod(0o600)

            drift = self.run_cli(
                home,
                "setup", "--non-interactive", "--vendors", "claude,xai",
                "--port", "9000", "--no-auth",
            )
            self.assertNotEqual(drift.returncode, 0)
            self.assertIn("does not match", drift.stderr)
            self.assertEqual(env_file.read_bytes(), original)

            env_file.write_text(f"export CLIPROXY_API_KEY='{'0' * 64}'\n")
            env_file.chmod(0o600)
            changed = env_file.read_bytes()
            content_drift = self.run_cli(
                home,
                "setup", "--non-interactive", "--vendors", "claude", "--no-auth",
            )
            self.assertNotEqual(content_drift.returncode, 0)
            self.assertIn("generated setup file has changed", content_drift.stderr)
            self.assertEqual(env_file.read_bytes(), changed)

    def test_proxy_discovery_precedence_is_explicit_then_env_then_path(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            bindir = root / "bin"
            path_first = self.make_proxy(bindir, "parable-cliproxy-api")
            self.make_proxy(bindir, "cli-proxy-api")
            env_proxy = self.make_proxy(root / "env", "env-proxy")
            explicit = self.make_proxy(root / "explicit", "explicit-proxy")
            common_env = {
                "PATH": f"{bindir}:{os.environ['PATH']}",
                "PARABLE_CLIPROXY_BIN": str(env_proxy),
            }

            explicit_home = root / "explicit-home"
            explicit_home.mkdir()
            result = self.run_cli(
                explicit_home,
                "setup", "--non-interactive", "--vendors", "claude",
                "--proxy-bin", str(explicit), "--no-auth",
                env_extra=common_env,
            )
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            manifest = json.loads(
                (explicit_home / ".config" / "parable" / "setup.json").read_text()
            )
            self.assertEqual(manifest["proxyBinary"], str(explicit.resolve()))

            env_home = root / "env-home"
            env_home.mkdir()
            result = self.run_cli(
                env_home,
                "setup", "--non-interactive", "--vendors", "claude", "--no-auth",
                env_extra=common_env,
            )
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            manifest = json.loads((env_home / ".config" / "parable" / "setup.json").read_text())
            self.assertEqual(manifest["proxyBinary"], str(env_proxy.resolve()))

            path_home = root / "path-home"
            path_home.mkdir()
            result = self.run_cli(
                path_home,
                "setup", "--non-interactive", "--vendors", "claude", "--no-auth",
                env_extra={"PATH": f"{bindir}:{os.environ['PATH']}"},
            )
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            manifest = json.loads((path_home / ".config" / "parable" / "setup.json").read_text())
            self.assertEqual(manifest["proxyBinary"], str(path_first.resolve()))

    def test_add_vendors_extends_existing_setup_in_place(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            home = root / "home"
            home.mkdir()
            proxy = self.make_proxy(root / "tools")

            base = self.run_cli(
                home,
                "setup", "--non-interactive", "--vendors", "claude,xai",
                "--proxy-bin", str(proxy), "--no-auth",
            )
            self.assertEqual(base.returncode, 0, base.stdout + base.stderr)
            setup_json = home / ".config" / "parable" / "setup.json"
            parable_toml = home / ".config" / "parable" / "parable.toml"
            cliproxy_yaml = home / ".config" / "parable" / "cliproxy.yaml"
            cliproxy_env = home / ".config" / "parable" / "cliproxy.env"

            # An existing OAuth credential record must survive an additive upgrade byte-
            # for-byte and untouched on disk (add-vendors never writes to the auth dir).
            auth_dir = home / ".cli-proxy-api"
            auth_dir.mkdir(parents=True, exist_ok=True)
            auth_record = auth_dir / "existing-claude.json"
            auth_record.write_text('{"type":"claude","refresh_token":"SECRET-CLAUDE"}\n')
            auth_record.chmod(0o600)

            watched = {
                "cliproxy.yaml": cliproxy_yaml,
                "cliproxy.env": cliproxy_env,
                "proxy binary": proxy,
                "auth record": auth_record,
            }
            before_bytes = {name: target.read_bytes() for name, target in watched.items()}
            before_stat = {name: target.stat() for name, target in watched.items()}
            # Force the filesystem mtime clock forward so an accidental rewrite (even one
            # that reproduces identical bytes) would be caught by an mtime comparison.
            time.sleep(1.1)

            added = self.run_cli(home, "setup", "--add-vendors", "kimi", "--no-auth")
            self.assertEqual(added.returncode, 0, added.stdout + added.stderr)
            self.assertIn("added vendors: kimi", added.stdout)
            manifest = json.loads(setup_json.read_text())
            self.assertEqual(manifest["vendors"], ["claude", "xai", "kimi"])
            self.assertIn("kimi-k3", parable_toml.read_text())

            # cliproxy yaml/env, the proxy binary, and the auth record must be untouched
            # by an additive upgrade -- verified by both content bytes and mtime/inode
            # metadata, not just a text-content comparison.
            for name, target in watched.items():
                self.assertEqual(target.read_bytes(), before_bytes[name], name)
                after_stat = target.stat()
                self.assertEqual(after_stat.st_mtime_ns, before_stat[name].st_mtime_ns, name)
                self.assertEqual(after_stat.st_ino, before_stat[name].st_ino, name)

            # Idempotent no-op: re-running with the same (now-already-included) vendor changes nothing.
            noop = self.run_cli(home, "setup", "--add-vendors", "kimi", "--no-auth")
            self.assertEqual(noop.returncode, 0, noop.stdout + noop.stderr)
            self.assertIn("no-op", noop.stdout)
            self.assertEqual(
                json.loads(setup_json.read_text())["vendors"], ["claude", "xai", "kimi"]
            )
            for name, target in watched.items():
                self.assertEqual(target.read_bytes(), before_bytes[name], name)

            # After the additive upgrade, plain `--no-auth`-less runs only need to authorize
            # providers that are actually still missing -- covered functionally by the
            # dedicated skip-present auth test in TestVendorAuthAndProxyLifecycle.

    def test_add_vendors_rejects_conflicting_flags_and_missing_setup(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            proxy = self.make_proxy(root / "tools")

            missing_home = root / "missing"
            missing_home.mkdir()
            missing = self.run_cli(missing_home, "setup", "--add-vendors", "kimi", "--no-auth")
            self.assertNotEqual(missing.returncode, 0)
            self.assertIn("existing complete setup", missing.stdout + missing.stderr)

            home = root / "home"
            home.mkdir()
            base = self.run_cli(
                home,
                "setup", "--non-interactive", "--vendors", "claude",
                "--proxy-bin", str(proxy), "--no-auth",
            )
            self.assertEqual(base.returncode, 0, base.stdout + base.stderr)

            conflicts = (
                (("--vendors", "claude,kimi"), "--vendors"),
                (("--port", "9999"), "--port"),
                (("--proxy-bin", str(proxy)), "--proxy-bin"),
                (("--build-proxy",), "--build-proxy"),
            )
            for extra_args, flag in conflicts:
                proc = self.run_cli(
                    home, "setup", "--add-vendors", "kimi", "--no-auth", *extra_args
                )
                self.assertNotEqual(proc.returncode, 0, f"{flag} should have been rejected")
                self.assertIn(flag, proc.stdout + proc.stderr)
            manifest = json.loads(
                (home / ".config" / "parable" / "setup.json").read_text()
            )
            self.assertEqual(manifest["vendors"], ["claude"])

    def test_add_vendors_honors_config_dir_override(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            home = root / "home"
            home.mkdir()
            proxy = self.make_proxy(root / "tools")
            config_dir = root / "custom-config"

            base = self.run_cli(
                home,
                "setup", "--non-interactive", "--vendors", "claude",
                "--proxy-bin", str(proxy), "--config-dir", str(config_dir), "--no-auth",
            )
            self.assertEqual(base.returncode, 0, base.stdout + base.stderr)
            self.assertTrue((config_dir / "setup.json").exists())

            added = self.run_cli(
                home, "setup", "--add-vendors", "kimi", "--config-dir", str(config_dir), "--no-auth",
            )
            self.assertEqual(added.returncode, 0, added.stdout + added.stderr)
            manifest = json.loads((config_dir / "setup.json").read_text())
            self.assertEqual(manifest["vendors"], ["claude", "kimi"])
            self.assertIn("kimi-k3", (config_dir / "parable.toml").read_text())

    def test_add_vendors_rejects_drift_and_recovers_from_stale_half_state(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            proxy = self.make_proxy(root / "tools")

            def fresh_claude_home(name: str) -> Path:
                home = root / name
                home.mkdir()
                base = self.run_cli(
                    home,
                    "setup", "--non-interactive", "--vendors", "claude",
                    "--proxy-bin", str(proxy), "--no-auth",
                )
                self.assertEqual(base.returncode, 0, base.stdout + base.stderr)
                return home

            # --- Arbitrary drift (not a canonical old/new pair) must still fail closed. ---
            drift_home = fresh_claude_home("drift")
            parable_toml = drift_home / ".config" / "parable" / "parable.toml"
            tampered = parable_toml.read_text() + "\n# tampered\n"
            parable_toml.write_text(tampered)
            drifted = self.run_cli(drift_home, "setup", "--add-vendors", "kimi", "--no-auth")
            self.assertNotEqual(drifted.returncode, 0)
            self.assertIn("refusing to overwrite", drifted.stdout + drifted.stderr)
            # Nothing should have been rewritten on top of the drifted content.
            self.assertEqual(parable_toml.read_text(), tampered)

            # --- Arbitrary garbage ".next" temp files (not real crash artifacts) must
            # also fail closed rather than being treated as recoverable state. ---
            garbage_home = fresh_claude_home("garbage")
            garbage_config_dir = garbage_home / ".config" / "parable"
            (garbage_config_dir / ".parable.toml.next").write_text("stale-half-write")
            (garbage_config_dir / ".setup.json.next").write_text("stale-half-write")
            garbage = self.run_cli(garbage_home, "setup", "--add-vendors", "kimi", "--no-auth")
            self.assertNotEqual(garbage.returncode, 0)
            self.assertEqual(
                json.loads((garbage_config_dir / "setup.json").read_text())["vendors"], ["claude"],
            )

            # Produce the real canonical NEW file contents by running a genuine, complete
            # --add-vendors upgrade once, so the half-state fixtures below use byte-exact
            # old/new content instead of hand-authored approximations.
            golden_home = fresh_claude_home("golden")
            golden_config_dir = golden_home / ".config" / "parable"
            old_toml = (golden_config_dir / "parable.toml").read_text()
            old_manifest = (golden_config_dir / "setup.json").read_text()
            golden_added = self.run_cli(golden_home, "setup", "--add-vendors", "kimi", "--no-auth")
            self.assertEqual(golden_added.returncode, 0, golden_added.stdout + golden_added.stderr)
            new_toml = (golden_config_dir / "parable.toml").read_text()
            golden_new_manifest = (golden_config_dir / "setup.json").read_text()
            self.assertNotEqual(old_toml, new_toml)
            self.assertNotEqual(old_manifest, golden_new_manifest)

            def new_manifest_for(config_dir: Path) -> str:
                manifest = json.loads((config_dir / "setup.json").read_text())
                manifest["vendors"] = ["claude", "kimi"]
                return json.dumps(manifest, indent=2) + "\n"

            # --- (a) rename completed for parable.toml (new) but not yet for setup.json
            # (old, with its ".next" sibling still holding the new manifest content). ---
            case_a_home = fresh_claude_home("case-a")
            case_a_config_dir = case_a_home / ".config" / "parable"
            case_a_new_manifest = new_manifest_for(case_a_config_dir)
            (case_a_config_dir / "parable.toml").write_text(new_toml)
            (case_a_config_dir / ".setup.json.next").write_text(case_a_new_manifest)
            recovered_a = self.run_cli(case_a_home, "setup", "--add-vendors", "kimi", "--no-auth")
            self.assertEqual(recovered_a.returncode, 0, recovered_a.stdout + recovered_a.stderr)
            self.assertEqual((case_a_config_dir / "parable.toml").read_text(), new_toml)
            self.assertEqual((case_a_config_dir / "setup.json").read_text(), case_a_new_manifest)
            manifest_a = json.loads((case_a_config_dir / "setup.json").read_text())
            self.assertEqual(manifest_a["vendors"], ["claude", "kimi"])
            self.assertFalse((case_a_config_dir / ".parable.toml.next").exists())
            self.assertFalse((case_a_config_dir / ".setup.json.next").exists())

            # --- (b) rename completed for setup.json (new) but not yet for parable.toml
            # (old, with its ".next" sibling still holding the new toml content). ---
            case_b_home = fresh_claude_home("case-b")
            case_b_config_dir = case_b_home / ".config" / "parable"
            case_b_new_manifest = new_manifest_for(case_b_config_dir)
            (case_b_config_dir / "setup.json").write_text(case_b_new_manifest)
            (case_b_config_dir / ".parable.toml.next").write_text(new_toml)
            recovered_b = self.run_cli(case_b_home, "setup", "--add-vendors", "kimi", "--no-auth")
            self.assertEqual(recovered_b.returncode, 0, recovered_b.stdout + recovered_b.stderr)
            self.assertEqual((case_b_config_dir / "parable.toml").read_text(), new_toml)
            self.assertEqual((case_b_config_dir / "setup.json").read_text(), case_b_new_manifest)
            manifest_b = json.loads((case_b_config_dir / "setup.json").read_text())
            self.assertEqual(manifest_b["vendors"], ["claude", "kimi"])
            self.assertFalse((case_b_config_dir / ".parable.toml.next").exists())
            self.assertFalse((case_b_config_dir / ".setup.json.next").exists())

            # --- The old canonical ".next"-files-only shape (both targets still old,
            # both ".next" siblings hold the new content) must also still recover. ---
            recovery_home = fresh_claude_home("recovery")
            recovery_config_dir = recovery_home / ".config" / "parable"
            recovery_new_manifest = new_manifest_for(recovery_config_dir)
            (recovery_config_dir / ".parable.toml.next").write_text(new_toml)
            (recovery_config_dir / ".setup.json.next").write_text(recovery_new_manifest)
            recovered = self.run_cli(recovery_home, "setup", "--add-vendors", "kimi", "--no-auth")
            self.assertEqual(recovered.returncode, 0, recovered.stdout + recovered.stderr)
            manifest = json.loads((recovery_config_dir / "setup.json").read_text())
            self.assertEqual(manifest["vendors"], ["claude", "kimi"])
            self.assertFalse((recovery_config_dir / ".parable.toml.next").exists())
            self.assertFalse((recovery_config_dir / ".setup.json.next").exists())


class TestManagedProxyBuild(unittest.TestCase):
    def make_tools(self, root: Path) -> tuple[Path, Path, Path]:
        bindir = root / "fake-bin"
        bindir.mkdir()
        git_log = root / "git.jsonl"
        go_log = root / "go.jsonl"
        git = bindir / "git"
        git.write_text("""#!/usr/bin/env python3
import json
import os
import sys
from pathlib import Path
with open(os.environ["FAKE_GIT_LOG"], "a") as handle:
    handle.write(json.dumps(sys.argv[1:]) + "\\n")
args = sys.argv[1:]
if args and args[0] == "clone":
    Path(args[-1]).mkdir(parents=True)
if "rev-parse" in args:
    print(os.environ.get("FAKE_GIT_REVISION", ""))
""")
        git.chmod(0o755)
        go = bindir / "go"
        go.write_text("""#!/usr/bin/env python3
import json
import os
import sys
from pathlib import Path
with open(os.environ["FAKE_GO_LOG"], "a") as handle:
    handle.write(json.dumps(sys.argv[1:]) + "\\n")
args = sys.argv[1:]
if args and args[0] == "build":
    output = Path(args[args.index("-o") + 1])
    output.write_text("#!/usr/bin/env sh\\nexit 0\\n")
    output.chmod(0o755)
""")
        go.chmod(0o755)
        return bindir, git_log, go_log

    def build_env(
        self,
        home: Path,
        bindir: Path,
        git_log: Path,
        go_log: Path,
        revision: str = PROXY_COMMIT,
    ) -> dict[str, str]:
        return os.environ | {
            "HOME": str(home),
            "PATH": f"{bindir}:{os.environ['PATH']}",
            "FAKE_GIT_LOG": str(git_log),
            "FAKE_GO_LOG": str(go_log),
            "FAKE_GIT_REVISION": revision,
        }

    def test_proxy_build_pins_source_patch_tests_and_private_binary(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            bindir, git_log, go_log = self.make_tools(root)
            destination = root / "managed" / PROXY_COMMIT
            proc = subprocess.run(
                [NODE, str(REPO / "bin" / "parable.js"),
                 "proxy", "build", "--install-dir", str(destination)],
                cwd=root,
                env=self.build_env(root, bindir, git_log, go_log),
                capture_output=True,
                text=True,
                timeout=60,
            )
            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            binary = destination / "parable-cliproxy-api"
            self.assertTrue(binary.is_file())
            self.assertEqual(binary.stat().st_mode & 0o777, 0o700)
            git_calls = [json.loads(line) for line in git_log.read_text().splitlines()]
            go_calls = [json.loads(line) for line in go_log.read_text().splitlines()]
            self.assertEqual(
                git_calls[0],
                ["clone", "--no-checkout", "https://github.com/router-for-me/CLIProxyAPI.git",
                 str(destination)],
            )
            self.assertIn(["-C", str(destination), "checkout", "--detach", PROXY_COMMIT], git_calls)
            self.assertTrue(any("am" in call for call in git_calls))
            self.assertEqual([call[0] for call in go_calls], ["test", "test", "build"])

    def test_proxy_upgrade_stages_new_binary_without_replacing_credentials(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            home = root / "home"
            home.mkdir()
            with socket.socket() as listener:
                listener.bind(("127.0.0.1", 0))
                port = listener.getsockname()[1]
            data_home = root / "data"
            old_proxy = root / "old-proxy"
            old_proxy.write_text("#!/usr/bin/env sh\nexit 0\n")
            old_proxy.chmod(0o755)
            bindir, git_log, go_log = self.make_tools(root)
            env = self.build_env(home, bindir, git_log, go_log)
            env["XDG_DATA_HOME"] = str(data_home)
            setup = subprocess.run(
                [
                    NODE, str(REPO / "bin" / "parable.js"),
                    "setup", "--non-interactive", "--vendors", "claude",
                    "--proxy-bin", str(old_proxy), "--port", str(port), "--no-auth",
                ],
                cwd=home,
                env=env,
                capture_output=True,
                text=True,
                timeout=60,
            )
            self.assertEqual(setup.returncode, 0, setup.stdout + setup.stderr)
            config_dir = home / ".config" / "parable"
            token_before = (config_dir / "cliproxy.env").read_bytes()

            upgraded = subprocess.run(
                [NODE, str(REPO / "bin" / "parable.js"), "proxy", "upgrade"],
                cwd=home,
                env=env,
                capture_output=True,
                text=True,
                timeout=60,
            )
            self.assertEqual(upgraded.returncode, 0, upgraded.stdout + upgraded.stderr)
            expected = (
                data_home / "parable" / "cliproxyapi" / PROXY_COMMIT
                / "parable-cliproxy-api"
            )
            manifest = json.loads((config_dir / "setup.json").read_text())
            self.assertEqual(manifest["proxyBinary"], str(expected))
            self.assertTrue(expected.is_file())
            self.assertEqual((config_dir / "cliproxy.env").read_bytes(), token_before)
            self.assertIn(
                "claude-code:\n  disable-cloaking-model-list: true\n",
                (config_dir / "cliproxy.yaml").read_text(),
            )
            self.assertIn("upgrade active", upgraded.stdout)

            again = subprocess.run(
                [NODE, str(REPO / "bin" / "parable.js"), "proxy", "upgrade"],
                cwd=home,
                env=env,
                capture_output=True,
                text=True,
                timeout=60,
            )
            self.assertEqual(again.returncode, 0, again.stdout + again.stderr)
            self.assertIn("proxy is current", again.stdout)

    def test_interactive_setup_requires_consent_before_build_work(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            bindir, git_log, go_log = self.make_tools(root)
            home = root / "home"
            home.mkdir()
            empty_path = root / "empty-path"
            empty_path.mkdir()
            env = self.build_env(home, bindir, git_log, go_log)
            # Keep git/go discoverable while ensuring no proxy binary is on PATH.
            python_bin = Path(shutil.which("python3") or "/usr/bin/python3").parent
            env["PATH"] = f"{bindir}:{empty_path}:{python_bin}"
            proc = subprocess.run(
                [NODE, str(REPO / "bin" / "parable.js"), "setup", "--no-auth"],
                cwd=home,
                env=env,
                input="n\nn\nn\nn\n",
                capture_output=True,
                text=True,
                timeout=60,
            )
            self.assertNotEqual(proc.returncode, 0)
            self.assertIn("Build pinned commit", proc.stdout)
            self.assertIn("CLIProxyAPI was not found", proc.stderr)
            self.assertFalse(git_log.exists())
            self.assertFalse(go_log.exists())
            self.assertFalse((home / ".config" / "parable").exists())

    def test_interactive_setup_builds_without_flag_after_consent(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            bindir, git_log, go_log = self.make_tools(root)
            home = root / "home"
            home.mkdir()
            data_home = root / "data"
            env = self.build_env(home, bindir, git_log, go_log)
            env["XDG_DATA_HOME"] = str(data_home)
            python_bin = Path(shutil.which("python3") or "/usr/bin/python3").parent
            env["PATH"] = f"{bindir}:{python_bin}"
            proc = subprocess.run(
                [NODE, str(REPO / "bin" / "parable.js"), "setup", "--no-auth"],
                cwd=home,
                env=env,
                input="n\nn\nn\ny\n",
                capture_output=True,
                text=True,
                timeout=60,
            )
            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("Build pinned commit", proc.stdout)
            self.assertIn("next: authorize each selected subscription, then run parable", proc.stdout)
            manifest = json.loads((home / ".config" / "parable" / "setup.json").read_text())
            expected = data_home / "parable" / "cliproxyapi" / PROXY_COMMIT / "parable-cliproxy-api"
            self.assertEqual(manifest["proxyBinary"], str(expected))
            self.assertTrue(expected.is_file())

    def test_wrong_source_pin_and_existing_destination_stop_before_patch_or_go(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            bindir, git_log, go_log = self.make_tools(root)
            destination = root / "wrong-source"
            proc = subprocess.run(
                [NODE, str(REPO / "bin" / "parable.js"),
                 "proxy", "build", "--install-dir", str(destination)],
                cwd=root,
                env=self.build_env(root, bindir, git_log, go_log, "0" * 40),
                capture_output=True,
                text=True,
                timeout=60,
            )
            self.assertNotEqual(proc.returncode, 0)
            self.assertIn("source pin mismatch", proc.stderr)
            calls = [json.loads(line) for line in git_log.read_text().splitlines()]
            self.assertFalse(any("am" in call for call in calls))
            self.assertFalse(go_log.exists())
            self.assertFalse(destination.exists())

            git_log.unlink()
            destination.mkdir()
            marker = destination / "owned-by-user"
            marker.write_text("keep")
            proc = subprocess.run(
                [NODE, str(REPO / "bin" / "parable.js"),
                 "proxy", "build", "--install-dir", str(destination)],
                cwd=root,
                env=self.build_env(root, bindir, git_log, go_log),
                capture_output=True,
                text=True,
                timeout=60,
            )
            self.assertNotEqual(proc.returncode, 0)
            self.assertIn("already exists", proc.stderr)
            self.assertEqual(marker.read_text(), "keep")
            self.assertFalse(git_log.exists())
            self.assertFalse(go_log.exists())

    def test_wrong_patch_checksum_stops_before_git_and_setup_can_build_explicitly(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            bindir, git_log, go_log = self.make_tools(root)
            package = root / "mutated-package"
            for name in ("bin", "lib"):
                shutil.copytree(REPO / name, package / name)
            shutil.copytree(REPO / "skills", package / "skills")
            patch = (
                package / "skills" / "parable" / "runtime" / "patches"
                / "cliproxyapi-v7.2.131-claude-effort.patch"
            )
            patch.write_text(patch.read_text() + "\n# checksum mutation\n")
            destination = root / "checksum"
            proc = subprocess.run(
                [NODE, str(package / "bin" / "parable.js"),
                 "proxy", "build", "--install-dir", str(destination)],
                cwd=root,
                env=self.build_env(root, bindir, git_log, go_log),
                capture_output=True,
                text=True,
                timeout=60,
            )
            self.assertNotEqual(proc.returncode, 0)
            self.assertIn("checksum mismatch", proc.stderr)
            self.assertFalse(git_log.exists())
            self.assertFalse(go_log.exists())
            self.assertFalse(destination.exists())

            setup_home = root / "setup-home"
            setup_home.mkdir()
            data_home = root / "data"
            env = self.build_env(setup_home, bindir, git_log, go_log)
            env["XDG_DATA_HOME"] = str(data_home)
            setup = subprocess.run(
                [NODE, str(REPO / "bin" / "parable.js"),
                 "setup", "--non-interactive", "--vendors", "claude",
                 "--build-proxy", "--no-auth"],
                cwd=setup_home,
                env=env,
                capture_output=True,
                text=True,
                timeout=60,
            )
            self.assertEqual(setup.returncode, 0, setup.stdout + setup.stderr)
            manifest = json.loads(
                (setup_home / ".config" / "parable" / "setup.json").read_text()
            )
            expected = data_home / "parable" / "cliproxyapi" / PROXY_COMMIT / "parable-cliproxy-api"
            self.assertEqual(manifest["proxyBinary"], str(expected))
            self.assertTrue(expected.is_file())


class TestVendorAuthAndProxyLifecycle(unittest.TestCase):
    def make_proxy(self, root: Path) -> tuple[Path, Path]:
        root.mkdir(parents=True, exist_ok=True)
        proxy = root / "fake-proxy"
        capture = root / "calls.jsonl"
        proxy.write_text("""#!/usr/bin/env python3
import json
import os
from pathlib import Path
import sys
with open(os.environ["FAKE_PROXY_CAPTURE"], "a") as handle:
    handle.write(json.dumps(sys.argv[1:]) + "\\n")
mapping = {
    "--codex-login": ("chatgpt", "codex"),
    "--codex-device-login": ("chatgpt", "codex"),
    "--claude-login": ("claude", "claude"),
    "--xai-login": ("xai", "xai"),
    "--kimi-login": ("kimi", "kimi"),
}
for flag, (vendor, record_type) in mapping.items():
    if flag in sys.argv and os.environ.get("FAKE_PROXY_SKIP_AUTH") != vendor:
        if os.environ.get("FAKE_PROXY_UMASK_CAPTURE"):
            current_umask = os.umask(0)
            os.umask(current_umask)
            Path(os.environ["FAKE_PROXY_UMASK_CAPTURE"]).write_text(oct(current_umask))
        target = Path.home() / ".cli-proxy-api" / f"fake-{vendor}.json"
        target.write_text(json.dumps({"type": record_type}))
        target.chmod(int(os.environ.get("FAKE_PROXY_RECORD_MODE", "0600"), 8))
if os.environ.get("FAKE_PROXY_STDOUT"):
    print(os.environ["FAKE_PROXY_STDOUT"])
raise SystemExit(int(os.environ.get("FAKE_PROXY_EXIT", "0")))
""")
        proxy.chmod(0o755)
        return proxy, capture

    def run_cli(
        self,
        home: Path,
        proxy: Path,
        capture: Path,
        *args: str,
        extra_env: dict[str, str] | None = None,
    ) -> subprocess.CompletedProcess:
        env = os.environ | {
            "HOME": str(home),
            "FAKE_PROXY_CAPTURE": str(capture),
        }
        env.pop("PARABLE_CONFIG", None)
        env.pop("PARABLE_CLIPROXY_BIN", None)
        if extra_env:
            env.update(extra_env)
        return subprocess.run(
            [NODE, str(REPO / "bin" / "parable.js"), *args],
            cwd=home,
            env=env,
            capture_output=True,
            text=True,
            timeout=60,
        )

    def setup(
        self,
        home: Path,
        proxy: Path,
        capture: Path,
        vendors: str = "chatgpt,claude,xai",
    ) -> subprocess.CompletedProcess:
        return self.run_cli(
            home,
            proxy,
            capture,
            "setup", "--non-interactive", "--vendors", vendors,
            "--proxy-bin", str(proxy), "--no-auth",
        )

    def calls(self, capture: Path) -> list[list[str]]:
        if not capture.exists():
            return []
        return [json.loads(line) for line in capture.read_text().splitlines()]

    def test_auth_add_delegates_only_exact_native_flags_and_preserves_records(self):
        with tempfile.TemporaryDirectory() as tmp:
            home = Path(tmp)
            proxy, capture = self.make_proxy(home / "tools")
            setup = self.setup(home, proxy, capture)
            self.assertEqual(setup.returncode, 0, setup.stdout + setup.stderr)
            auth_dir = home / ".cli-proxy-api"
            existing = auth_dir / "existing.json"
            existing.write_text('{"type":"codex","access_token":"SECRET-KEEP"}\n')
            existing.chmod(0o600)
            before = existing.read_bytes()
            config = home / ".config" / "parable" / "cliproxy.yaml"

            cases = [
                (("auth", "add", "chatgpt"),
                 ["--config", str(config), "--codex-login"]),
                (("auth", "add", "chatgpt", "--device"),
                 ["--config", str(config), "--codex-device-login"]),
                (("auth", "add", "claude"),
                 ["--config", str(config), "--claude-login", "--no-browser"]),
                (("auth", "add", "xai"),
                 ["--config", str(config), "--xai-login", "--no-browser"]),
            ]
            for command, expected in cases:
                proc = self.run_cli(home, proxy, capture, *command)
                self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
                self.assertEqual(self.calls(capture)[-1], expected)
                self.assertEqual(existing.read_bytes(), before)
            self.assertIn("localhost:54545", self.run_cli(
                home, proxy, capture, "auth", "add", "claude"
            ).stdout)
            self.assertEqual(existing.read_bytes(), before)

    def test_setup_runs_selected_auth_additively_unless_no_auth(self):
        with tempfile.TemporaryDirectory() as tmp:
            home = Path(tmp)
            proxy, capture = self.make_proxy(home / "tools")
            proc = self.run_cli(
                home,
                proxy,
                capture,
                "setup", "--non-interactive", "--vendors", "chatgpt,claude,xai",
                "--proxy-bin", str(proxy),
            )
            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            config = home / ".config" / "parable" / "cliproxy.yaml"
            self.assertEqual(self.calls(capture), [
                ["--config", str(config), "--claude-login", "--no-browser"],
                ["--config", str(config), "--codex-login"],
                ["--config", str(config), "--xai-login", "--no-browser"],
            ])
            self.assertIn("authorization complete", proc.stdout)

            capture.write_text("")
            rerun = self.run_cli(
                home,
                proxy,
                capture,
                "setup", "--non-interactive", "--vendors", "chatgpt,claude,xai",
                "--proxy-bin", str(proxy),
            )
            self.assertEqual(rerun.returncode, 0, rerun.stdout + rerun.stderr)
            self.assertEqual(self.calls(capture), [])
            for vendor in ("claude", "chatgpt", "xai"):
                self.assertIn(f"{vendor}: already authorized", rerun.stdout)

    def test_auth_add_uses_private_umask_and_secures_proxy_record(self):
        with tempfile.TemporaryDirectory() as tmp:
            home = Path(tmp)
            proxy, capture = self.make_proxy(home / "tools")
            setup = self.setup(home, proxy, capture, vendors="claude")
            self.assertEqual(setup.returncode, 0, setup.stdout + setup.stderr)
            umask_capture = home / "child-umask"
            proc = self.run_cli(
                home, proxy, capture, "auth", "add", "claude",
                extra_env={
                    "FAKE_PROXY_RECORD_MODE": "0664",
                    "FAKE_PROXY_UMASK_CAPTURE": str(umask_capture),
                },
            )
            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertEqual(umask_capture.read_text(), "0o77")
            record = home / ".cli-proxy-api" / "fake-claude.json"
            self.assertEqual(stat.S_IMODE(record.stat().st_mode), 0o600)
            self.assertIn("claude: secured credential permissions", proc.stdout)

    def test_auth_login_walks_only_selected_missing_vendors_and_hands_off_once(self):
        with tempfile.TemporaryDirectory() as tmp:
            home = Path(tmp)
            proxy, capture = self.make_proxy(home / "tools")
            setup = self.setup(home, proxy, capture)
            self.assertEqual(setup.returncode, 0, setup.stdout + setup.stderr)
            claude_record = home / ".cli-proxy-api" / "existing-claude.json"
            claude_record.write_text('{"type":"claude"}')
            claude_record.chmod(0o664)
            unrelated = home / ".cli-proxy-api" / "unrelated.json"
            unrelated.write_text('{"type":"kimi"}')
            unrelated.chmod(0o664)
            proc = self.run_cli(home, proxy, capture, "auth", "login")
            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            config = home / ".config" / "parable" / "cliproxy.yaml"
            self.assertEqual(self.calls(capture), [
                ["--config", str(config), "--codex-login"],
                ["--config", str(config), "--xai-login", "--no-browser"],
            ])
            self.assertEqual(stat.S_IMODE(claude_record.stat().st_mode), 0o600)
            self.assertEqual(stat.S_IMODE(unrelated.stat().st_mode), 0o664)
            self.assertIn("claude: secured credential permissions", proc.stdout)
            self.assertIn("claude: already authorized", proc.stdout)
            self.assertEqual(proc.stdout.count(
                "In a new terminal, open your project and run:"
            ), 1)
            self.assertIn("  parable\n", proc.stdout)

            before = capture.read_bytes()
            again = self.run_cli(home, proxy, capture, "auth", "login")
            self.assertEqual(again.returncode, 0, again.stdout + again.stderr)
            self.assertEqual(capture.read_bytes(), before)
            for vendor in ("claude", "chatgpt", "xai"):
                self.assertIn(f"{vendor}: already authorized", again.stdout)
            self.assertEqual(again.stdout.count(
                "In a new terminal, open your project and run:"
            ), 1)

    def test_add_vendors_auth_only_authorizes_newly_missing_providers(self):
        with tempfile.TemporaryDirectory() as tmp:
            home = Path(tmp)
            proxy, capture = self.make_proxy(home / "tools")
            setup = self.setup(home, proxy, capture, vendors="claude,xai")
            self.assertEqual(setup.returncode, 0, setup.stdout + setup.stderr)
            # Authorize claude and xai up front so the post-upgrade auth pass has nothing
            # to do for them, and must only reach for the newly-added kimi vendor.
            for vendor, flag in (("claude", "--claude-login"), ("xai", "--xai-login")):
                pre = self.run_cli(home, proxy, capture, "auth", "add", vendor)
                self.assertEqual(pre.returncode, 0, pre.stdout + pre.stderr)
            capture.write_text("")

            added = self.run_cli(home, proxy, capture, "setup", "--add-vendors", "kimi")
            self.assertEqual(added.returncode, 0, added.stdout + added.stderr)
            config = home / ".config" / "parable" / "cliproxy.yaml"
            self.assertEqual(self.calls(capture), [
                ["--config", str(config), "--kimi-login", "--no-browser"],
            ])
            self.assertIn("claude: already authorized", added.stdout)
            self.assertIn("xai: already authorized", added.stdout)
            self.assertIn("authorization complete", added.stdout)
            manifest = json.loads(
                (home / ".config" / "parable" / "setup.json").read_text()
            )
            self.assertEqual(manifest["vendors"], ["claude", "xai", "kimi"])

    def test_add_vendors_no_auth_defers_authorization(self):
        with tempfile.TemporaryDirectory() as tmp:
            home = Path(tmp)
            proxy, capture = self.make_proxy(home / "tools")
            setup = self.setup(home, proxy, capture, vendors="claude")
            self.assertEqual(setup.returncode, 0, setup.stdout + setup.stderr)
            added = self.run_cli(
                home, proxy, capture, "setup", "--add-vendors", "kimi", "--no-auth"
            )
            self.assertEqual(added.returncode, 0, added.stdout + added.stderr)
            self.assertEqual(self.calls(capture), [])
            self.assertIn("authorize each newly selected subscription", added.stdout)

    def test_auth_rejects_zero_exit_without_a_provider_record(self):
        with tempfile.TemporaryDirectory() as tmp:
            home = Path(tmp)
            proxy, capture = self.make_proxy(home / "tools")
            setup = self.setup(home, proxy, capture, vendors="claude")
            self.assertEqual(setup.returncode, 0, setup.stdout + setup.stderr)
            proc = self.run_cli(
                home, proxy, capture, "auth", "add", "claude",
                extra_env={"FAKE_PROXY_SKIP_AUTH": "claude"},
            )
            self.assertNotEqual(proc.returncode, 0)
            self.assertIn("exited without creating a private credential record", proc.stderr)
            self.assertIn("parable auth add claude", proc.stderr)

    def test_auth_rejects_missing_unselected_unsupported_and_bad_device_before_spawn(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            home = root / "home"
            home.mkdir()
            proxy, capture = self.make_proxy(root / "tools")
            setup = self.setup(home, proxy, capture, vendors="claude")
            self.assertEqual(setup.returncode, 0, setup.stdout + setup.stderr)
            rejected = (
                (("auth", "add", "xai"), "not selected"),
                (("auth", "add", "kimi"), "not selected"),
                (("auth", "add", "glm"), "unsupported auth vendor"),
                (("auth", "add", "claude", "--device"), "only for chatgpt"),
            )
            for command, message in rejected:
                proc = self.run_cli(home, proxy, capture, *command)
                self.assertNotEqual(proc.returncode, 0)
                self.assertIn(message, proc.stderr)
                self.assertEqual(self.calls(capture), [])

            missing_home = root / "missing"
            missing_home.mkdir()
            missing = self.run_cli(
                missing_home, proxy, capture, "auth", "add", "chatgpt"
            )
            self.assertNotEqual(missing.returncode, 0)
            self.assertIn("setup is missing", missing.stderr)
            self.assertEqual(self.calls(capture), [])

            proxy.unlink()
            missing_binary = self.run_cli(home, proxy, capture, "auth", "add", "claude")
            self.assertNotEqual(missing_binary.returncode, 0)
            self.assertIn("configured proxy binary", missing_binary.stderr)
            self.assertEqual(self.calls(capture), [])

    def test_auth_status_is_aggregate_only_and_does_not_require_proxy_binary(self):
        with tempfile.TemporaryDirectory() as tmp:
            home = Path(tmp)
            proxy, capture = self.make_proxy(home / "tools")
            setup = self.setup(home, proxy, capture)
            self.assertEqual(setup.returncode, 0, setup.stdout + setup.stderr)
            auth_dir = home / ".cli-proxy-api"
            records = {
                "account-alpha.json": {"type": "codex", "access_token": "SECRET-CODEX",
                                       "email": "private@example.invalid"},
                "account-beta.json": {"type": "claude", "refresh_token": "SECRET-CLAUDE"},
                "account-gamma.json": {"type": "xai", "id_token": "SECRET-XAI"},
                "account-delta.json": {"type": "kimi", "access_token": "SECRET-KIMI"},
                "other.json": {"type": "glm", "access_token": "SECRET-GLM"},
            }
            for name, value in records.items():
                target = auth_dir / name
                target.write_text(json.dumps(value))
                target.chmod(0o600)
            malformed = auth_dir / "malformed.json"
            malformed.write_text("{SECRET-MALFORMED")
            malformed.chmod(0o600)
            bad_mode = auth_dir / "bad-mode.json"
            bad_mode.write_text('{"type":"codex","access_token":"SECRET-BAD-MODE"}')
            bad_mode.chmod(0o644)
            outside = home / "outside-record"
            outside.write_text('{"type":"xai","access_token":"SECRET-SYMLINK"}')
            (auth_dir / "linked.json").symlink_to(outside)

            proxy.unlink()
            status_proc = self.run_cli(
                home, proxy, capture, "auth", "status", "--json"
            )
            self.assertEqual(status_proc.returncode, 0, status_proc.stdout + status_proc.stderr)
            status = json.loads(status_proc.stdout)
            self.assertEqual(status["providers"], {
                "chatgpt": {"present": True, "recordCount": 1},
                "claude": {"present": True, "recordCount": 1},
                "xai": {"present": True, "recordCount": 1},
                "kimi": {"present": True, "recordCount": 1},
            })
            self.assertEqual(status["records"], {
                "total": 8,
                "userOnly": 5,
                "invalidMode": 2,
                "parseErrors": 1,
                "unrecognized": 1,
                "allModesValid": False,
            })
            self.assertTrue(status["directoryModeValid"])
            self.assertTrue(status["scanned"])
            self.assertEqual(stat.S_IMODE(bad_mode.stat().st_mode), 0o644)
            forbidden = [
                "SECRET-", "private@example.invalid", "account-alpha", "bad-mode",
                "linked.json", str(auth_dir), str(outside),
            ]
            for value in forbidden:
                self.assertNotIn(value, status_proc.stdout + status_proc.stderr)

            text_status = self.run_cli(home, proxy, capture, "auth", "status")
            self.assertEqual(text_status.returncode, 0, text_status.stdout + text_status.stderr)
            self.assertIn("chatgpt  present=yes records=1", text_status.stdout)
            for value in forbidden:
                self.assertNotIn(value, text_status.stdout + text_status.stderr)

            auth_dir.chmod(0o755)
            unsafe = self.run_cli(home, proxy, capture, "auth", "status", "--json")
            self.assertEqual(unsafe.returncode, 0, unsafe.stdout + unsafe.stderr)
            unsafe_status = json.loads(unsafe.stdout)
            self.assertFalse(unsafe_status["directoryModeValid"])
            self.assertFalse(unsafe_status["scanned"])
            self.assertEqual(unsafe_status["records"]["total"], 0)
            for value in forbidden:
                self.assertNotIn(value, unsafe.stdout + unsafe.stderr)

    def test_proxy_start_is_foreground_exact_and_preserves_child_exit(self):
        with tempfile.TemporaryDirectory() as tmp:
            home = Path(tmp)
            proxy, capture = self.make_proxy(home / "tools")
            setup = self.setup(home, proxy, capture)
            self.assertEqual(setup.returncode, 0, setup.stdout + setup.stderr)
            config = home / ".config" / "parable" / "cliproxy.yaml"
            config.write_text(config.read_text().replace(
                "transient-error-cooldown-seconds: -1\n",
                "",
            ))
            config.chmod(0o600)
            proc = self.run_cli(
                home,
                proxy,
                capture,
                "proxy", "start",
                extra_env={"FAKE_PROXY_EXIT": "17", "FAKE_PROXY_STDOUT": "native-proxy-output"},
            )
            self.assertEqual(proc.returncode, 17, proc.stdout + proc.stderr)
            self.assertEqual(
                self.calls(capture),
                [["--config", str(config), "--local-model"]],
            )
            self.assertIn("native-proxy-output", proc.stdout)
            self.assertIn("proxy: updated generated retry policy", proc.stdout)
            self.assertIn(
                "transient-error-cooldown-seconds: -1\n",
                config.read_text(),
            )

            capture.unlink()
            proxy.unlink()
            missing = self.run_cli(home, proxy, capture, "proxy", "start")
            self.assertNotEqual(missing.returncode, 0)
            self.assertIn("configured proxy binary", missing.stderr)
            self.assertEqual(self.calls(capture), [])


class TestOnboardingFinalizeEndToEnd(unittest.TestCase):
    def make_proxy(self, bindir: Path, capture: Path) -> Path:
        proxy = bindir / "fake-subscription-proxy"
        proxy.write_text("""#!/usr/bin/env python3
import json
import os
from pathlib import Path
import sys
with open(os.environ["FAKE_PROXY_CAPTURE"], "a") as handle:
    handle.write(json.dumps(sys.argv[1:]) + "\\n")
mapping = {
    "--codex-login": ("chatgpt", "codex"),
    "--claude-login": ("claude", "claude"),
    "--xai-login": ("xai", "xai"),
    "--kimi-login": ("kimi", "kimi"),
}
for flag, (vendor, record_type) in mapping.items():
    if flag in sys.argv:
        target = Path.home() / ".cli-proxy-api" / f"fake-{vendor}.json"
        target.write_text(json.dumps({"type": record_type}))
        target.chmod(0o600)
""")
        proxy.chmod(0o755)
        return proxy

    def make_repo(self, root: Path, name: str = "repo") -> Path:
        repo = root / name
        agents = repo / ".claude" / "agents"
        agents.mkdir(parents=True)
        (agents / "handwritten.md").write_text(
            "---\nname: handwritten\ndescription: keep\n---\nUser-owned.\n"
        )
        (agents / "parable-handwritten.md").write_text(
            "---\nname: parable-handwritten\ndescription: also keep\n---\nUser-owned.\n"
        )
        subprocess.run(["git", "init", "-q"], cwd=repo, check=True)
        return repo

    def environment(
        self,
        home: Path,
        bindir: Path,
        proxy_capture: Path,
        claude_capture: Path,
    ) -> dict[str, str]:
        env = os.environ | {
            "HOME": str(home),
            "PATH": f"{bindir}:{os.environ['PATH']}",
            "FAKE_PROXY_CAPTURE": str(proxy_capture),
            "FAKE_CLAUDE_CAPTURE": str(claude_capture),
            "CLAUDE_CONFIG_DIR": str(home / ".claude-native"),
            "CODEX_HOME": str(home / ".codex-native"),
            "PARABLE_USAGE_CACHE": str(home / "usage-cache.json"),
            "CLAUDE_CODE_EXPERIMENTAL_AGENT_TEAMS": "1",
        }
        for name in (
            "PARABLE_CONFIG",
            "PARABLE_CLIPROXY_BIN",
            "CLIPROXY_API_KEY",
            "ANTHROPIC_AUTH_TOKEN",
        ):
            env.pop(name, None)
        return env

    def run_cli(
        self,
        repo: Path,
        env: dict[str, str],
        *args: str,
    ) -> subprocess.CompletedProcess:
        return subprocess.run(
            [NODE, str(REPO / "bin" / "parable.js"), *args],
            cwd=repo,
            env=env,
            capture_output=True,
            text=True,
            timeout=60,
        )

    def setup_token(self, home: Path) -> str:
        value = (home / ".config" / "parable" / "cliproxy.env").read_text()
        prefix = "export CLIPROXY_API_KEY='"
        self.assertTrue(value.startswith(prefix))
        return value[len(prefix):-2]

    def test_setup_auth_catalog_finalize_and_first_claude_launch(self):
        exact_models = [
            "gpt-5.6-sol",
            "gpt-5.6-terra",
            "gpt-5.6-luna",
            "claude-fable-5",
            "claude-sonnet-5",
            "claude-opus-4-8",
            "claude-haiku-4-5-20251001",
            "grok-4.6",
            "kimi-k3",
        ]
        expected_agents = {
            "parable-sol-exact": "gpt-5.6-sol",
            "parable-terra": "gpt-5.6-terra",
            "parable-luna": "gpt-5.6-luna",
            "parable-fable-exact": "claude-fable-5",
            "parable-sonnet-exact": "claude-sonnet-5",
            "parable-opus-exact": "claude-opus-4-8",
            "parable-haiku-exact": "claude-haiku-4-5-20251001",
            "parable-grok": "grok-4.6",
            "parable-kimi": "kimi-k3",
        }
        with tempfile.TemporaryDirectory() as tmp, model_server(
            exact_models + ["unrelated-model"]
        ) as (server, _base_url, _initial_token):
            root = Path(tmp)
            home = root / "home"
            home.mkdir()
            repo = self.make_repo(root)
            bindir = fake_bin(tmp)
            proxy_capture = root / "proxy-calls.jsonl"
            claude_capture = root / "claude.json"
            proxy = self.make_proxy(bindir, proxy_capture)
            env = self.environment(home, bindir, proxy_capture, claude_capture)
            port = str(server.server_address[1])

            setup = self.run_cli(
                repo,
                env,
                "setup", "--non-interactive", "--vendors", "chatgpt,claude,xai,kimi",
                "--proxy-bin", str(proxy), "--port", port,
            )
            self.assertEqual(setup.returncode, 0, setup.stdout + setup.stderr)
            token = self.setup_token(home)
            server.expected_token = token
            proxy_calls = [json.loads(line) for line in proxy_capture.read_text().splitlines()]
            config_path = home / ".config" / "parable" / "cliproxy.yaml"
            self.assertEqual(proxy_calls, [
                ["--config", str(config_path), "--claude-login", "--no-browser"],
                ["--config", str(config_path), "--codex-login"],
                ["--config", str(config_path), "--xai-login", "--no-browser"],
                ["--config", str(config_path), "--kimi-login", "--no-browser"],
            ])

            finalized = self.run_cli(repo, env, "setup", "finalize", "--json")
            self.assertEqual(finalized.returncode, 0, finalized.stdout + finalized.stderr)
            self.assertTrue(server.authorization_ok)
            self.assertNotIn(token, finalized.stdout + finalized.stderr)
            report = json.loads(finalized.stdout)
            self.assertTrue(report["ready"])
            self.assertEqual(report["parentModel"], "claude-fable-5")
            self.assertEqual(
                {item["name"]: item["model"] for item in report["agents"]},
                expected_agents,
            )
            self.assertEqual(report["catalog"]["configuredCount"], 9)
            self.assertFalse(report["degraded"])
            self.assertEqual(report["unavailableAgents"], [])
            self.assertEqual(
                report["next"],
                "parable",
            )

            agents_dir = repo / ".claude" / "agents"
            for name, model in expected_agents.items():
                target = agents_dir / f"{name}.md"
                self.assertTrue(target.is_file())
                self.assertIn(f'model: "{model}"', target.read_text())
                self.assertRegex(
                    target.read_text(),
                    r'(?m)^effort: "(?:low|medium|high|xhigh|max)"$',
                )
                self.assertNotIn(token, target.read_text())
            self.assertTrue((agents_dir / "handwritten.md").is_file())
            self.assertTrue((agents_dir / "parable-handwritten.md").is_file())

            before = {
                path: (path.read_bytes(), path.stat().st_mtime_ns)
                for path in agents_dir.glob("parable-*.md")
            }
            confirmed = self.run_cli(repo, env, "setup", "finalize", "--json")
            self.assertEqual(confirmed.returncode, 0, confirmed.stdout + confirmed.stderr)
            confirmed_report = json.loads(confirmed.stdout)
            self.assertEqual(confirmed_report["sync"], {
                "changed": 0,
                "unchanged": 9,
                "removed": 0,
            })
            for path, snapshot in before.items():
                self.assertEqual((path.read_bytes(), path.stat().st_mtime_ns), snapshot)

            launch = self.run_cli(repo, env, "claude", "--print", "hello")
            self.assertEqual(launch.returncode, 0, launch.stdout + launch.stderr)
            self.assertNotIn(token, launch.stdout + launch.stderr)
            captured_text = claude_capture.read_text()
            self.assertNotIn(token, captured_text)
            captured = json.loads(captured_text)
            welcome_plugin = (
                REPO / "skills" / "parable" / "runtime" / "welcome-plugin"
            )
            self.assertEqual(
                captured["argv"],
                [
                    "--plugin-dir", str(welcome_plugin),
                    "--model", "claude-fable-5[1m]", "--print", "hello",
                ],
            )
            self.assertIsNone(captured["welcome_message"])
            self.assertTrue(captured["auth_token_present"])
            self.assertFalse(captured["source_token_present"])

            bare = self.run_cli(repo, env)
            self.assertEqual(bare.returncode, 0, bare.stdout + bare.stderr)
            self.assertIn("brain: claude-fable-5", bare.stdout)
            captured = json.loads(claude_capture.read_text())
            welcome_plugin = (
                REPO / "skills" / "parable" / "runtime" / "welcome-plugin"
            )
            self.assertEqual(
                captured["argv"],
                [
                    "--plugin-dir", str(welcome_plugin),
                    "--model", "claude-fable-5[1m]", "--effort", "high",
                ],
            )
            self.assertIn("_ __   __ _ _ __", captured["welcome_message"])
            self.assertIn("🐢  🐘  🦊", captured["welcome_message"])
            self.assertIn("BRAIN   FABLE · claude-fable-5 · 1M ctx", captured["welcome_message"])
            # Multi-model cast: MAX_CONTEXT teaches non-Claude workers their
            # safe ceiling, while process-wide auto-compact controls stay out
            # of the 1M Fable parent's environment.
            self.assertEqual(captured["max_context_tokens"], "372000")
            self.assertIsNone(captured["auto_compact_window"])
            self.assertIsNone(captured["auto_compact_pct"])
            self.assertIn("TERRA", captured["welcome_message"])
            self.assertIn("React and frontend", captured["welcome_message"])
            self.assertNotIn(token, captured["welcome_message"])

            pinned = self.run_cli(
                repo, env, "--brain", "fable", "--dangerously-skip-permissions",
            )
            self.assertEqual(pinned.returncode, 0, pinned.stdout + pinned.stderr)
            captured = json.loads(claude_capture.read_text())
            self.assertEqual(
                captured["argv"],
                [
                    "--plugin-dir", str(welcome_plugin),
                    "--model", "claude-fable-5[1m]", "--effort", "high",
                    "--dangerously-skip-permissions",
                ],
            )
            self.assertIn("explicit fable parent", captured["welcome_message"])

            solo_before = {
                path: (path.read_bytes(), path.stat().st_mtime_ns)
                for path in agents_dir.glob("parable-*.md")
            }
            solo = self.run_cli(repo, env, "--solo", "kimi")
            self.assertEqual(solo.returncode, 0, solo.stdout + solo.stderr)
            self.assertIn("solo: kimi-k3", solo.stdout)
            self.assertNotIn("agents:", solo.stdout)
            captured = json.loads(claude_capture.read_text())
            self.assertEqual(
                captured["argv"],
                [
                    "--plugin-dir", str(welcome_plugin),
                    "--model", "kimi-k3", "--disallowedTools", "Agent",
                    "--effort", "high",
                ],
            )
            self.assertFalse(
                captured["inherited"]["CLAUDE_CODE_EXPERIMENTAL_AGENT_TEAMS"]
            )
            # Solo pins the real window of the exact model so auto-compact
            # fires before the upstream limit, not after.
            self.assertEqual(captured["max_context_tokens"], "1000000")
            self.assertEqual(captured["auto_compact_window"], "1000000")
            self.assertIn("SOLO    KIMI · kimi-k3", captured["welcome_message"])
            self.assertIn("1M ctx", captured["welcome_message"])
            self.assertIn("You are the only agent", captured["welcome_message"])
            self.assertNotIn("BRAIN", captured["welcome_message"])
            self.assertNotIn("CAST", captured["welcome_message"])
            solo_after = {
                path: (path.read_bytes(), path.stat().st_mtime_ns)
                for path in agents_dir.glob("parable-*.md")
            }
            self.assertEqual(solo_after, solo_before)

            solo_exact = self.run_cli(
                repo, env, "--solo=kimi-k3", "--print", "solo-exact"
            )
            self.assertEqual(
                solo_exact.returncode, 0, solo_exact.stdout + solo_exact.stderr
            )
            captured = json.loads(claude_capture.read_text())
            self.assertEqual(
                captured["argv"],
                [
                    "--model", "kimi-k3", "--disallowedTools", "Agent",
                    "--effort", "high", "--print", "solo-exact",
                ],
            )
            self.assertIsNone(captured["welcome_message"])

            legacy_solo = self.run_cli(
                repo, env, "claude", "--solo", "kimi", "--", "--print", "legacy-solo"
            )
            self.assertEqual(
                legacy_solo.returncode, 0, legacy_solo.stdout + legacy_solo.stderr
            )
            captured = json.loads(claude_capture.read_text())
            self.assertEqual(
                captured["argv"],
                [
                    "--model", "kimi-k3", "--disallowedTools", "Agent",
                    "--print", "legacy-solo",
                ],
            )

            direct = self.run_cli(
                repo, env, "--dangerously-skip-permissions", "--effort", "low",
                "--print", "direct",
            )
            self.assertEqual(direct.returncode, 0, direct.stdout + direct.stderr)
            captured = json.loads(claude_capture.read_text())
            self.assertEqual(
                captured["argv"],
                [
                    "--plugin-dir", str(welcome_plugin),
                    "--model", "claude-fable-5[1m]", "--dangerously-skip-permissions",
                    "--effort", "low", "--print", "direct",
                ],
            )
            self.assertIsNone(captured["welcome_message"])

            auto = self.run_cli(
                repo, env, "claude", "--brain", "auto", "--", "--print", "auto"
            )
            self.assertEqual(auto.returncode, 0, auto.stdout + auto.stderr)
            self.assertIn("brain: claude-fable-5", auto.stdout)
            captured = json.loads(claude_capture.read_text())
            self.assertEqual(
                captured["argv"],
                [
                    "--plugin-dir", str(welcome_plugin),
                    "--model", "claude-fable-5[1m]", "--print", "auto",
                ],
            )

            grok = self.run_cli(
                repo, env, "claude", "--brain", "grok", "--", "--print", "grok-parent"
            )
            self.assertEqual(grok.returncode, 0, grok.stdout + grok.stderr)
            self.assertIn("brain: grok-4.6 (explicit grok parent)", grok.stdout)
            captured = json.loads(claude_capture.read_text())
            self.assertEqual(
                captured["argv"],
                [
                    "--plugin-dir", str(welcome_plugin),
                    "--model", "grok-4.6", "--print", "grok-parent",
                ],
            )
            self.assertEqual(
                json.loads(captured["agent_state"])["parent"],
                ["parable-grok"],
            )

    def test_finalize_subset_and_missing_optional_exact_id_degrades_without_aliases(self):
        with tempfile.TemporaryDirectory() as tmp, model_server([
            "claude-fable-5", "claude-sonnet-5", "claude-opus-4-8",
            "claude-haiku-4-5-20251001",
        ]) as (server, _base_url, _initial_token):
            root = Path(tmp)
            home = root / "subset-home"
            home.mkdir()
            repo = self.make_repo(root, "subset-repo")
            bindir = fake_bin(tmp)
            proxy_capture = root / "subset-proxy.jsonl"
            claude_capture = root / "subset-claude.json"
            proxy = self.make_proxy(bindir, proxy_capture)
            env = self.environment(home, bindir, proxy_capture, claude_capture)
            setup = self.run_cli(
                repo,
                env,
                "setup", "--non-interactive", "--vendors", "claude",
                "--proxy-bin", str(proxy), "--port", str(server.server_address[1]),
                "--no-auth",
            )
            self.assertEqual(setup.returncode, 0, setup.stdout + setup.stderr)
            server.expected_token = self.setup_token(home)
            finalized = self.run_cli(repo, env, "setup", "finalize", "--json")
            self.assertEqual(finalized.returncode, 0, finalized.stdout + finalized.stderr)
            report = json.loads(finalized.stdout)
            self.assertEqual(
                {item["name"]: item["model"] for item in report["agents"]},
                {
                    "parable-fable-exact": "claude-fable-5",
                    "parable-haiku-exact": "claude-haiku-4-5-20251001",
                    "parable-opus-exact": "claude-opus-4-8",
                    "parable-sonnet-exact": "claude-sonnet-5",
                },
            )
            self.assertEqual(report["parentModel"], "claude-fable-5")
            auto = self.run_cli(
                repo, env, "claude", "--brain", "auto", "--", "--print", "claude-only"
            )
            self.assertEqual(auto.returncode, 0, auto.stdout + auto.stderr)
            self.assertIn(
                "brain: claude-fable-5 (Sol is not configured; Grok is not configured; using Fable)",
                auto.stdout,
            )
            captured = json.loads(claude_capture.read_text())
            welcome_plugin = (
                REPO / "skills" / "parable" / "runtime" / "welcome-plugin"
            )
            self.assertEqual(
                captured["argv"],
                [
                    "--plugin-dir", str(welcome_plugin),
                    "--model", "claude-fable-5[1m]", "--print", "claude-only",
                ],
            )
            explicit_sol = self.run_cli(repo, env, "claude", "--brain", "sol")
            self.assertNotEqual(explicit_sol.returncode, 0)
            self.assertIn("requires configured model 'gpt-5.6-sol'", explicit_sol.stderr)

        misleading = [
            "gpt-5.6-sol",
            "gpt-5.6-terra",
            "gpt-5.6-luna",
            "claude-fable-5",
            "claude-sonnet-5",
            "claude-opus-4-8",
            "claude-haiku-4-5-20251001",
            "grok-4.6-latest",
            "GROK-4.6",
        ]
        with tempfile.TemporaryDirectory() as tmp, model_server(
            misleading
        ) as (server, _base_url, _initial_token):
            root = Path(tmp)
            home = root / "missing-home"
            home.mkdir()
            repo = self.make_repo(root, "missing-repo")
            bindir = fake_bin(tmp)
            proxy_capture = root / "missing-proxy.jsonl"
            claude_capture = root / "missing-claude.json"
            proxy = self.make_proxy(bindir, proxy_capture)
            env = self.environment(home, bindir, proxy_capture, claude_capture)
            setup = self.run_cli(
                repo,
                env,
                "setup", "--non-interactive", "--vendors", "chatgpt,claude,xai",
                "--proxy-bin", str(proxy), "--port", str(server.server_address[1]),
                "--no-auth",
            )
            self.assertEqual(setup.returncode, 0, setup.stdout + setup.stderr)
            server.expected_token = self.setup_token(home)
            finalized = self.run_cli(repo, env, "setup", "finalize", "--json")
            self.assertEqual(finalized.returncode, 0, finalized.stdout + finalized.stderr)
            report = json.loads(finalized.stdout)
            self.assertTrue(report["ready"])
            self.assertTrue(report["degraded"])
            self.assertEqual(
                report["unavailableAgents"],
                [{"name": "parable-grok", "model": "grok-4.6"}],
            )
            agents = repo / ".claude" / "agents"
            self.assertTrue((agents / "parable-grok.md").is_file())
            self.assertTrue((agents / "handwritten.md").is_file())
            self.assertTrue((agents / "parable-handwritten.md").is_file())
            self.assertFalse(claude_capture.exists())


class TestMagicalClaudeSupervisor(unittest.TestCase):
    MODELS = [
        "gpt-5.6-sol",
        "gpt-5.6-terra",
        "gpt-5.6-luna",
        "claude-fable-5",
        "claude-sonnet-5",
        "claude-opus-4-8",
        "claude-haiku-4-5-20251001",
        "grok-4.6",
    ]

    PROXY = r'''#!/usr/bin/env python3
import json
import os
import re
import signal
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

capture_path = os.environ["FAKE_PROXY_CAPTURE"]

def event(kind, **fields):
    with open(capture_path, "a") as handle:
        handle.write(json.dumps({"event": kind, "pid": os.getpid(), **fields}) + "\n")

event("start", argv=sys.argv[1:])

def stop(signum, _frame):
    event("signal", signal=signum)
    raise SystemExit(128 + signum)

for handled in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
    signal.signal(handled, stop)

mode = os.environ.get("FAKE_PROXY_MODE", "serve")
if mode == "early":
    raise SystemExit(int(os.environ.get("FAKE_PROXY_EXIT", "17")))
if mode == "hang":
    while True:
        time.sleep(0.05)

config_path = sys.argv[sys.argv.index("--config") + 1]
config = open(config_path).read()
port = int(re.search(r"^port: ([0-9]+)$", config, re.MULTILINE).group(1))
token = re.search(r'^  - "([0-9a-f]{64})"$', config, re.MULTILINE).group(1)
models = json.loads(os.environ["FAKE_PROXY_MODELS"])

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path != "/v1/models":
            self.send_error(404)
            return
        if self.headers.get("Authorization") != f"Bearer {token}":
            self.send_error(401)
            return
        body = json.dumps({"data": [{"id": model} for model in models]}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, _format, *_args):
        pass

server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
exit_after = os.environ.get("FAKE_PROXY_EXIT_AFTER_MS")
if exit_after:
    timer = threading.Timer(
        int(exit_after) / 1000,
        lambda: os._exit(int(os.environ.get("FAKE_PROXY_EXIT", "19"))),
    )
    timer.daemon = True
    timer.start()
try:
    server.serve_forever(poll_interval=0.05)
finally:
    server.server_close()
    event("stop")
'''

    def free_port(self) -> int:
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            return listener.getsockname()[1]

    def setup_case(self, tmp: str, port: int | None = None) -> dict:
        root = Path(tmp)
        home = root / "home"
        home.mkdir()
        repo = root / "repo"
        repo.mkdir()
        subprocess.run(["git", "init", "-q"], cwd=repo, check=True)
        bindir = fake_bin(tmp)
        proxy = bindir / "fake-managed-proxy"
        proxy.write_text(self.PROXY)
        proxy.chmod(0o755)
        proxy_capture = root / "proxy.jsonl"
        claude_capture = root / "claude.json"
        claude_signal_capture = root / "claude-signal.json"
        env = os.environ | {
            "HOME": str(home),
            "PATH": f"{bindir}:{os.environ['PATH']}",
            "FAKE_PROXY_CAPTURE": str(proxy_capture),
            "FAKE_PROXY_MODELS": json.dumps(self.MODELS),
            "FAKE_CLAUDE_CAPTURE": str(claude_capture),
            "FAKE_CLAUDE_SIGNAL_CAPTURE": str(claude_signal_capture),
        }
        for name in (
            "PARABLE_CONFIG",
            "PARABLE_CLIPROXY_BIN",
            "CLIPROXY_API_KEY",
            "ANTHROPIC_AUTH_TOKEN",
        ):
            env.pop(name, None)
        selected_port = port or self.free_port()
        setup = subprocess.run(
            [
                NODE, str(REPO / "bin" / "parable.js"),
                "setup", "--non-interactive", "--vendors", "chatgpt,claude,xai",
                "--proxy-bin", str(proxy), "--port", str(selected_port), "--no-auth",
            ],
            cwd=repo,
            env=env,
            capture_output=True,
            text=True,
            timeout=60,
        )
        self.assertEqual(setup.returncode, 0, setup.stdout + setup.stderr)
        token_text = (home / ".config" / "parable" / "cliproxy.env").read_text()
        token = token_text.removeprefix("export CLIPROXY_API_KEY='").removesuffix("'\n")
        return {
            "home": home,
            "repo": repo,
            "env": env,
            "port": selected_port,
            "token": token,
            "proxy_capture": proxy_capture,
            "claude_capture": claude_capture,
            "claude_signal_capture": claude_signal_capture,
        }

    def run_claude(self, case: dict, **extra_env: str) -> subprocess.CompletedProcess:
        env = case["env"] | extra_env
        return subprocess.run(
            [NODE, str(REPO / "bin" / "parable.js"), "claude", "--print", "hello"],
            cwd=case["repo"],
            env=env,
            capture_output=True,
            text=True,
            timeout=20,
        )

    def events(self, case: dict) -> list[dict]:
        path = case["proxy_capture"]
        if not path.exists():
            return []
        return [json.loads(line) for line in path.read_text().splitlines()]

    def assert_pid_gone(self, pid: int):
        for _ in range(100):
            try:
                os.kill(pid, 0)
            except ProcessLookupError:
                return
            time.sleep(0.02)
        self.fail(f"child process {pid} remained after supervisor exit")

    def wait_for_path(self, target: Path, proc: subprocess.Popen):
        for _ in range(200):
            if target.exists():
                return
            if proc.poll() is not None:
                stdout, stderr = proc.communicate()
                self.fail(f"supervisor exited before readiness: {stdout}{stderr}")
            time.sleep(0.025)
        proc.kill()
        stdout, stderr = proc.communicate()
        self.fail(f"timed out waiting for {target.name}: {stdout}{stderr}")

    def test_owned_proxy_starts_then_claude_exit_is_preserved_and_proxy_is_cleaned(self):
        with tempfile.TemporaryDirectory() as tmp:
            case = self.setup_case(tmp)
            proc = self.run_claude(case, FAKE_CLAUDE_EXIT="23")
            self.assertEqual(proc.returncode, 23, proc.stdout + proc.stderr)
            self.assertIn("proxy: starting managed CLIProxyAPI", proc.stdout)
            self.assertTrue(case["claude_capture"].is_file())
            events = self.events(case)
            self.assertEqual(events[0]["argv"][-1], "--local-model")
            self.assertTrue(any(item["event"] == "signal" for item in events))
            self.assert_pid_gone(events[0]["pid"])
            evidence = proc.stdout + proc.stderr + json.dumps(events)
            self.assertNotIn(case["token"], evidence)

    def test_finalize_starts_and_cleans_managed_proxy_when_stopped(self):
        with tempfile.TemporaryDirectory() as tmp:
            case = self.setup_case(tmp)
            proc = subprocess.run(
                [NODE, str(REPO / "bin" / "parable.js"),
                 "setup", "finalize", "--json"],
                cwd=case["repo"],
                env=case["env"],
                capture_output=True,
                text=True,
                timeout=20,
            )
            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            report = json.loads(proc.stdout)
            self.assertTrue(report["ready"])
            self.assertEqual(report["catalog"]["configuredCount"], len(self.MODELS))
            events = self.events(case)
            self.assertEqual(events[0]["argv"][-1], "--local-model")
            self.assertTrue(any(item["event"] == "signal" for item in events))
            self.assert_pid_gone(events[0]["pid"])
            self.assertNotIn(case["token"], proc.stdout + proc.stderr + json.dumps(events))

    def test_healthy_existing_proxy_is_reused_and_never_stopped(self):
        with tempfile.TemporaryDirectory() as tmp, model_server(self.MODELS) as (
            server, _base_url, _initial_token
        ):
            case = self.setup_case(tmp, server.server_address[1])
            server.expected_token = case["token"]
            proc = self.run_claude(case)
            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("proxy: reusing healthy configured endpoint", proc.stdout)
            self.assertTrue(server.authorization_ok)
            self.assertEqual(self.events(case), [])

    def test_plain_launch_migrates_previous_retry_policy_before_proxy_start(self):
        with tempfile.TemporaryDirectory() as tmp:
            case = self.setup_case(tmp)
            config = case["home"] / ".config" / "parable" / "cliproxy.yaml"
            config.write_text(config.read_text().replace(
                "transient-error-cooldown-seconds: -1\n",
                "",
            ))
            config.chmod(0o600)

            proc = self.run_claude(case)

            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("proxy: updated generated retry policy", proc.stdout)
            self.assertIn(
                "transient-error-cooldown-seconds: -1\n",
                config.read_text(),
            )

    def test_teammate_interruption_resumes_exact_session_and_replies(self):
        with tempfile.TemporaryDirectory() as tmp:
            case = self.setup_case(tmp)
            calls = Path(tmp) / "claude-calls.jsonl"
            state = Path(tmp) / "teammate-interrupted"
            session_id = "12345678-1234-4234-9234-123456789abc"
            env = case["env"] | {
                "FAKE_CLAUDE_CALLS": str(calls),
                "FAKE_CLAUDE_TEAMMATE_INTERRUPT_ONCE": "1",
                "FAKE_CLAUDE_TEAMMATE_INTERRUPT_STATE": str(state),
            }

            proc = subprocess.run(
                [NODE, str(REPO / "bin" / "parable.js")],
                cwd=case["repo"],
                env=env,
                capture_output=True,
                text=True,
                timeout=20,
            )

            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn(
                f"session: teammate update interrupted the active turn; resuming {session_id}",
                proc.stdout,
            )
            captured_calls = [
                json.loads(line) for line in calls.read_text().splitlines()
            ]
            self.assertEqual(len(captured_calls), 2)
            self.assertNotIn("--resume", captured_calls[0]["argv"])
            final = captured_calls[-1]["argv"]
            resume_at = final.index("--resume")
            self.assertEqual(final[resume_at + 1], session_id)
            self.assertIn("--reply-on-resume", final)
            self.assertTrue(captured_calls[-1]["teammate_recovery_active"])

    def test_context_failure_compacts_with_sonnet_and_resumes_exact_session(self):
        with tempfile.TemporaryDirectory() as tmp:
            case = self.setup_case(tmp)
            calls = Path(tmp) / "claude-calls.jsonl"
            compact_state = Path(tmp) / "compact-finished"
            env = case["env"] | {
                "FAKE_CLAUDE_CALLS": str(calls),
                "FAKE_CLAUDE_CONTEXT_FAILURE_ONCE": "1",
                "FAKE_CLAUDE_CONTEXT_TOKENS": "321400",
                "FAKE_CLAUDE_POST_COMPACT_TOKENS": "42000",
                "FAKE_CLAUDE_COMPACT_STATE": str(compact_state),
            }
            proc = subprocess.run(
                [NODE, str(REPO / "bin" / "parable.js")],
                cwd=case["repo"],
                env=env,
                capture_output=True,
                text=True,
                timeout=20,
            )
            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            session_id = "12345678-1234-4234-9234-123456789abc"
            self.assertIn(
                f"context: limit reached; compacting with Sonnet 5, then resuming {session_id}",
                proc.stdout,
            )
            self.assertIn(
                "resume: compacted 321,400 to 42,000 tokens with Sonnet 5",
                proc.stdout,
            )
            self.assertIn(
                "resume: compacting 321,400 tokens with Sonnet 5; "
                "this can take several minutes",
                proc.stdout,
            )
            captured_calls = [
                json.loads(line) for line in calls.read_text().splitlines()
            ]
            self.assertEqual(len(captured_calls), 5)
            self.assertNotIn("--resume", captured_calls[0]["argv"])
            for call in captured_calls[1:]:
                self.assertIn("--resume", call["argv"])
                resume_at = call["argv"].index("--resume")
                self.assertEqual(call["argv"][resume_at + 1], session_id)
            preflight = captured_calls[1:4]
            self.assertTrue(all(
                call["argv"][call["argv"].index("--model") + 1]
                == "claude-sonnet-5[1m]"
                for call in preflight
            ))
            final = captured_calls[-1]
            self.assertEqual(
                final["argv"][final["argv"].index("--model") + 1],
                "claude-fable-5[1m]",
            )
            self.assertIsNone(final["auto_compact_window"])
            events = self.events(case)
            self.assertTrue(any(item["event"] == "signal" for item in events))
            self.assert_pid_gone(events[0]["pid"])

    def test_resume_picker_selection_resumes_exact_full_window_session(self):
        with tempfile.TemporaryDirectory() as tmp:
            case = self.setup_case(tmp)
            calls = Path(tmp) / "claude-calls.jsonl"
            compact_state = Path(tmp) / "compact-finished"
            session_id = "12345678-1234-4234-9234-123456789abc"
            env = case["env"] | {
                "FAKE_CLAUDE_CALLS": str(calls),
                "FAKE_CLAUDE_RESUME_PICKER_SESSION": session_id,
                "FAKE_CLAUDE_CONTEXT_TOKENS": "321400",
                "FAKE_CLAUDE_POST_COMPACT_TOKENS": "42000",
                "FAKE_CLAUDE_COMPACT_STATE": str(compact_state),
            }
            proc = subprocess.run(
                [NODE, str(REPO / "bin" / "parable.js"), "--resume"],
                cwd=case["repo"],
                env=env,
                capture_output=True,
                text=True,
                timeout=20,
            )
            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn(
                f"context: resume selected; validating the target window, then resuming {session_id}",
                proc.stdout,
            )
            self.assertNotIn("resume: compacting", proc.stdout)
            captured_calls = [
                json.loads(line) for line in calls.read_text().splitlines()
            ]
            self.assertEqual(len(captured_calls), 2)
            initial = captured_calls[0]
            self.assertTrue(initial["resume_picker_recovery"])
            self.assertEqual(initial["argv"].count("--resume"), 1)
            self.assertEqual(initial["argv"][-1], "--resume")
            final = captured_calls[1]
            self.assertFalse(final["resume_picker_recovery"])
            resume_at = final["argv"].index("--resume")
            self.assertEqual(final["argv"][resume_at + 1], session_id)
            self.assertEqual(
                final["argv"][final["argv"].index("--model") + 1],
                "claude-fable-5[1m]",
            )
            events = self.events(case)
            self.assertTrue(any(item["event"] == "signal" for item in events))
            self.assert_pid_gone(events[0]["pid"])

    def test_context_recovery_is_capped_at_one_restart(self):
        with tempfile.TemporaryDirectory() as tmp:
            case = self.setup_case(tmp)
            calls = Path(tmp) / "claude-calls.jsonl"
            env = case["env"] | {
                "FAKE_CLAUDE_CALLS": str(calls),
                "FAKE_CLAUDE_CONTEXT_FAILURE_ALWAYS": "1",
                "FAKE_CLAUDE_CONTEXT_TOKENS": "321400",
                "FAKE_CLAUDE_POST_COMPACT_TOKENS": "42000",
                "FAKE_CLAUDE_COMPACT_STATE": str(Path(tmp) / "compact-finished"),
                "FAKE_CLAUDE_EXIT": "44",
            }
            proc = subprocess.run(
                [NODE, str(REPO / "bin" / "parable.js")],
                cwd=case["repo"],
                env=env,
                capture_output=True,
                text=True,
                timeout=20,
            )
            self.assertEqual(proc.returncode, 44, proc.stdout + proc.stderr)
            self.assertEqual(proc.stdout.count("context: limit reached"), 1)
            captured_calls = [
                json.loads(line) for line in calls.read_text().splitlines()
            ]
            self.assertEqual(len(captured_calls), 5)

    def test_context_recovery_normalizes_forked_explicit_session_launch(self):
        with tempfile.TemporaryDirectory() as tmp:
            case = self.setup_case(tmp)
            calls = Path(tmp) / "claude-calls.jsonl"
            compact_state = Path(tmp) / "compact-finished"
            failure_state = Path(tmp) / "context-failed"
            original_session = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
            recovered_session = "12345678-1234-4234-9234-123456789abc"
            env = case["env"] | {
                "FAKE_CLAUDE_CALLS": str(calls),
                "FAKE_CLAUDE_CONTEXT_FAILURE_ONCE": "1",
                "FAKE_CLAUDE_CONTEXT_FAILURE_STATE": str(failure_state),
                "FAKE_CLAUDE_CONTEXT_TOKENS": "321400",
                "FAKE_CLAUDE_POST_COMPACT_TOKENS": "42000",
                "FAKE_CLAUDE_COMPACT_STATE": str(compact_state),
            }
            proc = subprocess.run(
                [
                    NODE,
                    str(REPO / "bin" / "parable.js"),
                    "--resume",
                    "seed-session",
                    "--fork-session",
                    "--session-id",
                    original_session,
                ],
                cwd=case["repo"],
                env=env,
                capture_output=True,
                text=True,
                timeout=20,
            )
            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            captured_calls = [
                json.loads(line) for line in calls.read_text().splitlines()
            ]
            self.assertEqual(len(captured_calls), 5)
            initial = captured_calls[0]["argv"]
            self.assertIn("--fork-session", initial)
            self.assertIn("--session-id", initial)
            for call in captured_calls[1:]:
                argv = call["argv"]
                self.assertNotIn("--fork-session", argv)
                self.assertNotIn("--session-id", argv)
                self.assertIn("--resume", argv)
                resume_at = argv.index("--resume")
                self.assertEqual(argv[resume_at + 1], recovered_session)

    def test_non_persistent_launch_does_not_offer_impossible_recovery(self):
        with tempfile.TemporaryDirectory() as tmp:
            case = self.setup_case(tmp)
            proc = subprocess.run(
                [
                    NODE,
                    str(REPO / "bin" / "parable.js"),
                    "--print",
                    "--no-session-persistence",
                    "hello",
                ],
                cwd=case["repo"],
                env=case["env"] | {
                    "PARABLE_CONTEXT_RECOVERY_FILE": str(Path(tmp) / "forged-request.json"),
                    "PARABLE_CONTEXT_RESUME_PICKER": "1",
                },
                capture_output=True,
                text=True,
                timeout=20,
            )
            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            captured = json.loads(case["claude_capture"].read_text())
            self.assertFalse(captured["context_recovery_enabled"])
            self.assertFalse(captured["resume_picker_recovery"])

    def test_wrong_listener_fails_closed_before_proxy_or_claude(self):
        with tempfile.TemporaryDirectory() as tmp, model_server(self.MODELS) as (
            server, _base_url, _wrong_token
        ):
            case = self.setup_case(tmp, server.server_address[1])
            proc = self.run_claude(case)
            self.assertEqual(proc.returncode, 1)
            self.assertIn("occupied or unhealthy (HTTP 401)", proc.stderr)
            self.assertIn("refusing to start or stop an unknown listener", proc.stderr)
            self.assertEqual(self.events(case), [])
            self.assertFalse(case["claude_capture"].exists())

    def test_proxy_early_exit_and_readiness_timeout_fail_without_orphans(self):
        with tempfile.TemporaryDirectory() as tmp:
            early = self.setup_case(tmp)
            proc = self.run_claude(early, FAKE_PROXY_MODE="early", FAKE_PROXY_EXIT="17")
            self.assertEqual(proc.returncode, 17, proc.stdout + proc.stderr)
            self.assertIn("before readiness", proc.stderr)
            self.assertFalse(early["claude_capture"].exists())
            self.assert_pid_gone(self.events(early)[0]["pid"])

        with tempfile.TemporaryDirectory() as tmp:
            waiting = self.setup_case(tmp)
            proc = self.run_claude(
                waiting,
                FAKE_PROXY_MODE="hang",
                PARABLE_PROXY_READY_TIMEOUT_MS="150",
            )
            self.assertEqual(proc.returncode, 1)
            self.assertIn("timed out after 150ms", proc.stderr)
            self.assertFalse(waiting["claude_capture"].exists())
            events = self.events(waiting)
            self.assertTrue(any(item["event"] == "signal" for item in events))
            self.assert_pid_gone(events[0]["pid"])

    def test_proxy_exit_while_claude_runs_stops_claude_and_preserves_proxy_exit(self):
        with tempfile.TemporaryDirectory() as tmp:
            case = self.setup_case(tmp)
            proc = self.run_claude(
                case,
                FAKE_CLAUDE_WAIT="1",
                # Leave enough time for the fake Claude process to install its
                # signal handler on a busy host before the proxy exits.
                FAKE_PROXY_EXIT_AFTER_MS="1000",
                FAKE_PROXY_EXIT="19",
            )
            self.assertEqual(proc.returncode, 19, proc.stdout + proc.stderr)
            self.assertIn("while Claude was running", proc.stderr)
            signal_report = json.loads(case["claude_signal_capture"].read_text())
            self.assertEqual(signal_report["signal"], signal.SIGTERM)
            self.assert_pid_gone(signal_report["pid"])
            self.assert_pid_gone(self.events(case)[0]["pid"])

    def test_parent_signals_reach_both_owned_children_and_leave_no_orphans(self):
        for sent in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
            with self.subTest(signal=sent), tempfile.TemporaryDirectory() as tmp:
                case = self.setup_case(tmp)
                env = case["env"] | {"FAKE_CLAUDE_WAIT": "1"}
                proc = subprocess.Popen(
                    [NODE, str(REPO / "bin" / "parable.js"), "claude", "--print", "hello"],
                    cwd=case["repo"],
                    env=env,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                )
                self.wait_for_path(case["claude_capture"], proc)
                os.kill(proc.pid, sent)
                stdout, stderr = proc.communicate(timeout=10)
                self.assertEqual(proc.returncode, 128 + sent, stdout + stderr)
                claude_signal = json.loads(case["claude_signal_capture"].read_text())
                self.assertEqual(claude_signal["signal"], sent)
                events = self.events(case)
                self.assertTrue(any(
                    item["event"] == "signal" and item["signal"] == sent
                    for item in events
                ))
                self.assert_pid_gone(claude_signal["pid"])
                self.assert_pid_gone(events[0]["pid"])


if __name__ == "__main__":
    unittest.main()
