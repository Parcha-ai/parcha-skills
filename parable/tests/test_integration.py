"""Subprocess-level tests: cmd_run against fake harness binaries, and the
node installer. No network, no real codex/pi."""

import json
import os
import secrets
import subprocess
import tempfile
import threading
import unittest
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
SCRIPT = REPO / "skills" / "parable" / "scripts" / "parable.py"

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
import sys

capture = {
    "argv": sys.argv[1:],
    "base_url": os.environ.get("ANTHROPIC_BASE_URL"),
    "auth_token_present": bool(os.environ.get("ANTHROPIC_AUTH_TOKEN")),
    "source_token_present": "PARABLE_PROXY_TOKEN" in os.environ,
    "inherited": {
        key: key in os.environ
        for key in (
            "ANTHROPIC_API_KEY",
            "CLAUDE_CODE_OAUTH_TOKEN",
            "CLAUDE_CODE_SUBAGENT_MODEL",
        )
    },
}
with open(os.environ["FAKE_CLAUDE_CAPTURE"], "w") as handle:
    json.dump(capture, handle)
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
            self.assertEqual(
                captured["argv"],
                ["--model", "gpt-5.6-sol", "--print", "hello"],
            )
            self.assertEqual(captured["base_url"], base_url)
            self.assertTrue(captured["auth_token_present"])
            self.assertFalse(captured["source_token_present"])
            self.assertEqual(
                captured["inherited"],
                {
                    "ANTHROPIC_API_KEY": False,
                    "CLAUDE_CODE_OAUTH_TOKEN": False,
                    "CLAUDE_CODE_SUBAGENT_MODEL": False,
                },
            )
            self.assertEqual(settings.read_bytes(), before)
            agent = repo / ".claude" / "agents" / "parable-kimi.md"
            self.assertIn('model: "kimi-k3"', agent.read_text())
            self.assertNotIn(token, agent.read_text())
            self.assertNotIn(token, (repo / "parable.toml").read_text())

    def test_launcher_fails_closed_when_a_routed_model_is_absent(self):
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
            self.assertNotEqual(proc.returncode, 0)
            self.assertIn("proxy model catalog is missing: kimi-k3", proc.stderr)
            self.assertFalse(capture.exists())
            self.assertFalse((repo / ".claude" / "agents" / "parable-kimi.md").exists())

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


if __name__ == "__main__":
    unittest.main()
