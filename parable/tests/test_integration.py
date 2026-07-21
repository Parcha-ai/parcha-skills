"""Subprocess-level tests: cmd_run against fake harness binaries, and the
node installer. No network, no real codex/pi."""

import hashlib
import json
import os
import secrets
import shutil
import signal
import socket
import subprocess
import tempfile
import threading
import time
import unittest
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
SCRIPT = REPO / "skills" / "parable" / "scripts" / "parable.py"
NODE = shutil.which("node") or "node"
PROXY_COMMIT = "93d74a890a44802f656d7f39a573916b2611896e"
PROXY_PATCH_SHA256 = "d35b422da321265150fe393da80a686862ef642ee45c65a3e2fb908d689d5d1f"

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
        )
    },
}
with open(os.environ["FAKE_CLAUDE_CAPTURE"], "w") as handle:
    json.dump(capture, handle)
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
        self.assertIn("parable claude --brain auto -- --effort high", readme)
        self.assertNotIn("# terminal 1: foreground local proxy", readme)
        self.assertNotIn('"$PARABLE" setup finalize\n"$PARABLE" claude', readme)

        self.assertIn("./install.sh", guide)
        self.assertIn("parable claude --brain auto -- --effort high", guide)
        self.assertIn("That is the whole ordinary path.", guide)
        self.assertIn("stops only the proxy process it owns", guide)
        self.assertIn("Neither command is part of ordinary onboarding.", guide)

        for surface in (skill, providers):
            self.assertIn("parable.sh", surface)
            self.assertIn("parable claude --brain auto", surface)
            self.assertIn("setup finalize", surface)
            self.assertIn("proxy start", surface)

        self.assertIn("install parable.sh", skill)
        self.assertIn("AskUserQuestion", skill)
        self.assertIn("request_user_input", skill)
        self.assertIn("do not silently infer paid subscriptions", skill)
        self.assertIn("--non-interactive", skill)
        self.assertIn("--vendors claude[,chatgpt][,xai]", skill)
        self.assertIn("--build-proxy", skill)
        self.assertIn("Do not pass `--no-auth` during ordinary onboarding", skill)

        self.assertIn('chmod +x "$DEST"/parable.sh', installer)
        self.assertIn('exec "$DEST/parable.sh" "$@"', installer)

        help_proc = subprocess.run(
            [NODE, str(REPO / "bin" / "parable.js")],
            capture_output=True,
            text=True,
            timeout=60,
        )
        self.assertEqual(help_proc.returncode, 0, help_proc.stdout + help_proc.stderr)
        self.assertIn("supervise the proxy", help_proc.stdout)
        self.assertIn("diagnostic foreground", help_proc.stdout)

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
            launch = "parable claude --brain auto -- --effort high"
            self.assertEqual(first.stdout.count(handoff), 1)
            self.assertIn(launch, first.stdout)

            installed = home / ".local" / "share" / "parable" / "0.1.11"
            durable = home / ".local" / "bin" / "parable"
            self.assertTrue((installed / "bin" / "parable.js").is_file())
            self.assertTrue((installed / "lib" / "onboarding.js").is_file())
            self.assertTrue((installed / "skills" / "parable" / "SKILL.md").is_file())
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
            self.assertNotIn("In a new terminal", proc.stdout)

    def test_bundled_runtime_version_and_patch_match_package(self):
        package = json.loads((REPO / "package.json").read_text())
        version = (REPO / "skills" / "parable" / "runtime" / "VERSION").read_text().strip()
        self.assertEqual(version, package["version"])
        patch = (
            REPO / "skills" / "parable" / "runtime" / "patches"
            / "cliproxyapi-v7.2.88-claude-effort.patch"
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
                "grok-4.5",
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

    def test_interactive_and_all_vendor_configs_use_exact_models(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            proxy = self.make_proxy(root / "tools")
            interactive_home = root / "interactive"
            interactive_home.mkdir()
            interactive = self.run_cli(
                interactive_home,
                "setup", "--proxy-bin", str(proxy), "--no-auth",
                input_text="n\nn\n",
            )
            self.assertEqual(interactive.returncode, 0, interactive.stdout + interactive.stderr)
            interactive_manifest = json.loads(
                (interactive_home / ".config" / "parable" / "setup.json").read_text()
            )
            self.assertEqual(interactive_manifest["vendors"], ["claude"])

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
            self.assertIn('model = "grok-4.5"', claude_xai_config)
            self.assertNotIn("gpt-5.6-", claude_xai_config)

            all_home = root / "all"
            all_home.mkdir()
            all_vendors = self.run_cli(
                all_home,
                "setup", "--non-interactive",
                "--vendors", "xai,chatgpt,claude",
                "--proxy-bin", str(proxy), "--port", "9123", "--no-auth",
            )
            self.assertEqual(all_vendors.returncode, 0, all_vendors.stdout + all_vendors.stderr)
            config_dir = all_home / ".config" / "parable"
            manifest = json.loads((config_dir / "setup.json").read_text())
            self.assertEqual(manifest["vendors"], ["claude", "chatgpt", "xai"])
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
                "grok-4.5",
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
                'architecture=["fable_exact","opus_exact","sol_exact"]',
                config.replace(" ", ""),
            )
            self.assertNotIn("kimi", config.lower())

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
                                     ("claude,kimi", "unsupported vendor")):
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
                input="n\nn\nn\n",
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
                input="n\nn\ny\n",
                capture_output=True,
                text=True,
                timeout=60,
            )
            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("Build pinned commit", proc.stdout)
            self.assertIn("next: authorize each selected subscription, then run parable claude", proc.stdout)
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
                / "cliproxyapi-v7.2.88-claude-effort.patch"
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
}
for flag, (vendor, record_type) in mapping.items():
    if flag in sys.argv and os.environ.get("FAKE_PROXY_SKIP_AUTH") != vendor:
        target = Path.home() / ".cli-proxy-api" / f"fake-{vendor}.json"
        target.write_text(json.dumps({"type": record_type}))
        target.chmod(0o600)
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
                (("auth", "add", "kimi"), "unsupported auth vendor"),
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
                "other.json": {"type": "kimi", "access_token": "SECRET-KIMI"},
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
            })
            self.assertEqual(status["records"], {
                "total": 7,
                "userOnly": 4,
                "invalidMode": 2,
                "parseErrors": 1,
                "unrecognized": 1,
                "allModesValid": False,
            })
            self.assertTrue(status["directoryModeValid"])
            self.assertTrue(status["scanned"])
            forbidden = [
                "SECRET-", "private@example.invalid", "account-alpha", "bad-mode",
                "linked.json", str(auth_dir), str(outside), "kimi",
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
            "grok-4.5",
        ]
        expected_agents = {
            "parable-sol-exact": "gpt-5.6-sol",
            "parable-terra": "gpt-5.6-terra",
            "parable-luna": "gpt-5.6-luna",
            "parable-fable-exact": "claude-fable-5",
            "parable-sonnet-exact": "claude-sonnet-5",
            "parable-opus-exact": "claude-opus-4-8",
            "parable-haiku-exact": "claude-haiku-4-5-20251001",
            "parable-grok": "grok-4.5",
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
                "setup", "--non-interactive", "--vendors", "chatgpt,claude,xai",
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
            self.assertEqual(report["catalog"]["requiredCount"], 8)
            self.assertEqual(
                report["next"],
                "parable claude --brain auto -- --effort high",
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
                "unchanged": 8,
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
            self.assertEqual(
                captured["argv"],
                ["--model", "claude-fable-5", "--print", "hello"],
            )
            self.assertTrue(captured["auth_token_present"])
            self.assertFalse(captured["source_token_present"])

            auto = self.run_cli(
                repo, env, "claude", "--brain", "auto", "--", "--print", "auto"
            )
            self.assertEqual(auto.returncode, 0, auto.stdout + auto.stderr)
            self.assertIn("brain: claude-fable-5", auto.stdout)
            captured = json.loads(claude_capture.read_text())
            self.assertEqual(
                captured["argv"],
                ["--model", "claude-fable-5", "--print", "auto"],
            )

    def test_finalize_subset_and_missing_exact_id_fail_closed_without_aliases(self):
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
            self.assertIn("brain: claude-fable-5 (Sol is not configured; using Fable)", auto.stdout)
            captured = json.loads(claude_capture.read_text())
            self.assertEqual(
                captured["argv"],
                ["--model", "claude-fable-5", "--print", "claude-only"],
            )
            explicit_sol = self.run_cli(repo, env, "claude", "--brain", "sol")
            self.assertNotEqual(explicit_sol.returncode, 0)
            self.assertIn("requires configured catalog model 'gpt-5.6-sol'", explicit_sol.stderr)

        misleading = [
            "gpt-5.6-sol",
            "gpt-5.6-terra",
            "gpt-5.6-luna",
            "claude-fable-5",
            "claude-sonnet-5",
            "claude-opus-4-8",
            "claude-haiku-4-5-20251001",
            "grok-4.5-latest",
            "GROK-4.5",
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
            self.assertNotEqual(finalized.returncode, 0)
            self.assertIn("proxy model catalog is missing: grok-4.5", finalized.stderr)
            agents = repo / ".claude" / "agents"
            self.assertEqual(
                sorted(path.name for path in agents.iterdir()),
                ["handwritten.md", "parable-handwritten.md"],
            )
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
        "grok-4.5",
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
            self.assertEqual(report["catalog"]["requiredCount"], len(self.MODELS))
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
                FAKE_PROXY_EXIT_AFTER_MS="300",
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
