"""Shared test doubles: a scriptable stand-in for `claude -p` speaking stream-json."""

from __future__ import annotations

import os
import stat
import textwrap
from pathlib import Path

# Per user turn: if FAKE_REPLY is set, run it with /bin/sh and use its stdout as the
# result (non-zero exit -> the harness exits with that code after writing stderr);
# otherwise echo a counter so tests can prove one process served several turns.
FAKE_CLAUDE = textwrap.dedent(
    """\
    #!/usr/bin/env python3
    import json, os, subprocess, sys, time
    sid = sys.argv[sys.argv.index("--resume") + 1] if "--resume" in sys.argv else "fresh"
    n = 0
    log = os.environ.get("FAKE_LOG")
    prompts = os.environ.get("FAKE_PROMPTS")
    script = os.environ.get("FAKE_REPLY")
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        n += 1
        text = json.loads(line)["message"]["content"][0]["text"]
        if log:
            open(log, "a").write(f"{os.getpid()} {n} {text[:40]}\\n")
        if prompts:
            open(prompts, "a").write(text + "\\n===\\n")
        if script:
            done = subprocess.run(["/bin/sh", "-c", script], capture_output=True, text=True)
            if done.returncode != 0:
                sys.stderr.write(done.stdout + done.stderr); sys.stderr.flush(); sys.exit(done.returncode)
            reply = done.stdout
        else:
            if "SLEEP" in text:
                time.sleep(5)
            if "CRASH" in text:
                sys.stderr.write("You've hit your session limit\\n"); sys.exit(1)
            if "SILENT" in text:
                reply = "nothing to add here\\nNO_REPLY"
            elif "ERRORFLAG" in text:
                print(json.dumps({"type": "result", "is_error": True, "result": "API overloaded", "session_id": sid})); sys.stdout.flush(); continue
            else:
                reply = f"turn {n} of pid {os.getpid()}: {text.splitlines()[-1][:60]}"
        print(json.dumps({"type": "assistant", "session_id": sid}))
        print(json.dumps({"type": "result", "result": reply, "session_id": sid, "is_error": False}))
        sys.stdout.flush()
    """
)


def write_fake_claude(directory: Path) -> Path:
    path = Path(directory) / "fake-claude"
    path.write_text(FAKE_CLAUDE, encoding="utf-8")
    path.chmod(path.stat().st_mode | stat.S_IEXEC)
    return path


def direct_launch(command, cwd, env, settings):
    return command, env, "direct"


def child_env(passthrough=()):
    keys = ("PATH", "FAKE_LOG", "FAKE_PROMPTS", "FAKE_REPLY", *passthrough)
    return {k: os.environ[k] for k in keys if k in os.environ}


class Descriptor:
    def __init__(self, owners=("U12345678",), workspace_id="T12345678"):
        self.authorized_owner_ids = tuple(owners)
        self.canonical_owner_ids = tuple(owners)
        self.workspace_id = workspace_id
        self.persona_id = "primary"
        self.policy_generation = 1
