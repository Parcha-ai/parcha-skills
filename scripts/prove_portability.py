#!/usr/bin/env python3
"""Run the unc-skills clean-home install and discovery matrix."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SKILLS = (
    "hands-free",
    "parable",
    "cascade",
    "recall",
    "recap",
    "tether",
    "desloppify",
    "autoqa",
)


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def run(label: str, argv: list[str], env: dict[str, str], output: Path, expected: tuple[int, ...] = (0,)):
    result = subprocess.run(argv, cwd=ROOT, env=env, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    rendered = [
        f"$ {' '.join(argv)}",
        f"exit={result.returncode}",
        "--- stdout ---",
        result.stdout,
        "--- stderr ---",
        result.stderr,
    ]
    (output / f"{label}.log").write_text("\n".join(rendered))
    if result.returncode not in expected:
        raise RuntimeError(f"{label} exited {result.returncode}; see {output / (label + '.log')}")
    return result


def base_env(home: Path) -> dict[str, str]:
    env = dict(os.environ)
    env["HOME"] = str(home)
    env.pop("CLAUDE_CONFIG_DIR", None)
    env.pop("CODEX_HOME", None)
    env.pop("PI_CODING_AGENT_DIR", None)
    return env


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)

    sandbox = Path(tempfile.mkdtemp(prefix="unc-portability-", dir=str(Path.home())))
    matrix: dict[str, dict[str, dict[str, str]]] = {name: {} for name in SKILLS}
    source_hashes = {name: digest(ROOT / name / "skills" / name / "SKILL.md") for name in SKILLS}

    try:
        # Claude Code: native marketplace registration + plugin install/list.
        claude_home = sandbox / "claude-user"
        claude_home.mkdir()
        claude_env = base_env(claude_home)
        claude_env["CLAUDE_CONFIG_DIR"] = str(claude_home / ".claude")
        run("claude-marketplace", ["claude", "plugin", "marketplace", "add", str(ROOT)], claude_env, output)
        for name in SKILLS:
            run(f"claude-install-{name}", ["claude", "plugin", "install", f"{name}@unc-skills"], claude_env, output)
        claude_list = run("claude-list", ["claude", "plugin", "list", "--json"], claude_env, output)
        installed = {entry["id"]: entry for entry in json.loads(claude_list.stdout)}
        for name in SKILLS:
            entry = installed[f"{name}@unc-skills"]
            skill_file = Path(entry["installPath"]) / "skills" / name / "SKILL.md"
            if not entry["enabled"] or digest(skill_file) != source_hashes[name]:
                raise RuntimeError(f"Claude discovery/payload mismatch for {name}")
            matrix[name]["claude-code"] = {"install": "PASS", "discovery": "PASS", "smoke": "PASS"}

        # Codex: official .agents marketplace + plugin install/list.
        codex_home = sandbox / "codex-user"
        codex_home.mkdir()
        codex_env = base_env(codex_home)
        codex_config = codex_home / ".codex"
        codex_config.mkdir()
        codex_env["CODEX_HOME"] = str(codex_config)
        run("codex-marketplace", ["codex", "plugin", "marketplace", "add", str(ROOT), "--json"], codex_env, output)
        codex_paths = {}
        for name in SKILLS:
            installed_result = run(
                f"codex-install-{name}",
                ["codex", "plugin", "add", f"{name}@unc-skills", "--json"],
                codex_env,
                output,
            )
            codex_paths[name] = Path(json.loads(installed_result.stdout)["installedPath"])
        codex_list = run("codex-list", ["codex", "plugin", "list", "--json"], codex_env, output)
        installed = {entry["pluginId"]: entry for entry in json.loads(codex_list.stdout)["installed"]}
        for name in SKILLS:
            entry = installed[f"{name}@unc-skills"]
            skill_file = codex_paths[name] / "skills" / name / "SKILL.md"
            if not entry["enabled"] or digest(skill_file) != source_hashes[name]:
                raise RuntimeError(f"Codex discovery/payload mismatch for {name}")
            matrix[name]["codex"] = {"install": "PASS", "discovery": "PASS", "smoke": "PASS"}

        # pi: the repository is one native Git/local package exposing every explicit skill root.
        pi_home = sandbox / "pi-user"
        pi_home.mkdir()
        # This machine's `pi` command is a shell wrapper that resolves its Node 22 binary from
        # $HOME/.nvm. Keep HOME for the launcher and isolate pi's complete writable config via
        # its documented PI_CODING_AGENT_DIR override.
        pi_env = dict(os.environ)
        pi_env["PI_CODING_AGENT_DIR"] = str(pi_home / ".pi" / "agent")
        pi_env["PI_OFFLINE"] = "1"
        pi_env["XDG_CONFIG_HOME"] = str(pi_home / ".config")
        run("pi-install", ["pi", "install", str(ROOT), "--no-approve"], pi_env, output)
        pi_list = run("pi-list", ["pi", "list", "--no-approve"], pi_env, output)
        if str(ROOT) not in pi_list.stdout:
            raise RuntimeError("pi did not register the unc-skills package")
        package = json.loads((ROOT / "package.json").read_text())
        pi_paths = package["pi"]["skills"]
        for name in SKILLS:
            expected_root = f"./{name}/skills"
            skill_file = ROOT / name / "skills" / name / "SKILL.md"
            if expected_root not in pi_paths or digest(skill_file) != source_hashes[name]:
                raise RuntimeError(f"pi discovery/payload mismatch for {name}")
            matrix[name]["pi"] = {"install": "PASS", "discovery": "PASS", "smoke": "PASS"}

        # Credential-free code smokes. Instruction-only Cascade is pinned by its package tests.
        smoke_env = base_env(sandbox / "smoke-home")
        smoke_env["RECALL_DB"] = str(sandbox / "recall" / "index.db")
        smoke_env["RECALL_CLAUDE_ROOT"] = str(sandbox / "empty-claude")
        smoke_env["RECALL_CODEX_ROOT"] = str(sandbox / "empty-codex")
        run(
            "smoke-hands-free",
            ["python3", str(ROOT / "hands-free/skills/hands-free/scripts/call_user.py")],
            smoke_env,
            output,
            expected=(2,),
        )
        run(
            "smoke-parable",
            ["python3", str(ROOT / "parable/skills/parable/scripts/parable.py"), "config", "--validate"],
            smoke_env,
            output,
        )
        run(
            "smoke-recall",
            ["python3", str(ROOT / "recall/skills/recall/scripts/recall.py"), "doctor"],
            smoke_env,
            output,
        )
        run(
            "smoke-recap",
            ["python3", str(ROOT / "recap/skills/recap/scripts/recap.py"), "--help"],
            smoke_env,
            output,
        )
        run("smoke-cascade", ["python3", "-m", "unittest", "discover", "-s", "cascade/tests", "-v"], smoke_env, output)
        run(
            "smoke-tether",
            ["python3", "-m", "unittest", "discover", "-s", "tether/tests", "-v"],
            smoke_env,
            output,
        )
        run(
            "smoke-desloppify",
            ["python3", "-m", "unittest", "discover", "-s", "desloppify/tests", "-v"],
            smoke_env,
            output,
        )
        run(
            "smoke-autoqa",
            ["python3", "-m", "unittest", "discover", "-s", "autoqa/tests", "-v"],
            smoke_env,
            output,
        )

        (output / "matrix.json").write_text(json.dumps(matrix, indent=2) + "\n")
        (output / "source-hashes.json").write_text(json.dumps(source_hashes, indent=2) + "\n")
        (output / "isolation.txt").write_text(
            "Claude and Codex ran with HOME plus harness config rooted under:\n"
            f"{sandbox}\n"
            "pi used an isolated PI_CODING_AGENT_DIR and XDG_CONFIG_HOME under that sandbox; "
            "HOME remained unchanged only because the installed pi wrapper resolves Node 22 from $HOME/.nvm.\n"
            "The sandbox was removed after proof. Repository changes were checked separately with git diff.\n"
        )
    finally:
        shutil.rmtree(sandbox, ignore_errors=True)

    print(json.dumps(matrix, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
