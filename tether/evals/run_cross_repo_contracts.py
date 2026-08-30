#!/usr/bin/env python3
"""Reproduce Tether caller-contract drift against an exact clean Parcha tree."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import pathlib
import re
import subprocess  # nosec B404 - fixed local Git/Python invocations
import sys
import tempfile
from typing import Any


WRAPPER = pathlib.Path(
    ".claude/skills/greppy-machine-setup/assets/hermes/bin/hermes-run-claude.sh.tmpl"
)
HOOK = pathlib.Path(
    ".claude/skills/greppy-machine-setup/assets/hermes/bin/hermes-claude-slack-hook.py"
)
SIGNUP = pathlib.Path(
    ".claude/skills/greppy-machine-setup/assets/hermes/bin/hermes-signup-replay.sh.tmpl"
)
HISTORICAL_SIGNUP_COMMIT = "52b7173d23"


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def git(repo: pathlib.Path, *args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(  # nosec B603 - no shell; reviewed arguments
        ["/usr/bin/git", *args],
        cwd=repo,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=check,
    )


def hook_probe(repo: pathlib.Path) -> dict[str, Any]:
    with tempfile.TemporaryDirectory() as directory:
        home = pathlib.Path(directory)
        tether = home / ".local/bin/tether"
        tether.parent.mkdir(parents=True)
        argv_file = home / "argv.json"
        tether.write_text(
            "#!/usr/bin/env python3\n"
            "import json,os,sys\n"
            "open(os.environ['TETHER_ARGV_FILE'],'w').write(json.dumps(sys.argv[1:]))\n"
            "raise SystemExit(2)\n",
            encoding="utf-8",
        )
        tether.chmod(0o700)
        env = {
            **os.environ,
            "HOME": str(home),
            "HERMES_RELAY_CHANNEL": "C12345678",
            "HERMES_RELAY_THREAD": "1785000000.000001",
            "TETHER_ARGV_FILE": str(argv_file),
        }
        event = json.dumps(
            {"hook_event_name": "Stop", "last_assistant_message": "synthetic complete"}
        )
        completed = subprocess.run(  # nosec B603 - exact reviewed source path
            [sys.executable, str(repo / HOOK)],
            input=event,
            env=env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=20,
            check=False,
        )
        argv = json.loads(argv_file.read_text(encoding="utf-8")) if argv_file.is_file() else []
        observed = {
            "hook_exit_code": completed.returncode,
            "invoked_tether": bool(argv),
            "tether_subcommand": argv[0] if argv else None,
            "has_idempotency_key": "--idempotency-key" in argv,
            "failure_logged": "Tether exited 2" in completed.stderr,
        }
        return {
            "id": "hook_idempotency_key",
            "classification": "cross_repo_defect_observed",
            "observed": observed,
            "passed": (
                observed["hook_exit_code"] == 0
                and observed["invoked_tether"]
                and observed["tether_subcommand"] == "post"
                and not observed["has_idempotency_key"]
                and observed["failure_logged"]
            ),
            "falsifier": "The real hook supplies --idempotency-key or propagates the rejected write.",
        }


def wrapper_probe(repo: pathlib.Path) -> dict[str, Any]:
    text = (repo / WRAPPER).read_text(encoding="utf-8")
    match = re.search(
        r'"\$HOME/\.local/bin/tether" reply(?P<body>.*?)\n\s*fi', text, re.DOTALL
    )
    body = match.group("body") if match else ""
    observed = {
        "reply_call_found": match is not None,
        "has_reply_key": "--reply-key" in body,
        "suppresses_failure": "|| true" in body,
        "attach_parser_fields": sorted(
            set(re.findall(r'\["([a-z_]+)"\]', text[text.find("ATTACH_JSON"):text.find("if [ -n", text.find("ATTACH_JSON"))]))
        ),
    }
    return {
        "id": "wrapper_reply_key",
        "classification": "cross_repo_defect_observed",
        "observed": observed,
        "passed": (
            observed["reply_call_found"]
            and not observed["has_reply_key"]
            and observed["suppresses_failure"]
            and observed["attach_parser_fields"] == ["bridge_id"]
        ),
        "falsifier": "The real wrapper parses/passes reply_key or no longer hides reply failure.",
    }


def signup_probe(repo: pathlib.Path) -> dict[str, Any]:
    historical = git(repo, "show", f"{HISTORICAL_SIGNUP_COMMIT}:{SIGNUP}").stdout
    current = (repo / SIGNUP).read_text(encoding="utf-8")
    historical_run = '/run/greppy-hermes/__INSTANCE__/signup-intake' in historical
    current_private_state = 'signup-intake' in current and 'install -d -m 0750' in current
    current_read_check = '/usr/bin/test -r "$intake_file"' in current
    return {
        "id": "signup_intake_path",
        "classification": "historical_defect_and_current_control_observed",
        "observed": {
            "historical_commit": HISTORICAL_SIGNUP_COMMIT,
            "historical_used_ephemeral_run_path": historical_run,
            "current_uses_private_state_intake": current_private_state,
            "current_preflights_reader_access": current_read_check,
            "historical_source_sha256": sha256(historical.encode()),
            "current_source_sha256": sha256(current.encode()),
        },
        "passed": historical_run and current_private_state and current_read_check,
        "falsifier": "Historical source lacks the /run path or current source lacks durable private intake/readability preflight.",
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--fleet-repo", required=True, type=pathlib.Path)
    parser.add_argument("--expected-commit", required=True)
    parser.add_argument("--output", required=True, type=pathlib.Path)
    args = parser.parse_args()

    repo = args.fleet_repo.resolve()
    head = git(repo, "rev-parse", "HEAD").stdout.strip()
    status = git(repo, "status", "--porcelain=v1", "--untracked-files=all").stdout
    detached = git(repo, "symbolic-ref", "-q", "HEAD", check=False).returncode != 0
    sources = {
        str(path): sha256((repo / path).read_bytes()) for path in (WRAPPER, HOOK, SIGNUP)
    }
    results = [wrapper_probe(repo), hook_probe(repo), signup_probe(repo)]
    valid = (
        head == args.expected_commit
        and status == ""
        and detached
        and all(result["passed"] for result in results)
    )
    report = {
        "schema_version": 1,
        "repository": "Parcha-ai/parcha",
        "target_commit": head,
        "expected_commit": args.expected_commit,
        "clean": status == "",
        "detached_head": detached,
        "source_sha256": sources,
        "python": sys.version.split()[0],
        "command": [
            "run_cross_repo_contracts.py", "--fleet-repo", "<clean-detached-worktree>",
            "--expected-commit", args.expected_commit, "--output", "<report>",
        ],
        "results": results,
        "valid": valid,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return 0 if valid else 1


if __name__ == "__main__":
    raise SystemExit(main())
