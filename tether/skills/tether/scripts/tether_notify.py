#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib.util
import json
import os
import shutil
import subprocess
import sys
import time
import uuid
from collections.abc import Sequence
from pathlib import Path
from types import ModuleType


DATA_HOME = Path(os.environ.get("XDG_DATA_HOME", Path.home() / ".local" / "share"))
RUNTIME_PATH = DATA_HOME / "tether" / "bridge_runtime.py"
SETUP_TIMEOUT_SECONDS = 900
SERVICE_TIMEOUT_SECONDS = 60


def _load_runtime(path: Path = RUNTIME_PATH) -> ModuleType:
    if not path.is_file():
        raise SystemExit("Tether runtime is not installed; run the package installer")
    spec = importlib.util.spec_from_file_location("tether_bridge_runtime", path)
    if spec is None or spec.loader is None:
        raise SystemExit("Tether runtime could not be loaded")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


_runtime = _load_runtime()
broker_call = _runtime.broker_call
doctor = _runtime.doctor
zellij_pane_identity = _runtime.zellij_pane_identity


def detected_source(args: argparse.Namespace) -> tuple[str, dict[str, str]]:
    cwd = str(Path.cwd())
    if getattr(args, "run_id", None):
        return "headless_run", {"run_id": args.run_id, "queue_id": args.run_id, "cwd": cwd}
    if getattr(args, "hermes_session_id", None):
        return "hermes_session", {"session_id": args.hermes_session_id, "cwd": cwd}
    terminal = {}
    terminal_identity = None
    if os.getenv("ZELLIJ_SESSION_NAME") and os.getenv("ZELLIJ_PANE_ID"):
        terminal_identity = zellij_pane_identity(
            os.environ["ZELLIJ_SESSION_NAME"],
            os.environ["ZELLIJ_PANE_ID"],
            cwd,
        )
        terminal = {
            "zellij_session": os.environ["ZELLIJ_SESSION_NAME"],
            "zellij_pane_id": os.environ["ZELLIJ_PANE_ID"],
            "pane_agent": terminal_identity["pane_agent"],
            "pane_command_hash": terminal_identity["pane_command_hash"],
        }
    if os.getenv("CLAUDE_CODE_SESSION_ID"):
        return "claude_session", {"session_id": os.environ["CLAUDE_CODE_SESSION_ID"], "cwd": cwd, **terminal}
    if os.getenv("CODEX_THREAD_ID"):
        return "codex_session", {"session_id": os.environ["CODEX_THREAD_ID"], "cwd": cwd, **terminal}
    if terminal:
        return "zellij_pane", terminal_identity
    raise SystemExit("No resumable context found; pass --run-id for a headless run")


def attached_source(args: argparse.Namespace) -> tuple[str, dict[str, str]]:
    cwd = str(Path(args.cwd or Path.cwd()).resolve())
    zellij_session = str(args.zellij_session or "")
    zellij_pane = str(args.zellij_pane_id or "")
    if bool(zellij_session) != bool(zellij_pane):
        raise SystemExit("--zellij-session and --zellij-pane-id must be provided together")
    terminal = {}
    terminal_identity = None
    if zellij_session:
        terminal_identity = zellij_pane_identity(zellij_session, zellij_pane, cwd)
        terminal = {
            "zellij_session": zellij_session,
            "zellij_pane_id": zellij_pane,
            "pane_agent": terminal_identity["pane_agent"],
            "pane_command_hash": terminal_identity["pane_command_hash"],
        }
    if args.claude_session_id and args.codex_session_id:
        raise SystemExit("choose one native session ID")
    if args.claude_session_id:
        return "claude_session", {
            "session_id": args.claude_session_id,
            "cwd": cwd,
            **terminal,
        }
    if args.codex_session_id:
        return "codex_session", {
            "session_id": args.codex_session_id,
            "cwd": cwd,
            **terminal,
        }
    if terminal_identity:
        return "zellij_pane", terminal_identity
    return detected_source(args)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Send or answer a resumable Hermes Slack thread")
    sub = parser.add_subparsers(dest="command", required=True)
    notify = sub.add_parser("notify")
    notify.add_argument("--text", required=True)
    notify.add_argument("--channel")
    notify.add_argument("--owner")
    notify.add_argument("--team")
    notify.add_argument("--idempotency-key", default="")
    notify.add_argument("--run-id")
    notify.add_argument("--hermes-session-id")
    notify.add_argument("--file")
    reply = sub.add_parser("reply")
    reply.add_argument("--bridge-id", required=True)
    reply.add_argument("--reply-key")
    reply.add_argument("--text", required=True)
    rebind = sub.add_parser("rebind")
    rebind.add_argument("--channel", required=True)
    rebind.add_argument("--thread-ts", required=True)
    attach = sub.add_parser("attach")
    attach.add_argument("--channel", required=True)
    attach.add_argument("--thread-ts", required=True)
    attach.add_argument("--owner")
    attach.add_argument("--team")
    attach.add_argument("--idempotency-key", required=True)
    attach.add_argument("--claude-session-id")
    attach.add_argument("--codex-session-id")
    attach.add_argument("--zellij-session")
    attach.add_argument("--zellij-pane-id")
    attach.add_argument("--run-id")
    attach.add_argument("--hermes-session-id")
    attach.add_argument("--cwd")
    attach.add_argument("--json", action="store_true")
    post = sub.add_parser("post")
    post.add_argument("--channel", required=True)
    post.add_argument("--thread-ts", required=True)
    post.add_argument("--text", required=True)
    history = sub.add_parser("history")
    history.add_argument("--channel")
    history.add_argument("--limit", type=int, default=15)
    thread = sub.add_parser("thread")
    thread.add_argument("--channel", required=True)
    thread.add_argument("--thread-ts", required=True)
    thread.add_argument("--limit", type=int, default=100)
    users = sub.add_parser("users")
    users.add_argument("--query", required=True)
    dm = sub.add_parser("dm")
    dm.add_argument("--user", action="append", required=True)
    dm.add_argument("--text", required=True)
    dm.add_argument("--team")
    dm.add_argument("--idempotency-key", required=True)
    dm.add_argument("--run-id")
    dm.add_argument("--hermes-session-id")
    sub.add_parser("doctor")
    sub.add_parser("identity")
    setup = sub.add_parser("setup")
    setup.add_argument("--non-interactive", action="store_true")
    setup.add_argument("--no-restart", action="store_true")
    return parser


def _run_hermes(command: list[str], timeout: int) -> subprocess.CompletedProcess[str]:
    # The Hermes executable is resolved to an absolute path by shutil.which.
    return subprocess.run(command, timeout=timeout, text=True)  # nosec B603


def _find_hermes() -> str | None:
    configured = os.getenv("HERMES_BIN", "").strip()
    candidates = [
        configured,
        shutil.which("hermes") or "",
        str(Path.home() / ".local" / "bin" / "hermes"),
        str(Path(os.getenv("HERMES_HOME", Path.home() / ".hermes")) / "hermes-agent" / "venv" / "bin" / "hermes"),
    ]
    return next((path for path in candidates if path and os.path.isfile(path) and os.access(path, os.X_OK)), None)


def _enable_plugin(hermes: str) -> int:
    enabled = _run_hermes([hermes, "plugins", "enable", "tether"], SERVICE_TIMEOUT_SECONDS)
    if enabled.returncode:
        print("Tether was installed but Hermes could not enable its plugin.", file=sys.stderr)
        return enabled.returncode
    hermes_home = Path(os.getenv("HERMES_HOME", Path.home() / ".hermes"))
    if (hermes_home / "plugins" / "session-bridge").is_dir():
        disabled = _run_hermes(
            [hermes, "plugins", "disable", "session-bridge"],
            SERVICE_TIMEOUT_SECONDS,
        )
        if disabled.returncode:
            print("Refusing to run Tether alongside the legacy session-bridge plugin.", file=sys.stderr)
            return disabled.returncode
    return 0


def _configure_peer_agents(hermes: str) -> int:
    settings = (
        ("slack.allow_bots", "all"),
        ("display.busy_ack_enabled", "false"),
    )
    for key, value in settings:
        configured = _run_hermes(
            [hermes, "config", "set", key, value],
            SERVICE_TIMEOUT_SECONDS,
        )
        if configured.returncode:
            print(f"Tether could not configure {key} for peer-agent threads.", file=sys.stderr)
            return configured.returncode
    return 0


def run_setup(args: argparse.Namespace) -> int:
    hermes = _find_hermes()
    if not hermes:
        print(
            "Hermes Agent is required. Install it from "
            "https://github.com/NousResearch/hermes-agent, then run `tether setup` again.",
            file=sys.stderr,
        )
        return 2
    plugin_result = _enable_plugin(hermes)
    if plugin_result:
        return plugin_result
    peer_result = _configure_peer_agents(hermes)
    if peer_result:
        return peer_result
    if args.non_interactive:
        result = _run_hermes([hermes, "slack", "manifest", "--write"], SERVICE_TIMEOUT_SECONDS)
        if result.returncode:
            return result.returncode
        print("Slack manifest generated. Run `hermes gateway setup`, then `tether doctor`.")
        return 0

    print("Tether will now open Hermes's Slack setup. It generates the current app manifest,")
    print("finishes the private Socket Mode configuration, and sets your operator allowlist.")
    result = _run_hermes([hermes, "gateway", "setup"], SETUP_TIMEOUT_SECONDS)
    if result.returncode:
        return result.returncode

    if not args.no_restart:
        restarted = _run_hermes([hermes, "gateway", "restart"], SERVICE_TIMEOUT_SECONDS)
        if restarted.returncode:
            print("No running gateway service found; installing it now.")
            installed = _run_hermes([hermes, "gateway", "install"], SERVICE_TIMEOUT_SECONDS)
            if installed.returncode == 0:
                started = _run_hermes([hermes, "gateway", "start"], SERVICE_TIMEOUT_SECONDS)
                if started.returncode:
                    return started.returncode

    deadline = time.monotonic() + 15
    while True:
        ok, checks = doctor()
        if ok or time.monotonic() >= deadline:
            break
        time.sleep(0.5)
    print("\n".join(checks))
    if not ok:
        print("Tether is installed, but the gateway is not ready yet. Fix the FAIL lines and rerun `tether doctor`.")
        return 1
    print("Tether is ready. Ask your agent: ‘Let me know in Slack when this is done.’")
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "doctor":
        ok, checks = doctor()
        print("\n".join(checks))
        return 0 if ok else 1
    if args.command == "identity":
        print(json.dumps(broker_call({"op": "identity"}), ensure_ascii=False))
        return 0
    if args.command == "setup":
        return run_setup(args)
    if args.command == "reply":
        result = broker_call({
            "op": "reply", "bridge_id": args.bridge_id,
            "reply_key": args.reply_key or "", "text": args.text,
        })
    elif args.command == "rebind":
        kind, source = detected_source(args)
        result = broker_call({
            "op": "rebind", "channel_id": args.channel,
            "thread_ts": args.thread_ts, "source_kind": kind, "source": source,
        })
    elif args.command == "attach":
        kind, source = attached_source(args)
        result = broker_call({
            "op": "attach", "source_kind": kind, "source": source,
            "owner_user_id": args.owner or "", "channel_id": args.channel,
            "team_id": args.team or "", "thread_ts": args.thread_ts,
            "idempotency_key": args.idempotency_key,
        })
        if args.json:
            print(json.dumps(result, ensure_ascii=False))
            return 0
    elif args.command == "post":
        result = broker_call({
            "op": "thread_reply", "channel_id": args.channel,
            "thread_ts": args.thread_ts, "text": args.text,
        })
    elif args.command == "history":
        result = broker_call({"op": "history", "channel_id": args.channel or "", "limit": args.limit})
        print(json.dumps(result["messages"], ensure_ascii=False))
        return 0
    elif args.command == "thread":
        result = broker_call({
            "op": "thread_history",
            "channel_id": args.channel,
            "thread_ts": args.thread_ts,
            "limit": args.limit,
        })
        print(json.dumps(result["messages"], ensure_ascii=False))
        return 0
    elif args.command == "users":
        result = broker_call({"op": "users", "query": args.query})
        print(json.dumps(result["users"], ensure_ascii=False))
        return 0
    elif args.command == "dm":
        kind, source = detected_source(args)
        result = broker_call({
            "op": "dm", "text": args.text, "source_kind": kind, "source": source,
            "user_ids": args.user, "team_id": args.team or "",
            "idempotency_key": args.idempotency_key,
        })
        print(result["channel_id"])
        return 0
    else:
        kind, source = detected_source(args)
        result = broker_call({
            "op": "notify", "text": args.text, "source_kind": kind, "source": source,
            "owner_user_id": args.owner or "", "channel_id": args.channel or "", "team_id": args.team or "",
            "idempotency_key": args.idempotency_key or str(uuid.uuid4()), "file_path": args.file,
        })
    print(result["thread_ts"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
