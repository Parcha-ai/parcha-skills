#!/usr/bin/env python3
"""Herdr-native control surface for the credential-isolated Tether broker."""

from __future__ import annotations

import argparse
import curses
import hashlib
import json
import os
import shutil
import stat
import subprocess  # nosec B404 - fixed executables and argv-only commands
import sys
import time
import uuid
from pathlib import Path
from typing import Any


PLUGIN_ID = "parcha.tether"
MAX_CONTEXT_BYTES = 64 * 1024
MAX_RESULT_BYTES = 1024 * 1024
CONTEXT_FIELDS = frozenset({
    "workspace_id",
    "workspace_label",
    "workspace_cwd",
    "tab_id",
    "tab_label",
    "focused_pane_id",
    "focused_pane_cwd",
    "focused_pane_agent",
    "focused_pane_status",
    "selected_text",
    "invocation_source",
    "correlation_id",
    "clicked_url",
    "link_handler_id",
})


class PluginError(RuntimeError):
    pass


def _context_from_environment() -> dict[str, str]:
    raw = os.environ.get("HERDR_PLUGIN_CONTEXT_JSON", "")
    if not raw or len(raw.encode("utf-8")) > MAX_CONTEXT_BYTES:
        return {}
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    if not isinstance(payload, dict):
        return {}
    return {
        key: value[:8192]
        for key, value in payload.items()
        if key in CONTEXT_FIELDS and isinstance(value, str) and value
    }


def _secure_state_directory() -> Path:
    raw = os.environ.get("HERDR_PLUGIN_STATE_DIR", "")
    if not raw:
        raise PluginError("Herdr did not provide a plugin state directory")
    path = Path(raw)
    if not path.is_absolute():
        raise PluginError("Herdr plugin state directory is not absolute")
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    info = path.lstat()
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
        raise PluginError("Herdr plugin state path is unsafe")
    if info.st_uid != os.geteuid():
        raise PluginError("Herdr plugin state directory has the wrong owner")
    os.chmod(path, 0o700)
    return path


def _purge_stale_contexts(state: Path) -> None:
    cutoff = time.time() - 3600
    for candidate in state.glob("invocation-*.json"):
        try:
            info = candidate.lstat()
            if stat.S_ISREG(info.st_mode) and info.st_uid == os.geteuid() and info.st_mtime < cutoff:
                candidate.unlink()
        except OSError:
            continue


def save_invocation(context: dict[str, str], mode: str) -> str:
    state = _secure_state_directory()
    _purge_stale_contexts(state)
    token = uuid.uuid4().hex
    target = state / f"invocation-{token}.json"
    payload = json.dumps(
        {"mode": mode, "context": context},
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    if len(payload) > MAX_CONTEXT_BYTES:
        raise PluginError("Herdr invocation context is too large")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(target, flags, 0o600)
    with os.fdopen(descriptor, "wb") as stream:
        stream.write(payload)
    return token


def load_invocation(token: str) -> tuple[str, dict[str, str]]:
    if len(token) != 32 or any(char not in "0123456789abcdef" for char in token):
        raise PluginError("invalid cockpit invocation")
    target = _secure_state_directory() / f"invocation-{token}.json"
    info = target.lstat()
    if (
        not stat.S_ISREG(info.st_mode)
        or info.st_uid != os.geteuid()
        or stat.S_IMODE(info.st_mode) != 0o600
        or info.st_nlink != 1
        or info.st_size > MAX_CONTEXT_BYTES
    ):
        raise PluginError("cockpit invocation state is unsafe")
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    finally:
        target.unlink(missing_ok=True)
    context = payload.get("context") if isinstance(payload, dict) else None
    mode = payload.get("mode") if isinstance(payload, dict) else None
    if not isinstance(context, dict) or not isinstance(mode, str):
        raise PluginError("cockpit invocation state is malformed")
    return mode, {
        key: value
        for key, value in context.items()
        if key in CONTEXT_FIELDS and isinstance(value, str)
    }


def _herdr_binary() -> str:
    candidate = os.environ.get("HERDR_BIN_PATH", "") or shutil.which("herdr") or ""
    if not candidate:
        raise PluginError("Herdr executable is unavailable")
    return candidate


def _tether_binary() -> str:
    candidate = shutil.which("tether") or str(Path.home() / ".local" / "bin" / "tether")
    if not Path(candidate).is_file() or not os.access(candidate, os.X_OK):
        raise PluginError("Tether core is not installed; run `tether setup --herdr`")
    return candidate


def open_cockpit(mode: str) -> int:
    context = _context_from_environment()
    token = save_invocation(context, mode)
    command = [
        _herdr_binary(),
        "plugin",
        "pane",
        "open",
        "--plugin",
        PLUGIN_ID,
        "--entrypoint",
        "cockpit",
        "--env",
        f"TETHER_INVOCATION_ID={token}",
        "--focus",
    ]
    # Herdr popup panes always target the active pane and reject an explicit
    # --target-pane. The single-use invocation record preserves the exact
    # pane context that Tether independently verifies inside the cockpit.
    result = subprocess.run(command, check=False)  # nosec B603
    if result.returncode != 0:
        (_secure_state_directory() / f"invocation-{token}.json").unlink(missing_ok=True)
    return int(result.returncode)


def run_json(command: list[str], *, input_text: str | None = None) -> dict[str, Any]:
    result = subprocess.run(  # nosec B603
        command,
        input=input_text,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=65,
        check=False,
    )
    output = result.stdout.encode("utf-8", errors="replace")
    if len(output) > MAX_RESULT_BYTES:
        raise PluginError("Tether returned an oversized response")
    if result.returncode != 0:
        detail = result.stderr.strip().splitlines()[-1] if result.stderr.strip() else "command failed"
        raise PluginError(detail[:500])
    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise PluginError("Tether returned malformed JSON") from exc
    if not isinstance(payload, dict):
        raise PluginError("Tether returned an invalid response")
    return payload


def status_for(context: dict[str, str]) -> dict[str, Any]:
    pane = context.get("focused_pane_id", "")
    if not pane:
        raise PluginError("Focus a Herdr agent pane, then reopen Tether")
    command = [_tether_binary(), "herdr", "status", "--pane", pane, "--json"]
    cwd = context.get("focused_pane_cwd", "")
    if cwd:
        command.extend(["--cwd", cwd])
    return run_json(command)


def _idempotency_key(action: str, status: dict[str, Any], value: str) -> str:
    material = "\0".join((action, str(status.get("terminal_id", "")), value))
    return "herdr-" + hashlib.sha256(material.encode("utf-8")).hexdigest()[:32]


def _prompt(screen: Any, row: int, label: str, initial: str = "") -> str:
    height, width = screen.getmaxyx()
    screen.move(min(row, height - 1), 0)
    screen.clrtoeol()
    screen.addnstr(min(row, height - 1), 0, label, max(1, width - 1))
    curses.echo()
    try:
        value = screen.getstr(
            min(row, height - 1),
            min(len(label), max(0, width - 2)),
            max(1, width - len(label) - 2),
        ).decode("utf-8", errors="replace")
    finally:
        curses.noecho()
    return value or initial


def _confirm(screen: Any, row: int, action: str) -> bool:
    return _prompt(screen, row, f"Type {action} to confirm: ").strip().lower() == action


def _draw(screen: Any, status: dict[str, Any], notice: str) -> None:
    screen.erase()
    height, width = screen.getmaxyx()
    bound = bool(status.get("bound"))
    bridge = status.get("bridge") if isinstance(status.get("bridge"), dict) else {}
    lines = [
        "+ TETHER / HERDR " + "-" * max(0, min(48, width - 18)),
        f"agent       {status.get('agent') or 'unknown'}",
        f"pane        {status.get('pane_id') or 'unavailable'}",
        f"binding     {bridge.get('binding_state', 'active') if bound else 'unbound'}",
        f"thread      {bridge.get('channel_id', '-')} / {bridge.get('thread_ts', '-')}",
        f"generation  {bridge.get('binding_generation', '-')}",
        f"queue       {status.get('queued', 0)} pending / {status.get('uncertain', 0)} uncertain",
        "",
        "[c] create   [a] attach   [r] rebind   [d] detach",
        "[u] unresolved   [h] doctor   [space] refresh   [q] quit",
    ]
    if not status.get("named"):
        lines.extend(["", "Create/attach/rebind will assign a visible occupant-bound Tether name."])
    if notice:
        lines.extend(["", notice])
    for row, line in enumerate(lines[: max(0, height - 1)]):
        screen.addnstr(row, 0, str(line), max(1, width - 1))
    screen.refresh()


def _run_action(
    screen: Any,
    context: dict[str, str],
    status: dict[str, Any],
    key: str,
    prefill: str,
) -> str:
    tether = _tether_binary()
    pane = context.get("focused_pane_id", "")
    bridge = status.get("bridge") if isinstance(status.get("bridge"), dict) else {}
    if key == "c":
        channel = _prompt(screen, 14, "Slack channel ID (blank uses configured default): ")
        message = _prompt(screen, 15, "Initial message: ")
        if not message or not _confirm(screen, 16, "create"):
            return "Create cancelled."
        command = [
            tether, "herdr", "create", "--pane", pane,
            "--idempotency-key", _idempotency_key("create", status, channel + "\0" + message),
            "--text-stdin", "--json",
        ]
        if channel:
            command.extend(["--channel", channel])
        result = run_json(command, input_text=message)
        return f"Created thread {result.get('thread_ts', '')}."
    if key == "a":
        slack_url = _prompt(screen, 14, "Slack thread URL: ", prefill)
        if not slack_url or not _confirm(screen, 15, "attach"):
            return "Attach cancelled."
        result = run_json([
            tether, "herdr", "attach", "--pane", pane,
            "--slack-url-stdin",
            "--idempotency-key", _idempotency_key("attach", status, slack_url),
            "--json",
        ], input_text=slack_url)
        return f"Attached thread {result.get('thread_ts', '')}."
    if key == "r":
        if not bridge:
            return "No active binding to rebind."
        if not _confirm(screen, 14, "rebind"):
            return "Rebind cancelled."
        result = run_json([
            tether, "herdr", "rebind", "--pane", pane,
            "--channel", str(bridge.get("channel_id", "")),
            "--thread-ts", str(bridge.get("thread_ts", "")), "--json",
        ])
        return f"Rebound generation {result.get('binding_generation', '')}."
    if key == "d":
        if not bridge:
            return "No active binding to detach."
        if not _confirm(screen, 14, "detach"):
            return "Detach cancelled."
        result = run_json([
            tether, "herdr", "detach",
            "--bridge-id", str(bridge.get("bridge_id", "")),
            "--expected-generation", str(bridge.get("binding_generation", "")),
            "--json",
        ])
        return f"Detached {result.get('bridge_id', '')}."
    if key == "h":
        result = run_json([tether, "doctor", "--json"])
        checks = result.get("checks") if isinstance(result.get("checks"), list) else []
        return "Doctor: " + "; ".join(str(item) for item in checks[-3:])
    if key == "u":
        result = run_json([tether, "unresolved", "--json"])
        operations = result.get("operations") if isinstance(result.get("operations"), list) else []
        if not operations:
            return "No unresolved work."
        operation = operations[0] if isinstance(operations[0], dict) else {}
        choice = _prompt(screen, 14, "First unresolved item: retry, complete, abandon, or cancel: ").strip()
        if choice not in {"retry", "complete", "abandon"}:
            return "Resolution cancelled."
        if not _confirm(screen, 15, choice):
            return "Resolution cancelled."
        run_json([
            tether, "resolve", "--kind", str(operation.get("kind", "")),
            "--id", str(operation.get("id", "")), "--action", choice, "--json",
        ])
        return f"Resolved {operation.get('kind', '')} with {choice}."
    return ""


def cockpit(screen: Any, mode: str, context: dict[str, str]) -> None:
    curses.curs_set(0)
    notice = ""
    prefill = context.get("clicked_url") or context.get("selected_text") or ""
    status = status_for(context)
    pending_mode = mode
    while True:
        _draw(screen, status, notice)
        if pending_mode == "rebind":
            key = "r"
            pending_mode = ""
        elif pending_mode == "attach" and prefill:
            key = "a"
            pending_mode = ""
        else:
            pressed = screen.getch()
            if pressed in (ord("q"), 27):
                return
            if pressed == ord(" "):
                key = ""
            else:
                key = chr(pressed).lower() if 0 <= pressed < 256 else ""
        try:
            if key in {"c", "a", "r", "d", "u", "h"}:
                notice = _run_action(screen, context, status, key, prefill)
            status = status_for(context)
        except (PluginError, OSError, subprocess.SubprocessError) as exc:
            notice = f"ERROR: {str(exc)[:500]}"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Tether's Herdr plugin")
    sub = parser.add_subparsers(dest="command", required=True)
    opening = sub.add_parser("open")
    opening.add_argument("--prefill-thread", action="store_true")
    opening.add_argument("--mode", choices=("status", "rebind"), default="status")
    sub.add_parser("cockpit")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "open":
        mode = "attach" if args.prefill_thread else args.mode
        return open_cockpit(mode)
    token = os.environ.get("TETHER_INVOCATION_ID", "")
    if token:
        mode, context = load_invocation(token)
    else:
        mode, context = "status", _context_from_environment()
    if not sys.stdin.isatty() or not sys.stdout.isatty():
        print(json.dumps(status_for(context), ensure_ascii=False, sort_keys=True))
        return 0
    curses.wrapper(cockpit, mode, context)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except PluginError as error:
        print(f"Tether plugin: {error}", file=sys.stderr)
        raise SystemExit(1) from error
