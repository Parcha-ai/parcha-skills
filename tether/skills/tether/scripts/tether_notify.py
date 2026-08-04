#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib.util
import json
import os
import shutil
# Hermes is invoked with a fixed argv list, never a shell.
import subprocess  # nosec B404
import sys
import time
from collections.abc import Sequence
from pathlib import Path
from types import ModuleType


DATA_HOME = Path(os.environ.get("XDG_DATA_HOME", Path.home() / ".local" / "share"))
RUNTIME_PATH = DATA_HOME / "tether" / "bridge_runtime.py"
SETUP_TIMEOUT_SECONDS = 900
SERVICE_TIMEOUT_SECONDS = 60
MAX_MESSAGE_BYTES = 512 * 1024


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
redact_text = getattr(_runtime, "redact_text", None)
zellij_pane_identity = _runtime.zellij_pane_identity
herdr_agent_identity = _runtime.herdr_agent_identity
working_directory_identity = _runtime.working_directory_identity


def _safe_error(error: BaseException) -> str:
    if not callable(redact_text):
        return type(error).__name__
    detail = redact_text(str(error)).strip()
    return detail[:500] or type(error).__name__


def _terminal_source(identity: dict[str, str]) -> dict[str, str]:
    if identity.get("herdr_terminal_id"):
        return {
            key: identity[key]
            for key in (
                "herdr_session",
                "herdr_socket_path",
                "herdr_terminal_id",
                "herdr_pane_id",
                "herdr_agent_name",
                "herdr_agent_session_source",
                "herdr_agent_session_kind",
                "herdr_agent_session_value",
                "herdr_protocol",
                "pane_agent",
                "process_identity",
            )
        }
    return {
        "zellij_session": identity["session_name"],
        "zellij_pane_id": identity["pane_id"],
        "pane_agent": identity["pane_agent"],
        "process_identity": identity["process_identity"],
        "pane_command_hash": identity["pane_command_hash"],
    }


def _select_native_source(
    *,
    claude_session_id: str,
    codex_session_id: str,
    cwd: str,
    terminal_identity: dict[str, str] | None,
) -> tuple[str, dict[str, str]] | None:
    available = {
        "claude": claude_session_id,
        "codex": codex_session_id,
    }
    available = {agent: value for agent, value in available.items() if value}
    if terminal_identity is not None:
        pane_agent = terminal_identity["pane_agent"]
        if pane_agent not in {"claude", "codex"}:
            raise SystemExit(
                "captured terminal does not match a supported native agent"
            )
        native_session_id = terminal_identity.get("native_session_id", "")
        if not available and not native_session_id:
            return None
        if pane_agent not in available:
            if native_session_id and not available:
                available[pane_agent] = native_session_id
            else:
                expected = " or ".join(sorted(available)) or "none"
                raise SystemExit(
                    f"Captured pane runs {pane_agent}, but ambient native session is {expected}; "
                    "rebind from the intended agent"
                )
        selected = pane_agent
        if native_session_id and native_session_id != available[selected]:
            raise SystemExit(
                "Herdr native session identity does not match the active agent environment; "
                "rebind from the intended agent"
            )
        terminal = _terminal_source(terminal_identity)
    else:
        if not available:
            return None
        if len(available) != 1:
            expected = " or ".join(sorted(available))
            raise SystemExit(
                f"Both Claude and Codex session IDs are present ({expected}) without an exact "
                "pane identity; pass an explicit session to attach or clear the inherited variable"
            )
        selected = next(iter(available))
        terminal = {}
    return (
        f"{selected}_session",
        {
            "session_id": available[selected],
            **working_directory_identity(cwd),
            **terminal,
        },
    )


def _ambient_terminal_identity(cwd: str) -> dict[str, str] | None:
    herdr_values = {
        "session": os.getenv("HERDR_SESSION", ""),
        "socket": os.getenv("HERDR_SOCKET_PATH", ""),
        "pane": os.getenv("HERDR_PANE_ID", ""),
    }
    if os.getenv("HERDR_ENV") or any(herdr_values.values()):
        if not all(herdr_values.values()):
            raise SystemExit(
                "Herdr environment is incomplete; reattach the intended Herdr session"
            )
        return herdr_agent_identity(
            herdr_values["socket"],
            herdr_values["pane"],
            herdr_values["session"],
            cwd,
        )
    if os.getenv("ZELLIJ_SESSION_NAME") and os.getenv("ZELLIJ_PANE_ID"):
        return zellij_pane_identity(
            os.environ["ZELLIJ_SESSION_NAME"],
            os.environ["ZELLIJ_PANE_ID"],
            cwd,
        )
    return None


def detected_source(args: argparse.Namespace) -> tuple[str, dict[str, str]]:
    cwd = str(Path.cwd())
    explicit_source = (
        "headless run"
        if getattr(args, "run_id", None)
        else "Hermes session"
        if getattr(args, "hermes_session_id", None)
        else ""
    )
    if explicit_source:
        if any(
            os.getenv(name)
            for name in (
                "CLAUDE_CODE_SESSION_ID",
                "CODEX_THREAD_ID",
                "ZELLIJ_SESSION_NAME",
                "ZELLIJ_PANE_ID",
                "HERDR_ENV",
                "HERDR_SESSION",
                "HERDR_SOCKET_PATH",
                "HERDR_PANE_ID",
            )
        ):
            raise SystemExit(
                f"an explicit {explicit_source} cannot replace an active "
                "Codex, Claude Code, Herdr, or Zellij binding; repair or rebind the "
                "exact native session"
            )
    if getattr(args, "run_id", None):
        return "headless_run", {"run_id": args.run_id, "queue_id": args.run_id, "cwd": cwd}
    if getattr(args, "hermes_session_id", None):
        return "hermes_session", {"session_id": args.hermes_session_id, "cwd": cwd}
    terminal_identity = _ambient_terminal_identity(cwd)
    selected = _select_native_source(
        claude_session_id=os.getenv("CLAUDE_CODE_SESSION_ID", ""),
        codex_session_id=os.getenv("CODEX_THREAD_ID", ""),
        cwd=cwd,
        terminal_identity=terminal_identity,
    )
    if selected is not None:
        return selected
    if terminal_identity:
        return "zellij_pane", terminal_identity
    raise SystemExit("No resumable context found; pass --run-id for a headless run")


def attached_source(args: argparse.Namespace) -> tuple[str, dict[str, str]]:
    cwd = str(Path(args.cwd or Path.cwd()).resolve())
    zellij_session = str(args.zellij_session or "")
    zellij_pane = str(args.zellij_pane_id or "")
    if bool(zellij_session) != bool(zellij_pane):
        raise SystemExit("--zellij-session and --zellij-pane-id must be provided together")
    has_ambient_herdr = bool(
        os.getenv("HERDR_ENV")
        or os.getenv("HERDR_SESSION")
        or os.getenv("HERDR_SOCKET_PATH")
        or os.getenv("HERDR_PANE_ID")
    )
    if has_ambient_herdr and zellij_session:
        raise SystemExit(
            "an explicit Zellij endpoint cannot replace the active Herdr endpoint"
        )
    terminal_identity = None
    if zellij_session:
        terminal_identity = zellij_pane_identity(zellij_session, zellij_pane, cwd)
    elif has_ambient_herdr:
        terminal_identity = _ambient_terminal_identity(cwd)
    if args.claude_session_id and args.codex_session_id:
        raise SystemExit("choose one native session ID")
    selected = _select_native_source(
        claude_session_id=str(args.claude_session_id or ""),
        codex_session_id=str(args.codex_session_id or ""),
        cwd=cwd,
        terminal_identity=terminal_identity,
    )
    if selected is not None:
        return selected
    if terminal_identity:
        return "zellij_pane", terminal_identity
    return detected_source(args)


def _add_message_input(parser: argparse.ArgumentParser) -> None:
    inputs = parser.add_mutually_exclusive_group(required=True)
    inputs.add_argument(
        "--text",
        help="deprecated: message text in argv; use --text-stdin or --text-fd",
    )
    inputs.add_argument(
        "--text-stdin",
        action="store_true",
        help="read UTF-8 message text from stdin",
    )
    inputs.add_argument(
        "--text-fd",
        type=int,
        metavar="FD",
        help="read UTF-8 message text from an inherited descriptor (FD >= 3)",
    )


def _read_message_stream(stream: object, source: str) -> str:
    reader = getattr(stream, "read", None)
    if not callable(reader):
        raise SystemExit(f"message input from {source} is not readable")
    payload = reader(MAX_MESSAGE_BYTES + 1)
    if not isinstance(payload, bytes):
        raise SystemExit(f"message input from {source} did not produce bytes")
    if len(payload) > MAX_MESSAGE_BYTES:
        raise SystemExit(f"message text from {source} exceeds {MAX_MESSAGE_BYTES} bytes")
    try:
        text = payload.decode("utf-8", errors="strict")
    except UnicodeDecodeError as exc:
        raise SystemExit(f"message text from {source} is not valid UTF-8") from exc
    return _validate_message_text(text, source)


def _validate_message_text(text: str, source: str) -> str:
    if not text:
        raise SystemExit(f"message text from {source} is empty")
    if len(text.encode("utf-8")) > MAX_MESSAGE_BYTES:
        raise SystemExit(f"message text from {source} exceeds {MAX_MESSAGE_BYTES} bytes")
    if "\x00" in text:
        raise SystemExit("message text may not contain NUL bytes")
    return text


def message_text(args: argparse.Namespace) -> str:
    if args.text is not None:
        print(
            "DEPRECATED: --text exposes message content in process arguments; "
            "use --text-stdin or --text-fd.",
            file=sys.stderr,
        )
        return _validate_message_text(str(args.text), "argv")
    if args.text_stdin:
        return _read_message_stream(sys.stdin.buffer, "stdin")
    descriptor = args.text_fd
    if descriptor is None or descriptor < 3:
        raise SystemExit("--text-fd must be an inherited file descriptor of 3 or greater")
    try:
        with os.fdopen(os.dup(descriptor), "rb", closefd=True) as stream:
            return _read_message_stream(stream, f"fd {descriptor}")
    except OSError as exc:
        raise SystemExit(
            f"could not read message text from fd {descriptor}: {exc.strerror or exc}"
        ) from exc


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Send or answer a resumable Hermes Slack thread")
    sub = parser.add_subparsers(dest="command", required=True)
    notify = sub.add_parser("notify")
    _add_message_input(notify)
    notify.add_argument("--channel")
    notify.add_argument("--owner")
    notify.add_argument("--team")
    notify.add_argument("--idempotency-key", required=True)
    notify.add_argument("--run-id")
    notify.add_argument("--hermes-session-id")
    notify.add_argument("--file")
    reply = sub.add_parser("reply")
    reply.add_argument("--bridge-id", required=True)
    reply.add_argument("--reply-key", required=True)
    _add_message_input(reply)
    rebind = sub.add_parser("rebind")
    rebind.add_argument("--team")
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
    _add_message_input(post)
    post.add_argument("--idempotency-key", required=True)
    history = sub.add_parser("history")
    history.add_argument("--channel")
    history.add_argument("--limit", type=int, default=15)
    thread = sub.add_parser("thread")
    thread.add_argument("--channel", required=True)
    thread.add_argument("--thread-ts", required=True)
    thread.add_argument("--limit", type=int, default=100)
    unresolved = sub.add_parser("unresolved")
    unresolved.add_argument("--team")
    unresolved.add_argument("--json", action="store_true")
    resolve = sub.add_parser("resolve")
    resolve.add_argument("--kind", choices=("ingress", "attempt"), required=True)
    resolve.add_argument("--id", required=True)
    resolve.add_argument(
        "--action",
        choices=("retry", "complete", "abandon"),
        required=True,
    )
    resolve.add_argument("--team")
    resolve.add_argument("--json", action="store_true")
    sub.add_parser("doctor")
    sub.add_parser("identity")
    sub.add_parser("maintenance")
    setup = sub.add_parser("setup")
    setup.add_argument("--non-interactive", action="store_true")
    setup.add_argument("--no-restart", action="store_true")
    return parser


def _run_hermes(
    command: list[str],
    timeout: int,
    *,
    capture_output: bool = False,
) -> subprocess.CompletedProcess[str]:
    # The Hermes executable is resolved to an absolute path by shutil.which.
    return subprocess.run(  # nosec B603
        command,
        timeout=timeout,
        text=True,
        capture_output=capture_output,
    )


def _find_hermes() -> str | None:
    configured = os.getenv("HERMES_BIN", "").strip()
    candidates = [
        configured,
        shutil.which("hermes") or "",
        str(Path.home() / ".local" / "bin" / "hermes"),
        str(Path(os.getenv("HERMES_HOME", Path.home() / ".hermes")) / "hermes-agent" / "venv" / "bin" / "hermes"),
    ]
    return next((path for path in candidates if path and os.path.isfile(path) and os.access(path, os.X_OK)), None)


def _plugin_states(hermes: str) -> dict[str, str]:
    listed = _run_hermes(
        [hermes, "plugins", "list", "--plain"],
        SERVICE_TIMEOUT_SECONDS,
        capture_output=True,
    )
    if listed.returncode:
        raise RuntimeError("Hermes plugin state could not be read")
    states = {"tether": "absent", "session-bridge": "absent"}
    seen: set[str] = set()
    for raw_line in (listed.stdout or "").splitlines():
        fields = raw_line.split()
        if not fields or fields[-1] not in states:
            continue
        name = fields[-1]
        if name in seen:
            raise RuntimeError(f"Hermes reported an ambiguous {name} plugin state")
        seen.add(name)
        if fields[0] == "enabled":
            states[name] = "enabled"
        elif fields[0] == "disabled" or fields[:2] == ["not", "enabled"]:
            states[name] = "disabled"
        else:
            raise RuntimeError(f"Hermes reported an unknown {name} plugin state")
    return states


def _config_value(hermes: str, key: str) -> tuple[bool, str]:
    result = _run_hermes(
        [hermes, "config", "get", key],
        SERVICE_TIMEOUT_SECONDS,
        capture_output=True,
    )
    if result.returncode == 0:
        return True, (result.stdout or "").rstrip("\n")
    if result.returncode == 1 and "not set" in (result.stderr or "").lower():
        return False, ""
    raise RuntimeError(f"Hermes config value could not be read: {key}")


def _snapshot_setup(hermes: str) -> dict[str, object]:
    return {
        "plugins": _plugin_states(hermes),
        "config": {
            key: _config_value(hermes, key)
            for key in ("slack.allow_bots", "display.busy_ack_enabled")
        },
        "plugin_mutations": [],
        "config_mutations": [],
    }


def _snapshot_dict(snapshot: dict[str, object], key: str) -> dict[str, object]:
    value = snapshot.get(key)
    if not isinstance(value, dict):
        raise RuntimeError(f"invalid setup snapshot section: {key}")
    return value


def _snapshot_list(snapshot: dict[str, object], key: str) -> list[str]:
    value = snapshot.get(key)
    if not isinstance(value, list) or not all(
        isinstance(item, str) for item in value
    ):
        raise RuntimeError(f"invalid setup snapshot section: {key}")
    return value


def _set_plugin(hermes: str, name: str, enabled: bool) -> int:
    action = "enable" if enabled else "disable"
    result = _run_hermes(
        [hermes, "plugins", action, name],
        SERVICE_TIMEOUT_SECONDS,
    )
    return result.returncode


def _apply_plugin_transition(hermes: str, snapshot: dict[str, object]) -> int:
    plugins = _snapshot_dict(snapshot, "plugins")
    if plugins.get("session-bridge") == "enabled":
        result = _set_plugin(hermes, "session-bridge", False)
        if result:
            print(
                "Refusing to enable Tether while the legacy session-bridge is active.",
                file=sys.stderr,
            )
            return result
        plugin_mutations = _snapshot_list(snapshot, "plugin_mutations")
        plugin_mutations.append("session-bridge")
    if plugins.get("tether") != "enabled":
        result = _set_plugin(hermes, "tether", True)
        if result:
            print("Hermes could not enable the Tether plugin.", file=sys.stderr)
            return result
        plugin_mutations = _snapshot_list(snapshot, "plugin_mutations")
        plugin_mutations.append("tether")
    return 0


def _configure_peer_agents(hermes: str, snapshot: dict[str, object]) -> int:
    settings = (
        ("slack.allow_bots", "mentions"),
        ("display.busy_ack_enabled", "false"),
    )
    config = _snapshot_dict(snapshot, "config")
    config_mutations = _snapshot_list(snapshot, "config_mutations")
    for key, value in settings:
        present, previous = config[key]
        if present and previous == value:
            continue
        configured = _run_hermes(
            [hermes, "config", "set", key, value],
            SERVICE_TIMEOUT_SECONDS,
        )
        if configured.returncode:
            print(f"Tether could not configure {key} for peer-agent threads.", file=sys.stderr)
            return configured.returncode
        config_mutations.append(key)
    return 0


def _restore_setup(hermes: str, snapshot: dict[str, object]) -> bool:
    ok = True
    config = _snapshot_dict(snapshot, "config")
    plugins = _snapshot_dict(snapshot, "plugins")
    config_mutations = _snapshot_list(snapshot, "config_mutations")
    plugin_mutations = _snapshot_list(snapshot, "plugin_mutations")

    for key in reversed(config_mutations):
        present, value = config[key]
        command = [hermes, "config", "set", key, value] if present else [
            hermes,
            "config",
            "unset",
            key,
        ]
        try:
            result = _run_hermes(command, SERVICE_TIMEOUT_SECONDS)
        except (OSError, subprocess.TimeoutExpired):
            result = None
        if result is None or result.returncode:
            print(f"Setup rollback could not restore {key}.", file=sys.stderr)
            ok = False

    tether_was_enabled = plugins.get("tether") == "enabled"
    for name in reversed(plugin_mutations):
        enabled = plugins.get(name) == "enabled"
        if name == "session-bridge" and tether_was_enabled:
            # Never restore the unsafe state where both bridges are enabled.
            continue
        try:
            result = _set_plugin(hermes, name, enabled)
        except (OSError, subprocess.TimeoutExpired):
            result = 1
        if result:
            print(f"Setup rollback could not restore the {name} plugin.", file=sys.stderr)
            ok = False
    return ok


class _SetupFailure(Exception):
    def __init__(self, returncode: int, message: str = "") -> None:
        super().__init__(message)
        self.returncode = returncode or 1


def run_setup(args: argparse.Namespace) -> int:
    hermes = _find_hermes()
    if not hermes:
        print(
            "Hermes Agent is required. Install it from "
            "https://github.com/NousResearch/hermes-agent, then run `tether setup` again.",
            file=sys.stderr,
        )
        return 2
    try:
        snapshot = _snapshot_setup(hermes)
    except KeyboardInterrupt:
        print("Tether setup was interrupted before making changes.", file=sys.stderr)
        return 130
    except Exception as error:
        print(
            f"Tether setup stopped before making changes: {_safe_error(error)}",
            file=sys.stderr,
        )
        return 2

    try:
        plugin_result = _apply_plugin_transition(hermes, snapshot)
        if plugin_result:
            raise _SetupFailure(plugin_result)
        peer_result = _configure_peer_agents(hermes, snapshot)
        if peer_result:
            raise _SetupFailure(peer_result)
        if args.non_interactive:
            result = _run_hermes(
                [hermes, "slack", "manifest", "--write"],
                SERVICE_TIMEOUT_SECONDS,
            )
            if result.returncode:
                raise _SetupFailure(result.returncode)
            print("Slack manifest generated. Run `hermes gateway setup`, then `tether doctor`.")
            return 0

        print("Tether will now open Hermes's Slack setup. It generates the current app manifest,")
        print("finishes the private Socket Mode configuration, and sets your operator allowlist.")
        result = _run_hermes([hermes, "gateway", "setup"], SETUP_TIMEOUT_SECONDS)
        if result.returncode:
            raise _SetupFailure(result.returncode)

        if not args.no_restart:
            restarted = _run_hermes([hermes, "gateway", "restart"], SERVICE_TIMEOUT_SECONDS)
            if restarted.returncode:
                print("No running gateway service found; installing it now.")
                installed = _run_hermes(
                    [hermes, "gateway", "install"],
                    SERVICE_TIMEOUT_SECONDS,
                )
                if installed.returncode:
                    raise _SetupFailure(installed.returncode)
                started = _run_hermes(
                    [hermes, "gateway", "start"],
                    SERVICE_TIMEOUT_SECONDS,
                )
                if started.returncode:
                    raise _SetupFailure(started.returncode)

        deadline = time.monotonic() + 15
        while True:
            ok, checks = doctor()
            if ok or time.monotonic() >= deadline:
                break
            time.sleep(0.5)
        print("\n".join(checks))
        if not ok:
            raise _SetupFailure(
                1,
                "Tether is installed, but the gateway is not ready.",
            )
        print("Tether is ready. Ask your agent: ‘Let me know in Slack when this is done.’")
        return 0
    except (Exception, KeyboardInterrupt) as error:
        if isinstance(error, _SetupFailure):
            returncode = error.returncode
        elif isinstance(error, KeyboardInterrupt):
            returncode = 130
        else:
            returncode = 1
        if str(error):
            print(_safe_error(error), file=sys.stderr)
        try:
            restored = _restore_setup(hermes, snapshot)
        except (Exception, KeyboardInterrupt) as rollback_error:
            print(
                f"Tether setup rollback failed: {_safe_error(rollback_error)}",
                file=sys.stderr,
            )
            restored = False
        if not restored:
            print(
                "Tether setup failed and rollback was incomplete; keep only one bridge enabled.",
                file=sys.stderr,
            )
        return returncode


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "doctor":
        ok, checks = doctor()
        print("\n".join(checks))
        return 0 if ok else 1
    if args.command == "identity":
        print(json.dumps(broker_call({"op": "identity"}), ensure_ascii=False))
        return 0
    if args.command == "maintenance":
        print(
            json.dumps(
                broker_call({"op": "maintenance"}),
                ensure_ascii=False,
                sort_keys=True,
            )
        )
        return 0
    if args.command == "unresolved":
        result = broker_call({"op": "unresolved", "team_id": args.team or ""})
        print(json.dumps(result, ensure_ascii=False, sort_keys=True))
        return 0
    if args.command == "resolve":
        result = broker_call({
            "op": "resolve",
            "kind": args.kind,
            "id": args.id,
            "action": args.action,
            "team_id": args.team or "",
        })
        if args.json:
            print(json.dumps({
                **result,
                "resolution": {
                    "kind": args.kind,
                    "id": args.id,
                    "action": args.action,
                },
            }, ensure_ascii=False, sort_keys=True))
        else:
            quoted_id = json.dumps(args.id, ensure_ascii=True)
            print(f"resolved {args.kind} {quoted_id} with action={args.action}")
        return 0
    if args.command == "setup":
        return run_setup(args)
    if args.command == "reply":
        result = broker_call({
            "op": "reply", "bridge_id": args.bridge_id,
            "reply_key": args.reply_key, "text": message_text(args),
        })
    elif args.command == "rebind":
        kind, source = detected_source(args)
        result = broker_call({
            "op": "rebind", "team_id": args.team or "",
            "channel_id": args.channel,
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
            "thread_ts": args.thread_ts, "text": message_text(args),
            "idempotency_key": args.idempotency_key,
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
    else:
        kind, source = detected_source(args)
        result = broker_call({
            "op": "notify", "text": message_text(args), "source_kind": kind, "source": source,
            "owner_user_id": args.owner or "", "channel_id": args.channel or "", "team_id": args.team or "",
            "idempotency_key": args.idempotency_key, "file_path": args.file,
        })
    print(result["thread_ts"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
