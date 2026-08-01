from __future__ import annotations

import inspect
import os
from pathlib import Path

# Fixed local Git verification only; no shell or untrusted executable lookup.
import subprocess  # nosec B404
from typing import Any


SUPPORTED_HERMES_VERSION = "0.19.0"
TESTED_HERMES_COMMIT = "b9ba7c78e41b5d187e2c8fb446655c4b71c42aa5"
_CRITICAL_HERMES_PATHS = (
    "hermes_cli/__init__.py",
    "hermes_cli/plugins.py",
    "gateway/run.py",
    "gateway/platforms/base.py",
    "plugins/platforms/slack/adapter.py",
)


class HermesCompatibilityError(RuntimeError):
    pass


def installed_hermes_version() -> str:
    try:
        from hermes_cli import __version__
    except (ImportError, AttributeError) as exc:
        raise HermesCompatibilityError(
            "Hermes version could not be determined"
        ) from exc
    return str(__version__)


def _parameters(target: Any) -> set[str]:
    try:
        return set(inspect.signature(target).parameters)
    except (TypeError, ValueError) as exc:
        raise HermesCompatibilityError(
            f"Hermes callable signature is unavailable: {target!r}"
        ) from exc


def _source_file(target: Any, label: str) -> Path:
    try:
        raw = inspect.getsourcefile(target)
    except (TypeError, OSError) as exc:
        raise HermesCompatibilityError(
            f"{label} source location is unavailable"
        ) from exc
    if not raw:
        raise HermesCompatibilityError(f"{label} source location is unavailable")
    try:
        return Path(raw).resolve(strict=True)
    except OSError as exc:
        raise HermesCompatibilityError(
            f"{label} source file is unavailable"
        ) from exc


def _repository_root(source: Path) -> Path:
    for candidate in (source.parent, *source.parents):
        if (candidate / ".git").exists():
            return candidate.resolve()
    raise HermesCompatibilityError(
        "Hermes must run from the exact audited source checkout; "
        "no Git repository contains the loaded Hermes package"
    )


def _git_binary() -> str:
    for candidate in ("/usr/bin/git", "/bin/git"):
        if Path(candidate).is_file():
            return candidate
    raise HermesCompatibilityError(
        "A system Git binary is required to verify Hermes source"
    )


def _git(root: Path, *arguments: str) -> str:
    command = [
        _git_binary(),
        "-c",
        f"safe.directory={root}",
        "-C",
        str(root),
        *arguments,
    ]
    environment = {
        "HOME": os.environ.get("HOME", "/"),
        "PATH": os.defpath,
        "LC_ALL": "C",
        "GIT_CONFIG_GLOBAL": os.devnull,
        "GIT_CONFIG_NOSYSTEM": "1",
    }
    try:
        result = subprocess.run(  # nosec B603
            command,
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
            env=environment,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise HermesCompatibilityError(
            "Hermes source verification could not run Git"
        ) from exc
    if result.returncode != 0:
        detail = (result.stderr or result.stdout).strip().splitlines()
        suffix = f": {detail[0]}" if detail else ""
        raise HermesCompatibilityError(
            f"Hermes source verification failed{suffix}"
        )
    return result.stdout.strip()


def verify_hermes_source(
    adapter_type: type,
    *,
    expected_commit: str = TESTED_HERMES_COMMIT,
) -> str:
    try:
        import hermes_cli
    except ImportError as exc:
        raise HermesCompatibilityError("Hermes package is unavailable") from exc

    package_source = _source_file(hermes_cli, "Hermes package")
    root = _repository_root(package_source)
    expected_package = (root / "hermes_cli" / "__init__.py").resolve()
    expected_adapter = (
        root / "plugins" / "platforms" / "slack" / "adapter.py"
    ).resolve()
    adapter_source = _source_file(adapter_type, "Hermes Slack adapter")
    if package_source != expected_package or adapter_source != expected_adapter:
        raise HermesCompatibilityError(
            "Hermes package or Slack adapter is not loaded from the audited checkout"
        )

    head = _git(root, "rev-parse", "--verify", "HEAD")
    if head != expected_commit:
        raise HermesCompatibilityError(
            f"unsupported Hermes source commit {head!r}; "
            f"Tether requires {expected_commit}"
        )

    missing = [
        relative
        for relative in _CRITICAL_HERMES_PATHS
        if not (root / relative).is_file()
    ]
    if missing:
        raise HermesCompatibilityError(
            "Hermes audited source files are missing: " + ", ".join(missing)
        )
    dirty = _git(root, "status", "--porcelain=v1", "--untracked-files=all")
    if dirty:
        raise HermesCompatibilityError(
            "Hermes audited checkout has tracked or untracked local changes"
        )
    ignored_source = _git(
        root,
        "ls-files",
        "--others",
        "--ignored",
        "--exclude-standard",
        "--",
        "hermes_cli",
        "gateway",
        "plugins",
        "tools",
    )
    if any(
        Path(relative).suffix in {".py", ".pyi"}
        for relative in ignored_source.splitlines()
    ):
        raise HermesCompatibilityError(
            "Hermes audited source trees contain ignored Python overlays"
        )
    return head


def validate_adapter(adapter_type: type, *, version: str | None = None) -> str:
    observed = version if version is not None else installed_hermes_version()
    if observed != SUPPORTED_HERMES_VERSION:
        raise HermesCompatibilityError(
            f"unsupported Hermes version {observed!r}; "
            f"Tether supports {SUPPORTED_HERMES_VERSION}"
        )
    verify_hermes_source(adapter_type)
    required = {
        "_handle_slack_message": {"self", "event", "payload"},
        "_get_client": {"self", "chat_id", "team_id"},
        "_ensure_dm_conversation": {"self", "chat_id", "team_id"},
        "_is_ignored_channel": {"self", "channel_id"},
        "_remove_reaction": {"self", "channel", "timestamp", "emoji", "team_id"},
        "_pop_slash_context": {"self", "chat_id", "team_id"},
        "_send_slash_ephemeral": {"self", "ctx", "content"},
        "_resolve_thread_ts": {"self", "reply_to", "metadata"},
        "_maybe_blocks": {"self", "content"},
        "_upload_file": {
            "self",
            "chat_id",
            "file_path",
            "caption",
            "reply_to",
            "metadata",
        },
        "edit_message": {
            "self",
            "chat_id",
            "message_id",
            "content",
            "finalize",
            "metadata",
        },
        "format_message": {"self", "content"},
        "send_clarify": {
            "self",
            "chat_id",
            "question",
            "choices",
            "clarify_id",
            "session_key",
            "metadata",
        },
        "send_exec_approval": {
            "self",
            "chat_id",
            "command",
            "session_key",
            "description",
            "metadata",
        },
        "send_private_notice": {
            "self",
            "chat_id",
            "user_id",
            "content",
            "reply_to",
            "metadata",
        },
        "send_slash_confirm": {
            "self",
            "chat_id",
            "title",
            "message",
            "session_key",
            "confirm_id",
            "metadata",
        },
        "send_video": {
            "self",
            "chat_id",
            "video_path",
            "caption",
            "reply_to",
            "metadata",
        },
        "send_document": {
            "self",
            "chat_id",
            "file_path",
            "caption",
            "file_name",
            "reply_to",
            "metadata",
        },
        "send_multiple_images": {
            "self",
            "chat_id",
            "images",
            "metadata",
            "human_delay",
        },
        "truncate_message": {"content", "max_length"},
        "stop_typing": {"self", "chat_id", "metadata"},
        "connect": {"self"},
        "send": {"self", "chat_id", "content", "reply_to", "metadata"},
    }
    for name, expected in required.items():
        target = getattr(adapter_type, name, None)
        if target is None:
            raise HermesCompatibilityError(
                f"Hermes Slack adapter is missing {name}"
            )
        missing = expected - _parameters(target)
        if missing:
            raise HermesCompatibilityError(
                f"Hermes Slack adapter {name} is missing parameters: "
                + ", ".join(sorted(missing))
            )
    return observed


def register_authoritative_gateway_hook(ctx: Any, callback: Any) -> None:
    """Install Tether first in Hermes' audited gateway-hook chain.

    Hermes 0.19.0 stops at the first ``allow`` or ``rewrite`` result. Tether
    therefore must run first for Slack to preserve its one-writer decision.
    Non-Slack events still fall through to later hooks.
    """

    ctx.register_hook("pre_gateway_dispatch", callback)
    manager = getattr(ctx, "_manager", None)
    hooks = getattr(manager, "_hooks", None)
    if not isinstance(hooks, dict):
        raise HermesCompatibilityError(
            "Hermes plugin hook registry is incompatible with Tether"
        )
    callbacks = hooks.get("pre_gateway_dispatch")
    if not isinstance(callbacks, list) or callback not in callbacks:
        raise HermesCompatibilityError(
            "Hermes did not register Tether's gateway hook"
        )
    callbacks[:] = [candidate for candidate in callbacks if candidate is not callback]
    callbacks.insert(0, callback)
    if not callbacks or callbacks[0] is not callback:
        raise HermesCompatibilityError(
            "Tether could not become the authoritative Slack gateway hook"
        )


def make_send_result(
    send_callable: Any,
    *,
    success: bool,
    message_id: str | None = None,
    error: str | None = None,
    raw_response: Any = None,
    retryable: bool = False,
) -> Any:
    namespace = getattr(send_callable, "__globals__", {})
    result_type = namespace.get("SendResult") if isinstance(namespace, dict) else None
    values = {
        "success": success,
        "message_id": message_id,
        "error": error,
        "raw_response": raw_response,
        "retryable": retryable,
    }
    if callable(result_type):
        try:
            return result_type(**values)
        except TypeError as exc:
            raise HermesCompatibilityError(
                "Hermes SendResult constructor is incompatible with Tether"
            ) from exc
    return {"ok": success, **values}


def workspace_client(adapter: Any, channel_id: str, team_id: str) -> Any:
    if not channel_id or not team_id:
        raise HermesCompatibilityError(
            "Slack workspace and channel identities are required"
        )
    try:
        return adapter._get_client(channel_id, team_id=team_id)
    except TypeError as exc:
        raise HermesCompatibilityError(
            "Hermes Slack client selection is not workspace-aware"
        ) from exc


def _collect_block_mentions(blocks: Any) -> list[str]:
    mentions: list[str] = []

    def walk(node: Any, in_quote: bool) -> None:
        if isinstance(node, list):
            for item in node:
                walk(item, in_quote)
            return
        if not isinstance(node, dict):
            return
        node_type = node.get("type")
        quoted = in_quote or node_type == "rich_text_quote"
        if node_type == "user" and not quoted:
            user_id = str(node.get("user_id") or "")
            if user_id:
                mentions.append(f"<@{user_id}>")
        for key in ("elements", "element"):
            child = node.get(key)
            if child is not None:
                walk(child, quoted)

    walk(blocks, False)
    return mentions


def mention_detection_text(event: dict[str, Any]) -> str:
    flat = str(event.get("text") or "")
    mentions = _collect_block_mentions(event.get("blocks") or [])
    extra = [mention for mention in mentions if mention not in flat]
    if not extra:
        return flat
    return (flat.strip() + "\n" + " ".join(extra)).strip()


def event_declares_bot_sender(event: dict[str, Any]) -> bool:
    if event.get("bot_id") or event.get("bot_profile"):
        return True
    if event.get("subtype") == "bot_message":
        return True
    profile = event.get("user_profile")
    if isinstance(profile, dict) and bool(profile.get("is_bot")):
        return True
    return bool(event.get("app_id") and not event.get("client_msg_id"))


def reaction_marker(team_id: str, message_ts: str) -> tuple[str, str] | str:
    return (str(team_id), str(message_ts)) if team_id else str(message_ts)


async def remove_reaction(
    adapter: Any,
    *,
    channel_id: str,
    message_ts: str,
    emoji: str,
    team_id: str,
) -> Any:
    try:
        return await adapter._remove_reaction(
            channel_id,
            message_ts,
            emoji,
            team_id,
        )
    except TypeError as exc:
        raise HermesCompatibilityError(
            "Hermes reaction removal is not workspace-aware"
        ) from exc
