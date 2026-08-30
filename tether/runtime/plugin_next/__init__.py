"""Tether gateway plugin — public Hermes seams only, shadow mode.

register(ctx) wires exactly four things, every one a documented public API:

- ``pre_gateway_dispatch`` observer: evaluates Tether's strict admission for
  each inbound event, journals the decision durably, and in this release
  ALWAYS returns None (shadow). It never raises into the gateway and never
  blocks traffic Tether does not own.
- ``hermes tether`` CLI subcommand: shadow journal status as JSON.
- ``ctx.on_unload`` (when the host offers it): closes the journal.
- Nothing else. No private attribute of any Hermes object is touched; all
  optional context surfaces are feature-detected with hasattr so the plugin
  loads unchanged on Hermes 0.19.0 (v2026.7.20) through current main.

The security domain (workspace, authorized owners, persona, policy
generation) comes from Tether's own config file — one authority shared with
the broker and CLI — never duplicated into Hermes config.
"""

from __future__ import annotations

import contextlib
import json
import logging
import os
import sqlite3
import time
import tomllib
from pathlib import Path
from typing import Any

from . import admission
from .journal import DurableJournal, JournalError

logger = logging.getLogger("tether.plugin")

_BINDING_CACHE_TTL_SECONDS = 5.0


def _hermes_home() -> Path:
    return Path(os.environ.get("HERMES_HOME", str(Path.home() / ".hermes"))).expanduser()


def _config_path() -> Path:
    config_home = Path(
        os.environ.get("XDG_CONFIG_HOME", str(Path.home() / ".config"))
    ).expanduser()
    return config_home / "tether" / "config.toml"


def load_settings(path: Path | None = None) -> admission.AdmissionSettings:
    """Read the security domain from Tether's config; fail closed to empty."""
    target = _config_path() if path is None else path
    try:
        raw = tomllib.loads(target.read_text(encoding="utf-8"))
    except (OSError, ValueError, tomllib.TOMLDecodeError):
        raw = {}
    allowed = raw.get("allowed_users") or []
    if not isinstance(allowed, list):
        allowed = []
    return admission.AdmissionSettings(
        workspace_id=str(raw.get("team_id") or ""),
        allowed_users=frozenset(
            str(user) for user in allowed if isinstance(user, str) and user
        ),
        persona_id=str(raw.get("persona_id") or ""),
        policy_generation=int(raw.get("policy_generation") or 0),
    )


class BindingIndex:
    """Read-only, throttled snapshot of Tether's bound Slack threads."""

    def __init__(self, database: Path, ttl_seconds: float = _BINDING_CACHE_TTL_SECONDS):
        self.database = Path(database)
        self.ttl_seconds = ttl_seconds
        self._cached: frozenset[tuple[str, str]] = frozenset()
        self._loaded_at = 0.0

    def bound_threads(self) -> frozenset[tuple[str, str]]:
        now = time.monotonic()
        if now - self._loaded_at < self.ttl_seconds:
            return self._cached
        threads: set[tuple[str, str]] = set()
        try:
            uri = f"{self.database.resolve().as_uri()}?mode=ro"
            connection = sqlite3.connect(uri, uri=True, timeout=2)
            try:
                schema_version = int(
                    connection.execute("PRAGMA user_version").fetchone()[0]
                )
                if schema_version >= 18:
                    rows = connection.execute(
                        # Only 'active' is live. 'rebind_required' means the
                        # endpoint's incarnation moved and the domain will
                        # refuse the turn; treating it as bound would admit
                        # traffic against a dead binding once active mode
                        # ships. 'pending_root' has no thread yet.
                        "SELECT channel_id,thread_ts FROM thread_bindings "
                        "WHERE thread_ts IS NOT NULL AND state='active'"
                    )
                else:
                    rows = connection.execute(
                        "SELECT channel_id,thread_ts FROM bridges "
                        "WHERE thread_ts IS NOT NULL AND thread_ts!=''"
                    )
                threads = {
                    (str(row[0]), str(row[1]))
                    for row in rows
                }
            finally:
                connection.close()
        except (sqlite3.Error, OSError, ValueError):
            # Fail closed to "nothing bound": Tether then claims nothing.
            threads = set()
        self._cached = frozenset(threads)
        self._loaded_at = now
        return self._cached


def _event_fields(event: Any) -> dict[str, Any]:
    source = getattr(event, "source", None)
    platform = getattr(getattr(source, "platform", None), "value", None) or str(
        getattr(source, "platform", "") or ""
    )
    chat_type = str(getattr(source, "chat_type", "") or "")
    chat_id = getattr(source, "chat_id", None)
    thread_id = getattr(source, "thread_id", None)
    parent_chat_id = getattr(source, "parent_chat_id", None)
    if chat_type == "thread" and parent_chat_id:
        channel, thread = parent_chat_id, thread_id or chat_id
    else:
        channel, thread = chat_id, thread_id
    return {
        "platform": str(platform).lower(),
        "workspace": getattr(source, "scope_id", None) or getattr(source, "guild_id", None),
        "channel": str(channel) if channel else None,
        "thread": str(thread) if thread else None,
        "actor": getattr(source, "user_id", None),
        "actor_is_bot": bool(getattr(source, "is_bot", False)),
        "message_id": getattr(event, "message_id", None)
        or getattr(source, "message_id", None),
    }


def register(ctx: Any) -> None:
    home = _hermes_home()
    journal = DurableJournal(home / "plugin-data" / "tether")
    bindings = BindingIndex(home / "bridges.db")
    settings = load_settings()

    if not settings.configured:
        logger.warning(
            "tether: security domain incomplete in %s; shadow journal records "
            "every event as unconfigured and Tether claims nothing",
            _config_path(),
        )

    shadow_requested = True
    if hasattr(ctx, "get_config"):
        with contextlib.suppress(Exception):
            shadow_requested = bool(ctx.get_config("shadow_mode", True))
    if not shadow_requested:
        # Active mode ships with the audited cutover slice; refusing here is
        # the fail-closed choice, and it is loud, not silent.
        logger.warning(
            "tether: shadow_mode=false requested but active mode is not "
            "released; staying in shadow"
        )

    def on_pre_gateway_dispatch(event: Any = None, **_kwargs: Any) -> None:
        try:
            if event is None:
                return None
            fields = _event_fields(event)
            if fields["platform"] != "slack":
                return None
            decision = admission.evaluate(
                platform=fields["platform"],
                workspace=fields["workspace"],
                channel=fields["channel"],
                thread=fields["thread"],
                actor=fields["actor"],
                actor_is_bot=fields["actor_is_bot"],
                message_id=fields["message_id"],
                settings=settings,
                bound_threads=bindings.bound_threads(),
            )
            event_key = (
                f"slack:{fields['workspace'] or '-'}:{fields['channel'] or '-'}:"
                f"{fields['message_id'] or decision['fingerprint']}"
            )
            journal.record(
                event_key,
                decision,
                platform=fields["platform"],
                workspace=fields["workspace"],
                channel=fields["channel"],
                thread=fields["thread"],
                actor=fields["actor"],
            )
        except Exception:  # pragma: no cover - the gateway must never break
            logger.exception("tether: shadow observation failed; event untouched")
        return None

    ctx.register_hook("pre_gateway_dispatch", on_pre_gateway_dispatch)

    def _cli_setup(parser: Any) -> None:
        parser.add_argument(
            "subcommand",
            nargs="?",
            default="status",
            choices=["status"],
        )
        parser.add_argument("--json", action="store_true", default=True)

    def _cli_handler(args: Any) -> int:
        del args
        print(json.dumps(journal.summary(), sort_keys=True))
        return 0

    with contextlib.suppress(Exception):
        ctx.register_cli_command(
            "tether",
            help="Tether shadow status",
            setup_fn=_cli_setup,
            handler_fn=_cli_handler,
            description="Slack-thread continuation (shadow mode)",
        )

    if hasattr(ctx, "on_unload"):
        with contextlib.suppress(Exception):
            ctx.on_unload(journal.close)
