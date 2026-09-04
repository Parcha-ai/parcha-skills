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

Admission needs exactly two things: the workspace and the authorized owner
set. Both resolve the way the deployed broker resolves them — Tether's own
config merged with Hermes's explicit SLACK_ALLOWED_USERS /
GATEWAY_ALLOWED_USERS allowlists — so observing the shadow never requires
changing config the running broker would reject.
"""

from __future__ import annotations

import contextlib
import json
import logging
import os
import re
import sqlite3
import sys
import time
import tomllib
from pathlib import Path
from typing import Any

from . import admission
from . import active as active_module
from . import broker as broker_module
from .slack_egress import SlackEgress
from .journal import DurableJournal

logger = logging.getLogger("hermes_plugins.tether_next")

_BINDING_CACHE_TTL_SECONDS = 5.0
_USER_ID = re.compile(r"[A-Z0-9]{2,32}")


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
    candidates = [str(user) for user in allowed if isinstance(user, str) and user]
    # The deployed broker merges config overrides with Hermes's own explicit
    # allowlists (effective_allowed_users in bridge_runtime); an operator who
    # authorized someone through Hermes has authorized them for Tether too.
    # Resolving only the config file here would make the shadow under-claim
    # exactly the traffic it exists to compare against.
    for name in ("SLACK_ALLOWED_USERS", "GATEWAY_ALLOWED_USERS"):
        candidates.extend(
            value.strip() for value in os.environ.get(name, "").split(",")
        )
    peers = raw.get("trusted_bot_users") or []
    peer_candidates = [str(user) for user in peers if isinstance(user, str) and user] if isinstance(peers, list) else []
    for name in ("TETHER_ALLOWED_BOT_USERS", "HERMES_TRUSTED_BOT_USERS"):
        peer_candidates.extend(value.strip() for value in os.environ.get(name, "").split(","))
    return admission.AdmissionSettings(
        workspace_id=str(raw.get("team_id") or ""),
        allowed_users=frozenset(
            user for user in candidates
            if user and user != "*" and _USER_ID.fullmatch(user)
        ),
        trusted_bot_users=frozenset(
            user for user in peer_candidates if user and _USER_ID.fullmatch(user)
        ),
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
    logger.info("tether: register() entered")
    journal = DurableJournal(home / "plugin-data" / "tether")
    bindings = BindingIndex(home / "bridges.db")
    # Threads bound in the schema-18 domain (active mode) count as bound too:
    # admission stays the single gate, it just reads both stores.
    domain_bindings = BindingIndex(home / "plugin-data" / "tether" / "domain.db", ttl_seconds=0.0)
    settings = load_settings()

    active_settings = active_module.load_active_settings(_config_path())
    slice_: active_module.ActiveSlice | None = None
    if active_settings.enabled and settings.configured:
        slice_ = _build_active_slice(ctx, home, settings, active_settings)
        if slice_ is not None and getattr(slice_, "slack", None) is not None:
            try:
                settings = admission.AdmissionSettings(
                    workspace_id=settings.workspace_id,
                    allowed_users=settings.allowed_users,
                    trusted_bot_users=settings.trusted_bot_users,
                    self_user_id=str(slice_.slack.identity().get("user_id") or ""),
                )
            except Exception:
                logger.warning("tether: could not resolve own Slack identity; self-messages may be claimed")
        logger.warning(
            "tether: active slice %s (workspace=%s owners=%d peers=%d)",
            "started" if slice_ else "NOT built",
            settings.workspace_id, len(settings.allowed_users), len(settings.trusted_bot_users),
        )
    elif active_settings.enabled:
        logger.warning("tether: active=true but security domain incomplete; staying shadow")

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
            logger.debug("tether: hook event %s", fields)
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
                text=str(getattr(event, "text", "") or ""),
                settings=settings,
                bound_threads=bindings.bound_threads() | domain_bindings.bound_threads(),
            )
            event_key = (
                f"slack:{fields['workspace'] or '-'}:{fields['channel'] or '-'}:"
                f"{fields['message_id'] or decision['fingerprint']}"
            )
            claimed = None
            if slice_ is not None and decision.get("verdict") == "admit":
                try:
                    claimed = slice_.claim(fields, str(getattr(event, "text", "") or ""))
                except Exception:
                    logger.exception("tether: claim failed for %s; event falls through", event_key)
                decision = dict(decision, claimed=claimed is not None)
            journal.record(
                event_key,
                decision,
                platform=fields["platform"],
                workspace=fields["workspace"],
                channel=fields["channel"],
                thread=fields["thread"],
                actor=fields["actor"],
            )
            logger.info(
                "tether: decision %s/%s claimed=%s",
                decision.get("verdict"), decision.get("reason"), claimed is not None,
            )
            if claimed is not None:
                # Tether owns this turn; Hermes' own agent must not also answer.
                return {"action": "skip", "reason": "tether-claimed"}
        except Exception:  # pragma: no cover - the gateway must never break
            logger.exception("tether: observation failed; event untouched")
        return None

    ctx.register_hook("pre_gateway_dispatch", on_pre_gateway_dispatch)
    if slice_ is not None:
        slice_.start()
        broker = getattr(slice_, "broker", None)
        if broker is not None:
            broker.start()
            logger.warning("tether: broker listening on %s", broker.socket_path)

    def _cli_setup(parser: Any) -> None:
        parser.add_argument(
            "subcommand",
            nargs="?",
            default="status",
            choices=["status", "bind"],
        )
        parser.add_argument("--json", action="store_true", default=True)
        parser.add_argument("--channel")
        parser.add_argument("--thread-ts")
        parser.add_argument("--owner")
        parser.add_argument("--claude-session-id")
        parser.add_argument("--codex-session-id")
        parser.add_argument("--cwd", default=os.getcwd())

    def _cli_handler(args: Any) -> int:
        if getattr(args, "subcommand", "status") == "bind":
            if slice_ is None:
                print(json.dumps({"ok": False, "error": "active_mode_disabled"}))
                return 2
            kind, session = ("codex_session", args.codex_session_id) if args.codex_session_id else ("claude_session", args.claude_session_id)
            if not (session and args.channel and args.thread_ts):
                print(json.dumps({"ok": False, "error": "usage: bind --channel C --thread-ts TS (--claude-session-id|--codex-session-id) ID"}))
                return 2
            owner = args.owner or next(iter(sorted(settings.allowed_users)), "")
            binding = slice_.bind(
                source_kind=kind, session_id=session, cwd=args.cwd,
                team_id=settings.workspace_id, channel_id=args.channel,
                thread_ts=args.thread_ts, owner_user_id=owner,
            )
            print(json.dumps({"ok": True, "binding_id": binding["binding_id"]}))
            return 0
        summary = journal.summary()
        summary["active"] = slice_ is not None
        print(json.dumps(summary, sort_keys=True))
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
        if slice_ is not None:
            with contextlib.suppress(Exception):
                ctx.on_unload(slice_.stop)
            broker = getattr(slice_, "broker", None)
            if broker is not None:
                with contextlib.suppress(Exception):
                    ctx.on_unload(broker.stop)


def _build_active_slice(
    ctx: Any,
    home: Path,
    settings: admission.AdmissionSettings,
    active_settings: active_module.ActiveSettings,
) -> active_module.ActiveSlice | None:
    """Wire the schema-18 domain, the exact-turn driver, and Hermes egress."""
    try:
        import domain_runtime
        import domain_schema
        import native_driver
    except ImportError:
        # Installed layout: runtime modules live in $XDG_DATA_HOME/tether; the
        # source layout keeps them one directory above this package.
        data_home = Path(
            os.environ.get("XDG_DATA_HOME", str(Path.home() / ".local" / "share"))
        ).expanduser()
        for candidate in (data_home / "tether", Path(__file__).resolve().parents[1]):
            if str(candidate) not in sys.path and candidate.is_dir():
                sys.path.insert(0, str(candidate))
        try:
            import domain_runtime
            import domain_schema
            import native_driver
        except ImportError:
            logger.error("tether: active mode requested but domain runtime is not installed")
            return None
    root = home / "plugin-data" / "tether"
    root.mkdir(parents=True, exist_ok=True)
    database = root / "domain.db"
    if not database.exists():
        connection = sqlite3.connect(database)
        try:
            domain_schema.install_schema(connection)
            connection.execute(f"PRAGMA user_version={domain_schema.SCHEMA_VERSION}")
            connection.commit()
        finally:
            connection.close()
        os.chmod(database, 0o600)
    runtime = domain_runtime.DomainRuntime(database)
    driver = native_driver.NativeDriver(runtime, work_root=root / "driver")
    descriptor = domain_schema.SecurityDomainDescriptor(
        instance_uid=os.geteuid(),
        workspace_id=settings.workspace_id,
        persona_id=active_settings.persona_id,
        authorized_owner_ids=tuple(sorted(settings.allowed_users)),
        policy_generation=active_settings.policy_generation,
    )

    slack = SlackEgress()

    def egress(channel_id: str, thread_ts: str, text: str) -> Any:
        return slack.post(channel_id, text, thread_ts=thread_ts)

    slice_ = active_module.ActiveSlice(
        runtime=runtime, driver=driver, settings=active_settings,
        egress=egress, descriptor=descriptor, slack=slack,
    )
    server = broker_module.BrokerServer(home / "bridge.sock", slice_.handle)
    slice_.broker = server
    return slice_
