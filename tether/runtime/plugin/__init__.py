from __future__ import annotations

import asyncio
import contextlib
import contextvars
import datetime
import functools
import hashlib
import importlib
import importlib.util
import inspect
import json
import logging
import os
import re
import sys
import threading
import time
import uuid
import urllib.parse
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from types import ModuleType
from typing import Any


def _load_runtime() -> ModuleType:
    injected = sys.modules.get("bridge_runtime")
    if injected is not None:
        return injected
    data_home = Path(os.environ.get("XDG_DATA_HOME", Path.home() / ".local" / "share"))
    runtime_path = data_home / "tether" / "bridge_runtime.py"
    spec = importlib.util.spec_from_file_location("tether_bridge_runtime", runtime_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Tether runtime is unavailable at {runtime_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


runtime = _load_runtime()
Store = runtime.Store
broker_call = runtime.broker_call
continue_native = runtime.continue_native
deliver_zellij = runtime.deliver_zellij
interrupt_zellij = runtime.interrupt_zellij
effective_allowed_users = runtime.effective_allowed_users
load_config = runtime.load_config
redact_text = runtime.redact_text
start_broker = runtime.start_broker
stage_reply_payload = runtime.stage_reply_payload
validate_reply_text = runtime.validate_reply_text


def _load_routing() -> ModuleType:
    injected = sys.modules.get("tether_routing")
    if injected is not None:
        return injected
    runtime_path = Path(runtime.__file__).resolve()
    routing_path = runtime_path.with_name("routing.py")
    spec = importlib.util.spec_from_file_location("tether_routing", routing_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Tether routing core is unavailable at {routing_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


routing = _load_routing()


def _load_hermes_compat() -> ModuleType:
    injected = sys.modules.get("tether_hermes_compat")
    if injected is not None:
        return injected
    runtime_path = Path(runtime.__file__).resolve()
    compat_path = runtime_path.with_name("hermes_compat.py")
    spec = importlib.util.spec_from_file_location(
        "tether_hermes_compat", compat_path
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(
            f"Tether Hermes compatibility module is unavailable at {compat_path}"
        )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


hermes_compat = _load_hermes_compat()


def _load_slack_protocol() -> ModuleType:
    injected = sys.modules.get("tether_slack_protocol")
    if injected is not None:
        return injected
    runtime_path = Path(runtime.__file__).resolve()
    protocol_path = runtime_path.with_name("slack_protocol.py")
    spec = importlib.util.spec_from_file_location(
        "tether_slack_protocol", protocol_path
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(
            f"Tether Slack protocol module is unavailable at {protocol_path}"
        )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


slack_protocol = _load_slack_protocol()


log = logging.getLogger(__name__)
SLACK_MENTION_PATTERN = re.compile(r"<@([A-Z0-9]+)(?:\|[^>]+)?>")
ROUTING_DECISION_KEY = "_tether_routing_decision"
ROUTING_ERROR_KEY = "_tether_routing_error"
_HERMES_EGRESS_CONTEXT: contextvars.ContextVar[dict[str, Any] | None] = (
    contextvars.ContextVar("tether_hermes_egress", default=None)
)


@dataclass
class PluginState:
    store: Any = None
    store_lock_fd: int = -1
    broker: Any = None
    ready: bool = False
    bridge_locks: dict[str, asyncio.Lock] = field(default_factory=dict)
    active_cancellations: dict[str, threading.Event] = field(default_factory=dict)
    dispatch_context: dict[str, tuple[Any, Any, Any]] = field(default_factory=dict)
    recovery_worker_started: bool = False
    recovery_wake_counter: int = 0
    recovery_lock: threading.Lock = field(default_factory=threading.Lock)
    reply_poller: asyncio.Task | None = None
    hermes_ingress_finalizers: set[asyncio.Task] = field(default_factory=set)
    poll_cursor: int = 0
    joined_channels: set[tuple[str, str]] = field(default_factory=set)
    slack_transport_connected: bool | None = None
    last_inbound_at: float | None = None
    last_poll_at: float | None = None
    last_poll_error_at: float | None = None
    thread_bot_participants: dict[
        tuple[str, str, str], tuple[float, frozenset[str]]
    ] = field(default_factory=dict)
    thread_root_bridges: dict[
        tuple[str, str, str], tuple[float, str]
    ] = field(default_factory=dict)
    slack_retry_after: Any = field(
        default_factory=slack_protocol.RetryAfterCoordinator
    )


state = PluginState()
store = state.store


def _bounded_env_int(name: str, default: int, minimum: int, maximum: int) -> int:
    try:
        value = int(os.getenv(name, str(default)))
    except ValueError:
        return default
    return max(minimum, min(value, maximum))


def _reply_poll_interval() -> int:
    return _bounded_env_int("TETHER_REPLY_POLL_SECONDS", 60, 60, 3600)


def _import_native_slack_participation(adapter) -> int:
    """Seed restart recovery from Hermes's recent native Slack sessions."""
    sessions_path = runtime.HERMES_HOME / "sessions" / "sessions.json"
    try:
        payload = json.loads(sessions_path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return 0
    sessions = payload.values() if isinstance(payload, dict) else ()
    cutoff = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=7)
    recent_sessions = []
    for session in sessions:
        if not isinstance(session, dict):
            continue
        updated_at = str(session.get("updated_at") or "")
        try:
            updated = datetime.datetime.fromisoformat(updated_at.replace("Z", "+00:00"))
            if updated.tzinfo is None:
                updated = updated.replace(tzinfo=datetime.timezone.utc)
        except ValueError:
            continue
        if updated >= cutoff:
            recent_sessions.append((updated, session))
    recent_sessions.sort(key=lambda item: item[0], reverse=True)
    imported = 0
    for updated, session in recent_sessions[:2000]:
        origin = session.get("origin")
        if not isinstance(origin, dict) or origin.get("platform") != "slack":
            continue
        channel_id = str(origin.get("chat_id") or "")
        thread_ts = str(origin.get("thread_id") or "")
        if not channel_id or not thread_ts:
            continue
        team_id = str(getattr(adapter, "_channel_team", {}).get(channel_id, "") or "")
        store.mark_participation(
            team_id,
            channel_id,
            thread_ts,
            observed_at=updated.astimezone(datetime.timezone.utc).isoformat(),
        )
        imported += 1
    return imported


def _health_status() -> dict[str, Any]:
    now = time.monotonic()
    interval = _reply_poll_interval()
    poll_age = None if state.last_poll_at is None else max(0, int(now - state.last_poll_at))
    poll_healthy = (
        state.reply_poller is not None
        and not state.reply_poller.done()
        and poll_age is not None
        and poll_age <= max(90, interval * 3)
        and (state.last_poll_error_at is None or state.last_poll_error_at < state.last_poll_at)
    )
    return {
        "slack_transport_connected": state.slack_transport_connected,
        "reply_poll_healthy": poll_healthy,
        "reply_poll_age_seconds": poll_age,
        "inbound_observed": state.last_inbound_at is not None,
    }


def _event_channel_id(event: dict[str, Any]) -> str:
    return str(event.get("channel") or event.get("channel_id") or "")


def _payload_team_id(payload: Any) -> str:
    if not isinstance(payload, dict):
        return ""
    direct = str(payload.get("team_id") or "")
    if direct:
        return direct
    team = payload.get("team")
    if isinstance(team, dict) and team.get("id"):
        return str(team["id"])
    authorizations = payload.get("authorizations")
    if isinstance(authorizations, list):
        for authorization in authorizations:
            if isinstance(authorization, dict) and authorization.get("team_id"):
                return str(authorization["team_id"])
    return ""


def _event_team_id(adapter, event: dict[str, Any], payload: Any = None) -> str:
    channel_id = _event_channel_id(event)
    return str(
        event.get("team")
        or event.get("team_id")
        or _payload_team_id(payload)
        or getattr(adapter, "_channel_team", {}).get(channel_id, "")
        or ""
    )


async def _broker_owns_workspace(team_id: str) -> bool:
    broker_server = state.broker
    broker = getattr(broker_server, "broker", None)
    require_workspace = getattr(broker, "require_workspace", None)
    if require_workspace is None:
        return broker_server is None
    try:
        await asyncio.to_thread(require_workspace, team_id)
    except Exception:
        return False
    return True


def _local_bot_user_id(adapter, team_id: str) -> str:
    return str(
        getattr(adapter, "_team_bot_user_ids", {}).get(team_id)
        or getattr(adapter, "_bot_user_id", "")
        or ""
    )


def _local_bot_id(adapter, team_id: str) -> str:
    return str(
        getattr(adapter, "_team_bot_ids", {}).get(team_id)
        or getattr(adapter, "_bot_id", "")
        or ""
    )


def _csv_env(name: str) -> frozenset[str]:
    return frozenset(
        part.strip()
        for part in os.getenv(name, "").split(",")
        if part.strip()
    )


def _allowed_channels(adapter) -> frozenset[str]:
    resolver = getattr(adapter, "_slack_allowed_channels", None)
    if callable(resolver):
        try:
            return frozenset(str(value) for value in resolver() if str(value))
        except Exception:
            return frozenset()
    raw = getattr(getattr(adapter, "config", None), "extra", {}).get(
        "allowed_channels", ""
    )
    if isinstance(raw, list):
        return frozenset(str(value).strip() for value in raw if str(value).strip())
    return frozenset(part.strip() for part in str(raw).split(",") if part.strip())


def _routing_policy(adapter, team_id: str):
    self_user_id = _local_bot_user_id(adapter, team_id)
    if not self_user_id:
        return None
    return routing.RoutingPolicy(
        self_bot_user_id=self_user_id,
        self_bot_id=_local_bot_id(adapter, team_id),
        hermes_writer_id=f"hermes:{self_user_id}",
        allowed_human_user_ids=frozenset(effective_allowed_users()),
        trusted_peer_user_ids=_allowed_peer_bot_users(),
        trusted_peer_bot_ids=_csv_env("TETHER_ALLOWED_BOT_IDS"),
        allowed_channel_ids=_allowed_channels(adapter),
    )


def _slack_error_status_headers(
    exc: BaseException,
) -> tuple[int | None, Mapping[str, Any]]:
    response = getattr(exc, "response", None)
    candidates = (response, exc)
    status: int | None = None
    headers: Mapping[str, Any] = {}
    for candidate in candidates:
        if candidate is None:
            continue
        raw_status = getattr(candidate, "status_code", None)
        if raw_status is None and isinstance(candidate, Mapping):
            raw_status = candidate.get("status_code") or candidate.get("status")
        try:
            if raw_status is not None:
                status = int(raw_status)
        except (TypeError, ValueError):
            pass
        raw_headers = getattr(candidate, "headers", None)
        if raw_headers is None and isinstance(candidate, Mapping):
            raw_headers = candidate.get("headers")
        if isinstance(raw_headers, Mapping):
            headers = raw_headers
        if status is not None and headers:
            break
    return status, headers


async def _slack_api_call(
    team_id: str,
    method: str,
    operation: Callable[[], Awaitable[Any]],
) -> Any:
    key = slack_protocol.SlackMethodKey(team_id, method)
    for attempt in range(2):
        await state.slack_retry_after.wait_async(key)
        try:
            return await operation()
        except Exception as exc:
            status, headers = _slack_error_status_headers(exc)
            if status != 429:
                raise
            state.slack_retry_after.record_429(key, headers)
            if attempt:
                raise
    raise RuntimeError("unreachable Slack retry state")


def _validate_slack_cursor_page(
    response: Any,
    cursor_state: Any,
    *,
    max_pages: int,
) -> Any:
    payload = getattr(response, "data", response)
    if not isinstance(payload, Mapping):
        payload = {}
    else:
        payload = dict(payload)
        # A successful Slack SDK call has already checked `ok`. Lightweight
        # compatible clients may return only the response body fields.
        if "ok" not in payload and "error" not in payload:
            payload["ok"] = True
    return slack_protocol.validate_cursor_page(
        payload,
        cursor_state,
        items_key="messages",
        max_pages=max_pages,
    )


async def _resolve_user_kind(
    adapter,
    *,
    user_id: str,
    channel_id: str,
    team_id: str,
) -> str | None:
    if not user_id:
        return None
    cache = getattr(adapter, "_tether_user_kinds", None)
    if cache is None:
        cache = adapter._tether_user_kinds = {}
    cache_key = (team_id, user_id)
    cached = cache.get(cache_key)
    if cached in {"bot", "human"}:
        return cached
    client = hermes_compat.workspace_client(adapter, channel_id, team_id)
    try:
        result = await _slack_api_call(
            team_id,
            "users.info",
            lambda: client.users_info(user=user_id),
        )
    except Exception:
        return None
    result_payload = getattr(result, "data", result)
    user = (
        result_payload.get("user")
        if isinstance(result_payload, Mapping)
        else None
    )
    if not isinstance(user, Mapping):
        return None
    profile = user.get("profile")
    is_bot = bool(
        user.get("is_bot")
        or user.get("is_workflow_bot")
        or user.get("is_app_user")
        or (isinstance(profile, Mapping) and profile.get("bot_id"))
    )
    kind = "bot" if is_bot else "human"
    cache[cache_key] = kind
    return kind


async def _event_actor_kind(
    adapter,
    event: dict[str, Any],
    *,
    channel_id: str,
    team_id: str,
) -> str | None:
    if hermes_compat.event_declares_bot_sender(event):
        return "bot"
    return await _resolve_user_kind(
        adapter,
        user_id=str(event.get("user") or ""),
        channel_id=channel_id,
        team_id=team_id,
    )


async def _conversation_kind(
    adapter,
    event: dict[str, Any],
    channel_id: str,
    team_id: str,
):
    raw = str(event.get("channel_type") or "").lower()
    if raw == "im":
        return routing.ConversationKind.DM
    if raw == "mpim":
        return routing.ConversationKind.MPIM
    if raw in {"channel", "group"}:
        return routing.ConversationKind.CHANNEL
    if channel_id.startswith("D"):
        return routing.ConversationKind.DM
    if channel_id.startswith("C"):
        return routing.ConversationKind.CHANNEL
    if not channel_id.startswith("G"):
        return None
    try:
        client = hermes_compat.workspace_client(adapter, channel_id, team_id)
        result = await _slack_api_call(
            team_id,
            "conversations.info",
            lambda: client.conversations_info(channel=channel_id),
        )
        result_payload = getattr(result, "data", result)
        conversation = (
            result_payload.get("channel", {})
            if isinstance(result_payload, Mapping)
            else {}
        )
    except Exception:
        return None
    if conversation.get("is_mpim"):
        return routing.ConversationKind.MPIM
    if conversation.get("is_im"):
        return routing.ConversationKind.DM
    if conversation.get("is_channel") or conversation.get("is_group"):
        return routing.ConversationKind.CHANNEL
    return None


async def _classify_mentions(
    adapter,
    event: dict[str, Any],
    team_id: str,
    channel_id: str,
    policy,
) -> tuple[frozenset[str], frozenset[str], frozenset[str], frozenset[str]]:
    mentioned = frozenset(
        SLACK_MENTION_PATTERN.findall(
            hermes_compat.mention_detection_text(event)
        )
    )
    bots: set[str] = set()
    humans: set[str] = set()
    unresolved: set[str] = set()
    known_bots = set(policy.trusted_peer_user_ids) | {policy.self_bot_user_id}
    for user_id in mentioned:
        if user_id in known_bots:
            bots.add(user_id)
            continue
        kind = await _resolve_user_kind(
            adapter,
            user_id=user_id,
            channel_id=channel_id,
            team_id=team_id,
        )
        if kind == "bot":
            bots.add(user_id)
            continue
        if kind == "human":
            humans.add(user_id)
            continue
        unresolved.add(user_id)
    return (
        mentioned,
        frozenset(bots),
        frozenset(humans),
        frozenset(unresolved),
    )


def _participation_timestamp(
    team_id: str,
    channel_id: str,
    thread_ts: str,
    lease_seconds: int,
) -> float | None:
    hours = max(1, min(24 * 90, (lease_seconds + 3599) // 3600 + 1))
    for stored_team, stored_channel, stored_thread, updated_at in (
        store.recent_participating_threads(hours=hours, limit=2_000)
    ):
        if (
            stored_team == team_id
            and stored_channel == channel_id
            and stored_thread == thread_ts
        ):
            return float(updated_at)
    return None


async def _thread_bot_users(
    adapter,
    team_id: str,
    channel_id: str,
    thread_ts: str,
    self_user_id: str,
    *,
    use_cached_snapshot: bool = False,
) -> frozenset[str] | None:
    key = (team_id, channel_id, thread_ts)
    cached = state.thread_bot_participants.get(key)
    now = time.monotonic()
    if use_cached_snapshot:
        if cached is not None and now - cached[0] <= 15:
            return cached[1]
        return None
    try:
        client = hermes_compat.workspace_client(
            adapter, channel_id, team_id
        )
        result = await _slack_api_call(
            team_id,
            "conversations.replies",
            lambda: client.conversations_replies(
                channel=channel_id,
                ts=thread_ts,
                limit=15,
                include_all_metadata=True,
            ),
        )
        page = _validate_slack_cursor_page(
            result,
            slack_protocol.CursorState(),
            max_pages=100,
        )
    except Exception:
        return None
    if not page.page.complete:
        return None
    return await _cache_complete_thread_snapshot(
        adapter,
        team_id,
        channel_id,
        thread_ts,
        page.page.items,
    )


async def _cache_complete_thread_snapshot(
    adapter,
    team_id: str,
    channel_id: str,
    thread_ts: str,
    messages: tuple[Mapping[str, Any], ...],
) -> frozenset[str] | None:
    """Resolve and cache the bot ownership of a complete thread snapshot."""
    fragment = await _thread_snapshot_fragment(
        adapter,
        team_id,
        channel_id,
        thread_ts,
        messages,
    )
    if fragment is None:
        return None
    participants, root_bridge_id = fragment
    return _store_complete_thread_snapshot(
        team_id,
        channel_id,
        thread_ts,
        participants,
        root_bridge_id,
    )


async def _thread_snapshot_fragment(
    adapter,
    team_id: str,
    channel_id: str,
    thread_ts: str,
    messages: tuple[Mapping[str, Any], ...],
) -> tuple[frozenset[str], str] | None:
    """Resolve one cursor page without claiming that the thread is complete."""
    participants: set[str] = set()
    root_bridge_id = ""
    for message in messages:
        user_id = str(message.get("user") or "")
        if str(message.get("ts") or "") == thread_ts:
            metadata = message.get("metadata")
            payload = (
                metadata.get("event_payload")
                if isinstance(metadata, Mapping)
                else None
            )
            if (
                isinstance(metadata, Mapping)
                and metadata.get("event_type") == "tether_root"
                and isinstance(payload, Mapping)
            ):
                root_bridge_id = str(payload.get("bridge_id") or "")
        if hermes_compat.event_declares_bot_sender(message):
            is_bot = True
        elif user_id:
            kind = await _resolve_user_kind(
                adapter,
                user_id=user_id,
                channel_id=channel_id,
                team_id=team_id,
            )
            if kind is None:
                return None
            is_bot = kind == "bot"
        else:
            is_bot = False
        if not is_bot:
            continue
        if user_id:
            participants.add(user_id)
        else:
            bot_id = str(message.get("bot_id") or "")
            participants.add(f"bot:{bot_id}" if bot_id else "bot:unresolved")
    return frozenset(participants), root_bridge_id


def _store_complete_thread_snapshot(
    team_id: str,
    channel_id: str,
    thread_ts: str,
    participants: frozenset[str],
    root_bridge_id: str,
) -> frozenset[str]:
    key = (team_id, channel_id, thread_ts)
    observed_at = time.monotonic()
    state.thread_bot_participants[key] = (
        observed_at,
        frozenset(participants),
    )
    state.thread_root_bridges[key] = (observed_at, root_bridge_id)
    return participants


def _reply_poll_message(
    message: Mapping[str, Any],
    thread_ts: str,
) -> dict[str, str] | None:
    message_ts = str(message.get("ts") or "")
    text = str(message.get("text") or "")
    if not message_ts or message_ts == thread_ts or not text.strip():
        return None
    normalized = {
        key: str(message.get(key) or "")
        for key in (
            "ts",
            "thread_ts",
            "text",
            "user",
            "bot_id",
            "subtype",
            "channel_type",
        )
    }
    normalized["thread_ts"] = thread_ts
    return normalized


def _merge_reply_poll_messages(
    persisted: tuple[dict[str, str], ...],
    current: tuple[Mapping[str, Any], ...],
    thread_ts: str,
) -> tuple[dict[str, str], ...]:
    merged: dict[str, dict[str, str]] = {}
    for message in persisted:
        normalized = _reply_poll_message(message, thread_ts)
        if normalized is not None:
            merged[normalized["ts"]] = normalized
    for message in current:
        normalized = _reply_poll_message(message, thread_ts)
        if normalized is not None:
            merged[normalized["ts"]] = normalized
    return tuple(merged.values())


def _binding_for_bridge(bridge, *, ambient_owned: bool = False):
    if bridge is None:
        return None
    if bridge.source_kind == "headless_run":
        kind = routing.BindingKind.HEADLESS
    elif bridge.source_kind == "hermes_session":
        kind = routing.BindingKind.HERMES
    else:
        kind = routing.BindingKind.NATIVE
    return routing.ActiveBinding(
        kind=kind,
        bridge_id=bridge.bridge_id,
        writer_id=f"bridge:{bridge.bridge_id}",
        owner_user_id=bridge.owner_user_id,
        active=bridge.status == "active",
        binding_generation=bridge.binding_generation,
        ambient_owned=ambient_owned,
    )


async def _routing_thread_state(
    adapter,
    message,
    policy,
    *,
    use_cached_snapshot: bool = False,
    require_participant_history: bool = True,
) -> tuple[Any, str]:
    if not message.thread_ts:
        return None, ""
    identity = routing.ThreadIdentity(
        message.identity.team_id,
        message.identity.channel_id,
        message.thread_ts,
    )
    bridge = store.find(
        message.identity.team_id,
        message.identity.channel_id,
        message.thread_ts,
    )
    if bridge is not None:
        if bridge.team_id != message.identity.team_id:
            return None, "bridge_workspace_unresolved"
        ambient_owned = (
            message.conversation_kind is routing.ConversationKind.DM
            or store.owns_thread_root(
                bridge.bridge_id,
                message.identity.team_id,
                message.identity.channel_id,
                message.thread_ts,
            )
        )
        if not ambient_owned and require_participant_history:
            bot_users = await _thread_bot_users(
                adapter,
                message.identity.team_id,
                message.identity.channel_id,
                message.thread_ts,
                policy.self_bot_user_id,
                use_cached_snapshot=use_cached_snapshot,
            )
            if bot_users is None:
                return routing.ThreadState(
                    identity=identity,
                    binding=_binding_for_bridge(bridge),
                ), ""
            marker = state.thread_root_bridges.get(
                (
                    message.identity.team_id,
                    message.identity.channel_id,
                    message.thread_ts,
                )
            )
            ambient_owned = (
                bot_users == frozenset({policy.self_bot_user_id})
                and marker is not None
                and marker[1] == bridge.bridge_id
            )
        return routing.ThreadState(
            identity=identity,
            binding=_binding_for_bridge(
                bridge,
                ambient_owned=ambient_owned,
            ),
        ), ""

    lease_seconds = _bounded_env_int(
        "TETHER_PARTICIPATION_LEASE_SECONDS",
        7 * 24 * 3600,
        60,
        30 * 24 * 3600,
    )
    participated_at = _participation_timestamp(
        message.identity.team_id,
        message.identity.channel_id,
        message.thread_ts,
        lease_seconds,
    )
    if participated_at is None or participated_at + lease_seconds < message.observed_at:
        return routing.ThreadState(identity=identity), ""
    if not require_participant_history:
        return routing.ThreadState(identity=identity), ""
    bot_users = await _thread_bot_users(
        adapter,
        message.identity.team_id,
        message.identity.channel_id,
        message.thread_ts,
        policy.self_bot_user_id,
        use_cached_snapshot=use_cached_snapshot,
    )
    if bot_users is None:
        return routing.ThreadState(identity=identity), ""
    lease = routing.ParticipationLease(
        owner_bot_user_id=policy.self_bot_user_id,
        writer_id=policy.hermes_writer_id,
        expires_at=participated_at + lease_seconds,
        competing_bot_user_ids=bot_users - {policy.self_bot_user_id},
    )
    return routing.ThreadState(identity=identity, participation=lease), ""


def _composite_event_id(decision) -> str:
    team_id, channel_id, message_ts = decision.dedupe_key
    return f"slack:{team_id}:{channel_id}:{message_ts}"


def _normalize_slack_mutation(
    event: dict[str, Any],
    payload: Any = None,
) -> bool:
    canonical_payload: Mapping[str, Any] = event
    if isinstance(payload, Mapping):
        envelope = dict(payload)
        envelope["event"] = event
        canonical_payload = envelope
    normalized = slack_protocol.canonicalize_message_mutation(
        canonical_payload
    )
    disposition = normalized.disposition
    event["_tether_mutation_disposition"] = disposition.value
    event["_tether_mutation_reason"] = normalized.reason
    if disposition is slack_protocol.MutationDisposition.NOT_MUTATION:
        return True
    if disposition in {
        slack_protocol.MutationDisposition.IGNORE,
        slack_protocol.MutationDisposition.INVALID,
    }:
        return False
    mutation = normalized.mutation
    if mutation is None:
        return False
    mutation_kind = mutation.kind.value
    nested_key = "message" if mutation_kind == "edit" else "previous_message"
    nested = event.get(nested_key)
    has_message_context = isinstance(nested, Mapping)
    actor_user_id = mutation.actor_user_id
    thread_ts = mutation.thread_ts
    if (
        mutation_kind == "delete"
        and (actor_user_id is None or not has_message_context)
        and mutation.team_id
        and mutation.channel_id
    ):
        persisted = store.slack_mutation_target_identity(
            mutation.team_id,
            mutation.channel_id,
            mutation.target_ts,
        )
        if persisted is not None:
            actor_user_id = persisted["user_id"]
            thread_ts = persisted["thread_ts"]
            has_message_context = True
    if (
        mutation.event_ts is None
        or actor_user_id is None
        or not has_message_context
    ):
        event["_tether_mutation_disposition"] = "invalid"
        event["_tether_mutation_reason"] = "mutation_identity_unresolved"
        return False
    replacement_text = mutation.replacement_text or ""
    if mutation_kind == "edit" and not replacement_text.strip():
        event["_tether_mutation_disposition"] = "invalid"
        event["_tether_mutation_reason"] = "replacement_text_missing"
        return False
    mutation_ts = mutation.event_ts
    if mutation_ts == mutation.target_ts:
        mutation_ts = f"{mutation.target_ts}:{mutation_kind}"
    thread_ts = thread_ts or mutation.target_ts
    if mutation_kind == "edit":
        notice = (
            f"[Slack message {mutation.target_ts} was edited. Disregard its prior text "
            "and use this authoritative replacement.]\n\n"
            f"{replacement_text}"
        )
    else:
        notice = (
            f"[Slack message {mutation.target_ts} was deleted. Stop or disregard work "
            "that depended only on that message.]"
        )
    updates = {
        "ts": mutation_ts,
        "thread_ts": thread_ts,
        "text": notice,
        "user": actor_user_id,
        "subtype": "",
        "_tether_original_subtype": (
            "message_changed" if mutation_kind == "edit" else "message_deleted"
        ),
        "_tether_mutation": {
            "kind": mutation_kind,
            "target_ts": mutation.target_ts,
            "replacement_text": replacement_text,
        },
    }
    if mutation.channel_id:
        updates["channel"] = mutation.channel_id
    if mutation.team_id:
        updates["team"] = mutation.team_id
    event.update(updates)
    return True


async def _route_slack_event(adapter, event: dict[str, Any], payload: Any = None):
    existing = event.get(ROUTING_DECISION_KEY)
    if isinstance(existing, routing.RoutingDecision):
        return existing
    channel_id = _event_channel_id(event)
    team_id = _event_team_id(adapter, event, payload)
    message_ts = str(event.get("ts") or "")
    if not team_id or not channel_id or not message_ts:
        event[ROUTING_ERROR_KEY] = "slack_identity_unresolved"
        return None
    if not await _broker_owns_workspace(team_id):
        event[ROUTING_ERROR_KEY] = "slack_workspace_not_owned"
        return None
    policy = _routing_policy(adapter, team_id)
    if policy is None:
        event[ROUTING_ERROR_KEY] = "local_bot_identity_unresolved"
        return None
    conversation_kind = await _conversation_kind(
        adapter, event, channel_id, team_id
    )
    if conversation_kind is None:
        event[ROUTING_ERROR_KEY] = "conversation_identity_unresolved"
        return None
    actor_kind = await _event_actor_kind(
        adapter,
        event,
        channel_id=channel_id,
        team_id=team_id,
    )
    if actor_kind is None:
        event[ROUTING_ERROR_KEY] = "actor_identity_unresolved"
        return None
    is_bot = actor_kind == "bot"
    try:
        actor = routing.ActorIdentity(
            user_id=str(event.get("user") or ""),
            bot_id=str(event.get("bot_id") or ""),
            is_bot=is_bot,
        )
        mentions = await _classify_mentions(
            adapter,
            event,
            team_id,
            channel_id,
            policy,
        )
        subtype = str(event.get("subtype") or "")
        if subtype == "message_changed":
            event_kind = routing.EventKind.EDIT
        elif subtype == "message_deleted":
            event_kind = routing.EventKind.DELETE
        else:
            event_kind = routing.EventKind.MESSAGE
        message = routing.NormalizedMessage(
            identity=routing.MessageIdentity(team_id, channel_id, message_ts),
            actor=actor,
            conversation_kind=conversation_kind,
            observed_at=time.time(),
            thread_ts=str(event.get("thread_ts") or "") or None,
            event_kind=event_kind,
            mentioned_user_ids=mentions[0],
            mentioned_bot_user_ids=mentions[1],
            mentioned_human_user_ids=mentions[2],
            unresolved_mention_user_ids=mentions[3],
        )
    except (TypeError, ValueError):
        event[ROUTING_ERROR_KEY] = "slack_message_normalization_failed"
        return None
    thread, thread_error = await _routing_thread_state(
        adapter,
        message,
        policy,
        use_cached_snapshot=bool(event.get("_tether_polled")),
        require_participant_history=not bool(mentions[1] or mentions[3]),
    )
    if thread_error:
        event[ROUTING_ERROR_KEY] = thread_error
        return None
    decision = routing.decide_route(message, thread, policy)
    event[ROUTING_DECISION_KEY] = decision
    return decision


def _prepare_authoritative_hermes_gate(adapter) -> bool:
    extra = getattr(getattr(adapter, "config", None), "extra", None)
    if not isinstance(extra, dict):
        return False
    # The pure router already enforces peer-bot trust and exact targeting.
    # Leaving these legacy Hermes gates active would create a second authority
    # that can reject a route Tether admitted.
    extra["allow_bots"] = "mentions"
    extra["strict_mention"] = False
    return True


def _allowed_peer_bot_users() -> set[str]:
    return {
        value.strip()
        for value in os.getenv("TETHER_ALLOWED_BOT_USERS", "").split(",")
        if value.strip()
    }


def _resolve_slack_adapter():
    errors = []
    live_module = "hermes_plugins.slack_platform.adapter"
    module = sys.modules.get(live_module)
    if module is None:
        try:
            from gateway.platform_registry import platform_registry
            platform_registry.get("slack")
            module = sys.modules.get(live_module)
        except (ImportError, AttributeError) as exc:
            errors.append(f"gateway.platform_registry: {type(exc).__name__}")
    if module is not None:
        adapter = getattr(module, "SlackAdapter", None)
        if adapter is not None and hasattr(adapter, "_handle_slack_message"):
            return adapter
        errors.append(f"{live_module}: incompatible")

    for module_name in (live_module, "plugins.platforms.slack.adapter"):
        try:
            module = importlib.import_module(module_name)
            adapter = getattr(module, "SlackAdapter")
            if hasattr(adapter, "_handle_slack_message"):
                return adapter
        except (ImportError, AttributeError) as exc:
            errors.append(f"{module_name}: {type(exc).__name__}")
    raise RuntimeError(
        "Hermes Slack adapter is unavailable or incompatible with Tether ("
        + "; ".join(errors) + ")"
    )


def _validate_hermes_compatibility() -> str:
    return hermes_compat.validate_adapter(_resolve_slack_adapter())


def _is_slack_gateway_event(event: Any) -> bool:
    source = getattr(event, "source", None)
    platform = getattr(getattr(source, "platform", None), "value", "")
    return platform == "slack"


async def _renew_thread_ingress_lease(event_id: str, lease_id: str) -> None:
    while True:
        await asyncio.sleep(15)
        if not store.renew_thread_ingress(event_id, lease_id):
            return


def _finalize_hermes_ingress_claim(
    event: Mapping[str, Any],
    ingress_claim: tuple[str, str, int],
    egress_context: Mapping[str, Any] | None,
) -> None:
    gateway_started = bool(event.get("_tether_gateway_dispatch_started"))
    egress_failed = str((egress_context or {}).get("failed") or "")
    sealed = store.seal_thread_ingress_egress(
        *ingress_claim,
        allow_empty=gateway_started and not egress_failed,
    )
    if sealed in {"completed", "pending"}:
        if isinstance(event, dict):
            event["_tether_ingress_dispatched"] = True
    elif sealed == "empty" and gateway_started:
        if not store.mark_thread_ingress_uncertain(
            *ingress_claim,
            error_code=(
                egress_failed
                or "hermes_egress_reservation_failed"
            ),
        ):
            log.error(
                "Tether could not fence failed Hermes egress; "
                "operator review is required"
            )
    elif sealed == "empty":
        store.release_thread_ingress(
            ingress_claim[0],
            ingress_claim[1],
            error_code="hermes_gateway_not_started",
        )
    else:
        log.error(
            "Tether could not seal Hermes ingress egress; "
            "the event will not be blindly retried"
        )


async def _finalize_deferred_hermes_ingress(
    event: dict[str, Any],
    ingress_claim: tuple[str, str, int],
    egress_context: dict[str, Any] | None,
    heartbeat: asyncio.Task | None,
) -> None:
    """Finalize ingress after Hermes's spawned background task finishes.

    Hermes's platform handler intentionally returns immediately after creating
    the per-session processing task. Finalizing at that foreground return
    races the gateway hook and releases the durable claim before the task can
    use it. Keep the claim lease alive until the exact task that crossed the
    authoritative hook completes.
    """

    timeout_seconds = _bounded_env_int(
        "TETHER_HERMES_DISPATCH_START_TIMEOUT_SECONDS",
        3600,
        30,
        86400,
    )
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_seconds
    dispatch_task: asyncio.Task | None = None
    try:
        while loop.time() < deadline:
            candidate = event.get("_tether_gateway_dispatch_task")
            if isinstance(candidate, asyncio.Task):
                dispatch_task = candidate
                break
            await asyncio.sleep(0.01)
        if dispatch_task is None:
            store.release_thread_ingress(
                ingress_claim[0],
                ingress_claim[1],
                error_code="hermes_gateway_start_timeout",
            )
            return
        try:
            await asyncio.shield(dispatch_task)
        except asyncio.CancelledError:
            if not dispatch_task.cancelled():
                raise
            store.mark_thread_ingress_uncertain(
                *ingress_claim,
                error_code="hermes_background_cancelled",
            )
            return
        except Exception as exc:
            store.mark_thread_ingress_uncertain(
                *ingress_claim,
                error_code=type(exc).__name__,
            )
            return
        _finalize_hermes_ingress_claim(
            event,
            ingress_claim,
            egress_context,
        )
    except asyncio.CancelledError:
        if event.get("_tether_gateway_dispatch_started"):
            store.mark_thread_ingress_uncertain(
                *ingress_claim,
                error_code="hermes_finalizer_cancelled",
            )
        else:
            store.release_thread_ingress(
                ingress_claim[0],
                ingress_claim[1],
                error_code="hermes_gateway_not_started",
            )
        raise
    finally:
        if heartbeat is not None:
            heartbeat.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await heartbeat


def _hermes_ingress_payload(
    event: Mapping[str, Any],
    mutation: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    persisted = event.get("_tether_persisted_ingress_payload")
    if isinstance(persisted, Mapping):
        return dict(persisted)
    payload: dict[str, Any] = {
        "text": hermes_compat.mention_detection_text(event),
        "subtype": str(event.get("subtype") or ""),
        "message_ts": str(event.get("ts") or ""),
        "event_thread_ts": str(event.get("thread_ts") or ""),
        "user": str(event.get("user") or ""),
        "bot_id": str(event.get("bot_id") or ""),
        "channel_type": str(event.get("channel_type") or ""),
        "polled": bool(event.get("_tether_polled")),
    }
    if mutation is not None:
        payload["mutation"] = dict(mutation)
    return payload


def _replayed_routing_event(record: Mapping[str, Any]) -> dict[str, Any] | None:
    payload = record.get("payload")
    if not isinstance(payload, Mapping):
        return None
    event_id = str(record.get("event_id") or "")
    message_ts = str(payload.get("message_ts") or "")
    if not message_ts and event_id.startswith("slack:"):
        message_ts = event_id.rsplit(":", 1)[-1]
    team_id = str(record.get("team_id") or "")
    channel_id = str(record.get("channel_id") or "")
    if not message_ts or not team_id or not channel_id:
        return None
    event: dict[str, Any] = {
        "team": team_id,
        "channel": channel_id,
        "ts": message_ts,
        "thread_ts": str(payload.get("event_thread_ts") or ""),
        "text": str(payload.get("text") or ""),
        "subtype": str(payload.get("subtype") or ""),
        "user": str(payload.get("user") or ""),
        "bot_id": str(payload.get("bot_id") or ""),
        "channel_type": str(
            payload.get("channel_type")
            or ("im" if channel_id.startswith("D") else "channel")
        ),
        "_tether_polled": bool(payload.get("polled")),
        "_tether_local_replay": True,
        "_tether_persisted_ingress_payload": dict(payload),
    }
    mutation = payload.get("mutation")
    if isinstance(mutation, Mapping):
        event["_tether_mutation"] = dict(mutation)
    return event


def _replayed_hermes_event(record: Mapping[str, Any]) -> dict[str, Any] | None:
    payload = record.get("payload")
    if not isinstance(payload, Mapping):
        return None
    event_id = str(record.get("event_id") or "")
    message_ts = str(payload.get("message_ts") or "")
    if not message_ts and event_id.startswith("slack:"):
        message_ts = event_id.rsplit(":", 1)[-1]
    team_id = str(record.get("team_id") or "")
    channel_id = str(record.get("channel_id") or "")
    if not message_ts or not team_id or not channel_id:
        return None
    event: dict[str, Any] = {
        "team": team_id,
        "channel": channel_id,
        "ts": message_ts,
        "thread_ts": str(payload.get("event_thread_ts") or ""),
        "text": str(payload.get("text") or ""),
        "subtype": str(payload.get("subtype") or ""),
        "user": str(payload.get("user") or ""),
        "bot_id": str(payload.get("bot_id") or ""),
        "channel_type": str(
            payload.get("channel_type")
            or ("im" if channel_id.startswith("D") else "channel")
        ),
        "_tether_polled": True,
        "_tether_local_replay": True,
        "_tether_persisted_ingress_payload": dict(payload),
    }
    mutation = payload.get("mutation")
    if isinstance(mutation, Mapping):
        event["_tether_mutation"] = dict(mutation)
    event[ROUTING_DECISION_KEY] = routing.RoutingDecision(
        action=routing.RouteAction.HERMES,
        reason="durable_pre_dispatch_replay",
        message_identity=routing.MessageIdentity(
            team_id,
            channel_id,
            message_ts,
        ),
        writer_id=str(record.get("writer_id") or ""),
        bridge_id=str(record.get("bridge_id") or "") or None,
        binding_generation=record.get("binding_generation"),
    )
    return event


async def _recover_pending_slack_ingress(adapter) -> int:
    recovered = 0
    for record in store.recoverable_routing_ingress(limit=20):
        event = _replayed_routing_event(record)
        if event is None:
            log.error(
                "Tether cannot reconstruct pending Slack routing ingress %s",
                record.get("event_id", "unknown"),
            )
            continue
        try:
            await adapter._handle_slack_message(event)
        except Exception as exc:
            log.error(
                "Tether local Slack routing replay failed for %s (%s)",
                record.get("event_id", "unknown"),
                type(exc).__name__,
            )
            continue
        if event.get("_tether_ingress_dispatched") or event.get(
            "_tether_ingress_transferred"
        ):
            recovered += 1
    for record in store.recoverable_hermes_ingress(limit=20):
        event = _replayed_hermes_event(record)
        if event is None:
            log.error(
                "Tether cannot reconstruct pending Hermes ingress %s",
                record.get("event_id", "unknown"),
            )
            continue
        try:
            await adapter._handle_slack_message(event)
        except Exception as exc:
            log.error(
                "Tether local Hermes ingress replay failed for %s (%s)",
                record.get("event_id", "unknown"),
                type(exc).__name__,
            )
            continue
        if event.get("_tether_ingress_dispatched"):
            recovered += 1
    return recovered


def _hermes_send_arguments(
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
) -> tuple[str, str, str, dict[str, Any]]:
    chat_id = str(kwargs.get("chat_id") or (args[0] if args else ""))
    content_value = kwargs.get("content")
    if content_value is None and len(args) >= 2:
        content_value = args[1]
    content = str(content_value or "")
    positional_metadata = (
        args[2]
        if len(args) == 3
        and isinstance(args[2], Mapping)
        and "metadata" not in kwargs
        and "reply_to" not in kwargs
        else None
    )
    reply_to = str(
        kwargs.get("reply_to")
        or (
            args[2]
            if len(args) >= 3 and positional_metadata is None
            else ""
        )
        or ""
    )
    metadata_value = kwargs.get("metadata")
    if metadata_value is None and len(args) >= 4:
        metadata_value = args[3]
    if metadata_value is None:
        metadata_value = positional_metadata
    metadata = dict(metadata_value) if isinstance(metadata_value, Mapping) else {}
    if not chat_id:
        raise ValueError("Hermes Slack destination is required")
    return chat_id, content, reply_to, metadata


def _is_silence_control_output(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    return not value.strip() or runtime.is_silence_control_output(value)


def _hermes_workspace_id(
    adapter,
    chat_id: str,
    metadata: dict[str, Any],
) -> str:
    resolver = getattr(adapter, "_metadata_team_id", None)
    from_metadata = str(resolver(metadata) or "") if callable(resolver) else ""
    return str(
        from_metadata
        or getattr(adapter, "_channel_team", {}).get(chat_id, "")
        or ""
    )


def _hermes_message_group_id(
    egress_context: dict[str, Any] | None,
    *,
    team_id: str,
    channel_id: str,
    thread_ts: str,
    messages: list[dict[str, Any]],
) -> str:
    payload_hash = hashlib.sha256(
        json.dumps(
            {
                "team_id": team_id,
                "channel_id": channel_id,
                "thread_ts": thread_ts,
                "messages": messages,
            },
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()
    if egress_context is None:
        return "hsg_" + uuid.uuid4().hex
    claim = egress_context.get("claim")
    if not isinstance(claim, tuple) or len(claim) != 3:
        raise RuntimeError("Hermes egress context has no ingress lease")
    sequence = egress_context.get("sequence", 0)
    if not isinstance(sequence, int) or sequence < 0:
        raise RuntimeError("Hermes egress sequence is invalid")
    return "hsg_" + hashlib.sha256(
        f"{claim[0]}\0{sequence}\0{payload_hash}".encode()
    ).hexdigest()[:32]


def _advance_hermes_egress_sequence(
    egress_context: dict[str, Any] | None,
) -> None:
    if egress_context is not None:
        egress_context["sequence"] = int(
            egress_context.get("sequence", 0)
        ) + 1


def _durable_slack_broker():
    server = state.broker
    broker = getattr(server, "broker", None) if server is not None else None
    if broker is None or not callable(
        getattr(broker, "_deliver_staged_message", None)
    ):
        raise RuntimeError("Tether durable Slack broker is unavailable")
    return broker


async def _deliver_hermes_message_group(
    group_id: str,
    *,
    team_id: str,
    channel_id: str,
    thread_ts: str,
    messages: list[dict[str, Any]],
    egress_context: dict[str, Any] | None,
) -> dict[str, Any]:
    claim = (
        egress_context.get("claim")
        if isinstance(egress_context, dict)
        else None
    )
    reserve_kwargs: dict[str, Any] = {}
    if isinstance(claim, tuple) and len(claim) == 3:
        reserve_kwargs = {
            "ingress_event_id": str(claim[0]),
            "ingress_lease_id": str(claim[1]),
            "ingress_fence_epoch": int(claim[2]),
        }
    rows = await asyncio.to_thread(
        store.reserve_message_group,
        group_id,
        team_id,
        channel_id,
        thread_ts,
        messages,
        **reserve_kwargs,
    )
    broker = _durable_slack_broker()
    last_result: dict[str, Any] = {}
    for row in rows:
        last_result = await asyncio.to_thread(
            broker._deliver_staged_message,
            str(row["idempotency_key"]),
        )
    return last_result


async def _deliver_hermes_message_update(
    group_id: str,
    *,
    team_id: str,
    channel_id: str,
    thread_ts: str,
    target_message_ts: str,
    text: str,
    options: dict[str, Any],
    egress_context: dict[str, Any] | None,
) -> dict[str, Any]:
    claim = (
        egress_context.get("claim")
        if isinstance(egress_context, dict)
        else None
    )
    reserve_kwargs: dict[str, Any] = {}
    if isinstance(claim, tuple) and len(claim) == 3:
        reserve_kwargs = {
            "ingress_event_id": str(claim[0]),
            "ingress_lease_id": str(claim[1]),
            "ingress_fence_epoch": int(claim[2]),
        }
    row = await asyncio.to_thread(
        store.reserve_message_update,
        group_id,
        team_id,
        channel_id,
        thread_ts,
        target_message_ts,
        text,
        options,
        **reserve_kwargs,
    )
    broker = _durable_slack_broker()
    return await asyncio.to_thread(
        broker._deliver_staged_message,
        str(row["idempotency_key"]),
    )


def _install_redacted_slack_method(
    adapter_type: type,
    method_name: str,
    *,
    text_fields: tuple[str, ...] = (),
    text_list_fields: tuple[str, ...] = (),
) -> None:
    original = getattr(adapter_type, method_name, None)
    if not callable(original):
        return

    @functools.wraps(original)
    async def redacted(self, *args, **kwargs):
        try:
            bound = inspect.signature(original).bind(
                self,
                *args,
                **kwargs,
            )
        except (TypeError, ValueError) as exc:
            raise RuntimeError(
                f"Hermes Slack {method_name} arguments are incompatible"
            ) from exc
        for field_name in text_fields:
            value = bound.arguments.get(field_name)
            if isinstance(value, str):
                bound.arguments[field_name] = redact_text(value)
        for field_name in text_list_fields:
            values = bound.arguments.get(field_name)
            if isinstance(values, list):
                bound.arguments[field_name] = [
                    redact_text(str(value))
                    for value in values
                ]
        return await original(*bound.args, **bound.kwargs)

    setattr(adapter_type, method_name, redacted)


def _safe_attachment_name(file_path: str) -> str:
    basename = Path(file_path).name
    cleaned = re.sub(
        r"[\x00-\x1f\x7f/\\]+",
        "-",
        redact_text(basename),
    ).strip(".- ")
    return (cleaned or "attachment")[:180]


def _install_guarded_slack_upload_method(
    adapter_type: type,
    method_name: str,
    *,
    path_field: str,
    preserve_document_name: bool = False,
) -> None:
    original = getattr(adapter_type, method_name, None)
    if not callable(original):
        return

    @functools.wraps(original)
    async def guarded(self, *args, **kwargs):
        try:
            bound = inspect.signature(original).bind(
                self,
                *args,
                **kwargs,
            )
        except (TypeError, ValueError) as exc:
            raise RuntimeError(
                f"Hermes Slack {method_name} arguments are incompatible"
            ) from exc
        source = bound.arguments.get(path_field)
        if not isinstance(source, str) or not source:
            return hermes_compat.make_send_result(
                original,
                success=False,
                error="attachment_rejected_by_tether",
            )
        try:
            staged = runtime.stage_safe_upload(source)
        except (runtime.security.SecurityError, OSError, ValueError):
            log.warning(
                "Tether rejected a Hermes Slack attachment before egress"
            )
            return hermes_compat.make_send_result(
                original,
                success=False,
                error="attachment_rejected_by_tether",
            )
        try:
            bound.arguments[path_field] = str(staged.path)
            if (
                preserve_document_name
                and not bound.arguments.get("file_name")
            ):
                bound.arguments["file_name"] = _safe_attachment_name(source)
            return await original(*bound.args, **bound.kwargs)
        finally:
            with contextlib.suppress(OSError):
                staged.path.unlink()

    setattr(adapter_type, method_name, guarded)


def _install_guarded_slack_image_batch(adapter_type: type) -> None:
    original = getattr(adapter_type, "send_multiple_images", None)
    if not callable(original):
        return

    @functools.wraps(original)
    async def guarded(self, *args, **kwargs):
        try:
            bound = inspect.signature(original).bind(
                self,
                *args,
                **kwargs,
            )
        except (TypeError, ValueError) as exc:
            raise RuntimeError(
                "Hermes Slack send_multiple_images arguments are incompatible"
            ) from exc
        images = bound.arguments.get("images")
        if not isinstance(images, list):
            return None
        staged_files = []
        safe_images = []
        try:
            for item in images:
                if (
                    not isinstance(item, (list, tuple))
                    or len(item) != 2
                ):
                    raise ValueError("invalid Hermes Slack image batch")
                image_url, alt_text = str(item[0]), redact_text(str(item[1]))
                if image_url.startswith("file://"):
                    source = urllib.parse.unquote(image_url[7:])
                    staged = runtime.stage_safe_upload(source)
                    staged_files.append(staged)
                    image_url = staged.path.as_uri()
                safe_images.append((image_url, alt_text))
            bound.arguments["images"] = safe_images
            return await original(*bound.args, **bound.kwargs)
        except (runtime.security.SecurityError, OSError, ValueError):
            log.warning(
                "Tether rejected a Hermes Slack image batch before egress"
            )
            return None
        finally:
            for staged in staged_files:
                with contextlib.suppress(OSError):
                    staged.path.unlink()

    setattr(adapter_type, "send_multiple_images", guarded)


def _install_durable_interaction_fallbacks(adapter_type: type) -> None:
    for method_name in ("send_exec_approval", "send_slash_confirm"):
        original = getattr(adapter_type, method_name, None)
        if not callable(original):
            continue

        @functools.wraps(original)
        async def require_text_fallback(
            self,
            *args,
            __original=original,
            **kwargs,
        ):
            return hermes_compat.make_send_result(
                __original,
                success=False,
                error="tether_requires_durable_text_interaction",
            )

        setattr(adapter_type, method_name, require_text_fallback)

    original_clarify = getattr(adapter_type, "send_clarify", None)
    if not callable(original_clarify):
        return

    @functools.wraps(original_clarify)
    async def durable_clarify(self, *args, **kwargs):
        try:
            bound = inspect.signature(original_clarify).bind(
                self,
                *args,
                **kwargs,
            )
        except (TypeError, ValueError) as exc:
            raise RuntimeError(
                "Hermes Slack send_clarify arguments are incompatible"
            ) from exc
        question = bound.arguments.get("question")
        if isinstance(question, str):
            bound.arguments["question"] = redact_text(question)
        choices = bound.arguments.get("choices")
        if isinstance(choices, list):
            bound.arguments["choices"] = [
                redact_text(str(choice))
                for choice in choices
            ]
        parent = super(adapter_type, self)
        fallback = getattr(parent, "send_clarify", None)
        if not callable(fallback):
            return hermes_compat.make_send_result(
                original_clarify,
                success=False,
                error="tether_durable_clarify_unavailable",
            )
        return await fallback(
            chat_id=bound.arguments.get("chat_id"),
            question=bound.arguments.get("question"),
            choices=bound.arguments.get("choices"),
            clarify_id=bound.arguments.get("clarify_id"),
            session_key=bound.arguments.get("session_key"),
            metadata=bound.arguments.get("metadata"),
        )

    setattr(adapter_type, "send_clarify", durable_clarify)


def _install_slack_bridge_prefilter():
    SlackAdapter = _resolve_slack_adapter()
    if not hasattr(SlackAdapter, "_handle_slack_message"):
        raise RuntimeError("Hermes Slack adapter is incompatible with Tether")
    if getattr(SlackAdapter, "_tether_prefilter", False):
        return
    original = SlackAdapter._handle_slack_message
    original_connect = SlackAdapter.connect
    original_send = getattr(SlackAdapter, "send", None)
    original_edit = getattr(SlackAdapter, "edit_message", None)
    original_restart = getattr(SlackAdapter, "_restart_socket_mode", None)

    @functools.wraps(original)
    async def bridged_handle(self, event, *args, **kwargs):
        ingress_claim: tuple[str, str, int] | None = None
        routing_event_id = ""
        payload = args[0] if args and isinstance(args[0], dict) else kwargs.get("payload")
        if not _normalize_slack_mutation(event, payload):
            if event.get("_tether_mutation_disposition") == "invalid":
                log.warning(
                    "Tether rejected Slack message mutation: %s",
                    event.get("_tether_mutation_reason", "invalid"),
                )
            return None
        if not event.get("_tether_polled"):
            state.last_inbound_at = time.monotonic()
            state.slack_transport_connected = True
        _ensure_reply_poller(self)
        try:
            channel_id = _event_channel_id(event)
            team_id = _event_team_id(self, event, payload)
            message_ts = str(event.get("ts") or "")
            if not team_id or not channel_id or not message_ts:
                event[ROUTING_ERROR_KEY] = "slack_identity_unresolved"
                return None
            routing_event_id = (
                f"slack:{team_id}:{channel_id}:{message_ts}"
            )
            ingress_thread_ts = str(event.get("thread_ts") or message_ts)
            mutation = event.get("_tether_mutation")
            ingress_payload = _hermes_ingress_payload(
                event,
                mutation if isinstance(mutation, Mapping) else None,
            )
            pre_routed_replay = bool(
                event.get("_tether_local_replay")
                and isinstance(
                    event.get(ROUTING_DECISION_KEY),
                    routing.RoutingDecision,
                )
            )
            if not pre_routed_replay:
                reservation = store.reserve_routing_ingress(
                    routing_event_id,
                    team_id,
                    channel_id,
                    ingress_thread_ts,
                    ingress_payload,
                )
                if reservation != "routing":
                    return None
            decision = await _route_slack_event(self, event, payload)
            if decision is None:
                reason = str(
                    event.get(ROUTING_ERROR_KEY) or "routing_unavailable"
                )
                if reason == "slack_workspace_not_owned":
                    store.cancel_routing_ingress(routing_event_id, reason)
                else:
                    store.defer_routing_ingress(routing_event_id, reason)
                log.warning(
                    "Tether dropped Slack event before Hermes routing: %s",
                    reason,
                )
                return None
            if decision.action is routing.RouteAction.SILENT:
                store.cancel_routing_ingress(
                    routing_event_id,
                    decision.reason,
                )
                return None
            if not _prepare_authoritative_hermes_gate(self):
                event[ROUTING_ERROR_KEY] = "hermes_routing_override_unavailable"
                store.defer_routing_ingress(
                    routing_event_id,
                    "hermes_routing_override_unavailable",
                )
                return None
            thread_ts = str(event.get("thread_ts") or "")
            if thread_ts:
                # Hermes's current Slack adapter has no public per-event routing
                # override. This compatibility mark bypasses only its mention
                # gate; Tether's pure decision above remains authoritative.
                self._bot_message_ts.add(thread_ts)
            if decision.action is routing.RouteAction.HERMES:
                event_id = routing_event_id
                channel_id = decision.message_identity.channel_id
                team_id = decision.message_identity.team_id
                if isinstance(mutation, dict):
                    target_ts = str(mutation.get("target_ts") or "")
                    if target_ts:
                        store.cancel_pending_hermes_ingress(
                            f"slack:{team_id}:{channel_id}:{target_ts}",
                            str(mutation.get("kind") or ""),
                        )
                claim = store.claim_thread_ingress(
                    event_id,
                    team_id,
                    channel_id,
                    ingress_thread_ts,
                    route_action="hermes",
                    writer_id=str(decision.writer_id or ""),
                    bridge_id=str(decision.bridge_id or ""),
                    binding_generation=decision.binding_generation,
                    payload=ingress_payload,
                )
                if claim["status"] != "claimed":
                    return None
                ingress_claim = (
                    event_id,
                    str(claim["lease_id"]),
                    int(claim["fence_epoch"]),
                )
                event["_tether_ingress_claim"] = ingress_claim
        except Exception as exc:
            if routing_event_id and ingress_claim is None:
                with contextlib.suppress(Exception):
                    store.defer_routing_ingress(
                        routing_event_id,
                        "routing_internal_error",
                    )
            log.error(
                "Tether routing failed closed before the Hermes Slack gate (%s)",
                type(exc).__name__,
            )
            return None
        heartbeat = (
            asyncio.create_task(
                _renew_thread_ingress_lease(
                    ingress_claim[0],
                    ingress_claim[1],
                )
            )
            if ingress_claim is not None
            else None
        )
        egress_context = (
            {"claim": ingress_claim, "failed": "", "sequence": 0}
            if ingress_claim is not None
            else None
        )
        egress_token = (
            _HERMES_EGRESS_CONTEXT.set(egress_context)
            if egress_context is not None
            else None
        )
        try:
            result = await original(self, event, *args, **kwargs)
        except BaseException as exc:
            if ingress_claim is not None:
                if event.get("_tether_gateway_dispatch_started"):
                    store.mark_thread_ingress_uncertain(
                        *ingress_claim,
                        error_code=type(exc).__name__,
                    )
                else:
                    store.release_thread_ingress(
                        ingress_claim[0],
                        ingress_claim[1],
                        error_code=type(exc).__name__,
                    )
            raise
        else:
            if ingress_claim is not None:
                gateway_started = bool(
                    event.get("_tether_gateway_dispatch_started")
                )
                dispatch_task = event.get(
                    "_tether_gateway_dispatch_task"
                )
                current_handler_task = asyncio.current_task()
                background_running = (
                    isinstance(dispatch_task, asyncio.Task)
                    and dispatch_task is not current_handler_task
                    and not dispatch_task.done()
                )
                if not gateway_started or background_running:
                    finalizer = asyncio.create_task(
                        _finalize_deferred_hermes_ingress(
                            event,
                            ingress_claim,
                            egress_context,
                            heartbeat,
                        )
                    )
                    state.hermes_ingress_finalizers.add(finalizer)
                    finalizer.add_done_callback(
                        state.hermes_ingress_finalizers.discard
                    )
                    heartbeat = None
                else:
                    _finalize_hermes_ingress_claim(
                        event,
                        ingress_claim,
                        egress_context,
                    )
            return result
        finally:
            if egress_token is not None:
                _HERMES_EGRESS_CONTEXT.reset(egress_token)
            if heartbeat is not None:
                heartbeat.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await heartbeat

    SlackAdapter._handle_slack_message = bridged_handle

    @functools.wraps(original_connect)
    async def bridged_connect(self, *args, **kwargs):
        connected = await original_connect(self, *args, **kwargs)
        state.slack_transport_connected = bool(connected)
        if connected:
            imported = _import_native_slack_participation(self)
            if imported:
                log.info("Tether imported %d recent native Slack thread(s)", imported)
            _ensure_reply_poller(self)
        return connected

    SlackAdapter.connect = bridged_connect
    if original_send is not None:
        @functools.wraps(original_send)
        async def bridged_send(self, *args, **kwargs):
            thread_ts = ""
            metadata: dict[str, Any] = {}
            egress_context = _HERMES_EGRESS_CONTEXT.get()
            try:
                channel_id, content, reply_to, metadata = (
                    _hermes_send_arguments(args, kwargs)
                )
                ignored = getattr(self, "_is_ignored_channel", None)
                if callable(ignored) and ignored(channel_id):
                    return hermes_compat.make_send_result(
                        original_send,
                        success=False,
                        error="ignored_channel",
                    )
                if _is_silence_control_output(content):
                    if callable(getattr(self, "stop_typing", None)):
                        with contextlib.suppress(Exception):
                            await self.stop_typing(
                                channel_id,
                                metadata=metadata,
                    )
                    return hermes_compat.make_send_result(
                        original_send,
                        success=True,
                    )
                team_id = _hermes_workspace_id(
                    self,
                    channel_id,
                    metadata,
                )
                dm_resolver = getattr(self, "_ensure_dm_conversation", None)
                if callable(dm_resolver):
                    channel_id = str(
                        await dm_resolver(
                            channel_id,
                            team_id=team_id or None,
                        )
                    )
                    team_id = _hermes_workspace_id(
                        self,
                        channel_id,
                        metadata,
                    ) or team_id
                if callable(ignored) and ignored(channel_id):
                    return hermes_compat.make_send_result(
                        original_send,
                        success=False,
                        error="ignored_channel",
                    )
                safe_content = redact_text(content)
                slash_context = (
                    self._pop_slash_context(
                        channel_id,
                        team_id=team_id or "",
                    )
                    if callable(getattr(self, "_pop_slash_context", None))
                    else None
                )
                if slash_context is not None:
                    return await self._send_slash_ephemeral(
                        slash_context,
                        safe_content,
                    )
                if not team_id:
                    raise RuntimeError(
                        "Slack workspace identity is required for coordinated egress"
                    )
                formatter = getattr(self, "format_message", None)
                formatted = (
                    str(formatter(safe_content))
                    if callable(formatter)
                    else safe_content
                )
                chunker = getattr(self, "truncate_message", None)
                max_length = min(
                    int(getattr(self, "MAX_MESSAGE_LENGTH", runtime.MAX_TEXT)),
                    int(runtime.MAX_TEXT),
                )
                chunks = (
                    list(chunker(formatted, max_length))
                    if callable(chunker)
                    else [formatted]
                )
                if not chunks or any(not isinstance(chunk, str) for chunk in chunks):
                    raise RuntimeError("Hermes Slack formatter returned no message")
                resolver = getattr(self, "_resolve_thread_ts", None)
                thread_ts = str(
                    resolver(reply_to, metadata)
                    if callable(resolver)
                    else (
                        metadata.get("thread_id")
                        or metadata.get("thread_ts")
                        or reply_to
                        or ""
                    )
                    or ""
                )
                blocks = (
                    self._maybe_blocks(safe_content)
                    if len(chunks) == 1
                    and callable(getattr(self, "_maybe_blocks", None))
                    else None
                )
                broadcast = bool(
                    getattr(getattr(self, "config", None), "extra", {}).get(
                        "reply_broadcast",
                        False,
                    )
                )
                messages: list[dict[str, Any]] = []
                for index, chunk in enumerate(chunks):
                    options: dict[str, Any] = {"mrkdwn": True}
                    if blocks and index == 0:
                        options["blocks"] = blocks
                    if thread_ts and broadcast and index == 0:
                        options["reply_broadcast"] = True
                    messages.append({"text": chunk, "options": options})
                group_id = _hermes_message_group_id(
                    egress_context,
                    team_id=team_id,
                    channel_id=channel_id,
                    thread_ts=thread_ts,
                    messages=messages,
                )
                result = await _deliver_hermes_message_group(
                    group_id,
                    team_id=team_id,
                    channel_id=channel_id,
                    thread_ts=thread_ts,
                    messages=messages,
                    egress_context=egress_context,
                )
                _advance_hermes_egress_sequence(egress_context)
                if thread_ts and callable(getattr(self, "stop_typing", None)):
                    with contextlib.suppress(Exception):
                        await self.stop_typing(
                            channel_id,
                            metadata=metadata,
                        )
                sent_ts = str(result.get("message_ts") or "")
                if sent_ts and hasattr(self, "_bot_message_ts"):
                    self._bot_message_ts.add(sent_ts)
                    if thread_ts:
                        self._bot_message_ts.add(thread_ts)
                    maximum = int(
                        getattr(self, "_BOT_TS_MAX", len(self._bot_message_ts) + 1)
                    )
                    if len(self._bot_message_ts) > maximum:
                        excess = len(self._bot_message_ts) - maximum // 2
                        for old_ts in list(self._bot_message_ts)[:excess]:
                            self._bot_message_ts.discard(old_ts)
                if thread_ts:
                    state.thread_bot_participants.pop(
                        (team_id, channel_id, thread_ts),
                        None,
                    )
                return hermes_compat.make_send_result(
                    original_send,
                    success=True,
                    message_id=sent_ts or None,
                    raw_response=result,
                )
            except Exception as exc:
                if isinstance(egress_context, dict):
                    egress_context["failed"] = (
                        "hermes_egress_" + type(exc).__name__
                    )[:128]
                if thread_ts and callable(getattr(self, "stop_typing", None)):
                    with contextlib.suppress(Exception):
                        await self.stop_typing(
                            str(
                                kwargs.get("chat_id")
                                or (args[0] if args else "")
                            ),
                            metadata=metadata,
                        )
                safe_error = redact_text(str(exc) or type(exc).__name__)[:500]
                log.error(
                    "Tether durable Hermes Slack send failed: %s",
                    type(exc).__name__,
                )
                return hermes_compat.make_send_result(
                    original_send,
                    success=False,
                    error=safe_error,
                    retryable=True,
                )

        SlackAdapter.send = bridged_send
    if original_edit is not None:
        @functools.wraps(original_edit)
        async def bridged_edit(self, *args, **kwargs):
            egress_context = _HERMES_EGRESS_CONTEXT.get()
            try:
                bound = inspect.signature(original_edit).bind(
                    self,
                    *args,
                    **kwargs,
                )
                channel_id = str(bound.arguments.get("chat_id") or "")
                message_id = str(bound.arguments.get("message_id") or "")
                content = str(bound.arguments.get("content") or "")
                metadata_value = bound.arguments.get("metadata")
                metadata = (
                    dict(metadata_value)
                    if isinstance(metadata_value, Mapping)
                    else {}
                )
                ignored = getattr(self, "_is_ignored_channel", None)
                if callable(ignored) and ignored(channel_id):
                    return hermes_compat.make_send_result(
                        original_edit,
                        success=False,
                        error="ignored_channel",
                    )
                team_id = _hermes_workspace_id(
                    self,
                    channel_id,
                    metadata,
                )
                if not team_id:
                    raise RuntimeError(
                        "Slack workspace identity is required for coordinated egress"
                    )
                safe_content = redact_text(content)
                formatter = getattr(self, "format_message", None)
                formatted = (
                    str(formatter(safe_content))
                    if callable(formatter)
                    else safe_content
                )
                chunker = getattr(self, "truncate_message", None)
                max_length = min(
                    int(getattr(self, "MAX_MESSAGE_LENGTH", runtime.MAX_TEXT)),
                    int(runtime.MAX_TEXT),
                )
                chunks = (
                    list(chunker(formatted, max_length))
                    if callable(chunker)
                    else [formatted]
                )
                if not chunks or not isinstance(chunks[0], str):
                    raise RuntimeError(
                        "Hermes Slack formatter returned no update"
                    )
                update_text = chunks[0]
                options: dict[str, Any] = {}
                if bool(bound.arguments.get("finalize")):
                    blocks = (
                        self._maybe_blocks(safe_content)
                        if callable(getattr(self, "_maybe_blocks", None))
                        else None
                    )
                    if blocks:
                        options["blocks"] = blocks
                resolver = getattr(self, "_resolve_thread_ts", None)
                thread_ts = str(
                    resolver(None, metadata)
                    if callable(resolver)
                    else (
                        metadata.get("thread_id")
                        or metadata.get("thread_ts")
                        or ""
                    )
                    or ""
                )
                identity = [{
                    "operation": "update",
                    "target_message_ts": message_id,
                    "text": update_text,
                    "options": options,
                }]
                group_id = _hermes_message_group_id(
                    egress_context,
                    team_id=team_id,
                    channel_id=channel_id,
                    thread_ts=thread_ts,
                    messages=identity,
                )
                result = await _deliver_hermes_message_update(
                    group_id,
                    team_id=team_id,
                    channel_id=channel_id,
                    thread_ts=thread_ts,
                    target_message_ts=message_id,
                    text=update_text,
                    options=options,
                    egress_context=egress_context,
                )
                _advance_hermes_egress_sequence(egress_context)
                return hermes_compat.make_send_result(
                    original_edit,
                    success=True,
                    message_id=message_id,
                    raw_response=result,
                )
            except Exception as exc:
                if isinstance(egress_context, dict):
                    egress_context["failed"] = (
                        "hermes_egress_" + type(exc).__name__
                    )[:128]
                log.error(
                    "Tether durable Hermes Slack update failed: %s",
                    type(exc).__name__,
                )
                return hermes_compat.make_send_result(
                    original_edit,
                    success=False,
                    error=redact_text(
                        str(exc) or type(exc).__name__
                    )[:500],
                    retryable=True,
                )

        SlackAdapter.edit_message = bridged_edit
    for method_name, text_fields, text_list_fields in (
        ("send_private_notice", ("content",), ()),
        ("_upload_file", ("caption",), ()),
        ("send_image_file", ("caption",), ()),
        ("send_image", ("caption",), ()),
        ("send_voice", ("caption",), ()),
        ("send_video", ("caption",), ()),
        ("send_document", ("caption", "file_name"), ()),
    ):
        _install_redacted_slack_method(
            SlackAdapter,
            method_name,
            text_fields=text_fields,
            text_list_fields=text_list_fields,
        )
    _install_durable_interaction_fallbacks(SlackAdapter)
    for method_name, path_field, preserve_document_name in (
        ("_upload_file", "file_path", False),
        ("send_video", "video_path", False),
        ("send_document", "file_path", True),
    ):
        _install_guarded_slack_upload_method(
            SlackAdapter,
            method_name,
            path_field=path_field,
            preserve_document_name=preserve_document_name,
        )
    _install_guarded_slack_image_batch(SlackAdapter)
    if original_restart is not None:
        @functools.wraps(original_restart)
        async def bridged_restart(self, *args, **kwargs):
            result = await original_restart(self, *args, **kwargs)
            try:
                state.slack_transport_connected = bool(await self._socket_transport_connected())
            except Exception:
                state.slack_transport_connected = False
            return result

        SlackAdapter._restart_socket_mode = bridged_restart
    SlackAdapter._tether_prefilter = True


async def _poll_recent_replies(adapter) -> int:
    hours = _bounded_env_int("TETHER_REPLY_RECOVERY_HOURS", 24, 1, 168)
    workspace_limit = _bounded_env_int("TETHER_REPLY_POLL_BATCH", 10, 1, 25)
    max_pages = _bounded_env_int("TETHER_REPLY_POLL_MAX_PAGES", 25, 1, 100)
    bridges = [
        bridge
        for bridge in store.active_bridges()
        if bridge.team_id and bridge.channel_id and bridge.thread_ts
    ]
    bridge_keys = {(bridge.team_id, bridge.channel_id, bridge.thread_ts) for bridge in bridges}
    participating = [
        item for item in store.recent_participating_threads(hours=max(hours, 168), limit=500)
        if all(item[:3]) and item[:3] not in bridge_keys
    ]
    targets = [
        (bridge, bridge.team_id, bridge.channel_id, str(bridge.thread_ts), None)
        for bridge in bridges
    ] + [(None, *item) for item in participating]
    target_keys = {
        (team_id, channel_id, thread_ts)
        for _bridge, team_id, channel_id, thread_ts, _since in targets
    }
    durable_polling = all(
        callable(getattr(store, name, None))
        for name in (
            "claim_slack_read_budget",
            "select_reply_poll_targets",
            "reply_poll_page_state",
            "save_reply_poll_page_state",
            "clear_reply_poll_page_state",
        )
    )
    if not durable_polling:
        log.error(
            "Tether disabled Slack reply recovery because the Store lacks "
            "durable coordinated polling"
        )
        return 0
    selected_keys = store.select_reply_poll_targets(
        sorted(target_keys),
        workspace_limit=workspace_limit,
    )
    by_key = {
        (target[1], target[2], target[3]): target
        for target in targets
    }
    batch = [
        by_key[key]
        for key in selected_keys
        if key in by_key
    ]
    if not batch:
        return 0
    oldest = f"{time.time() - hours * 3600:.6f}"
    recovered = 0
    succeeded = 0
    failures = 0
    for bridge, team_id, channel_id, thread_ts, participation_since in batch:
        thread_key = (team_id, channel_id, thread_ts)
        state.thread_bot_participants.pop(thread_key, None)
        state.thread_root_bridges.pop(thread_key, None)
        try:
            client = hermes_compat.workspace_client(
                adapter, channel_id, team_id
            )
            channel_key = (team_id, channel_id)
            if channel_id.startswith("C") and channel_key not in state.joined_channels:
                if not store.claim_slack_read_budget(
                    team_id,
                    "conversations.history",
                ):
                    continue
                try:
                    await _slack_api_call(
                        team_id,
                        "conversations.history",
                        lambda: client.conversations_history(
                            channel=channel_id,
                            limit=1,
                        ),
                    )
                except Exception as exc:
                    if "not_in_channel" not in str(exc):
                        raise
                    await _slack_api_call(
                        team_id,
                        "conversations.join",
                        lambda: client.conversations_join(channel=channel_id),
                    )
                state.joined_channels.add(channel_key)
            saved_page = store.reply_poll_page_state(*thread_key)
            if saved_page is None:
                page_state = slack_protocol.CursorState()
                page_oldest = (
                    f"{max(float(oldest), participation_since):.6f}"
                    if participation_since is not None
                    else oldest
                )
                pending_messages: tuple[dict[str, str], ...] = ()
            else:
                page_state = slack_protocol.CursorState(
                    next_cursor=saved_page.next_cursor,
                    seen_cursors=saved_page.seen_cursors,
                    pages_seen=saved_page.pages_seen,
                )
                page_oldest = saved_page.page_oldest
                accumulated_bots = set(saved_page.bot_user_ids)
                accumulated_root = saved_page.root_bridge_id
                pending_messages = saved_page.pending_messages
            if saved_page is None:
                accumulated_bots: set[str] = set()
                accumulated_root = ""
            request: dict[str, Any] = {
                "channel": channel_id,
                "ts": thread_ts,
                "oldest": page_oldest,
                "inclusive": False,
                "limit": 15,
                "include_all_metadata": True,
            }
            if page_state.next_cursor:
                request["cursor"] = page_state.next_cursor
            if not store.claim_slack_read_budget(
                team_id,
                "conversations.replies",
            ):
                continue
            result = await _slack_api_call(
                team_id,
                "conversations.replies",
                lambda: client.conversations_replies(**request),
            )
            page = _validate_slack_cursor_page(
                result,
                page_state,
                max_pages=max_pages,
            )
            messages = page.page.items
            fragment = await _thread_snapshot_fragment(
                adapter,
                team_id,
                channel_id,
                thread_ts,
                messages,
            )
            if fragment is None:
                raise RuntimeError("Slack thread participant identity is unresolved")
            fragment_bots, fragment_root = fragment
            accumulated_bots.update(fragment_bots)
            if fragment_root:
                if accumulated_root and accumulated_root != fragment_root:
                    raise RuntimeError("Slack thread root identity changed across pages")
                accumulated_root = fragment_root
            if page.page.complete:
                _store_complete_thread_snapshot(
                    team_id,
                    channel_id,
                    thread_ts,
                    frozenset(accumulated_bots),
                    accumulated_root,
                )
            messages_to_handle = _merge_reply_poll_messages(
                pending_messages,
                messages,
                thread_ts,
            )
            succeeded += 1
        except Exception as exc:
            failures += 1
            target = bridge.bridge_id if bridge is not None else f"{channel_id}:{thread_ts}"
            detail = (
                str(exc)
                if isinstance(exc, hermes_compat.HermesCompatibilityError)
                else type(exc).__name__
            )
            log.warning("Could not poll Tether thread %s: %s", target, detail)
            continue
        if not page.page.complete:
            store.save_reply_poll_page_state(
                *thread_key,
                next_cursor=page.state.next_cursor,
                seen_cursors=page.state.seen_cursors,
                pages_seen=page.state.pages_seen,
                page_oldest=page_oldest,
                bot_user_ids=tuple(sorted(accumulated_bots)),
                root_bridge_id=accumulated_root,
                pending_messages=messages_to_handle,
            )
            continue
        for message in messages_to_handle:
            event = dict(message)
            event.update({
                "channel": channel_id,
                "team": team_id,
                "thread_ts": thread_ts,
                "_tether_polled": True,
            })
            if channel_id.startswith("D"):
                event["channel_type"] = "im"
            await adapter._handle_slack_message(event)
            if event.get("_tether_ingress_dispatched") or event.get(
                "_tether_ingress_transferred"
            ):
                recovered += 1
        store.clear_reply_poll_page_state(*thread_key)
    if failures and not succeeded:
        raise RuntimeError("every Slack thread poll failed")
    return recovered


async def _reply_poll_loop(adapter) -> None:
    while True:
        try:
            locally_recovered = await _recover_pending_slack_ingress(adapter)
            recovered = await _poll_recent_replies(adapter)
            state.last_poll_at = time.monotonic()
            total_recovered = recovered + locally_recovered
            if total_recovered:
                log.info(
                    "Tether recovered %d admitted Slack turn%s",
                    total_recovered,
                    "" if total_recovered == 1 else "s",
                )
            for bridge_id in store.queued_bridge_ids():
                _schedule_bridge_drain(bridge_id)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            state.last_poll_error_at = time.monotonic()
            log.error(
                "Tether Slack reply poll failed (%s)",
                type(exc).__name__,
            )
        await asyncio.sleep(_reply_poll_interval())


def _ensure_reply_poller(adapter) -> None:
    if state.reply_poller is None or state.reply_poller.done():
        state.reply_poller = asyncio.get_running_loop().create_task(_reply_poll_loop(adapter))


def _reply_delta(text: str) -> str:
    marker = "[End of thread context]"
    if marker in text:
        text = text.rsplit(marker, 1)[1]
    return text.strip()


async def _remove_processing_reaction(
    adapter,
    *,
    channel_id: str,
    message_ts: str,
    team_id: str,
) -> Any:
    def operation():
        return hermes_compat.remove_reaction(
            adapter,
            channel_id=channel_id,
            message_ts=message_ts,
            emoji="eyes",
            team_id=team_id,
        )
    if not team_id:
        return None
    return await _slack_api_call(team_id, "reactions.remove", operation)


def _suppress_bridge_reaction(event, gateway):
    adapter = gateway.adapters[event.source.platform]
    event_id = str(event.message_id or event.source.message_id or "")
    team_id = str(
        getattr(event.source, "scope_id", "")
        or getattr(event.source, "guild_id", "")
        or ""
    )
    marker = hermes_compat.reaction_marker(team_id, event_id)
    reacting = getattr(adapter, "_reacting_message_ids", None)
    if reacting is not None:
        reacting.discard(marker)
    if event_id and hasattr(adapter, "_remove_reaction"):
        asyncio.get_running_loop().create_task(
            _remove_processing_reaction(
                adapter,
                channel_id=str(event.source.chat_id),
                message_ts=event_id,
                team_id=team_id,
            )
        )


def _failure_reason(exc: Exception) -> str:
    text = str(exc).lower()
    if "credential" in text or "authentication" in text or "401" in text:
        return "the native session credential could not be obtained or authenticated"
    if "timed out" in text:
        return "the continuation timed out and was stopped cleanly"
    if "cancelled" in text:
        return "the continuation was cancelled by the operator"
    if "session" in text and ("no longer" in text or "not found" in text or "invalid" in text):
        return "the captured agent session is no longer resumable"
    return "the bound session could not be resumed"


def _batch_prompt(items) -> str:
    if len(items) == 1:
        return items[0]["text"]
    sections = [
        f"[Slack follow-up {index} of {len(items)}]\n{item['text']}"
        for index, item in enumerate(items, start=1)
    ]
    return (
        "These follow-ups arrived while the bound session was busy. Handle them together, "
        "using the latest message when requests overlap.\n\n" + "\n\n".join(sections)
    )


def _finish_batch(items, error: str | None = None) -> None:
    for item in items:
        store.finish_event(item["event_id"], error)


def _run_recovered_event(bridge, items):
    prompt = _batch_prompt(items)
    if _has_bound_zellij_pane(bridge):
        _submit_zellij_attempt(bridge, items, prompt)
        return None
    attempt_id, response = _submit_detached_attempt(
        bridge,
        items,
        prompt,
        None,
    )
    if response != "NO_REPLY":
        broker_call({
            "op": "reply",
            "bridge_id": bridge.bridge_id,
            "reply_key": attempt_id,
            "text": response,
        })
    return None


def _has_bound_zellij_pane(bridge) -> bool:
    return runtime.source_binding(bridge).endpoint_kind == "zellij_pane"


def _submit_zellij_attempt(bridge, items, prompt: str) -> str:
    event_ids = [str(item["event_id"]) for item in items]
    attempt_id = runtime.delivery_attempt_id(
        bridge.bridge_id,
        event_ids,
        bridge.binding_generation,
    )
    if not store.prepare_delivery_attempt(
        event_ids,
        bridge.bridge_id,
        bridge.binding_generation,
        attempt_id,
    ):
        raise runtime.NativeContinuationError(
            "binding changed before pane delivery; retry against the current binding",
            code="binding_generation_changed",
        )
    if not store.mark_attempt_submitting(
        attempt_id,
        bridge.bridge_id,
        bridge.binding_generation,
    ):
        store.requeue_prepared_attempt(
            attempt_id,
            bridge.bridge_id,
            "binding_changed_before_submission",
        )
        raise runtime.NativeContinuationError(
            "binding changed before pane submission started",
            code="binding_generation_changed",
        )
    try:
        deliver_zellij(bridge, prompt, attempt_id)
    except Exception as exc:
        if store.attempt_state(attempt_id, bridge.bridge_id) in {
            "replying",
            "acknowledged",
        }:
            return attempt_id
        if (
            isinstance(exc, runtime.NativeContinuationError)
            and exc.code == "terminal_submit_not_started"
        ):
            store.requeue_prepared_attempt(
                attempt_id,
                bridge.bridge_id,
                exc.code,
            )
        else:
            store.mark_attempt_uncertain(
                attempt_id,
                bridge.bridge_id,
                (
                    exc.code
                    if isinstance(exc, runtime.NativeContinuationError)
                    else "terminal_submit_uncertain"
                ),
            )
        raise
    if not store.mark_attempt_awaiting_ack(
        attempt_id,
        bridge.bridge_id,
        bridge.binding_generation,
    ):
        store.fail_attempt(
            attempt_id,
            bridge.bridge_id,
            "binding changed before pane acknowledgment could be armed",
        )
        raise runtime.NativeContinuationError(
            "binding changed after pane submission; rebind before retrying",
            code="binding_generation_changed",
        )
    return attempt_id


def _submit_detached_attempt(
    bridge,
    items,
    prompt: str,
    cancellation: threading.Event | None,
) -> tuple[str, str]:
    event_ids = [str(item["event_id"]) for item in items]
    attempt_id = runtime.delivery_attempt_id(
        bridge.bridge_id,
        event_ids,
        bridge.binding_generation,
    )
    if not store.prepare_delivery_attempt(
        event_ids,
        bridge.bridge_id,
        bridge.binding_generation,
        attempt_id,
        delivery_kind="detached_native",
    ):
        raise runtime.NativeContinuationError(
            "binding changed before native continuation; retry against the current binding",
            code="binding_generation_changed",
        )
    if not store.mark_attempt_submitting(
        attempt_id,
        bridge.bridge_id,
        bridge.binding_generation,
    ):
        store.requeue_prepared_attempt(
            attempt_id,
            bridge.bridge_id,
            "binding_changed_before_submission",
        )
        raise runtime.NativeContinuationError(
            "binding changed before native continuation started",
            code="binding_generation_changed",
        )
    persisted: dict[str, str] = {}

    def persist_response(response: str) -> None:
        cleaned = validate_reply_text(response or "")
        if cleaned == "NO_REPLY":
            if store.acknowledge_attempt(
                attempt_id,
                bridge.bridge_id,
                ack_kind="no_reply",
            ) <= 0:
                raise runtime.NativeContinuationError(
                    "The detached continuation acknowledgment could not be persisted.",
                    code="binding_generation_changed",
                    binding_id=bridge.bridge_id,
                )
        else:
            stage_reply_payload(
                store,
                bridge.bridge_id,
                attempt_id,
                cleaned,
            )
        persisted["cleaned"] = cleaned

    try:
        continue_native(
            bridge,
            prompt
            + "\n\nReturn one useful Slack update only. Default to 50 words and "
            "3 sentences; exceed that when needed for a complete or safe answer. "
            "Return exactly NO_REPLY only if no useful response is needed.",
            cancellation,
            persist_response,
        )
    except Exception as exc:
        store.fail_attempt(
            attempt_id,
            bridge.bridge_id,
            f"{type(exc).__name__}: {_failure_reason(exc)}",
        )
        raise
    cleaned = persisted.get("cleaned", "")
    if not cleaned:
        raise runtime.NativeContinuationError(
            "The detached continuation returned without persisting its response.",
            code="detached_response_not_persisted",
            binding_id=bridge.bridge_id,
        )
    return attempt_id, cleaned


def _recover_queued_events():
    try:
        while True:
            with state.recovery_lock:
                observed_wake = state.recovery_wake_counter
            for bridge_id in store.queued_bridge_ids():
                while True:
                    items = store.claim_event_batch(bridge_id)
                    if not items:
                        break
                    bridge = store.get(bridge_id)
                    if bridge is None or not bridge.thread_ts:
                        _finish_batch(items, "bridge is no longer active")
                        continue
                    try:
                        _run_recovered_event(bridge, items)
                    except Exception as exc:
                        reason = _failure_reason(exc)
                        log.error(
                            "Recovered bridge reply failed for %s: %s",
                            bridge_id,
                            reason,
                        )
                        break
            with state.recovery_lock:
                if state.recovery_wake_counter != observed_wake:
                    continue
                state.recovery_worker_started = False
                return
    except BaseException:
        with state.recovery_lock:
            state.recovery_worker_started = False
        raise


def _schedule_bridge_drain(bridge_id: str) -> None:
    context = state.dispatch_context.get(bridge_id)
    if context is not None:
        loop, gateway, platform = context

        def schedule() -> None:
            loop.create_task(_drain_bridge(bridge_id, gateway, platform))

        try:
            loop.call_soon_threadsafe(schedule)
            return
        except RuntimeError:
            state.dispatch_context.pop(bridge_id, None)
    with state.recovery_lock:
        state.recovery_wake_counter += 1
        if state.recovery_worker_started:
            return
        state.recovery_worker_started = True
    threading.Thread(
        target=_recover_queued_events,
        name="hermes-bridge-recovery",
        daemon=True,
    ).start()


async def _drain_bridge(bridge_id, gateway, platform):
    lock = state.bridge_locks.setdefault(bridge_id, asyncio.Lock())
    async with lock:
        while True:
            items = store.claim_event_batch(bridge_id)
            if not items:
                return
            bridge = store.get(bridge_id)
            if bridge is None or not bridge.thread_ts:
                _finish_batch(items, "bridge is no longer active")
                continue
            cancellation = threading.Event()
            state.active_cancellations[bridge_id] = cancellation
            attempt_id: str | None = None
            try:
                prompt = _batch_prompt(items)
                if _has_bound_zellij_pane(bridge):
                    attempt_id = await asyncio.to_thread(
                        _submit_zellij_attempt, bridge, items, prompt
                    )
                else:
                    attempt_id, response = await asyncio.to_thread(
                        _submit_detached_attempt,
                        bridge,
                        items,
                        prompt,
                        cancellation,
                    )
                if _has_bound_zellij_pane(bridge):
                    continue
                if response != "NO_REPLY":
                    await asyncio.to_thread(
                        broker_call,
                        {
                            "op": "reply",
                            "bridge_id": bridge.bridge_id,
                            "reply_key": attempt_id,
                            "text": response,
                        },
                    )
            except Exception as exc:
                reason = _failure_reason(exc)
                if attempt_id is None:
                    _finish_batch(items, f"{type(exc).__name__}: {reason}")
                elif store.attempt_state(attempt_id, bridge.bridge_id) in {
                    "prepared",
                    "submitting",
                }:
                    store.fail_attempt(
                        attempt_id,
                        bridge.bridge_id,
                        f"{type(exc).__name__}: {reason}",
                    )
                log.error("Bridge reply failed for %s: %s", bridge.bridge_id, reason)
                return
            finally:
                state.active_cancellations.pop(bridge_id, None)


def _authorized(bridge, user_id: str) -> bool:
    allowed = set(effective_allowed_users())
    return user_id in allowed and (bridge.owner_user_id == "*" or bridge.owner_user_id == user_id)


def _interrupt_active_zellij_attempt(bridge) -> int:
    active = store.active_zellij_attempt(bridge.bridge_id)
    if active is None:
        return 0
    interrupt_zellij(bridge)
    return store.cancel_zellij_attempt(
        str(active["attempt_id"]),
        bridge.bridge_id,
        int(active["binding_generation"]),
    )


async def _post_control_notice(
    bridge,
    *,
    idempotency_key: str,
    text: str,
) -> None:
    try:
        await asyncio.to_thread(
            broker_call,
            {
                "op": "thread_reply",
                "team_id": bridge.team_id,
                "channel_id": bridge.channel_id,
                "thread_ts": bridge.thread_ts,
                "idempotency_key": idempotency_key,
                "text": text,
            },
        )
    except Exception as exc:
        log.error(
            "Tether could not post a durable control notice for %s: %s",
            bridge.bridge_id,
            _failure_reason(exc),
        )


def _gateway_routing_decision(event):
    raw_message = getattr(event, "raw_message", None)
    if not isinstance(raw_message, dict):
        return None
    decision = raw_message.get(ROUTING_DECISION_KEY)
    return decision if isinstance(decision, routing.RoutingDecision) else None


def _routing_skip_reason(decision) -> str:
    aliases = {
        "human_not_authorized": "bridge-user-not-authorized",
        "binding_owner_mismatch": "bridge-user-not-authorized",
        "untrusted_peer_bot": "bridge-bot-not-authorized",
        "peer_bot_did_not_target_self": "bridge-bot-not-targeted",
    }
    return aliases.get(decision.reason, f"tether-route-{decision.reason}")


def _dispatch_hermes_gateway(
    *,
    event,
    gateway,
    decision,
    bridge,
    event_id: str,
    thread_ts: str,
) -> dict[str, str]:
    raw_message = getattr(event, "raw_message", None)
    ingress_claim = (
        raw_message.get("_tether_ingress_claim")
        if isinstance(raw_message, dict)
        else None
    )
    if (
        not isinstance(ingress_claim, tuple)
        or len(ingress_claim) != 3
        or ingress_claim[0] != event_id
    ):
        return {"action": "skip", "reason": "tether-ingress-claim-missing"}
    if raw_message.get("_tether_gateway_dispatch_started"):
        return {"action": "skip", "reason": "tether-duplicate"}
    if decision.bridge_id is not None:
        _suppress_bridge_reaction(event, gateway)
        if (
            not thread_ts
            or bridge is None
            or decision.bridge_id != bridge.bridge_id
        ):
            return {"action": "skip", "reason": "tether-binding-unavailable"}
        if bridge.source_kind not in {"headless_run", "hermes_session"}:
            return {"action": "skip", "reason": "tether-writer-kind-mismatch"}
    if not store.mark_thread_ingress_dispatched(*ingress_claim):
        return {"action": "skip", "reason": "tether-ingress-lease-lost"}
    raw_message["_tether_gateway_dispatch_started"] = True
    dispatch_task = None
    with contextlib.suppress(RuntimeError):
        dispatch_task = asyncio.current_task()
    if dispatch_task is not None:
        raw_message["_tether_gateway_dispatch_task"] = dispatch_task
    if decision.bridge_id is None:
        return {"action": "allow"}
    run_id = str(
        bridge.source.get("run_id")
        or bridge.source.get("session_id")
        or "unknown"
    )
    cwd = str(bridge.source.get("cwd") or "")
    return {
        "action": "rewrite",
        "text": (
            f"[Durable Hermes continuation for run {run_id}; "
            f"original working directory {cwd}. Use the root message and "
            "thread history as the run report. The original process may "
            "have exited; continue as an operator conversation and return "
            "verified results in this thread.]\n\n"
            + event.text
        ),
    }


@dataclass(frozen=True)
class _NativeDispatch:
    event_id: str
    team_id: str
    channel_id: str
    delta: str
    lease_id: str
    fence_epoch: int
    binding_generation: int
    bridge: Any
    gateway: Any
    platform: Any
    raw_message: dict[str, Any] | None


def _release_native_dispatch(
    dispatch: _NativeDispatch,
    error_code: str,
) -> None:
    store.release_thread_ingress(
        dispatch.event_id,
        dispatch.lease_id,
        error_code,
    )


def _start_native_drain(dispatch: _NativeDispatch) -> None:
    loop = asyncio.get_running_loop()
    state.dispatch_context[dispatch.bridge.bridge_id] = (
        loop,
        dispatch.gateway,
        dispatch.platform,
    )
    loop.create_task(
        _drain_bridge(
            dispatch.bridge.bridge_id,
            dispatch.gateway,
            dispatch.platform,
        )
    )


def _dispatch_native_mutation(
    dispatch: _NativeDispatch,
    mutation: dict[str, Any],
) -> dict[str, str]:
    target_ts = str(mutation.get("target_ts") or "")
    try:
        result = store.apply_native_mutation(
            dispatch.event_id,
            dispatch.lease_id,
            dispatch.fence_epoch,
            f"slack:{dispatch.team_id}:{dispatch.channel_id}:{target_ts}",
            dispatch.bridge.bridge_id,
            dispatch.binding_generation,
            str(mutation.get("kind") or ""),
            str(mutation.get("replacement_text") or ""),
            dispatch.delta,
        )
    except BaseException as exc:
        _release_native_dispatch(dispatch, type(exc).__name__)
        raise
    if result == "stale":
        _release_native_dispatch(
            dispatch,
            "binding_generation_changed",
        )
        return {
            "action": "skip",
            "reason": "tether-binding-generation-changed",
        }
    if dispatch.raw_message is not None:
        dispatch.raw_message["_tether_ingress_transferred"] = True
    if result == "interrupt":
        cancellation = state.active_cancellations.get(
            dispatch.bridge.bridge_id
        )
        if cancellation is not None:
            cancellation.set()
        try:
            _interrupt_active_zellij_attempt(dispatch.bridge)
        except Exception as exc:
            active = store.active_zellij_attempt(
                dispatch.bridge.bridge_id
            )
            if active is not None:
                store.mark_attempt_interrupt_unverified(
                    str(active["attempt_id"]),
                    dispatch.bridge.bridge_id,
                    int(active["binding_generation"]),
                )
            log.error(
                "Tether could not verify interruption of the edited Zellij "
                "turn for %s: %s",
                dispatch.bridge.bridge_id,
                _failure_reason(exc),
            )
            asyncio.get_running_loop().create_task(
                _post_control_notice(
                    dispatch.bridge,
                    idempotency_key=(
                        "control:mutation-interrupt:"
                        + hashlib.sha256(
                            dispatch.event_id.encode()
                        ).hexdigest()[:24]
                    ),
                    text=(
                        "_The edit or deletion was recorded, but Tether "
                        "could not verify stopping the active terminal "
                        "command. The attempt is blocked for explicit "
                        "operator resolution; run `tether unresolved` "
                        "before continuing._"
                    ),
                )
            )
            return {
                "action": "skip",
                "reason": "tether-mutation-interrupt-unverified",
            }
    _start_native_drain(dispatch)
    return {
        "action": "skip",
        "reason": f"tether-mutation-{result}",
    }


def _cancellation_message(
    *,
    had_active_process: bool,
    cancelled_zellij: int,
    cancelled_queued: int,
) -> str:
    if had_active_process or cancelled_zellij:
        suffix = (
            f" Discarded {cancelled_queued} queued repl"
            f"{'y' if cancelled_queued == 1 else 'ies'}."
            if cancelled_queued
            else ""
        )
        return "_Cancellation delivered to the active continuation._" + suffix
    if cancelled_queued:
        return (
            f"_Cancelled {cancelled_queued} queued repl"
            f"{'y' if cancelled_queued == 1 else 'ies'} before execution._"
        )
    return "_Nothing is currently running in the bound session._"


def _dispatch_native_cancel(
    dispatch: _NativeDispatch,
) -> dict[str, str]:
    try:
        cancellation = state.active_cancellations.get(
            dispatch.bridge.bridge_id
        )
        cancelled_queued = store.cancel_queued(dispatch.bridge.bridge_id)
        if cancellation is not None:
            cancellation.set()
        try:
            cancelled_zellij = _interrupt_active_zellij_attempt(
                dispatch.bridge
            )
        except runtime.NativeContinuationError:
            message = (
                "_Could not verify delivery of the terminal interrupt; "
                "the active continuation remains blocked. Rebind the thread "
                "after checking the pane._"
            )
        else:
            message = _cancellation_message(
                had_active_process=cancellation is not None,
                cancelled_zellij=cancelled_zellij,
                cancelled_queued=cancelled_queued,
            )
        if not store.complete_thread_ingress(
            dispatch.event_id,
            dispatch.lease_id,
            dispatch.fence_epoch,
        ):
            return {"action": "skip", "reason": "tether-ingress-lease-lost"}
    except BaseException as exc:
        _release_native_dispatch(dispatch, type(exc).__name__)
        raise
    asyncio.get_running_loop().create_task(
        _post_control_notice(
            dispatch.bridge,
            idempotency_key=(
                "control:cancel:"
                + hashlib.sha256(dispatch.event_id.encode()).hexdigest()[:24]
            ),
            text=message,
        )
    )
    return {"action": "skip", "reason": "tether-cancel"}


def _dispatch_native_turn(
    dispatch: _NativeDispatch,
) -> dict[str, str]:
    try:
        inserted = store.transfer_thread_ingress(
            dispatch.event_id,
            dispatch.lease_id,
            dispatch.fence_epoch,
            dispatch.bridge.bridge_id,
            dispatch.binding_generation,
            dispatch.delta,
        )
    except BaseException as exc:
        _release_native_dispatch(dispatch, type(exc).__name__)
        raise
    if not inserted:
        _release_native_dispatch(
            dispatch,
            "binding_generation_changed",
        )
        return {
            "action": "skip",
            "reason": "tether-binding-generation-changed",
        }
    if dispatch.raw_message is not None:
        dispatch.raw_message["_tether_ingress_transferred"] = True
    _start_native_drain(dispatch)
    return {"action": "skip", "reason": "tether-handled"}


def _dispatch_native_gateway(
    *,
    event,
    gateway,
    decision,
    bridge,
    event_id: str,
    team_id: str,
    channel_id: str,
    thread_ts: str,
) -> dict[str, str]:
    if not thread_ts:
        return {"action": "skip", "reason": "tether-native-thread-missing"}
    _suppress_bridge_reaction(event, gateway)
    if bridge is None or decision.bridge_id != bridge.bridge_id:
        return {"action": "skip", "reason": "tether-binding-unavailable"}
    if decision.action is not routing.RouteAction.NATIVE:
        return {
            "action": "skip",
            "reason": "tether-routing-action-unsupported",
        }
    if bridge.source_kind in {"headless_run", "hermes_session"}:
        return {"action": "skip", "reason": "tether-writer-kind-mismatch"}
    if decision.binding_generation is None:
        return {
            "action": "skip",
            "reason": "tether-binding-generation-missing",
        }
    delta = _reply_delta(event.text)
    raw_message = getattr(event, "raw_message", None)
    claim = store.claim_thread_ingress(
        event_id,
        team_id,
        channel_id,
        thread_ts,
        route_action="native",
        writer_id=str(decision.writer_id or ""),
        bridge_id=bridge.bridge_id,
        binding_generation=decision.binding_generation,
        payload={
            "text": delta,
            "user": str(
                raw_message.get("user")
                if isinstance(raw_message, Mapping)
                else ""
            ),
            "message_ts": decision.message_identity.message_ts,
            "event_thread_ts": thread_ts,
        },
    )
    if claim["status"] != "claimed":
        return {"action": "skip", "reason": "tether-duplicate"}
    dispatch = _NativeDispatch(
        event_id=event_id,
        team_id=team_id,
        channel_id=channel_id,
        delta=delta,
        lease_id=str(claim["lease_id"]),
        fence_epoch=int(claim["fence_epoch"]),
        binding_generation=decision.binding_generation,
        bridge=bridge,
        gateway=gateway,
        platform=event.source.platform,
        raw_message=raw_message if isinstance(raw_message, dict) else None,
    )
    mutation = (
        raw_message.get("_tether_mutation")
        if isinstance(raw_message, dict)
        else None
    )
    if isinstance(mutation, dict):
        return _dispatch_native_mutation(dispatch, mutation)
    if delta.lower().strip(" .!?") in {
        "cancel",
        "stop",
        "nvm",
        "never mind",
        "nevermind",
    }:
        return _dispatch_native_cancel(dispatch)
    return _dispatch_native_turn(dispatch)


def _pre_gateway_dispatch_impl(*, event, gateway, **_kwargs):
    source = event.source
    if getattr(source.platform, "value", "") != "slack":
        return None
    decision = _gateway_routing_decision(event)
    if decision is None:
        # The async Slack ingress adapter is the only component with enough
        # information to resolve workspace, conversation, mentions, and thread
        # ownership. Never recreate a weaker decision from gateway fields.
        _suppress_bridge_reaction(event, gateway)
        return {"action": "skip", "reason": "tether-routing-decision-missing"}
    if decision.action is routing.RouteAction.SILENT:
        _suppress_bridge_reaction(event, gateway)
        return {"action": "skip", "reason": _routing_skip_reason(decision)}
    team_id = decision.message_identity.team_id
    channel_id = decision.message_identity.channel_id
    thread_ts = str(source.thread_id or "")
    bridge = (
        store.find(team_id, channel_id, thread_ts)
        if thread_ts
        else None
    )
    if bridge is not None and (
        bridge.team_id != team_id
        or bridge.channel_id != channel_id
        or str(bridge.thread_ts) != thread_ts
    ):
        _suppress_bridge_reaction(event, gateway)
        return {
            "action": "skip",
            "reason": "tether-binding-identity-mismatch",
        }
    event_id = _composite_event_id(decision)
    if decision.action is routing.RouteAction.HERMES:
        return _dispatch_hermes_gateway(
            event=event,
            gateway=gateway,
            decision=decision,
            bridge=bridge,
            event_id=event_id,
            thread_ts=thread_ts,
        )
    return _dispatch_native_gateway(
        event=event,
        gateway=gateway,
        decision=decision,
        bridge=bridge,
        event_id=event_id,
        team_id=team_id,
        channel_id=channel_id,
        thread_ts=thread_ts,
    )


def _pre_gateway_dispatch(*, event, gateway, **kwargs):
    if not _is_slack_gateway_event(event):
        return None
    if not state.ready:
        try:
            _suppress_bridge_reaction(event, gateway)
        except Exception as exc:
            log.error(
                "Tether could not suppress a fail-closed reaction (%s)",
                type(exc).__name__,
            )
        return {"action": "skip", "reason": "tether-not-ready"}
    try:
        return _pre_gateway_dispatch_impl(
            event=event,
            gateway=gateway,
            **kwargs,
        )
    except Exception as exc:
        log.error(
            "Tether gateway routing failed closed (%s)",
            type(exc).__name__,
        )
        try:
            _suppress_bridge_reaction(event, gateway)
        except Exception as reaction_exc:
            log.error(
                "Tether could not suppress its processing reaction (%s)",
                type(reaction_exc).__name__,
            )
        return {"action": "skip", "reason": "tether-routing-internal-error"}


def register(ctx) -> None:
    global store
    state.ready = False
    hermes_compat.register_authoritative_gateway_hook(
        ctx,
        _pre_gateway_dispatch,
    )
    token = os.getenv("SLACK_BOT_TOKEN", "")
    if not token:
        log.error("Tether disabled: Hermes has no Slack bot credential")
        return
    try:
        version = _validate_hermes_compatibility()
    except Exception as exc:
        log.critical(
            "Tether blocked Slack dispatch because Hermes compatibility "
            "could not be verified (%s)",
            type(exc).__name__,
        )
        return
    log.info("Tether verified Hermes Slack compatibility for %s", version)
    load_config()
    if state.broker is None:
        active_store, active_lock_fd = runtime.open_locked_store()
        try:
            state.broker = start_broker(
                token,
                health_provider=_health_status,
                attempt_closed=_schedule_bridge_drain,
                store=active_store,
                lock_fd=active_lock_fd,
            )
        except BaseException:
            state.store = None
            state.store_lock_fd = -1
            store = None
            raise
        state.store = active_store
        state.store_lock_fd = -1
        store = active_store
    elif state.store is None:
        state.store = state.broker.broker.store
        store = state.store
    store.requeue_processing()
    queued = store.queued_bridge_ids()
    if queued:
        _schedule_bridge_drain(queued[0])
    _install_slack_bridge_prefilter()
    state.ready = True
