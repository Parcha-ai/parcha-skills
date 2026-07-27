from __future__ import annotations

import asyncio
import datetime
import functools
import importlib
import importlib.util
import json
import logging
import os
import sys
import threading
import time
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
effective_allowed_users = runtime.effective_allowed_users
load_config = runtime.load_config
start_broker = runtime.start_broker
validate_reply_text = runtime.validate_reply_text


log = logging.getLogger(__name__)


@dataclass
class PluginState:
    store: Any = field(default_factory=Store)
    broker: Any = None
    bridge_locks: dict[str, asyncio.Lock] = field(default_factory=dict)
    active_cancellations: dict[str, threading.Event] = field(default_factory=dict)
    recovery_worker_started: bool = False
    reply_poller: asyncio.Task | None = None
    poll_cursor: int = 0
    joined_channels: set[tuple[str, str]] = field(default_factory=set)
    slack_transport_connected: bool | None = None
    last_inbound_at: float | None = None
    last_poll_at: float | None = None
    last_poll_error_at: float | None = None


state = PluginState()
store = state.store


def _bounded_env_int(name: str, default: int, minimum: int, maximum: int) -> int:
    try:
        value = int(os.getenv(name, str(default)))
    except ValueError:
        return default
    return max(minimum, min(value, maximum))


def _reply_poll_interval() -> int:
    return _bounded_env_int("TETHER_REPLY_POLL_SECONDS", 30, 10, 300)


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


def _bridge_for_slack_event(adapter, event):
    thread_ts = str(event.get("thread_ts") or "")
    channel_id = str(event.get("channel") or event.get("channel_id") or "")
    if not thread_ts or not channel_id:
        return None
    team_id = str(
        event.get("team") or event.get("team_id")
        or getattr(adapter, "_channel_team", {}).get(channel_id, "") or ""
    )
    return store.find(team_id, channel_id, thread_ts)


def _mark_bridge_thread_before_slack_gate(adapter, event) -> bool:
    bridge = _bridge_for_slack_event(adapter, event)
    thread_ts = str(event.get("thread_ts") or "")
    channel_id = str(event.get("channel") or event.get("channel_id") or "")
    team_id = str(
        event.get("team") or event.get("team_id")
        or getattr(adapter, "_channel_team", {}).get(channel_id, "") or ""
    )
    if bridge is None and not store.participates(team_id, channel_id, thread_ts):
        return False
    adapter._bot_message_ts.add(thread_ts)
    return True


async def _discover_existing_thread_participation(adapter, event) -> bool:
    """Recover participation for threads created before persistence existed."""
    thread_ts = str(event.get("thread_ts") or "")
    channel_id = str(event.get("channel") or event.get("channel_id") or "")
    if not thread_ts or not channel_id:
        return False
    team_id = str(
        event.get("team") or event.get("team_id")
        or getattr(adapter, "_channel_team", {}).get(channel_id, "") or ""
    )
    key = (team_id, channel_id, thread_ts)
    misses = getattr(adapter, "_tether_participation_misses", None)
    if misses is None:
        misses = adapter._tether_participation_misses = set()
    if key in misses:
        return False
    bot_user_id = (
        getattr(adapter, "_team_bot_user_ids", {}).get(team_id)
        or getattr(adapter, "_bot_user_id", None)
    )
    if not bot_user_id:
        return False
    try:
        result = await adapter._get_client(channel_id).conversations_replies(
            channel=channel_id, ts=thread_ts, limit=200,
        )
    except Exception:
        return False
    participated = any(
        isinstance(message, dict) and str(message.get("user") or "") == str(bot_user_id)
        for message in result.get("messages", [])
    )
    if participated:
        store.mark_participation(team_id, channel_id, thread_ts)
        adapter._bot_message_ts.add(thread_ts)
        return True
    misses.add(key)
    return False


def _is_bot_message(event: dict[str, Any]) -> bool:
    return bool(event.get("bot_id")) or event.get("subtype") == "bot_message"


def _allowed_peer_bot_users() -> set[str]:
    return {
        value.strip()
        for value in os.getenv("TETHER_ALLOWED_BOT_USERS", "").split(",")
        if value.strip()
    }


def _is_silence_control_output(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    return value.strip().upper() in {"NO_REPLY", "NO REPLY", "[SILENT]", "SILENT"}


def _allows_bot_message(adapter, event: dict[str, Any], team_id: str = "") -> bool:
    event_identities = {
        value for value in (
            str(event.get("user") or ""),
            str(event.get("bot_id") or ""),
        ) if value
    }
    if not event_identities.intersection(_allowed_peer_bot_users()):
        return False
    mode = str(
        getattr(getattr(adapter, "config", None), "extra", {}).get("allow_bots")
        or os.getenv("SLACK_ALLOW_BOTS", "none")
    ).lower().strip()
    if mode == "all":
        return True
    if mode != "mentions":
        return False
    bot_user_id = (
        getattr(adapter, "_team_bot_user_ids", {}).get(team_id)
        or getattr(adapter, "_bot_user_id", None)
    )
    return bool(bot_user_id and f"<@{bot_user_id}>" in str(event.get("text") or ""))


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


def _install_slack_bridge_prefilter():
    SlackAdapter = _resolve_slack_adapter()
    if not hasattr(SlackAdapter, "_handle_slack_message"):
        raise RuntimeError("Hermes Slack adapter is incompatible with Tether")
    if getattr(SlackAdapter, "_tether_prefilter", False):
        return
    original = SlackAdapter._handle_slack_message
    original_connect = SlackAdapter.connect
    original_send = getattr(SlackAdapter, "send", None)
    original_restart = getattr(SlackAdapter, "_restart_socket_mode", None)

    @functools.wraps(original)
    async def bridged_handle(self, event, *args, **kwargs):
        if not event.get("_tether_polled"):
            state.last_inbound_at = time.monotonic()
            state.slack_transport_connected = True
        _ensure_reply_poller(self)
        try:
            bridge = _bridge_for_slack_event(self, event)
            channel_id = str(event.get("channel") or event.get("channel_id") or "")
            team_id = str(
                event.get("team") or event.get("team_id")
                or getattr(self, "_channel_team", {}).get(channel_id, "") or ""
            )
            if _is_bot_message(event) and not _allows_bot_message(self, event, team_id):
                return None
            marked = _mark_bridge_thread_before_slack_gate(self, event)
            if not marked and event.get("thread_ts"):
                marked = await _discover_existing_thread_participation(self, event)
            if marked and bridge is None and not event.get("_tether_polled"):
                event_id = str(event.get("ts") or "")
                thread_ts = str(event.get("thread_ts") or "")
                if event_id and not store.mark_thread_ingress(
                    event_id, team_id, channel_id, thread_ts,
                ):
                    return None
        except Exception:
            log.exception("Could not evaluate a Slack bridge thread before the mention gate")
        return await original(self, event, *args, **kwargs)

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
            content = kwargs.get("content")
            if content is None and len(args) >= 2:
                content = args[1]
            if _is_silence_control_output(content):
                log.info("Tether suppressed an internal silence control token at Slack egress")
                return {"ok": True, "suppressed": True}
            result = await original_send(self, *args, **kwargs)
            succeeded = (
                bool(result.get("ok", result.get("success", True)))
                if isinstance(result, dict)
                else bool(getattr(result, "success", True))
            )
            if not succeeded:
                return result
            channel_id = str(kwargs.get("chat_id") or kwargs.get("channel") or (args[0] if args else ""))
            metadata = kwargs.get("metadata") or (args[3] if len(args) >= 4 else {}) or {}
            reply_to = kwargs.get("reply_to") or (args[2] if len(args) >= 3 else "")
            thread_ts = str(
                metadata.get("thread_id") or metadata.get("thread_ts") or reply_to or ""
            )
            if channel_id and thread_ts:
                team_id = str(getattr(self, "_channel_team", {}).get(channel_id, "") or "")
                store.mark_participation(team_id, channel_id, thread_ts)
            return result

        SlackAdapter.send = bridged_send
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
    batch_size = _bounded_env_int("TETHER_REPLY_POLL_BATCH", 10, 1, 25)
    bridges = store.recent_active_bridges(hours=hours, limit=100)
    bridge_keys = {(bridge.team_id, bridge.channel_id, bridge.thread_ts) for bridge in bridges}
    participating = [
        item for item in store.recent_participating_threads(hours=max(hours, 168), limit=500)
        if item[:3] not in bridge_keys
    ]
    targets = [
        (bridge, bridge.team_id, bridge.channel_id, str(bridge.thread_ts), None)
        for bridge in bridges
    ] + [(None, *item) for item in participating]
    if not targets:
        return 0
    start = state.poll_cursor % len(targets)
    batch = (targets + targets)[start:start + min(batch_size, len(targets))]
    state.poll_cursor = (start + len(batch)) % len(targets)
    oldest = f"{time.time() - hours * 3600:.6f}"
    recovered = 0
    succeeded = 0
    allowed_users = set(effective_allowed_users())
    for bridge, team_id, channel_id, thread_ts, participation_since in batch:
        try:
            client = adapter._get_client(channel_id)
            channel_key = (team_id, channel_id)
            if channel_id.startswith("C") and channel_key not in state.joined_channels:
                try:
                    await client.conversations_history(channel=channel_id, limit=1)
                except Exception as exc:
                    if "not_in_channel" not in str(exc):
                        raise
                    await client.conversations_join(channel=channel_id)
                state.joined_channels.add(channel_key)
            result = await client.conversations_replies(
                channel=channel_id,
                ts=thread_ts,
                oldest=(
                    f"{max(float(oldest), participation_since):.6f}"
                    if participation_since is not None else oldest
                ),
                inclusive=False,
                limit=100,
            )
            succeeded += 1
        except Exception as exc:
            target = bridge.bridge_id if bridge is not None else f"{channel_id}:{thread_ts}"
            log.warning("Could not poll Tether thread %s: %s", target, type(exc).__name__)
            continue
        for message in result.get("messages", []):
            if not isinstance(message, dict):
                continue
            event_id = str(message.get("ts") or "")
            user_id = str(message.get("user") or "")
            text = str(message.get("text") or "")
            bot_allowed = _allows_bot_message(adapter, message, team_id)
            if (
                not event_id
                or event_id == thread_ts
                or not text.strip()
                or (_is_bot_message(message) and not bot_allowed)
                or (
                    not _is_bot_message(message)
                    and (
                        user_id not in allowed_users
                        or (bridge is not None and not _authorized(bridge, user_id))
                    )
                )
                or store.has_ingress(event_id)
            ):
                continue
            if bridge is None:
                claimed = store.mark_thread_ingress(
                    event_id, team_id, channel_id, thread_ts,
                )
                if not claimed:
                    continue
            event = dict(message)
            event.update({
                "channel": channel_id,
                "team": team_id,
                "thread_ts": thread_ts,
                "channel_type": "im" if channel_id.startswith("D") else "channel",
                "_tether_polled": True,
            })
            await adapter._handle_slack_message(event)
            recovered += 1
    if batch and not succeeded:
        raise RuntimeError("every Slack thread poll failed")
    return recovered


async def _reply_poll_loop(adapter) -> None:
    while True:
        try:
            recovered = await _poll_recent_replies(adapter)
            state.last_poll_at = time.monotonic()
            if recovered:
                log.warning("Tether recovered %d Slack thread repl%s by polling", recovered, "y" if recovered == 1 else "ies")
        except asyncio.CancelledError:
            raise
        except Exception:
            state.last_poll_error_at = time.monotonic()
            log.exception("Tether Slack reply poll failed")
        await asyncio.sleep(_reply_poll_interval())


def _ensure_reply_poller(adapter) -> None:
    if state.reply_poller is None or state.reply_poller.done():
        state.reply_poller = asyncio.get_running_loop().create_task(_reply_poll_loop(adapter))


def _reply_delta(text: str) -> str:
    marker = "[End of thread context]"
    if marker in text:
        text = text.rsplit(marker, 1)[1]
    return text.strip()


def _suppress_bridge_reaction(event, gateway):
    adapter = gateway.adapters[event.source.platform]
    event_id = str(event.message_id or event.source.message_id or "")
    reacting = getattr(adapter, "_reacting_message_ids", None)
    if reacting is not None:
        reacting.discard(event_id)
    if event_id and hasattr(adapter, "_remove_reaction"):
        asyncio.get_running_loop().create_task(
            adapter._remove_reaction(str(event.source.chat_id), event_id, "eyes")
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
        deliver_zellij(bridge, prompt)
        return ""
    return continue_native(
        bridge,
        prompt + "\n\nReturn one useful Slack update only, within 50 words and 3 sentences. "
        "Return exactly NO_REPLY if the thread is already answered.",
    )


def _has_bound_zellij_pane(bridge) -> bool:
    source = bridge.source
    return bool(
        (source.get("session_name") or source.get("zellij_session"))
        and (source.get("pane_id") or source.get("zellij_pane_id"))
        and source.get("pane_command_hash")
    )


def _recover_queued_events():
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
                response = _run_recovered_event(bridge, items)
                if response and response.strip() != "NO_REPLY":
                    response = validate_reply_text(response)
                    broker_call({"op": "reply", "bridge_id": bridge_id, "text": response})
                _finish_batch(items)
            except Exception as exc:
                reason = _failure_reason(exc)
                _finish_batch(items, f"{type(exc).__name__}: {reason}")
                log.error("Recovered bridge reply failed for %s: %s", bridge_id, reason)
                try:
                    broker_call({"op": "reply", "bridge_id": bridge_id, "text": f"The recovered continuation failed because {reason}."})
                except Exception:
                    log.error("Could not report recovered bridge failure for %s", bridge_id)


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
            adapter = gateway.adapters[platform]
            cancellation = threading.Event()
            state.active_cancellations[bridge_id] = cancellation
            try:
                prompt = _batch_prompt(items)
                if _has_bound_zellij_pane(bridge):
                    await asyncio.to_thread(deliver_zellij, bridge, prompt)
                    response = ""
                else:
                    response = await asyncio.to_thread(
                        continue_native,
                        bridge,
                        prompt + "\n\nReturn one useful Slack update only, within 50 words and 3 sentences. "
                        "Return exactly NO_REPLY if the thread is already answered.",
                        cancellation,
                    )
                if response and response.strip() != "NO_REPLY":
                    response = validate_reply_text(response)
                    await adapter.send(bridge.channel_id, response, metadata={"thread_id": bridge.thread_ts})
                _finish_batch(items)
            except Exception as exc:
                reason = _failure_reason(exc)
                _finish_batch(items, f"{type(exc).__name__}: {reason}")
                log.error("Bridge reply failed for %s: %s", bridge.bridge_id, reason)
                try:
                    await adapter.send(
                        bridge.channel_id,
                        f"I received this reply, but {reason}. Nothing was sent to a different session.",
                        metadata={"thread_id": bridge.thread_ts},
                    )
                except Exception:
                    log.exception("Could not report bridge failure in Slack for %s", bridge.bridge_id)
            finally:
                state.active_cancellations.pop(bridge_id, None)


def _authorized(bridge, user_id: str) -> bool:
    allowed = set(effective_allowed_users())
    return user_id in allowed and (bridge.owner_user_id == "*" or bridge.owner_user_id == user_id)


def _pre_gateway_dispatch(*, event, gateway, **_kwargs):
    source = event.source
    if getattr(source.platform, "value", "") != "slack" or not source.thread_id:
        return None
    bridge = store.find(str(source.guild_id or ""), str(source.chat_id), str(source.thread_id))
    if bridge is None:
        return None
    # Once a thread resolves to Tether, the exact bound session is its sole
    # writer for admitted human and trusted peer-agent turns. Hermes must not
    # leave its generic processing/success reaction behind.
    is_bot = bool(getattr(source, "is_bot", False))
    user_id = str(source.user_id or "")
    if is_bot and user_id not in _allowed_peer_bot_users():
        return {"action": "skip", "reason": "bridge-bot-not-authorized"}
    # Tether must not leave its generic
    # processing/success reaction behind. In particular, an authorization
    # rejection is not a successful handoff.
    _suppress_bridge_reaction(event, gateway)
    if not is_bot and not _authorized(bridge, user_id):
        return {"action": "skip", "reason": "bridge-user-not-authorized"}
    event_id = str(event.message_id or source.message_id or "")
    if not store.mark_ingress(event_id, bridge.bridge_id):
        return {"action": "skip", "reason": "tether-duplicate"}
    if bridge.source_kind in {"headless_run", "hermes_session"}:
        run_id = str(bridge.source.get("run_id") or bridge.source.get("session_id") or "unknown")
        cwd = str(bridge.source.get("cwd") or "")
        return {
            "action": "rewrite",
            "text": (
                f"[Durable Hermes continuation for run {run_id}; original working directory {cwd}. "
                "Use the root message and thread history as the run report. The original process may have exited; "
                "continue as an operator conversation and return verified results in this thread.]\n\n" + event.text
            ),
        }
    delta = _reply_delta(event.text)
    normalized = delta.lower().strip(" .!?")
    if normalized in {"cancel", "stop", "nvm", "never mind", "nevermind"}:
        cancellation = state.active_cancellations.get(bridge.bridge_id)
        cancelled_queued = store.cancel_queued(bridge.bridge_id)
        if cancellation is not None:
            cancellation.set()
            suffix = f" and discarded {cancelled_queued} queued replies" if cancelled_queued else ""
            message = f"_Cancellation requested; stopping the active continuation cleanly{suffix}._"
        elif cancelled_queued:
            message = f"_Cancelled {cancelled_queued} queued replies before execution._"
        else:
            message = "_Nothing is currently running in the bound session._"
        if store.claim_event(event_id, bridge.bridge_id):
            store.finish_event(event_id)
        asyncio.get_running_loop().create_task(
            gateway.adapters[source.platform].send(bridge.channel_id, message, metadata={"thread_id": bridge.thread_ts})
        )
        return {"action": "skip", "reason": "tether-cancel"}
    inserted = store.enqueue_event(event_id, bridge.bridge_id, delta)
    if inserted:
        asyncio.get_running_loop().create_task(_drain_bridge(bridge.bridge_id, gateway, source.platform))
    return {"action": "skip", "reason": "tether-handled"}


def register(ctx) -> None:
    token = os.getenv("SLACK_BOT_TOKEN", "")
    if not token:
        log.error("Tether disabled: Hermes has no Slack bot credential")
        return
    load_config()
    if state.broker is None:
        state.broker = start_broker(token, health_provider=_health_status)
    store.requeue_processing()
    if not state.recovery_worker_started and store.queued_bridge_ids():
        threading.Thread(target=_recover_queued_events, name="hermes-bridge-recovery", daemon=True).start()
        state.recovery_worker_started = True
    _install_slack_bridge_prefilter()
    ctx.register_hook("pre_gateway_dispatch", _pre_gateway_dispatch)
