"""Active slice: bound Slack thread -> schema-18 turn -> exact-turn driver -> reply.

This is the one path that makes an agent a coworker instead of a notifier.
Everything durable lives in the domain database (single writer: this plugin);
every terminal outcome is a fenced driver receipt; the reply leaves through
whatever egress callable the host hands us (Hermes ``send_message``), never a
Slack SDK of our own.

The scheduler is a plain daemon thread. ``reap`` blocks on the harness process
for up to ``native_timeout_seconds``, so it must never run on the gateway's
event loop.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import threading
import time
import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

from .broker import BrokerRefused


logger = logging.getLogger("hermes_plugins.tether_next.active")

Egress = Callable[[str, str, str], Any]  # (channel_id, thread_ts, text)

# The harness inherits only what a login shell would give it. The gateway's own
# environment carries the model-proxy variables (ANTHROPIC_*/OPENAI_*), Slack
# tokens and 1Password material; passing those through made `claude --resume`
# route to the proxy and exit 1 instead of using the operator's own login.
SAFE_CHILD_ENV = frozenset({
    "HOME", "USER", "LOGNAME", "PATH", "SHELL", "LANG", "LC_ALL", "LC_CTYPE",
    "TERM", "TMPDIR", "XDG_CONFIG_HOME", "XDG_CACHE_HOME", "XDG_DATA_HOME",
    "CODEX_HOME", "CLAUDE_HOME", "SSL_CERT_FILE", "SSL_CERT_DIR",
})


def child_env(
    source: dict[str, str] | None = None,
    passthrough: tuple[str, ...] = (),
) -> dict[str, str]:
    """Login-shell env plus an explicit, per-instance passthrough.

    Isolated gateway users have no claude.ai login; they reach the model through
    the machine proxy, so the operator lists ANTHROPIC_BASE_URL/ANTHROPIC_API_KEY
    in ``harness_env``. The Claude CLI appends ``/v1`` itself, so a proxy URL
    configured with that suffix is normalised or the CLI reports every model as
    missing.
    """
    base = os.environ if source is None else source
    env = {key: value for key, value in base.items() if key in SAFE_CHILD_ENV}
    for key in passthrough:
        if key in base:
            value = base[key]
            if key.endswith("_BASE_URL"):
                value = value.rstrip("/")
                if value.endswith("/v1"):
                    value = value[:-3]
            env[key] = value
    return env


@dataclass(frozen=True)
class ActiveSettings:
    enabled: bool = False
    claude_binary: str = "claude"
    claude_resume_args: tuple[str, ...] = ()
    codex_binary: str = "codex"
    codex_resume_args: tuple[str, ...] = ()
    native_timeout_seconds: int = 1800
    max_reply_sentences: int = 3
    poll_interval_seconds: float = 2.0
    persona_id: str = "primary"
    policy_generation: int = 1
    harness_env: tuple[str, ...] = ()
    extra: dict[str, Any] = field(default_factory=dict)


def load_active_settings(path: Path) -> ActiveSettings:
    try:
        raw = tomllib.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, tomllib.TOMLDecodeError):
        raw = {}

    def strings(key: str) -> tuple[str, ...]:
        value = raw.get(key) or []
        return tuple(str(item) for item in value if isinstance(item, str)) if isinstance(value, list) else ()

    def integer(key: str, default: int) -> int:
        value = raw.get(key, default)
        return int(value) if isinstance(value, int) and not isinstance(value, bool) else default

    return ActiveSettings(
        enabled=bool(raw.get("active", False)),
        claude_binary=str(raw.get("claude_binary") or "claude"),
        claude_resume_args=strings("claude_resume_args"),
        codex_binary=str(raw.get("codex_binary") or "codex"),
        codex_resume_args=strings("codex_resume_args"),
        native_timeout_seconds=integer("native_timeout_seconds", 1800),
        max_reply_sentences=integer("max_reply_sentences", 3),
        persona_id=str(raw.get("persona_id") or "primary"),
        policy_generation=integer("policy_generation", 1),
        harness_env=strings("harness_env"),
        extra={"default_channel": str(raw.get("default_channel") or "")},
    )


def endpoint_key_for(source_kind: str, session_id: str) -> str:
    return f"detached_native:{source_kind}:{session_id}"


def compose_prompt(context: dict[str, Any], settings: ActiveSettings) -> str:
    """The turn as the harness sees it: who said what, and how to answer."""
    lines = [
        "You are continuing this coding session from a Slack thread. New messages:",
        "",
    ]
    for turn in context["turns"]:
        payload: dict[str, Any] = {}
        if turn.get("payload_inline"):
            try:
                payload = json.loads(turn["payload_inline"])
            except ValueError:
                payload = {"text": str(turn["payload_inline"])}
        who = payload.get("user") or "someone"
        lines.append(f"<@{who}>: {payload.get('text', '').strip()}")
    lines += [
        "",
        "Answer the thread as a colleague would: at most "
        f"{settings.max_reply_sentences} short sentences, concrete, no meta-narration, "
        "no restating the question. Mention people as <@USERID>. If nothing needs "
        "saying, reply with exactly NO_REPLY.",
    ]
    return "\n".join(lines)


def harness_command(context: dict[str, Any], settings: ActiveSettings, prompt: str) -> list[str]:
    source = context["source"]
    session_id = str(source.get("session_id") or "")
    if not session_id:
        raise ValueError("endpoint source has no session_id")
    if context["source_kind"] == "codex_session":
        binary = shutil.which(settings.codex_binary) or settings.codex_binary
        return [binary, "exec", "resume", *settings.codex_resume_args, session_id, prompt]
    binary = shutil.which(settings.claude_binary) or settings.claude_binary
    return [
        binary, "--print", "--resume", session_id, "--output-format", "text",
        *settings.claude_resume_args, prompt,
    ]


class ActiveSlice:
    def __init__(
        self,
        *,
        runtime: Any,
        driver: Any,
        settings: ActiveSettings,
        egress: Egress,
        descriptor: Any,
        command_factory: Callable[[dict[str, Any], ActiveSettings, str], list[str]] = harness_command,
        slack: Any = None,
    ):
        self.runtime = runtime
        self.driver = driver
        self.settings = settings
        self.egress = egress
        self.descriptor = descriptor
        self.command_factory = command_factory
        self.slack: Any = slack
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    # -- binding management (CLI) ------------------------------------------------

    def bind(
        self,
        *,
        source_kind: str,
        session_id: str,
        cwd: str,
        team_id: str,
        channel_id: str,
        thread_ts: str,
        owner_user_id: str,
    ) -> dict[str, Any]:
        source = {"session_id": session_id, "cwd": cwd}
        endpoint = self.runtime.register_endpoint(
            endpoint_key=endpoint_key_for(source_kind, session_id),
            endpoint_kind="detached_native",
            source_kind=source_kind,
            source_json=json.dumps(source, sort_keys=True, separators=(",", ":")),
            ref_version=1,
            descriptor=self.descriptor,
        )
        return self.runtime.bind_thread(
            endpoint_id=endpoint["endpoint_id"],
            team_id=team_id,
            channel_id=channel_id,
            thread_ts=thread_ts,
            owner_user_id=owner_user_id,
            idempotency_key=f"bind:{team_id}:{channel_id}:{thread_ts}",
        )

    # -- ingress -------------------------------------------------------------------

    def claim(self, fields: dict[str, Any], text: str) -> dict[str, Any] | None:
        """Admit one authorized Slack message on a bound thread. None = not ours."""
        binding = self.runtime.find_active_binding(
            team_id=str(fields.get("workspace") or ""),
            channel_id=str(fields.get("channel") or ""),
            thread_ts=str(fields.get("thread") or ""),
        )
        if binding is None:
            return None
        message_id = str(fields.get("message_id") or "")
        event_key = f"slack:{fields.get('workspace')}:{fields.get('channel')}:{message_id}"
        payload = json.dumps(
            {"user": fields.get("actor"), "text": text, "ts": message_id},
            sort_keys=True,
        )
        try:
            self.runtime.admit_turn(
                binding_id=binding["binding_id"],
                event_key=event_key,
                ordered_at=message_id or str(time.time()),
                payload_inline=payload,
            )
        except Exception as exc:  # duplicate delivery is the common case
            code = getattr(exc, "code", type(exc).__name__)
            if code not in {"turn_exists", "event_exists", "duplicate"}:
                logger.warning("tether: admit_turn refused %s (%s)", event_key, code)
                return None
        return {"binding_id": binding["binding_id"], "event_key": event_key}

    # -- scheduling -----------------------------------------------------------------

    def run_once(self) -> int:
        """Drive every endpoint with ready work through one attempt. Returns count."""
        driven = 0
        for endpoint_id in self.runtime.endpoints_with_ready_turns():
            attempt = self.runtime.schedule_next(endpoint_id)
            if attempt is None:
                continue
            driven += 1
            self._drive(attempt)
        return driven

    def _drive(self, attempt: dict[str, Any]) -> None:
        context = self.runtime.attempt_context(attempt["attempt_id"])
        prompt = compose_prompt(context, self.settings)
        try:
            command = self.command_factory(context, self.settings, prompt)
            cwd = Path(str(context["source"].get("cwd") or os.getcwd()))
            if not cwd.is_dir():
                cwd = Path.home()
            launched = self.driver.launch(
                attempt, command=command, cwd=cwd,
                env=child_env(passthrough=self.settings.harness_env),
            )
            result = self.driver.reap(
                attempt, launched, timeout_seconds=self.settings.native_timeout_seconds
            )
        except Exception as exc:
            logger.error(
                "tether: attempt %s did not reach a receipt (%s)",
                attempt["attempt_id"], type(exc).__name__, exc_info=True,
            )
            return
        if result.get("state") != "completed_with_response":
            return
        final = self.runtime.attempt_context(attempt["attempt_id"])
        text = self._read_response(final.get("response_ref"))
        if not text.strip():
            return
        try:
            self.egress(final["channel_id"], final["thread_ts"], text.strip())
        except Exception:
            logger.error("tether: egress failed for %s", attempt["attempt_id"], exc_info=True)

    def _read_response(self, response_ref: str | None) -> str:
        if not response_ref:
            return ""
        ref = str(response_ref)
        if ref.startswith("blob:sha256:"):
            # The driver stores content-addressed blobs; the ref is the digest.
            candidate = Path(self.driver.blob_root) / ref.rsplit(":", 1)[1]
        else:
            candidate = Path(ref)
            if not candidate.is_absolute():
                candidate = Path(self.driver.blob_root) / candidate.name
        try:
            return candidate.read_text(encoding="utf-8", errors="replace")
        except OSError:
            return ""

    # -- broker ops (the CLI contract) ---------------------------------------------

    def _identity(self) -> dict[str, Any]:
        slack = getattr(self, "slack", None)
        if slack is None:
            raise BrokerRefused("slack_unconfigured", "no Slack bot token in the gateway")
        try:
            return slack.identity()
        except Exception as exc:
            raise BrokerRefused("slack_unreachable", str(exc), retryable=True) from exc

    def _team(self, request: dict[str, Any]) -> str:
        return str(request.get("team_id") or self.descriptor.workspace_id)

    def _owner(self, request: dict[str, Any]) -> str:
        owner = str(request.get("owner_user_id") or "")
        if owner:
            return owner
        owners = tuple(self.descriptor.canonical_owner_ids) if hasattr(self.descriptor, "canonical_owner_ids") else tuple(self.descriptor.authorized_owner_ids)
        return owners[0] if owners else ""

    def _source(self, request: dict[str, Any]) -> tuple[str, str, str]:
        kind = str(request.get("source_kind") or "")
        source = request.get("source") or {}
        if kind not in {"claude_session", "codex_session"} or not isinstance(source, dict):
            raise BrokerRefused(
                "source_unsupported",
                "Tether v2 binds Claude Code and Codex sessions; run from inside one, or pass "
                "--claude-session-id/--codex-session-id",
            )
        session_id = str(source.get("session_id") or "")
        if not session_id:
            raise BrokerRefused("source_unsupported", "session_id missing")
        return kind, session_id, str(source.get("cwd") or os.getcwd())

    def handle(self, request: dict[str, Any]) -> dict[str, Any]:
        op = str(request.get("op") or "")
        handler = getattr(self, f"op_{op}", None)
        if handler is None:
            raise BrokerRefused("unsupported_op", f"Tether v2 does not implement op={op!r}")
        try:
            return handler(request)
        except BrokerRefused:
            raise
        except Exception as exc:
            code = getattr(exc, "code", None)
            if isinstance(code, str) and code:
                # Domain refusals carry their own code; hand it to the caller.
                raise BrokerRefused(code, str(exc)) from exc
            raise

    def op_status(self, request: dict[str, Any]) -> dict[str, Any]:
        counts = self.runtime.counts()
        connected: bool | None = None
        membership = "unconfigured"
        slack = getattr(self, "slack", None)
        if slack is not None and slack.configured:
            try:
                slack.identity()
                connected = True
            except Exception:
                connected = False
            if self.settings.extra.get("default_channel"):
                membership = slack.membership(str(self.settings.extra["default_channel"]))
        owners = tuple(self.descriptor.authorized_owner_ids)
        return {
            "implementation": "tether",
            "protocol_version": 6,
            "peer_uid_enforced": True,
            "root_refused": True,
            "owner_configured": bool(owners),
            "allowed_user_count": len(owners),
            "slack_transport_connected": connected,
            "default_channel_membership": membership,
            "reply_poll_healthy": True,
            "queued_delivery_count": counts["ready_turns"],
            "uncertain_delivery_count": counts["uncertain_attempts"],
            "blocked_bridge_count": counts["rebind_required"],
            "schema_version": 18,
        }

    def op_identity(self, request: dict[str, Any]) -> dict[str, Any]:
        return dict(self._identity())

    def op_maintenance(self, request: dict[str, Any]) -> dict[str, Any]:
        return {"performed": []}

    def op_notify(self, request: dict[str, Any]) -> dict[str, Any]:
        """Post a root message and bind the calling session to its thread."""
        text = str(request.get("text") or "").strip()
        if not text:
            raise BrokerRefused("text_required")
        kind, session_id, cwd = self._source(request)
        team_id = self._team(request)
        channel_id = str(request.get("channel_id") or self.settings.extra.get("default_channel") or "")
        if not channel_id:
            raise BrokerRefused("channel_required", "no --channel and no default_channel configured")
        key = str(request.get("idempotency_key") or "")
        if not key:
            raise BrokerRefused("idempotency_key_required")
        source = {"session_id": session_id, "cwd": cwd}
        endpoint = self.runtime.register_endpoint(
            endpoint_key=endpoint_key_for(kind, session_id),
            endpoint_kind="detached_native",
            source_kind=kind,
            source_json=json.dumps(source, sort_keys=True, separators=(",", ":")),
            ref_version=1,
            descriptor=self.descriptor,
        )
        # Idempotent: the binding row is created pending_root under the key
        # first, so a retried notify never posts twice.
        binding = self.runtime.bind_thread(
            endpoint_id=endpoint["endpoint_id"], team_id=team_id, channel_id=channel_id,
            owner_user_id=self._owner(request), idempotency_key=f"notify:{team_id}:{channel_id}:{key}",
        )
        if binding.get("thread_ts"):
            return {"status": "duplicate", "state": "posted", "team_id": team_id,
                    "channel_id": channel_id, "thread_ts": binding["thread_ts"],
                    "message_ts": binding["thread_ts"], "bridge_id": binding["binding_id"]}
        ts = self._post(channel_id, text, None)
        binding = self.runtime.activate_binding(binding["binding_id"], ts)
        return {"status": "posted", "state": "posted", "team_id": team_id, "channel_id": channel_id,
                "thread_ts": ts, "message_ts": ts, "bridge_id": binding["binding_id"]}

    def op_attach(self, request: dict[str, Any]) -> dict[str, Any]:
        kind, session_id, cwd = self._source(request)
        team_id = self._team(request)
        channel_id = str(request.get("channel_id") or "")
        thread_ts = str(request.get("thread_ts") or "")
        if not channel_id or not thread_ts:
            raise BrokerRefused("thread_required", "--channel and --thread-ts are required")
        binding = self.bind(
            source_kind=kind, session_id=session_id, cwd=cwd, team_id=team_id,
            channel_id=channel_id, thread_ts=thread_ts, owner_user_id=self._owner(request),
        )
        return {"status": "attached", "team_id": team_id, "channel_id": channel_id,
                "thread_ts": thread_ts, "bridge_id": binding["binding_id"]}

    def op_rebind(self, request: dict[str, Any]) -> dict[str, Any]:
        kind, session_id, cwd = self._source(request)
        team_id = self._team(request)
        channel_id = str(request.get("channel_id") or "")
        thread_ts = str(request.get("thread_ts") or "")
        if not channel_id or not thread_ts:
            raise BrokerRefused("thread_required")
        existing = self.runtime.live_binding_for_thread(
            team_id=team_id, channel_id=channel_id, thread_ts=thread_ts
        )
        if existing is not None:
            try:
                self.runtime.close_binding(existing["binding_id"])
            except Exception as exc:
                raise BrokerRefused(getattr(exc, "code", "binding_busy"), str(exc), retryable=True) from exc
        source = {"session_id": session_id, "cwd": cwd}
        endpoint = self.runtime.register_endpoint(
            endpoint_key=endpoint_key_for(kind, session_id), endpoint_kind="detached_native",
            source_kind=kind, source_json=json.dumps(source, sort_keys=True, separators=(",", ":")),
            ref_version=1, descriptor=self.descriptor,
        )
        binding = self.runtime.bind_thread(
            endpoint_id=endpoint["endpoint_id"], team_id=team_id, channel_id=channel_id,
            thread_ts=thread_ts, owner_user_id=self._owner(request),
            idempotency_key=f"rebind:{team_id}:{channel_id}:{thread_ts}:{session_id}:{time.time_ns()}",
        )
        return {"status": "rebound", "team_id": team_id, "channel_id": channel_id,
                "thread_ts": thread_ts, "bridge_id": binding["binding_id"]}

    def op_close(self, request: dict[str, Any]) -> dict[str, Any]:
        team_id = self._team(request)
        channel_id = str(request.get("channel_id") or "")
        thread_ts = str(request.get("thread_ts") or "")
        binding_id = str(request.get("bridge_id") or "")
        if not binding_id:
            found = self.runtime.live_binding_for_thread(team_id=team_id, channel_id=channel_id, thread_ts=thread_ts)
            if found is None:
                raise BrokerRefused("binding_unknown")
            binding_id = found["binding_id"]
        try:
            closed = self.runtime.close_binding(binding_id)
        except Exception as exc:
            raise BrokerRefused(getattr(exc, "code", "binding_busy"), str(exc), retryable=True) from exc
        return {"status": "closed", "bridge_id": closed["binding_id"], "team_id": team_id,
                "channel_id": closed.get("channel_id"), "thread_ts": closed.get("thread_ts")}

    def op_thread_reply(self, request: dict[str, Any]) -> dict[str, Any]:
        text = str(request.get("text") or "").strip()
        channel_id = str(request.get("channel_id") or "")
        thread_ts = str(request.get("thread_ts") or "")
        if not text or not channel_id or not thread_ts:
            raise BrokerRefused("thread_required", "channel, thread-ts and text are required")
        if text.strip() == "NO_REPLY":
            return {"status": "no_reply", "team_id": self._team(request), "channel_id": channel_id, "thread_ts": thread_ts}
        ts = self._post(channel_id, text, thread_ts)
        return {"status": "posted", "team_id": self._team(request), "channel_id": channel_id,
                "thread_ts": thread_ts, "message_ts": ts}

    def op_reply(self, request: dict[str, Any]) -> dict[str, Any]:
        """A bound session answering its thread by binding id (legacy `tether reply`)."""
        binding_id = str(request.get("bridge_id") or "")
        text = str(request.get("text") or "")
        context = self.runtime.binding_thread(binding_id) if hasattr(self.runtime, "binding_thread") else None
        if context is None:
            raise BrokerRefused("binding_unknown")
        if text.strip() == "NO_REPLY":
            return {"status": "no_reply", "bridge_id": binding_id, "team_id": context["team_id"],
                    "channel_id": context["channel_id"], "thread_ts": context["thread_ts"],
                    "reply_key": request.get("reply_key")}
        ts = self._post(context["channel_id"], text.strip(), context["thread_ts"])
        return {"status": "posted", "bridge_id": binding_id, "team_id": context["team_id"],
                "channel_id": context["channel_id"], "thread_ts": context["thread_ts"],
                "message_ts": ts, "reply_key": request.get("reply_key")}

    def op_history(self, request: dict[str, Any]) -> dict[str, Any]:
        channel_id = str(request.get("channel_id") or self.settings.extra.get("default_channel") or "")
        if not channel_id:
            raise BrokerRefused("channel_required")
        limit = int(request.get("limit") or 20)
        return {"team_id": self._team(request), "channel_id": channel_id,
                "messages": self._slack().history(channel_id, limit=max(1, min(limit, 200)))}

    def op_thread_history(self, request: dict[str, Any]) -> dict[str, Any]:
        channel_id = str(request.get("channel_id") or "")
        thread_ts = str(request.get("thread_ts") or "")
        if not channel_id or not thread_ts:
            raise BrokerRefused("thread_required")
        return {"team_id": self._team(request), "channel_id": channel_id, "thread_ts": thread_ts,
                "messages": self._slack().thread_replies(channel_id, thread_ts)}

    def op_unresolved(self, request: dict[str, Any]) -> dict[str, Any]:
        operations: list[dict[str, Any]] = []
        for attempt in self.runtime.uncertain_attempts() if hasattr(self.runtime, "uncertain_attempts") else []:
            operations.append({"kind": "attempt", "id": attempt["attempt_id"], "state": attempt["state"],
                               "error_code": attempt.get("error_code"), "binding_id": attempt.get("binding_id")})
        return {"team_id": self._team(request), "operations": operations}

    def op_resolve(self, request: dict[str, Any]) -> dict[str, Any]:
        raise BrokerRefused(
            "unsupported_op",
            "operator resolution of uncertain attempts is not exposed through the broker yet; "
            "use hermes tether status to inspect",
        )

    def op_herdr_context(self, request: dict[str, Any]) -> dict[str, Any]:
        raise BrokerRefused("unsupported_op", "Herdr panes are not supported by Tether v2")

    def _slack(self) -> Any:
        slack = getattr(self, "slack", None)
        if slack is None or not slack.configured:
            raise BrokerRefused("slack_unconfigured", "no Slack bot token in the gateway")
        return slack

    def _post(self, channel_id: str, text: str, thread_ts: str | None) -> str:
        try:
            return self._slack().post(channel_id, text, thread_ts=thread_ts)
        except BrokerRefused:
            raise
        except Exception as exc:
            code = getattr(exc, "code", "slack_error")
            raise BrokerRefused(f"slack_{code}", str(exc), retryable=code in {"transport", "ratelimited"}) from exc

    # -- lifecycle ------------------------------------------------------------------

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(target=self._loop, name="tether-active", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    def _loop(self) -> None:
        heartbeat = Path(self.driver.work_root).parent / "active.heartbeat"
        while not self._stop.is_set():
            try:
                heartbeat.write_text(str(time.time()), encoding="utf-8")
                self.run_once()
            except Exception:
                logger.error("tether: scheduler pass failed", exc_info=True)
            self._stop.wait(self.settings.poll_interval_seconds)
