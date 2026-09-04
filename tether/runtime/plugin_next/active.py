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
import subprocess  # nosec B404 - fixed argv, no shell
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


def user_bus_path(uid: int | None = None) -> Path:
    return Path(f"/run/user/{os.getuid() if uid is None else uid}/bus")


def sandbox_facts() -> dict[str, bool]:
    """What this process can do; the harness child inherits exactly this."""
    facts = {"no_new_privs": False, "var_writable": True}
    try:
        for line in Path("/proc/self/status").read_text().splitlines():
            if line.startswith("NoNewPrivs:"):
                facts["no_new_privs"] = line.split()[-1] == "1"
    except OSError:
        pass
    facts["var_writable"] = os.access("/var/lib", os.W_OK)
    return facts


def resolve_launcher(settings: ActiveSettings) -> str:
    """'systemd-user' when the operator's user manager is reachable, else 'direct'."""
    mode = settings.launcher
    if mode == "direct":
        return "direct"
    reachable = user_bus_path().exists() and shutil.which("systemd-run") is not None
    if mode == "systemd-user" and not reachable:
        logger.warning("tether: launcher=systemd-user but %s is unreachable; running direct", user_bus_path())
        return "direct"
    return "systemd-user" if reachable else "direct"


def launch_plan(
    command: list[str], cwd: Path, env: dict[str, str], settings: ActiveSettings,
) -> tuple[list[str], dict[str, str], str]:
    """Wrap ``command`` for the chosen launcher; returns (argv, popen_env, launcher).

    systemd-user: ``systemd-run --user --pipe --wait`` asks the operator's user
    manager to run the harness in the user's own slice -- full groups, sudo,
    docker, a writable filesystem -- instead of inside the gateway's hardened
    unit. Exit status propagates; RuntimeMaxSec bounds the service the way
    the driver bounds the client.
    """
    launcher = resolve_launcher(settings)
    if launcher != "systemd-user":
        return command, env, "direct"
    argv = [
        shutil.which("systemd-run") or "systemd-run", "--user", "--quiet", "--pipe", "--wait",
        "--collect", f"--property=WorkingDirectory={cwd}",
        f"--property=RuntimeMaxSec={settings.native_timeout_seconds + 30}",
        "--property=KillMode=control-group",
    ]
    argv += [f"--setenv={key}={value}" for key, value in sorted(env.items())]
    argv += ["--", *command]
    bus = user_bus_path()
    popen_env = dict(env)
    popen_env["XDG_RUNTIME_DIR"] = str(bus.parent)
    popen_env["DBUS_SESSION_BUS_ADDRESS"] = f"unix:path={bus}"
    return argv, popen_env, "systemd-user"


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
    # Where a harness turn runs. "systemd-user": in the operator's own systemd
    # user manager, outside the gateway's sandbox (the default when the user
    # bus is reachable). "direct": as a child of the gateway, inheriting its
    # hardening. "auto" picks systemd-user when possible and says so in the prompt
    # when it cannot, so the session never mistakes the sandbox for the host.
    launcher: str = "auto"
    presence: bool = True
    ack_emoji: str = "eyes"
    done_emoji: str = "white_check_mark"
    fail_emoji: str = "warning"
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
        launcher=str(raw.get("launcher") or "auto"),
        presence=bool(raw.get("presence", True)),
        extra={"default_channel": str(raw.get("default_channel") or "")},
    )


def endpoint_key_for(source_kind: str, session_id: str) -> str:
    return f"detached_native:{source_kind}:{session_id}"


def runtime_truth(launcher: str) -> str:
    if launcher == "systemd-user":
        return (
            "Runtime truth: this turn runs in your operator's own systemd user session with "
            "the same user, groups, sudo and docker access they have. If a command is denied "
            "or a service is down, quote the exact error and ask; never infer a host or disk "
            "fault from a permission error."
        )
    facts = sandbox_facts()
    return (
        "Runtime truth: this turn runs INSIDE the gateway's hardened systemd unit, not in a "
        "login shell: no sudo (no_new_privs=%s), no docker socket, /var /etc /usr read-only "
        "(var_writable=%s), no ~/.ssh. Those limits belong to this turn, not to the host: "
        "the host is healthy unless you prove otherwise from a command you ran in this turn. "
        "If the work needs those privileges, say so in one sentence and stop; never infer a "
        "host or disk fault from a permission error."
        % (str(facts["no_new_privs"]).lower(), str(facts["var_writable"]).lower())
    )


def compose_prompt(context: dict[str, Any], settings: ActiveSettings, launcher: str = "direct") -> str:
    """The turn as the harness sees it: who said what, where you are, what to do.

    A bound session is the engineer who owns the work, not a chat persona.
    It continues the work with its tools and reports with evidence; the
    sentence cap applies to the reply, never to the work.
    """
    source = context.get("source") or {}
    lines = [
        "You are a Tether continuation of your own Claude Code / Codex session "
        f"(session {source.get('session_id', '?')}, cwd {source.get('cwd', '?')}, host "
        f"{os.uname().nodename}). New messages arrived in the Slack thread bound to this "
        "session:",
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
        "You own this work. Do what the message needs with your tools first (reproduce, fix, "
        "rerun, verify), then reply. Report with evidence: file and line, command and exit "
        "code, PR link, test count.",
        runtime_truth(launcher),
        "Reply contract: whatever you print is posted verbatim into the thread by Tether. Do "
        "not call tether reply/post/notify, do not mention bridge ids or reply keys, do not "
        "write 'Reply to' headers or any note to your operator; the thread is your reader.",
        f"Reply in at most {max(settings.max_reply_sentences, 3)} short sentences, as a colleague: "
        "no meta-narration, no restating the question. Mention people as <@USERID>. If the "
        "messages need no reply from you, respond with exactly NO_REPLY.",
    ]
    return "\n".join(lines)


def create_session(
    source_kind: str,
    cwd: Path,
    task: str,
    settings: ActiveSettings,
    *,
    runner: Callable[..., subprocess.CompletedProcess] = subprocess.run,
    timeout: int = 600,
) -> str:
    """Start a fresh harness session on this box, seeded with the task; return its id.

    The session runs its first turn now so the id exists on disk and later
    `--resume` finds it. The task text is the first user turn, so the session
    already knows what it is for when the thread starts talking to it.
    """
    env = child_env(passthrough=settings.harness_env)
    if source_kind == "codex_session":
        binary = shutil.which(settings.codex_binary) or settings.codex_binary
        command = [binary, "exec", "--json", *settings.codex_resume_args, task]
        completed = runner(command, cwd=str(cwd), env=env, input="", capture_output=True, text=True, timeout=timeout)  # nosec B603
        for line in completed.stdout.splitlines():
            try:
                event = json.loads(line)
            except ValueError:
                continue
            if event.get("type") == "thread.started" and event.get("thread_id"):
                return str(event["thread_id"])
        raise RuntimeError(f"codex did not report a thread id (exit {completed.returncode})")
    binary = shutil.which(settings.claude_binary) or settings.claude_binary
    command = [binary, "-p", "--output-format", "json", *settings.claude_resume_args, task]
    completed = runner(command, cwd=str(cwd), env=env, capture_output=True, text=True, timeout=timeout)  # nosec B603
    try:
        payload = json.loads(completed.stdout.strip().splitlines()[-1]) if completed.stdout.strip() else {}
    except ValueError:
        payload = {}
    session_id = str(payload.get("session_id") or "")
    if not session_id:
        raise RuntimeError(f"claude did not report a session id (exit {completed.returncode})")
    return session_id


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
        else:
            # Presence: the thread sees "seen" within a second, the way a
            # colleague reacts before they go and do the thing.
            self._react(binding["channel_id"], message_id, self.settings.ack_emoji)
        return {"binding_id": binding["binding_id"], "event_key": event_key}

    def _react(self, channel_id: str, message_ts: str, emoji: str) -> None:
        if not self.settings.presence or not message_ts or not emoji:
            return
        slack = getattr(self, "slack", None)
        if slack is None or not getattr(slack, "configured", False):
            return
        try:
            slack.react(channel_id, message_ts, emoji)
        except Exception:
            logger.debug("tether: reaction %s failed", emoji, exc_info=True)

    def _unreact(self, channel_id: str, message_ts: str, emoji: str) -> None:
        if not self.settings.presence or not message_ts or not emoji:
            return
        slack = getattr(self, "slack", None)
        if slack is None or not getattr(slack, "configured", False):
            return
        try:
            slack.unreact(channel_id, message_ts, emoji)
        except Exception:
            logger.debug("tether: un-reaction %s failed", emoji, exc_info=True)

    def _turn_message_ids(self, context: dict[str, Any]) -> list[str]:
        ids: list[str] = []
        for turn in context.get("turns", []):
            try:
                ids.append(str(json.loads(turn.get("payload_inline") or "{}").get("ts") or ""))
            except ValueError:
                pass
        return [i for i in ids if i]

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
        launcher = resolve_launcher(self.settings)
        prompt = compose_prompt(context, self.settings, launcher)
        try:
            command = self.command_factory(context, self.settings, prompt)
            cwd = Path(str(context["source"].get("cwd") or os.getcwd()))
            if not cwd.is_dir():
                cwd = Path.home()
            argv, popen_env, launcher = launch_plan(
                command, cwd, child_env(passthrough=self.settings.harness_env), self.settings,
            )
            logger.info("tether: attempt %s launcher=%s", attempt["attempt_id"], launcher)
            launched = self.driver.launch(attempt, command=argv, cwd=cwd, env=popen_env)
            result = self.driver.reap(
                attempt, launched, timeout_seconds=self.settings.native_timeout_seconds
            )
        except Exception as exc:
            logger.error(
                "tether: attempt %s did not reach a receipt (%s)",
                attempt["attempt_id"], type(exc).__name__, exc_info=True,
            )
            for ts in self._turn_message_ids(context):
                self._react(context["channel_id"], ts, self.settings.fail_emoji)
            return
        state = result.get("state")
        marker = self.settings.done_emoji if state in {"completed_with_response", "no_reply"} else self.settings.fail_emoji
        for ts in self._turn_message_ids(context):
            self._unreact(context["channel_id"], ts, self.settings.ack_emoji)
            self._react(context["channel_id"], ts, marker)
        if state != "completed_with_response":
            if state not in {"no_reply"}:
                self._post_failure_notice(context, attempt, result)
            return
        final = self.runtime.attempt_context(attempt["attempt_id"])
        text = self._read_response(final.get("response_ref"))
        if not text.strip():
            return
        try:
            self.egress(final["channel_id"], final["thread_ts"], text.strip())
        except Exception:
            logger.error("tether: egress failed for %s", attempt["attempt_id"], exc_info=True)
            for ts in self._turn_message_ids(context):
                self._react(context["channel_id"], ts, self.settings.fail_emoji)

    def _post_failure_notice(self, context: dict[str, Any], attempt: dict[str, Any], result: dict[str, Any]) -> None:
        """A failed turn says so, in one line, with the harness's own words.

        Silence after a warning emoji reads as being ignored. The reason is
        whatever the harness printed (rate limit, auth, crash), redacted to a
        single line, so the thread knows whether to wait or to escalate.
        """
        reason = ""
        try:
            work = self.driver._attempt_dir(attempt["attempt_id"])  # noqa: SLF001 - same package
            for name in ("response.out", "stderr.log"):
                path = work / name
                if path.exists():
                    text = path.read_text(encoding="utf-8", errors="replace").strip()
                    if text:
                        reason = text.splitlines()[-1].strip()[:200]
                        break
        except Exception:  # best effort: the notice must never fail the drive
            reason = ""
        code = str(result.get("error_code") or "")
        if not code:
            try:
                code = str(self.runtime.attempt_context(attempt["attempt_id"]).get("error_code") or "")
            except Exception:
                code = ""
        code = code or str(result.get("state") or "failed")
        who = ""
        for turn in context.get("turns", []):
            try:
                who = str(json.loads(turn.get("payload_inline") or "{}").get("user") or "")
            except ValueError:
                pass
        mention = f"<@{who}> " if who and who != "operator" else ""
        detail = f": {reason}" if reason else ""
        text = (
            f"{mention}I could not take this turn ({code}{detail}). "
            "Reply here again later to retry, or ping my operator if it is urgent."
        )
        try:
            self.egress(context["channel_id"], context["thread_ts"], text)
        except Exception:
            logger.error("tether: failure notice egress failed for %s", attempt["attempt_id"], exc_info=True)

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
            "harness_launcher": resolve_launcher(self.settings),
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

    def op_spawn(self, request: dict[str, Any]) -> dict[str, Any]:
        """Start a fresh harness session for a task and bind it to a thread.

        This is what lets an agent say "I'll take it": the session is created
        on this box, in the requested repo, seeded with the task, then bound so
        every later message in the thread continues that same session.
        """
        kind = str(request.get("harness") or "claude")
        source_kind = {"claude": "claude_session", "codex": "codex_session"}.get(kind)
        if source_kind is None:
            raise BrokerRefused("harness_unsupported", "harness must be claude or codex")
        task = str(request.get("task") or "").strip()
        if not task:
            raise BrokerRefused("task_required")
        cwd = Path(str(request.get("cwd") or os.getcwd())).expanduser()
        if not cwd.is_dir():
            raise BrokerRefused("cwd_missing", f"{cwd} is not a directory")
        team_id = self._team(request)
        channel_id = str(request.get("channel_id") or self.settings.extra.get("default_channel") or "")
        thread_ts = str(request.get("thread_ts") or "")
        if not channel_id:
            raise BrokerRefused("channel_required")
        try:
            session_id = self._create_session(source_kind, cwd, task)
        except Exception as exc:
            raise BrokerRefused("spawn_failed", str(exc)[:300]) from exc
        if not thread_ts:
            root = str(request.get("root_text") or "").strip() or f"On it: {task[:200]}"
            thread_ts = self._post(channel_id, root, None)
        binding = self.bind(
            source_kind=source_kind, session_id=session_id, cwd=str(cwd), team_id=team_id,
            channel_id=channel_id, thread_ts=thread_ts, owner_user_id=self._owner(request),
        )
        return {"status": "spawned", "harness": kind, "session_id": session_id, "cwd": str(cwd),
                "team_id": team_id, "channel_id": channel_id, "thread_ts": thread_ts,
                "bridge_id": binding["binding_id"]}

    def _create_session(self, source_kind: str, cwd: Path, task: str) -> str:
        return create_session(source_kind, cwd, task, self.settings)

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
        team_id = self._team(request)
        # An operator posting into a bound thread through the broker is an
        # instruction to the session that owns it. Slack ingress would drop it
        # as our own message, so admit it here as a turn.
        claimed = self.claim(
            {"workspace": team_id, "channel": channel_id, "thread": thread_ts,
             "actor": str(request.get("actor") or "operator"), "message_id": ts},
            text.strip(),
        )
        return {"status": "posted", "team_id": team_id, "channel_id": channel_id,
                "thread_ts": thread_ts, "message_ts": ts, "turn_admitted": claimed is not None}

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
