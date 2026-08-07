from __future__ import annotations

import contextlib
import datetime
import fcntl
import hashlib
import importlib.util
import json
import http.client
import math
import os
import re
import selectors
import shlex
import shutil
import signal
import socket
import socketserver
import ssl
import sqlite3
import stat
import struct
# All invocations below use fixed argv lists without a shell.
import subprocess  # nosec B404
import sys
import threading
import time
import tomllib
import urllib.parse
import uuid
from dataclasses import dataclass
from pathlib import Path
from collections.abc import Callable
from typing import Any, TypedDict


def _load_security_module() -> Any:
    path = Path(__file__).resolve().with_name("security.py")
    injected = sys.modules.get("security")
    if injected is not None:
        injected_path = getattr(injected, "__file__", "")
        with contextlib.suppress(OSError, TypeError, ValueError):
            if Path(injected_path).resolve() == path:
                return injected
    module_name = (
        "_tether_runtime_security_"
        + hashlib.sha256(str(path).encode()).hexdigest()[:16]
    )
    existing = sys.modules.get(module_name)
    if existing is not None:
        return existing
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Tether security module could not be loaded")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


security = _load_security_module()


def _load_hermes_compat_module() -> Any:
    path = Path(__file__).resolve().with_name("hermes_compat.py")
    module_name = (
        "_tether_runtime_hermes_compat_"
        + hashlib.sha256(str(path).encode()).hexdigest()[:16]
    )
    existing = sys.modules.get(module_name)
    if existing is not None:
        return existing
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(
            "Tether Hermes compatibility module could not be loaded"
        )
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


hermes_compat = _load_hermes_compat_module()


def _load_slack_protocol_module() -> Any:
    path = Path(__file__).resolve().with_name("slack_protocol.py")
    module_name = (
        "_tether_runtime_slack_protocol_"
        + hashlib.sha256(str(path).encode()).hexdigest()[:16]
    )
    existing = sys.modules.get(module_name)
    if existing is not None:
        return existing
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Tether Slack protocol module could not be loaded")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


slack_protocol = _load_slack_protocol_module()
_SLACK_RETRY_COORDINATOR = slack_protocol.RetryAfterCoordinator()
_SLACK_CALL_CONTEXT = threading.local()
_SLACK_TOKEN_WORKSPACES: dict[str, str] = {}
_SLACK_TOKEN_WORKSPACES_LOCK = threading.Lock()
_RECOVERY_WARNING_TIMES: dict[str, float] = {}
_RECOVERY_WARNING_LOCK = threading.Lock()
_SLACK_TLS_CONTEXT = ssl.create_default_context()
_SLACK_TLS_CONTEXT.minimum_version = ssl.TLSVersion.TLSv1_2
_RECOVERY_WARNING_INTERVAL_SECONDS = 300.0


HERMES_HOME = Path(os.environ.get("HERMES_HOME", Path.home() / ".hermes")).expanduser()
DATA_HOME = Path(os.environ.get("XDG_DATA_HOME", Path.home() / ".local" / "share")).expanduser()
CONFIG_HOME = Path(os.environ.get("XDG_CONFIG_HOME", Path.home() / ".config")).expanduser()
UPLOAD_APPROVED_ROOTS = tuple(
    value
    for value in os.environ.get("TETHER_UPLOAD_APPROVED_ROOTS", "").split(
        os.pathsep
    )
    if value
)
UPLOAD_STAGING_DIRECTORY = os.environ.get(
    "TETHER_UPLOAD_STAGING_DIRECTORY",
    str(HERMES_HOME / "upload-staging"),
)
UPLOAD_MAX_BYTES = os.environ.get(
    "TETHER_UPLOAD_MAX_BYTES",
    str(security.DEFAULT_UPLOAD_LIMIT),
)
RUNTIME_HOME = DATA_HOME / "tether"
CONFIG_PATH = Path(os.environ.get("TETHER_CONFIG", CONFIG_HOME / "tether" / "config.toml")).expanduser()
DB_PATH = HERMES_HOME / "bridges.db"
SOCKET_PATH = HERMES_HOME / "bridge.sock"
ID_PATTERN = re.compile(r"^[A-Z][A-Z0-9]{7,}$")
CHANNEL_ID_PATTERN = re.compile(r"^[CDG][A-Z0-9]{7,}$")
SAFE_CHILD_ENV = {
    "HOME", "USER", "LOGNAME", "PATH", "SHELL", "LANG", "LC_ALL", "LC_CTYPE",
    "TERM", "TMPDIR", "XDG_CONFIG_HOME", "XDG_CACHE_HOME", "XDG_DATA_HOME",
    "CODEX_HOME", "CLAUDE_HOME", "SSL_CERT_FILE", "SSL_CERT_DIR",
}
FORBIDDEN_CREDENTIAL_ENV = frozenset({
    "BASH_ENV", "ENV", "HOME", "IFS", "LD_LIBRARY_PATH", "LD_PRELOAD",
    "NODE_OPTIONS", "NODE_PATH", "OP_SERVICE_ACCOUNT_TOKEN", "PATH",
    "PROMPT_COMMAND", "PYTHONHOME", "PYTHONPATH", "SHELL",
})
FORBIDDEN_CREDENTIAL_PREFIXES = (
    "DYLD_",
    "GIT_",
    "SLACK_",
    "SSH_",
)
MAX_TEXT = 35_000
MAX_NATIVE_OUTPUT = 35_000
MAX_NATIVE_STDOUT_BYTES = MAX_NATIVE_OUTPUT * 4
MAX_NATIVE_STDERR_BYTES = 64 * 1024
NATIVE_STREAM_CHUNK_BYTES = 64 * 1024
MAX_SLACK_API_RESPONSE_BYTES = 8 * 1024 * 1024
MAX_BROKER_REQUEST_BYTES = 1 * 1024 * 1024
MAX_BROKER_RESPONSE_BYTES = 8 * 1024 * 1024
MAX_SLACK_FILENAME = 180
MAX_SOURCE_VALUE = 4_096
MAX_IDEMPOTENCY_KEY = 256
ZELLIJ_WRITE_CHUNK_CHARS = 96
DEFAULT_BROKER_MAX_CONNECTIONS = 32
DEFAULT_BROKER_READ_TIMEOUT_SECONDS = 5.0
REPLY_KEY_PATTERN = re.compile(r"^[A-Za-z0-9_-]{8,128}$")
HERMES_SEND_GROUP_PATTERN = re.compile(r"^hsg_[0-9a-f]{32}$")
SESSION_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,255}$")
CONFIG_VERSION = 1
BINDING_VERSION = 3
SUPPORTED_BINDING_VERSIONS = frozenset({1, 2, BINDING_VERSION})
SCHEMA_VERSION = 15
RECONCILIATION_PAGE_LIMIT = 15
RECONCILIATION_INTERVAL_SECONDS = 60
RECONCILIATION_MAX_PAGES = 1_000
PROCESS_EPOCH = uuid.uuid4().hex
PROCESS_IDENTITY_PREFIX = "linux-proc-v2:"
PROCESS_IDENTITY_FIELDS = frozenset({
    "agent", "boot", "exe", "exe_path", "pane", "pid", "session", "start", "tty",
})
HERDR_PROCESS_IDENTITY_PREFIX = "herdr-proc-v1:"
HERDR_PROCESS_IDENTITY_FIELDS = frozenset({
    "agent", "boot", "exe", "exe_path", "pid", "start", "terminal", "tty",
})
HERDR_PROTOCOL_VERSION = 19
HERDR_AGENT_NAME_PATTERN = re.compile(r"^[a-z][a-z0-9_-]{0,31}$")
HERDR_TERMINAL_ID_PATTERN = re.compile(r"^term_[A-Za-z0-9]+$")
COMMON_SOURCE_FIELDS = frozenset({
    "binding_version", "binding_state", "endpoint_kind", "delivery_policy",
    "process_identity", "cwd_realpath", "cwd_device", "cwd_inode",
    "cwd_owner_uid",
})
HERDR_ENDPOINT_FIELDS = frozenset({
    "herdr_session", "herdr_socket_path", "herdr_terminal_id",
    "herdr_pane_id", "herdr_agent_name", "herdr_agent_session_source",
    "herdr_agent_session_kind", "herdr_agent_session_value", "herdr_protocol",
})
BINDING_METADATA_FIELDS = frozenset({
    "binding_version", "binding_state", "endpoint_kind", "delivery_policy",
})
SOURCE_FIELDS = {
    "zellij_pane": frozenset({
        "session_name", "pane_id", "zellij_session", "zellij_pane_id", "cwd",
        "pane_command_hash", "pane_agent",
    }) | COMMON_SOURCE_FIELDS,
    "claude_session": frozenset({
        "session_id", "zellij_session", "zellij_pane_id", "cwd",
        "pane_command_hash", "pane_agent",
    }) | COMMON_SOURCE_FIELDS | HERDR_ENDPOINT_FIELDS,
    "codex_session": frozenset({
        "session_id", "zellij_session", "zellij_pane_id", "cwd",
        "pane_command_hash", "pane_agent",
    }) | COMMON_SOURCE_FIELDS | HERDR_ENDPOINT_FIELDS,
    "hermes_session": frozenset({"session_id", "run_id", "cwd"}) | COMMON_SOURCE_FIELDS,
    "headless_run": frozenset({"run_id", "queue_id", "cwd"}) | COMMON_SOURCE_FIELDS,
}
SLACK_METHOD_PATHS = {
    "auth.test": "/api/auth.test",
    "chat.postMessage": "/api/chat.postMessage",
    "chat.update": "/api/chat.update",
    "conversations.history": "/api/conversations.history",
    "conversations.join": "/api/conversations.join",
    "conversations.replies": "/api/conversations.replies",
    "files.completeUploadExternal": "/api/files.completeUploadExternal",
    "files.getUploadURLExternal": "/api/files.getUploadURLExternal",
    "files.info": "/api/files.info",
}
SLACK_FILE_ID_PATTERN = re.compile(r"^F[A-Z0-9]{8,}$")
ROOT_UPLOAD_PHASES = frozenset({
    "none",
    "reserved",
    "allocating",
    "allocation_uncertain",
    "allocated",
    "uploading_bytes",
    "bytes_uncertain",
    "bytes_uploaded",
    "completing",
    "completion_uncertain",
    "reconciling",
    "completion_confirmed",
    "completed",
})
class BridgeRequest(TypedDict, total=False):
    op: str
    text: str
    source_kind: str
    source: dict[str, Any]
    owner_user_id: str
    team_id: str
    channel_id: str
    idempotency_key: str
    thread_ts: str
    bridge_id: str
    reply_key: str
    file_path: str | None
    limit: int
    herdr_terminal_id: str
    herdr_agent_name: str
    herdr_agent_session_value: str
    herdr_agent: str
    expected_generation: int


@dataclass(frozen=True)
class Config:
    default_channel: str = ""
    default_owner: str = ""
    allow_channel_owner_restrictions: bool = False
    team_id: str = ""
    allowed_users: tuple[str, ...] = ()
    native_timeout_seconds: int = 1800
    max_reply_words: int = 50
    max_reply_chars: int = 500
    max_reply_sentences: int = 3
    retention_days: int = 30
    codex_binary: str = "codex"
    claude_binary: str = "claude"
    codex_resume_args: tuple[str, ...] = ()
    claude_resume_args: tuple[str, ...] = ()
    credential_command: tuple[str, ...] = ()
    credential_env_allowlist: tuple[str, ...] = ()
    zellij_agent_commands: tuple[str, ...] = ("claude", "codex", "gemini", "hermes", "pi")


CONFIG_KEYS = frozenset({
    "config_version",
    "default_channel",
    "default_owner",
    "allow_channel_owner_restrictions",
    "team_id",
    "allowed_users",
    "native_timeout_seconds",
    "max_reply_words",
    "max_reply_chars",
    "max_reply_sentences",
    "retention_days",
    "codex_binary",
    "claude_binary",
    "codex_resume_args",
    "claude_resume_args",
    "credential_command",
    "credential_env_allowlist",
    "zellij_agent_commands",
})
CONFIG_CONTROL_PATTERN = re.compile(r"[\x00-\x1f\x7f]")


def _config_integer(
    raw: dict[str, Any],
    name: str,
    default: int,
) -> int:
    value = raw.get(name, default)
    if not isinstance(value, int) or isinstance(value, bool):
        raise ValueError(f"{name} must be an integer")
    return value


def _config_string(
    raw: dict[str, Any],
    name: str,
    default: str = "",
    *,
    allow_empty: bool = True,
) -> str:
    value = raw.get(name, default)
    if (
        not isinstance(value, str)
        or (not allow_empty and not value)
        or CONFIG_CONTROL_PATTERN.search(value)
    ):
        suffix = "string" if allow_empty else "non-empty string"
        raise ValueError(f"{name} must be a {suffix} without control characters")
    return value


def _config_string_array(
    raw: dict[str, Any],
    name: str,
    default: list[str],
    *,
    allow_empty: bool = True,
) -> list[str]:
    values = raw.get(name, default)
    if (
        not isinstance(values, list)
        or (not allow_empty and not values)
        or not all(
            isinstance(value, str)
            and bool(value)
            and CONFIG_CONTROL_PATTERN.search(value) is None
            for value in values
        )
    ):
        raise ValueError(
            f"{name} must be a string array without empty or control-character values"
        )
    return values


def minimum_retention_days() -> int:
    try:
        recovery_hours = int(
            os.getenv("TETHER_REPLY_RECOVERY_HOURS", "24")
        )
    except ValueError:
        recovery_hours = 24
    recovery_hours = max(1, min(recovery_hours, 168))
    return max(2, (recovery_hours + 23) // 24 + 1)


def load_config(path: Path = CONFIG_PATH) -> Config:
    candidate = Path(path).expanduser()
    if not candidate.exists() and not candidate.is_symlink():
        return Config()
    raw = tomllib.loads(
        security.read_private_text(
            candidate,
            max_bytes=256 * 1024,
        )
    )
    unknown = sorted(set(raw) - CONFIG_KEYS)
    if unknown:
        raise ValueError(
            "unknown Tether config key"
            + ("s" if len(unknown) != 1 else "")
            + ": "
            + ", ".join(unknown)
        )
    version = _config_integer(raw, "config_version", CONFIG_VERSION)
    if version != CONFIG_VERSION:
        raise ValueError(
            f"config_version {version} is unsupported; expected {CONFIG_VERSION}"
        )
    timeout = _config_integer(raw, "native_timeout_seconds", 1800)
    if not 30 <= timeout <= 86_400:
        raise ValueError("native_timeout_seconds must be between 30 and 86400")
    max_reply_words = _config_integer(raw, "max_reply_words", 50)
    max_reply_chars = _config_integer(raw, "max_reply_chars", 500)
    max_reply_sentences = _config_integer(raw, "max_reply_sentences", 3)
    retention_days = _config_integer(raw, "retention_days", 30)
    if not 20 <= max_reply_words <= 500:
        raise ValueError("max_reply_words must be between 20 and 500")
    if not 100 <= max_reply_chars <= 4_000:
        raise ValueError("max_reply_chars must be between 100 and 4000")
    if not 1 <= max_reply_sentences <= 20:
        raise ValueError("max_reply_sentences must be between 1 and 20")
    minimum_retention = minimum_retention_days()
    if not minimum_retention <= retention_days <= 3_650:
        raise ValueError(
            "retention_days must be between "
            f"{minimum_retention} and 3650 for the configured Slack recovery horizon"
        )
    command = _config_string_array(raw, "credential_command", [])
    codex_args = _config_string_array(raw, "codex_resume_args", [])
    claude_args = _config_string_array(raw, "claude_resume_args", [])
    allowlist = _config_string_array(raw, "credential_env_allowlist", [])
    zellij_commands = _config_string_array(
        raw,
        "zellij_agent_commands",
        ["claude", "codex", "gemini", "hermes", "pi"],
        allow_empty=False,
    )
    users = _config_string_array(raw, "allowed_users", [])
    if command and not Path(command[0]).expanduser().is_absolute():
        raise ValueError("credential_command executable must be an absolute path")
    if any(_credential_key_is_forbidden(key) for key in allowlist):
        raise ValueError("credential_env_allowlist contains an execution-control key")
    if not all(ID_PATTERN.fullmatch(value) for value in users):
        raise ValueError("allowed_users contains an invalid Slack member ID")
    default_channel = _config_string(raw, "default_channel")
    default_owner = _config_string(raw, "default_owner")
    allow_channel_owner_restrictions = raw.get("allow_channel_owner_restrictions", False)
    team_id = _config_string(raw, "team_id")
    if not isinstance(allow_channel_owner_restrictions, bool):
        raise ValueError("allow_channel_owner_restrictions must be a boolean")
    if default_channel and not CHANNEL_ID_PATTERN.fullmatch(default_channel):
        raise ValueError("default_channel is not a valid Slack channel ID")
    if default_owner and default_owner != "*" and not ID_PATTERN.fullmatch(default_owner):
        raise ValueError("default_owner is not a valid Slack member ID")
    if team_id and not ID_PATTERN.fullmatch(team_id):
        raise ValueError("team_id is not a valid Slack workspace ID")
    return Config(
        default_channel=default_channel,
        default_owner=default_owner,
        allow_channel_owner_restrictions=allow_channel_owner_restrictions,
        team_id=team_id,
        allowed_users=tuple(users),
        native_timeout_seconds=timeout,
        max_reply_words=max_reply_words,
        max_reply_chars=max_reply_chars,
        max_reply_sentences=max_reply_sentences,
        retention_days=retention_days,
        codex_binary=_config_string(
            raw,
            "codex_binary",
            "codex",
            allow_empty=False,
        ),
        claude_binary=_config_string(
            raw,
            "claude_binary",
            "claude",
            allow_empty=False,
        ),
        codex_resume_args=tuple(codex_args),
        claude_resume_args=tuple(claude_args),
        credential_command=tuple(command),
        credential_env_allowlist=tuple(allowlist),
        zellij_agent_commands=tuple(zellij_commands),
    )


def effective_allowed_users(config: Config | None = None) -> tuple[str, ...]:
    """Merge Tether overrides with Hermes's existing explicit allowlists."""
    config = config or load_config()
    candidates = list(config.allowed_users)
    for name in ("SLACK_ALLOWED_USERS", "GATEWAY_ALLOWED_USERS"):
        candidates.extend(value.strip() for value in os.getenv(name, "").split(","))
    result = []
    for value in candidates:
        if value != "*" and ID_PATTERN.fullmatch(value) and value not in result:
            result.append(value)
    return tuple(result)


def effective_channel(config: Config | None = None) -> str:
    config = config or load_config()
    return config.default_channel or os.getenv("SLACK_HOME_CHANNEL", "").strip()


@dataclass(frozen=True)
class Bridge:
    bridge_id: str
    source_kind: str
    source: dict[str, Any]
    owner_user_id: str
    team_id: str
    channel_id: str
    thread_ts: str | None
    idempotency_key: str
    status: str
    binding_generation: int = 1
    binding_version: int = 1
    binding_state: str = "legacy"
    binding_error_code: str = ""


@dataclass(frozen=True)
class ReplyPollPageState:
    team_id: str
    channel_id: str
    thread_ts: str
    next_cursor: str | None
    seen_cursors: tuple[str, ...]
    pages_seen: int
    page_oldest: str
    bot_user_ids: tuple[str, ...] = ()
    root_bridge_id: str = ""
    pending_messages: tuple[dict[str, str], ...] = ()


@dataclass(frozen=True)
class SourceBinding:
    version: int
    source_kind: str
    state: str
    endpoint_kind: str
    delivery_policy: str
    adapter: str
    session_id: str = ""
    run_id: str = ""
    cwd: str = ""
    cwd_realpath: str = ""
    cwd_device: str = ""
    cwd_inode: str = ""
    cwd_owner_uid: str = ""
    zellij_session: str = ""
    zellij_pane_id: str = ""
    herdr_session: str = ""
    herdr_socket_path: str = ""
    herdr_terminal_id: str = ""
    herdr_pane_id: str = ""
    herdr_agent_name: str = ""
    herdr_agent_session_source: str = ""
    herdr_agent_session_kind: str = ""
    herdr_agent_session_value: str = ""
    herdr_protocol: str = ""
    pane_agent: str = ""
    process_identity: str = ""

    @property
    def is_native(self) -> bool:
        return self.source_kind in {"claude_session", "codex_session", "zellij_pane"}

    @property
    def uses_zellij(self) -> bool:
        return self.endpoint_kind == "zellij_pane"

    @property
    def uses_herdr(self) -> bool:
        return self.endpoint_kind == "herdr_agent"


class NativeContinuationError(RuntimeError):
    def __init__(
        self,
        message: str,
        code: str = "native_continuation_failed",
        *,
        binding_id: str = "",
        retryable: bool | None = None,
        status: str = "",
        next_action: str = "",
    ):
        super().__init__(message)
        self.code = code
        profile = ERROR_PROFILES.get(code, ERROR_PROFILES["native_continuation_failed"])
        self.binding_id = binding_id
        self.retryable = profile["retryable"] if retryable is None else retryable
        self.status = status or str(profile["status"])
        self.next_action = next_action or str(profile["next_action"])

    def as_payload(self) -> dict[str, Any]:
        return {
            "ok": False,
            "code": self.code,
            "message": security.redact_egress_text(str(self))[:500],
            "binding_id": self.binding_id,
            "status": self.status,
            "retryable": self.retryable,
            "next_action": self.next_action,
        }


ERROR_PROFILES: dict[str, dict[str, str | bool]] = {
    "native_continuation_failed": {
        "status": "failed",
        "retryable": True,
        "next_action": "Inspect the bound session, then retry or rebind.",
    },
    "binding_rebind_required": {
        "status": "rebind_required",
        "retryable": False,
        "next_action": "Run `tether rebind` from the intended live session.",
    },
    "binding_generation_changed": {
        "status": "stale",
        "retryable": True,
        "next_action": "Retry against the latest verified binding generation.",
    },
    "endpoint_mismatch": {
        "status": "rebind_required",
        "retryable": False,
        "next_action": "Rebind the thread to the intended delivery endpoint.",
    },
    "process_identity_missing": {
        "status": "rebind_required",
        "retryable": False,
        "next_action": "Start the agent in the intended pane, then rebind.",
    },
    "process_identity_changed": {
        "status": "stale",
        "retryable": False,
        "next_action": "Rebind to the current agent process before retrying.",
    },
    "process_identity_ambiguous": {
        "status": "ambiguous",
        "retryable": False,
        "next_action": "Select one live agent process, then rebind.",
    },
    "adapter_pane_mismatch": {
        "status": "ambiguous",
        "retryable": False,
        "next_action": "Rebind from the pane running the intended adapter.",
    },
    "cwd_identity_changed": {
        "status": "rebind_required",
        "retryable": False,
        "next_action": "Rebind from the intended working directory before retrying.",
    },
    "ack_timeout": {
        "status": "stale",
        "retryable": True,
        "next_action": "Inspect the bound session, then retry or rebind.",
    },
    "terminal_submit_not_started": {
        "status": "retryable",
        "retryable": True,
        "next_action": "Retry the delivery against the same verified binding.",
    },
    "terminal_submit_uncertain": {
        "status": "uncertain",
        "retryable": False,
        "next_action": "Inspect the bound session before retrying or rebinding.",
    },
    "invalid_request": {
        "status": "rejected",
        "retryable": False,
        "next_action": "Correct the request and retry.",
    },
    "peer_uid_mismatch": {
        "status": "rejected",
        "retryable": False,
        "next_action": "Run Tether under the same dedicated non-root account as the broker.",
    },
    "workspace_mismatch": {
        "status": "rejected",
        "retryable": False,
        "next_action": "Use the Slack workspace where this Tether app is installed.",
    },
    "broker_busy": {
        "status": "busy",
        "retryable": True,
        "next_action": "Retry after the current local broker requests finish.",
    },
    "broker_unavailable": {
        "status": "unavailable",
        "retryable": True,
        "next_action": "Run `tether doctor`; retry after the local broker is reachable.",
    },
    "request_timeout": {
        "status": "rejected",
        "retryable": True,
        "next_action": "Reconnect and send one complete broker request.",
    },
    "slack_reconciliation_pending": {
        "status": "pending",
        "retryable": True,
        "next_action": "Retry the same idempotency key after reconciliation advances.",
    },
    "slack_reconciliation_failed": {
        "status": "blocked",
        "retryable": False,
        "next_action": "Inspect the local Tether reconciliation record before retrying.",
    },
    "broker_internal_error": {
        "status": "failed",
        "retryable": True,
        "next_action": "Run `tether doctor`; retry after the broker is healthy.",
    },
}


def _binding_error(
    code: str,
    message: str,
    *,
    binding_id: str = "",
) -> NativeContinuationError:
    return NativeContinuationError(message, code=code, binding_id=binding_id)


def _safe_error_response(exc: Exception) -> dict[str, Any]:
    if isinstance(exc, NativeContinuationError):
        return exc.as_payload()
    if isinstance(exc, (ValueError, json.JSONDecodeError)):
        return NativeContinuationError(
            security.redact_egress_text(str(exc))[:500],
            code="invalid_request",
        ).as_payload()
    return NativeContinuationError(
        "Tether could not complete the request.",
        code="broker_internal_error",
    ).as_payload()


def _broker_response_frame(response: dict[str, Any]) -> bytes:
    if (
        not isinstance(response, dict)
        or not isinstance(response.get("ok"), bool)
    ):
        response = {}
    try:
        frame = (
            json.dumps(
                response,
                allow_nan=False,
                ensure_ascii=False,
                separators=(",", ":"),
            ).encode("utf-8")
            + b"\n"
            if response
            else b""
        )
    except (TypeError, ValueError, RecursionError):
        frame = b""
    if len(frame) <= MAX_BROKER_RESPONSE_BYTES:
        if frame:
            return frame
    fallback = _safe_error_response(
        NativeContinuationError(
            "Tether's local broker could not encode a bounded protocol response.",
            code="broker_internal_error",
        )
    )
    return (
        json.dumps(
            fallback,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
        + b"\n"
    )


def _parse_process_identity(value: str) -> dict[str, str | int]:
    if not value.startswith(PROCESS_IDENTITY_PREFIX):
        raise ValueError("pane process identity has an unsupported format")
    try:
        payload = json.loads(value.removeprefix(PROCESS_IDENTITY_PREFIX))
    except json.JSONDecodeError as exc:
        raise ValueError("pane process identity is malformed") from exc
    if not isinstance(payload, dict) or set(payload) != PROCESS_IDENTITY_FIELDS:
        raise ValueError("pane process identity is incomplete")
    string_fields = PROCESS_IDENTITY_FIELDS - {"pid"}
    if (
        not all(isinstance(payload[field], str) for field in string_fields)
        or not isinstance(payload["pid"], int)
        or isinstance(payload["pid"], bool)
    ):
        raise ValueError("pane process identity has invalid field types")
    if (
        payload["pid"] <= 0
        or not SESSION_ID_PATTERN.fullmatch(payload["session"])
        or not SESSION_ID_PATTERN.fullmatch(payload["agent"])
        or not str(payload["pane"]).isdigit()
        or not str(payload["start"]).isdigit()
        or not str(payload["tty"]).isdigit()
        or int(str(payload["tty"])) <= 0
        or not re.fullmatch(r"[0-9a-fA-F-]{16,64}", str(payload["boot"]))
        or not re.fullmatch(r"[0-9a-f]+:[0-9a-f]+", str(payload["exe"]))
        or not re.fullmatch(r"[0-9a-f]{16}", str(payload["exe_path"]))
    ):
        raise ValueError("pane process identity contains invalid values")
    return payload


def _parse_herdr_process_identity(value: str) -> dict[str, str | int]:
    if not value.startswith(HERDR_PROCESS_IDENTITY_PREFIX):
        raise ValueError("Herdr process identity has an unsupported format")
    try:
        payload = json.loads(value.removeprefix(HERDR_PROCESS_IDENTITY_PREFIX))
    except json.JSONDecodeError as exc:
        raise ValueError("Herdr process identity is malformed") from exc
    if not isinstance(payload, dict) or set(payload) != HERDR_PROCESS_IDENTITY_FIELDS:
        raise ValueError("Herdr process identity is incomplete")
    string_fields = HERDR_PROCESS_IDENTITY_FIELDS - {"pid"}
    if (
        not all(isinstance(payload[field], str) for field in string_fields)
        or not isinstance(payload["pid"], int)
        or isinstance(payload["pid"], bool)
    ):
        raise ValueError("Herdr process identity has invalid field types")
    if (
        payload["pid"] <= 0
        or not SESSION_ID_PATTERN.fullmatch(str(payload["agent"]))
        or not HERDR_TERMINAL_ID_PATTERN.fullmatch(str(payload["terminal"]))
        or not str(payload["start"]).isdigit()
        or not str(payload["tty"]).lstrip("-").isdigit()
        or int(str(payload["tty"])) <= 0
        or not re.fullmatch(r"[0-9a-fA-F-]{16,64}", str(payload["boot"]))
        or not re.fullmatch(r"[0-9a-f]+:[0-9a-f]+", str(payload["exe"]))
        or not re.fullmatch(r"[0-9a-f]{16}", str(payload["exe_path"]))
    ):
        raise ValueError("Herdr process identity contains invalid values")
    return payload


def _native_adapter(kind: str) -> str:
    return {
        "claude_session": "claude",
        "codex_session": "codex",
        "zellij_pane": "",
        "hermes_session": "hermes",
        "headless_run": "headless",
    }.get(kind, "")


def _validated_source_map(kind: str, raw_source: Any) -> dict[str, str]:
    if kind not in SOURCE_FIELDS:
        raise ValueError("unsupported bridge source kind")
    if (
        not isinstance(raw_source, dict)
        or set(raw_source) - SOURCE_FIELDS[kind]
    ):
        raise ValueError("invalid bridge source")
    if not all(
        isinstance(key, str) and isinstance(value, str)
        for key, value in raw_source.items()
    ):
        raise ValueError("bridge source values must be strings")
    source = {key: value for key, value in raw_source.items() if value}
    if any(len(value) > MAX_SOURCE_VALUE for value in source.values()):
        raise ValueError("bridge source value is too large")
    supplied_version = source.get("binding_version", "")
    if supplied_version and supplied_version not in {
        str(version) for version in SUPPORTED_BINDING_VERSIONS
    }:
        raise ValueError("unsupported bridge binding version")
    return source


def _source_cwd_identity(source: dict[str, str]) -> dict[str, str]:
    identity = {
        "cwd_realpath": source.get("cwd_realpath", ""),
        "cwd_device": source.get("cwd_device", ""),
        "cwd_inode": source.get("cwd_inode", ""),
        "cwd_owner_uid": source.get("cwd_owner_uid", ""),
    }
    if not any(identity.values()):
        return identity
    if not all(identity.values()):
        raise ValueError("working directory identity is incomplete")
    if (
        not Path(identity["cwd_realpath"]).is_absolute()
        or "\x00" in identity["cwd_realpath"]
        or not identity["cwd_device"].isdigit()
        or not identity["cwd_inode"].isdigit()
        or not identity["cwd_owner_uid"].isdigit()
        or int(identity["cwd_inode"]) <= 0
    ):
        raise ValueError("working directory identity is invalid")
    return identity


def _source_pane_fields(
    source: dict[str, str],
) -> tuple[str, str, str, str, bool]:
    aliases = (
        (
            "zellij_session",
            "session_name",
            "contradictory Zellij session aliases",
        ),
        (
            "zellij_pane_id",
            "pane_id",
            "contradictory Zellij pane aliases",
        ),
        (
            "run_id",
            "queue_id",
            "contradictory headless run IDs",
        ),
    )
    for primary, legacy, error in aliases:
        if (
            source.get(primary)
            and source.get(legacy)
            and source[primary] != source[legacy]
        ):
            raise ValueError(error)
    zellij_session = (
        source.get("zellij_session")
        or source.get("session_name", "")
    )
    zellij_pane = (
        source.get("zellij_pane_id")
        or source.get("pane_id", "")
    )
    pane_agent = source.get("pane_agent", "")
    process_identity = source.get("process_identity", "")
    has_pane_data = any(
        (
            zellij_session,
            zellij_pane,
            source.get("pane_command_hash", ""),
        )
    )
    return (
        zellij_session,
        zellij_pane,
        pane_agent,
        process_identity,
        has_pane_data,
    )


def _source_herdr_fields(source: dict[str, str]) -> tuple[dict[str, str], bool]:
    fields = {
        field: source.get(field, "")
        for field in HERDR_ENDPOINT_FIELDS
    }
    return fields, any(fields.values())


def _validate_bound_process_identity(
    process_identity: str,
    *,
    pane_agent: str,
    zellij_session: str,
    zellij_pane: str,
) -> None:
    if not process_identity:
        return
    identity = _parse_process_identity(process_identity)
    if str(identity["agent"]) != pane_agent:
        raise ValueError("pane process identity contradicts the bound adapter")
    if str(identity["session"]) != zellij_session:
        raise ValueError(
            "pane process identity contradicts the Zellij session"
        )
    if str(identity["pane"]) != zellij_pane.removeprefix("terminal_"):
        raise ValueError("pane process identity contradicts the Zellij pane")


def _validate_bound_herdr_process_identity(
    process_identity: str,
    *,
    pane_agent: str,
    terminal_id: str,
) -> None:
    if not process_identity:
        return
    identity = _parse_herdr_process_identity(process_identity)
    if str(identity["agent"]) != pane_agent:
        raise ValueError("Herdr process identity contradicts the bound adapter")
    if str(identity["terminal"]) != terminal_id:
        raise ValueError("Herdr process identity contradicts the bound terminal")


def _binding_delivery_contract(
    kind: str,
    *,
    session_id: str,
    run_id: str,
    cwd: str,
    zellij_session: str,
    zellij_pane: str,
    pane_agent: str,
    process_identity: str,
    has_pane_data: bool,
    herdr: dict[str, str],
    has_herdr_data: bool,
    allow_legacy: bool,
) -> tuple[str, str, str, str]:
    adapter = _native_adapter(kind)
    state = "verified"
    if kind in {"claude_session", "codex_session"}:
        if not session_id:
            raise ValueError(f"{adapter} binding requires a session ID")
        if not cwd:
            raise ValueError(
                f"{adapter} binding requires a working directory"
            )
        if has_pane_data and has_herdr_data:
            raise ValueError("native binding cannot target both Zellij and Herdr")
        endpoint = (
            "herdr_agent"
            if has_herdr_data
            else "zellij_pane"
            if has_pane_data
            else "detached_native"
        )
        if has_herdr_data:
            required = (
                "herdr_session",
                "herdr_socket_path",
                "herdr_terminal_id",
                "herdr_pane_id",
                "herdr_agent_name",
                "herdr_agent_session_source",
                "herdr_agent_session_kind",
                "herdr_agent_session_value",
                "herdr_protocol",
            )
            if not all(herdr.get(field) for field in required):
                raise ValueError("native Herdr binding is incomplete")
            if pane_agent != adapter:
                raise ValueError(
                    f"captured Herdr agent is "
                    f"{pane_agent or 'unknown'}, not {adapter}"
                )
            if not Path(herdr["herdr_socket_path"]).is_absolute():
                raise ValueError("Herdr socket path must be absolute")
            if not SESSION_ID_PATTERN.fullmatch(herdr["herdr_session"]):
                raise ValueError("Herdr session name is invalid")
            if not SESSION_ID_PATTERN.fullmatch(herdr["herdr_pane_id"]):
                raise ValueError("Herdr pane ID is invalid")
            if not HERDR_TERMINAL_ID_PATTERN.fullmatch(
                herdr["herdr_terminal_id"]
            ):
                raise ValueError("Herdr terminal ID is invalid")
            if not HERDR_AGENT_NAME_PATTERN.fullmatch(
                herdr["herdr_agent_name"]
            ):
                raise ValueError("Herdr agent name is invalid")
            for field in (
                "herdr_agent_session_source",
                "herdr_agent_session_kind",
                "herdr_agent_session_value",
            ):
                if not SESSION_ID_PATTERN.fullmatch(herdr[field]):
                    raise ValueError("Herdr native session reference is invalid")
            if herdr["herdr_agent_session_value"] != session_id:
                raise ValueError(
                    "Herdr native session reference contradicts the source session"
                )
            if herdr["herdr_protocol"] != str(HERDR_PROTOCOL_VERSION):
                raise ValueError("unsupported Herdr protocol")
            if not process_identity:
                raise ValueError(
                    "native Herdr binding requires an exact process identity"
                )
            return adapter, state, endpoint, "native_required"
        if has_pane_data:
            if not all((zellij_session, zellij_pane, pane_agent)):
                raise ValueError("native pane binding is incomplete")
            if pane_agent != adapter:
                raise ValueError(
                    f"captured pane runs "
                    f"{pane_agent or 'an unknown agent'}, not {adapter}"
                )
            if not process_identity:
                if not allow_legacy:
                    raise ValueError(
                        "native pane binding requires an exact process identity"
                    )
                state = "rebind_required"
        return adapter, state, endpoint, "native_required"
    if kind == "zellij_pane":
        if not all((zellij_session, zellij_pane, pane_agent)):
            raise ValueError(
                "Zellij binding requires session, pane, and agent identity"
            )
        if pane_agent not in {
            "claude",
            "codex",
            "gemini",
            "hermes",
            "pi",
        }:
            raise ValueError("Zellij pane agent is unsupported")
        if not process_identity:
            if not allow_legacy:
                raise ValueError(
                    "Zellij binding requires an exact process identity"
                )
            state = "rebind_required"
        return pane_agent, state, "zellij_pane", "native_required"
    if kind == "hermes_session":
        if not (session_id or run_id):
            raise ValueError("Hermes binding requires a session or run ID")
    elif not run_id:
        raise ValueError("headless binding requires a run ID")
    return adapter, state, "hermes_continuation", "explicit_headless"


def _validate_declared_binding_contract(
    source: dict[str, str],
    *,
    state: str,
    endpoint: str,
    policy: str,
    process_identity: str,
) -> None:
    if source.get("binding_version", "") != str(BINDING_VERSION):
        return
    claims = (
        ("binding_state", state, "binding state"),
        ("endpoint_kind", endpoint, "binding endpoint"),
        ("delivery_policy", policy, "binding policy"),
    )
    for field, actual, label in claims:
        if source.get(field) and source[field] != actual:
            raise ValueError(f"{label} contradicts source identity")
    if endpoint in {"zellij_pane", "herdr_agent"} and not process_identity:
        raise ValueError(
            "verified live endpoint requires a process identity"
        )


def _canonical_source(
    kind: str,
    raw_source: Any,
    *,
    allow_legacy: bool = False,
) -> tuple[dict[str, str], SourceBinding]:
    source = _validated_source_map(kind, raw_source)
    session_id = source.get("session_id", "")
    run_id = source.get("run_id") or source.get("queue_id", "")
    cwd = source.get("cwd", "")
    cwd_identity = _source_cwd_identity(source)
    (
        zellij_session,
        zellij_pane,
        pane_agent,
        process_identity,
        has_pane_data,
    ) = _source_pane_fields(source)
    herdr, has_herdr_data = _source_herdr_fields(source)
    if (
        not has_pane_data
        and not has_herdr_data
        and (pane_agent or process_identity)
    ):
        raise ValueError(
            "live process metadata requires a Zellij or Herdr endpoint"
        )
    if session_id and not SESSION_ID_PATTERN.fullmatch(session_id):
        raise ValueError("native session ID is invalid or option-like")
    if run_id and not SESSION_ID_PATTERN.fullmatch(run_id):
        raise ValueError("run ID is invalid or option-like")
    if has_pane_data:
        _validate_bound_process_identity(
            process_identity,
            pane_agent=pane_agent,
            zellij_session=zellij_session,
            zellij_pane=zellij_pane,
        )
    if has_herdr_data:
        _validate_bound_herdr_process_identity(
            process_identity,
            pane_agent=pane_agent,
            terminal_id=herdr["herdr_terminal_id"],
        )
    adapter, state, endpoint, policy = _binding_delivery_contract(
        kind,
        session_id=session_id,
        run_id=run_id,
        cwd=cwd,
        zellij_session=zellij_session,
        zellij_pane=zellij_pane,
        pane_agent=pane_agent,
        process_identity=process_identity,
        has_pane_data=has_pane_data,
        herdr=herdr,
        has_herdr_data=has_herdr_data,
        allow_legacy=allow_legacy,
    )
    _validate_declared_binding_contract(
        source,
        state=state,
        endpoint=endpoint,
        policy=policy,
        process_identity=process_identity,
    )
    effective_version = 1 if state == "rebind_required" else BINDING_VERSION
    source.update(
        {
            "binding_version": str(effective_version),
            "binding_state": state,
            "endpoint_kind": endpoint,
            "delivery_policy": policy,
        }
    )
    if zellij_session:
        if kind == "zellij_pane":
            source["session_name"] = zellij_session
            source["pane_id"] = zellij_pane
        else:
            source["zellij_session"] = zellij_session
            source["zellij_pane_id"] = zellij_pane
    binding = SourceBinding(
        version=effective_version,
        source_kind=kind,
        state=state,
        endpoint_kind=endpoint,
        delivery_policy=policy,
        adapter=adapter,
        session_id=session_id,
        run_id=run_id,
        cwd=cwd,
        cwd_realpath=cwd_identity["cwd_realpath"],
        cwd_device=cwd_identity["cwd_device"],
        cwd_inode=cwd_identity["cwd_inode"],
        cwd_owner_uid=cwd_identity["cwd_owner_uid"],
        zellij_session=zellij_session,
        zellij_pane_id=zellij_pane,
        herdr_session=herdr["herdr_session"],
        herdr_socket_path=herdr["herdr_socket_path"],
        herdr_terminal_id=herdr["herdr_terminal_id"],
        herdr_pane_id=herdr["herdr_pane_id"],
        herdr_agent_name=herdr["herdr_agent_name"],
        herdr_agent_session_source=herdr["herdr_agent_session_source"],
        herdr_agent_session_kind=herdr["herdr_agent_session_kind"],
        herdr_agent_session_value=herdr["herdr_agent_session_value"],
        herdr_protocol=herdr["herdr_protocol"],
        pane_agent=pane_agent,
        process_identity=process_identity,
    )
    return source, binding


def source_binding(bridge: Bridge) -> SourceBinding:
    if bridge.binding_state != "verified":
        source = bridge.source
        return SourceBinding(
            version=bridge.binding_version,
            source_kind=bridge.source_kind,
            state=bridge.binding_state,
            endpoint_kind=str(source.get("endpoint_kind") or "unknown"),
            delivery_policy=str(source.get("delivery_policy") or "native_required"),
            adapter=_native_adapter(bridge.source_kind),
            session_id=str(source.get("session_id") or ""),
            run_id=str(source.get("run_id") or source.get("queue_id") or ""),
            cwd=str(source.get("cwd") or ""),
            cwd_realpath=str(source.get("cwd_realpath") or ""),
            cwd_device=str(source.get("cwd_device") or ""),
            cwd_inode=str(source.get("cwd_inode") or ""),
            cwd_owner_uid=str(source.get("cwd_owner_uid") or ""),
            zellij_session=str(
                source.get("zellij_session") or source.get("session_name") or ""
            ),
            zellij_pane_id=str(
                source.get("zellij_pane_id") or source.get("pane_id") or ""
            ),
            herdr_session=str(source.get("herdr_session") or ""),
            herdr_socket_path=str(source.get("herdr_socket_path") or ""),
            herdr_terminal_id=str(source.get("herdr_terminal_id") or ""),
            herdr_pane_id=str(source.get("herdr_pane_id") or ""),
            herdr_agent_name=str(source.get("herdr_agent_name") or ""),
            herdr_agent_session_source=str(
                source.get("herdr_agent_session_source") or ""
            ),
            herdr_agent_session_kind=str(
                source.get("herdr_agent_session_kind") or ""
            ),
            herdr_agent_session_value=str(
                source.get("herdr_agent_session_value") or ""
            ),
            herdr_protocol=str(source.get("herdr_protocol") or ""),
            pane_agent=str(source.get("pane_agent") or ""),
            process_identity=str(source.get("process_identity") or ""),
        )
    _, binding = _canonical_source(
        bridge.source_kind, bridge.source, allow_legacy=True
    )
    return SourceBinding(
        **{
            **binding.__dict__,
            "version": bridge.binding_version,
            "state": bridge.binding_state,
        }
    )


def endpoint_identity_key(binding: SourceBinding) -> str:
    if binding.endpoint_kind == "zellij_pane":
        identity = (
            "zellij_pane",
            binding.zellij_session,
            binding.zellij_pane_id.removeprefix("terminal_"),
        )
    elif binding.endpoint_kind == "herdr_agent":
        identity = (
            "herdr_agent",
            binding.herdr_socket_path,
            binding.herdr_agent_name,
        )
    elif binding.endpoint_kind == "detached_native":
        identity = (
            "detached_native",
            binding.source_kind,
            binding.session_id,
        )
    elif binding.endpoint_kind == "hermes_continuation":
        identity = (
            "hermes_continuation",
            binding.source_kind,
            binding.session_id or binding.run_id,
        )
    else:
        raise ValueError("binding endpoint cannot be identified")
    if any(not value for value in identity):
        raise ValueError("binding endpoint identity is incomplete")
    canonical = "\0".join(identity).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def require_deliverable_binding(bridge: Bridge, endpoint: str | None = None) -> SourceBinding:
    binding = source_binding(bridge)
    if binding.state != "verified":
        raise _binding_error(
            "binding_rebind_required",
            "The binding cannot safely receive a continuation.",
            binding_id=bridge.bridge_id,
        )
    if endpoint and binding.endpoint_kind != endpoint:
        raise _binding_error(
            "endpoint_mismatch",
            "The requested delivery path does not match the bound endpoint.",
            binding_id=bridge.bridge_id,
        )
    return binding


def delivery_attempt_id(
    bridge_id: str,
    event_ids: tuple[str, ...] | list[str],
    binding_generation: int,
) -> str:
    if (
        not bridge_id.startswith("brg_")
        or not event_ids
        or any(not value for value in event_ids)
        or binding_generation < 1
    ):
        raise ValueError("bridge, event IDs, and binding generation are required")
    material = json.dumps(
        {
            "bridge": bridge_id,
            "events": list(event_ids),
            "generation": binding_generation,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return "att_" + hashlib.sha256(material.encode()).hexdigest()[:24]


class Store:
    def __init__(self, path: Path = DB_PATH):
        candidate = Path(path).expanduser()
        if not candidate.is_absolute():
            raise security.StatePathError("Tether database path must be absolute")
        security.secure_state_directory(candidate.parent, create=True)
        if candidate.exists() or candidate.is_symlink():
            info = candidate.lstat()
            if (
                stat.S_ISLNK(info.st_mode)
                or not stat.S_ISREG(info.st_mode)
                or info.st_uid != os.geteuid()
                or info.st_nlink != 1
            ):
                raise security.StatePathError(
                    "existing Tether database must be an owner-owned regular file"
                )
            # Older Tether releases relied on the caller's umask and commonly
            # created 0644 databases. Preserve that migration path while still
            # rejecting links, foreign owners, and multiply-linked files.
            security.secure_state_file(candidate)
        else:
            security.secure_state_file(candidate, create=True)
        self.path = candidate
        db = sqlite3.connect(candidate, timeout=30)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA busy_timeout=30000")
        try:
            observed_version = int(db.execute("PRAGMA user_version").fetchone()[0])
            if observed_version > SCHEMA_VERSION:
                raise RuntimeError(
                    f"Tether database schema {observed_version} is newer than this runtime"
                )
            self._execute_with_lock_retry(db, "PRAGMA journal_mode=WAL")
            self._execute_with_lock_retry(db, "BEGIN IMMEDIATE")
            locked_version = int(db.execute("PRAGMA user_version").fetchone()[0])
            if locked_version > SCHEMA_VERSION:
                raise RuntimeError(
                    f"Tether database schema {locked_version} is newer than this runtime"
                )
            self._migrate(db)
            db.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
            db.commit()
            self._secure_sqlite_files()
        except BaseException:
            db.rollback()
            raise
        finally:
            db.close()
        self.recover_delivery_attempts()

    def _secure_sqlite_files(self) -> None:
        for candidate in (
            self.path,
            Path(f"{self.path}-wal"),
            Path(f"{self.path}-shm"),
        ):
            if candidate.exists() or candidate.is_symlink():
                try:
                    security.secure_state_file(candidate)
                except security.StatePathError:
                    if candidate.exists() or candidate.is_symlink():
                        raise

    @staticmethod
    def _execute_with_lock_retry(
        db: sqlite3.Connection,
        statement: str,
        *,
        timeout_seconds: float = 30,
    ) -> sqlite3.Cursor:
        deadline = time.monotonic() + timeout_seconds
        delay = 0.01
        while True:
            try:
                return db.execute(statement)
            except sqlite3.OperationalError as exc:
                if "locked" not in str(exc).lower() or time.monotonic() >= deadline:
                    raise
                time.sleep(delay)
                delay = min(delay * 2, 0.25)

    @classmethod
    def _migrate(cls, db: sqlite3.Connection) -> None:
        statements = (
            """
            CREATE TABLE IF NOT EXISTS bridges (
              bridge_id TEXT PRIMARY KEY, source_kind TEXT NOT NULL,
              source_json TEXT NOT NULL, owner_user_id TEXT NOT NULL,
              team_id TEXT NOT NULL DEFAULT '', channel_id TEXT NOT NULL,
              thread_ts TEXT, idempotency_key TEXT NOT NULL UNIQUE,
              status TEXT NOT NULL DEFAULT 'pending',
              binding_version INTEGER NOT NULL DEFAULT 1,
              binding_generation INTEGER NOT NULL DEFAULT 1,
              binding_state TEXT NOT NULL DEFAULT 'legacy',
              binding_error_code TEXT,
              endpoint_key TEXT NOT NULL DEFAULT '',
              created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
              updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """,
            """
            CREATE UNIQUE INDEX IF NOT EXISTS bridge_thread
            ON bridges(team_id, channel_id, thread_ts)
            WHERE thread_ts IS NOT NULL AND status = 'active'
            """,
            """
            CREATE TABLE IF NOT EXISTS bridge_events (
              event_id TEXT PRIMARY KEY, bridge_id TEXT NOT NULL,
              state TEXT NOT NULL, error TEXT,
              payload_json TEXT NOT NULL DEFAULT '{}',
              attempt_id TEXT,
              binding_generation INTEGER,
              created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
              updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS bridge_replies (
              reply_key TEXT PRIMARY KEY, bridge_id TEXT NOT NULL,
              message_ts TEXT, text_hash TEXT,
              payload_text TEXT, client_msg_id TEXT,
              lease_id TEXT, lease_owner TEXT, lease_expires_at TEXT,
              retry_count INTEGER NOT NULL DEFAULT 0,
              state TEXT NOT NULL DEFAULT 'reserved', error TEXT,
              created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
              updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS bridge_roots (
              bridge_id TEXT PRIMARY KEY,
              state TEXT NOT NULL DEFAULT 'reserved',
              payload_text TEXT NOT NULL,
              client_msg_id TEXT NOT NULL,
              requested_thread_ts TEXT,
              thread_ts TEXT,
              lease_id TEXT, lease_owner TEXT, lease_expires_at TEXT,
              retry_count INTEGER NOT NULL DEFAULT 0,
              error TEXT,
              staged_path TEXT,
              staged_size INTEGER,
              staged_sha256 TEXT,
              staged_owner_uid INTEGER,
              staged_device INTEGER,
              staged_inode INTEGER,
              staged_source_device INTEGER,
              staged_source_inode INTEGER,
              upload_filename TEXT,
              slack_file_id TEXT,
              upload_phase TEXT NOT NULL DEFAULT 'none',
              file_message_ts TEXT,
              created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
              updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS slack_messages (
              idempotency_key TEXT PRIMARY KEY,
              team_id TEXT NOT NULL, channel_id TEXT NOT NULL,
              thread_ts TEXT NOT NULL, payload_text TEXT NOT NULL,
              payload_options_json TEXT NOT NULL DEFAULT '{}',
              operation TEXT NOT NULL DEFAULT 'post',
              target_message_ts TEXT NOT NULL DEFAULT '',
              ingress_event_id TEXT, egress_group_id TEXT,
              egress_chunk_index INTEGER, egress_chunk_count INTEGER,
              text_hash TEXT NOT NULL, client_msg_id TEXT NOT NULL,
              message_ts TEXT,
              state TEXT NOT NULL DEFAULT 'pending',
              lease_id TEXT, lease_owner TEXT, lease_expires_at TEXT,
              retry_count INTEGER NOT NULL DEFAULT 0,
              error TEXT,
              created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
              updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS slack_reconciliations (
              reconciliation_key TEXT PRIMARY KEY,
              team_id TEXT NOT NULL, method TEXT NOT NULL,
              channel_id TEXT NOT NULL, thread_ts TEXT NOT NULL DEFAULT '',
              target_kind TEXT NOT NULL, target_id TEXT NOT NULL,
              oldest_ts TEXT NOT NULL,
              next_cursor TEXT NOT NULL DEFAULT '',
              seen_cursors_json TEXT NOT NULL DEFAULT '[]',
              pages_seen INTEGER NOT NULL DEFAULT 0,
              state TEXT NOT NULL DEFAULT 'pending',
              result_ts TEXT, error TEXT,
              next_attempt_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
              created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
              updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
              CHECK (pages_seen >= 0)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS slack_reconciliation_limits (
              team_id TEXT NOT NULL, method TEXT NOT NULL,
              next_allowed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
              updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
              PRIMARY KEY (team_id, method)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS thread_participation (
              team_id TEXT NOT NULL DEFAULT '', channel_id TEXT NOT NULL,
              thread_ts TEXT NOT NULL,
              updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
              PRIMARY KEY (team_id, channel_id, thread_ts)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS slack_reply_poll_state (
              team_id TEXT NOT NULL DEFAULT '', channel_id TEXT NOT NULL,
              thread_ts TEXT NOT NULL,
              next_cursor TEXT,
              seen_cursors_json TEXT NOT NULL DEFAULT '[]',
              pages_seen INTEGER NOT NULL DEFAULT 0,
              page_oldest TEXT NOT NULL,
              bot_user_ids_json TEXT NOT NULL DEFAULT '[]',
              root_bridge_id TEXT NOT NULL DEFAULT '',
              pending_messages_json TEXT NOT NULL DEFAULT '[]',
              updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
              PRIMARY KEY (team_id, channel_id, thread_ts),
              CHECK (pages_seen >= 0)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS slack_reply_poll_rotation (
              team_id TEXT PRIMARY KEY,
              last_channel_id TEXT NOT NULL,
              last_thread_ts TEXT NOT NULL,
              updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS slack_reply_poll_scheduler (
              scheduler_id INTEGER PRIMARY KEY CHECK (scheduler_id = 1),
              last_team_id TEXT NOT NULL,
              updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS thread_ingress (
              event_id TEXT PRIMARY KEY,
              team_id TEXT NOT NULL DEFAULT '', channel_id TEXT NOT NULL,
              thread_ts TEXT NOT NULL,
              route_action TEXT NOT NULL DEFAULT 'hermes',
              writer_id TEXT NOT NULL DEFAULT 'legacy',
              bridge_id TEXT,
              binding_generation INTEGER,
              payload_json TEXT NOT NULL DEFAULT '{}',
              fence_epoch INTEGER NOT NULL DEFAULT 1,
              egress_sealed INTEGER NOT NULL DEFAULT 0,
              state TEXT NOT NULL DEFAULT 'completed',
              lease_id TEXT, lease_owner TEXT, lease_expires_at TEXT,
              retry_count INTEGER NOT NULL DEFAULT 0,
              error_code TEXT,
              created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
              updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS bridge_attempts (
              attempt_id TEXT PRIMARY KEY,
              reply_key TEXT NOT NULL UNIQUE,
              bridge_id TEXT NOT NULL,
              binding_generation INTEGER NOT NULL,
              delivery_kind TEXT NOT NULL,
              state TEXT NOT NULL,
              ack_kind TEXT,
              message_ts TEXT,
              error_code TEXT,
              created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
              updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
              submitted_at TEXT,
              acknowledged_at TEXT
            )
            """,
        )
        for statement in statements:
            db.execute(statement)

        cls._add_missing_columns(
            db,
            "slack_reply_poll_state",
            {
                "bot_user_ids_json": "TEXT NOT NULL DEFAULT '[]'",
                "root_bridge_id": "TEXT NOT NULL DEFAULT ''",
                "pending_messages_json": "TEXT NOT NULL DEFAULT '[]'",
            },
        )
        cls._add_missing_columns(
            db,
            "slack_messages",
            {
                "payload_options_json": "TEXT NOT NULL DEFAULT '{}'",
                "operation": "TEXT NOT NULL DEFAULT 'post'",
                "target_message_ts": "TEXT NOT NULL DEFAULT ''",
                "ingress_event_id": "TEXT",
                "egress_group_id": "TEXT",
                "egress_chunk_index": "INTEGER",
                "egress_chunk_count": "INTEGER",
            },
        )
        cls._add_missing_columns(
            db,
            "bridge_events",
            {
                "payload_json": "TEXT NOT NULL DEFAULT '{}'",
                "updated_at": "TEXT",
                "attempt_id": "TEXT",
                "binding_generation": "INTEGER",
            },
        )
        db.execute(
            "UPDATE bridge_events SET updated_at=created_at WHERE updated_at IS NULL"
        )
        cls._add_missing_columns(
            db,
            "bridges",
            {
                "binding_version": "INTEGER NOT NULL DEFAULT 1",
                "binding_generation": "INTEGER NOT NULL DEFAULT 1",
                "binding_state": "TEXT NOT NULL DEFAULT 'legacy'",
                "binding_error_code": "TEXT",
                "endpoint_key": "TEXT NOT NULL DEFAULT ''",
                "updated_at": "TEXT",
            },
        )
        db.execute(
            "UPDATE bridges SET updated_at=created_at WHERE updated_at IS NULL"
        )
        legacy_ingress = db.execute(
            """
            SELECT 1 FROM sqlite_master
            WHERE type='table' AND name='bridge_ingress'
            """
        ).fetchone()
        if legacy_ingress is not None:
            db.execute(
                """
                INSERT OR IGNORE INTO bridge_events(
                  event_id,bridge_id,state,error,payload_json,
                  binding_generation,created_at,updated_at
                )
                SELECT ingress.event_id,ingress.bridge_id,'failed',
                       'legacy_ingress_tombstone','{}',
                       bridges.binding_generation,
                       ingress.created_at,ingress.created_at
                FROM bridge_ingress AS ingress
                JOIN bridges ON bridges.bridge_id=ingress.bridge_id
                """
            )
            db.execute("DROP TABLE bridge_ingress")
        db.execute("DROP TABLE IF EXISTS thread_routes")
        cls._add_missing_columns(
            db,
            "thread_ingress",
            {
                "state": "TEXT NOT NULL DEFAULT 'completed'",
                "lease_id": "TEXT",
                "lease_owner": "TEXT",
                "lease_expires_at": "TEXT",
                "retry_count": "INTEGER NOT NULL DEFAULT 0",
                "error_code": "TEXT",
                "updated_at": "TEXT",
                "route_action": "TEXT NOT NULL DEFAULT 'hermes'",
                "writer_id": "TEXT NOT NULL DEFAULT 'legacy'",
                "bridge_id": "TEXT",
                "binding_generation": "INTEGER",
                "payload_json": "TEXT NOT NULL DEFAULT '{}'",
                "fence_epoch": "INTEGER NOT NULL DEFAULT 1",
                "egress_sealed": "INTEGER NOT NULL DEFAULT 0",
            },
        )
        db.execute(
            "UPDATE thread_ingress SET updated_at=created_at WHERE updated_at IS NULL"
        )
        db.execute(
            """
            CREATE INDEX IF NOT EXISTS thread_ingress_recovery
            ON thread_ingress(state,lease_expires_at)
            """
        )
        cls._add_missing_columns(
            db,
            "bridge_replies",
            {
                "text_hash": "TEXT",
                "payload_text": "TEXT",
                "client_msg_id": "TEXT",
                "lease_id": "TEXT",
                "lease_owner": "TEXT",
                "lease_expires_at": "TEXT",
                "retry_count": "INTEGER NOT NULL DEFAULT 0",
                "state": "TEXT NOT NULL DEFAULT 'reserved'",
                "error": "TEXT",
                "updated_at": "TEXT",
            },
        )
        db.execute(
            "UPDATE bridge_replies SET updated_at=created_at WHERE updated_at IS NULL"
        )
        db.execute(
            """
            CREATE INDEX IF NOT EXISTS slack_message_recovery
            ON slack_messages(state,lease_expires_at,created_at)
            """
        )
        db.execute(
            """
            CREATE INDEX IF NOT EXISTS slack_reconciliation_recovery
            ON slack_reconciliations(
              state,next_attempt_at,team_id,method,updated_at
            )
            """
        )
        db.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS slack_message_egress_chunk
            ON slack_messages(egress_group_id,egress_chunk_index)
            WHERE egress_group_id IS NOT NULL
            """
        )
        cls._add_missing_columns(
            db,
            "bridge_roots",
            {
                "state": "TEXT NOT NULL DEFAULT 'reserved'",
                "payload_text": "TEXT NOT NULL DEFAULT ''",
                "client_msg_id": "TEXT NOT NULL DEFAULT ''",
                "requested_thread_ts": "TEXT",
                "thread_ts": "TEXT",
                "lease_id": "TEXT",
                "lease_owner": "TEXT",
                "lease_expires_at": "TEXT",
                "retry_count": "INTEGER NOT NULL DEFAULT 0",
                "error": "TEXT",
                "staged_path": "TEXT",
                "staged_size": "INTEGER",
                "staged_sha256": "TEXT",
                "staged_owner_uid": "INTEGER",
                "staged_device": "INTEGER",
                "staged_inode": "INTEGER",
                "staged_source_device": "INTEGER",
                "staged_source_inode": "INTEGER",
                "upload_filename": "TEXT",
                "slack_file_id": "TEXT",
                "upload_phase": "TEXT NOT NULL DEFAULT 'none'",
                "file_message_ts": "TEXT",
                "updated_at": "TEXT",
            },
        )
        db.execute(
            """
            UPDATE bridge_roots
            SET upload_phase=CASE
              WHEN staged_path IS NULL THEN 'none'
              WHEN state='complete' THEN 'completed'
              ELSE 'reserved'
            END
            WHERE upload_phase IS NULL
               OR upload_phase=''
               OR (staged_path IS NOT NULL AND upload_phase='none')
            """
        )
        db.execute(
            "UPDATE bridge_roots SET updated_at=created_at WHERE updated_at IS NULL"
        )
        db.execute(
            """
            CREATE INDEX IF NOT EXISTS bridge_event_attempt
            ON bridge_events(attempt_id)
            WHERE attempt_id IS NOT NULL
            """
        )
        db.execute("DROP INDEX IF EXISTS bridge_one_open_attempt")
        db.execute(
            """
            CREATE UNIQUE INDEX bridge_one_open_attempt
            ON bridge_attempts(bridge_id)
            WHERE state IN (
              'prepared','submitting','uncertain','awaiting_ack','replying'
            )
            """
        )
        db.execute(
            """
            UPDATE bridge_events
            SET state='failed',error='orphaned legacy delivery attempt',
                updated_at=CURRENT_TIMESTAMP
            WHERE state IN (
              'prepared','submitted','submitting','uncertain','awaiting_ack'
            )
              AND (
                attempt_id IS NULL OR NOT EXISTS (
                  SELECT 1 FROM bridge_attempts
                  WHERE bridge_attempts.attempt_id=bridge_events.attempt_id
                )
              )
            """
        )
        cls._backfill_bindings(db)
        duplicate_keys = db.execute(
            """
            SELECT endpoint_key FROM bridges
            WHERE endpoint_key!='' AND status IN ('pending','active')
            GROUP BY endpoint_key HAVING count(*) > 1
            """
        ).fetchall()
        for duplicate in duplicate_keys:
            rows = db.execute(
                """
                SELECT bridge_id FROM bridges
                WHERE endpoint_key=? AND status IN ('pending','active')
                ORDER BY updated_at DESC,created_at DESC,rowid DESC
                """,
                (duplicate["endpoint_key"],),
            ).fetchall()
            for stale in rows[1:]:
                db.execute(
                    """
                    UPDATE bridges
                    SET status='closed',binding_state='rebind_required',
                        binding_error_code='endpoint_conflict_migrated',
                        updated_at=CURRENT_TIMESTAMP
                    WHERE bridge_id=?
                    """,
                    (stale["bridge_id"],),
                )
        db.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS bridge_endpoint_owner
            ON bridges(endpoint_key)
            WHERE endpoint_key!='' AND status IN ('pending','active')
            """
        )

    @staticmethod
    def _add_missing_columns(
        db: sqlite3.Connection,
        table: str,
        columns: dict[str, str],
    ) -> None:
        present = {str(row[1]) for row in db.execute(f"PRAGMA table_info({table})")}
        for name, declaration in columns.items():
            if name not in present:
                db.execute(f"ALTER TABLE {table} ADD COLUMN {name} {declaration}")

    @staticmethod
    def _backfill_bindings(db: sqlite3.Connection) -> None:
        rows = db.execute(
            """
            SELECT bridge_id,source_kind,source_json,binding_version,binding_state
            FROM bridges
            """
        ).fetchall()
        for row in rows:
            try:
                raw_source = json.loads(row["source_json"])
                claimed_v2 = (
                    int(row["binding_version"]) == BINDING_VERSION
                    and str(row["binding_state"]) == "verified"
                )
                _, binding = _canonical_source(
                    str(row["source_kind"]),
                    raw_source,
                    allow_legacy=not claimed_v2,
                )
                version = binding.version
                state = binding.state
                endpoint_key = endpoint_identity_key(binding)
                error_code = (
                    "process_identity_missing"
                    if state == "rebind_required" else None
                )
            except (ValueError, TypeError, json.JSONDecodeError):
                version = 1
                state = "rebind_required"
                endpoint_key = ""
                error_code = "binding_invalid"
            db.execute(
                """
                UPDATE bridges
                SET binding_version=?,binding_state=?,binding_error_code=?,
                    endpoint_key=?
                WHERE bridge_id=?
                """,
                (version, state, error_code, endpoint_key, row["bridge_id"]),
            )

    @contextlib.contextmanager
    def connect(self):
        security.secure_state_file(self.path)
        db = sqlite3.connect(self.path, timeout=30)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA busy_timeout=30000")
        db.execute("PRAGMA journal_mode=WAL")
        self._secure_sqlite_files()
        try:
            yield db
            db.commit()
            self._secure_sqlite_files()
        except BaseException:
            db.rollback()
            raise
        finally:
            db.close()
            self._secure_sqlite_files()

    @staticmethod
    def decode(row: sqlite3.Row | None) -> Bridge | None:
        if row is None:
            return None
        keys = set(row.keys())
        try:
            raw_source = json.loads(row["source_json"])
            source, _ = _canonical_source(
                str(row["source_kind"]), raw_source, allow_legacy=True
            )
        except (ValueError, TypeError, json.JSONDecodeError):
            raw_source = {}
            source = {
                key: value
                for key, value in (
                    raw_source.items() if isinstance(raw_source, dict) else ()
                )
                if isinstance(key, str) and isinstance(value, str)
            }
            source.update({
                "binding_version": "1",
                "binding_state": "rebind_required",
                "endpoint_kind": "unknown",
                "delivery_policy": "native_required",
            })
        binding_generation = (
            int(row["binding_generation"]) if "binding_generation" in keys else 1
        )
        binding_version = (
            int(row["binding_version"])
            if "binding_version" in keys else int(source.get("binding_version", "1"))
        )
        binding_state = (
            str(row["binding_state"])
            if "binding_state" in keys else str(source.get("binding_state", "legacy"))
        )
        binding_error = (
            str(row["binding_error_code"] or "")
            if "binding_error_code" in keys else ""
        )
        source.update({
            "binding_version": str(binding_version),
            "binding_state": binding_state,
        })
        return Bridge(
            row["bridge_id"], row["source_kind"], source,
            row["owner_user_id"], row["team_id"], row["channel_id"], row["thread_ts"],
            row["idempotency_key"], row["status"],
            binding_generation,
            binding_version,
            binding_state,
            binding_error,
        )

    @staticmethod
    def validate_source(kind: str, raw_source: Any) -> dict[str, str]:
        source, _ = _canonical_source(kind, raw_source)
        return source

    def create(self, request: BridgeRequest) -> Bridge:
        required = ("source_kind", "source", "owner_user_id", "channel_id", "idempotency_key")
        if any(not request.get(key) for key in required):
            raise ValueError("source, owner, channel, and idempotency key are required")
        kind = str(request["source_kind"])
        source, binding = _canonical_source(kind, request["source"])
        endpoint_key = endpoint_identity_key(binding)
        persisted_source = {
            key: value for key, value in source.items()
            if key not in BINDING_METADATA_FIELDS
        }
        idempotency_key = str(request["idempotency_key"])
        if len(idempotency_key) > MAX_IDEMPOTENCY_KEY:
            raise ValueError("idempotency key is too large")
        channel = str(request["channel_id"])
        owner = str(request["owner_user_id"])
        team = str(request.get("team_id") or "")
        if not CHANNEL_ID_PATTERN.fullmatch(channel) or (owner != "*" and not ID_PATTERN.fullmatch(owner)):
            raise ValueError("invalid Slack channel or owner ID")
        if team and not ID_PATTERN.fullmatch(team):
            raise ValueError("invalid Slack workspace ID")
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT * FROM bridges WHERE idempotency_key=?", (idempotency_key,)).fetchone()
            if row:
                existing = self.decode(row)
                if existing is None:
                    raise RuntimeError("stored bridge could not be decoded")
                existing_source = {
                    key: value
                    for key, value in existing.source.items()
                    if key not in BINDING_METADATA_FIELDS
                }
                requested_thread = str(request.get("thread_ts") or "")
                if (
                    existing.source_kind != kind
                    or existing_source != persisted_source
                    or existing.owner_user_id != owner
                    or existing.team_id != team
                    or existing.channel_id != channel
                    or (
                        requested_thread
                        and str(existing.thread_ts or "") != requested_thread
                    )
                ):
                    raise ValueError(
                        "idempotency key already belongs to a different binding request"
                    )
                return existing
            bridge_id = "brg_" + uuid.uuid4().hex
            try:
                db.execute(
                    """
                    INSERT INTO bridges(
                      bridge_id,source_kind,source_json,owner_user_id,team_id,
                      channel_id,thread_ts,idempotency_key,status,binding_version,
                      binding_generation,binding_state,binding_error_code,
                      endpoint_key
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        bridge_id,
                        kind,
                        json.dumps(persisted_source, separators=(",", ":")),
                        owner,
                        team,
                        channel,
                        request.get("thread_ts"),
                        idempotency_key,
                        "pending",
                        binding.version,
                        1,
                        binding.state,
                        None,
                        endpoint_key,
                    ),
                )
            except sqlite3.IntegrityError as exc:
                if "bridges.endpoint_key" in str(exc):
                    raise ValueError(
                        "native endpoint already has an active Tether binding"
                    ) from exc
                raise
            row = db.execute("SELECT * FROM bridges WHERE bridge_id=?", (bridge_id,)).fetchone()
            return self.decode(row)  # type: ignore[return-value]

    def bind(self, bridge_id: str, thread_ts: str) -> Bridge:
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute(
                """
                UPDATE bridges
                SET thread_ts=?,status='active',updated_at=CURRENT_TIMESTAMP
                WHERE bridge_id=? AND status!='closed'
                """,
                (thread_ts, bridge_id),
            )
            return self.decode(db.execute("SELECT * FROM bridges WHERE bridge_id=?", (bridge_id,)).fetchone())  # type: ignore[return-value]

    def reserve_root(
        self,
        bridge_id: str,
        payload_text: str,
        requested_thread_ts: str,
        *,
        staged_upload: Any | None = None,
        upload_filename: str = "",
    ) -> dict[str, Any]:
        safe_text = redact_text(payload_text)[:MAX_TEXT]
        if not safe_text.strip():
            raise ValueError("notification text is empty after redaction")
        client_msg_id = str(
            uuid.uuid5(uuid.NAMESPACE_URL, f"tether:root:{bridge_id}")
        )
        staged_values: tuple[Any, ...]
        if staged_upload is None:
            if upload_filename:
                raise ValueError("upload filename requires a staged upload")
            staged_values = (None,) * 8
        else:
            staged_values = (
                str(staged_upload.path),
                int(staged_upload.size),
                str(staged_upload.sha256),
                int(staged_upload.owner_uid),
                int(staged_upload.device),
                int(staged_upload.inode),
                int(staged_upload.source_device),
                int(staged_upload.source_inode),
            )
            if not upload_filename:
                raise ValueError("staged upload requires a stable filename")
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            if db.execute(
                "SELECT 1 FROM bridges WHERE bridge_id=?",
                (bridge_id,),
            ).fetchone() is None:
                raise ValueError("bridge not found")
            existing = db.execute(
                "SELECT * FROM bridge_roots WHERE bridge_id=?",
                (bridge_id,),
            ).fetchone()
            expected = (
                safe_text,
                client_msg_id,
                requested_thread_ts or None,
                *staged_values,
                upload_filename or None,
            )
            if existing is not None:
                observed = (
                    str(existing["payload_text"]),
                    str(existing["client_msg_id"]),
                    str(existing["requested_thread_ts"] or "") or None,
                    str(existing["staged_path"] or "") or None,
                    (
                        int(existing["staged_size"])
                        if existing["staged_size"] is not None
                        else None
                    ),
                    str(existing["staged_sha256"] or "") or None,
                    (
                        int(existing["staged_owner_uid"])
                        if existing["staged_owner_uid"] is not None
                        else None
                    ),
                    (
                        int(existing["staged_device"])
                        if existing["staged_device"] is not None
                        else None
                    ),
                    (
                        int(existing["staged_inode"])
                        if existing["staged_inode"] is not None
                        else None
                    ),
                    (
                        int(existing["staged_source_device"])
                        if existing["staged_source_device"] is not None
                        else None
                    ),
                    (
                        int(existing["staged_source_inode"])
                        if existing["staged_source_inode"] is not None
                        else None
                    ),
                    str(existing["upload_filename"] or "") or None,
                )
                if observed != expected:
                    raise ValueError(
                        "idempotency key already belongs to a different "
                        "Slack root payload"
                    )
                return dict(existing)
            db.execute(
                """
                INSERT INTO bridge_roots(
                  bridge_id,payload_text,client_msg_id,requested_thread_ts,
                  staged_path,staged_size,staged_sha256,staged_owner_uid,
                  staged_device,staged_inode,staged_source_device,
                  staged_source_inode,upload_filename,upload_phase
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    bridge_id,
                    safe_text,
                    client_msg_id,
                    requested_thread_ts or None,
                    *staged_values,
                    upload_filename or None,
                    "reserved" if staged_upload is not None else "none",
                ),
            )
            return dict(
                db.execute(
                    "SELECT * FROM bridge_roots WHERE bridge_id=?",
                    (bridge_id,),
                ).fetchone()
            )

    def root_record(self, bridge_id: str) -> dict[str, Any] | None:
        with self.connect() as db:
            row = db.execute(
                "SELECT * FROM bridge_roots WHERE bridge_id=?",
                (bridge_id,),
            ).fetchone()
            return dict(row) if row is not None else None

    def owns_thread_root(
        self,
        bridge_id: str,
        team_id: str,
        channel_id: str,
        thread_ts: str,
    ) -> bool:
        """Return whether this bridge durably created the Slack thread root.

        A bridge posted into an existing thread is an attachment, not the
        ambient owner of that thread. The accepted root timestamp is already
        durable local evidence, so routing a live reply does not need a
        history read merely to rediscover that fact.
        """

        if not bridge_id or not team_id or not channel_id or not thread_ts:
            return False
        with self.connect() as db:
            return (
                db.execute(
                    """
                    SELECT 1
                    FROM bridges AS bridge
                    JOIN bridge_roots AS root
                      ON root.bridge_id = bridge.bridge_id
                    WHERE bridge.bridge_id=?
                      AND bridge.team_id=?
                      AND bridge.channel_id=?
                      AND bridge.thread_ts=?
                      AND bridge.status='active'
                      AND root.requested_thread_ts IS NULL
                      AND root.thread_ts=bridge.thread_ts
                    """,
                    (bridge_id, team_id, channel_id, thread_ts),
                ).fetchone()
                is not None
            )

    def claim_root(
        self,
        bridge_id: str,
        *,
        lease_owner: str = PROCESS_EPOCH,
        lease_seconds: int = 45,
    ) -> dict[str, Any]:
        if not re.fullmatch(r"[0-9a-f]{32}", lease_owner):
            raise ValueError("invalid root delivery lease owner")
        lease_seconds = max(15, min(int(lease_seconds), 300))
        lease_id = "rot_" + uuid.uuid4().hex
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                "SELECT * FROM bridge_roots WHERE bridge_id=?",
                (bridge_id,),
            ).fetchone()
            if row is None:
                raise ValueError("Slack root outbox is unavailable")
            state = str(row["state"])
            if state == "complete":
                result = dict(row)
                result["status"] = "complete"
                return result
            if (
                state in {"delivering", "uploading"}
                and str(row["lease_owner"] or "") == lease_owner
            ):
                return {"status": "busy"}
            next_state = "uploading" if row["thread_ts"] else "delivering"
            claimed = db.execute(
                """
                UPDATE bridge_roots
                SET state=?,lease_id=?,lease_owner=?,
                    lease_expires_at=datetime('now',?),
                    retry_count=retry_count+1,error=NULL,
                    updated_at=CURRENT_TIMESTAMP
                WHERE bridge_id=? AND state!='complete'
                """,
                (
                    next_state,
                    lease_id,
                    lease_owner,
                    f"+{lease_seconds} seconds",
                    bridge_id,
                ),
            )
            if claimed.rowcount != 1:
                return {"status": "busy"}
            result = dict(
                db.execute(
                    "SELECT * FROM bridge_roots WHERE bridge_id=?",
                    (bridge_id,),
                ).fetchone()
            )
            result.update({
                "status": "claimed",
                "lease_id": lease_id,
                "previous_state": state,
            })
            return result

    def record_root_post(
        self,
        bridge_id: str,
        lease_id: str,
        thread_ts: str,
    ) -> bool:
        if not thread_ts:
            raise ValueError("Slack root timestamp is required")
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                """
                SELECT requested_thread_ts,staged_path FROM bridge_roots
                WHERE bridge_id=? AND lease_id=?
                  AND state IN ('delivering','uploading')
                """,
                (bridge_id, lease_id),
            ).fetchone()
            if row is None:
                return False
            requested = str(row["requested_thread_ts"] or "")
            if requested and requested != thread_ts:
                raise RuntimeError(
                    "Slack accepted the root in a different requested thread"
                )
            bridge = db.execute(
                "SELECT thread_ts,status FROM bridges WHERE bridge_id=?",
                (bridge_id,),
            ).fetchone()
            if bridge is None or str(bridge["status"]) == "closed":
                return False
            existing_thread = str(bridge["thread_ts"] or "")
            if existing_thread and existing_thread != thread_ts:
                raise RuntimeError("bridge root changed during delivery")
            db.execute(
                """
                UPDATE bridges
                SET thread_ts=?,status='active',updated_at=CURRENT_TIMESTAMP
                WHERE bridge_id=? AND status!='closed'
                """,
                (thread_ts, bridge_id),
            )
            has_file = bool(row["staged_path"])
            completed = db.execute(
                """
                UPDATE bridge_roots
                SET thread_ts=?,state=?,
                    lease_id=CASE WHEN ? THEN lease_id ELSE NULL END,
                    lease_owner=CASE WHEN ? THEN lease_owner ELSE NULL END,
                    lease_expires_at=CASE WHEN ? THEN lease_expires_at ELSE NULL END,
                    error=NULL,updated_at=CURRENT_TIMESTAMP
                WHERE bridge_id=? AND lease_id=?
                  AND state IN ('delivering','uploading')
                """,
                (
                    thread_ts,
                    "uploading" if has_file else "complete",
                    has_file,
                    has_file,
                    has_file,
                    bridge_id,
                    lease_id,
                ),
            )
            return completed.rowcount == 1

    def complete_root_file(
        self,
        bridge_id: str,
        lease_id: str,
        message_ts: str,
        *,
        file_id: str = "",
    ) -> bool:
        if file_id and not SLACK_FILE_ID_PATTERN.fullmatch(file_id):
            raise ValueError("invalid Slack file ID")
        with self.connect() as db:
            statement = """
                UPDATE bridge_roots
                SET state='complete',upload_phase='completed',
                    file_message_ts=?,
                    lease_id=NULL,lease_owner=NULL,lease_expires_at=NULL,
                    error=NULL,updated_at=CURRENT_TIMESTAMP
                WHERE bridge_id=? AND lease_id=? AND state='uploading'
            """
            values: tuple[Any, ...] = (message_ts, bridge_id, lease_id)
            if file_id:
                statement += " AND slack_file_id=?"
                values += (file_id,)
            completed = db.execute(statement, values)
            return completed.rowcount == 1

    def begin_root_file_allocation(
        self,
        bridge_id: str,
        lease_id: str,
    ) -> bool:
        with self.connect() as db:
            updated = db.execute(
                """
                UPDATE bridge_roots
                SET upload_phase='allocating',slack_file_id=NULL,
                    file_message_ts=NULL,error=NULL,
                    updated_at=CURRENT_TIMESTAMP
                WHERE bridge_id=? AND lease_id=? AND state='uploading'
                  AND staged_path IS NOT NULL
                  AND upload_phase IN (
                    'reserved','allocating','allocation_uncertain',
                    'allocated','uploading_bytes','bytes_uncertain'
                  )
                """,
                (bridge_id, lease_id),
            )
            return updated.rowcount == 1

    def record_root_file_allocation(
        self,
        bridge_id: str,
        lease_id: str,
        file_id: str,
    ) -> bool:
        if not SLACK_FILE_ID_PATTERN.fullmatch(file_id):
            raise ValueError("invalid Slack file ID")
        with self.connect() as db:
            updated = db.execute(
                """
                UPDATE bridge_roots
                SET slack_file_id=?,upload_phase='allocated',
                    error=NULL,updated_at=CURRENT_TIMESTAMP
                WHERE bridge_id=? AND lease_id=? AND state='uploading'
                  AND upload_phase='allocating'
                """,
                (file_id, bridge_id, lease_id),
            )
            return updated.rowcount == 1

    def set_root_file_upload_phase(
        self,
        bridge_id: str,
        lease_id: str,
        phase: str,
        *,
        expected: tuple[str, ...],
        file_id: str = "",
    ) -> bool:
        if phase not in ROOT_UPLOAD_PHASES or not expected:
            raise ValueError("invalid root upload phase transition")
        if any(item not in ROOT_UPLOAD_PHASES for item in expected):
            raise ValueError("invalid expected root upload phase")
        if file_id and not SLACK_FILE_ID_PATTERN.fullmatch(file_id):
            raise ValueError("invalid Slack file ID")
        placeholders = ",".join("?" for _ in expected)
        # Only the number of bound placeholders is dynamic; expected values stay bound.
        statement = f"UPDATE bridge_roots SET upload_phase=?,updated_at=CURRENT_TIMESTAMP WHERE bridge_id=? AND lease_id=? AND state='uploading' AND upload_phase IN ({placeholders})"  # nosec
        values: tuple[Any, ...] = (phase, bridge_id, lease_id, *expected)
        if file_id:
            statement += " AND slack_file_id=?"
            values += (file_id,)
        with self.connect() as db:
            updated = db.execute(statement, values)
            return updated.rowcount == 1

    def release_root(
        self,
        bridge_id: str,
        lease_id: str,
        error: str,
    ) -> bool:
        safe_error = security.redact_egress_text(error)[:256]
        with self.connect() as db:
            released = db.execute(
                """
                UPDATE bridge_roots
                SET state=CASE WHEN thread_ts IS NULL
                               THEN 'uncertain' ELSE 'root_posted' END,
                    lease_id=NULL,lease_owner=NULL,lease_expires_at=NULL,
                    error=?,updated_at=CURRENT_TIMESTAMP
                WHERE bridge_id=? AND lease_id=?
                  AND state IN ('delivering','uploading')
                """,
                (_safe_label(safe_error, 256), bridge_id, lease_id),
            )
            return released.rowcount == 1

    def pending_root_ids(self) -> list[str]:
        with self.connect() as db:
            return [
                str(row["bridge_id"])
                for row in db.execute(
                    """
                    SELECT bridge_id FROM bridge_roots
                    WHERE state NOT IN ('complete','cancelled')
                    ORDER BY created_at,bridge_id
                    """
                )
            ]

    def get(self, bridge_id: str) -> Bridge | None:
        with self.connect() as db:
            return self.decode(db.execute("SELECT * FROM bridges WHERE bridge_id=?", (bridge_id,)).fetchone())

    def find(self, team_id: str, channel_id: str, thread_ts: str) -> Bridge | None:
        with self.connect() as db:
            row = db.execute(
                "SELECT * FROM bridges WHERE team_id=? AND channel_id=? AND thread_ts=? AND status='active'",
                (team_id, channel_id, thread_ts),
            ).fetchone()
            if row is None and team_id:
                row = db.execute(
                    "SELECT * FROM bridges WHERE team_id='' AND channel_id=? AND thread_ts=? AND status='active'",
                    (channel_id, thread_ts),
                ).fetchone()
            return self.decode(row)

    def find_thread(
        self,
        team_id: str,
        channel_id: str,
        thread_ts: str,
    ) -> Bridge | None:
        if not team_id:
            raise ValueError("Slack workspace ID is required")
        with self.connect() as db:
            row = db.execute(
                """
                SELECT * FROM bridges
                WHERE team_id=? AND channel_id=? AND thread_ts=?
                  AND status='active'
                """,
                (team_id, channel_id, thread_ts),
            ).fetchone()
            return self.decode(row)

    def find_herdr_endpoint(
        self,
        terminal_id: str,
        agent_name: str,
        native_session_value: str,
        agent: str,
    ) -> Bridge | None:
        """Resolve one active Herdr binding without trusting plugin context as authority."""
        if not HERDR_TERMINAL_ID_PATTERN.fullmatch(terminal_id):
            raise ValueError("invalid Herdr terminal ID")
        if not HERDR_AGENT_NAME_PATTERN.fullmatch(agent_name):
            raise ValueError("invalid Herdr agent name")
        if not SESSION_ID_PATTERN.fullmatch(native_session_value):
            raise ValueError("invalid Herdr native session reference")
        if agent not in {"codex", "claude"}:
            raise ValueError("unsupported Herdr agent")
        matches: list[Bridge] = []
        with self.connect() as db:
            rows = db.execute(
                "SELECT * FROM bridges WHERE status='active' ORDER BY updated_at DESC"
            ).fetchall()
        for row in rows:
            bridge = self.decode(row)
            if bridge is None or bridge.source_kind != f"{agent}_session":
                continue
            source = bridge.source
            if (
                source.get("endpoint_kind") == "herdr_agent"
                and source.get("herdr_agent_name") == agent_name
                and source.get("herdr_agent_session_value") == native_session_value
            ):
                matches.append(bridge)
        if len(matches) > 1:
            raise ValueError("multiple active bindings match this Herdr agent")
        return matches[0] if matches else None

    def bridge_work_counts(self, bridge_id: str) -> dict[str, int]:
        with self.connect() as db:
            queued = int(
                db.execute(
                    """
                    SELECT count(*) FROM bridge_events
                    WHERE bridge_id=? AND state IN (
                      'pending','processing','prepared','submitting','awaiting_ack','replying'
                    )
                    """,
                    (bridge_id,),
                ).fetchone()[0]
            )
            uncertain = int(
                db.execute(
                    """
                    SELECT count(*) FROM bridge_attempts
                    WHERE bridge_id=? AND state='uncertain'
                    """,
                    (bridge_id,),
                ).fetchone()[0]
            ) + int(
                db.execute(
                    """
                    SELECT count(*) FROM thread_ingress
                    WHERE bridge_id=? AND state='uncertain'
                    """,
                    (bridge_id,),
                ).fetchone()[0]
            )
        return {"queued": queued, "uncertain": uncertain}

    def rebind(
        self,
        bridge_id: str,
        source_kind: str,
        source: dict[str, str],
        expected_generation: int | None = None,
    ) -> Bridge:
        validated, binding = _canonical_source(source_kind, source)
        endpoint_key = endpoint_identity_key(binding)
        persisted_source = {
            key: value for key, value in validated.items()
            if key not in BINDING_METADATA_FIELDS
        }
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            current = db.execute(
                """
                SELECT binding_generation FROM bridges
                WHERE bridge_id=? AND status='active'
                """,
                (bridge_id,),
            ).fetchone()
            if current is None:
                raise ValueError("active bridge not found")
            generation = int(current["binding_generation"])
            if expected_generation is not None and generation != expected_generation:
                raise ValueError("binding changed; reload before rebinding")
            if db.execute(
                """
                SELECT 1 FROM bridge_attempts
                WHERE bridge_id=?
                  AND state IN (
                    'prepared','submitting','uncertain','awaiting_ack','replying'
                  )
                """,
                (bridge_id,),
            ).fetchone():
                raise ValueError("binding has an active delivery attempt")
            if db.execute(
                """
                SELECT 1 FROM bridge_events
                WHERE bridge_id=? AND state IN (
                  'processing','prepared','submitting','uncertain',
                  'awaiting_ack','replying'
                )
                """,
                (bridge_id,),
            ).fetchone():
                raise ValueError("binding has queued or active delivery work")
            if db.execute(
                """
                SELECT 1 FROM thread_ingress
                WHERE bridge_id=? AND binding_generation=?
                  AND state IN ('pending','processing','dispatched','uncertain')
                """,
                (bridge_id, generation),
            ).fetchone():
                raise ValueError("binding has claimed or pending Slack ingress")
            try:
                cursor = db.execute(
                    """
                    UPDATE bridges
                    SET source_kind=?,source_json=?,binding_version=?,
                        binding_generation=binding_generation+1,binding_state=?,
                        binding_error_code=NULL,endpoint_key=?,
                        updated_at=CURRENT_TIMESTAMP
                    WHERE bridge_id=? AND status='active'
                      AND binding_generation=?
                    """,
                    (
                        source_kind,
                        json.dumps(
                            persisted_source,
                            ensure_ascii=False,
                            separators=(",", ":"),
                        ),
                        binding.version,
                        binding.state,
                        endpoint_key,
                        bridge_id,
                        generation,
                    ),
                )
            except sqlite3.IntegrityError as exc:
                if "bridges.endpoint_key" in str(exc):
                    raise ValueError(
                        "native endpoint already has an active Tether binding"
                    ) from exc
                raise
            if cursor.rowcount != 1:
                raise ValueError("active bridge not found")
            db.execute(
                """
                UPDATE bridge_events
                SET binding_generation=?,updated_at=CURRENT_TIMESTAMP
                WHERE bridge_id=? AND state='queued'
                  AND (
                    binding_generation IS NULL
                    OR binding_generation=?
                  )
                """,
                (generation + 1, bridge_id, generation),
            )
        bridge = self.get(bridge_id)
        if bridge is None:
            raise ValueError("active bridge not found")
        return bridge

    def close(
        self,
        bridge_id: str,
        *,
        expected_generation: int | None = None,
    ) -> Bridge:
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                "SELECT * FROM bridges WHERE bridge_id=?",
                (bridge_id,),
            ).fetchone()
            bridge = self.decode(row)
            if bridge is None:
                raise ValueError("bridge not found")
            if bridge.status == "closed":
                return bridge
            if bridge.status not in {"pending", "active"}:
                raise ValueError("bridge cannot be closed from its current state")
            if (
                expected_generation is not None
                and bridge.binding_generation != expected_generation
            ):
                raise ValueError("binding changed; reload before closing")
            if db.execute(
                """
                SELECT 1 FROM bridge_attempts
                WHERE bridge_id=? AND state IN (
                  'prepared','submitting','uncertain','awaiting_ack','replying'
                )
                """,
                (bridge_id,),
            ).fetchone():
                raise ValueError("binding has an active delivery attempt")
            if db.execute(
                """
                SELECT 1 FROM bridge_events
                WHERE bridge_id=? AND state IN (
                  'queued','processing','prepared','submitting','uncertain',
                  'awaiting_ack','replying'
                )
                """,
                (bridge_id,),
            ).fetchone():
                raise ValueError("binding has queued or active delivery work")
            if db.execute(
                """
                SELECT 1 FROM thread_ingress
                WHERE bridge_id=? AND binding_generation=?
                  AND state IN ('pending','processing','dispatched','uncertain')
                """,
                (bridge_id, bridge.binding_generation),
            ).fetchone():
                raise ValueError("binding has claimed or pending Slack ingress")
            if db.execute(
                """
                SELECT 1 FROM bridge_roots
                WHERE bridge_id=? AND state!='complete'
                """,
                (bridge_id,),
            ).fetchone():
                raise ValueError("binding root delivery is still in progress")
            closed = db.execute(
                """
                UPDATE bridges
                SET status='closed',
                    binding_generation=binding_generation+1,
                    updated_at=CURRENT_TIMESTAMP
                WHERE bridge_id=? AND status IN ('pending','active')
                  AND binding_generation=?
                """,
                (bridge_id, bridge.binding_generation),
            )
            if closed.rowcount != 1:
                raise ValueError("binding changed during close")
            if bridge.thread_ts:
                db.execute(
                    """
                    DELETE FROM thread_participation
                    WHERE team_id=? AND channel_id=? AND thread_ts=?
                    """,
                    (bridge.team_id, bridge.channel_id, bridge.thread_ts),
                )
            result = self.decode(
                db.execute(
                    "SELECT * FROM bridges WHERE bridge_id=?",
                    (bridge_id,),
                ).fetchone()
            )
            if result is None:
                raise RuntimeError("closed bridge could not be decoded")
            return result

    def mark_participation(
        self,
        team_id: str,
        channel_id: str,
        thread_ts: str,
        observed_at: str | None = None,
    ) -> None:
        if not channel_id or not thread_ts:
            raise ValueError("channel and thread timestamp are required")
        timestamp = observed_at or datetime.datetime.now(datetime.timezone.utc).isoformat()
        with self.connect() as db:
            db.execute(
                """
                INSERT INTO thread_participation(team_id,channel_id,thread_ts,updated_at)
                VALUES(?,?,?,datetime(?))
                ON CONFLICT(team_id,channel_id,thread_ts)
                DO UPDATE SET updated_at=MAX(thread_participation.updated_at,excluded.updated_at)
                """,
                (team_id, channel_id, thread_ts, timestamp),
            )
            db.execute(
                """
                UPDATE bridges SET updated_at=CURRENT_TIMESTAMP
                WHERE team_id=? AND channel_id=? AND thread_ts=?
                  AND status='active'
                """,
                (team_id, channel_id, thread_ts),
            )

    def participates(self, team_id: str, channel_id: str, thread_ts: str) -> bool:
        if not channel_id or not thread_ts:
            return False
        with self.connect() as db:
            row = db.execute(
                "SELECT 1 FROM thread_participation WHERE team_id=? AND channel_id=? AND thread_ts=?",
                (team_id, channel_id, thread_ts),
            ).fetchone()
            if row is None and team_id:
                row = db.execute(
                    "SELECT 1 FROM thread_participation WHERE team_id='' AND channel_id=? AND thread_ts=?",
                    (channel_id, thread_ts),
                ).fetchone()
            return row is not None

    def recent_participating_threads(
        self, hours: int = 168, limit: int = 500,
    ) -> list[tuple[str, str, str, float]]:
        hours = max(1, min(hours, 24 * 90))
        limit = max(1, min(limit, 2_000))
        with self.connect() as db:
            rows = db.execute(
                """
                SELECT team_id,channel_id,thread_ts,updated_at FROM thread_participation
                WHERE updated_at >= datetime('now', ?)
                ORDER BY updated_at DESC LIMIT ?
                """,
                (f"-{hours} hours", limit),
            ).fetchall()
            return [
                (
                    row[0], row[1], row[2],
                    datetime.datetime.fromisoformat(row[3]).replace(
                        tzinfo=datetime.timezone.utc,
                    ).timestamp(),
                )
                for row in rows
            ]

    @staticmethod
    def _reply_poll_identity(
        team_id: str,
        channel_id: str,
        thread_ts: str,
    ) -> tuple[str, str, str]:
        values = (team_id, channel_id, thread_ts)
        if (
            not all(isinstance(value, str) for value in values)
            or not channel_id
            or not thread_ts
            or len(team_id) > 128
            or len(channel_id) > 128
            or len(thread_ts) > 128
            or any(CONFIG_CONTROL_PATTERN.search(value) for value in values)
        ):
            raise ValueError("invalid Slack reply poll target")
        return values

    @staticmethod
    def _slack_read_budget_identity(
        team_id: str,
        method: str,
    ) -> tuple[str, str]:
        if (
            not isinstance(team_id, str)
            or not ID_PATTERN.fullmatch(team_id)
            or method not in {
                "conversations.history",
                "conversations.replies",
            }
        ):
            raise ValueError("invalid Slack read budget identity")
        return team_id, method

    @staticmethod
    def _claim_slack_read_budget_in_tx(
        db: sqlite3.Connection,
        team_id: str,
        method: str,
    ) -> bool:
        limit = db.execute(
            """
            SELECT next_allowed_at FROM slack_reconciliation_limits
            WHERE team_id=? AND method=?
            """,
            (team_id, method),
        ).fetchone()
        if (
            limit is not None
            and not db.execute(
                "SELECT datetime(?) <= CURRENT_TIMESTAMP",
                (limit["next_allowed_at"],),
            ).fetchone()[0]
        ):
            return False
        modifier = f"+{RECONCILIATION_INTERVAL_SECONDS} seconds"
        db.execute(
            """
            INSERT INTO slack_reconciliation_limits(
              team_id,method,next_allowed_at,updated_at
            ) VALUES(?,?,datetime('now',?),CURRENT_TIMESTAMP)
            ON CONFLICT(team_id,method) DO UPDATE SET
              next_allowed_at=excluded.next_allowed_at,
              updated_at=CURRENT_TIMESTAMP
            """,
            (team_id, method, modifier),
        )
        return True

    def claim_slack_read_budget(
        self,
        team_id: str,
        method: str,
    ) -> bool:
        """Claim the shared cross-process budget for a Slack history method."""
        identity = self._slack_read_budget_identity(team_id, method)
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            return self._claim_slack_read_budget_in_tx(db, *identity)

    def select_reply_poll_targets(
        self,
        targets: list[tuple[str, str, str]],
        *,
        workspace_limit: int = 10,
    ) -> list[tuple[str, str, str]]:
        """Synchronize targets and select one durable round-robin thread per workspace."""
        if (
            not isinstance(workspace_limit, int)
            or isinstance(workspace_limit, bool)
            or workspace_limit < 1
            or workspace_limit > 25
        ):
            raise ValueError("workspace_limit must be between 1 and 25")
        normalized = sorted({
            self._reply_poll_identity(*target)
            for target in targets
        })
        target_set = set(normalized)
        by_team: dict[str, list[tuple[str, str, str]]] = {}
        for target in normalized:
            by_team.setdefault(target[0], []).append(target)
        teams = sorted(by_team)

        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            for row in db.execute(
                """
                SELECT team_id,channel_id,thread_ts
                FROM slack_reply_poll_state
                """
            ).fetchall():
                key = (str(row[0]), str(row[1]), str(row[2]))
                if key not in target_set:
                    db.execute(
                        """
                        DELETE FROM slack_reply_poll_state
                        WHERE team_id=? AND channel_id=? AND thread_ts=?
                        """,
                        key,
                    )
            for row in db.execute(
                "SELECT team_id FROM slack_reply_poll_rotation"
            ).fetchall():
                if str(row[0]) not in by_team:
                    db.execute(
                        "DELETE FROM slack_reply_poll_rotation WHERE team_id=?",
                        (str(row[0]),),
                    )

            if not teams:
                db.execute("DELETE FROM slack_reply_poll_scheduler")
                return []

            scheduler = db.execute(
                """
                SELECT last_team_id FROM slack_reply_poll_scheduler
                WHERE scheduler_id=1
                """
            ).fetchone()
            if scheduler is None:
                team_start = 0
            else:
                last_team = str(scheduler[0])
                team_start = next(
                    (
                        index
                        for index, team in enumerate(teams)
                        if team > last_team
                    ),
                    0,
                )
            ordered_teams = teams[team_start:] + teams[:team_start]
            selected_teams = ordered_teams[:workspace_limit]

            selected: list[tuple[str, str, str]] = []
            for team_id in selected_teams:
                candidates = by_team[team_id]
                marker = db.execute(
                    """
                    SELECT last_channel_id,last_thread_ts
                    FROM slack_reply_poll_rotation
                    WHERE team_id=?
                    """,
                    (team_id,),
                ).fetchone()
                last_key = (
                    (str(marker[0]), str(marker[1]))
                    if marker is not None else ("", "")
                )
                chosen = next(
                    (
                        candidate
                        for candidate in candidates
                        if (candidate[1], candidate[2]) > last_key
                    ),
                    candidates[0],
                )
                selected.append(chosen)
                db.execute(
                    """
                    INSERT INTO slack_reply_poll_rotation(
                      team_id,last_channel_id,last_thread_ts,updated_at
                    ) VALUES(?,?,?,CURRENT_TIMESTAMP)
                    ON CONFLICT(team_id) DO UPDATE SET
                      last_channel_id=excluded.last_channel_id,
                      last_thread_ts=excluded.last_thread_ts,
                      updated_at=CURRENT_TIMESTAMP
                    """,
                    chosen,
                )
            db.execute(
                """
                INSERT INTO slack_reply_poll_scheduler(
                  scheduler_id,last_team_id,updated_at
                ) VALUES(1,?,CURRENT_TIMESTAMP)
                ON CONFLICT(scheduler_id) DO UPDATE SET
                  last_team_id=excluded.last_team_id,
                  updated_at=CURRENT_TIMESTAMP
                """,
                (selected_teams[-1],),
            )
            return selected

    def reply_poll_page_state(
        self,
        team_id: str,
        channel_id: str,
        thread_ts: str,
    ) -> ReplyPollPageState | None:
        key = self._reply_poll_identity(team_id, channel_id, thread_ts)
        with self.connect() as db:
            row = db.execute(
                """
                SELECT next_cursor,seen_cursors_json,pages_seen,page_oldest,
                       bot_user_ids_json,root_bridge_id,pending_messages_json
                FROM slack_reply_poll_state
                WHERE team_id=? AND channel_id=? AND thread_ts=?
                """,
                key,
            ).fetchone()
        if row is None:
            return None
        try:
            next_cursor = row["next_cursor"]
            seen = json.loads(str(row["seen_cursors_json"]))
            pages_seen = int(row["pages_seen"])
            page_oldest = str(row["page_oldest"])
            bot_user_ids = json.loads(str(row["bot_user_ids_json"]))
            root_bridge_id = str(row["root_bridge_id"] or "")
            pending_messages = json.loads(str(row["pending_messages_json"]))
            oldest_value = float(page_oldest)
            if (
                next_cursor is not None
                and (
                    not isinstance(next_cursor, str)
                    or not next_cursor
                    or len(next_cursor) > 16_384
                )
            ):
                raise ValueError("invalid next cursor")
            if (
                not isinstance(seen, list)
                or len(seen) > 100
                or len(set(seen)) != len(seen)
                or any(
                    not isinstance(cursor, str)
                    or not cursor
                    or len(cursor) > 16_384
                    for cursor in seen
                )
                or pages_seen < 0
                or pages_seen > 100
                or not math.isfinite(oldest_value)
                or oldest_value < 0
                or not isinstance(bot_user_ids, list)
                or len(bot_user_ids) > 1_000
                or len(set(bot_user_ids)) != len(bot_user_ids)
                or any(
                    not isinstance(user_id, str)
                    or not user_id
                    or len(user_id) > 128
                    or CONFIG_CONTROL_PATTERN.search(user_id)
                    for user_id in bot_user_ids
                )
                or len(root_bridge_id) > 128
                or CONFIG_CONTROL_PATTERN.search(root_bridge_id)
                or not self._valid_reply_poll_messages(pending_messages)
            ):
                raise ValueError("invalid reply poll page state")
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise RuntimeError("stored Slack reply poll state is invalid") from exc
        return ReplyPollPageState(
            team_id=key[0],
            channel_id=key[1],
            thread_ts=key[2],
            next_cursor=next_cursor,
            seen_cursors=tuple(seen),
            pages_seen=pages_seen,
            page_oldest=page_oldest,
            bot_user_ids=tuple(bot_user_ids),
            root_bridge_id=root_bridge_id,
            pending_messages=tuple(dict(message) for message in pending_messages),
        )

    @staticmethod
    def _valid_reply_poll_messages(messages: Any) -> bool:
        allowed_keys = {
            "ts",
            "thread_ts",
            "text",
            "user",
            "bot_id",
            "subtype",
            "channel_type",
        }
        return (
            isinstance(messages, (list, tuple))
            and len(messages) <= 1_500
            and all(
                isinstance(message, dict)
                and set(message) <= allowed_keys
                and isinstance(message.get("ts"), str)
                and bool(message.get("ts"))
                and len(message["ts"]) <= 128
                and not CONFIG_CONTROL_PATTERN.search(message["ts"])
                and all(
                    isinstance(value, str)
                    and len(value) <= (MAX_TEXT if key == "text" else 128)
                    and not (
                        key != "text"
                        and CONFIG_CONTROL_PATTERN.search(value)
                    )
                    for key, value in message.items()
                )
                for message in messages
            )
        )

    def save_reply_poll_page_state(
        self,
        team_id: str,
        channel_id: str,
        thread_ts: str,
        *,
        next_cursor: str | None,
        seen_cursors: tuple[str, ...],
        pages_seen: int,
        page_oldest: str,
        bot_user_ids: tuple[str, ...] = (),
        root_bridge_id: str = "",
        pending_messages: tuple[dict[str, str], ...] = (),
    ) -> None:
        key = self._reply_poll_identity(team_id, channel_id, thread_ts)
        if (
            next_cursor is not None
            and (
                not isinstance(next_cursor, str)
                or not next_cursor
                or len(next_cursor) > 16_384
            )
        ):
            raise ValueError("invalid Slack reply poll cursor")
        if (
            not isinstance(seen_cursors, tuple)
            or len(seen_cursors) > 100
            or len(set(seen_cursors)) != len(seen_cursors)
            or any(
                not isinstance(cursor, str)
                or not cursor
                or len(cursor) > 16_384
                for cursor in seen_cursors
            )
            or not isinstance(pages_seen, int)
            or isinstance(pages_seen, bool)
            or pages_seen < 0
            or pages_seen > 100
            or not isinstance(page_oldest, str)
            or not isinstance(bot_user_ids, tuple)
            or len(bot_user_ids) > 1_000
            or len(set(bot_user_ids)) != len(bot_user_ids)
            or any(
                not isinstance(user_id, str)
                or not user_id
                or len(user_id) > 128
                or CONFIG_CONTROL_PATTERN.search(user_id)
                for user_id in bot_user_ids
            )
            or not isinstance(root_bridge_id, str)
            or len(root_bridge_id) > 128
            or CONFIG_CONTROL_PATTERN.search(root_bridge_id)
            or not self._valid_reply_poll_messages(pending_messages)
        ):
            raise ValueError("invalid Slack reply poll page state")
        try:
            oldest_value = float(page_oldest)
        except ValueError as exc:
            raise ValueError("invalid Slack reply poll oldest timestamp") from exc
        if not math.isfinite(oldest_value) or oldest_value < 0:
            raise ValueError("invalid Slack reply poll oldest timestamp")
        seen_json = json.dumps(
            list(seen_cursors),
            separators=(",", ":"),
        )
        bot_users_json = json.dumps(
            sorted(bot_user_ids),
            separators=(",", ":"),
        )
        pending_messages_json = json.dumps(
            pending_messages,
            separators=(",", ":"),
            sort_keys=True,
        )
        if len(pending_messages_json.encode("utf-8")) > 16 * 1024 * 1024:
            raise ValueError("Slack reply poll messages exceed the storage limit")
        with self.connect() as db:
            db.execute(
                """
                INSERT INTO slack_reply_poll_state(
                  team_id,channel_id,thread_ts,next_cursor,
                  seen_cursors_json,pages_seen,page_oldest,
                  bot_user_ids_json,root_bridge_id,pending_messages_json,
                  updated_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP)
                ON CONFLICT(team_id,channel_id,thread_ts) DO UPDATE SET
                  next_cursor=excluded.next_cursor,
                  seen_cursors_json=excluded.seen_cursors_json,
                  pages_seen=excluded.pages_seen,
                  page_oldest=excluded.page_oldest,
                  bot_user_ids_json=excluded.bot_user_ids_json,
                  root_bridge_id=excluded.root_bridge_id,
                  pending_messages_json=excluded.pending_messages_json,
                  updated_at=CURRENT_TIMESTAMP
                """,
                (
                    *key,
                    next_cursor,
                    seen_json,
                    pages_seen,
                    page_oldest,
                    bot_users_json,
                    root_bridge_id,
                    pending_messages_json,
                ),
            )

    def clear_reply_poll_page_state(
        self,
        team_id: str,
        channel_id: str,
        thread_ts: str,
    ) -> None:
        key = self._reply_poll_identity(team_id, channel_id, thread_ts)
        with self.connect() as db:
            db.execute(
                """
                DELETE FROM slack_reply_poll_state
                WHERE team_id=? AND channel_id=? AND thread_ts=?
                """,
                key,
            )

    def claim_thread_ingress(
        self, event_id: str, team_id: str, channel_id: str, thread_ts: str,
        *,
        route_action: str = "hermes",
        writer_id: str = "legacy",
        bridge_id: str = "",
        binding_generation: int | None = None,
        payload: dict[str, Any] | None = None,
        lease_seconds: int = 45,
        lease_owner: str = PROCESS_EPOCH,
    ) -> dict[str, Any]:
        if not event_id or not team_id or not channel_id or not thread_ts:
            raise ValueError("complete Slack thread identity is required")
        if route_action not in {"hermes", "native"} or not writer_id:
            raise ValueError("Slack ingress route and writer are required")
        if binding_generation is not None and binding_generation < 1:
            raise ValueError("Slack ingress binding generation is invalid")
        if not re.fullmatch(r"[0-9a-f]{32}", lease_owner):
            raise ValueError("invalid thread ingress lease owner")
        payload_json = json.dumps(
            payload or {},
            ensure_ascii=False,
            separators=(",", ":"),
        )
        if len(payload_json) > MAX_TEXT * 2:
            raise ValueError("Slack ingress payload is too large")
        lease_seconds = max(15, min(int(lease_seconds), 300))
        modifier = f"+{lease_seconds} seconds"
        lease_id = "tin_" + uuid.uuid4().hex
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                db.execute(
                    """
                    INSERT INTO thread_ingress(
                      event_id,team_id,channel_id,thread_ts,route_action,
                      writer_id,bridge_id,binding_generation,payload_json,
                      fence_epoch,state,
                      lease_id,lease_owner,lease_expires_at,retry_count,updated_at
                    ) VALUES(?,?,?,?,?,?,?,?,?,1,'processing',?,?,
                             datetime('now',?),1,CURRENT_TIMESTAMP)
                    """,
                    (
                        event_id,
                        team_id,
                        channel_id,
                        thread_ts,
                        route_action,
                        writer_id,
                        bridge_id or None,
                        binding_generation,
                        payload_json,
                        lease_id,
                        lease_owner,
                        modifier,
                    ),
                )
            except sqlite3.IntegrityError:
                row = db.execute(
                    """
                    SELECT team_id,channel_id,thread_ts,route_action,writer_id,
                           bridge_id,binding_generation,payload_json,state,
                           lease_owner,lease_expires_at,fence_epoch
                    FROM thread_ingress WHERE event_id=?
                    """,
                    (event_id,),
                ).fetchone()
                if row is None:
                    return {"status": "busy"}
                if (
                    str(row["team_id"]) != team_id
                    or str(row["channel_id"]) != channel_id
                    or str(row["thread_ts"]) != thread_ts
                ):
                    raise RuntimeError(
                        "Slack event identity changed across ingress attempts"
                    )
                state = str(row["state"])
                if state == "routing":
                    claimed = db.execute(
                        """
                        UPDATE thread_ingress
                        SET route_action=?,writer_id=?,bridge_id=?,
                            binding_generation=?,payload_json=?,
                            state='processing',lease_id=?,lease_owner=?,
                            lease_expires_at=datetime('now',?),
                            fence_epoch=fence_epoch+1,
                            retry_count=retry_count+1,error_code=NULL,
                            egress_sealed=0,
                            updated_at=CURRENT_TIMESTAMP
                        WHERE event_id=? AND state='routing'
                        """,
                        (
                            route_action,
                            writer_id,
                            bridge_id or None,
                            binding_generation,
                            payload_json,
                            lease_id,
                            lease_owner,
                            modifier,
                            event_id,
                        ),
                    )
                    if claimed.rowcount != 1:
                        return {"status": "busy"}
                    row = db.execute(
                        "SELECT fence_epoch FROM thread_ingress WHERE event_id=?",
                        (event_id,),
                    ).fetchone()
                    return {
                        "status": "claimed",
                        "lease_id": lease_id,
                        "fence_epoch": int(row["fence_epoch"]),
                    }
                if state in {
                    "completed",
                    "transferred",
                    "cancelled",
                    "dispatched",
                    "uncertain",
                }:
                    return {"status": state}
                if (
                    str(row["route_action"]) != route_action
                    or str(row["writer_id"]) != writer_id
                    or str(row["bridge_id"] or "") != bridge_id
                    or (
                        int(row["binding_generation"])
                        if row["binding_generation"] is not None
                        else None
                    ) != binding_generation
                    or str(row["payload_json"] or "{}") != payload_json
                ):
                    raise RuntimeError(
                        "Slack event route changed across ingress attempts"
                    )
                if (
                    state == "processing"
                    and row["lease_expires_at"]
                    and db.execute(
                        "SELECT datetime(?) > CURRENT_TIMESTAMP",
                        (row["lease_expires_at"],),
                    ).fetchone()[0]
                ):
                    return {"status": "busy"}
                if (
                    state == "processing"
                    and str(row["lease_owner"] or "") == lease_owner
                ):
                    return {"status": "busy"}
                claimed = db.execute(
                    """
                    UPDATE thread_ingress
                    SET state='processing',lease_id=?,lease_owner=?,
                        lease_expires_at=datetime('now',?),
                        fence_epoch=fence_epoch+1,
                        retry_count=retry_count+1,error_code=NULL,
                        egress_sealed=0,
                        updated_at=CURRENT_TIMESTAMP
                    WHERE event_id=?
                      AND state NOT IN ('completed','transferred','cancelled')
                      AND (
                        state!='processing'
                        OR lease_expires_at IS NULL
                        OR datetime(lease_expires_at) <= CURRENT_TIMESTAMP
                      )
                    """,
                    (lease_id, lease_owner, modifier, event_id),
                )
                if claimed.rowcount != 1:
                    return {"status": "busy"}
            row = db.execute(
                "SELECT fence_epoch FROM thread_ingress WHERE event_id=?",
                (event_id,),
            ).fetchone()
            return {
                "status": "claimed",
                "lease_id": lease_id,
                "fence_epoch": int(row["fence_epoch"]),
            }

    def reserve_routing_ingress(
        self,
        event_id: str,
        team_id: str,
        channel_id: str,
        thread_ts: str,
        payload: dict[str, Any],
    ) -> str:
        if not event_id or not team_id or not channel_id or not thread_ts:
            raise ValueError("complete Slack ingress identity is required")
        payload_json = json.dumps(
            payload,
            ensure_ascii=False,
            separators=(",", ":"),
        )
        if len(payload_json) > MAX_TEXT * 2:
            raise ValueError("Slack ingress payload is too large")
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                db.execute(
                    """
                    INSERT INTO thread_ingress(
                      event_id,team_id,channel_id,thread_ts,route_action,
                      writer_id,payload_json,state,retry_count,updated_at
                    ) VALUES(?,?,?,?,'unresolved','unresolved',?,'routing',0,
                             CURRENT_TIMESTAMP)
                    """,
                    (
                        event_id,
                        team_id,
                        channel_id,
                        thread_ts,
                        payload_json,
                    ),
                )
                return "routing"
            except sqlite3.IntegrityError:
                row = db.execute(
                    """
                    SELECT team_id,channel_id,thread_ts,payload_json,state
                    FROM thread_ingress WHERE event_id=?
                    """,
                    (event_id,),
                ).fetchone()
                if row is None:
                    return "busy"
                if (
                    str(row["team_id"]) != team_id
                    or str(row["channel_id"]) != channel_id
                    or str(row["thread_ts"]) != thread_ts
                ):
                    raise RuntimeError(
                        "Slack event identity changed before routing"
                    )
                state = str(row["state"])
                if (
                    state == "routing"
                    and str(row["payload_json"] or "{}") != payload_json
                ):
                    raise RuntimeError(
                        "Slack event payload changed before routing"
                    )
                return state

    def defer_routing_ingress(
        self,
        event_id: str,
        error_code: str,
        *,
        max_attempts: int = 12,
    ) -> str:
        safe_code = _safe_label(error_code, 128) or "slack_routing_failed"
        max_attempts = max(1, min(int(max_attempts), 100))
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                """
                SELECT retry_count FROM thread_ingress
                WHERE event_id=? AND state='routing'
                """,
                (event_id,),
            ).fetchone()
            if row is None:
                return "stale"
            next_count = int(row["retry_count"] or 0) + 1
            next_state = "uncertain" if next_count >= max_attempts else "routing"
            updated = db.execute(
                """
                UPDATE thread_ingress
                SET state=?,retry_count=?,error_code=?,
                    updated_at=CURRENT_TIMESTAMP
                WHERE event_id=? AND state='routing'
                """,
                (next_state, next_count, safe_code, event_id),
            )
            return next_state if updated.rowcount == 1 else "stale"

    def cancel_routing_ingress(
        self,
        event_id: str,
        reason: str,
    ) -> bool:
        safe_reason = _safe_label(reason, 128) or "slack_routing_rejected"
        with self.connect() as db:
            cancelled = db.execute(
                """
                UPDATE thread_ingress
                SET state='cancelled',error_code=?,
                    updated_at=CURRENT_TIMESTAMP
                WHERE event_id=? AND state='routing'
                """,
                (safe_reason, event_id),
            )
            return cancelled.rowcount == 1

    def quarantine_invalid_thread_ingress(self, event_id: str) -> bool:
        with self.connect() as db:
            changed = db.execute(
                """
                UPDATE thread_ingress
                SET state='uncertain',lease_id=NULL,lease_owner=NULL,
                    lease_expires_at=NULL,error_code='stored_ingress_invalid',
                    updated_at=CURRENT_TIMESTAMP
                WHERE event_id=?
                  AND state IN ('routing','pending','processing')
                """,
                (event_id,),
            )
            return changed.rowcount == 1

    @staticmethod
    def _decode_recoverable_ingress(
        row: sqlite3.Row,
    ) -> tuple[datetime.datetime, dict[str, Any]]:
        updated = datetime.datetime.fromisoformat(str(row["updated_at"]))
        if updated.tzinfo is None:
            updated = updated.replace(tzinfo=datetime.timezone.utc)
        else:
            updated = updated.astimezone(datetime.timezone.utc)
        payload = json.loads(str(row["payload_json"] or "{}"))
        if not isinstance(payload, dict):
            raise ValueError("stored ingress payload is not an object")
        return updated, payload

    def recoverable_routing_ingress(
        self,
        *,
        limit: int = 20,
        now: datetime.datetime | None = None,
    ) -> list[dict[str, Any]]:
        limit = max(1, min(int(limit), 100))
        current = now or datetime.datetime.now(datetime.timezone.utc)
        with self.connect() as db:
            rows = db.execute(
                """
                SELECT event_id,team_id,channel_id,thread_ts,payload_json,
                       retry_count,updated_at
                FROM thread_ingress
                WHERE route_action='unresolved' AND state='routing'
                ORDER BY updated_at,event_id
                LIMIT ?
                """,
                (limit * 4,),
            ).fetchall()
        recovered: list[dict[str, Any]] = []
        for row in rows:
            retry_count = max(0, int(row["retry_count"] or 0))
            delay_seconds = min(300, 2 ** min(max(1, retry_count), 8))
            try:
                updated, payload = self._decode_recoverable_ingress(row)
            except (TypeError, ValueError, json.JSONDecodeError):
                self.quarantine_invalid_thread_ingress(str(row["event_id"]))
                continue
            if (current - updated).total_seconds() < delay_seconds:
                continue
            recovered.append(
                {
                    "event_id": str(row["event_id"]),
                    "team_id": str(row["team_id"]),
                    "channel_id": str(row["channel_id"]),
                    "thread_ts": str(row["thread_ts"]),
                    "payload": payload,
                    "retry_count": retry_count,
                }
            )
            if len(recovered) >= limit:
                break
        return recovered

    def transfer_thread_ingress(
        self,
        event_id: str,
        lease_id: str,
        fence_epoch: int,
        bridge_id: str,
        binding_generation: int,
        text: str,
    ) -> bool:
        if not text.strip() or len(text) > MAX_TEXT:
            raise ValueError("native ingress text is empty or too large")
        payload_json = json.dumps(
            {"text": text},
            ensure_ascii=False,
            separators=(",", ":"),
        )
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            ingress = db.execute(
                """
                SELECT bridge_id,binding_generation,route_action,state
                FROM thread_ingress
                WHERE event_id=? AND lease_id=? AND fence_epoch=?
                """,
                (event_id, lease_id, fence_epoch),
            ).fetchone()
            bridge = db.execute(
                """
                SELECT binding_generation FROM bridges
                WHERE bridge_id=? AND status='active'
                """,
                (bridge_id,),
            ).fetchone()
            if (
                ingress is None
                or bridge is None
                or str(ingress["state"]) != "processing"
                or str(ingress["route_action"]) != "native"
                or str(ingress["bridge_id"] or "") != bridge_id
                or int(ingress["binding_generation"] or 0)
                != binding_generation
                or int(bridge["binding_generation"]) != binding_generation
            ):
                return False
            try:
                db.execute(
                    """
                    INSERT INTO bridge_events(
                      event_id,bridge_id,state,payload_json,binding_generation,
                      updated_at
                    ) VALUES(?,?,'queued',?,?,CURRENT_TIMESTAMP)
                    """,
                    (
                        event_id,
                        bridge_id,
                        payload_json,
                        binding_generation,
                    ),
                )
            except sqlite3.IntegrityError:
                return False
            transferred = db.execute(
                """
                UPDATE thread_ingress
                SET state='transferred',lease_id=NULL,lease_owner=NULL,
                    lease_expires_at=NULL,error_code=NULL,
                    updated_at=CURRENT_TIMESTAMP
                WHERE event_id=? AND lease_id=? AND fence_epoch=?
                  AND state='processing'
                """,
                (event_id, lease_id, fence_epoch),
            )
            if transferred.rowcount != 1:
                raise RuntimeError(
                    "native ingress lease was lost during durable transfer"
                )
            return True

    def apply_native_mutation(
        self,
        event_id: str,
        lease_id: str,
        fence_epoch: int,
        target_event_id: str,
        bridge_id: str,
        binding_generation: int,
        mutation_kind: str,
        replacement_text: str,
        notification_text: str,
    ) -> str:
        if mutation_kind not in {"edit", "delete"}:
            raise ValueError("unsupported Slack message mutation")
        if mutation_kind == "edit" and (
            not replacement_text.strip() or len(replacement_text) > MAX_TEXT
        ):
            raise ValueError("edited Slack text is empty or too large")
        if not notification_text.strip() or len(notification_text) > MAX_TEXT:
            raise ValueError("Slack mutation notice is empty or too large")
        replacement_payload = json.dumps(
            {"text": replacement_text},
            ensure_ascii=False,
            separators=(",", ":"),
        )
        notification_payload = json.dumps(
            {"text": notification_text},
            ensure_ascii=False,
            separators=(",", ":"),
        )
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            ingress = db.execute(
                """
                SELECT bridge_id,binding_generation,route_action,state
                FROM thread_ingress
                WHERE event_id=? AND lease_id=? AND fence_epoch=?
                """,
                (event_id, lease_id, fence_epoch),
            ).fetchone()
            bridge = db.execute(
                """
                SELECT binding_generation FROM bridges
                WHERE bridge_id=? AND status='active'
                """,
                (bridge_id,),
            ).fetchone()
            if (
                ingress is None
                or bridge is None
                or str(ingress["state"]) != "processing"
                or str(ingress["route_action"]) != "native"
                or str(ingress["bridge_id"] or "") != bridge_id
                or int(ingress["binding_generation"] or 0)
                != binding_generation
                or int(bridge["binding_generation"]) != binding_generation
            ):
                return "stale"
            target = db.execute(
                """
                SELECT state,binding_generation FROM bridge_events
                WHERE event_id=? AND bridge_id=?
                """,
                (target_event_id, bridge_id),
            ).fetchone()
            result = "notified"
            if (
                target is not None
                and target["binding_generation"] is not None
                and int(target["binding_generation"]) != binding_generation
            ):
                return "stale"
            if target is not None and str(target["state"]) == "queued":
                if mutation_kind == "edit":
                    db.execute(
                        """
                        UPDATE bridge_events
                        SET payload_json=?,error=NULL,
                            updated_at=CURRENT_TIMESTAMP
                        WHERE event_id=? AND bridge_id=? AND state='queued'
                        """,
                        (replacement_payload, target_event_id, bridge_id),
                    )
                    result = "revised"
                else:
                    db.execute(
                        """
                        UPDATE bridge_events
                        SET state='failed',error='slack_message_deleted',
                            updated_at=CURRENT_TIMESTAMP
                        WHERE event_id=? AND bridge_id=? AND state='queued'
                        """,
                        (target_event_id, bridge_id),
                    )
                    result = "cancelled"
            else:
                try:
                    db.execute(
                        """
                        INSERT INTO bridge_events(
                          event_id,bridge_id,state,payload_json,
                          binding_generation,updated_at
                        ) VALUES(?,?,'queued',?,?,CURRENT_TIMESTAMP)
                        """,
                        (
                            event_id,
                            bridge_id,
                            notification_payload,
                            binding_generation,
                        ),
                    )
                except sqlite3.IntegrityError:
                    result = "duplicate"
                if (
                    result != "duplicate"
                    and target is not None
                    and str(target["state"]) not in {
                    "delivered",
                    "failed",
                    }
                ):
                    result = "interrupt"
            transferred = db.execute(
                """
                UPDATE thread_ingress
                SET state='transferred',lease_id=NULL,lease_owner=NULL,
                    lease_expires_at=NULL,error_code=NULL,
                    updated_at=CURRENT_TIMESTAMP
                WHERE event_id=? AND lease_id=? AND fence_epoch=?
                  AND state='processing'
                """,
                (event_id, lease_id, fence_epoch),
            )
            if transferred.rowcount != 1:
                raise RuntimeError(
                    "Slack mutation lease was lost during durable transfer"
                )
            return result

    def cancel_pending_hermes_ingress(
        self,
        event_id: str,
        mutation_kind: str,
    ) -> str:
        if mutation_kind not in {"edit", "delete"}:
            raise ValueError("unsupported Slack message mutation")
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                """
                SELECT state,route_action FROM thread_ingress
                WHERE event_id=?
                """,
                (event_id,),
            ).fetchone()
            if row is None or str(row["route_action"]) != "hermes":
                return "missing"
            state = str(row["state"])
            if state == "pending":
                db.execute(
                    """
                    UPDATE thread_ingress
                    SET state='cancelled',lease_id=NULL,lease_owner=NULL,
                        lease_expires_at=NULL,error_code=?,
                        updated_at=CURRENT_TIMESTAMP
                    WHERE event_id=? AND state='pending'
                    """,
                    (f"slack_message_{mutation_kind}", event_id),
                )
                return "cancelled"
            if state in {"processing", "dispatched"}:
                db.execute(
                    """
                    UPDATE thread_ingress
                    SET state='uncertain',lease_id=NULL,lease_owner=NULL,
                        lease_expires_at=NULL,error_code=?,
                        updated_at=CURRENT_TIMESTAMP
                    WHERE event_id=? AND state IN ('processing','dispatched')
                    """,
                    (f"slack_message_{mutation_kind}_while_processing", event_id),
                )
                return "uncertain"
            return state

    def slack_mutation_target_identity(
        self,
        team_id: str,
        channel_id: str,
        message_ts: str,
    ) -> dict[str, str] | None:
        if (
            not ID_PATTERN.fullmatch(team_id)
            or not CHANNEL_ID_PATTERN.fullmatch(channel_id)
            or not re.fullmatch(r"\d{1,20}\.\d{1,20}", message_ts)
        ):
            return None
        event_id = f"slack:{team_id}:{channel_id}:{message_ts}"
        with self.connect() as db:
            row = db.execute(
                """
                SELECT thread_ts,payload_json FROM thread_ingress
                WHERE event_id=? AND team_id=? AND channel_id=?
                """,
                (event_id, team_id, channel_id),
            ).fetchone()
        if row is None:
            return None
        try:
            payload = json.loads(str(row["payload_json"] or "{}"))
        except json.JSONDecodeError:
            return None
        if not isinstance(payload, dict):
            return None
        user_id = str(payload.get("user") or "")
        thread_ts = str(
            payload.get("event_thread_ts")
            or row["thread_ts"]
            or message_ts
        )
        if (
            not ID_PATTERN.fullmatch(user_id)
            or not re.fullmatch(r"\d{1,20}\.\d{1,20}", thread_ts)
        ):
            return None
        return {
            "user_id": user_id,
            "thread_ts": thread_ts,
        }

    def mark_thread_ingress_dispatched(
        self,
        event_id: str,
        lease_id: str,
        fence_epoch: int,
    ) -> bool:
        with self.connect() as db:
            dispatched = db.execute(
                """
                UPDATE thread_ingress
                SET state='dispatched',updated_at=CURRENT_TIMESTAMP
                WHERE event_id=? AND lease_id=? AND fence_epoch=?
                  AND state='processing' AND error_code IS NULL
                """,
                (event_id, lease_id, fence_epoch),
            )
            return dispatched.rowcount == 1

    def mark_thread_ingress_uncertain(
        self,
        event_id: str,
        lease_id: str,
        fence_epoch: int,
        error_code: str,
    ) -> bool:
        safe_code = _safe_label(error_code, 128) or "hermes_dispatch_uncertain"
        with self.connect() as db:
            uncertain = db.execute(
                """
                UPDATE thread_ingress
                SET state='uncertain',lease_id=NULL,lease_owner=NULL,
                    lease_expires_at=NULL,error_code=?,
                    updated_at=CURRENT_TIMESTAMP
                WHERE event_id=? AND lease_id=? AND fence_epoch=?
                  AND state='dispatched'
                """,
                (safe_code, event_id, lease_id, fence_epoch),
            )
            return uncertain.rowcount == 1

    @staticmethod
    def _expire_dispatched_thread_ingress(db: sqlite3.Connection) -> int:
        expired = db.execute(
            """
            UPDATE thread_ingress
            SET state='uncertain',lease_id=NULL,lease_owner=NULL,
                lease_expires_at=NULL,
                error_code='hermes_dispatch_lease_expired',
                updated_at=CURRENT_TIMESTAMP
            WHERE state='dispatched'
              AND (
                lease_expires_at IS NULL
                OR datetime(lease_expires_at) <= CURRENT_TIMESTAMP
              )
            """
        )
        return int(expired.rowcount)

    def renew_thread_ingress(
        self,
        event_id: str,
        lease_id: str,
        *,
        lease_seconds: int = 45,
    ) -> bool:
        lease_seconds = max(15, min(int(lease_seconds), 300))
        with self.connect() as db:
            renewed = db.execute(
                """
                UPDATE thread_ingress
                SET lease_expires_at=datetime('now',?),
                    updated_at=CURRENT_TIMESTAMP
                WHERE event_id=? AND lease_id=?
                  AND state IN ('processing','dispatched')
                """,
                (f"+{lease_seconds} seconds", event_id, lease_id),
            )
            return renewed.rowcount == 1

    def complete_thread_ingress(
        self,
        event_id: str,
        lease_id: str,
        fence_epoch: int | None = None,
    ) -> bool:
        with self.connect() as db:
            if fence_epoch is None:
                completed = db.execute(
                    """
                    UPDATE thread_ingress
                    SET state='completed',lease_id=NULL,lease_owner=NULL,
                        lease_expires_at=NULL,error_code=NULL,
                        updated_at=CURRENT_TIMESTAMP
                    WHERE event_id=? AND lease_id=?
                      AND state IN ('processing','dispatched')
                      AND error_code IS NULL
                    """,
                    (event_id, lease_id),
                )
            else:
                completed = db.execute(
                    """
                    UPDATE thread_ingress
                    SET state='completed',lease_id=NULL,lease_owner=NULL,
                        lease_expires_at=NULL,error_code=NULL,
                        updated_at=CURRENT_TIMESTAMP
                    WHERE event_id=? AND lease_id=?
                      AND state IN ('processing','dispatched')
                      AND error_code IS NULL
                      AND fence_epoch=?
                    """,
                    (event_id, lease_id, fence_epoch),
                )
            return completed.rowcount == 1

    def seal_thread_ingress_egress(
        self,
        event_id: str,
        lease_id: str,
        fence_epoch: int,
        *,
        allow_empty: bool,
    ) -> str:
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            ingress = db.execute(
                """
                SELECT state,lease_id,fence_epoch
                FROM thread_ingress
                WHERE event_id=?
                """,
                (event_id,),
            ).fetchone()
            if (
                ingress is None
                or str(ingress["lease_id"] or "") != lease_id
                or int(ingress["fence_epoch"]) != fence_epoch
                or str(ingress["state"]) not in {"processing", "dispatched"}
            ):
                return "stale"
            counts = db.execute(
                """
                SELECT count(*) AS total,
                       sum(CASE WHEN state='sent' THEN 0 ELSE 1 END) AS pending
                FROM slack_messages
                WHERE ingress_event_id=?
                """,
                (event_id,),
            ).fetchone()
            total = int(counts["total"] or 0)
            pending = int(counts["pending"] or 0)
            if total == 0 and not allow_empty:
                return "empty"
            if pending == 0:
                completed = db.execute(
                    """
                    UPDATE thread_ingress
                    SET state='completed',egress_sealed=1,
                        lease_id=NULL,lease_owner=NULL,lease_expires_at=NULL,
                        error_code=NULL,updated_at=CURRENT_TIMESTAMP
                    WHERE event_id=? AND lease_id=? AND fence_epoch=?
                      AND state IN ('processing','dispatched')
                    """,
                    (event_id, lease_id, fence_epoch),
                )
                return "completed" if completed.rowcount == 1 else "stale"
            sealed = db.execute(
                """
                UPDATE thread_ingress
                SET state='uncertain',egress_sealed=1,
                    lease_id=NULL,lease_owner=NULL,lease_expires_at=NULL,
                    error_code='hermes_egress_pending',
                    updated_at=CURRENT_TIMESTAMP
                WHERE event_id=? AND lease_id=? AND fence_epoch=?
                  AND state IN ('processing','dispatched')
                """,
                (event_id, lease_id, fence_epoch),
            )
            return "pending" if sealed.rowcount == 1 else "stale"

    def release_thread_ingress(
        self,
        event_id: str,
        lease_id: str,
        error_code: str,
    ) -> bool:
        safe_code = _safe_label(error_code, 128) or "hermes_dispatch_failed"
        with self.connect() as db:
            released = db.execute(
                """
                UPDATE thread_ingress
                SET state='pending',lease_id=NULL,lease_owner=NULL,
                    lease_expires_at=NULL,error_code=?,
                    updated_at=CURRENT_TIMESTAMP
                WHERE event_id=? AND lease_id=? AND state='processing'
                """,
                (safe_code, event_id, lease_id),
            )
            return released.rowcount == 1

    def unresolved_operations(
        self,
        team_id: str,
        *,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        if not ID_PATTERN.fullmatch(team_id):
            raise ValueError("invalid Slack workspace")
        if (
            not isinstance(limit, int)
            or isinstance(limit, bool)
            or limit < 1
            or limit > 100
        ):
            raise ValueError("unresolved operation limit must be between 1 and 100")
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            self._expire_dispatched_thread_ingress(db)
            ingress = db.execute(
                """
                SELECT event_id,bridge_id,binding_generation,route_action,
                       error_code,updated_at
                FROM thread_ingress
                WHERE team_id=? AND state='uncertain'
                ORDER BY updated_at,event_id
                LIMIT ?
                """,
                (team_id, limit),
            ).fetchall()
            remaining = max(0, limit - len(ingress))
            attempts = db.execute(
                """
                SELECT attempts.attempt_id,attempts.bridge_id,
                       attempts.binding_generation,attempts.delivery_kind,
                       attempts.error_code,attempts.updated_at
                FROM bridge_attempts AS attempts
                JOIN bridges ON bridges.bridge_id=attempts.bridge_id
                WHERE bridges.team_id=? AND attempts.state='uncertain'
                ORDER BY attempts.updated_at,attempts.attempt_id
                LIMIT ?
                """,
                (team_id, remaining),
            ).fetchall()
            remaining = max(0, remaining - len(attempts))
            reconciliations = db.execute(
                """
                SELECT reconciliation_key,target_kind,target_id,error,updated_at
                FROM slack_reconciliations
                WHERE team_id=? AND state='failed'
                ORDER BY updated_at,reconciliation_key
                LIMIT ?
                """,
                (team_id, remaining),
            ).fetchall()
        result = [
            {
                "kind": "ingress",
                "id": str(row["event_id"]),
                "bridge_id": str(row["bridge_id"] or ""),
                "binding_generation": (
                    int(row["binding_generation"])
                    if row["binding_generation"] is not None
                    else None
                ),
                "operation": str(row["route_action"]),
                "error_code": str(row["error_code"] or ""),
                "updated_at": str(row["updated_at"]),
            }
            for row in ingress
        ]
        result.extend(
            {
                "kind": "attempt",
                "id": str(row["attempt_id"]),
                "bridge_id": str(row["bridge_id"]),
                "binding_generation": int(row["binding_generation"]),
                "operation": str(row["delivery_kind"]),
                "error_code": str(row["error_code"] or ""),
                "updated_at": str(row["updated_at"]),
            }
            for row in attempts
        )
        result.extend(
            {
                "kind": "reconciliation",
                "id": str(row["reconciliation_key"]),
                "bridge_id": "",
                "binding_generation": None,
                "operation": str(row["target_kind"]),
                "target_id": str(row["target_id"]),
                "error_code": str(row["error"] or ""),
                "updated_at": str(row["updated_at"]),
            }
            for row in reconciliations
        )
        return result

    def resolve_uncertain_operation(
        self,
        team_id: str,
        kind: str,
        operation_id: str,
        action: str,
    ) -> dict[str, Any]:
        if (
            not ID_PATTERN.fullmatch(team_id)
            or kind not in {"ingress", "attempt", "reconciliation"}
            or action not in {"retry", "complete", "abandon"}
            or not operation_id
            or len(operation_id) > 256
            or CONFIG_CONTROL_PATTERN.search(operation_id)
        ):
            raise ValueError("invalid uncertain operation resolution")
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            if kind == "ingress":
                row = db.execute(
                    """
                    SELECT state,route_action,team_id,bridge_id
                    FROM thread_ingress
                    WHERE event_id=?
                    """,
                    (operation_id,),
                ).fetchone()
                if row is None or str(row["team_id"]) != team_id:
                    raise ValueError("uncertain ingress not found")
                current = str(row["state"])
                bridge_id = str(row["bridge_id"] or "")
                route_action = str(row["route_action"])
                desired = (
                    "routing"
                    if action == "retry" and route_action == "unresolved"
                    else {
                        "retry": "pending",
                        "complete": "completed",
                        "abandon": "cancelled",
                    }[action]
                )
                if current == desired:
                    return {
                        "kind": kind,
                        "id": operation_id,
                        "action": action,
                        "state": desired,
                        "bridge_id": bridge_id,
                        "deduplicated": True,
                    }
                if current != "uncertain":
                    raise ValueError("ingress is not awaiting operator resolution")
                if action == "retry" and route_action not in {
                    "hermes",
                    "unresolved",
                }:
                    raise ValueError(
                        "native ingress retry must resolve its delivery attempt"
                    )
                changed = db.execute(
                    """
                    UPDATE thread_ingress
                    SET state=?,lease_id=NULL,lease_owner=NULL,
                        lease_expires_at=NULL,error_code=?,
                        egress_sealed=CASE WHEN ?='retry' THEN 0
                                           ELSE egress_sealed END,
                        updated_at=CURRENT_TIMESTAMP
                    WHERE event_id=? AND state='uncertain'
                    """,
                    (
                        desired,
                        f"operator_{action}",
                        action,
                        operation_id,
                    ),
                )
                if changed.rowcount == 1 and action in {"complete", "abandon"}:
                    target_messages = db.execute(
                        """
                        SELECT client_msg_id FROM slack_messages
                        WHERE ingress_event_id=? AND message_ts IS NULL
                          AND state!='sent'
                        """,
                        (operation_id,),
                    ).fetchall()
                    db.execute(
                        """
                        UPDATE slack_messages
                        SET state='cancelled',lease_id=NULL,lease_owner=NULL,
                            lease_expires_at=NULL,
                            error='operator_resolved_ingress',
                            updated_at=CURRENT_TIMESTAMP
                        WHERE ingress_event_id=? AND message_ts IS NULL
                          AND state!='sent'
                        """,
                        (operation_id,),
                    )
                    for message in target_messages:
                        db.execute(
                            """
                            UPDATE slack_reconciliations
                            SET state='abandoned',error='operator_resolved_ingress',
                                updated_at=CURRENT_TIMESTAMP
                            WHERE target_kind='message' AND target_id=?
                              AND state IN ('pending','failed')
                            """,
                            (str(message["client_msg_id"]),),
                        )
            elif kind == "attempt":
                row = db.execute(
                    """
                    SELECT attempts.state,attempts.bridge_id
                    FROM bridge_attempts AS attempts
                    JOIN bridges ON bridges.bridge_id=attempts.bridge_id
                    WHERE attempts.attempt_id=? AND bridges.team_id=?
                    """,
                    (operation_id, team_id),
                ).fetchone()
                if row is None:
                    raise ValueError("uncertain delivery attempt not found")
                current = str(row["state"])
                bridge_id = str(row["bridge_id"])
                desired = {
                    "retry": "requeued",
                    "complete": "acknowledged",
                    "abandon": "cancelled",
                }[action]
                if current == desired:
                    return {
                        "kind": kind,
                        "id": operation_id,
                        "action": action,
                        "state": desired,
                        "bridge_id": bridge_id,
                        "deduplicated": True,
                    }
                if current != "uncertain":
                    raise ValueError(
                        "delivery attempt is not awaiting operator resolution"
                    )
                changed = db.execute(
                    """
                    UPDATE bridge_attempts
                    SET state=?,error_code=?,
                        ack_kind=CASE WHEN ?='complete'
                                      THEN 'operator_confirmed' ELSE ack_kind END,
                        acknowledged_at=CASE WHEN ?='complete'
                                             THEN CURRENT_TIMESTAMP
                                             ELSE acknowledged_at END,
                        updated_at=CURRENT_TIMESTAMP
                    WHERE attempt_id=? AND bridge_id=? AND state='uncertain'
                    """,
                    (
                        desired,
                        f"operator_{action}",
                        action,
                        action,
                        operation_id,
                        bridge_id,
                    ),
                )
                if changed.rowcount == 1:
                    if action == "retry":
                        db.execute(
                            """
                            UPDATE bridge_events
                            SET state='queued',attempt_id=NULL,
                                binding_generation=NULL,error=NULL,
                                updated_at=CURRENT_TIMESTAMP
                            WHERE bridge_id=? AND attempt_id=?
                              AND state='uncertain'
                            """,
                            (bridge_id, operation_id),
                        )
                    else:
                        db.execute(
                            """
                            UPDATE bridge_events
                            SET state=?,error=?,updated_at=CURRENT_TIMESTAMP
                            WHERE bridge_id=? AND attempt_id=?
                              AND state='uncertain'
                            """,
                            (
                                "delivered" if action == "complete" else "failed",
                                None if action == "complete" else "operator_abandon",
                                bridge_id,
                                operation_id,
                            ),
                        )
            else:
                if action == "complete":
                    raise ValueError(
                        "reconciliation completion requires verified Slack evidence; "
                        "retry or abandon it"
                    )
                row = db.execute(
                    """
                    SELECT state,target_kind,target_id
                    FROM slack_reconciliations
                    WHERE reconciliation_key=? AND team_id=?
                    """,
                    (operation_id, team_id),
                ).fetchone()
                if row is None:
                    raise ValueError("failed Slack reconciliation not found")
                current = str(row["state"])
                target_kind = str(row["target_kind"])
                target_id = str(row["target_id"])
                desired = "pending" if action == "retry" else "abandoned"
                bridge_id = ""
                if current == desired:
                    return {
                        "kind": kind,
                        "id": operation_id,
                        "action": action,
                        "state": desired,
                        "bridge_id": bridge_id,
                        "deduplicated": True,
                    }
                if current != "failed":
                    raise ValueError(
                        "Slack reconciliation is not awaiting operator resolution"
                    )
                if action == "retry":
                    changed = db.execute(
                        """
                        UPDATE slack_reconciliations
                        SET next_cursor='',seen_cursors_json='[]',pages_seen=0,
                            state='pending',result_ts=NULL,error=NULL,
                            next_attempt_at=CURRENT_TIMESTAMP,
                            updated_at=CURRENT_TIMESTAMP
                        WHERE reconciliation_key=? AND team_id=? AND state='failed'
                        """,
                        (operation_id, team_id),
                    )
                else:
                    changed = db.execute(
                        """
                        UPDATE slack_reconciliations
                        SET state='abandoned',next_cursor='',error='operator_abandon',
                            updated_at=CURRENT_TIMESTAMP
                        WHERE reconciliation_key=? AND team_id=? AND state='failed'
                        """,
                        (operation_id, team_id),
                    )
                    if changed.rowcount == 1:
                        if target_kind == "root":
                            bridge_id = target_id.partition(":")[0]
                            db.execute(
                                """
                                UPDATE bridge_roots
                                SET state='cancelled',lease_id=NULL,lease_owner=NULL,
                                    lease_expires_at=NULL,error='operator_abandon',
                                    updated_at=CURRENT_TIMESTAMP
                                WHERE bridge_id=? AND state!='complete'
                                """,
                                (bridge_id,),
                            )
                        elif target_kind == "file":
                            target = db.execute(
                                """
                                SELECT bridge_id FROM bridge_roots
                                WHERE slack_file_id=?
                                """,
                                (target_id,),
                            ).fetchone()
                            bridge_id = (
                                str(target["bridge_id"]) if target is not None else ""
                            )
                            db.execute(
                                """
                                UPDATE bridge_roots
                                SET state='complete',upload_phase='abandoned',
                                    lease_id=NULL,lease_owner=NULL,
                                    lease_expires_at=NULL,error='operator_abandon',
                                    updated_at=CURRENT_TIMESTAMP
                                WHERE slack_file_id=? AND state!='complete'
                                """,
                                (target_id,),
                            )
                        elif target_kind == "reply":
                            target = db.execute(
                                """
                                SELECT reply_key,bridge_id FROM bridge_replies
                                WHERE client_msg_id=?
                                """,
                                (target_id,),
                            ).fetchone()
                            if target is not None:
                                reply_key = str(target["reply_key"])
                                bridge_id = str(target["bridge_id"])
                                db.execute(
                                    """
                                    UPDATE bridge_replies
                                    SET state='cancelled',lease_id=NULL,
                                        lease_owner=NULL,lease_expires_at=NULL,
                                        error='operator_abandon',
                                        updated_at=CURRENT_TIMESTAMP
                                    WHERE reply_key=? AND message_ts IS NULL
                                    """,
                                    (reply_key,),
                                )
                                db.execute(
                                    """
                                    UPDATE bridge_attempts
                                    SET state='cancelled',
                                        error_code='operator_abandon',
                                        updated_at=CURRENT_TIMESTAMP
                                    WHERE attempt_id=? AND bridge_id=?
                                      AND state!='acknowledged'
                                    """,
                                    (reply_key, bridge_id),
                                )
                                db.execute(
                                    """
                                    UPDATE bridge_events
                                    SET state='failed',error='operator_abandon',
                                        updated_at=CURRENT_TIMESTAMP
                                    WHERE bridge_id=? AND attempt_id=?
                                      AND state!='delivered'
                                    """,
                                    (bridge_id, reply_key),
                                )
                        elif target_kind == "message":
                            db.execute(
                                """
                                UPDATE slack_messages
                                SET state='cancelled',lease_id=NULL,lease_owner=NULL,
                                    lease_expires_at=NULL,error='operator_abandon',
                                    updated_at=CURRENT_TIMESTAMP
                                WHERE client_msg_id=? AND message_ts IS NULL
                                """,
                                (target_id,),
                            )
            if changed.rowcount != 1:
                raise RuntimeError("uncertain operation changed during resolution")
        return {
            "kind": kind,
            "id": operation_id,
            "action": action,
            "state": desired,
            "bridge_id": bridge_id,
            "deduplicated": False,
        }

    def recoverable_hermes_ingress(
        self,
        *,
        limit: int = 20,
        now: datetime.datetime | None = None,
    ) -> list[dict[str, Any]]:
        limit = max(1, min(int(limit), 100))
        current = now or datetime.datetime.now(datetime.timezone.utc)
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            self._expire_dispatched_thread_ingress(db)
            rows = db.execute(
                """
                SELECT event_id,team_id,channel_id,thread_ts,writer_id,
                       bridge_id,binding_generation,payload_json,retry_count,
                       state,updated_at
                FROM thread_ingress
                WHERE route_action='hermes'
                  AND (
                    state='pending'
                    OR (
                      state='processing'
                      AND (
                        lease_expires_at IS NULL
                        OR datetime(lease_expires_at) <= CURRENT_TIMESTAMP
                      )
                    )
                  )
                ORDER BY updated_at,event_id
                LIMIT ?
                """,
                (limit * 4,),
            ).fetchall()
        recovered: list[dict[str, Any]] = []
        for row in rows:
            retry_count = max(1, int(row["retry_count"] or 1))
            delay_seconds = min(300, 2 ** min(retry_count, 8))
            try:
                updated, payload = self._decode_recoverable_ingress(row)
            except (TypeError, ValueError, json.JSONDecodeError):
                self.quarantine_invalid_thread_ingress(str(row["event_id"]))
                continue
            if (current - updated).total_seconds() < delay_seconds:
                continue
            recovered.append(
                {
                    "event_id": str(row["event_id"]),
                    "team_id": str(row["team_id"]),
                    "channel_id": str(row["channel_id"]),
                    "thread_ts": str(row["thread_ts"]),
                    "writer_id": str(row["writer_id"]),
                    "bridge_id": str(row["bridge_id"] or ""),
                    "binding_generation": (
                        int(row["binding_generation"])
                        if row["binding_generation"] is not None
                        else None
                    ),
                    "payload": payload,
                    "retry_count": retry_count,
                    "state": str(row["state"]),
                }
            )
            if len(recovered) >= limit:
                break
        return recovered

    def active_bridges(self) -> list[Bridge]:
        with self.connect() as db:
            rows = db.execute(
                """
                SELECT * FROM bridges
                WHERE status='active' AND thread_ts IS NOT NULL
                ORDER BY updated_at DESC,created_at DESC
                """,
            ).fetchall()
            return [bridge for row in rows if (bridge := self.decode(row)) is not None]

    def recent_active_bridges(self, hours: int = 24, limit: int = 100) -> list[Bridge]:
        """Compatibility query for diagnostics; recovery uses all active bindings."""
        hours = max(1, min(hours, 24 * 90))
        limit = max(1, min(limit, 2_000))
        with self.connect() as db:
            rows = db.execute(
                """
                SELECT * FROM bridges
                WHERE status='active' AND thread_ts IS NOT NULL
                  AND updated_at >= datetime('now', ?)
                ORDER BY updated_at DESC,created_at DESC LIMIT ?
                """,
                (f"-{hours} hours", limit),
            ).fetchall()
            return [
                bridge for row in rows
                if (bridge := self.decode(row)) is not None
            ]

    def has_ingress(self, event_id: str) -> bool:
        with self.connect() as db:
            return bool(
                db.execute("SELECT 1 FROM bridge_events WHERE event_id=?", (event_id,)).fetchone()
                or db.execute("SELECT 1 FROM thread_ingress WHERE event_id=?", (event_id,)).fetchone()
            )

    def claim_event(self, event_id: str, bridge_id: str) -> bool:
        with self.connect() as db:
            bridge = db.execute(
                """
                SELECT binding_generation FROM bridges
                WHERE bridge_id=? AND status='active'
                """,
                (bridge_id,),
            ).fetchone()
            if bridge is None:
                return False
            try:
                db.execute(
                    """
                    INSERT INTO bridge_events(
                      event_id,bridge_id,state,binding_generation
                    ) VALUES(?,?,'processing',?)
                    """,
                    (event_id, bridge_id, int(bridge["binding_generation"])),
                )
                return True
            except sqlite3.IntegrityError:
                return False

    def enqueue_event(self, event_id: str, bridge_id: str, text: str) -> bool:
        if not event_id or not text.strip() or len(text) > MAX_TEXT:
            return False
        payload = json.dumps({"text": text}, ensure_ascii=False, separators=(",", ":"))
        with self.connect() as db:
            bridge = db.execute(
                """
                SELECT binding_generation FROM bridges
                WHERE bridge_id=? AND status='active'
                """,
                (bridge_id,),
            ).fetchone()
            if bridge is None:
                return False
            try:
                db.execute(
                    """
                    INSERT INTO bridge_events(
                      event_id,bridge_id,state,payload_json,binding_generation,
                      updated_at
                    ) VALUES(?,?,'queued',?,?,CURRENT_TIMESTAMP)
                    """,
                    (
                        event_id,
                        bridge_id,
                        payload,
                        int(bridge["binding_generation"]),
                    ),
                )
                return True
            except sqlite3.IntegrityError:
                return False

    def claim_next_event(self, bridge_id: str) -> dict[str, str] | None:
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            bridge = db.execute(
                """
                SELECT binding_generation FROM bridges
                WHERE bridge_id=? AND status='active'
                """,
                (bridge_id,),
            ).fetchone()
            if bridge is None:
                return None
            generation = int(bridge["binding_generation"])
            db.execute(
                """
                UPDATE bridge_events
                SET state='failed',error='binding_generation_changed',
                    updated_at=CURRENT_TIMESTAMP
                WHERE bridge_id=? AND state='queued'
                  AND binding_generation IS NOT NULL
                  AND binding_generation!=?
                """,
                (bridge_id, generation),
            )
            if db.execute(
                """
                SELECT 1 FROM bridge_events
                WHERE bridge_id=? AND state IN (
                  'processing','prepared','submitting','uncertain',
                  'awaiting_ack','replying'
                )
                """,
                (bridge_id,),
            ).fetchone():
                return None
            row = db.execute(
                """
                SELECT event_id,payload_json FROM bridge_events
                WHERE bridge_id=? AND state='queued'
                  AND (binding_generation IS NULL OR binding_generation=?)
                ORDER BY created_at,event_id LIMIT 1
                """,
                (bridge_id, generation),
            ).fetchone()
            if row is None:
                return None
            db.execute("UPDATE bridge_events SET state='processing',updated_at=CURRENT_TIMESTAMP WHERE event_id=?", (row["event_id"],))
            payload = json.loads(row["payload_json"] or "{}")
            return {"event_id": str(row["event_id"]), "text": str(payload.get("text") or "")}

    def claim_event_batch(self, bridge_id: str, limit: int = 20) -> list[dict[str, str]]:
        """Claim the currently queued follow-ups as one agent turn."""
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            bridge = db.execute(
                """
                SELECT binding_generation FROM bridges
                WHERE bridge_id=? AND status='active'
                """,
                (bridge_id,),
            ).fetchone()
            if bridge is None:
                return []
            generation = int(bridge["binding_generation"])
            db.execute(
                """
                UPDATE bridge_events
                SET state='failed',error='binding_generation_changed',
                    updated_at=CURRENT_TIMESTAMP
                WHERE bridge_id=? AND state='queued'
                  AND binding_generation IS NOT NULL
                  AND binding_generation!=?
                """,
                (bridge_id, generation),
            )
            if db.execute(
                """
                SELECT 1 FROM bridge_events
                WHERE bridge_id=? AND state IN (
                  'processing','prepared','submitting','uncertain',
                  'awaiting_ack','replying'
                )
                """,
                (bridge_id,),
            ).fetchone():
                return []
            rows = db.execute(
                """
                SELECT event_id,payload_json FROM bridge_events
                WHERE bridge_id=? AND state='queued'
                  AND (binding_generation IS NULL OR binding_generation=?)
                ORDER BY created_at,event_id LIMIT ?
                """,
                (
                    bridge_id,
                    generation,
                    max(1, min(limit, 100)),
                ),
            ).fetchall()
            if not rows:
                return []
            event_ids = [str(row["event_id"]) for row in rows]
            db.executemany(
                """
                UPDATE bridge_events SET state='processing',updated_at=CURRENT_TIMESTAMP
                WHERE event_id=?
                """,
                ((event_id,) for event_id in event_ids),
            )
            return [
                {
                    "event_id": str(row["event_id"]),
                    "text": str(json.loads(row["payload_json"] or "{}").get("text") or ""),
                }
                for row in rows
            ]

    def pending_count(self, bridge_id: str) -> int:
        with self.connect() as db:
            return int(db.execute(
                """
                SELECT count(*) FROM bridge_events
                WHERE bridge_id=?
                  AND state IN (
                    'queued','processing','prepared','submitting','uncertain',
                    'awaiting_ack','replying'
                  )
                """,
                (bridge_id,),
            ).fetchone()[0])

    def queued_bridge_ids(self) -> list[str]:
        with self.connect() as db:
            return [str(row[0]) for row in db.execute("SELECT DISTINCT bridge_id FROM bridge_events WHERE state='queued'")]

    def cancel_queued(self, bridge_id: str) -> int:
        with self.connect() as db:
            cursor = db.execute(
                "UPDATE bridge_events SET state='failed',error='cancelled before start',updated_at=CURRENT_TIMESTAMP WHERE bridge_id=? AND state='queued'",
                (bridge_id,),
            )
            return int(cursor.rowcount)

    def requeue_processing(self) -> None:
        with self.connect() as db:
            db.execute("UPDATE bridge_events SET state='queued',error=NULL,updated_at=CURRENT_TIMESTAMP WHERE state='processing'")

    def finish_event(self, event_id: str, error: str | None = None) -> None:
        safe_error = security.redact_egress_text(error or "")[:1000] or None
        with self.connect() as db:
            db.execute(
                """
                UPDATE bridge_events
                SET state=?,error=?,updated_at=CURRENT_TIMESTAMP
                WHERE event_id=? AND state='processing'
                """,
                ("failed" if safe_error else "delivered", safe_error, event_id),
            )

    def prepare_delivery_attempt(
        self,
        event_ids: list[str],
        bridge_id: str,
        binding_generation: int,
        attempt_id: str,
        delivery_kind: str = "zellij",
    ) -> bool:
        if (
            not event_ids
            or not REPLY_KEY_PATTERN.fullmatch(attempt_id)
            or binding_generation < 1
            or delivery_kind not in {"zellij", "herdr", "detached_native"}
        ):
            return False
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            bridge = db.execute(
                """
                SELECT binding_generation FROM bridges
                WHERE bridge_id=? AND status='active'
                """,
                (bridge_id,),
            ).fetchone()
            if bridge is None or int(bridge["binding_generation"]) != binding_generation:
                return False
            placeholders = ",".join("?" for _ in event_ids)
            # event_ids only determines bound-placeholder count; IDs stay parameters.
            statement = f"SELECT event_id FROM bridge_events WHERE bridge_id=? AND state='processing' AND event_id IN ({placeholders})"  # nosec
            rows = db.execute(statement, (bridge_id, *event_ids)).fetchall()
            if {str(row["event_id"]) for row in rows} != set(event_ids):
                return False
            try:
                db.execute(
                    """
                    INSERT INTO bridge_attempts(
                      attempt_id,reply_key,bridge_id,binding_generation,
                      delivery_kind,state
                    ) VALUES(?,?,?,?,?,'prepared')
                    """,
                    (
                        attempt_id,
                        attempt_id,
                        bridge_id,
                        binding_generation,
                        delivery_kind,
                    ),
                )
            except sqlite3.IntegrityError:
                existing = db.execute(
                    """
                    SELECT bridge_id,binding_generation,delivery_kind,state
                    FROM bridge_attempts WHERE attempt_id=?
                    """,
                    (attempt_id,),
                ).fetchone()
                if (
                    existing is None
                    or str(existing["bridge_id"]) != bridge_id
                    or int(existing["binding_generation"]) != binding_generation
                    or str(existing["delivery_kind"]) != delivery_kind
                    or str(existing["state"]) != "requeued"
                ):
                    return False
                reset = db.execute(
                    """
                    UPDATE bridge_attempts
                    SET state='prepared',ack_kind=NULL,message_ts=NULL,
                        error_code=NULL,submitted_at=NULL,acknowledged_at=NULL,
                        updated_at=CURRENT_TIMESTAMP
                    WHERE attempt_id=? AND bridge_id=? AND state='requeued'
                    """,
                    (attempt_id, bridge_id),
                )
                if reset.rowcount != 1:
                    return False
            statement = f"UPDATE bridge_events SET state='prepared',attempt_id=?,binding_generation=?,updated_at=CURRENT_TIMESTAMP WHERE bridge_id=? AND state='processing' AND event_id IN ({placeholders})"  # nosec
            db.execute(
                statement,
                (attempt_id, binding_generation, bridge_id, *event_ids),
            )
            return True

    def mark_attempt_submitting(
        self,
        attempt_id: str,
        bridge_id: str,
        binding_generation: int,
    ) -> bool:
        return self._transition_attempt(
            attempt_id,
            bridge_id,
            binding_generation,
            from_states=("prepared",),
            to_state="submitting",
            event_state="submitting",
            mark_submitted=True,
        )

    def mark_attempt_uncertain(
        self,
        attempt_id: str,
        bridge_id: str,
        error_code: str = "terminal_submit_uncertain",
    ) -> bool:
        safe_code = _safe_label(error_code, 128) or "terminal_submit_uncertain"
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            attempt = db.execute(
                """
                UPDATE bridge_attempts
                SET state='uncertain',error_code=?,updated_at=CURRENT_TIMESTAMP
                WHERE attempt_id=? AND bridge_id=? AND state='submitting'
                """,
                (safe_code, attempt_id, bridge_id),
            )
            if attempt.rowcount != 1:
                return False
            db.execute(
                """
                UPDATE bridge_events
                SET state='uncertain',error=?,updated_at=CURRENT_TIMESTAMP
                WHERE bridge_id=? AND attempt_id=? AND state='submitting'
                """,
                (safe_code, bridge_id, attempt_id),
            )
            return True

    def mark_attempt_interrupt_unverified(
        self,
        attempt_id: str,
        bridge_id: str,
        binding_generation: int,
    ) -> bool:
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            current = db.execute(
                """
                SELECT state FROM bridge_attempts
                WHERE attempt_id=? AND bridge_id=? AND binding_generation=?
                  AND delivery_kind IN ('zellij','herdr')
                """,
                (attempt_id, bridge_id, binding_generation),
            ).fetchone()
            if current is None:
                return False
            if str(current["state"]) == "uncertain":
                return True
            attempt = db.execute(
                """
                UPDATE bridge_attempts
                SET state='uncertain',
                    error_code='terminal_interrupt_unverified',
                    updated_at=CURRENT_TIMESTAMP
                WHERE attempt_id=? AND bridge_id=? AND binding_generation=?
                  AND delivery_kind IN ('zellij','herdr')
                  AND state IN ('submitting','awaiting_ack')
                """,
                (attempt_id, bridge_id, binding_generation),
            )
            if attempt.rowcount != 1:
                return False
            events = db.execute(
                """
                UPDATE bridge_events
                SET state='uncertain',error='terminal_interrupt_unverified',
                    updated_at=CURRENT_TIMESTAMP
                WHERE bridge_id=? AND attempt_id=? AND binding_generation=?
                  AND state IN ('submitting','awaiting_ack')
                """,
                (bridge_id, attempt_id, binding_generation),
            )
            if events.rowcount <= 0:
                raise RuntimeError(
                    "unverified terminal interrupt has no matching live events"
                )
            return True

    def requeue_prepared_attempt(
        self,
        attempt_id: str,
        bridge_id: str,
        error_code: str = "submission_not_started",
    ) -> bool:
        safe_code = _safe_label(error_code, 128) or "submission_not_started"
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            attempt = db.execute(
                """
                UPDATE bridge_attempts
                SET state='requeued',error_code=?,updated_at=CURRENT_TIMESTAMP
                WHERE attempt_id=? AND bridge_id=?
                  AND state IN ('prepared','submitting')
                """,
                (safe_code, attempt_id, bridge_id),
            )
            if attempt.rowcount != 1:
                return False
            db.execute(
                """
                UPDATE bridge_events
                SET state='queued',attempt_id=NULL,binding_generation=NULL,
                    error=NULL,updated_at=CURRENT_TIMESTAMP
                WHERE bridge_id=? AND attempt_id=?
                  AND state IN ('prepared','submitting')
                """,
                (bridge_id, attempt_id),
            )
            return True

    def _transition_attempt(
        self,
        attempt_id: str,
        bridge_id: str,
        binding_generation: int,
        *,
        from_states: tuple[str, ...],
        to_state: str,
        event_state: str,
        mark_submitted: bool = False,
    ) -> bool:
        if not REPLY_KEY_PATTERN.fullmatch(attempt_id) or not from_states:
            return False
        placeholders = ",".join("?" for _ in from_states)
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            bridge = db.execute(
                """
                SELECT binding_generation FROM bridges
                WHERE bridge_id=? AND status='active'
                """,
                (bridge_id,),
            ).fetchone()
            if (
                bridge is None
                or int(bridge["binding_generation"]) != binding_generation
            ):
                return False
            # SQL fragments are fixed literals; states remain bound parameters.
            submitted_clause = (
                ",submitted_at=CURRENT_TIMESTAMP" if mark_submitted else ""
            )
            statement = f"UPDATE bridge_attempts SET state=?,updated_at=CURRENT_TIMESTAMP{submitted_clause} WHERE attempt_id=? AND bridge_id=? AND binding_generation=? AND state IN ({placeholders})"  # nosec
            attempt = db.execute(
                statement,
                (
                    to_state,
                    attempt_id,
                    bridge_id,
                    binding_generation,
                    *from_states,
                ),
            )
            if attempt.rowcount != 1:
                current = db.execute(
                    """
                    SELECT state FROM bridge_attempts
                    WHERE attempt_id=? AND bridge_id=? AND binding_generation=?
                    """,
                    (attempt_id, bridge_id, binding_generation),
                ).fetchone()
                return bool(
                    current is not None
                    and str(current["state"]) in {"replying", "acknowledged"}
                )
            statement = f"UPDATE bridge_events SET state=?,updated_at=CURRENT_TIMESTAMP WHERE bridge_id=? AND attempt_id=? AND binding_generation=? AND state IN ({placeholders})"  # nosec
            events = db.execute(
                statement,
                (
                    event_state,
                    bridge_id,
                    attempt_id,
                    binding_generation,
                    *from_states,
                ),
            )
            if events.rowcount <= 0:
                raise RuntimeError("delivery attempt event state is inconsistent")
            return True

    def mark_attempt_awaiting_ack(
        self,
        attempt_id: str,
        bridge_id: str,
        binding_generation: int,
    ) -> bool:
        return self._transition_attempt(
            attempt_id,
            bridge_id,
            binding_generation,
            from_states=("prepared", "submitting", "uncertain"),
            to_state="awaiting_ack",
            event_state="awaiting_ack",
        )

    def acknowledge_attempt(
        self,
        attempt_id: str,
        bridge_id: str,
        ack_kind: str = "reply",
        message_ts: str = "",
    ) -> int:
        if not REPLY_KEY_PATTERN.fullmatch(attempt_id):
            return 0
        if ack_kind not in {"reply", "no_reply"}:
            raise ValueError("invalid acknowledgment kind")
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            attempt = db.execute(
                """
                SELECT binding_generation,state FROM bridge_attempts
                WHERE attempt_id=? AND reply_key=? AND bridge_id=?
                """,
                (attempt_id, attempt_id, bridge_id),
            ).fetchone()
            if attempt is None:
                return 0
            if str(attempt["state"]) == "acknowledged":
                return 0
            if str(attempt["state"]) not in {
                "submitting", "uncertain", "awaiting_ack", "replying"
            }:
                return 0
            current = db.execute(
                "SELECT binding_generation FROM bridges WHERE bridge_id=?",
                (bridge_id,),
            ).fetchone()
            if (
                current is None
                or int(current["binding_generation"])
                != int(attempt["binding_generation"])
            ):
                return 0
            cursor = db.execute(
                """
                UPDATE bridge_events
                SET state='delivered',error=NULL,updated_at=CURRENT_TIMESTAMP
                WHERE bridge_id=? AND attempt_id=?
                  AND state IN (
                    'submitting','uncertain','awaiting_ack','replying'
                  )
                """,
                (bridge_id, attempt_id),
            )
            db.execute(
                """
                UPDATE bridge_attempts
                SET state='acknowledged',ack_kind=?,message_ts=?,
                    acknowledged_at=CURRENT_TIMESTAMP,updated_at=CURRENT_TIMESTAMP
                WHERE attempt_id=? AND bridge_id=?
                """,
                (ack_kind, message_ts or None, attempt_id, bridge_id),
            )
            return int(cursor.rowcount)

    def fail_attempt(self, attempt_id: str, bridge_id: str, error: str) -> int:
        safe_error = security.redact_egress_text(error)[:1000]
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            cursor = db.execute(
                """
                UPDATE bridge_events
                SET state='failed',error=?,updated_at=CURRENT_TIMESTAMP
                WHERE bridge_id=? AND attempt_id=?
                  AND state IN (
                    'prepared','submitting','uncertain','awaiting_ack','replying'
                  )
                """,
                (safe_error, bridge_id, attempt_id),
            )
            db.execute(
                """
                UPDATE bridge_attempts
                SET state='failed',error_code=?,updated_at=CURRENT_TIMESTAMP
                WHERE attempt_id=? AND bridge_id=?
                  AND state IN (
                    'prepared','submitting','uncertain','awaiting_ack','replying'
                  )
                """,
                (safe_error, attempt_id, bridge_id),
            )
            return int(cursor.rowcount)

    def attempt_state(self, attempt_id: str, bridge_id: str) -> str | None:
        with self.connect() as db:
            row = db.execute(
                """
                SELECT state FROM bridge_attempts
                WHERE bridge_id=? AND attempt_id=?
                """,
                (bridge_id, attempt_id),
            ).fetchone()
            return str(row["state"]) if row is not None else None

    def active_live_attempt(
        self,
        bridge_id: str,
        delivery_kind: str | None = None,
    ) -> dict[str, Any] | None:
        if delivery_kind not in {None, "zellij", "herdr"}:
            raise ValueError("invalid live delivery kind")
        kind_clause = "AND delivery_kind=?" if delivery_kind else "AND delivery_kind IN ('zellij','herdr')"
        parameters: tuple[Any, ...] = (
            (bridge_id, delivery_kind)
            if delivery_kind
            else (bridge_id,)
        )
        with self.connect() as db:
            row = db.execute(
                f"""
                SELECT attempt_id,binding_generation,delivery_kind,state
                FROM bridge_attempts
                WHERE bridge_id=? {kind_clause}
                  AND state IN ('submitting','uncertain','awaiting_ack')
                ORDER BY updated_at DESC,created_at DESC LIMIT 1
                """,  # nosec B608 - kind_clause is a fixed literal above
                parameters,
            ).fetchone()
            if row is None:
                return None
            return {
                "attempt_id": str(row["attempt_id"]),
                "binding_generation": int(row["binding_generation"]),
                "delivery_kind": str(row["delivery_kind"]),
                "state": str(row["state"]),
            }

    def active_zellij_attempt(self, bridge_id: str) -> dict[str, Any] | None:
        return self.active_live_attempt(bridge_id, "zellij")

    def cancel_zellij_attempt(
        self,
        attempt_id: str,
        bridge_id: str,
        binding_generation: int,
        *,
        reason: str = "operator_cancelled",
    ) -> int:
        return self.cancel_live_attempt(
            attempt_id,
            bridge_id,
            binding_generation,
            delivery_kind="zellij",
            reason=reason,
        )

    def cancel_live_attempt(
        self,
        attempt_id: str,
        bridge_id: str,
        binding_generation: int,
        *,
        delivery_kind: str,
        reason: str = "operator_cancelled",
    ) -> int:
        if delivery_kind not in {"zellij", "herdr"}:
            raise ValueError("invalid live delivery kind")
        safe_reason = _safe_label(reason, 128) or "operator_cancelled"
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            bridge = db.execute(
                """
                SELECT binding_generation FROM bridges
                WHERE bridge_id=? AND status='active'
                """,
                (bridge_id,),
            ).fetchone()
            if (
                bridge is None
                or int(bridge["binding_generation"]) != binding_generation
            ):
                return 0
            cancelled = db.execute(
                """
                UPDATE bridge_attempts
                SET state='cancelled',error_code=?,
                    updated_at=CURRENT_TIMESTAMP
                WHERE attempt_id=? AND bridge_id=?
                  AND binding_generation=? AND delivery_kind=?
                  AND state IN ('submitting','uncertain','awaiting_ack')
                """,
                (
                    safe_reason,
                    attempt_id,
                    bridge_id,
                    binding_generation,
                    delivery_kind,
                ),
            )
            if cancelled.rowcount != 1:
                return 0
            events = db.execute(
                """
                UPDATE bridge_events
                SET state='failed',error=?,updated_at=CURRENT_TIMESTAMP
                WHERE bridge_id=? AND attempt_id=? AND binding_generation=?
                  AND state IN ('submitting','uncertain','awaiting_ack')
                """,
                (
                    safe_reason,
                    bridge_id,
                    attempt_id,
                    binding_generation,
                ),
            )
            if events.rowcount <= 0:
                raise RuntimeError(
                    "cancelled live attempt has no matching live events"
                )
            return int(events.rowcount)

    def recover_delivery_attempts(
        self,
        timeout_seconds: int = 1800,
    ) -> dict[str, int]:
        # Time alone cannot prove whether an ambiguous terminal submission or
        # Slack reply ran. Only reconciliation or explicit operator action may
        # close these attempts.
        del timeout_seconds
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            prepared = db.execute(
                "SELECT attempt_id,bridge_id FROM bridge_attempts WHERE state='prepared'"
            ).fetchall()
            for row in prepared:
                db.execute(
                    """
                    UPDATE bridge_events
                    SET state='queued',attempt_id=NULL,binding_generation=NULL,
                        error=NULL,updated_at=CURRENT_TIMESTAMP
                    WHERE bridge_id=? AND attempt_id=? AND state='prepared'
                    """,
                    (row["bridge_id"], row["attempt_id"]),
                )
            if prepared:
                db.executemany(
                    """
                    UPDATE bridge_attempts
                    SET state='requeued',error_code='restart_before_submission',
                        updated_at=CURRENT_TIMESTAMP
                    WHERE attempt_id=? AND bridge_id=? AND state='prepared'
                    """,
                    (
                        (row["attempt_id"], row["bridge_id"])
                        for row in prepared
                    ),
                )
            uncertain = db.execute(
                """
                UPDATE bridge_attempts
                SET state='uncertain',error_code='restart_during_submission',
                    updated_at=CURRENT_TIMESTAMP
                WHERE state='submitting'
                """
            )
            db.execute(
                """
                UPDATE bridge_events
                SET state='uncertain',error='restart_during_submission',
                    updated_at=CURRENT_TIMESTAMP
                WHERE state='submitting'
                """
            )
            return {
                "requeued": len(prepared),
                "uncertain": int(uncertain.rowcount),
                "expired": 0,
            }

    def validate_attempt_reply(self, reply_key: str, bridge_id: str) -> None:
        if not REPLY_KEY_PATTERN.fullmatch(reply_key):
            raise ValueError("invalid reply key")
        with self.connect() as db:
            attempt = db.execute(
                """
                SELECT binding_generation,state FROM bridge_attempts
                WHERE attempt_id=? AND reply_key=? AND bridge_id=?
                """,
                (reply_key, reply_key, bridge_id),
            ).fetchone()
            current = db.execute(
                """
                SELECT binding_generation FROM bridges
                WHERE bridge_id=? AND status='active'
                """,
                (bridge_id,),
            ).fetchone()
            if (
                attempt is None
                or current is None
                or int(attempt["binding_generation"])
                != int(current["binding_generation"])
                or str(attempt["state"])
                not in {
                    "submitting", "uncertain", "awaiting_ack", "replying"
                }
            ):
                raise ValueError(
                    "reply key is not attached to a live delivery attempt"
                )

    def stage_reply(
        self,
        reply_key: str,
        bridge_id: str,
        text: str,
        text_hash: str,
        client_msg_id: str,
    ) -> tuple[str, str]:
        if not re.fullmatch(r"[0-9a-f]{64}", text_hash):
            raise ValueError("invalid reply content hash")
        if (
            not text
            or len(text) > MAX_TEXT
            or not re.fullmatch(r"[0-9a-f-]{36}", client_msg_id)
        ):
            raise ValueError("invalid reply outbox payload")
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            attempt = db.execute(
                """
                SELECT binding_generation,state FROM bridge_attempts
                WHERE attempt_id=? AND reply_key=? AND bridge_id=?
                """,
                (reply_key, reply_key, bridge_id),
            ).fetchone()
            current = db.execute(
                """
                SELECT binding_generation FROM bridges
                WHERE bridge_id=? AND status='active'
                """,
                (bridge_id,),
            ).fetchone()
            row = db.execute(
                """
                SELECT bridge_id,message_ts,text_hash,payload_text,client_msg_id,state
                FROM bridge_replies WHERE reply_key=?
                """,
                (reply_key,),
            ).fetchone()
            if (
                attempt is None
                or current is None
                or int(attempt["binding_generation"])
                != int(current["binding_generation"])
            ):
                raise ValueError(
                    "reply key is not attached to a live delivery attempt"
                )
            if row is not None:
                if str(row["bridge_id"]) != bridge_id:
                    raise ValueError("reply key belongs to a different bridge")
                if str(row["text_hash"] or "") != text_hash:
                    raise ValueError("reply key was reused with different content")
                if row["payload_text"] not in {None, text}:
                    raise ValueError("reply key was reused with different payload")
                if row["client_msg_id"] not in {None, client_msg_id}:
                    raise ValueError("reply key was reused with a different Slack identity")
                if (
                    str(attempt["state"]) == "acknowledged"
                    and bool(row["message_ts"])
                ):
                    return "sent", str(row["message_ts"])
                if str(attempt["state"]) not in {
                    "submitting", "uncertain", "awaiting_ack", "replying"
                }:
                    raise ValueError(
                        "reply key is not attached to a live delivery attempt"
                    )
                db.execute(
                    """
                    UPDATE bridge_replies
                    SET payload_text=COALESCE(payload_text,?),
                        client_msg_id=COALESCE(client_msg_id,?),
                        state=CASE WHEN state='reserved' THEN 'pending' ELSE state END,
                        updated_at=CURRENT_TIMESTAMP
                    WHERE reply_key=?
                    """,
                    (text, client_msg_id, reply_key),
                )
                db.execute(
                    """
                    UPDATE bridge_attempts
                    SET state='replying',updated_at=CURRENT_TIMESTAMP
                    WHERE attempt_id=? AND bridge_id=?
                      AND state IN (
                        'submitting','uncertain','awaiting_ack','replying'
                      )
                    """,
                    (reply_key, bridge_id),
                )
                db.execute(
                    """
                    UPDATE bridge_events
                    SET state='replying',updated_at=CURRENT_TIMESTAMP
                    WHERE bridge_id=? AND attempt_id=?
                      AND state IN (
                        'submitting','uncertain','awaiting_ack','replying'
                      )
                    """,
                    (bridge_id, reply_key),
                )
                return (
                    "sent" if row["message_ts"] else "pending",
                    str(row["message_ts"] or ""),
                )
            if str(attempt["state"]) not in {
                "submitting", "uncertain", "awaiting_ack", "replying"
            }:
                raise ValueError(
                    "reply key is not attached to a live delivery attempt"
                )
            db.execute(
                """
                INSERT INTO bridge_replies(
                  reply_key,bridge_id,text_hash,payload_text,client_msg_id,
                  state,updated_at
                ) VALUES(?,?,?,?,?,'pending',CURRENT_TIMESTAMP)
                """,
                (reply_key, bridge_id, text_hash, text, client_msg_id),
            )
            db.execute(
                """
                UPDATE bridge_attempts
                SET state='replying',updated_at=CURRENT_TIMESTAMP
                WHERE attempt_id=? AND bridge_id=?
                  AND state IN (
                    'submitting','uncertain','awaiting_ack','replying'
                  )
                """,
                (reply_key, bridge_id),
            )
            db.execute(
                """
                UPDATE bridge_events
                SET state='replying',updated_at=CURRENT_TIMESTAMP
                WHERE bridge_id=? AND attempt_id=?
                  AND state IN (
                    'submitting','uncertain','awaiting_ack','replying'
                  )
                """,
                (bridge_id, reply_key),
            )
            return "pending", ""

    def claim_reply(
        self,
        reply_key: str,
        bridge_id: str,
        lease_seconds: int = 60,
        lease_owner: str = PROCESS_EPOCH,
    ) -> dict[str, Any]:
        if not re.fullmatch(r"[0-9a-f]{32}", lease_owner):
            raise ValueError("invalid reply lease owner")
        lease_seconds = max(10, min(int(lease_seconds), 300))
        modifier = f"+{lease_seconds} seconds"
        lease_id = "rpl_" + uuid.uuid4().hex
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                """
                SELECT bridge_id,message_ts,payload_text,client_msg_id,state,
                       lease_owner,lease_expires_at
                FROM bridge_replies WHERE reply_key=?
                """,
                (reply_key,),
            ).fetchone()
            if row is None or str(row["bridge_id"]) != bridge_id:
                raise ValueError("reply outbox record not found")
            if row["message_ts"] or str(row["state"]) == "sent":
                return {
                    "status": "sent",
                    "message_ts": str(row["message_ts"] or ""),
                }
            if not row["payload_text"] or not row["client_msg_id"]:
                raise RuntimeError("reply outbox payload is incomplete")
            if (
                str(row["state"]) == "delivering"
                and str(row["lease_owner"] or "") == lease_owner
            ):
                return {"status": "busy"}
            if (
                str(row["state"]) == "delivering"
                and row["lease_expires_at"]
                and db.execute(
                    "SELECT datetime(?) > CURRENT_TIMESTAMP",
                    (row["lease_expires_at"],),
                ).fetchone()[0]
            ):
                return {"status": "busy"}
            previous_state = str(row["state"])
            claimed = db.execute(
                """
                UPDATE bridge_replies
                SET state='delivering',lease_id=?,lease_owner=?,
                    lease_expires_at=datetime('now',?),
                    retry_count=retry_count+1,updated_at=CURRENT_TIMESTAMP
                WHERE reply_key=? AND bridge_id=? AND message_ts IS NULL
                """,
                (lease_id, lease_owner, modifier, reply_key, bridge_id),
            )
            if claimed.rowcount != 1:
                return {"status": "busy"}
            return {
                "status": "claimed",
                "lease_id": lease_id,
                "text": str(row["payload_text"]),
                "client_msg_id": str(row["client_msg_id"]),
                "previous_state": previous_state,
            }

    def complete_reply(
        self,
        reply_key: str,
        bridge_id: str,
        lease_id: str,
        message_ts: str,
    ) -> int:
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            completed = db.execute(
                """
                UPDATE bridge_replies
                SET message_ts=?,state='sent',error=NULL,lease_id=NULL,
                    lease_owner=NULL,lease_expires_at=NULL,
                    updated_at=CURRENT_TIMESTAMP
                WHERE reply_key=? AND bridge_id=? AND lease_id=?
                  AND state='delivering' AND message_ts IS NULL
                """,
                (message_ts, reply_key, bridge_id, lease_id),
            )
            if completed.rowcount != 1:
                row = db.execute(
                    """
                    SELECT message_ts,state FROM bridge_replies
                    WHERE reply_key=? AND bridge_id=?
                    """,
                    (reply_key, bridge_id),
                ).fetchone()
                if row is not None and row["message_ts"] == message_ts:
                    return 0
                raise RuntimeError("reply outbox lease was lost before completion")
            attempt = db.execute(
                """
                UPDATE bridge_attempts
                SET state='acknowledged',ack_kind='reply',message_ts=?,
                    acknowledged_at=CURRENT_TIMESTAMP,
                    updated_at=CURRENT_TIMESTAMP,error_code=NULL
                WHERE attempt_id=? AND reply_key=? AND bridge_id=?
                  AND state IN (
                    'submitting','uncertain','awaiting_ack','replying'
                  )
                """,
                (message_ts, reply_key, reply_key, bridge_id),
            )
            db.execute(
                """
                UPDATE bridge_events
                SET state='delivered',error=NULL,updated_at=CURRENT_TIMESTAMP
                WHERE bridge_id=? AND attempt_id=?
                  AND state IN (
                    'submitting','uncertain','awaiting_ack','replying'
                  )
                """,
                (bridge_id, reply_key),
            )
            return int(attempt.rowcount)

    def record_reply_error(
        self,
        reply_key: str,
        bridge_id: str,
        lease_id: str,
        error: str,
    ) -> None:
        with self.connect() as db:
            db.execute(
                """
                UPDATE bridge_replies
                SET state='uncertain',error=?,lease_id=NULL,
                    lease_owner=NULL,lease_expires_at=NULL,
                    updated_at=CURRENT_TIMESTAMP
                WHERE reply_key=? AND bridge_id=? AND lease_id=?
                  AND state='delivering' AND message_ts IS NULL
                """,
                (
                    security.redact_egress_text(
                        error or "Slack delivery uncertain"
                    )[:500],
                    reply_key,
                    bridge_id,
                    lease_id,
                ),
            )

    def release_abandoned_reply_leases(
        self,
        lease_owner: str = PROCESS_EPOCH,
    ) -> int:
        if not re.fullmatch(r"[0-9a-f]{32}", lease_owner):
            raise ValueError("invalid reply lease owner")
        with self.connect() as db:
            cursor = db.execute(
                """
                UPDATE bridge_replies
                SET state='uncertain',
                    error=CASE
                      WHEN lease_owner IS NULL
                      THEN 'reply_delivery_lease_owner_missing'
                      ELSE 'reply_delivery_lease_expired'
                    END,
                    lease_id=NULL,lease_owner=NULL,lease_expires_at=NULL,
                    updated_at=CURRENT_TIMESTAMP
                WHERE state='delivering' AND message_ts IS NULL
                  AND (lease_owner IS NULL OR lease_owner!=?)
                  AND (
                    lease_owner IS NULL
                    OR lease_expires_at IS NULL
                    OR datetime(lease_expires_at) <= CURRENT_TIMESTAMP
                  )
                """,
                (lease_owner,),
            )
            return int(cursor.rowcount)

    def pending_reply_keys(self) -> list[tuple[str, str]]:
        with self.connect() as db:
            return [
                (str(row["reply_key"]), str(row["bridge_id"]))
                for row in db.execute(
                    """
                    SELECT reply_key,bridge_id FROM bridge_replies
                    WHERE message_ts IS NULL
                      AND state IN ('pending','uncertain')
                    ORDER BY created_at,reply_key
                    """
                )
            ]

    @staticmethod
    def _prepare_slack_message_payload(
        text: str,
        options: dict[str, Any] | None,
    ) -> tuple[str, str, str]:
        if not text or len(text) > MAX_TEXT:
            raise ValueError("invalid Slack message payload")
        if options is not None and not isinstance(options, dict):
            raise ValueError("Slack message options must be an object")
        normalized_options = dict(options or {})
        if set(normalized_options) - {"blocks", "mrkdwn", "reply_broadcast"}:
            raise ValueError("unsupported Slack message option")
        if "blocks" in normalized_options and not isinstance(
            normalized_options["blocks"],
            list,
        ):
            raise ValueError("Slack message blocks must be a list")
        for name in ("mrkdwn", "reply_broadcast"):
            if name in normalized_options and not isinstance(
                normalized_options[name],
                bool,
            ):
                raise ValueError(f"Slack message option {name} must be boolean")
        try:
            safe_options = security.redact_egress_json(normalized_options)
            options_json = json.dumps(
                safe_options,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
                allow_nan=False,
            )
        except (TypeError, ValueError, RecursionError) as exc:
            raise ValueError(
                "Slack message options must be valid bounded JSON"
            ) from exc
        if len(options_json.encode("utf-8")) > 256 * 1024:
            raise ValueError("Slack message options are too large")
        payload_text = redact_text(text)[:MAX_TEXT]
        text_hash = hashlib.sha256(payload_text.encode()).hexdigest()
        return payload_text, options_json, text_hash

    @staticmethod
    def _validate_slack_message_destination(
        team_id: str,
        channel_id: str,
        thread_ts: str,
    ) -> None:
        if (
            not ID_PATTERN.fullmatch(team_id)
            or not CHANNEL_ID_PATTERN.fullmatch(channel_id)
            or (
                thread_ts
                and not re.fullmatch(r"\d{1,20}\.\d{1,20}", thread_ts)
            )
        ):
            raise ValueError("invalid Slack message destination or payload")

    def reserve_message(
        self,
        idempotency_key: str,
        team_id: str,
        channel_id: str,
        thread_ts: str,
        text: str,
        options: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if (
            not idempotency_key
            or len(idempotency_key) > MAX_IDEMPOTENCY_KEY
            or CONFIG_CONTROL_PATTERN.search(idempotency_key)
        ):
            raise ValueError("invalid Slack message idempotency key")
        self._validate_slack_message_destination(
            team_id,
            channel_id,
            thread_ts,
        )
        payload_text, options_json, text_hash = (
            self._prepare_slack_message_payload(text, options)
        )
        client_msg_id = str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"tether:message:{team_id}:{idempotency_key}",
            )
        )
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                """
                SELECT team_id,channel_id,thread_ts,payload_text,
                       payload_options_json,operation,target_message_ts,
                       text_hash,client_msg_id,
                       message_ts,state
                FROM slack_messages WHERE idempotency_key=?
                """,
                (idempotency_key,),
            ).fetchone()
            if row is not None:
                if (
                    str(row["team_id"]) != team_id
                    or str(row["channel_id"]) != channel_id
                    or str(row["thread_ts"]) != thread_ts
                    or str(row["payload_text"]) != payload_text
                    or str(row["payload_options_json"] or "{}") != options_json
                    or str(row["operation"] or "post") != "post"
                    or str(row["target_message_ts"] or "") != ""
                    or str(row["text_hash"]) != text_hash
                    or str(row["client_msg_id"]) != client_msg_id
                ):
                    raise ValueError(
                        "idempotency key already belongs to a different "
                        "Slack message"
                    )
                return {
                    "status": (
                        "sent"
                        if row["message_ts"] or str(row["state"]) == "sent"
                        else "pending"
                    ),
                    "message_ts": str(row["message_ts"] or ""),
                    "client_msg_id": client_msg_id,
                }
            db.execute(
                """
                INSERT INTO slack_messages(
                  idempotency_key,team_id,channel_id,thread_ts,payload_text,
                  payload_options_json,operation,target_message_ts,text_hash,
                  client_msg_id,state,updated_at
                ) VALUES(?,?,?,?,?,?,'post','',?,?,'pending',
                         CURRENT_TIMESTAMP)
                """,
                (
                    idempotency_key,
                    team_id,
                    channel_id,
                    thread_ts,
                    payload_text,
                    options_json,
                    text_hash,
                    client_msg_id,
                ),
            )
        return {
            "status": "pending",
            "message_ts": "",
            "client_msg_id": client_msg_id,
        }

    @staticmethod
    def _require_ingress_lease(
        db: sqlite3.Connection,
        *,
        ingress_event_id: str,
        ingress_lease_id: str,
        ingress_fence_epoch: int | None,
        team_id: str,
        channel_id: str,
    ) -> None:
        linked = bool(ingress_event_id)
        if linked != bool(ingress_lease_id) or linked != (
            ingress_fence_epoch is not None
        ):
            raise ValueError("complete Hermes ingress lease identity is required")
        if not linked:
            return
        if (
            not ingress_event_id.startswith("slack:")
            or len(ingress_event_id) > 256
            or not ingress_lease_id.startswith("tin_")
            or ingress_fence_epoch is None
            or ingress_fence_epoch < 1
        ):
            raise ValueError("invalid Hermes ingress lease identity")
        ingress = db.execute(
            """
            SELECT team_id,channel_id,state,lease_id,fence_epoch
            FROM thread_ingress
            WHERE event_id=?
            """,
            (ingress_event_id,),
        ).fetchone()
        if (
            ingress is None
            or str(ingress["team_id"]) != team_id
            or str(ingress["channel_id"]) != channel_id
            or str(ingress["state"]) not in {"processing", "dispatched"}
            or str(ingress["lease_id"] or "") != ingress_lease_id
            or int(ingress["fence_epoch"]) != ingress_fence_epoch
        ):
            raise RuntimeError("Hermes Slack send lost its ingress lease")

    def reserve_message_group(
        self,
        group_id: str,
        team_id: str,
        channel_id: str,
        thread_ts: str,
        messages: list[dict[str, Any]],
        *,
        ingress_event_id: str = "",
        ingress_lease_id: str = "",
        ingress_fence_epoch: int | None = None,
    ) -> list[dict[str, str]]:
        if not HERMES_SEND_GROUP_PATTERN.fullmatch(group_id):
            raise ValueError("invalid Hermes Slack send group")
        self._validate_slack_message_destination(
            team_id,
            channel_id,
            thread_ts,
        )
        if not messages or len(messages) > 64:
            raise ValueError("Hermes Slack send group must contain 1 to 64 chunks")
        prepared: list[dict[str, Any]] = []
        for index, message in enumerate(messages):
            if not isinstance(message, dict):
                raise ValueError("Hermes Slack send chunk must be an object")
            raw_options = message.get("options")
            if raw_options is not None and not isinstance(raw_options, dict):
                raise ValueError("Hermes Slack send options must be an object")
            payload_text, options_json, text_hash = (
                self._prepare_slack_message_payload(
                    str(message.get("text") or ""),
                    raw_options,
                )
            )
            idempotency_key = f"hermes:{group_id}:{index}"
            prepared.append(
                {
                    "index": index,
                    "idempotency_key": idempotency_key,
                    "payload_text": payload_text,
                    "options_json": options_json,
                    "text_hash": text_hash,
                    "client_msg_id": str(
                        uuid.uuid5(
                            uuid.NAMESPACE_URL,
                            f"tether:message:{team_id}:{idempotency_key}",
                        )
                    ),
                }
            )

        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            self._require_ingress_lease(
                db,
                ingress_event_id=ingress_event_id,
                ingress_lease_id=ingress_lease_id,
                ingress_fence_epoch=ingress_fence_epoch,
                team_id=team_id,
                channel_id=channel_id,
            )

            existing = db.execute(
                """
                SELECT idempotency_key,team_id,channel_id,thread_ts,
                       payload_text,payload_options_json,operation,
                       target_message_ts,ingress_event_id,
                       egress_chunk_index,egress_chunk_count,text_hash,
                       client_msg_id,message_ts,state
                FROM slack_messages
                WHERE egress_group_id=?
                ORDER BY egress_chunk_index
                """,
                (group_id,),
            ).fetchall()
            if existing:
                if len(existing) != len(prepared):
                    raise RuntimeError(
                        "Hermes Slack send group is incomplete or corrupted"
                    )
                results: list[dict[str, str]] = []
                for expected, row in zip(prepared, existing, strict=True):
                    if (
                        str(row["idempotency_key"])
                        != expected["idempotency_key"]
                        or str(row["team_id"]) != team_id
                        or str(row["channel_id"]) != channel_id
                        or str(row["thread_ts"]) != thread_ts
                        or str(row["payload_text"])
                        != expected["payload_text"]
                        or str(row["payload_options_json"] or "{}")
                        != expected["options_json"]
                        or str(row["operation"] or "post") != "post"
                        or str(row["target_message_ts"] or "") != ""
                        or str(row["ingress_event_id"] or "")
                        != ingress_event_id
                        or int(row["egress_chunk_index"])
                        != expected["index"]
                        or int(row["egress_chunk_count"]) != len(prepared)
                        or str(row["text_hash"]) != expected["text_hash"]
                        or str(row["client_msg_id"])
                        != expected["client_msg_id"]
                    ):
                        raise RuntimeError(
                            "Hermes Slack send group changed after reservation"
                        )
                    results.append(
                        {
                            "idempotency_key": expected["idempotency_key"],
                            "status": (
                                "sent"
                                if row["message_ts"]
                                or str(row["state"]) == "sent"
                                else "pending"
                            ),
                            "message_ts": str(row["message_ts"] or ""),
                            "client_msg_id": expected["client_msg_id"],
                        }
                    )
                return results

            for item in prepared:
                db.execute(
                    """
                    INSERT INTO slack_messages(
                      idempotency_key,team_id,channel_id,thread_ts,payload_text,
                      payload_options_json,operation,target_message_ts,
                      ingress_event_id,egress_group_id,egress_chunk_index,
                      egress_chunk_count,text_hash,client_msg_id,state,updated_at
                    ) VALUES(?,?,?,?,?,?,'post','',?,?,?,?,?,?,'pending',
                             CURRENT_TIMESTAMP)
                    """,
                    (
                        item["idempotency_key"],
                        team_id,
                        channel_id,
                        thread_ts,
                        item["payload_text"],
                        item["options_json"],
                        ingress_event_id or None,
                        group_id,
                        item["index"],
                        len(prepared),
                        item["text_hash"],
                        item["client_msg_id"],
                    ),
                )
        return [
            {
                "idempotency_key": item["idempotency_key"],
                "status": "pending",
                "message_ts": "",
                "client_msg_id": item["client_msg_id"],
            }
            for item in prepared
        ]

    def reserve_message_update(
        self,
        group_id: str,
        team_id: str,
        channel_id: str,
        thread_ts: str,
        target_message_ts: str,
        text: str,
        options: dict[str, Any] | None = None,
        *,
        ingress_event_id: str = "",
        ingress_lease_id: str = "",
        ingress_fence_epoch: int | None = None,
    ) -> dict[str, str]:
        if not HERMES_SEND_GROUP_PATTERN.fullmatch(group_id):
            raise ValueError("invalid Hermes Slack update group")
        self._validate_slack_message_destination(
            team_id,
            channel_id,
            thread_ts,
        )
        if not re.fullmatch(r"\d{1,20}\.\d{1,20}", target_message_ts):
            raise ValueError("invalid Slack update target")
        payload_text, options_json, text_hash = (
            self._prepare_slack_message_payload(text, options)
        )
        idempotency_key = f"hermes:{group_id}:0"
        client_msg_id = str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"tether:message:{team_id}:{idempotency_key}",
            )
        )
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            self._require_ingress_lease(
                db,
                ingress_event_id=ingress_event_id,
                ingress_lease_id=ingress_lease_id,
                ingress_fence_epoch=ingress_fence_epoch,
                team_id=team_id,
                channel_id=channel_id,
            )
            row = db.execute(
                """
                SELECT team_id,channel_id,thread_ts,payload_text,
                       payload_options_json,operation,target_message_ts,
                       ingress_event_id,egress_chunk_index,
                       egress_chunk_count,text_hash,client_msg_id,
                       message_ts,state
                FROM slack_messages
                WHERE egress_group_id=?
                """,
                (group_id,),
            ).fetchone()
            if row is not None:
                if (
                    str(row["team_id"]) != team_id
                    or str(row["channel_id"]) != channel_id
                    or str(row["thread_ts"]) != thread_ts
                    or str(row["payload_text"]) != payload_text
                    or str(row["payload_options_json"] or "{}") != options_json
                    or str(row["operation"] or "") != "update"
                    or str(row["target_message_ts"] or "")
                    != target_message_ts
                    or str(row["ingress_event_id"] or "")
                    != ingress_event_id
                    or int(row["egress_chunk_index"]) != 0
                    or int(row["egress_chunk_count"]) != 1
                    or str(row["text_hash"]) != text_hash
                    or str(row["client_msg_id"]) != client_msg_id
                ):
                    raise RuntimeError(
                        "Hermes Slack update changed after reservation"
                    )
                return {
                    "idempotency_key": idempotency_key,
                    "status": (
                        "sent"
                        if row["message_ts"]
                        or str(row["state"]) == "sent"
                        else "pending"
                    ),
                    "message_ts": str(row["message_ts"] or ""),
                    "client_msg_id": client_msg_id,
                }
            db.execute(
                """
                INSERT INTO slack_messages(
                  idempotency_key,team_id,channel_id,thread_ts,payload_text,
                  payload_options_json,operation,target_message_ts,
                  ingress_event_id,egress_group_id,egress_chunk_index,
                  egress_chunk_count,text_hash,client_msg_id,state,updated_at
                ) VALUES(?,?,?,?,?,?,'update',?,?,?,?,?,?,?,'pending',
                         CURRENT_TIMESTAMP)
                """,
                (
                    idempotency_key,
                    team_id,
                    channel_id,
                    thread_ts,
                    payload_text,
                    options_json,
                    target_message_ts,
                    ingress_event_id or None,
                    group_id,
                    0,
                    1,
                    text_hash,
                    client_msg_id,
                ),
            )
        return {
            "idempotency_key": idempotency_key,
            "status": "pending",
            "message_ts": "",
            "client_msg_id": client_msg_id,
        }

    def claim_message(
        self,
        idempotency_key: str,
        *,
        lease_seconds: int = 60,
        lease_owner: str = PROCESS_EPOCH,
    ) -> dict[str, Any]:
        if not re.fullmatch(r"[0-9a-f]{32}", lease_owner):
            raise ValueError("invalid Slack message lease owner")
        lease_seconds = max(10, min(int(lease_seconds), 300))
        lease_id = "msg_" + uuid.uuid4().hex
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                """
                SELECT team_id,channel_id,thread_ts,payload_text,client_msg_id,
                       payload_options_json,operation,target_message_ts,
                       message_ts,state,lease_owner,lease_expires_at
                FROM slack_messages WHERE idempotency_key=?
                """,
                (idempotency_key,),
            ).fetchone()
            if row is None:
                raise ValueError("Slack message outbox record not found")
            if row["message_ts"] or str(row["state"]) == "sent":
                return {
                    "status": "sent",
                    "message_ts": str(row["message_ts"] or ""),
                    "thread_ts": str(row["thread_ts"]),
                }
            if str(row["state"]) in {"cancelled", "failed"}:
                return {
                    "status": "terminal",
                    "state": str(row["state"]),
                    "thread_ts": str(row["thread_ts"]),
                }
            if (
                str(row["state"]) == "delivering"
                and row["lease_expires_at"]
                and db.execute(
                    "SELECT datetime(?) > CURRENT_TIMESTAMP",
                    (row["lease_expires_at"],),
                ).fetchone()[0]
            ):
                return {"status": "busy"}
            previous_state = str(row["state"])
            try:
                options = json.loads(str(row["payload_options_json"] or "{}"))
            except (TypeError, json.JSONDecodeError) as exc:
                raise RuntimeError(
                    "Slack message outbox options are invalid"
                ) from exc
            if not isinstance(options, dict):
                raise RuntimeError("Slack message outbox options are invalid")
            claimed = db.execute(
                """
                UPDATE slack_messages
                SET state='delivering',lease_id=?,lease_owner=?,
                    lease_expires_at=datetime('now',?),
                    retry_count=retry_count+1,updated_at=CURRENT_TIMESTAMP
                WHERE idempotency_key=? AND message_ts IS NULL
                  AND state IN ('pending','uncertain','delivering')
                  AND (
                    state!='delivering'
                    OR lease_expires_at IS NULL
                    OR datetime(lease_expires_at) <= CURRENT_TIMESTAMP
                  )
                """,
                (
                    lease_id,
                    lease_owner,
                    f"+{lease_seconds} seconds",
                    idempotency_key,
                ),
            )
            if claimed.rowcount != 1:
                return {"status": "busy"}
            return {
                "status": "claimed",
                "lease_id": lease_id,
                "team_id": str(row["team_id"]),
                "channel_id": str(row["channel_id"]),
                "thread_ts": str(row["thread_ts"]),
                "text": str(row["payload_text"]),
                "options": options,
                "operation": str(row["operation"] or "post"),
                "target_message_ts": str(row["target_message_ts"] or ""),
                "client_msg_id": str(row["client_msg_id"]),
                "previous_state": previous_state,
            }

    def complete_message(
        self,
        idempotency_key: str,
        lease_id: str,
        message_ts: str,
    ) -> None:
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            completed = db.execute(
                """
                UPDATE slack_messages
                SET message_ts=?,state='sent',error=NULL,lease_id=NULL,
                    lease_owner=NULL,lease_expires_at=NULL,
                    updated_at=CURRENT_TIMESTAMP
                WHERE idempotency_key=? AND lease_id=?
                  AND state='delivering' AND message_ts IS NULL
                """,
                (message_ts, idempotency_key, lease_id),
            )
            if completed.rowcount == 1:
                linked = db.execute(
                    """
                    SELECT ingress_event_id FROM slack_messages
                    WHERE idempotency_key=?
                    """,
                    (idempotency_key,),
                ).fetchone()
                ingress_event_id = (
                    str(linked["ingress_event_id"] or "")
                    if linked is not None
                    else ""
                )
                if ingress_event_id:
                    db.execute(
                        """
                        UPDATE thread_ingress
                        SET state='completed',lease_id=NULL,lease_owner=NULL,
                            lease_expires_at=NULL,error_code=NULL,
                            updated_at=CURRENT_TIMESTAMP
                        WHERE event_id=? AND state='uncertain'
                          AND egress_sealed=1
                          AND error_code='hermes_egress_pending'
                          AND NOT EXISTS (
                            SELECT 1 FROM slack_messages
                            WHERE ingress_event_id=? AND state!='sent'
                          )
                        """,
                        (ingress_event_id, ingress_event_id),
                    )
                return
            row = db.execute(
                """
                SELECT message_ts,state FROM slack_messages
                WHERE idempotency_key=?
                """,
                (idempotency_key,),
            ).fetchone()
            if (
                row is not None
                and str(row["state"]) == "sent"
                and str(row["message_ts"] or "") == message_ts
            ):
                return
            raise RuntimeError("Slack message outbox lease was lost before completion")

    def record_message_error(
        self,
        idempotency_key: str,
        lease_id: str,
        error: str,
    ) -> None:
        with self.connect() as db:
            db.execute(
                """
                UPDATE slack_messages
                SET state='uncertain',error=?,lease_id=NULL,
                    lease_owner=NULL,lease_expires_at=NULL,
                    updated_at=CURRENT_TIMESTAMP
                WHERE idempotency_key=? AND lease_id=?
                  AND state='delivering' AND message_ts IS NULL
                """,
                (
                    security.redact_egress_text(
                        error or "Slack delivery uncertain"
                    )[:500],
                    idempotency_key,
                    lease_id,
                ),
            )

    def release_abandoned_message_leases(
        self,
        lease_owner: str = PROCESS_EPOCH,
    ) -> int:
        if not re.fullmatch(r"[0-9a-f]{32}", lease_owner):
            raise ValueError("invalid Slack message lease owner")
        with self.connect() as db:
            cursor = db.execute(
                """
                UPDATE slack_messages
                SET state='uncertain',
                    error=CASE
                      WHEN lease_owner IS NULL
                      THEN 'message_delivery_lease_owner_missing'
                      ELSE 'message_delivery_lease_expired'
                    END,
                    lease_id=NULL,lease_owner=NULL,lease_expires_at=NULL,
                    updated_at=CURRENT_TIMESTAMP
                WHERE state='delivering' AND message_ts IS NULL
                  AND (lease_owner IS NULL OR lease_owner!=?)
                  AND (
                    lease_owner IS NULL
                    OR lease_expires_at IS NULL
                    OR datetime(lease_expires_at) <= CURRENT_TIMESTAMP
                  )
                """,
                (lease_owner,),
            )
            return int(cursor.rowcount)

    def pending_message_keys(self) -> list[str]:
        with self.connect() as db:
            return [
                str(row["idempotency_key"])
                for row in db.execute(
                    """
                    SELECT idempotency_key FROM slack_messages
                    WHERE message_ts IS NULL
                      AND state IN ('pending','uncertain')
                    ORDER BY rowid
                    """
                )
            ]

    @staticmethod
    def reconciliation_key(
        target_kind: str,
        team_id: str,
        channel_id: str,
        thread_ts: str,
        target_id: str,
    ) -> str:
        material = "\0".join(
            (target_kind, team_id, channel_id, thread_ts, target_id)
        )
        return "rec_" + hashlib.sha256(material.encode()).hexdigest()[:32]

    def ensure_reconciliation(
        self,
        *,
        reconciliation_key: str,
        team_id: str,
        method: str,
        channel_id: str,
        thread_ts: str,
        target_kind: str,
        target_id: str,
    ) -> dict[str, Any]:
        if (
            not re.fullmatch(r"rec_[0-9a-f]{32}", reconciliation_key)
            or not ID_PATTERN.fullmatch(team_id)
            or method not in {"conversations.history", "conversations.replies"}
            or not CHANNEL_ID_PATTERN.fullmatch(channel_id)
            or target_kind not in {
                "root",
                "reply",
                "message",
                "file",
            }
            or not target_id
            or len(target_id) > 256
            or CONFIG_CONTROL_PATTERN.search(target_id)
            or (
                method == "conversations.replies"
                and not re.fullmatch(r"\d{1,20}\.\d{1,20}", thread_ts)
            )
            or (method == "conversations.history" and thread_ts)
        ):
            raise ValueError("invalid Slack reconciliation identity")
        oldest_ts = f"{max(0.0, time.time() - 60.0):.6f}"
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                """
                SELECT * FROM slack_reconciliations
                WHERE reconciliation_key=?
                """,
                (reconciliation_key,),
            ).fetchone()
            if row is None:
                db.execute(
                    """
                    INSERT INTO slack_reconciliations(
                      reconciliation_key,team_id,method,channel_id,thread_ts,
                      target_kind,target_id,oldest_ts
                    ) VALUES(?,?,?,?,?,?,?,?)
                    """,
                    (
                        reconciliation_key,
                        team_id,
                        method,
                        channel_id,
                        thread_ts,
                        target_kind,
                        target_id,
                        oldest_ts,
                    ),
                )
                row = db.execute(
                    """
                    SELECT * FROM slack_reconciliations
                    WHERE reconciliation_key=?
                    """,
                    (reconciliation_key,),
                ).fetchone()
            if row is None:
                raise RuntimeError("Slack reconciliation could not be reserved")
            if any(
                str(row[name]) != value
                for name, value in (
                    ("team_id", team_id),
                    ("method", method),
                    ("channel_id", channel_id),
                    ("thread_ts", thread_ts),
                    ("target_kind", target_kind),
                    ("target_id", target_id),
                )
            ):
                raise ValueError(
                    "Slack reconciliation key belongs to a different target"
                )
            return dict(row)

    def claim_reconciliation_page(
        self,
        reconciliation_key: str,
    ) -> dict[str, Any]:
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                """
                SELECT * FROM slack_reconciliations
                WHERE reconciliation_key=?
                """,
                (reconciliation_key,),
            ).fetchone()
            if row is None:
                raise ValueError("Slack reconciliation record not found")
            state = str(row["state"])
            if state != "pending":
                return {
                    "status": state,
                    "result_ts": str(row["result_ts"] or ""),
                    "error": str(row["error"] or ""),
                }
            due = bool(
                db.execute(
                    "SELECT datetime(?) <= CURRENT_TIMESTAMP",
                    (row["next_attempt_at"],),
                ).fetchone()[0]
            )
            if not due:
                return {"status": "waiting"}
            identity = self._slack_read_budget_identity(
                str(row["team_id"]),
                str(row["method"]),
            )
            if not self._claim_slack_read_budget_in_tx(db, *identity):
                return {"status": "waiting"}
            modifier = f"+{RECONCILIATION_INTERVAL_SECONDS} seconds"
            db.execute(
                """
                UPDATE slack_reconciliations
                SET next_attempt_at=datetime('now',?),
                    updated_at=CURRENT_TIMESTAMP
                WHERE reconciliation_key=? AND state='pending'
                """,
                (modifier, reconciliation_key),
            )
            return {"status": "claimed", **dict(row)}

    def complete_reconciliation_page(
        self,
        reconciliation_key: str,
        *,
        expected_cursor: str,
        next_cursor: str,
        result_ts: str,
    ) -> dict[str, str]:
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                """
                SELECT next_cursor,seen_cursors_json,pages_seen,state
                FROM slack_reconciliations
                WHERE reconciliation_key=?
                """,
                (reconciliation_key,),
            ).fetchone()
            if row is None or str(row["state"]) != "pending":
                raise RuntimeError("Slack reconciliation state changed during a page")
            if str(row["next_cursor"] or "") != expected_cursor:
                raise RuntimeError("Slack reconciliation cursor changed during a page")
            try:
                seen = {
                    str(value)
                    for value in json.loads(row["seen_cursors_json"])
                    if isinstance(value, str)
                }
            except (TypeError, json.JSONDecodeError):
                seen = set()
            pages_seen = int(row["pages_seen"]) + 1
            error = ""
            state = "pending"
            if result_ts:
                state = "found"
            elif (
                next_cursor
                and (
                    len(next_cursor) > 2_048
                    or CONFIG_CONTROL_PATTERN.search(next_cursor)
                    or next_cursor in seen
                    or next_cursor == expected_cursor
                )
            ):
                state = "failed"
                error = "slack_reconciliation_invalid_cursor"
            elif pages_seen >= RECONCILIATION_MAX_PAGES and next_cursor:
                state = "failed"
                error = "slack_reconciliation_page_limit"
            elif not next_cursor:
                state = "not_found"
            if next_cursor and state == "pending":
                seen.add(next_cursor)
            db.execute(
                """
                UPDATE slack_reconciliations
                SET next_cursor=?,seen_cursors_json=?,pages_seen=?,
                    state=?,result_ts=?,error=?,
                    updated_at=CURRENT_TIMESTAMP
                WHERE reconciliation_key=? AND state='pending'
                  AND next_cursor=?
                """,
                (
                    next_cursor if state == "pending" else "",
                    json.dumps(sorted(seen), separators=(",", ":")),
                    pages_seen,
                    state,
                    result_ts or None,
                    error or None,
                    reconciliation_key,
                    expected_cursor,
                ),
            )
            return {
                "status": state,
                "result_ts": result_ts,
                "error": error,
            }

    def reset_reconciliation(self, reconciliation_key: str) -> None:
        with self.connect() as db:
            db.execute(
                "DELETE FROM slack_reconciliations WHERE reconciliation_key=?",
                (reconciliation_key,),
            )

    def defer_reconciliation(
        self,
        reconciliation_key: str,
        seconds: int = RECONCILIATION_INTERVAL_SECONDS,
    ) -> None:
        seconds = max(
            RECONCILIATION_INTERVAL_SECONDS,
            min(int(seconds), 3_600),
        )
        with self.connect() as db:
            updated = db.execute(
                """
                UPDATE slack_reconciliations
                SET next_attempt_at=datetime('now',?),
                    updated_at=CURRENT_TIMESTAMP
                WHERE reconciliation_key=? AND state='pending'
                """,
                (f"+{seconds} seconds", reconciliation_key),
            )
            if updated.rowcount != 1:
                raise RuntimeError(
                    "Slack reconciliation could not be deferred"
                )

    def pending_reconciliation_keys(self) -> list[str]:
        with self.connect() as db:
            return [
                str(row["reconciliation_key"])
                for row in db.execute(
                    """
                    SELECT reconciliation_key FROM slack_reconciliations
                    WHERE state='pending'
                    ORDER BY next_attempt_at,updated_at,reconciliation_key
                    """
                )
            ]

    def prune(self, retention_days: int = 30) -> dict[str, int]:
        """Delete completed delivery records outside the deduplication window.

        Active bindings and unresolved delivery states are never expired by
        maintenance. Their routing metadata remains available until an
        operator explicitly closes the binding in a future lifecycle command.
        """
        retention_days = max(
            minimum_retention_days(),
            min(int(retention_days), 3_650),
        )
        cutoff = f"-{retention_days} days"
        statements = (
            (
                "bridge_attempts",
                """
                DELETE FROM bridge_attempts
                WHERE state IN ('acknowledged','failed','requeued','cancelled')
                  AND updated_at < datetime('now',?)
                """,
            ),
            (
                "bridge_replies",
                """
                DELETE FROM bridge_replies
                WHERE state='sent' AND updated_at < datetime('now',?)
                """,
            ),
            (
                "bridge_roots",
                """
                DELETE FROM bridge_roots
                WHERE state='complete' AND updated_at < datetime('now',?)
                """,
            ),
            (
                "slack_messages",
                """
                DELETE FROM slack_messages
                WHERE state='sent' AND updated_at < datetime('now',?)
                """,
            ),
            (
                "slack_reconciliations",
                """
                DELETE FROM slack_reconciliations
                WHERE state IN ('found','not_found','abandoned')
                  AND updated_at < datetime('now',?)
                """,
            ),
            (
                "bridge_events",
                """
                DELETE FROM bridge_events
                WHERE state IN ('delivered','failed')
                  AND updated_at < datetime('now',?)
                """,
            ),
            (
                "thread_ingress",
                """
                DELETE FROM thread_ingress
                WHERE updated_at < datetime('now',?)
                  AND state IN ('completed','transferred','cancelled')
                """,
            ),
            (
                "thread_participation",
                """
                DELETE FROM thread_participation
                WHERE updated_at < datetime('now',?)
                  AND NOT EXISTS (
                    SELECT 1 FROM bridges
                    WHERE bridges.team_id=thread_participation.team_id
                      AND bridges.channel_id=thread_participation.channel_id
                      AND bridges.thread_ts=thread_participation.thread_ts
                      AND bridges.status='active'
                  )
                """,
            ),
        )
        counts: dict[str, int] = {}
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            for table, statement in statements:
                counts[table] = int(db.execute(statement, (cutoff,)).rowcount)
        return counts

    def storage_stats(self) -> dict[str, int]:
        tables = (
            "bridges",
            "bridge_events",
            "bridge_replies",
            "bridge_roots",
            "slack_messages",
            "slack_reconciliations",
            "slack_reconciliation_limits",
            "bridge_attempts",
            "thread_participation",
            "thread_ingress",
            "slack_reply_poll_state",
            "slack_reply_poll_rotation",
            "slack_reply_poll_scheduler",
        )
        with self.connect() as db:
            result = {
                table: int(
                    # table comes only from the fixed internal tuple above.
                    db.execute(
                        f"SELECT count(*) FROM {table}"  # nosec B608
                    ).fetchone()[0]
                )
                for table in tables
            }
        result["database_bytes"] = self.path.stat().st_size
        return result


class SlackAPIError(RuntimeError):
    """A definitive Slack API rejection with a machine-readable error code."""

    def __init__(self, code: str):
        self.code = _safe_label(code or "unknown", 80)
        super().__init__(f"Slack API error: {self.code}")


def _slack_call(token: str, method: str, payload: dict[str, Any]) -> dict[str, Any]:
    path = SLACK_METHOD_PATHS.get(method)
    if path is None:
        raise ValueError("unsupported Slack API method")
    token_identity = hashlib.sha256(token.encode()).hexdigest()
    for attempt in range(2):
        with _SLACK_TOKEN_WORKSPACES_LOCK:
            workspace = _SLACK_TOKEN_WORKSPACES.get(
                token_identity,
                "token-" + token_identity,
            )
        method_key = slack_protocol.SlackMethodKey(workspace, method)
        if not _SLACK_RETRY_COORDINATOR.wait(
            method_key,
            stop_event=getattr(_SLACK_CALL_CONTEXT, "stop_event", None),
        ):
            raise RuntimeError("Slack recovery stopped during rate-limit backoff")
        # nosemgrep: python.lang.security.audit.httpsconnection-detected.httpsconnection-detected
        connection = http.client.HTTPSConnection(
            "slack.com",
            timeout=30,
            context=_SLACK_TLS_CONTEXT,
        )
        try:
            headers = {"Authorization": "Bearer " + token}
            if method in {
                "conversations.replies",
                "files.getUploadURLExternal",
                "files.info",
            }:
                query = urllib.parse.urlencode(payload)
                connection.request("GET", f"{path}?{query}", headers=headers)
            else:
                connection.request(
                    "POST",
                    path,
                    body=json.dumps(payload).encode(),
                    headers={**headers, "Content-Type": "application/json"},
                )
            response = connection.getresponse()
            status = int(getattr(response, "status", 200))
            response_headers = {
                str(key): str(value)
                for key, value in getattr(response, "getheaders", lambda: [])()
            }
            raw_response = response.read(MAX_SLACK_API_RESPONSE_BYTES + 1)
            if len(raw_response) > MAX_SLACK_API_RESPONSE_BYTES:
                raise RuntimeError("Slack API response exceeds the size limit")
            try:
                result = json.loads(raw_response)
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise RuntimeError(
                    "Slack API returned an invalid response"
                ) from exc
        finally:
            connection.close()
        if not isinstance(result, dict):
            raise RuntimeError("Slack API returned an invalid response")
        if status == 429 or str(result.get("error") or "") == "ratelimited":
            _SLACK_RETRY_COORDINATOR.record_429(
                method_key,
                response_headers,
            )
            if attempt == 0:
                continue
            raise RuntimeError("Slack API rate limit remained active after retry")
        if not result.get("ok"):
            error = _safe_label(result.get("error") or "unknown", 80)
            raise SlackAPIError(error)
        if method == "auth.test":
            team_id = str(result.get("team_id") or "")
            if ID_PATTERN.fullmatch(team_id):
                with _SLACK_TOKEN_WORKSPACES_LOCK:
                    _SLACK_TOKEN_WORKSPACES[token_identity] = team_id
        return result
    raise RuntimeError("Slack API request could not be completed")


def redact_text(text: str) -> str:
    return security.redact_egress_text(text)


SILENCE_CONTROL_LINES = frozenset({
    "NO_REPLY",
    "NO REPLY",
    "[SILENT]",
    "SILENT",
})


def is_silence_control_output(value: Any) -> bool:
    """Recognize an intentional-silence marker at the output boundary.

    Agents sometimes explain their routing decision before emitting the
    documented marker. The marker controls the whole output when it is the
    final non-empty line; prose that merely mentions NO_REPLY remains visible.
    """
    if not isinstance(value, str):
        return False
    lines = [line.strip().upper() for line in value.splitlines() if line.strip()]
    return bool(lines) and lines[-1] in SILENCE_CONTROL_LINES


def validate_reply_text(text: str, config: Config | None = None) -> str:
    # The configured word, character, and sentence counts are writing targets,
    # not delivery gates. A useful reply must not disappear because it needed
    # more context than the default Slack style.
    _ = config or load_config()
    cleaned = text.strip()
    if is_silence_control_output(cleaned):
        return "NO_REPLY"
    if not cleaned:
        raise ValueError("reply text is empty")
    if len(cleaned) > MAX_TEXT:
        raise ValueError(
            f"Slack reply exceeds Tether's {MAX_TEXT}-character transport limit"
        )
    return cleaned


def stage_reply_payload(
    store: Store,
    bridge_id: str,
    reply_key: str,
    text: str,
) -> tuple[str, str]:
    cleaned = validate_reply_text(text)
    if cleaned == "NO_REPLY":
        raise ValueError("NO_REPLY is an acknowledgment, not a Slack outbox payload")
    outbound = redact_text(cleaned)[:MAX_TEXT]
    text_hash = hashlib.sha256(outbound.encode()).hexdigest()
    client_msg_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"tether:{reply_key}"))
    return store.stage_reply(
        reply_key,
        bridge_id,
        outbound,
        text_hash,
        client_msg_id,
    )


def slack_post(
    token: str,
    channel: str,
    text: str,
    thread_ts: str | None = None,
    *,
    client_msg_id: str | None = None,
    metadata_event_type: str = "",
    metadata_event_payload: dict[str, str] | None = None,
    options: dict[str, Any] | None = None,
) -> str:
    payload: dict[str, Any] = {"channel": channel, "text": redact_text(text)[:MAX_TEXT]}
    if thread_ts:
        payload["thread_ts"] = thread_ts
    if client_msg_id:
        payload["client_msg_id"] = client_msg_id
    if metadata_event_type:
        payload["metadata"] = {
            "event_type": metadata_event_type,
            "event_payload": dict(metadata_event_payload or {}),
        }
    elif client_msg_id:
        payload["metadata"] = {
            "event_type": "tether_reply",
            "event_payload": {"client_msg_id": client_msg_id},
        }
    for name in ("blocks", "mrkdwn", "reply_broadcast"):
        if name in (options or {}):
            payload[name] = options[name]
    return str(_slack_call(token, "chat.postMessage", payload)["ts"])


def slack_update(
    token: str,
    channel: str,
    message_ts: str,
    text: str,
    *,
    options: dict[str, Any] | None = None,
) -> str:
    payload: dict[str, Any] = {
        "channel": channel,
        "ts": message_ts,
        "text": redact_text(text)[:MAX_TEXT],
    }
    if "blocks" in (options or {}):
        payload["blocks"] = options["blocks"]
    result = _slack_call(token, "chat.update", payload)
    observed_ts = str(result.get("ts") or message_ts)
    if observed_ts != message_ts:
        raise RuntimeError("Slack updated an unexpected message")
    return observed_ts


def _upload_approved_roots() -> tuple[str, ...]:
    return UPLOAD_APPROVED_ROOTS


def _upload_max_bytes() -> int:
    raw = UPLOAD_MAX_BYTES
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError("TETHER_UPLOAD_MAX_BYTES must be an integer") from exc
    if not 1 <= value <= security.DEFAULT_UPLOAD_LIMIT:
        raise ValueError(
            "TETHER_UPLOAD_MAX_BYTES must be between 1 and "
            f"{security.DEFAULT_UPLOAD_LIMIT}"
        )
    return value


def _safe_upload_filename(filename: str, *, fallback: str = "attachment") -> str:
    basename = str(filename).replace("\\", "/").rsplit("/", 1)[-1]
    safe_name = re.sub(r"[^A-Za-z0-9._-]+", "-", basename).strip(".-")
    if not safe_name:
        safe_name = fallback
    return safe_name[:MAX_SLACK_FILENAME]


def _stable_root_filename(bridge_id: str, source_path: str) -> str:
    return _safe_upload_filename(
        f"tether-{bridge_id}-{Path(source_path).name}"
    )


def stage_safe_upload(file_path: str) -> Any:
    """Stage and scan one local attachment under the shared egress policy."""
    try:
        security.secure_state_directory(
            Path(UPLOAD_STAGING_DIRECTORY).parent,
            create=True,
        )
    except security.StatePathError as exc:
        raise security.UploadSecurityError(
            "upload staging parent is not private"
        ) from exc
    staged = security.stage_upload(
        str(Path(file_path).expanduser()),
        approved_roots=_upload_approved_roots(),
        staging_directory=UPLOAD_STAGING_DIRECTORY,
        max_bytes=_upload_max_bytes(),
    )
    descriptor = staged.open_verified()
    try:
        security.require_safe_upload_content(
            descriptor,
            max_bytes=_upload_max_bytes(),
        )
    except BaseException:
        with contextlib.suppress(OSError):
            staged.path.unlink()
        raise
    finally:
        os.close(descriptor)
    return staged


def _root_staged_upload(record: dict[str, Any]) -> Any | None:
    raw_path = str(record.get("staged_path") or "")
    if not raw_path:
        return None
    path = Path(raw_path)
    staging = Path(UPLOAD_STAGING_DIRECTORY).expanduser()
    if not path.is_absolute() or path.parent != staging:
        raise security.UploadSecurityError(
            "stored root upload is outside the private staging directory"
        )
    required = (
        "staged_size",
        "staged_sha256",
        "staged_owner_uid",
        "staged_device",
        "staged_inode",
        "staged_source_device",
        "staged_source_inode",
    )
    if any(record.get(key) is None for key in required):
        raise security.UploadSecurityError(
            "stored root upload metadata is incomplete"
        )
    return security.StagedUpload(
        path=path,
        size=int(record["staged_size"]),
        sha256=str(record["staged_sha256"]),
        owner_uid=int(record["staged_owner_uid"]),
        device=int(record["staged_device"]),
        inode=int(record["staged_inode"]),
        source_device=int(record["staged_source_device"]),
        source_inode=int(record["staged_source_inode"]),
    )


def _slack_method_key(token: str, method: str) -> Any:
    token_identity = hashlib.sha256(token.encode()).hexdigest()
    with _SLACK_TOKEN_WORKSPACES_LOCK:
        workspace = _SLACK_TOKEN_WORKSPACES.get(
            token_identity,
            "token-" + token_identity,
        )
    return slack_protocol.SlackMethodKey(workspace, method)


def _validate_slack_upload_url(upload_url: str) -> urllib.parse.SplitResult:
    if not upload_url or len(upload_url) > 8_192:
        raise RuntimeError("Slack returned an invalid external upload URL")
    try:
        parsed = urllib.parse.urlsplit(upload_url)
        port = parsed.port
    except ValueError as exc:
        raise RuntimeError("Slack returned an invalid external upload URL") from exc
    if (
        parsed.scheme != "https"
        or parsed.hostname != "files.slack.com"
        or port not in {None, 443}
        or parsed.username is not None
        or parsed.password is not None
        or bool(parsed.fragment)
        or not parsed.path.startswith("/upload/")
    ):
        raise RuntimeError("Slack returned an untrusted external upload URL")
    return parsed


def _allocate_slack_upload(
    token: str,
    filename: str,
    length: int,
) -> tuple[str, str]:
    if not filename or length < 0:
        raise ValueError("Slack upload filename and length are required")
    safe_filename = _safe_upload_filename(filename)
    result = _slack_call(
        token,
        "files.getUploadURLExternal",
        {"filename": safe_filename, "length": length},
    )
    file_id = str(result.get("file_id") or "")
    upload_url = str(result.get("upload_url") or "")
    if not SLACK_FILE_ID_PATTERN.fullmatch(file_id):
        raise RuntimeError("Slack returned an invalid external upload file ID")
    _validate_slack_upload_url(upload_url)
    return file_id, upload_url


def _require_safe_staged_upload(staged: Any) -> None:
    descriptor = staged.open_verified()
    try:
        security.require_safe_upload_content(
            descriptor,
            max_bytes=_upload_max_bytes(),
        )
    finally:
        os.close(descriptor)


def _upload_slack_bytes(
    token: str,
    upload_url: str,
    staged: Any,
) -> None:
    parsed = _validate_slack_upload_url(upload_url)
    method_key = _slack_method_key(token, "files.externalUpload")
    target = parsed.path
    if parsed.query:
        target += "?" + parsed.query
    for attempt in range(2):
        if not _SLACK_RETRY_COORDINATOR.wait(
            method_key,
            stop_event=getattr(_SLACK_CALL_CONTEXT, "stop_event", None),
        ):
            raise RuntimeError("Slack recovery stopped during rate-limit backoff")
        descriptor = staged.open_verified()
        # nosemgrep: python.lang.security.audit.httpsconnection-detected.httpsconnection-detected
        connection = http.client.HTTPSConnection(
            parsed.hostname,
            parsed.port or 443,
            timeout=30,
            context=_SLACK_TLS_CONTEXT,
        )
        try:
            security.require_safe_upload_content(
                descriptor,
                max_bytes=_upload_max_bytes(),
            )
            with open(
                staged.path,
                "rb",
                opener=lambda _path, _flags: descriptor,
            ) as verified:
                descriptor = -1
                connection.request(
                    "POST",
                    target,
                    body=verified,
                    headers={
                        "Content-Type": "application/octet-stream",
                        "Content-Length": str(int(staged.size)),
                    },
                )
                response = connection.getresponse()
                status = int(getattr(response, "status", 0))
                response_headers = {
                    str(key): str(value)
                    for key, value in getattr(
                        response,
                        "getheaders",
                        lambda: [],
                    )()
                }
                response.read(8_192)
        finally:
            if descriptor >= 0:
                os.close(descriptor)
            connection.close()
        if status == 200:
            return
        if status == 429:
            _SLACK_RETRY_COORDINATOR.record_429(
                method_key,
                response_headers,
            )
            if attempt == 0:
                continue
            raise RuntimeError(
                "Slack external upload rate limit remained active after retry"
            )
        raise RuntimeError(
            f"Slack external byte upload failed with HTTP {status}"
        )
    raise RuntimeError("Slack external byte upload could not be completed")


def _complete_slack_upload(
    token: str,
    channel: str,
    file_id: str,
    *,
    filename: str,
    text: str = "",
    thread_ts: str | None = None,
) -> dict[str, Any]:
    if not SLACK_FILE_ID_PATTERN.fullmatch(file_id):
        raise ValueError("invalid Slack file ID")
    safe_filename = _safe_upload_filename(filename)
    payload: dict[str, Any] = {
        "files": [{"id": file_id, "title": safe_filename}],
        "channel_id": channel,
    }
    if thread_ts:
        payload["thread_ts"] = thread_ts
    safe_text = redact_text(text)[:MAX_TEXT]
    if safe_text:
        payload["initial_comment"] = safe_text
    result = _slack_call(token, "files.completeUploadExternal", payload)
    returned_ids = {
        str(item.get("id") or "")
        for item in result.get("files", [])
        if isinstance(item, dict)
    }
    if returned_ids and file_id not in returned_ids:
        raise RuntimeError("Slack completed a different external file ID")
    return result


def _file_share_message_ts(file_object: dict[str, Any], channel: str) -> str:
    raw_file_id = str(file_object.get("id") or "")
    if raw_file_id and not SLACK_FILE_ID_PATTERN.fullmatch(raw_file_id):
        return ""
    for visibility in ("public", "private"):
        channel_shares = (
            (file_object.get("shares") or {})
            .get(visibility, {})
            .get(channel, [])
        )
        for share in channel_shares:
            if isinstance(share, dict) and share.get("ts"):
                return str(share["ts"])
    return ""


def slack_upload(
    token: str,
    channel: str,
    text: str,
    file_path: str,
    thread_ts: str | None = None,
    *,
    filename: str = "",
) -> str:
    try:
        security.secure_state_directory(
            Path(UPLOAD_STAGING_DIRECTORY).parent,
            create=True,
        )
    except security.StatePathError as exc:
        raise security.UploadSecurityError(
            "upload staging parent is not private"
        ) from exc
    staged = security.stage_upload(
        str(Path(file_path).expanduser()),
        approved_roots=_upload_approved_roots(),
        staging_directory=UPLOAD_STAGING_DIRECTORY,
        max_bytes=_upload_max_bytes(),
    )
    try:
        return _slack_upload_staged(
            token,
            channel,
            text,
            staged,
            thread_ts,
            filename=filename,
        )
    finally:
        with contextlib.suppress(OSError):
            staged.path.unlink()


def _slack_upload_staged(
    token: str,
    channel: str,
    text: str,
    staged: Any,
    thread_ts: str | None = None,
    *,
    filename: str = "",
) -> str:
    stable_filename = _safe_upload_filename(
        filename or Path(staged.path).name
    )
    _require_safe_staged_upload(staged)
    file_id, upload_url = _allocate_slack_upload(
        token,
        stable_filename,
        int(staged.size),
    )
    _upload_slack_bytes(token, upload_url, staged)
    _complete_slack_upload(
        token,
        channel,
        file_id,
        filename=stable_filename,
        text=text,
        thread_ts=thread_ts,
    )
    if thread_ts:
        return thread_ts
    info = _slack_call(token, "files.info", {"file": file_id})
    item = info.get("file")
    if isinstance(item, dict) and str(item.get("id") or "") == file_id:
        message_ts = _file_share_message_ts(item, channel)
        if message_ts:
            return message_ts
    raise RuntimeError("Slack upload succeeded without a root timestamp")


def _warn_recovery_failure(kind: str, exc: BaseException) -> None:
    now = time.monotonic()
    with _RECOVERY_WARNING_LOCK:
        previous = _RECOVERY_WARNING_TIMES.get(kind)
        if (
            previous is not None
            and now - previous < _RECOVERY_WARNING_INTERVAL_SECONDS
        ):
            return
        _RECOVERY_WARNING_TIMES[kind] = now
    print(
        f"Tether {kind} recovery failed; queued work will be retried "
        f"({type(exc).__name__}).",
        file=sys.stderr,
    )


def _after_durable_delivery(
    kind: str,
    action: Callable[[], Any],
) -> None:
    try:
        action()
    except Exception as exc:
        warning_key = f"post-delivery:{kind}"
        now = time.monotonic()
        with _RECOVERY_WARNING_LOCK:
            previous = _RECOVERY_WARNING_TIMES.get(warning_key)
            if (
                previous is not None
                and now - previous < _RECOVERY_WARNING_INTERVAL_SECONDS
            ):
                return
            _RECOVERY_WARNING_TIMES[warning_key] = now
        print(
            f"Tether {kind} bookkeeping failed after durable Slack delivery; "
            f"the accepted message will not be resent ({type(exc).__name__}).",
            file=sys.stderr,
        )


class Broker:
    def __init__(
        self,
        token: str,
        store: Store | None = None,
        health_provider: Callable[[], dict[str, Any]] | None = None,
        attempt_closed: Callable[[str], None] | None = None,
        *,
        verified_workspace_team_id: str = "",
    ):
        if not token:
            raise ValueError("Hermes Slack credential is unavailable")
        if (
            verified_workspace_team_id
            and not ID_PATTERN.fullmatch(verified_workspace_team_id)
        ):
            raise ValueError("verified Slack workspace ID is invalid")
        self.token = token
        self.store = store or Store()
        self.health_provider = health_provider
        self.attempt_closed = attempt_closed
        self.lease_owner = PROCESS_EPOCH
        self._notify_lock = threading.Lock()
        self._reply_lock = threading.Lock()
        self._message_lock = threading.Lock()
        self._recovery_lock = threading.Lock()
        self._workspace_lock = threading.Lock()
        self._workspace_team_id = verified_workspace_team_id
        self._joined_channels: set[str] = set()
        self._last_maintenance_at = 0.0

    def require_workspace(self, team_id: str = "") -> str:
        with self._workspace_lock:
            if not self._workspace_team_id:
                identity = _slack_call(self.token, "auth.test", {})
                resolved = str(identity.get("team_id") or "")
                if not ID_PATTERN.fullmatch(resolved):
                    raise NativeContinuationError(
                        "Tether could not resolve the Slack workspace for its bot token",
                        code="workspace_mismatch",
                    )
                self._workspace_team_id = resolved
            resolved = self._workspace_team_id
        if team_id and team_id != resolved:
            raise NativeContinuationError(
                "Tether rejected a Slack operation for a different workspace",
                code="workspace_mismatch",
            )
        return resolved

    def _wake_bridge(self, bridge_id: str) -> None:
        if self.attempt_closed is None:
            return
        with contextlib.suppress(Exception):
            self.attempt_closed(bridge_id)

    def _ensure_channel_membership(self, channel: str) -> None:
        if not channel.startswith("C") or channel in self._joined_channels:
            return
        try:
            # C-prefixed IDs include public channels, DMs, and group DMs. Probe
            # existing access first because Slack refuses conversations.join for DMs.
            _slack_call(self.token, "conversations.history", {"channel": channel, "limit": 1})
        except RuntimeError as exc:
            if "not_in_channel" not in str(exc):
                raise
            try:
                _slack_call(self.token, "conversations.join", {"channel": channel})
            except RuntimeError as join_exc:
                raise RuntimeError(
                    "Tether could not join the public Slack destination. Grant the bot "
                    "channels:join or invite it to the channel before creating a resumable thread "
                    f"({join_exc})"
                ) from join_exc
        self._joined_channels.add(channel)

    def _status(self, config: Config, allowed_users: tuple[str, ...]) -> dict[str, Any]:
        status = {
            "ok": True,
            "implementation": "tether",
            "protocol_version": 6,
            "channel_configured": bool(effective_channel(config)),
            "owner_configured": bool(config.default_owner or allowed_users),
            "allowed_user_count": len(allowed_users),
            "team_configured": bool(config.team_id),
            "retention_days": config.retention_days,
            "storage": self.store.storage_stats(),
            "broker_uid": os.geteuid(),
            "peer_uid_enforced": True,
            "root_refused": True,
        }
        if self.health_provider is not None:
            health = self.health_provider()
            if isinstance(health, dict):
                status.update(health)
        return status

    def _herdr_context(self, request: BridgeRequest) -> dict[str, Any]:
        terminal_id = str(request.get("herdr_terminal_id") or "")
        agent_name = str(request.get("herdr_agent_name") or "")
        native_session = str(request.get("herdr_agent_session_value") or "")
        agent = str(request.get("herdr_agent") or "")
        bridge = self.store.find_herdr_endpoint(
            terminal_id,
            agent_name,
            native_session,
            agent,
        )
        if bridge is None:
            return {
                "ok": True,
                "bound": False,
                "agent": agent,
                "queued": 0,
                "uncertain": 0,
            }
        counts = self.store.bridge_work_counts(bridge.bridge_id)
        return {
            "ok": True,
            "bound": True,
            "agent": agent,
            "bridge": {
                "bridge_id": bridge.bridge_id,
                "status": bridge.status,
                "binding_state": bridge.binding_state,
                "binding_generation": bridge.binding_generation,
                "channel_id": bridge.channel_id,
                "thread_ts": bridge.thread_ts or "",
            },
            **counts,
        }

    def _identity(self) -> dict[str, Any]:
        result = _slack_call(self.token, "auth.test", {})
        team_id = str(result.get("team_id") or "")
        if not ID_PATTERN.fullmatch(team_id):
            raise NativeContinuationError(
                "Tether could not resolve the Slack workspace for its bot token",
                code="workspace_mismatch",
            )
        with self._workspace_lock:
            if self._workspace_team_id and self._workspace_team_id != team_id:
                raise NativeContinuationError(
                    "Tether's Slack workspace identity changed",
                    code="workspace_mismatch",
                )
            self._workspace_team_id = team_id
        return {
            "ok": True,
            "team_id": team_id,
            "user_id": str(result.get("user_id") or ""),
            "user": str(result.get("user") or ""),
        }

    @staticmethod
    def _reconciliation_match(
        target_kind: str,
        target_id: str,
        messages: list[Any],
    ) -> str:
        for candidate in messages:
            if not isinstance(candidate, dict):
                continue
            if target_kind == "file":
                if any(
                    isinstance(item, dict)
                    and str(item.get("id") or "") == target_id
                    for item in candidate.get("files") or []
                ):
                    return str(candidate.get("ts") or "")
                continue
            metadata = candidate.get("metadata")
            if not isinstance(metadata, dict):
                continue
            event_payload = metadata.get("event_payload")
            if not isinstance(event_payload, dict):
                continue
            if target_kind == "root":
                bridge_id, separator, client_msg_id = target_id.partition(":")
                matched = (
                    separator
                    and metadata.get("event_type") == "tether_root"
                    and event_payload.get("bridge_id") == bridge_id
                    and event_payload.get("client_msg_id") == client_msg_id
                )
            else:
                event_type = (
                    "tether_reply"
                    if target_kind == "reply"
                    else "tether_message"
                )
                matched = (
                    metadata.get("event_type") == event_type
                    and event_payload.get("client_msg_id") == target_id
                )
            if matched and candidate.get("ts"):
                return str(candidate["ts"])
        return ""

    def _process_reconciliation(self, reconciliation_key: str) -> str:
        claimed = self.store.claim_reconciliation_page(reconciliation_key)
        status = str(claimed.get("status") or "")
        if status == "found":
            return str(claimed.get("result_ts") or "")
        if status == "not_found":
            return ""
        if status == "failed":
            raise NativeContinuationError(
                "Slack delivery reconciliation failed closed.",
                code="slack_reconciliation_failed",
            )
        if status != "claimed":
            raise NativeContinuationError(
                "Slack delivery reconciliation is pending.",
                code="slack_reconciliation_pending",
            )

        method = str(claimed["method"])
        payload: dict[str, Any] = {
            "channel": str(claimed["channel_id"]),
            "limit": RECONCILIATION_PAGE_LIMIT,
            "oldest": str(claimed["oldest_ts"]),
            "include_all_metadata": True,
        }
        thread_ts = str(claimed.get("thread_ts") or "")
        if method == "conversations.replies":
            payload["ts"] = thread_ts
        cursor = str(claimed.get("next_cursor") or "")
        if cursor:
            payload["cursor"] = cursor
        result = _slack_call(self.token, method, payload)
        messages = result.get("messages")
        if not isinstance(messages, list):
            raise RuntimeError(
                "Slack reconciliation returned an invalid message page"
            )
        result_ts = self._reconciliation_match(
            str(claimed["target_kind"]),
            str(claimed["target_id"]),
            messages,
        )
        metadata = result.get("response_metadata")
        if metadata is not None and not isinstance(metadata, dict):
            raise RuntimeError(
                "Slack reconciliation returned invalid response metadata"
            )
        raw_cursor = (metadata or {}).get("next_cursor") or ""
        if not isinstance(raw_cursor, str):
            raw_cursor = "\x00"
        advanced = self.store.complete_reconciliation_page(
            reconciliation_key,
            expected_cursor=cursor,
            next_cursor=raw_cursor,
            result_ts=result_ts,
        )
        advanced_status = str(advanced["status"])
        if advanced_status == "found":
            return str(advanced["result_ts"])
        if advanced_status == "not_found":
            return ""
        if advanced_status == "failed":
            raise NativeContinuationError(
                "Slack delivery reconciliation failed closed.",
                code="slack_reconciliation_failed",
            )
        raise NativeContinuationError(
            "Slack delivery reconciliation is pending.",
            code="slack_reconciliation_pending",
        )

    def _reconcile_target(
        self,
        *,
        team_id: str,
        method: str,
        channel_id: str,
        thread_ts: str,
        target_kind: str,
        target_id: str,
    ) -> tuple[str, str]:
        reconciliation_key = self.store.reconciliation_key(
            target_kind,
            team_id,
            channel_id,
            thread_ts,
            target_id,
        )
        self.store.ensure_reconciliation(
            reconciliation_key=reconciliation_key,
            team_id=team_id,
            method=method,
            channel_id=channel_id,
            thread_ts=thread_ts,
            target_kind=target_kind,
            target_id=target_id,
        )
        return reconciliation_key, self._process_reconciliation(
            reconciliation_key
        )

    def _reset_reconciliation(
        self,
        *,
        target_kind: str,
        team_id: str,
        channel_id: str,
        thread_ts: str,
        target_id: str,
    ) -> None:
        self.store.reset_reconciliation(
            self.store.reconciliation_key(
                target_kind,
                team_id,
                channel_id,
                thread_ts,
                target_id,
            )
        )

    def _arm_reconciliation(
        self,
        *,
        team_id: str,
        method: str,
        channel_id: str,
        thread_ts: str,
        target_kind: str,
        target_id: str,
    ) -> None:
        reconciliation_key = self.store.reconciliation_key(
            target_kind,
            team_id,
            channel_id,
            thread_ts,
            target_id,
        )
        self.store.reset_reconciliation(reconciliation_key)
        self.store.ensure_reconciliation(
            reconciliation_key=reconciliation_key,
            team_id=team_id,
            method=method,
            channel_id=channel_id,
            thread_ts=thread_ts,
            target_kind=target_kind,
            target_id=target_id,
        )
        self.store.defer_reconciliation(reconciliation_key)

    def _find_staged_root(
        self,
        bridge: Bridge,
        client_msg_id: str,
        requested_thread_ts: str,
    ) -> str:
        if requested_thread_ts:
            method = "conversations.replies"
            thread_ts = requested_thread_ts
        else:
            method = "conversations.history"
            thread_ts = ""
        _key, message_ts = self._reconcile_target(
            team_id=bridge.team_id,
            method=method,
            channel_id=bridge.channel_id,
            thread_ts=thread_ts,
            target_kind="root",
            target_id=f"{bridge.bridge_id}:{client_msg_id}",
        )
        return requested_thread_ts if message_ts and requested_thread_ts else message_ts

    def _find_staged_root_file(
        self,
        bridge: Bridge,
        file_id: str,
    ) -> str:
        if (
            not bridge.thread_ts
            or not SLACK_FILE_ID_PATTERN.fullmatch(file_id)
        ):
            return ""
        _key, message_ts = self._reconcile_target(
            team_id=bridge.team_id,
            method="conversations.replies",
            channel_id=bridge.channel_id,
            thread_ts=bridge.thread_ts,
            target_kind="file",
            target_id=file_id,
        )
        return message_ts or ""

    def _finish_root_file_locally(
        self,
        bridge: Bridge,
        lease_id: str,
        file_id: str,
        staged: Any,
        message_ts: str,
    ) -> str:
        if not self.store.complete_root_file(
            bridge.bridge_id,
            lease_id,
            message_ts or str(bridge.thread_ts or ""),
            file_id=file_id,
        ):
            raise RuntimeError(
                "Slack root attachment lost its durable lease"
            )
        self._reset_reconciliation(
            target_kind="file",
            team_id=bridge.team_id,
            channel_id=bridge.channel_id,
            thread_ts=str(bridge.thread_ts or ""),
            target_id=file_id,
        )
        with contextlib.suppress(OSError):
            staged.path.unlink()
        return message_ts or str(bridge.thread_ts or "")

    def _reconcile_root_file(
        self,
        bridge: Bridge,
        lease_id: str,
        file_id: str,
        staged: Any,
        *,
        expected: tuple[str, ...],
    ) -> str:
        if not self.store.set_root_file_upload_phase(
            bridge.bridge_id,
            lease_id,
            "reconciling",
            expected=expected,
            file_id=file_id,
        ):
            raise RuntimeError(
                "Slack root attachment lost its reconciliation lease"
            )
        try:
            message_ts = self._find_staged_root_file(bridge, file_id)
        except BaseException:
            self.store.set_root_file_upload_phase(
                bridge.bridge_id,
                lease_id,
                "completion_uncertain",
                expected=("reconciling",),
                file_id=file_id,
            )
            raise
        if message_ts:
            return self._finish_root_file_locally(
                bridge,
                lease_id,
                file_id,
                staged,
                message_ts,
            )
        if not self.store.set_root_file_upload_phase(
            bridge.bridge_id,
            lease_id,
            "completion_uncertain",
            expected=("reconciling",),
            file_id=file_id,
        ):
            raise RuntimeError(
                "Slack root attachment lost its reconciliation lease"
            )
        return ""

    def _complete_root_file_upload(
        self,
        bridge: Bridge,
        lease_id: str,
        file_id: str,
        filename: str,
        staged: Any,
        *,
        expected: tuple[str, ...],
    ) -> str:
        self._arm_reconciliation(
            target_kind="file",
            team_id=bridge.team_id,
            method="conversations.replies",
            channel_id=bridge.channel_id,
            thread_ts=str(bridge.thread_ts or ""),
            target_id=file_id,
        )
        if not self.store.set_root_file_upload_phase(
            bridge.bridge_id,
            lease_id,
            "completing",
            expected=expected,
            file_id=file_id,
        ):
            raise RuntimeError(
                "Slack root attachment lost its completion lease"
            )
        try:
            _complete_slack_upload(
                self.token,
                bridge.channel_id,
                file_id,
                filename=filename,
                thread_ts=bridge.thread_ts,
            )
        except BaseException:
            self.store.set_root_file_upload_phase(
                bridge.bridge_id,
                lease_id,
                "completion_uncertain",
                expected=("completing",),
                file_id=file_id,
            )
            raise
        if not self.store.set_root_file_upload_phase(
            bridge.bridge_id,
            lease_id,
            "completion_confirmed",
            expected=("completing",),
            file_id=file_id,
        ):
            raise RuntimeError(
                "Slack root attachment completion was accepted but its "
                "durable lease was lost"
            )
        return self._finish_root_file_locally(
            bridge,
            lease_id,
            file_id,
            staged,
            str(bridge.thread_ts or ""),
        )

    def _deliver_root_file(
        self,
        bridge: Bridge,
        claimed: dict[str, Any],
        lease_id: str,
        staged: Any,
    ) -> str:
        filename = str(claimed.get("upload_filename") or "")
        phase = str(claimed.get("upload_phase") or "reserved")
        file_id = str(claimed.get("slack_file_id") or "")
        _require_safe_staged_upload(staged)

        if phase in {"completion_confirmed", "completed"}:
            if not SLACK_FILE_ID_PATTERN.fullmatch(file_id):
                raise RuntimeError(
                    "durable Slack upload completion has no valid file ID"
                )
            return self._finish_root_file_locally(
                bridge,
                lease_id,
                file_id,
                staged,
                str(claimed.get("file_message_ts") or bridge.thread_ts or ""),
            )

        if phase in {"completing", "completion_uncertain", "reconciling"}:
            if not SLACK_FILE_ID_PATTERN.fullmatch(file_id):
                raise RuntimeError(
                    "uncertain Slack upload completion has no valid file ID"
                )
            message_ts = self._reconcile_root_file(
                bridge,
                lease_id,
                file_id,
                staged,
                expected=(phase,),
            )
            if message_ts:
                return message_ts
            return self._complete_root_file_upload(
                bridge,
                lease_id,
                file_id,
                filename,
                staged,
                expected=("completion_uncertain",),
            )

        if phase == "bytes_uploaded":
            if not SLACK_FILE_ID_PATTERN.fullmatch(file_id):
                raise RuntimeError(
                    "uploaded Slack bytes have no valid file ID"
                )
            return self._complete_root_file_upload(
                bridge,
                lease_id,
                file_id,
                filename,
                staged,
                expected=("bytes_uploaded",),
            )

        if not self.store.begin_root_file_allocation(
            bridge.bridge_id,
            lease_id,
        ):
            raise RuntimeError(
                "Slack root attachment lost its allocation lease"
            )
        try:
            file_id, upload_url = _allocate_slack_upload(
                self.token,
                filename,
                int(staged.size),
            )
        except BaseException:
            self.store.set_root_file_upload_phase(
                bridge.bridge_id,
                lease_id,
                "allocation_uncertain",
                expected=("allocating",),
            )
            raise
        if not self.store.record_root_file_allocation(
            bridge.bridge_id,
            lease_id,
            file_id,
        ):
            raise RuntimeError(
                "Slack root attachment lost its allocation lease"
            )
        if not self.store.set_root_file_upload_phase(
            bridge.bridge_id,
            lease_id,
            "uploading_bytes",
            expected=("allocated",),
            file_id=file_id,
        ):
            raise RuntimeError(
                "Slack root attachment lost its byte-upload lease"
            )
        try:
            _upload_slack_bytes(self.token, upload_url, staged)
        except BaseException:
            self.store.set_root_file_upload_phase(
                bridge.bridge_id,
                lease_id,
                "bytes_uncertain",
                expected=("uploading_bytes",),
                file_id=file_id,
            )
            raise
        if not self.store.set_root_file_upload_phase(
            bridge.bridge_id,
            lease_id,
            "bytes_uploaded",
            expected=("uploading_bytes",),
            file_id=file_id,
        ):
            raise RuntimeError(
                "Slack root attachment lost its byte-upload lease"
            )
        return self._complete_root_file_upload(
            bridge,
            lease_id,
            file_id,
            filename,
            staged,
            expected=("bytes_uploaded",),
        )

    def _deliver_staged_root(self, bridge: Bridge) -> dict[str, Any]:
        self.require_workspace(bridge.team_id)
        claimed = self.store.claim_root(
            bridge.bridge_id,
            lease_owner=self.lease_owner,
        )
        if claimed["status"] == "complete":
            return {
                "ok": True,
                "bridge_id": bridge.bridge_id,
                "thread_ts": str(claimed.get("thread_ts") or bridge.thread_ts or ""),
                "deduplicated": True,
            }
        if claimed["status"] != "claimed":
            raise NativeContinuationError(
                "The Slack root is already being delivered.",
                code="root_delivery_in_progress",
                binding_id=bridge.bridge_id,
                retryable=True,
                status="pending",
                next_action="Retry the same idempotency key without changing the payload.",
            )
        lease_id = str(claimed["lease_id"])
        previous_state = str(claimed.get("previous_state") or "")
        thread_ts = str(claimed.get("thread_ts") or "")
        requested_thread_ts = str(claimed.get("requested_thread_ts") or "")
        try:
            if not thread_ts:
                if previous_state in {"delivering", "uncertain"}:
                    thread_ts = self._find_staged_root(
                        bridge,
                        str(claimed["client_msg_id"]),
                        requested_thread_ts,
                    )
                if not thread_ts:
                    self._ensure_channel_membership(bridge.channel_id)
                    root_target_id = (
                        f"{bridge.bridge_id}:{claimed['client_msg_id']}"
                    )
                    self._arm_reconciliation(
                        target_kind="root",
                        team_id=bridge.team_id,
                        method=(
                            "conversations.replies"
                            if requested_thread_ts
                            else "conversations.history"
                        ),
                        channel_id=bridge.channel_id,
                        thread_ts=requested_thread_ts,
                        target_id=root_target_id,
                    )
                    message_ts = slack_post(
                        self.token,
                        bridge.channel_id,
                        str(claimed["payload_text"]),
                        requested_thread_ts or None,
                        client_msg_id=str(claimed["client_msg_id"]),
                        metadata_event_type="tether_root",
                        metadata_event_payload={
                            "bridge_id": bridge.bridge_id,
                            "client_msg_id": str(claimed["client_msg_id"]),
                        },
                    )
                    thread_ts = requested_thread_ts or message_ts
                if not self.store.record_root_post(
                    bridge.bridge_id,
                    lease_id,
                    thread_ts,
                ):
                    raise RuntimeError(
                        "Slack root delivery lost its durable lease"
                    )
                _after_durable_delivery(
                    "root reconciliation",
                    lambda: self._reset_reconciliation(
                        target_kind="root",
                        team_id=bridge.team_id,
                        channel_id=bridge.channel_id,
                        thread_ts=requested_thread_ts,
                        target_id=(
                            f"{bridge.bridge_id}:{claimed['client_msg_id']}"
                        ),
                    ),
                )
                bridge = self.store.get(bridge.bridge_id) or bridge

            staged = _root_staged_upload(claimed)
            if staged is not None:
                self._deliver_root_file(
                    bridge,
                    claimed,
                    lease_id,
                    staged,
                )
            accepted_thread = str(bridge.thread_ts or thread_ts)
            if accepted_thread:
                _after_durable_delivery(
                    "thread participation",
                    lambda: self.store.mark_participation(
                        bridge.team_id,
                        bridge.channel_id,
                        accepted_thread,
                    ),
                )
            return {
                "ok": True,
                "bridge_id": bridge.bridge_id,
                "thread_ts": bridge.thread_ts or thread_ts,
                "deduplicated": previous_state != "reserved",
            }
        except BaseException as exc:
            self.store.release_root(
                bridge.bridge_id,
                lease_id,
                f"{type(exc).__name__}: {exc}",
            )
            raise

    def _notify(
        self,
        incoming: BridgeRequest,
        config: Config,
        allowed_users: tuple[str, ...],
    ) -> dict[str, Any]:
        request = BridgeRequest(incoming)
        request["channel_id"] = str(request.get("channel_id") or effective_channel(config))
        request["owner_user_id"] = str(
            request.get("owner_user_id") or config.default_owner or ("*" if allowed_users else "")
        )
        request["team_id"] = self.require_workspace(
            str(request.get("team_id") or config.team_id)
        )
        if (
            request["channel_id"].startswith(("C", "G"))
            and request["owner_user_id"] != "*"
            and not config.allow_channel_owner_restrictions
        ):
            raise ValueError(
                "owner-restricted shared-channel bridges are disabled; omit --owner "
                "or explicitly set allow_channel_owner_restrictions=true"
            )
        text = str(request.get("text") or "")
        if not text.strip() or len(text) > MAX_TEXT:
            raise ValueError("notification text is empty or too large")
        bridge = self.store.create(request)
        existing_root = self.store.root_record(bridge.bridge_id)
        if (
            bridge.status == "active"
            and bridge.thread_ts
            and (
                existing_root is None
                or str(existing_root.get("state") or "") == "complete"
            )
        ):
            return {
                "ok": True,
                "bridge_id": bridge.bridge_id,
                "thread_ts": bridge.thread_ts,
                "deduplicated": True,
            }
        root_text = with_origin(text, bridge)
        requested_thread = str(request.get("thread_ts") or "")
        self._ensure_channel_membership(bridge.channel_id)
        if existing_root is None:
            staged = None
            file_path = str(request.get("file_path") or "")
            try:
                if file_path:
                    staged = stage_safe_upload(file_path)
                existing_root = self.store.reserve_root(
                    bridge.bridge_id,
                    root_text,
                    requested_thread,
                    staged_upload=staged,
                    upload_filename=(
                        _stable_root_filename(bridge.bridge_id, file_path)
                        if file_path
                        else ""
                    ),
                )
            except BaseException:
                if staged is not None:
                    with contextlib.suppress(OSError):
                        staged.path.unlink()
                raise
        else:
            safe_text = redact_text(root_text)[:MAX_TEXT]
            file_path = str(request.get("file_path") or "")
            if (
                str(existing_root.get("payload_text") or "") != safe_text
                or str(existing_root.get("requested_thread_ts") or "")
                != requested_thread
                or bool(existing_root.get("staged_path")) != bool(file_path)
                or (
                    file_path
                    and str(existing_root.get("upload_filename") or "")
                    != _stable_root_filename(bridge.bridge_id, file_path)
                )
            ):
                raise ValueError(
                    "idempotency key already belongs to a different "
                    "Slack root payload"
                )
        return self._deliver_staged_root(bridge)

    def _find_staged_reply(
        self,
        bridge: Bridge,
        client_msg_id: str,
    ) -> str:
        if not bridge.thread_ts:
            return ""
        _key, message_ts = self._reconcile_target(
            team_id=bridge.team_id,
            method="conversations.replies",
            channel_id=bridge.channel_id,
            thread_ts=bridge.thread_ts,
            target_kind="reply",
            target_id=client_msg_id,
        )
        return message_ts

    def _deliver_staged_reply(
        self,
        bridge: Bridge,
        reply_key: str,
    ) -> dict[str, Any]:
        self.require_workspace(bridge.team_id)
        claimed = self.store.claim_reply(
            reply_key,
            bridge.bridge_id,
            lease_owner=self.lease_owner,
        )
        if claimed["status"] == "sent":
            return {
                "ok": True,
                "bridge_id": bridge.bridge_id,
                "thread_ts": bridge.thread_ts,
                "message_ts": claimed.get("message_ts", ""),
                "deduplicated": True,
                "acknowledged_events": 0,
            }
        if claimed["status"] != "claimed":
            raise NativeContinuationError(
                "The exact Slack reply is already being delivered.",
                code="reply_delivery_in_progress",
                binding_id=bridge.bridge_id,
                retryable=True,
                status="pending",
                next_action="Retry the same reply key without changing the text.",
            )
        lease_id = claimed["lease_id"]
        client_msg_id = claimed["client_msg_id"]

        timestamp = ""
        if claimed.get("previous_state") in {"uncertain", "delivering"}:
            try:
                timestamp = self._find_staged_reply(bridge, client_msg_id)
            except Exception:
                self.store.record_reply_error(
                    reply_key,
                    bridge.bridge_id,
                    lease_id,
                    "Slack reconciliation outcome is uncertain",
                )
                raise
        if not timestamp:
            self._ensure_channel_membership(bridge.channel_id)
            self._arm_reconciliation(
                target_kind="reply",
                team_id=bridge.team_id,
                method="conversations.replies",
                channel_id=bridge.channel_id,
                thread_ts=str(bridge.thread_ts or ""),
                target_id=client_msg_id,
            )
            try:
                timestamp = slack_post(
                    self.token,
                    bridge.channel_id,
                    claimed["text"],
                    bridge.thread_ts,
                    client_msg_id=client_msg_id,
                )
            except Exception as exc:
                self.store.record_reply_error(
                    reply_key,
                    bridge.bridge_id,
                    lease_id,
                    f"{type(exc).__name__}: Slack delivery outcome is uncertain",
                )
                raise

        acknowledged = self.store.complete_reply(
            reply_key,
            bridge.bridge_id,
            lease_id,
            timestamp,
        )
        _after_durable_delivery(
            "reply reconciliation",
            lambda: self._reset_reconciliation(
                target_kind="reply",
                team_id=bridge.team_id,
                channel_id=bridge.channel_id,
                thread_ts=str(bridge.thread_ts or ""),
                target_id=client_msg_id,
            ),
        )
        if acknowledged > 0:
            _after_durable_delivery(
                "native reply wake",
                lambda: self._wake_bridge(bridge.bridge_id),
            )
        _after_durable_delivery(
            "thread participation",
            lambda: self.store.mark_participation(
                bridge.team_id,
                bridge.channel_id,
                bridge.thread_ts,
            ),
        )
        return {
            "ok": True,
            "bridge_id": bridge.bridge_id,
            "thread_ts": bridge.thread_ts,
            "message_ts": timestamp,
            "deduplicated": claimed.get("previous_state") != "pending",
            "acknowledged_events": acknowledged,
        }

    def _deliver_staged_message(
        self,
        idempotency_key: str,
    ) -> dict[str, Any]:
        claimed = self.store.claim_message(
            idempotency_key,
            lease_owner=self.lease_owner,
        )
        if claimed["status"] == "sent":
            return {
                "ok": True,
                "thread_ts": str(claimed.get("thread_ts") or ""),
                "message_ts": str(claimed.get("message_ts") or ""),
                "deduplicated": True,
            }
        if claimed["status"] == "terminal":
            raise NativeContinuationError(
                "The durable Slack message was cancelled by recovery policy.",
                code="message_delivery_cancelled",
                retryable=False,
                status=str(claimed.get("state") or "cancelled"),
                next_action=(
                    "Create a new message with a new idempotency key only "
                    "after confirming the cancellation was intentional."
                ),
            )
        if claimed["status"] != "claimed":
            raise NativeContinuationError(
                "The Slack message is already being delivered.",
                code="root_delivery_in_progress",
                retryable=True,
                status="pending",
                next_action=(
                    "Retry the same idempotency key without changing the payload."
                ),
            )
        lease_id = str(claimed["lease_id"])
        team_id = self.require_workspace(str(claimed["team_id"]))
        channel_id = str(claimed["channel_id"])
        thread_ts = str(claimed["thread_ts"])
        client_msg_id = str(claimed["client_msg_id"])
        operation = str(claimed.get("operation") or "post")
        target_message_ts = str(claimed.get("target_message_ts") or "")
        message_ts = ""
        if operation not in {"post", "update"}:
            self.store.record_message_error(
                idempotency_key,
                lease_id,
                "Slack outbox operation is invalid",
            )
            raise RuntimeError("Slack outbox operation is invalid")
        if operation == "post" and str(claimed.get("previous_state") or "") in {
            "uncertain",
            "delivering",
        }:
            try:
                _key, message_ts = self._reconcile_target(
                    team_id=team_id,
                    method=(
                        "conversations.replies"
                        if thread_ts
                        else "conversations.history"
                    ),
                    channel_id=channel_id,
                    thread_ts=thread_ts,
                    target_kind="message",
                    target_id=client_msg_id,
                )
            except Exception:
                self.store.record_message_error(
                    idempotency_key,
                    lease_id,
                    "Slack message reconciliation is pending or failed",
                )
                raise
        if not message_ts:
            self._ensure_channel_membership(channel_id)
            if operation == "post":
                self._arm_reconciliation(
                    target_kind="message",
                    team_id=team_id,
                    method=(
                        "conversations.replies"
                        if thread_ts
                        else "conversations.history"
                    ),
                    channel_id=channel_id,
                    thread_ts=thread_ts,
                    target_id=client_msg_id,
                )
            try:
                options = (
                    dict(claimed["options"])
                    if isinstance(claimed.get("options"), dict)
                    else {}
                )
                try:
                    if operation == "update":
                        message_ts = slack_update(
                            self.token,
                            channel_id,
                            target_message_ts,
                            str(claimed["text"]),
                            options=options,
                        )
                    else:
                        message_ts = slack_post(
                            self.token,
                            channel_id,
                            str(claimed["text"]),
                            thread_ts or None,
                            client_msg_id=client_msg_id,
                            metadata_event_type="tether_message",
                            metadata_event_payload={
                                "client_msg_id": client_msg_id,
                            },
                            options=options,
                        )
                except SlackAPIError as exc:
                    if exc.code != "invalid_blocks" or "blocks" not in options:
                        raise
                    fallback_options = dict(options)
                    fallback_options.pop("blocks", None)
                    if operation == "update":
                        message_ts = slack_update(
                            self.token,
                            channel_id,
                            target_message_ts,
                            str(claimed["text"]),
                            options=fallback_options,
                        )
                    else:
                        message_ts = slack_post(
                            self.token,
                            channel_id,
                            str(claimed["text"]),
                            thread_ts or None,
                            client_msg_id=client_msg_id,
                            metadata_event_type="tether_message",
                            metadata_event_payload={
                                "client_msg_id": client_msg_id,
                            },
                            options=fallback_options,
                        )
            except Exception as exc:
                self.store.record_message_error(
                    idempotency_key,
                    lease_id,
                    f"{type(exc).__name__}: Slack delivery outcome is uncertain",
                )
                raise
        self.store.complete_message(
            idempotency_key,
            lease_id,
            message_ts,
        )
        if operation == "post":
            _after_durable_delivery(
                "message reconciliation",
                lambda: self._reset_reconciliation(
                    target_kind="message",
                    team_id=team_id,
                    channel_id=channel_id,
                    thread_ts=thread_ts,
                    target_id=client_msg_id,
                ),
            )
        if thread_ts:
            _after_durable_delivery(
                "thread participation",
                lambda: self.store.mark_participation(
                    team_id,
                    channel_id,
                    thread_ts,
                ),
            )
        return {
            "ok": True,
            "thread_ts": thread_ts,
            "message_ts": message_ts,
            "deduplicated": str(claimed.get("previous_state") or "") != "pending",
        }

    def recover_reconciliations(self) -> int:
        recovered = 0
        for reconciliation_key in self.store.pending_reconciliation_keys():
            try:
                self._process_reconciliation(reconciliation_key)
                recovered += 1
            except NativeContinuationError as exc:
                if exc.code == "slack_reconciliation_pending":
                    continue
            except Exception as exc:
                _warn_recovery_failure("reconciliation", exc)
                continue
        return recovered

    def recover_messages(self) -> int:
        self.store.release_abandoned_message_leases(self.lease_owner)
        recovered = 0
        with self._message_lock:
            for idempotency_key in self.store.pending_message_keys():
                try:
                    self._deliver_staged_message(idempotency_key)
                    recovered += 1
                except Exception as exc:
                    _warn_recovery_failure("message", exc)
                    continue
        return recovered

    def recover_replies(self) -> int:
        with self._recovery_lock:
            self.store.release_abandoned_reply_leases(self.lease_owner)
            recovered = 0
            for reply_key, bridge_id in self.store.pending_reply_keys():
                bridge = self.store.get(bridge_id)
                if bridge is None or not bridge.thread_ts:
                    continue
                try:
                    with self._reply_lock:
                        self._deliver_staged_reply(bridge, reply_key)
                    recovered += 1
                except Exception as exc:
                    _warn_recovery_failure("reply", exc)
                    continue
            return recovered

    def recover_roots(self) -> int:
        recovered = 0
        with self._notify_lock:
            for bridge_id in self.store.pending_root_ids():
                bridge = self.store.get(bridge_id)
                if bridge is None or bridge.status == "closed":
                    continue
                try:
                    self._deliver_staged_root(bridge)
                    recovered += 1
                except Exception as exc:
                    _warn_recovery_failure("root-message", exc)
                    continue
        return recovered

    def run_reply_recovery(
        self,
        stop_event: threading.Event,
        interval_seconds: float = 10.0,
    ) -> None:
        _SLACK_CALL_CONTEXT.stop_event = stop_event
        try:
            while not stop_event.is_set():
                self.recover_reconciliations()
                self.recover_roots()
                self.recover_replies()
                self.recover_messages()
                if time.monotonic() - self._last_maintenance_at >= 24 * 3600:
                    self.run_maintenance()
                stop_event.wait(max(1.0, interval_seconds))
        finally:
            with contextlib.suppress(AttributeError):
                del _SLACK_CALL_CONTEXT.stop_event

    def run_maintenance(self) -> dict[str, int]:
        counts = self.store.prune(load_config().retention_days)
        self._last_maintenance_at = time.monotonic()
        return counts

    def _reply(self, request: BridgeRequest) -> dict[str, Any]:
        bridge = self.store.get(str(request.get("bridge_id") or ""))
        if not bridge or not bridge.thread_ts:
            raise ValueError("active bridge not found")
        self.require_workspace(bridge.team_id)
        text = validate_reply_text(str(request.get("text") or ""))
        reply_key = str(request.get("reply_key") or "")
        if not reply_key:
            raise ValueError("reply key is required")
        if not REPLY_KEY_PATTERN.fullmatch(reply_key):
            raise ValueError("invalid reply key")
        if text == "NO_REPLY":
            if not reply_key:
                raise ValueError("NO_REPLY requires a delivery attempt reply key")
            self.store.validate_attempt_reply(reply_key, bridge.bridge_id)
            acknowledged = self.store.acknowledge_attempt(
                reply_key, bridge.bridge_id, ack_kind="no_reply"
            )
            if acknowledged <= 0:
                raise ValueError(
                    "reply key is not attached to a live delivery attempt"
                )
            self._wake_bridge(bridge.bridge_id)
            return {
                "ok": True,
                "bridge_id": bridge.bridge_id,
                "thread_ts": bridge.thread_ts,
                "suppressed": True,
                "acknowledged_events": acknowledged,
            }
        stage_reply_payload(
            self.store,
            bridge.bridge_id,
            reply_key,
            text,
        )
        return self._deliver_staged_reply(bridge, reply_key)

    def _rebind(
        self,
        request: BridgeRequest,
        config: Config,
    ) -> dict[str, Any]:
        team_id = self.require_workspace(
            str(request.get("team_id") or config.team_id)
        )
        channel = str(request.get("channel_id") or "")
        thread_ts = str(request.get("thread_ts") or "")
        if not team_id or not channel or not thread_ts:
            raise ValueError(
                "Slack workspace, channel, and thread timestamp are required"
            )
        bridge = self.store.find_thread(team_id, channel, thread_ts)
        if bridge is None:
            raise ValueError("active bridge not found")
        source_kind = str(request.get("source_kind") or "")
        source = request.get("source")
        rebound = self.store.rebind(
            bridge.bridge_id,
            source_kind,
            source,
            expected_generation=bridge.binding_generation,
        )
        return {
            "ok": True,
            "bridge_id": rebound.bridge_id,
            "thread_ts": rebound.thread_ts,
            "source_kind": rebound.source_kind,
            "binding_generation": rebound.binding_generation,
        }

    def _close(
        self,
        request: BridgeRequest,
        config: Config,
    ) -> dict[str, Any]:
        requested_team = str(request.get("team_id") or "")
        if requested_team:
            self.require_workspace(requested_team)
        bridge_id = str(request.get("bridge_id") or "")
        bridge = self.store.get(bridge_id) if bridge_id else None
        if bridge is None:
            team_id = self.require_workspace(
                str(request.get("team_id") or config.team_id)
            )
            channel = str(request.get("channel_id") or "")
            thread_ts = str(request.get("thread_ts") or "")
            if not channel or not thread_ts:
                raise ValueError(
                    "bridge ID or Slack workspace, channel, and thread timestamp "
                    "are required"
                )
            bridge = self.store.find_thread(team_id, channel, thread_ts)
        if bridge is None:
            raise ValueError("active bridge not found")
        self.require_workspace(bridge.team_id)
        expected = request.get("expected_generation")
        closed = self.store.close(
            bridge.bridge_id,
            expected_generation=(
                int(expected)
                if expected is not None
                else bridge.binding_generation
            ),
        )
        return {
            "ok": True,
            "bridge_id": closed.bridge_id,
            "thread_ts": closed.thread_ts or "",
            "status": closed.status,
            "binding_generation": closed.binding_generation,
        }

    def _attach(
        self,
        incoming: BridgeRequest,
        config: Config,
        allowed_users: tuple[str, ...],
    ) -> dict[str, Any]:
        request = BridgeRequest(incoming)
        request["channel_id"] = str(request.get("channel_id") or effective_channel(config))
        request["owner_user_id"] = str(
            request.get("owner_user_id") or config.default_owner or ("*" if allowed_users else "")
        )
        request["team_id"] = self.require_workspace(
            str(request.get("team_id") or config.team_id)
        )
        thread_ts = str(request.get("thread_ts") or "")
        if not request["channel_id"] or not thread_ts:
            raise ValueError("Slack channel and existing thread timestamp are required")
        existing = self.store.find(request["team_id"], request["channel_id"], thread_ts)
        if existing is not None:
            if existing.idempotency_key == str(request.get("idempotency_key") or ""):
                return {
                    "ok": True,
                    "bridge_id": existing.bridge_id,
                    "thread_ts": existing.thread_ts,
                    "deduplicated": True,
                }
            raise ValueError("Slack thread already has an active Tether binding")
        bridge = self.store.create(request)
        bridge = self.store.bind(bridge.bridge_id, thread_ts)
        self.store.mark_participation(bridge.team_id, bridge.channel_id, thread_ts)
        return {
            "ok": True,
            "bridge_id": bridge.bridge_id,
            "thread_ts": bridge.thread_ts,
            "deduplicated": False,
        }

    def _history(self, request: BridgeRequest, config: Config) -> dict[str, Any]:
        self.require_workspace(str(request.get("team_id") or config.team_id))
        limit = max(1, min(int(request.get("limit", 15)), 100))
        channel = str(request.get("channel_id") or effective_channel(config))
        if not channel:
            raise ValueError("no Slack channel was provided and Hermes has no home channel")
        self._ensure_channel_membership(channel)
        result = _slack_call(self.token, "conversations.history", {"channel": channel, "limit": limit})
        messages = [
            {key: message.get(key) for key in ("ts", "text", "user", "bot_id") if message.get(key) is not None}
            for message in result.get("messages", [])
            if isinstance(message, dict)
        ]
        return {"ok": True, "messages": messages}

    def _thread_history(self, request: BridgeRequest, config: Config) -> dict[str, Any]:
        self.require_workspace(str(request.get("team_id") or config.team_id))
        limit = max(1, min(int(request.get("limit", 100)), 100))
        channel = str(request.get("channel_id") or effective_channel(config))
        thread_ts = str(request.get("thread_ts") or "")
        if not channel or not thread_ts:
            raise ValueError("Slack channel and thread timestamp are required")
        self._ensure_channel_membership(channel)
        result = _slack_call(
            self.token,
            "conversations.replies",
            {"channel": channel, "ts": thread_ts, "limit": limit},
        )
        messages = [
            {
                key: message.get(key)
                for key in ("ts", "thread_ts", "text", "user", "bot_id", "subtype")
                if message.get(key) is not None
            }
            for message in result.get("messages", [])
            if isinstance(message, dict)
        ]
        return {"ok": True, "messages": messages}

    def _thread_reply(self, request: BridgeRequest, config: Config) -> dict[str, Any]:
        team_id = self.require_workspace(
            str(request.get("team_id") or config.team_id)
        )
        channel = str(request.get("channel_id") or "")
        thread_ts = str(request.get("thread_ts") or "")
        text = str(request.get("text") or "")
        idempotency_key = str(request.get("idempotency_key") or "")
        if not channel or not thread_ts:
            raise ValueError("Slack channel and thread timestamp are required")
        if not text.strip() or len(text) > MAX_TEXT:
            raise ValueError("thread reply text is empty or too large")
        if not idempotency_key:
            raise ValueError("thread reply idempotency key is required")
        self.store.reserve_message(
            idempotency_key,
            team_id,
            channel,
            thread_ts,
            text,
        )
        return self._deliver_staged_message(idempotency_key)

    def _unresolved(
        self,
        request: BridgeRequest,
        config: Config,
    ) -> dict[str, Any]:
        team_id = self.require_workspace(
            str(request.get("team_id") or config.team_id)
        )
        limit = max(1, min(int(request.get("limit", 100)), 100))
        return {
            "ok": True,
            "operations": self.store.unresolved_operations(
                team_id,
                limit=limit,
            ),
        }

    def _resolve(
        self,
        request: BridgeRequest,
        config: Config,
    ) -> dict[str, Any]:
        team_id = self.require_workspace(
            str(request.get("team_id") or config.team_id)
        )
        resolution = self.store.resolve_uncertain_operation(
            team_id,
            str(request.get("kind") or ""),
            str(request.get("id") or ""),
            str(request.get("action") or ""),
        )
        if resolution["action"] == "retry":
            bridge_id = str(resolution.get("bridge_id") or "")
            if bridge_id:
                self._wake_bridge(bridge_id)
        return {"ok": True, **resolution}

    def handle(self, request: BridgeRequest) -> dict[str, Any]:
        operation = str(request.get("op", "notify"))
        config = load_config()
        allowed_users = effective_allowed_users(config)
        if operation == "status":
            return self._status(config, allowed_users)
        if operation == "identity":
            return self._identity()
        if operation == "herdr_context":
            return self._herdr_context(request)
        if operation == "maintenance":
            return {
                "ok": True,
                "pruned": self.run_maintenance(),
                "storage": self.store.storage_stats(),
            }
        if operation == "unresolved":
            return self._unresolved(request, config)
        if operation == "resolve":
            return self._resolve(request, config)
        if operation == "notify":
            with self._notify_lock:
                return self._notify(request, config, allowed_users)
        if operation == "attach":
            with self._notify_lock:
                return self._attach(request, config, allowed_users)
        if operation == "reply":
            with self._reply_lock:
                return self._reply(request)
        if operation == "rebind":
            return self._rebind(request, config)
        if operation == "close":
            return self._close(request, config)
        if operation == "history":
            return self._history(request, config)
        if operation == "thread_history":
            return self._thread_history(request, config)
        if operation == "thread_reply":
            with self._message_lock:
                return self._thread_reply(request, config)
        raise ValueError("unsupported operation")


class Handler(socketserver.StreamRequestHandler):
    def handle(self) -> None:
        try:
            if not isinstance(self.server, UnixServer):
                raise RuntimeError("invalid Tether broker server")
            self.request.settimeout(self.server.request_timeout_seconds)
            _pid, peer_uid, _gid = _peer_credentials(self.request)
            if peer_uid != self.server.owner_uid:
                raise NativeContinuationError(
                    "Tether rejected a broker request from a different Unix account",
                    code="peer_uid_mismatch",
                )
            raw = self.rfile.readline(MAX_BROKER_REQUEST_BYTES + 1)
            if len(raw) > MAX_BROKER_REQUEST_BYTES:
                raise ValueError("request too large")
            if not raw.endswith(b"\n"):
                raise ValueError("request is incomplete")
            request = json.loads(raw)
            if not isinstance(request, dict):
                raise ValueError("request must be a JSON object")
            response = self.server.broker.handle(request)
        except socket.timeout:
            response = _safe_error_response(
                NativeContinuationError(
                    "Tether closed an incomplete local broker request",
                    code="request_timeout",
                )
            )
        except Exception as exc:
            response = _safe_error_response(exc)
        with contextlib.suppress(BrokenPipeError, ConnectionResetError):
            self.wfile.write(_broker_response_frame(response))


class UnixServer(socketserver.ThreadingUnixStreamServer):
    daemon_threads = False
    block_on_close = True
    broker: Broker
    recovery_stop: threading.Event
    recovery_thread: threading.Thread
    lock_fd: int

    def __init__(
        self,
        server_address: str,
        request_handler_class: type[socketserver.BaseRequestHandler],
        *,
        max_connections: int = DEFAULT_BROKER_MAX_CONNECTIONS,
        request_timeout_seconds: float = DEFAULT_BROKER_READ_TIMEOUT_SECONDS,
    ) -> None:
        if not 1 <= max_connections <= 256:
            raise ValueError("broker max_connections must be between 1 and 256")
        if not 0.1 <= request_timeout_seconds <= 60:
            raise ValueError("broker request timeout must be between 0.1 and 60 seconds")
        self.owner_uid = os.geteuid()
        self.request_timeout_seconds = request_timeout_seconds
        self._connection_slots = threading.BoundedSemaphore(max_connections)
        super().__init__(server_address, request_handler_class)

    def process_request(
        self,
        request: socket.socket,
        client_address: Any,
    ) -> None:
        if not self._connection_slots.acquire(blocking=False):
            response = _safe_error_response(
                NativeContinuationError(
                    "Tether broker is at its local connection limit",
                    code="broker_busy",
                )
            )
            with contextlib.suppress(OSError):
                request.settimeout(0.2)
                request.sendall(_broker_response_frame(response))
            self.shutdown_request(request)
            return
        try:
            super().process_request(request, client_address)
        except BaseException:
            self._connection_slots.release()
            raise

    def process_request_thread(
        self,
        request: socket.socket,
        client_address: Any,
    ) -> None:
        try:
            super().process_request_thread(request, client_address)
        finally:
            self._connection_slots.release()

    def server_close(self) -> None:
        self.recovery_stop.set()
        recovery_thread = getattr(self, "recovery_thread", None)
        if (
            recovery_thread is not None
            and recovery_thread is not threading.current_thread()
        ):
            while recovery_thread.is_alive():
                recovery_thread.join()
        try:
            super().server_close()
        finally:
            lock_fd = getattr(self, "lock_fd", -1)
            if lock_fd >= 0:
                with contextlib.suppress(OSError):
                    fcntl.flock(lock_fd, fcntl.LOCK_UN)
                os.close(lock_fd)
                self.lock_fd = -1


def _peer_credentials(connection: socket.socket) -> tuple[int, int, int]:
    peer_option = getattr(socket, "SO_PEERCRED", None)
    if peer_option is None:
        raise RuntimeError("Tether requires Linux SO_PEERCRED support")
    raw = connection.getsockopt(
        socket.SOL_SOCKET,
        peer_option,
        struct.calcsize("3i"),
    )
    return struct.unpack("3i", raw)


def _acquire_broker_lock(path: Path) -> int:
    lock_path = path.with_name(path.name + ".lock")
    flags = (
        os.O_RDWR
        | os.O_CREAT
        | os.O_CLOEXEC
        | getattr(os, "O_NOFOLLOW", 0)
    )
    descriptor = os.open(lock_path, flags, 0o600)
    try:
        info = os.fstat(descriptor)
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.geteuid()
            or info.st_nlink != 1
        ):
            raise RuntimeError("Tether broker lock is not a private regular file")
        os.fchmod(descriptor, 0o600)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError(
                "another Tether broker already owns this database"
            ) from exc
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def open_locked_store(path: Path = DB_PATH) -> tuple[Store, int]:
    """Acquire the singleton lock before opening, migrating, or recovering SQLite."""
    candidate = Path(path).expanduser()
    security.secure_state_directory(candidate.parent, create=True)
    lock_fd = _acquire_broker_lock(candidate)
    try:
        return Store(candidate), lock_fd
    except BaseException:
        with contextlib.suppress(OSError):
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
        os.close(lock_fd)
        raise


def start_broker(
    token: str,
    path: Path = SOCKET_PATH,
    health_provider: Callable[[], dict[str, Any]] | None = None,
    attempt_closed: Callable[[str], None] | None = None,
    *,
    store: Store | None = None,
    lock_fd: int | None = None,
    max_connections: int = DEFAULT_BROKER_MAX_CONNECTIONS,
    request_timeout_seconds: float = DEFAULT_BROKER_READ_TIMEOUT_SECONDS,
) -> UnixServer:
    if os.geteuid() == 0:
        raise RuntimeError(
            "Tether refuses to run as root; use a dedicated non-root account"
        )
    security.secure_state_directory(path.parent, create=True)
    if store is None:
        if lock_fd is not None:
            raise ValueError("broker lock cannot be supplied without a store")
        active_store, active_lock_fd = open_locked_store(DB_PATH)
    else:
        if lock_fd is None or lock_fd < 0:
            raise ValueError("a supplied store requires its held singleton lock")
        active_store = store
        active_lock_fd = lock_fd
    try:
        if path.exists() or path.is_symlink():
            mode = path.lstat().st_mode
            if not stat.S_ISSOCK(mode):
                raise RuntimeError("refusing to replace non-socket bridge path")
            path.unlink()
        server = UnixServer(
            str(path),
            Handler,
            max_connections=max_connections,
            request_timeout_seconds=request_timeout_seconds,
        )
    except BaseException:
        fcntl.flock(active_lock_fd, fcntl.LOCK_UN)
        os.close(active_lock_fd)
        raise
    server.lock_fd = active_lock_fd
    server.broker = Broker(
        token,
        store=active_store,
        health_provider=health_provider,
        attempt_closed=attempt_closed,
    )
    server.broker.run_maintenance()
    server.recovery_stop = threading.Event()
    os.chmod(path, 0o600)
    threading.Thread(target=server.serve_forever, name="hermes-bridge-broker", daemon=True).start()
    server.recovery_thread = threading.Thread(
        target=server.broker.run_reply_recovery,
        args=(server.recovery_stop,),
        name="tether-slack-outbox-recovery",
        daemon=True,
    )
    server.recovery_thread.start()
    return server


def broker_call(request: BridgeRequest, path: Path = SOCKET_PATH) -> dict[str, Any]:
    if not isinstance(request, dict):
        raise ValueError("bridge request must be a JSON object")
    try:
        frame = (
            json.dumps(
                request,
                allow_nan=False,
                ensure_ascii=False,
                separators=(",", ":"),
            ).encode("utf-8")
            + b"\n"
        )
    except (TypeError, ValueError, RecursionError) as exc:
        raise ValueError("bridge request must be valid bounded JSON") from exc
    if len(frame) > MAX_BROKER_REQUEST_BYTES:
        raise ValueError("bridge request is too large")
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
        client.settimeout(35)
        try:
            client.connect(str(path))
            client.sendall(frame)
        except (BrokenPipeError, ConnectionResetError):
            # A saturated broker may send its structured rejection and close before
            # this client wins the write race. The response can still be readable.
            pass
        except OSError as exc:
            raise NativeContinuationError(
                "Tether could not reach its local broker",
                code="broker_unavailable",
            ) from exc
        chunks = bytearray()
        try:
            while not chunks.endswith(b"\n"):
                part = client.recv(65_536)
                if not part:
                    break
                chunks.extend(part)
                if len(chunks) > MAX_BROKER_RESPONSE_BYTES:
                    raise RuntimeError("bridge response is too large")
        except (BrokenPipeError, ConnectionResetError, socket.timeout, OSError) as exc:
            raise NativeContinuationError(
                "Tether lost its local broker connection",
                code="broker_unavailable",
            ) from exc
    if not chunks:
        raise NativeContinuationError(
            "Tether's local broker closed without a response",
            code="broker_unavailable",
        )
    try:
        result = json.loads(bytes(chunks))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise NativeContinuationError(
            "Tether's local broker returned an invalid response",
            code="broker_unavailable",
        ) from exc
    if (
        not isinstance(result, dict)
        or not isinstance(result.get("ok"), bool)
    ):
        raise NativeContinuationError(
            "Tether's local broker returned an invalid response contract",
            code="broker_unavailable",
        )
    if not result["ok"]:
        raise NativeContinuationError(
            str(result.get("message") or "Tether broker request failed")[:500],
            code=str(result.get("code") or "broker_internal_error"),
            binding_id=str(result.get("binding_id") or ""),
            retryable=bool(result.get("retryable", False)),
            status=str(result.get("status") or "failed"),
            next_action=str(result.get("next_action") or ""),
        )
    return result


def _short(value: Any) -> str:
    return _safe_label(value, 8)


def _safe_label(value: Any, limit: int = 48) -> str:
    return re.sub(r"[`\r\n\x00-\x1f\x7f]", "", str(value or ""))[:limit]


def origin_label(bridge: Bridge) -> str:
    source = bridge.source
    cwd = _safe_label(Path(str(source.get("cwd") or "")).name)
    zellij_session = _safe_label(
        source.get("zellij_session") or (source.get("session_name") if bridge.source_kind == "zellij_pane" else "")
    )
    zellij_pane = _safe_label(
        source.get("zellij_pane_id") or (source.get("pane_id") if bridge.source_kind == "zellij_pane" else "")
    )
    herdr_session = _safe_label(source.get("herdr_session"))
    herdr_terminal = _safe_label(source.get("herdr_terminal_id"))
    terminal = f" in Zellij `{zellij_session}`" if zellij_session else ""
    if terminal and zellij_pane:
        terminal += f" / pane `{zellij_pane}`"
    if herdr_session:
        terminal = f" in Herdr `{herdr_session}`"
        if herdr_terminal:
            terminal += f" / terminal `{herdr_terminal}`"
    if bridge.source_kind == "codex_session":
        label = f"Codex `{_short(source.get('session_id'))}`{terminal}"
    elif bridge.source_kind == "claude_session":
        label = f"Claude Code `{_short(source.get('session_id'))}`{terminal}"
    elif bridge.source_kind == "zellij_pane":
        label = f"Zellij `{zellij_session}` / pane `{zellij_pane}`"
    elif bridge.source_kind == "hermes_session":
        label = f"Hermes `{_short(source.get('session_id') or source.get('run_id'))}`"
    else:
        label = f"Headless run `{_short(source.get('run_id') or source.get('queue_id'))}`"
    return label + (f" · `{cwd}`" if cwd else "")


def with_origin(text: str, bridge: Bridge) -> str:
    suffix = f"\n\n_Origin: {origin_label(bridge)}_"
    return text.rstrip()[: MAX_TEXT - len(suffix)] + suffix


def _base_child_env() -> dict[str, str]:
    return {key: value for key, value in os.environ.items() if key in SAFE_CHILD_ENV}


def _resolve_executable(command: str) -> str:
    candidate = Path(command).expanduser()
    if candidate.is_absolute():
        if candidate.is_file() and os.access(candidate, os.X_OK):
            return str(candidate)
        raise NativeContinuationError(f"configured executable is unavailable: {candidate.name}")
    resolved = shutil.which(command, path=_base_child_env().get("PATH"))
    if resolved is None:
        raise NativeContinuationError(f"configured executable is unavailable: {command}")
    return resolved


def _resolve_credential_helper(command: str) -> str:
    candidate = Path(command).expanduser()
    if not candidate.is_absolute():
        raise NativeContinuationError(
            "credential helper executable must use an absolute path"
        )
    try:
        return str(security.validate_private_executable(candidate))
    except security.StatePathError as exc:
        raise NativeContinuationError(
            "credential helper executable failed ownership or mode validation"
        ) from exc


def _credential_key_is_forbidden(key: str) -> bool:
    upper = key.upper()
    return (
        upper in FORBIDDEN_CREDENTIAL_ENV
        or upper.startswith(FORBIDDEN_CREDENTIAL_PREFIXES)
    )


def _credential_env(bridge: Bridge, config: Config) -> dict[str, str]:
    if not config.credential_command:
        return {}
    metadata = json.dumps({
        "bridge_id": bridge.bridge_id,
        "source_kind": bridge.source_kind,
        "session_id": str(bridge.source.get("session_id") or "")[:128],
    })
    command = [
        _resolve_credential_helper(config.credential_command[0]),
        *config.credential_command[1:],
    ]
    # Administrator-only config, absolute executable, and shell-free argv.
    result = subprocess.run(  # nosec B603
        command, input=metadata, env=_base_child_env(), text=True,
        stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, timeout=30,
    )
    if result.returncode:
        raise NativeContinuationError("credential helper failed")
    try:
        values = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise NativeContinuationError("credential helper returned invalid JSON") from exc
    if not isinstance(values, dict) or not all(isinstance(k, str) and isinstance(v, str) for k, v in values.items()):
        raise NativeContinuationError("credential helper must return a JSON string map")
    permitted = set(config.credential_env_allowlist)
    if not values.keys() <= permitted:
        raise NativeContinuationError("credential helper returned a non-allowlisted key")
    if any(_credential_key_is_forbidden(key) for key in values):
        raise NativeContinuationError("credential helper returned a forbidden key")
    if any("\x00" in value or len(value) > 16_384 for value in values.values()):
        raise NativeContinuationError("credential helper returned an invalid value")
    if sum(len(key) + len(value) for key, value in values.items()) > 65_536:
        raise NativeContinuationError("credential helper returned too much data")
    return values


def working_directory_identity(cwd: str) -> dict[str, str]:
    try:
        resolved = Path(cwd).expanduser().resolve(strict=True)
        metadata = resolved.stat()
    except (FileNotFoundError, PermissionError, OSError) as exc:
        raise NativeContinuationError(
            "working directory is unavailable",
            code="cwd_identity_changed",
        ) from exc
    if not stat.S_ISDIR(metadata.st_mode):
        raise NativeContinuationError(
            "working directory is not a directory",
            code="cwd_identity_changed",
        )
    return {
        "cwd": str(Path(cwd).expanduser().absolute()),
        "cwd_realpath": str(resolved),
        "cwd_device": str(metadata.st_dev),
        "cwd_inode": str(metadata.st_ino),
        "cwd_owner_uid": str(metadata.st_uid),
    }


def _verified_working_directory(binding: SourceBinding) -> tuple[int, str]:
    required = (
        binding.cwd_realpath,
        binding.cwd_device,
        binding.cwd_inode,
        binding.cwd_owner_uid,
    )
    if not all(required):
        raise NativeContinuationError(
            "working directory identity is missing from this binding",
            code="binding_rebind_required",
        )
    expected = working_directory_identity(binding.cwd)
    if (
        expected["cwd_realpath"] != binding.cwd_realpath
        or expected["cwd_device"] != binding.cwd_device
        or expected["cwd_inode"] != binding.cwd_inode
        or expected["cwd_owner_uid"] != binding.cwd_owner_uid
    ):
        raise NativeContinuationError(
            "captured working directory was replaced or changed identity",
            code="cwd_identity_changed",
        )
    flags = os.O_RDONLY | os.O_CLOEXEC
    if hasattr(os, "O_DIRECTORY"):
        flags |= os.O_DIRECTORY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(binding.cwd_realpath, flags)
    except OSError as exc:
        raise NativeContinuationError(
            "captured working directory cannot be opened safely",
            code="cwd_identity_changed",
        ) from exc
    try:
        observed = os.fstat(descriptor)
        if (
            not stat.S_ISDIR(observed.st_mode)
            or observed.st_dev != int(binding.cwd_device)
            or observed.st_ino != int(binding.cwd_inode)
            or observed.st_uid != int(binding.cwd_owner_uid)
        ):
            raise NativeContinuationError(
                "captured working directory changed during verification",
                code="cwd_identity_changed",
            )
        descriptor_path = next(
            (
                candidate
                for candidate in (
                    f"/proc/self/fd/{descriptor}",
                    f"/dev/fd/{descriptor}",
                )
                if Path(candidate).is_dir()
            ),
            "",
        )
        if not descriptor_path:
            raise NativeContinuationError(
                "this host cannot pin a working directory descriptor",
                code="cwd_identity_changed",
            )
        return descriptor, descriptor_path
    except BaseException:
        os.close(descriptor)
        raise


def _wait_for_exit(process: subprocess.Popen[Any], timeout: float) -> bool:
    try:
        process.wait(timeout=timeout)
        return True
    except subprocess.TimeoutExpired:
        return False


def _stop_process_group(process: subprocess.Popen[Any]) -> None:
    with contextlib.suppress(ProcessLookupError):
        os.killpg(process.pid, signal.SIGTERM)
    if _wait_for_exit(process, 5):
        return
    with contextlib.suppress(ProcessLookupError):
        os.killpg(process.pid, signal.SIGKILL)
    if not _wait_for_exit(process, 5):
        raise NativeContinuationError("agent continuation process could not be stopped")


class _BoundedCapture:
    __slots__ = ("data", "keep_tail", "limit", "truncated")

    def __init__(self, limit: int, *, keep_tail: bool = False) -> None:
        self.data = bytearray()
        self.keep_tail = keep_tail
        self.limit = limit
        self.truncated = False

    def append(self, chunk: bytes) -> None:
        if len(self.data) + len(chunk) <= self.limit:
            self.data.extend(chunk)
            return
        self.truncated = True
        if self.keep_tail:
            self.data.extend(chunk)
            del self.data[: max(0, len(self.data) - self.limit)]
            return
        remaining = self.limit - len(self.data)
        if remaining > 0:
            self.data.extend(chunk[:remaining])


def _close_selector_stream(
    selector: selectors.BaseSelector,
    stream: Any,
) -> None:
    with contextlib.suppress(Exception):
        selector.unregister(stream)
    with contextlib.suppress(OSError):
        stream.close()


def _collect_native_output(
    process: subprocess.Popen[bytes],
    prompt: str,
    deadline: float,
    cancel_event: threading.Event | None,
) -> tuple[bytes, bytes, bool]:
    streams = (process.stdin, process.stdout, process.stderr)
    if any(stream is None for stream in streams):
        _stop_process_group(process)
        raise NativeContinuationError(
            "agent continuation pipes were not created"
        )
    stdin, stdout, stderr = streams
    payload = prompt.encode("utf-8")
    payload_offset = 0
    stdout_capture = _BoundedCapture(MAX_NATIVE_STDOUT_BYTES)
    stderr_capture = _BoundedCapture(
        MAX_NATIVE_STDERR_BYTES,
        keep_tail=True,
    )
    selector = selectors.DefaultSelector()
    output_streams = {stdout, stderr}
    stdin_open = bool(payload)
    exited_at: float | None = None
    sent_group_term = False
    sent_group_kill = False
    try:
        for stream in streams:
            os.set_blocking(stream.fileno(), False)
        selector.register(stdout, selectors.EVENT_READ, stdout_capture)
        selector.register(stderr, selectors.EVENT_READ, stderr_capture)
        if stdin_open:
            selector.register(stdin, selectors.EVENT_WRITE, None)
        else:
            stdin.close()
        while process.poll() is None or output_streams or stdin_open:
            now = time.monotonic()
            if cancel_event is not None and cancel_event.is_set():
                _stop_process_group(process)
                raise NativeContinuationError(
                    "agent continuation cancelled by the operator"
                )
            remaining = deadline - now
            if remaining <= 0:
                _stop_process_group(process)
                raise NativeContinuationError("agent continuation timed out")
            if process.poll() is not None:
                if exited_at is None:
                    exited_at = now
                if stdin_open:
                    _close_selector_stream(selector, stdin)
                    stdin_open = False
                elapsed = now - exited_at
                if elapsed >= 1.0 and not sent_group_term:
                    with contextlib.suppress(ProcessLookupError):
                        os.killpg(process.pid, signal.SIGTERM)
                    sent_group_term = True
                if elapsed >= 2.0 and not sent_group_kill:
                    with contextlib.suppress(ProcessLookupError):
                        os.killpg(process.pid, signal.SIGKILL)
                    sent_group_kill = True
                if elapsed >= 3.0:
                    for stream in tuple(output_streams):
                        _close_selector_stream(selector, stream)
                        output_streams.discard(stream)
                    break
            for key, _mask in selector.select(min(0.1, remaining)):
                stream = key.fileobj
                if stream is stdin:
                    try:
                        written = os.write(
                            stdin.fileno(),
                            payload[
                                payload_offset:
                                payload_offset + NATIVE_STREAM_CHUNK_BYTES
                            ],
                        )
                    except (BrokenPipeError, OSError):
                        written = 0
                        payload_offset = len(payload)
                    else:
                        payload_offset += written
                    if payload_offset >= len(payload):
                        _close_selector_stream(selector, stdin)
                        stdin_open = False
                    continue
                capture = key.data
                try:
                    chunk = os.read(
                        stream.fileno(),
                        NATIVE_STREAM_CHUNK_BYTES,
                    )
                except BlockingIOError:
                    continue
                except OSError:
                    chunk = b""
                if chunk:
                    capture.append(chunk)
                    continue
                _close_selector_stream(selector, stream)
                output_streams.discard(stream)
    except BaseException:
        if process.poll() is None:
            _stop_process_group(process)
        raise
    finally:
        selector.close()
        for stream in streams:
            with contextlib.suppress(OSError):
                stream.close()
    return (
        bytes(stdout_capture.data),
        bytes(stderr_capture.data),
        stdout_capture.truncated,
    )


def continue_native(
    bridge: Bridge,
    prompt: str,
    cancel_event: threading.Event | None = None,
    persist_response: Callable[[str], None] | None = None,
) -> str:
    config = load_config()
    binding = require_deliverable_binding(bridge, "detached_native")
    cwd = binding.cwd
    session_id = binding.session_id
    if bridge.source_kind == "codex_session":
        command = [_resolve_executable(config.codex_binary), "exec", "resume", *config.codex_resume_args, session_id, "-"]
    elif bridge.source_kind == "claude_session":
        command = [
            _resolve_executable(config.claude_binary), "--print", "--resume", session_id,
            "--output-format", "text", *config.claude_resume_args,
        ]
    else:
        raise ValueError("source is not a native coding session")
    if not session_id or not Path(cwd).is_dir():
        raise NativeContinuationError("captured session or working directory is no longer available")
    env = _base_child_env()
    env.update(_credential_env(bridge, config))
    cwd_descriptor, child_cwd = _verified_working_directory(binding)
    # Fixed agent CLI plus administrator-only resume flags; prompts remain on stdin.
    try:
        process = subprocess.Popen(  # nosec B603
            command, cwd=child_cwd, env=env, text=False, stdin=subprocess.PIPE,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            start_new_session=True, pass_fds=(cwd_descriptor,),
        )
    finally:
        os.close(cwd_descriptor)
    stdout_bytes, stderr_bytes, stdout_truncated = _collect_native_output(
        process,
        prompt,
        time.monotonic() + config.native_timeout_seconds,
        cancel_event,
    )
    stdout = stdout_bytes.decode("utf-8", errors="replace")
    stderr = stderr_bytes.decode("utf-8", errors="replace")
    if process.returncode:
        lowered = stderr.lower()
        if "401" in lowered or "authentication" in lowered or "api key" in lowered:
            raise NativeContinuationError("model authentication failed")
        if "session" in lowered and ("not found" in lowered or "invalid" in lowered):
            raise NativeContinuationError("captured agent session is no longer resumable")
        raise NativeContinuationError(f"agent continuation exited with status {process.returncode}")
    output = stdout.strip()
    if not output:
        raise NativeContinuationError("agent continuation returned no response")
    if stdout_truncated or len(output) > MAX_NATIVE_OUTPUT:
        output = output[:MAX_NATIVE_OUTPUT] + "\n\n_[Output truncated by Tether.]_"
    if persist_response is not None:
        persist_response(output)
    return output


def _validate_herdr_socket(socket_path: str) -> Path:
    candidate = Path(socket_path)
    if not candidate.is_absolute() or "\x00" in socket_path:
        raise _binding_error(
            "process_identity_unavailable",
            "Herdr socket path is invalid",
        )
    try:
        metadata = candidate.lstat()
    except (FileNotFoundError, PermissionError, OSError) as exc:
        raise _binding_error(
            "process_identity_unavailable",
            "Herdr socket is unavailable",
        ) from exc
    if (
        not stat.S_ISSOCK(metadata.st_mode)
        or metadata.st_uid != os.getuid()
        or stat.S_IMODE(metadata.st_mode) != 0o600
    ):
        raise _binding_error(
            "process_identity_unavailable",
            "Herdr socket is not a private socket owned by this user",
        )
    return candidate


def _herdr_call(
    socket_path: str,
    method: str,
    params: dict[str, Any],
    *,
    mutation: bool = False,
    timeout: float = 10.0,
) -> dict[str, Any]:
    candidate = _validate_herdr_socket(socket_path)
    request_id = "tether_" + uuid.uuid4().hex
    payload = json.dumps(
        {"id": request_id, "method": method, "params": params},
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8") + b"\n"
    if len(payload) > MAX_BROKER_REQUEST_BYTES:
        raise ValueError("Herdr request is too large")
    sent = False
    client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    client.settimeout(timeout)
    try:
        client.connect(str(candidate))
        if hasattr(socket, "SO_PEERCRED"):
            raw_peer = client.getsockopt(
                socket.SOL_SOCKET,
                socket.SO_PEERCRED,
                struct.calcsize("3i"),
            )
            _peer_pid, peer_uid, _peer_gid = struct.unpack("3i", raw_peer)
            if peer_uid != os.getuid():
                raise PermissionError("Herdr peer owner does not match this user")
        client.sendall(payload)
        sent = True
        response = bytearray()
        while b"\n" not in response:
            chunk = client.recv(min(65_536, MAX_BROKER_RESPONSE_BYTES + 1 - len(response)))
            if not chunk:
                raise ConnectionError("Herdr closed the socket before responding")
            response.extend(chunk)
            if len(response) > MAX_BROKER_RESPONSE_BYTES:
                raise ValueError("Herdr response is too large")
        frame, trailing = bytes(response).split(b"\n", 1)
        if trailing.strip():
            raise ValueError("Herdr returned unexpected response data")
        decoded = json.loads(frame)
    except NativeContinuationError:
        raise
    except Exception as exc:
        code = (
            "terminal_submit_uncertain"
            if mutation and sent
            else "terminal_submit_not_started"
            if mutation
            else "native_continuation_failed"
        )
        raise NativeContinuationError(
            "Herdr request could not be completed safely",
            code=code,
        ) from exc
    finally:
        client.close()
    if not isinstance(decoded, dict) or decoded.get("id") != request_id:
        raise NativeContinuationError(
            "Herdr returned an invalid response",
            code=("terminal_submit_uncertain" if mutation else "native_continuation_failed"),
        )
    error = decoded.get("error")
    if error is not None:
        error_code = str(error.get("code") or "") if isinstance(error, dict) else ""
        mutation_code = (
            "terminal_submit_not_started"
            if error_code == "agent_not_found"
            else "terminal_submit_uncertain"
        )
        raise NativeContinuationError(
            "Herdr rejected the exact agent operation",
            code=(mutation_code if mutation else "native_continuation_failed"),
        )
    result = decoded.get("result")
    if not isinstance(result, dict):
        raise NativeContinuationError(
            "Herdr response omitted its result",
            code=("terminal_submit_uncertain" if mutation else "native_continuation_failed"),
        )
    return result


def _herdr_result_record(
    result: dict[str, Any],
    *,
    result_type: str,
    field: str,
) -> dict[str, Any]:
    record = result.get(field)
    if result.get("type") != result_type or not isinstance(record, dict):
        raise NativeContinuationError("Herdr returned an unexpected response shape")
    return record


def _herdr_process_identity(
    process_info: dict[str, Any],
    *,
    terminal_id: str,
    expected_agent: str,
    config: Config | None = None,
    proc_root: Path = Path("/proc"),
) -> str:
    configured = config or load_config()
    trusted_paths = _trusted_agent_paths(configured, {expected_agent})
    if not trusted_paths.get(expected_agent):
        raise _binding_error(
            "process_identity_unavailable",
            f"the configured {expected_agent} executable is not trusted",
        )
    processes = process_info.get("foreground_processes")
    foreground_group = process_info.get("foreground_process_group_id")
    if not isinstance(processes, list) or not processes:
        raise _binding_error(
            "process_identity_missing",
            "Herdr terminal has no foreground agent process",
        )
    candidates: list[str] = []
    boot_id = _boot_id(proc_root)
    for record in processes:
        if not isinstance(record, dict) or isinstance(record.get("pid"), bool):
            continue
        try:
            pid = int(record["pid"])
            process_dir = proc_root / str(pid)
            stat_text = (process_dir / "stat").read_text(encoding="utf-8")
            fields = stat_text[stat_text.rfind(")") + 2 :].split()
            if len(fields) <= 19:
                continue
            process_group = int(fields[2])
            tty_number = int(fields[4])
            terminal_group = int(fields[5])
            if (
                tty_number <= 0
                or process_group != terminal_group
                or (
                    isinstance(foreground_group, int)
                    and foreground_group > 0
                    and process_group != foreground_group
                )
            ):
                continue
            raw_command = (process_dir / "cmdline").read_bytes()
            tokens = [
                value.decode("utf-8", "replace")
                for value in raw_command.split(b"\0")
                if value
            ]
            executable_link = process_dir / "exe"
            executable_path = str(executable_link.resolve(strict=True))
            executable_stat = executable_link.stat()
            agent, _quality = _process_agent(
                executable_path,
                tokens,
                {expected_agent},
                trusted_paths,
            )
            if agent != expected_agent:
                continue
            descriptor = {
                "agent": agent,
                "boot": boot_id,
                "exe": f"{executable_stat.st_dev:x}:{executable_stat.st_ino:x}",
                "exe_path": hashlib.sha256(
                    executable_path.encode("utf-8", "replace")
                ).hexdigest()[:16],
                "pid": pid,
                "start": fields[19],
                "terminal": terminal_id,
                "tty": str(tty_number),
            }
            candidates.append(
                HERDR_PROCESS_IDENTITY_PREFIX
                + json.dumps(descriptor, sort_keys=True, separators=(",", ":"))
            )
        except (
            FileNotFoundError,
            PermissionError,
            ProcessLookupError,
            ValueError,
            IndexError,
            KeyError,
            OSError,
        ):
            continue
    if not candidates:
        raise _binding_error(
            "process_identity_missing",
            "Herdr terminal has no trusted foreground agent process",
        )
    if len(candidates) != 1:
        raise _binding_error(
            "process_identity_ambiguous",
            "Herdr terminal has multiple trusted foreground agent processes",
        )
    return candidates[0]


def _herdr_agent_name(socket_path: str, terminal_id: str) -> str:
    digest = hashlib.sha256(
        (socket_path + "\0" + terminal_id).encode("utf-8")
    ).hexdigest()[:16]
    return "tether_" + digest


def herdr_agent_identity(
    socket_path: str,
    pane_id: str,
    session: str,
    cwd: str = "",
    config: Config | None = None,
    *,
    assign_name: bool = True,
) -> dict[str, str]:
    ping = _herdr_call(socket_path, "ping", {})
    if (
        ping.get("type") != "pong"
        or ping.get("protocol") != HERDR_PROTOCOL_VERSION
    ):
        raise _binding_error(
            "process_identity_unavailable",
            "Herdr protocol is unsupported by this Tether runtime",
        )
    pane = _herdr_result_record(
        _herdr_call(socket_path, "pane.get", {"pane_id": pane_id}),
        result_type="pane_info",
        field="pane",
    )
    agent = _herdr_result_record(
        _herdr_call(socket_path, "agent.get", {"target": pane_id}),
        result_type="agent_info",
        field="agent",
    )
    terminal_id = str(agent.get("terminal_id") or "")
    current_pane = str(agent.get("pane_id") or "")
    pane_agent = str(agent.get("agent") or "")
    if (
        terminal_id != str(pane.get("terminal_id") or "")
        or current_pane != str(pane.get("pane_id") or "")
        or pane_agent not in {"codex", "claude"}
        or agent.get("launch_pending") is True
    ):
        raise _binding_error(
            "adapter_pane_mismatch",
            "Herdr pane does not contain one supported live native agent",
        )
    native_session = agent.get("agent_session")
    if not isinstance(native_session, dict):
        raise _binding_error(
            "binding_rebind_required",
            "Herdr has no official native session reference; install its agent integration",
        )
    native_source = str(native_session.get("source") or "")
    native_agent = str(native_session.get("agent") or "")
    native_kind = str(native_session.get("kind") or "")
    native_value = str(native_session.get("value") or "")
    if native_agent != pane_agent or not all(
        (native_source, native_kind, native_value)
    ):
        raise _binding_error(
            "adapter_pane_mismatch",
            "Herdr native session reference does not match the live agent",
        )
    agent_name = str(agent.get("name") or "")
    if not agent_name and assign_name:
        agent_name = _herdr_agent_name(socket_path, terminal_id)
        agent = _herdr_result_record(
            _herdr_call(
                socket_path,
                "agent.rename",
                {"target": current_pane, "name": agent_name},
            ),
            result_type="agent_info",
            field="agent",
        )
    expected_session = {
        "source": native_source,
        "agent": pane_agent,
        "kind": native_kind,
        "value": native_value,
    }
    if (
        str(agent.get("name") or "") != agent_name
        or str(agent.get("terminal_id") or "") != terminal_id
        or str(agent.get("pane_id") or "") != current_pane
        or str(agent.get("agent") or "") != pane_agent
        or agent.get("agent_session") != expected_session
        or agent.get("launch_pending") is True
        or (assign_name and not HERDR_AGENT_NAME_PATTERN.fullmatch(agent_name))
    ):
        raise _binding_error(
            "process_identity_unavailable",
            "Herdr could not preserve the exact occupant-bound agent identity",
        )
    process_info = _herdr_result_record(
        _herdr_call(
            socket_path,
            "pane.process_info",
            {"pane_id": current_pane},
        ),
        result_type="pane_process_info",
        field="process_info",
    )
    process_identity = _herdr_process_identity(
        process_info,
        terminal_id=terminal_id,
        expected_agent=pane_agent,
        config=config,
    )
    verified_agent = _herdr_result_record(
        _herdr_call(
            socket_path,
            "agent.get",
            {"target": agent_name or current_pane},
        ),
        result_type="agent_info",
        field="agent",
    )
    if (
        str(verified_agent.get("name") or "") != agent_name
        or str(verified_agent.get("terminal_id") or "") != terminal_id
        or str(verified_agent.get("pane_id") or "") != current_pane
        or str(verified_agent.get("agent") or "") != pane_agent
        or verified_agent.get("agent_session") != expected_session
        or verified_agent.get("launch_pending") is True
    ):
        raise _binding_error(
            "process_identity_changed",
            "Herdr agent changed while its exact binding was captured",
        )
    return {
        "cwd": cwd,
        "pane_agent": pane_agent,
        "process_identity": process_identity,
        "herdr_session": session,
        "herdr_socket_path": str(_validate_herdr_socket(socket_path)),
        "herdr_terminal_id": terminal_id,
        "herdr_pane_id": current_pane,
        "herdr_agent_name": agent_name,
        "herdr_agent_session_source": native_source,
        "herdr_agent_session_kind": native_kind,
        "herdr_agent_session_value": native_value,
        "herdr_protocol": str(HERDR_PROTOCOL_VERSION),
        "native_session_id": native_value,
    }


def _same_herdr_process_identity(left: str, right: str) -> bool:
    """Compare one process incarnation while allowing Herdr handoff ID rotation."""
    try:
        left_identity = _parse_herdr_process_identity(left)
        right_identity = _parse_herdr_process_identity(right)
    except ValueError:
        return False
    return all(
        left_identity[field] == right_identity[field]
        for field in HERDR_PROCESS_IDENTITY_FIELDS - {"terminal"}
    )


def _current_herdr_agent(binding: SourceBinding) -> tuple[dict[str, Any], str]:
    ping = _herdr_call(binding.herdr_socket_path, "ping", {})
    if (
        ping.get("type") != "pong"
        or ping.get("protocol") != HERDR_PROTOCOL_VERSION
    ):
        raise _binding_error(
            "process_identity_changed",
            "Herdr protocol changed after this binding was captured",
        )
    agent = _herdr_result_record(
        _herdr_call(
            binding.herdr_socket_path,
            "agent.get",
            {"target": binding.herdr_agent_name},
        ),
        result_type="agent_info",
        field="agent",
    )
    native_session = agent.get("agent_session")
    expected_session = {
        "source": binding.herdr_agent_session_source,
        "agent": binding.pane_agent,
        "kind": binding.herdr_agent_session_kind,
        "value": binding.herdr_agent_session_value,
    }
    current_terminal = str(agent.get("terminal_id") or "")
    if (
        str(agent.get("name") or "") != binding.herdr_agent_name
        or not HERDR_TERMINAL_ID_PATTERN.fullmatch(current_terminal)
        or str(agent.get("agent") or "") != binding.pane_agent
        or native_session != expected_session
        or agent.get("launch_pending") is True
    ):
        raise _binding_error(
            "process_identity_changed",
            "captured Herdr terminal now hosts a different agent session",
        )
    current_pane = str(agent.get("pane_id") or "")
    process_info = _herdr_result_record(
        _herdr_call(
            binding.herdr_socket_path,
            "pane.process_info",
            {"pane_id": current_pane},
        ),
        result_type="pane_process_info",
        field="process_info",
    )
    process_identity = _herdr_process_identity(
        process_info,
        terminal_id=current_terminal,
        expected_agent=binding.pane_agent,
    )
    if not _same_herdr_process_identity(process_identity, binding.process_identity):
        raise _binding_error(
            "process_identity_changed",
            "captured Herdr terminal now hosts a different process incarnation",
        )
    return agent, current_pane


def _live_attempt_instruction(
    bridge: Bridge,
    text: str,
    attempt_id: str | None,
) -> tuple[str, str]:
    notifier = RUNTIME_HOME / "tether_notify.py"
    marker = attempt_id or ("att_" + uuid.uuid4().hex[:24])
    if not REPLY_KEY_PATTERN.fullmatch(marker):
        raise ValueError("invalid live delivery attempt ID")
    inbox_dir = RUNTIME_HOME / "inbox"
    RUNTIME_HOME.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    security.secure_state_directory(RUNTIME_HOME, create=True)
    security.secure_state_directory(inbox_dir, create=True)
    now = time.time()
    for pattern in ("att_*.txt", "tether-*.txt"):
        for stale in inbox_dir.glob(pattern):
            with contextlib.suppress(OSError):
                if not stale.is_symlink() and now - stale.stat().st_mtime > 86_400:
                    stale.unlink()
    inbox_path = inbox_dir / f"{marker}.txt"
    payload = text + "\n"
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(inbox_path, flags, 0o600)
    except FileExistsError:
        if security.read_private_text(inbox_path) != payload:
            raise NativeContinuationError(
                "live delivery inbox already contains different content",
                code="terminal_submit_uncertain",
            )
    else:
        with os.fdopen(descriptor, "w", encoding="utf-8") as inbox:
            inbox.write(payload)
    reply_command = (
        f"python3 {shlex.quote(str(notifier))} reply "
        f"--bridge-id {bridge.bridge_id} --reply-key {marker} --text-stdin"
    )
    instruction = (
        f"[Hermes Slack follow-up batch; {marker}] Read and handle the complete request in "
        f"{shlex.quote(str(inbox_path))}, then delete that file. "
        "Handle it as one turn. Check the current thread/task state before responding. "
        "Post at most one Slack message for this entire batch, only when a useful response is needed; "
        "do not post a second status summary or courtesy acknowledgment. Default to 50 words and "
        "3 sentences; exceed that only when necessary for a complete or safe answer. Never suppress "
        "a useful answer solely to meet the length target. If no new response is needed, submit "
        f"NO_REPLY. Provide the response on standard input, never in argv, to: {reply_command}"
    )
    return marker, instruction


def interrupt_herdr(bridge: Bridge) -> None:
    binding = require_deliverable_binding(bridge, "herdr_agent")
    _current_herdr_agent(binding)
    _herdr_call(
        binding.herdr_socket_path,
        "agent.send_keys",
        {"target": binding.herdr_agent_name, "keys": ["ctrl+c"]},
        mutation=True,
    )
    _current_herdr_agent(binding)


def deliver_herdr(
    bridge: Bridge,
    text: str,
    attempt_id: str | None = None,
) -> str:
    binding = require_deliverable_binding(bridge, "herdr_agent")
    current_agent, _current_pane = _current_herdr_agent(binding)
    current_terminal = str(current_agent.get("terminal_id") or "")
    marker, instruction = _live_attempt_instruction(bridge, text, attempt_id)
    result = _herdr_call(
        binding.herdr_socket_path,
        "agent.prompt",
        {"target": binding.herdr_agent_name, "text": instruction},
        mutation=True,
    )
    agent = _herdr_result_record(
        result,
        result_type="agent_prompted",
        field="agent",
    )
    if (
        str(agent.get("name") or "") != binding.herdr_agent_name
        or str(agent.get("terminal_id") or "") != current_terminal
        or str(agent.get("agent") or "") != binding.pane_agent
    ):
        raise NativeContinuationError(
            "Herdr accepted the prompt but returned a different agent",
            code="terminal_submit_uncertain",
            binding_id=bridge.bridge_id,
        )
    try:
        _current_herdr_agent(binding)
    except Exception as exc:
        raise NativeContinuationError(
            "Herdr accepted the prompt but the exact agent could not be reverified",
            code="terminal_submit_uncertain",
            binding_id=bridge.bridge_id,
        ) from exc
    return marker


def _pane_number(pane: str) -> int:
    normalized = pane.removeprefix("terminal_")
    if not normalized.isdigit():
        raise NativeContinuationError("captured Zellij pane is not a terminal pane")
    return int(normalized)


def _agent_name(value: str, allowed: set[str]) -> str:
    name = Path(value).name
    stem = Path(name).stem
    return name if name in allowed else stem if stem in allowed else ""


def _process_agent(
    executable: str,
    tokens: list[str],
    allowed: set[str],
    trusted_paths: dict[str, set[str]],
) -> tuple[str, int]:
    try:
        executable_path = str(Path(executable).resolve(strict=True))
    except (FileNotFoundError, PermissionError, OSError):
        return "", 0
    for agent, paths in trusted_paths.items():
        if agent in allowed and executable_path in paths:
            return agent, 3
    runtime = Path(executable_path).name
    if runtime in {
        "node", "nodejs", "python", "python3", "python3.11", "python3.12",
        "ruby", "deno", "bun",
    } and len(tokens) > 1:
        script_path = Path(tokens[1])
        if script_path.is_absolute():
            try:
                resolved_script = script_path.resolve(strict=True)
            except (FileNotFoundError, PermissionError, OSError):
                resolved_script = None
            if resolved_script is not None:
                for agent, paths in trusted_paths.items():
                    if agent in allowed and str(resolved_script) in paths:
                        return agent, 2
    return "", 0


def _command_agent(command: str, allowed: set[str]) -> str:
    try:
        tokens = shlex.split(command)
    except ValueError:
        return ""
    if not tokens:
        return ""
    for token in tokens[:2]:
        agent = _agent_name(token, allowed)
        if agent:
            return agent
    return ""


def _trusted_agent_paths(
    config: Config,
    allowed: set[str] | None = None,
) -> dict[str, set[str]]:
    configured = [
        (Path(command).stem, command)
        for command in config.zellij_agent_commands
    ]
    configured.extend(
        (
            ("codex", config.codex_binary),
            ("claude", config.claude_binary),
        )
    )
    allowed_agents = allowed or {agent for agent, _ in configured}
    trusted: dict[str, set[str]] = {
        agent: set() for agent in allowed_agents
    }
    for declared_agent, command in configured:
        if declared_agent not in allowed_agents:
            continue
        try:
            resolved = _resolve_executable(command)
            canonical = str(Path(resolved).resolve(strict=True))
        except (NativeContinuationError, FileNotFoundError, PermissionError, OSError):
            continue
        trusted[declared_agent].add(canonical)
    return {agent: paths for agent, paths in trusted.items() if paths}


def _boot_id(proc_root: Path) -> str:
    candidates = (
        proc_root / "sys" / "kernel" / "random" / "boot_id",
        Path("/proc/sys/kernel/random/boot_id"),
    )
    for path in candidates:
        try:
            value = path.read_text(encoding="utf-8").strip()
        except (FileNotFoundError, PermissionError, OSError):
            continue
        if re.fullmatch(r"[0-9a-fA-F-]{16,64}", value):
            return value.lower()
    raise _binding_error(
        "process_identity_unavailable",
        "kernel boot identity is unavailable; exact pane binding cannot be proven",
    )


def _zellij_agent_process(
    session: str,
    pane: str,
    allowed: set[str],
    proc_root: Path = Path("/proc"),
    metadata_agent: str = "",
    trusted_paths: dict[str, set[str]] | None = None,
) -> tuple[str, str]:
    if not trusted_paths:
        raise _binding_error(
            "process_identity_untrusted",
            "trusted agent executable paths are required for native binding",
        )
    boot_id = _boot_id(proc_root)
    candidates: list[dict[str, Any]] = []
    parents: dict[int, int] = {}
    pane_processes: dict[int, dict[str, int]] = {}
    for process_dir in proc_root.iterdir():
        if not process_dir.name.isdigit():
            continue
        try:
            environment = {
                item.split(b"=", 1)[0]: item.split(b"=", 1)[1]
                for item in (process_dir / "environ").read_bytes().split(b"\0")
                if b"=" in item
            }
            if environment.get(b"ZELLIJ_SESSION_NAME", b"").decode(
                "utf-8", "replace"
            ) != session or environment.get(b"ZELLIJ_PANE_ID", b"").decode(
                "utf-8", "replace"
            ).removeprefix("terminal_") != pane.removeprefix("terminal_"):
                continue
            stat_text = (process_dir / "stat").read_text(encoding="utf-8")
            fields = stat_text[stat_text.rfind(")") + 2 :].split()
            if len(fields) <= 19:
                continue
            pid = int(process_dir.name)
            ppid = int(fields[1])
            parents[pid] = ppid
            tty_number = int(fields[4])
            process_group = int(fields[2])
            foreground_group = int(fields[5])
            pane_processes[pid] = {
                "ppid": ppid,
                "tty": tty_number,
                "process_group": process_group,
                "foreground_group": foreground_group,
            }
            raw_command = (process_dir / "cmdline").read_bytes()
            tokens = [
                value.decode("utf-8", "replace")
                for value in raw_command.split(b"\0")
                if value
            ]
            try:
                executable_link = process_dir / "exe"
                executable_path = str(executable_link.resolve(strict=True))
                executable_stat = executable_link.stat()
            except (FileNotFoundError, PermissionError, OSError):
                continue
            agent, quality = _process_agent(
                executable_path,
                tokens,
                allowed,
                trusted_paths,
            )
            if not agent:
                continue
            foreground = foreground_group > 0 and process_group == foreground_group
            start_time = fields[19] if len(fields) > 19 else ""
            executable_identity = (
                f"{executable_stat.st_dev:x}:{executable_stat.st_ino:x}"
            )
            descriptor_payload = {
                "agent": agent,
                "boot": boot_id,
                "exe": executable_identity,
                "exe_path": hashlib.sha256(
                    executable_path.encode("utf-8", "replace")
                ).hexdigest()[:16],
                "pane": pane.removeprefix("terminal_"),
                "pid": pid,
                "session": session,
                "start": start_time,
                "tty": str(tty_number),
            }
            descriptor = PROCESS_IDENTITY_PREFIX + json.dumps(
                descriptor_payload, sort_keys=True, separators=(",", ":")
            )
            candidates.append({
                "foreground": foreground,
                "pid": pid,
                "ppid": ppid,
                "agent": agent,
                "quality": quality,
                "descriptor": descriptor,
                "tty": tty_number,
            })
        except (FileNotFoundError, PermissionError, ProcessLookupError, ValueError, IndexError):
            continue

    pane_ttys = {
        int(record["tty"])
        for record in pane_processes.values()
        if int(record["tty"]) > 0
    }
    if not pane_ttys:
        raise _binding_error(
            "process_identity_missing",
            "Zellij pane has no terminal device to anchor native identity",
        )
    if len(pane_ttys) != 1:
        raise _binding_error(
            "process_identity_ambiguous",
            "Zellij pane identity does not resolve to one terminal device",
        )
    pane_tty = next(iter(pane_ttys))

    def depth(candidate: dict[str, Any]) -> int:
        result = 0
        current = int(candidate["ppid"])
        seen: set[int] = set()
        while current > 0 and current not in seen:
            seen.add(current)
            result += 1
            current = parents.get(current, 0)
        return result

    anchored: list[dict[str, Any]] = []
    for candidate in candidates:
        has_pane_ancestor = int(candidate["ppid"]) in pane_processes
        if (
            candidate["foreground"]
            and int(candidate["tty"]) == pane_tty
            and has_pane_ancestor
        ):
            anchored.append(candidate)
    candidates = anchored
    if not candidates:
        raise _binding_error(
            "process_identity_missing",
            "Zellij pane has no foreground TTY-anchored allowlisted agent process",
        )
    ranked = sorted(
        candidates,
        key=lambda item: (
            int(bool(item["foreground"])),
            int(item["quality"]),
            depth(item),
        ),
        reverse=True,
    )
    best_score = (
        int(bool(ranked[0]["foreground"])),
        int(ranked[0]["quality"]),
        depth(ranked[0]),
    )
    selected = [
        candidate for candidate in ranked
        if (
            int(bool(candidate["foreground"])),
            int(candidate["quality"]),
            depth(candidate),
        ) == best_score
    ]
    if len(selected) != 1:
        raise NativeContinuationError(
            "Zellij pane contains multiple equally plausible agent processes; rebind after selecting one",
            code="process_identity_ambiguous",
        )
    return str(selected[0]["agent"]), str(selected[0]["descriptor"])


def zellij_pane_identity(
    session: str,
    pane: str,
    cwd: str = "",
    config: Config | None = None,
) -> dict[str, str]:
    zellij = _resolve_executable("zellij")
    command = [
        zellij, "--session", session, "action", "list-panes",
        "--json", "--command", "--state", "--all",
    ]
    result = subprocess.run(  # nosec B603
        command,
        check=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=10,
    )
    try:
        panes = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise NativeContinuationError("Zellij returned invalid pane metadata") from exc
    pane_number = _pane_number(pane)
    record = next(
        (
            item for item in panes
            if isinstance(item, dict)
            and item.get("id") == pane_number
            and not item.get("is_plugin")
        ),
        None,
    )
    if record is None or "exited" not in record or record.get("exited"):
        raise NativeContinuationError("captured Zellij pane is no longer active")
    configured = config or load_config()
    trusted_paths = _trusted_agent_paths(configured)
    if not trusted_paths:
        raise _binding_error(
            "process_identity_unavailable",
            "no configured Zellij agent executable resolves to a trusted path",
        )
    allowed = {
        Path(command).stem
        for command in configured.zellij_agent_commands
    } | set(trusted_paths)
    metadata_agent = _command_agent(
        str(record.get("terminal_command") or ""), allowed
    )
    agent, process_identity = _zellij_agent_process(
        session,
        str(pane_number),
        allowed,
        metadata_agent=metadata_agent,
        trusted_paths=trusted_paths,
    )
    if metadata_agent and metadata_agent != agent:
        raise _binding_error(
            "adapter_pane_mismatch",
            f"Zellij metadata names {metadata_agent}, but the live process is {agent}",
        )
    return {
        "session_name": session,
        "pane_id": str(pane_number),
        "cwd": cwd,
        "pane_agent": agent,
        "process_identity": process_identity,
        "pane_command_hash": hashlib.sha256(
            process_identity.encode()
        ).hexdigest(),
    }


def interrupt_zellij(bridge: Bridge) -> None:
    binding = require_deliverable_binding(bridge, "zellij_pane")
    session = binding.zellij_session
    pane = binding.zellij_pane_id
    if not session or not pane:
        raise ValueError("captured Zellij endpoint is incomplete")
    current = zellij_pane_identity(
        session,
        pane,
        str(bridge.source.get("cwd") or ""),
    )
    if current["process_identity"] != binding.process_identity:
        raise _binding_error(
            "process_identity_changed",
            "captured Zellij pane now hosts a different process",
            binding_id=bridge.bridge_id,
        )
    target = pane if pane.startswith(("terminal_", "plugin_")) else "terminal_" + pane
    zellij = _resolve_executable("zellij")
    try:
        subprocess.run(  # nosec B603
            [
                zellij,
                "--session",
                session,
                "action",
                "send-keys",
                "--pane-id",
                target,
                "Ctrl c",
            ],
            check=True,
            timeout=10,
        )
    except Exception as exc:
        raise NativeContinuationError(
            "The terminal interrupt could not be delivered.",
            code="terminal_submit_not_started",
            binding_id=bridge.bridge_id,
        ) from exc


def deliver_zellij(
    bridge: Bridge,
    text: str,
    attempt_id: str | None = None,
) -> str:
    binding = require_deliverable_binding(bridge, "zellij_pane")
    session = binding.zellij_session
    pane = binding.zellij_pane_id
    if not session or not pane:
        raise ValueError("captured Zellij endpoint is incomplete")
    expected_identity = binding.process_identity
    current = zellij_pane_identity(
        session,
        pane,
        str(bridge.source.get("cwd") or ""),
    )
    if current["process_identity"] != expected_identity:
        raise _binding_error(
            "process_identity_changed",
            "captured Zellij pane now hosts a different process",
            binding_id=bridge.bridge_id,
        )
    notifier = RUNTIME_HOME / "tether_notify.py"
    marker = attempt_id or ("att_" + uuid.uuid4().hex[:24])
    if not REPLY_KEY_PATTERN.fullmatch(marker):
        raise ValueError("invalid Zellij delivery attempt ID")
    inbox_dir = RUNTIME_HOME / "inbox"
    RUNTIME_HOME.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    security.secure_state_directory(RUNTIME_HOME, create=True)
    security.secure_state_directory(inbox_dir, create=True)
    now = time.time()
    for pattern in ("att_*.txt", "tether-*.txt"):
        for stale in inbox_dir.glob(pattern):
            with contextlib.suppress(OSError):
                if not stale.is_symlink() and now - stale.stat().st_mtime > 86_400:
                    stale.unlink()
    inbox_path = inbox_dir / f"{marker}.txt"
    payload = text + "\n"
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(inbox_path, flags, 0o600)
    except FileExistsError:
        if security.read_private_text(inbox_path) != payload:
            raise NativeContinuationError(
                "Zellij delivery inbox already contains different content",
                code="terminal_submit_uncertain",
            )
    else:
        with os.fdopen(descriptor, "w", encoding="utf-8") as inbox:
            inbox.write(payload)
    reply_command = (
        f"python3 {shlex.quote(str(notifier))} reply "
        f"--bridge-id {bridge.bridge_id} --reply-key {marker} --text-stdin"
    )
    instruction = (
        f"[Hermes Slack follow-up batch; {marker}] Read and handle the complete request in "
        f"{shlex.quote(str(inbox_path))}, then delete that file. "
        "Handle it as one turn. Check the current thread/task state before responding. "
        "Post at most one Slack message for this entire batch, only when a useful response is needed; "
        "do not post a second status summary or courtesy acknowledgment. Default to 50 words and "
        "3 sentences; exceed that only when necessary for a complete or safe answer. Never suppress "
        "a useful answer solely to meet the length target. If no new response is needed, submit "
        f"NO_REPLY. Provide the response on standard input, never in argv, to: {reply_command}"
    )
    target = pane if pane.startswith(("terminal_", "plugin_")) else "terminal_" + pane
    zellij = _resolve_executable("zellij")
    write_accepted = False
    try:
        # Absolute executable; session and pane are argv, never shell text.
        # Claude Code collapses one large synthetic terminal write into an
        # opaque ``[Pasted text]`` placeholder. Verify the marker in the first
        # bounded chunk before the rest of a long input scrolls it off-screen.
        chunks = [
            instruction[offset : offset + ZELLIJ_WRITE_CHUNK_CHARS]
            for offset in range(0, len(instruction), ZELLIJ_WRITE_CHUNK_CHARS)
        ]
        subprocess.run(  # nosec B603
            [
                zellij,
                "--session",
                session,
                "action",
                "write-chars",
                "--pane-id",
                target,
                chunks[0],
            ],
            check=True,
            timeout=10,
        )
        write_accepted = True
        time.sleep(0.15)
        staged = subprocess.run(  # nosec B603
            [zellij, "--session", session, "action", "dump-screen", "--pane-id", target],
            check=True,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=10,
        )
        if marker not in staged.stdout:
            raise RuntimeError(
                "Slack instruction was not visible in the captured Zellij pane"
            )
        for chunk in chunks[1:]:
            subprocess.run(  # nosec B603
                [
                    zellij,
                    "--session",
                    session,
                    "action",
                    "write-chars",
                    "--pane-id",
                    target,
                    chunk,
                ],
                check=True,
                timeout=10,
            )
    except Exception as exc:
        if write_accepted:
            raise NativeContinuationError(
                "Terminal text was accepted, but submission did not reach Enter.",
                code="terminal_submit_uncertain",
                binding_id=bridge.bridge_id,
            ) from exc
        raise NativeContinuationError(
            "The terminal submission did not start.",
            code="terminal_submit_not_started",
            binding_id=bridge.bridge_id,
        ) from exc
    try:
        before_enter = zellij_pane_identity(
            session,
            pane,
            str(bridge.source.get("cwd") or ""),
        )
        if before_enter["process_identity"] != expected_identity:
            raise RuntimeError(
                "captured agent changed after terminal text was staged"
            )
        subprocess.run(  # nosec B603
            [zellij, "--session", session, "action", "send-keys", "--pane-id", target, "Enter"],
            check=True,
            timeout=10,
        )
    except Exception as exc:
        raise NativeContinuationError(
            "Enter may have been accepted, but terminal submission could not be confirmed.",
            code="terminal_submit_uncertain",
            binding_id=bridge.bridge_id,
        ) from exc
    try:
        time.sleep(0.5)
        submitted = subprocess.run(  # nosec B603
            [
                zellij,
                "--session",
                session,
                "action",
                "dump-screen",
                "--pane-id",
                target,
                "--full",
            ],
            check=True,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=10,
        )
        if marker not in submitted.stdout:
            raise RuntimeError(
                "Slack instruction disappeared before submission could be verified"
            )
        after_submit = zellij_pane_identity(
            session,
            pane,
            str(bridge.source.get("cwd") or ""),
        )
        if after_submit["process_identity"] != expected_identity:
            raise RuntimeError(
                "captured agent exited or changed process after Slack submission"
            )
    except Exception as exc:
        raise NativeContinuationError(
            "Enter was sent, but terminal submission could not be verified.",
            code="terminal_submit_uncertain",
            binding_id=bridge.bridge_id,
        ) from exc
    return marker


def _discover_herdr_binary() -> str:
    candidates = [
        os.getenv("HERDR_BIN_PATH", ""),
        shutil.which("herdr", path=_base_child_env().get("PATH")) or "",
    ]
    candidates.extend(
        str(path)
        for path in sorted(
            (Path.home() / ".local" / "opt" / "herdr").glob("*/herdr"),
            reverse=True,
        )
    )
    for value in candidates:
        if not value:
            continue
        candidate = Path(value).expanduser()
        try:
            if candidate.is_file() and os.access(candidate, os.X_OK):
                return str(candidate.resolve(strict=True))
        except (FileNotFoundError, PermissionError, OSError):
            continue
    return ""


def doctor() -> tuple[bool, list[str]]:
    def safe_error(exc: BaseException) -> str:
        detail = security.redact_egress_text(str(exc)).strip()
        return (detail or exc.__class__.__name__)[:160]

    checks: list[str] = []
    ok = True
    if CONFIG_PATH.exists() or CONFIG_PATH.is_symlink():
        try:
            load_config()
            checks.append("ok config is owner-only and symlink-safe")
            checks.append("ok optional Tether overrides are valid")
        except Exception as exc:
            ok = False
            checks.append(f"FAIL config: {safe_error(exc)}")
    else:
        ok = False
        checks.append(f"FAIL missing config at {CONFIG_PATH}")
    if SOCKET_PATH.is_socket():
        mode = stat.S_IMODE(SOCKET_PATH.stat().st_mode)
        if mode != 0o600:
            ok = False
            checks.append(f"FAIL broker socket permissions are {mode:04o}; expected 0600")
        else:
            checks.append("ok broker socket is live and private")
        try:
            status = broker_call({"op": "status"})
            if status.get("implementation") != "tether":
                ok = False
                checks.append("FAIL broker belongs to a legacy bridge; disable it and restart Hermes")
            else:
                checks.append("ok Tether broker protocol is active")
            if status.get("allowed_user_count", 0):
                checks.append(f"ok {status['allowed_user_count']} Hermes/Tether operator(s) authorized")
            else:
                ok = False
                checks.append("FAIL no explicit Hermes or Tether operator allowlist")
            if status.get("channel_configured"):
                checks.append("ok default Slack destination inherited or configured")
            else:
                checks.append("WARN no default Slack channel; pass --channel when notifying")
            if not status.get("owner_configured"):
                ok = False
                checks.append("FAIL no default bridge owner; configure a Hermes allowlist or Tether owner")
            transport = status.get("slack_transport_connected")
            poll_healthy = status.get("reply_poll_healthy")
            if transport is True:
                checks.append("ok Slack Socket Mode reply ingress connected")
            elif transport is False:
                ok = False
                checks.append("FAIL Slack Socket Mode reply ingress is disconnected")
            else:
                ok = False
                checks.append("FAIL Slack Socket Mode reply ingress has not connected yet")
            if poll_healthy is True:
                checks.append("ok best-effort Slack polling worker active")
            elif poll_healthy is False:
                checks.append("WARN best-effort Slack polling worker is not healthy")
            else:
                checks.append("WARN best-effort Slack polling health is not yet observed")
        except Exception as exc:
            ok = False
            checks.append(f"FAIL broker readiness check: {safe_error(exc)}")
    else:
        ok = False
        checks.append("FAIL broker socket unavailable; restart the Hermes gateway")
    plugin = HERMES_HOME / "plugins" / "tether" / "__init__.py"
    runtime = RUNTIME_HOME / "bridge_runtime.py"
    for label, path in (("plugin", plugin), ("runtime", runtime)):
        if path.is_file():
            checks.append(f"ok {label} installed")
        else:
            ok = False
            checks.append(f"FAIL {label} missing")
    herdr_binary = _discover_herdr_binary()
    if not herdr_binary:
        checks.append("WARN Herdr client unavailable; Herdr live bindings are disabled")
    else:
        try:
            result = subprocess.run(  # nosec B603
                [herdr_binary, "api", "schema", "--json"],
                check=True,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=10,
            )
            schema = json.loads(result.stdout)
            if schema.get("protocol") != HERDR_PROTOCOL_VERSION:
                ok = False
                checks.append(
                    f"FAIL Herdr protocol={schema.get('protocol', 'unknown')}; "
                    f"expected {HERDR_PROTOCOL_VERSION}"
                )
            else:
                checks.append(
                    f"ok Herdr client protocol={HERDR_PROTOCOL_VERSION} available"
                )
        except Exception as exc:
            ok = False
            checks.append(f"FAIL Herdr client check: {safe_error(exc)}")
    ambient_herdr_socket = os.getenv("HERDR_SOCKET_PATH", "")
    if ambient_herdr_socket:
        try:
            ping = _herdr_call(ambient_herdr_socket, "ping", {})
            if ping.get("protocol") != HERDR_PROTOCOL_VERSION:
                raise ValueError("ambient Herdr protocol mismatch")
            checks.append("ok current Herdr session socket is private and compatible")
        except Exception as exc:
            ok = False
            checks.append(f"FAIL current Herdr session: {safe_error(exc)}")
    try:
        hermes_checkout = HERMES_HOME / "hermes-agent"
        if hermes_checkout.is_dir() and str(hermes_checkout) not in sys.path:
            sys.path.insert(0, str(hermes_checkout))
        from plugins.platforms.slack.adapter import SlackAdapter
        version = hermes_compat.validate_adapter(SlackAdapter)
        checks.append(
            f"ok Hermes {version} Slack compatibility contract verified"
        )
    except Exception as exc:
        ok = False
        checks.append(
            f"FAIL Hermes Slack compatibility could not be verified: "
            f"{safe_error(exc)}"
        )
    return ok, checks
