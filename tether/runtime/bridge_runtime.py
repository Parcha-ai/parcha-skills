from __future__ import annotations

import contextlib
import datetime
import hashlib
import json
import http.client
import os
import re
import shlex
import shutil
import signal
import socket
import socketserver
import sqlite3
import stat
import subprocess
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


HERMES_HOME = Path(os.environ.get("HERMES_HOME", Path.home() / ".hermes")).expanduser()
DATA_HOME = Path(os.environ.get("XDG_DATA_HOME", Path.home() / ".local" / "share")).expanduser()
CONFIG_HOME = Path(os.environ.get("XDG_CONFIG_HOME", Path.home() / ".config")).expanduser()
RUNTIME_HOME = DATA_HOME / "tether"
CONFIG_PATH = Path(os.environ.get("TETHER_CONFIG", CONFIG_HOME / "tether" / "config.toml")).expanduser()
DB_PATH = HERMES_HOME / "bridges.db"
SOCKET_PATH = HERMES_HOME / "bridge.sock"
ID_PATTERN = re.compile(r"^[A-Z][A-Z0-9]{7,}$")
CHANNEL_ID_PATTERN = re.compile(r"^[CDG][A-Z0-9]{7,}$")
SECRET_PATTERNS = (
    (re.compile(r"\bxox[baprs]-[A-Za-z0-9-]{10,}\b"), "[REDACTED_SLACK_TOKEN]"),
    (re.compile(r"\bxap[p]-[A-Za-z0-9-]{10,}\b"), "[REDACTED_SLACK_APP_TOKEN]"),
    (re.compile(r"\b(?:gh[pousr]_[A-Za-z0-9]{20,}|github_pat_[A-Za-z0-9_]{20,})\b"), "[REDACTED_GITHUB_TOKEN]"),
    (re.compile(r"\bsk-[A-Za-z0-9_-]{20,}\b"), "[REDACTED_API_KEY]"),
    (re.compile(r"\bAKIA[0-9A-Z]{16}\b"), "[REDACTED_AWS_ACCESS_KEY]"),
    (re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----.*?-----END [A-Z ]*PRIVATE KEY-----", re.DOTALL), "[REDACTED_PRIVATE_KEY]"),
)
SAFE_CHILD_ENV = {
    "HOME", "USER", "LOGNAME", "PATH", "SHELL", "LANG", "LC_ALL", "LC_CTYPE",
    "TERM", "TMPDIR", "XDG_CONFIG_HOME", "XDG_CACHE_HOME", "XDG_DATA_HOME",
    "CODEX_HOME", "CLAUDE_HOME", "SSL_CERT_FILE", "SSL_CERT_DIR", "HTTP_PROXY",
    "HTTPS_PROXY", "NO_PROXY",
}
MAX_TEXT = 35_000
MAX_NATIVE_OUTPUT = 35_000
MAX_SOURCE_VALUE = 4_096
MAX_IDEMPOTENCY_KEY = 256
REPLY_KEY_PATTERN = re.compile(r"^[A-Za-z0-9_-]{8,128}$")
SOURCE_FIELDS = {
    "zellij_pane": frozenset({
        "session_name", "pane_id", "zellij_session", "zellij_pane_id", "cwd",
        "pane_command_hash", "pane_agent",
    }),
    "claude_session": frozenset({
        "session_id", "zellij_session", "zellij_pane_id", "cwd",
        "pane_command_hash", "pane_agent",
    }),
    "codex_session": frozenset({
        "session_id", "zellij_session", "zellij_pane_id", "cwd",
        "pane_command_hash", "pane_agent",
    }),
    "hermes_session": frozenset({"session_id", "run_id", "cwd"}),
    "headless_run": frozenset({"run_id", "queue_id", "cwd"}),
}
SLACK_METHOD_PATHS = {
    "auth.test": "/api/auth.test",
    "chat.postMessage": "/api/chat.postMessage",
    "conversations.history": "/api/conversations.history",
    "conversations.join": "/api/conversations.join",
    "conversations.open": "/api/conversations.open",
    "conversations.replies": "/api/conversations.replies",
    "users.list": "/api/users.list",
}
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
    query: str
    user_ids: list[str]


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
    codex_binary: str = "codex"
    claude_binary: str = "claude"
    codex_resume_args: tuple[str, ...] = ()
    claude_resume_args: tuple[str, ...] = ()
    credential_command: tuple[str, ...] = ()
    credential_env_allowlist: tuple[str, ...] = ()
    zellij_agent_commands: tuple[str, ...] = ("claude", "codex", "gemini", "hermes", "pi")


def load_config(path: Path = CONFIG_PATH) -> Config:
    if not path.is_file():
        return Config()
    raw = tomllib.loads(path.read_text(encoding="utf-8"))
    timeout = int(raw.get("native_timeout_seconds", 1800))
    if not 30 <= timeout <= 86_400:
        raise ValueError("native_timeout_seconds must be between 30 and 86400")
    max_reply_words = int(raw.get("max_reply_words", 50))
    max_reply_chars = int(raw.get("max_reply_chars", 500))
    max_reply_sentences = int(raw.get("max_reply_sentences", 3))
    if not 20 <= max_reply_words <= 500:
        raise ValueError("max_reply_words must be between 20 and 500")
    if not 100 <= max_reply_chars <= 4_000:
        raise ValueError("max_reply_chars must be between 100 and 4000")
    if not 1 <= max_reply_sentences <= 20:
        raise ValueError("max_reply_sentences must be between 1 and 20")
    command = raw.get("credential_command") or []
    codex_args = raw.get("codex_resume_args") or []
    claude_args = raw.get("claude_resume_args") or []
    allowlist = raw.get("credential_env_allowlist") or []
    zellij_commands = raw.get("zellij_agent_commands", ["claude", "codex", "gemini", "hermes", "pi"])
    users = raw.get("allowed_users") or []
    for name, values in (
        ("credential_command", command), ("codex_resume_args", codex_args),
        ("claude_resume_args", claude_args), ("credential_env_allowlist", allowlist),
        ("zellij_agent_commands", zellij_commands),
    ):
        if not isinstance(values, list) or not all(isinstance(value, str) and value for value in values):
            raise ValueError(f"{name} must be a string array")
    if not isinstance(users, list) or not all(isinstance(value, str) and ID_PATTERN.fullmatch(value) for value in users):
        raise ValueError("allowed_users contains an invalid Slack member ID")
    default_channel = str(raw.get("default_channel") or "")
    default_owner = str(raw.get("default_owner") or "")
    allow_channel_owner_restrictions = raw.get("allow_channel_owner_restrictions", False)
    team_id = str(raw.get("team_id") or "")
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
        codex_binary=str(raw.get("codex_binary") or "codex"),
        claude_binary=str(raw.get("claude_binary") or "claude"),
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


class NativeContinuationError(RuntimeError):
    pass


class Store:
    def __init__(self, path: Path = DB_PATH):
        self.path = path
        path.parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.executescript(
                """
                CREATE TABLE IF NOT EXISTS bridges (
                  bridge_id TEXT PRIMARY KEY, source_kind TEXT NOT NULL,
                  source_json TEXT NOT NULL, owner_user_id TEXT NOT NULL,
                  team_id TEXT NOT NULL DEFAULT '', channel_id TEXT NOT NULL,
                  thread_ts TEXT, idempotency_key TEXT NOT NULL UNIQUE,
                  status TEXT NOT NULL DEFAULT 'pending',
                  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                );
                CREATE UNIQUE INDEX IF NOT EXISTS bridge_thread
                  ON bridges(team_id, channel_id, thread_ts)
                  WHERE thread_ts IS NOT NULL AND status = 'active';
                CREATE TABLE IF NOT EXISTS bridge_events (
                  event_id TEXT PRIMARY KEY, bridge_id TEXT NOT NULL,
                  state TEXT NOT NULL, error TEXT,
                  payload_json TEXT NOT NULL DEFAULT '{}',
                  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                );
                CREATE TABLE IF NOT EXISTS bridge_ingress (
                  event_id TEXT PRIMARY KEY, bridge_id TEXT NOT NULL,
                  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                );
                CREATE TABLE IF NOT EXISTS bridge_replies (
                  reply_key TEXT PRIMARY KEY, bridge_id TEXT NOT NULL,
                  message_ts TEXT,
                  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                );
                CREATE TABLE IF NOT EXISTS thread_participation (
                  team_id TEXT NOT NULL DEFAULT '', channel_id TEXT NOT NULL,
                  thread_ts TEXT NOT NULL,
                  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  PRIMARY KEY (team_id, channel_id, thread_ts)
                );
                CREATE TABLE IF NOT EXISTS thread_ingress (
                  event_id TEXT PRIMARY KEY,
                  team_id TEXT NOT NULL DEFAULT '', channel_id TEXT NOT NULL,
                  thread_ts TEXT NOT NULL,
                  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                );
                """
            )
            columns = {row[1] for row in db.execute("PRAGMA table_info(bridge_events)")}
            if "payload_json" not in columns:
                db.execute("ALTER TABLE bridge_events ADD COLUMN payload_json TEXT NOT NULL DEFAULT '{}'")
            if "updated_at" not in columns:
                db.execute("ALTER TABLE bridge_events ADD COLUMN updated_at TEXT")
                db.execute("UPDATE bridge_events SET updated_at=created_at WHERE updated_at IS NULL")

    @contextlib.contextmanager
    def connect(self):
        db = sqlite3.connect(self.path, timeout=30)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA busy_timeout=30000")
        try:
            yield db
            db.commit()
        except BaseException:
            db.rollback()
            raise
        finally:
            db.close()

    @staticmethod
    def decode(row: sqlite3.Row | None) -> Bridge | None:
        if row is None:
            return None
        return Bridge(
            row["bridge_id"], row["source_kind"], json.loads(row["source_json"]),
            row["owner_user_id"], row["team_id"], row["channel_id"], row["thread_ts"],
            row["idempotency_key"], row["status"],
        )

    @staticmethod
    def validate_source(kind: str, raw_source: Any) -> dict[str, str]:
        if kind not in SOURCE_FIELDS:
            raise ValueError("unsupported bridge source kind")
        if not isinstance(raw_source, dict) or set(raw_source) - SOURCE_FIELDS[kind]:
            raise ValueError("invalid bridge source")
        if not all(isinstance(key, str) and isinstance(value, str) for key, value in raw_source.items()):
            raise ValueError("bridge source values must be strings")
        source = {key: value for key, value in raw_source.items() if value}
        if any(len(value) > MAX_SOURCE_VALUE for value in source.values()):
            raise ValueError("bridge source value is too large")
        return source

    def create(self, request: BridgeRequest) -> Bridge:
        required = ("source_kind", "source", "owner_user_id", "channel_id", "idempotency_key")
        if any(not request.get(key) for key in required):
            raise ValueError("source, owner, channel, and idempotency key are required")
        kind = str(request["source_kind"])
        source = self.validate_source(kind, request["source"])
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
                return self.decode(row)  # type: ignore[return-value]
            bridge_id = "brg_" + uuid.uuid4().hex
            db.execute(
                "INSERT INTO bridges(bridge_id,source_kind,source_json,owner_user_id,team_id,channel_id,thread_ts,idempotency_key,status) VALUES(?,?,?,?,?,?,?,?,?)",
                (bridge_id, kind, json.dumps(source, separators=(",", ":")), owner, team, channel,
                 request.get("thread_ts"), idempotency_key, "pending"),
            )
            row = db.execute("SELECT * FROM bridges WHERE bridge_id=?", (bridge_id,)).fetchone()
            return self.decode(row)  # type: ignore[return-value]

    def bind(self, bridge_id: str, thread_ts: str) -> Bridge:
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute("UPDATE bridges SET thread_ts=?,status='active' WHERE bridge_id=? AND status!='closed'", (thread_ts, bridge_id))
            return self.decode(db.execute("SELECT * FROM bridges WHERE bridge_id=?", (bridge_id,)).fetchone())  # type: ignore[return-value]

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

    def find_thread(self, channel_id: str, thread_ts: str) -> Bridge | None:
        with self.connect() as db:
            rows = db.execute(
                """
                SELECT * FROM bridges
                WHERE channel_id=? AND thread_ts=? AND status='active'
                ORDER BY created_at DESC LIMIT 2
                """,
                (channel_id, thread_ts),
            ).fetchall()
            if len(rows) > 1:
                raise ValueError("multiple active bridges match this Slack thread")
            return self.decode(rows[0] if rows else None)

    def rebind(self, bridge_id: str, source_kind: str, source: dict[str, str]) -> Bridge:
        validated = self.validate_source(source_kind, source)
        with self.connect() as db:
            cursor = db.execute(
                """
                UPDATE bridges SET source_kind=?,source_json=?
                WHERE bridge_id=? AND status='active'
                """,
                (
                    source_kind,
                    json.dumps(validated, ensure_ascii=False, separators=(",", ":")),
                    bridge_id,
                ),
            )
            if cursor.rowcount != 1:
                raise ValueError("active bridge not found")
        bridge = self.get(bridge_id)
        if bridge is None:
            raise ValueError("active bridge not found")
        return bridge

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

    def mark_thread_ingress(
        self, event_id: str, team_id: str, channel_id: str, thread_ts: str,
    ) -> bool:
        if not event_id:
            return False
        with self.connect() as db:
            try:
                db.execute(
                    "INSERT INTO thread_ingress(event_id,team_id,channel_id,thread_ts) VALUES(?,?,?,?)",
                    (event_id, team_id, channel_id, thread_ts),
                )
                db.execute(
                    """
                    UPDATE thread_participation SET updated_at=CURRENT_TIMESTAMP
                    WHERE team_id=? AND channel_id=? AND thread_ts=?
                    """,
                    (team_id, channel_id, thread_ts),
                )
                return True
            except sqlite3.IntegrityError:
                return False

    def recent_active_bridges(self, hours: int = 24, limit: int = 100) -> list[Bridge]:
        hours = max(1, min(hours, 168))
        limit = max(1, min(limit, 500))
        with self.connect() as db:
            rows = db.execute(
                """
                SELECT * FROM bridges
                WHERE status='active' AND thread_ts IS NOT NULL
                  AND created_at >= datetime('now', ?)
                ORDER BY created_at DESC LIMIT ?
                """,
                (f"-{hours} hours", limit),
            ).fetchall()
            return [bridge for row in rows if (bridge := self.decode(row)) is not None]

    def mark_ingress(self, event_id: str, bridge_id: str) -> bool:
        if not event_id:
            return False
        with self.connect() as db:
            if db.execute("SELECT 1 FROM bridge_events WHERE event_id=?", (event_id,)).fetchone():
                return False
            try:
                db.execute(
                    "INSERT INTO bridge_ingress(event_id,bridge_id) VALUES(?,?)",
                    (event_id, bridge_id),
                )
                return True
            except sqlite3.IntegrityError:
                return False

    def has_ingress(self, event_id: str) -> bool:
        with self.connect() as db:
            return bool(
                db.execute("SELECT 1 FROM bridge_ingress WHERE event_id=?", (event_id,)).fetchone()
                or db.execute("SELECT 1 FROM bridge_events WHERE event_id=?", (event_id,)).fetchone()
                or db.execute("SELECT 1 FROM thread_ingress WHERE event_id=?", (event_id,)).fetchone()
            )

    def claim_event(self, event_id: str, bridge_id: str) -> bool:
        with self.connect() as db:
            try:
                db.execute("INSERT INTO bridge_events(event_id,bridge_id,state) VALUES(?,?,'processing')", (event_id, bridge_id))
                return True
            except sqlite3.IntegrityError:
                return False

    def enqueue_event(self, event_id: str, bridge_id: str, text: str) -> bool:
        if not event_id or not text.strip() or len(text) > MAX_TEXT:
            return False
        payload = json.dumps({"text": text}, ensure_ascii=False, separators=(",", ":"))
        with self.connect() as db:
            try:
                db.execute(
                    "INSERT INTO bridge_events(event_id,bridge_id,state,payload_json,updated_at) VALUES(?,?,'queued',?,CURRENT_TIMESTAMP)",
                    (event_id, bridge_id, payload),
                )
                return True
            except sqlite3.IntegrityError:
                return False

    def claim_next_event(self, bridge_id: str) -> dict[str, str] | None:
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            if db.execute("SELECT 1 FROM bridge_events WHERE bridge_id=? AND state='processing'", (bridge_id,)).fetchone():
                return None
            row = db.execute(
                "SELECT event_id,payload_json FROM bridge_events WHERE bridge_id=? AND state='queued' ORDER BY created_at,event_id LIMIT 1",
                (bridge_id,),
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
            if db.execute(
                "SELECT 1 FROM bridge_events WHERE bridge_id=? AND state='processing'",
                (bridge_id,),
            ).fetchone():
                return []
            rows = db.execute(
                """
                SELECT event_id,payload_json FROM bridge_events
                WHERE bridge_id=? AND state='queued'
                ORDER BY created_at,event_id LIMIT ?
                """,
                (bridge_id, max(1, min(limit, 100))),
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
                "SELECT count(*) FROM bridge_events WHERE bridge_id=? AND state IN ('queued','processing')", (bridge_id,)
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
        safe_error = (error or "")[:1000] or None
        with self.connect() as db:
            db.execute(
                "UPDATE bridge_events SET state=?,error=?,updated_at=CURRENT_TIMESTAMP WHERE event_id=?",
                ("failed" if safe_error else "delivered", safe_error, event_id),
            )

    def reserve_reply(self, reply_key: str, bridge_id: str) -> tuple[bool, str]:
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                "SELECT bridge_id,message_ts FROM bridge_replies WHERE reply_key=?",
                (reply_key,),
            ).fetchone()
            if row is not None:
                if str(row["bridge_id"]) != bridge_id:
                    raise ValueError("reply key belongs to a different bridge")
                return False, str(row["message_ts"] or "")
            db.execute(
                "INSERT INTO bridge_replies(reply_key,bridge_id) VALUES(?,?)",
                (reply_key, bridge_id),
            )
            return True, ""

    def complete_reply(self, reply_key: str, message_ts: str) -> None:
        with self.connect() as db:
            db.execute(
                "UPDATE bridge_replies SET message_ts=? WHERE reply_key=?",
                (message_ts, reply_key),
            )

    def release_reply(self, reply_key: str) -> None:
        with self.connect() as db:
            db.execute(
                "DELETE FROM bridge_replies WHERE reply_key=? AND message_ts IS NULL",
                (reply_key,),
            )


def _slack_call(token: str, method: str, payload: dict[str, Any]) -> dict[str, Any]:
    path = SLACK_METHOD_PATHS.get(method)
    if path is None:
        raise ValueError("unsupported Slack API method")
    connection = http.client.HTTPSConnection("slack.com", timeout=30)
    try:
        headers = {"Authorization": "Bearer " + token}
        if method in {"conversations.replies", "users.list"}:
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
        result = json.loads(response.read())
    finally:
        connection.close()
    if not isinstance(result, dict):
        raise RuntimeError("Slack API returned an invalid response")
    if not result.get("ok"):
        raise RuntimeError(f"Slack API error: {result.get('error', 'unknown')}")
    return result


def redact_text(text: str) -> str:
    for pattern, replacement in SECRET_PATTERNS:
        text = pattern.sub(replacement, text)
    return text


def validate_reply_text(text: str, config: Config | None = None) -> str:
    config = config or load_config()
    cleaned = text.strip()
    if cleaned == "NO_REPLY":
        return cleaned
    if not cleaned:
        raise ValueError("reply text is empty")
    words = cleaned.split()
    sentence_count = len(re.findall(r"[.!?](?:\s|$)", cleaned))
    if (
        len(cleaned) > config.max_reply_chars
        or len(words) > config.max_reply_words
        or sentence_count > config.max_reply_sentences
    ):
        raise ValueError(
            "Slack reply is too long; rewrite it as one useful update within "
            f"{config.max_reply_words} words, {config.max_reply_chars} characters, "
            f"and {config.max_reply_sentences} sentences"
        )
    return cleaned


def slack_post(token: str, channel: str, text: str, thread_ts: str | None = None) -> str:
    payload: dict[str, Any] = {"channel": channel, "text": redact_text(text)[:MAX_TEXT]}
    if thread_ts:
        payload["thread_ts"] = thread_ts
    return str(_slack_call(token, "chat.postMessage", payload)["ts"])


def slack_upload(token: str, channel: str, text: str, file_path: str, thread_ts: str | None = None) -> str:
    path = Path(file_path).expanduser().resolve()
    if not path.is_file():
        raise ValueError("attachment not found")
    from slack_sdk import WebClient
    result = WebClient(token=token).files_upload_v2(
        channel=channel, file=str(path), initial_comment=redact_text(text)[:MAX_TEXT], thread_ts=thread_ts
    )
    if not result.get("ok"):
        raise RuntimeError("Slack file upload failed")
    if thread_ts:
        return thread_ts
    files = result.get("files") or ([result.get("file")] if result.get("file") else [])
    for item in files:
        for visibility in ("public", "private"):
            for share in (item.get("shares") or {}).get(visibility, {}).get(channel, []):
                if share.get("ts"):
                    return str(share["ts"])
    raise RuntimeError("Slack upload succeeded without a root timestamp")


class Broker:
    def __init__(
        self,
        token: str,
        store: Store | None = None,
        health_provider: Callable[[], dict[str, Any]] | None = None,
    ):
        if not token:
            raise ValueError("Hermes Slack credential is unavailable")
        self.token = token
        self.store = store or Store()
        self.health_provider = health_provider
        self._notify_lock = threading.Lock()
        self._joined_channels: set[str] = set()

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
            "protocol_version": 3,
            "channel_configured": bool(effective_channel(config)),
            "owner_configured": bool(config.default_owner or allowed_users),
            "allowed_user_count": len(allowed_users),
            "team_configured": bool(config.team_id),
        }
        if self.health_provider is not None:
            health = self.health_provider()
            if isinstance(health, dict):
                status.update(health)
        return status

    def _identity(self) -> dict[str, Any]:
        result = _slack_call(self.token, "auth.test", {})
        return {
            "ok": True,
            "team_id": str(result.get("team_id") or ""),
            "user_id": str(result.get("user_id") or ""),
            "user": str(result.get("user") or ""),
        }

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
        request["team_id"] = str(request.get("team_id") or config.team_id)
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
        if bridge.status == "active" and bridge.thread_ts:
            return {
                "ok": True,
                "bridge_id": bridge.bridge_id,
                "thread_ts": bridge.thread_ts,
                "deduplicated": True,
            }
        root_text = with_origin(text, bridge)
        requested_thread = request.get("thread_ts")
        self._ensure_channel_membership(bridge.channel_id)
        if request.get("file_path"):
            timestamp = slack_upload(
                self.token,
                bridge.channel_id,
                root_text,
                str(request["file_path"]),
                requested_thread,
            )
        else:
            timestamp = slack_post(self.token, bridge.channel_id, root_text, requested_thread)
        if requested_thread:
            self.store.mark_participation(bridge.team_id, bridge.channel_id, str(requested_thread))
        bridge = self.store.bind(bridge.bridge_id, str(requested_thread or timestamp))
        return {
            "ok": True,
            "bridge_id": bridge.bridge_id,
            "thread_ts": bridge.thread_ts,
            "deduplicated": False,
        }

    def _reply(self, request: BridgeRequest) -> dict[str, Any]:
        bridge = self.store.get(str(request.get("bridge_id") or ""))
        if not bridge or not bridge.thread_ts:
            raise ValueError("active bridge not found")
        text = validate_reply_text(str(request.get("text") or ""))
        if text == "NO_REPLY":
            return {
                "ok": True,
                "bridge_id": bridge.bridge_id,
                "thread_ts": bridge.thread_ts,
                "suppressed": True,
            }
        reply_key = str(request.get("reply_key") or "")
        if reply_key and not REPLY_KEY_PATTERN.fullmatch(reply_key):
            raise ValueError("invalid reply key")
        if reply_key:
            reserved, existing = self.store.reserve_reply(reply_key, bridge.bridge_id)
            if not reserved:
                return {
                    "ok": True,
                    "bridge_id": bridge.bridge_id,
                    "thread_ts": bridge.thread_ts,
                    "message_ts": existing,
                    "deduplicated": True,
                }
        self._ensure_channel_membership(bridge.channel_id)
        try:
            timestamp = slack_post(self.token, bridge.channel_id, text, bridge.thread_ts)
        except Exception:
            if reply_key:
                self.store.release_reply(reply_key)
            raise
        if reply_key:
            self.store.complete_reply(reply_key, timestamp)
        self.store.mark_participation(bridge.team_id, bridge.channel_id, bridge.thread_ts)
        return {
            "ok": True,
            "bridge_id": bridge.bridge_id,
            "thread_ts": bridge.thread_ts,
            "message_ts": timestamp,
            "deduplicated": False,
        }

    def _rebind(self, request: BridgeRequest) -> dict[str, Any]:
        channel = str(request.get("channel_id") or "")
        thread_ts = str(request.get("thread_ts") or "")
        if not channel or not thread_ts:
            raise ValueError("Slack channel and thread timestamp are required")
        bridge = self.store.find_thread(channel, thread_ts)
        if bridge is None:
            raise ValueError("active bridge not found")
        source_kind = str(request.get("source_kind") or "")
        source = request.get("source")
        rebound = self.store.rebind(bridge.bridge_id, source_kind, source)
        return {
            "ok": True,
            "bridge_id": rebound.bridge_id,
            "thread_ts": rebound.thread_ts,
            "source_kind": rebound.source_kind,
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
        request["team_id"] = str(request.get("team_id") or config.team_id)
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
        channel = str(request.get("channel_id") or "")
        thread_ts = str(request.get("thread_ts") or "")
        text = str(request.get("text") or "")
        if not channel or not thread_ts:
            raise ValueError("Slack channel and thread timestamp are required")
        if not text.strip() or len(text) > MAX_TEXT:
            raise ValueError("thread reply text is empty or too large")
        self._ensure_channel_membership(channel)
        message_ts = slack_post(self.token, channel, text, thread_ts)
        self.store.mark_participation(
            str(request.get("team_id") or config.team_id), channel, thread_ts,
        )
        return {"ok": True, "thread_ts": thread_ts, "message_ts": message_ts}

    def _users(self, request: BridgeRequest, allowed_users: tuple[str, ...]) -> dict[str, Any]:
        query = str(request.get("query") or "").strip().casefold()
        if not query:
            raise ValueError("user query is required")
        result = _slack_call(self.token, "users.list", {"limit": 200})
        allowed = set(allowed_users)
        matches = []
        for member in result.get("members", []):
            if (
                not isinstance(member, dict)
                or str(member.get("id") or "") not in allowed
                or member.get("deleted")
                or member.get("is_bot")
            ):
                continue
            profile = member.get("profile") if isinstance(member.get("profile"), dict) else {}
            candidate = {
                "id": str(member.get("id") or ""),
                "name": str(member.get("name") or ""),
                "real_name": str(member.get("real_name") or profile.get("real_name") or ""),
                "display_name": str(profile.get("display_name") or ""),
            }
            if query in " ".join(candidate.values()).casefold():
                matches.append(candidate)
        return {"ok": True, "users": matches[:20]}

    def _direct_message(
        self,
        incoming: BridgeRequest,
        config: Config,
        allowed_users: tuple[str, ...],
    ) -> dict[str, Any]:
        recipients = tuple(dict.fromkeys(str(item) for item in incoming.get("user_ids", [])))
        if not 1 <= len(recipients) <= 7:
            raise ValueError("a DM requires between one and seven unique recipients")
        if any(not ID_PATTERN.fullmatch(user_id) for user_id in recipients):
            raise ValueError("DM recipient is not a valid Slack member ID")
        allowed = set(allowed_users)
        if not allowed or any(user_id not in allowed for user_id in recipients):
            raise ValueError("every DM recipient must be an authorized Hermes operator")
        text = validate_reply_text(str(incoming.get("text") or ""), config)
        opened = _slack_call(
            self.token,
            "conversations.open",
            {"users": ",".join(recipients), "return_im": True},
        )
        channel = opened.get("channel")
        channel_id = str(channel.get("id") if isinstance(channel, dict) else "")
        if not ID_PATTERN.fullmatch(channel_id):
            raise RuntimeError("Slack opened a DM without a valid channel ID")
        request = BridgeRequest(incoming)
        request["text"] = text
        request["channel_id"] = channel_id
        request["owner_user_id"] = "*"
        request["team_id"] = str(request.get("team_id") or config.team_id)
        result = self._notify(request, config, allowed_users)
        result["channel_id"] = channel_id
        return result

    def handle(self, request: BridgeRequest) -> dict[str, Any]:
        operation = str(request.get("op", "notify"))
        config = load_config()
        allowed_users = effective_allowed_users(config)
        if operation == "status":
            return self._status(config, allowed_users)
        if operation == "identity":
            return self._identity()
        if operation == "notify":
            with self._notify_lock:
                return self._notify(request, config, allowed_users)
        if operation == "attach":
            with self._notify_lock:
                return self._attach(request, config, allowed_users)
        if operation == "reply":
            return self._reply(request)
        if operation == "rebind":
            return self._rebind(request)
        if operation == "history":
            return self._history(request, config)
        if operation == "thread_history":
            return self._thread_history(request, config)
        if operation == "thread_reply":
            return self._thread_reply(request, config)
        if operation == "users":
            return self._users(request, allowed_users)
        if operation == "dm":
            with self._notify_lock:
                return self._direct_message(request, config, allowed_users)
        raise ValueError("unsupported operation")


class Handler(socketserver.StreamRequestHandler):
    def handle(self) -> None:
        try:
            raw = self.rfile.readline(1_048_577)
            if len(raw) > 1_048_576:
                raise ValueError("request too large")
            if not isinstance(self.server, UnixServer):
                raise RuntimeError("invalid Tether broker server")
            response = self.server.broker.handle(json.loads(raw))
        except Exception as exc:
            response = {"ok": False, "error": str(exc)[:500]}
        self.wfile.write(json.dumps(response, separators=(",", ":")).encode() + b"\n")


class UnixServer(socketserver.ThreadingUnixStreamServer):
    daemon_threads = True
    broker: Broker


def start_broker(
    token: str,
    path: Path = SOCKET_PATH,
    health_provider: Callable[[], dict[str, Any]] | None = None,
) -> UnixServer:
    path.parent.mkdir(parents=True, exist_ok=True)
    os.chmod(path.parent, 0o700)
    if path.exists() or path.is_symlink():
        mode = path.lstat().st_mode
        if not stat.S_ISSOCK(mode):
            raise RuntimeError("refusing to replace non-socket bridge path")
        path.unlink()
    server = UnixServer(str(path), Handler)
    server.broker = Broker(token, health_provider=health_provider)
    os.chmod(path, 0o600)
    threading.Thread(target=server.serve_forever, name="hermes-bridge-broker", daemon=True).start()
    return server


def broker_call(request: BridgeRequest, path: Path = SOCKET_PATH) -> dict[str, Any]:
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
        client.settimeout(35)
        client.connect(str(path))
        client.sendall(json.dumps(request, separators=(",", ":")).encode() + b"\n")
        chunks = b""
        while not chunks.endswith(b"\n"):
            part = client.recv(65_536)
            if not part:
                break
            chunks += part
            if len(chunks) > 1_048_576:
                raise RuntimeError("bridge response is too large")
    result = json.loads(chunks)
    if not result.get("ok"):
        raise RuntimeError(result.get("error", "bridge broker failed"))
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
    terminal = f" in Zellij `{zellij_session}`" if zellij_session else ""
    if terminal and zellij_pane:
        terminal += f" / pane `{zellij_pane}`"
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


def _credential_env(bridge: Bridge, config: Config) -> dict[str, str]:
    if not config.credential_command:
        return {}
    metadata = json.dumps({
        "bridge_id": bridge.bridge_id,
        "source_kind": bridge.source_kind,
        "session_id": str(bridge.source.get("session_id") or "")[:128],
    })
    command = [_resolve_executable(config.credential_command[0]), *config.credential_command[1:]]
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
    if any("SLACK" in key.upper() or key == "OP_SERVICE_ACCOUNT_TOKEN" for key in values):
        raise NativeContinuationError("credential helper returned a forbidden key")
    return values


def _wait_for_exit(process: subprocess.Popen[str], timeout: float) -> bool:
    try:
        process.wait(timeout=timeout)
        return True
    except subprocess.TimeoutExpired:
        return False


def _stop_process_group(process: subprocess.Popen[str]) -> None:
    with contextlib.suppress(ProcessLookupError):
        os.killpg(process.pid, signal.SIGTERM)
    if _wait_for_exit(process, 5):
        return
    with contextlib.suppress(ProcessLookupError):
        os.killpg(process.pid, signal.SIGKILL)
    if not _wait_for_exit(process, 5):
        raise NativeContinuationError("agent continuation process could not be stopped")


def continue_native(bridge: Bridge, prompt: str, cancel_event: threading.Event | None = None) -> str:
    config = load_config()
    source = bridge.source
    cwd = str(source.get("cwd") or "")
    session_id = str(source.get("session_id") or "")
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
    # Fixed agent CLI plus administrator-only resume flags; prompts remain on stdin.
    process = subprocess.Popen(  # nosec B603
        command, cwd=cwd, env=env, text=True, stdin=subprocess.PIPE,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True,
    )
    deadline = time.monotonic() + config.native_timeout_seconds
    pending_input: str | None = prompt
    while True:
        if cancel_event is not None and cancel_event.is_set():
            _stop_process_group(process)
            raise NativeContinuationError("agent continuation cancelled by the operator")
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            _stop_process_group(process)
            raise NativeContinuationError("agent continuation timed out")
        try:
            stdout, stderr = process.communicate(input=pending_input, timeout=min(1.0, remaining))
            break
        except subprocess.TimeoutExpired:
            pending_input = None
            continue
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
    if len(output) > MAX_NATIVE_OUTPUT:
        output = output[:MAX_NATIVE_OUTPUT] + "\n\n_[Output truncated by Tether.]_"
    return output


def _pane_number(pane: str) -> int:
    normalized = pane.removeprefix("terminal_")
    if not normalized.isdigit():
        raise NativeContinuationError("captured Zellij pane is not a terminal pane")
    return int(normalized)


def _zellij_agent_process(
    session: str,
    pane: str,
    allowed: set[str],
    proc_root: Path = Path("/proc"),
) -> tuple[str, str]:
    candidates: list[tuple[bool, int, str, str]] = []
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
            raw_command = (process_dir / "cmdline").read_bytes()
            tokens = [
                value.decode("utf-8", "replace")
                for value in raw_command.split(b"\0")
                if value
            ]
            agent = next(
                (
                    name
                    for token in tokens
                    for name in allowed
                    if Path(token).name == name
                ),
                "",
            )
            if not agent:
                continue
            stat_text = (process_dir / "stat").read_text(encoding="utf-8")
            fields = stat_text[stat_text.rfind(")") + 2 :].split()
            pid = int(process_dir.name)
            foreground = len(fields) > 19 and int(fields[5]) == pid
            start_time = fields[19] if len(fields) > 19 else ""
            descriptor = (
                f"proc:{pid}:{start_time}:"
                f"{hashlib.sha256(raw_command).hexdigest()}"
            )
            candidates.append((foreground, pid, agent, descriptor))
        except (FileNotFoundError, PermissionError, ProcessLookupError, ValueError, IndexError):
            continue
    foreground = [candidate for candidate in candidates if candidate[0]]
    selected = foreground or candidates
    if len(selected) != 1:
        raise NativeContinuationError(
            "Zellij omitted pane command metadata and an exact live agent process could not be resolved"
        )
    _, _, agent, descriptor = selected[0]
    return agent, descriptor


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
    if record is None or record.get("exited"):
        raise NativeContinuationError("captured Zellij pane is no longer active")
    configured = config or load_config()
    allowed = set(configured.zellij_agent_commands)
    terminal_command = str(record.get("terminal_command") or "")
    if terminal_command:
        tokens = shlex.split(terminal_command)
        agent = next(
            (name for token in tokens for name in allowed if Path(token).name == name),
            "",
        )
        fingerprint_source = terminal_command
    else:
        agent, fingerprint_source = _zellij_agent_process(
            session, str(pane_number), allowed
        )
    if not agent:
        raise NativeContinuationError(
            "captured Zellij pane is not running an allowlisted agent; use --run-id for a shell or cron"
        )
    return {
        "session_name": session,
        "pane_id": str(pane_number),
        "cwd": cwd,
        "pane_agent": agent,
        "pane_command_hash": hashlib.sha256(
            fingerprint_source.encode()
        ).hexdigest(),
    }


def deliver_zellij(bridge: Bridge, text: str) -> None:
    session = str(bridge.source.get("session_name") or bridge.source.get("zellij_session") or "")
    pane = str(bridge.source.get("pane_id") or bridge.source.get("zellij_pane_id") or "")
    if not session or not pane:
        raise ValueError("captured Zellij endpoint is incomplete")
    expected_hash = str(bridge.source.get("pane_command_hash") or "")
    if not expected_hash:
        raise NativeContinuationError("legacy Zellij bridge has no process identity; create a new notification")
    current = zellij_pane_identity(session, pane, str(bridge.source.get("cwd") or ""))
    if current["pane_command_hash"] != expected_hash:
        raise NativeContinuationError("captured Zellij pane now hosts a different process")
    notifier = RUNTIME_HOME / "tether_notify.py"
    marker = "tether-" + hashlib.sha256(
        f"{bridge.bridge_id}\0{text}".encode()
    ).hexdigest()[:12]
    inbox_dir = RUNTIME_HOME / "inbox"
    inbox_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    inbox_dir.chmod(0o700)
    now = time.time()
    for stale in inbox_dir.glob("tether-*.txt"):
        with contextlib.suppress(OSError):
            if not stale.is_symlink() and now - stale.stat().st_mtime > 86_400:
                stale.unlink()
    inbox_path = inbox_dir / f"{marker}.txt"
    flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(inbox_path, flags, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as inbox:
            inbox.write(text)
            inbox.write("\n")
    except BaseException:
        with contextlib.suppress(OSError):
            os.close(descriptor)
        raise
    inbox_path.chmod(0o600)
    instruction = (
        f"[Hermes Slack follow-up batch; {marker}] Read and handle the complete request in "
        f"{inbox_path}, then delete that file. "
        "Handle it as one turn. Check the current thread/task state before responding. "
        "Post at most one Slack message for this entire batch, only when a useful response is needed; "
        "do not post a second status summary or courtesy acknowledgment. Keep it within 50 words and "
        "3 sentences. If no new response is needed, run the command with NO_REPLY. Reply with: "
        f"python3 {notifier} reply --bridge-id {bridge.bridge_id} --reply-key {marker} "
        "--text '<your response or NO_REPLY>'"
    )
    target = pane if pane.startswith(("terminal_", "plugin_")) else "terminal_" + pane
    zellij = _resolve_executable("zellij")
    # Absolute executable; session and pane are argv, never shell text.
    subprocess.run(  # nosec B603
        [zellij, "--session", session, "action", "write-chars", "--pane-id", target, instruction],
        check=True,
        timeout=10,
    )
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
        raise NativeContinuationError("Slack instruction was not visible in the captured Zellij pane")
    subprocess.run(  # nosec B603
        [zellij, "--session", session, "action", "send-keys", "--pane-id", target, "Enter"],
        check=True,
        timeout=10,
    )
    time.sleep(0.5)
    submitted = subprocess.run(  # nosec B603
        [zellij, "--session", session, "action", "dump-screen", "--pane-id", target],
        check=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=10,
    )
    if marker not in submitted.stdout:
        raise NativeContinuationError("Slack instruction disappeared before submission could be verified")
    after_submit = zellij_pane_identity(session, pane, str(bridge.source.get("cwd") or ""))
    if after_submit["pane_command_hash"] != expected_hash:
        raise NativeContinuationError("captured agent exited or changed process after Slack submission")


def doctor() -> tuple[bool, list[str]]:
    checks: list[str] = []
    ok = True
    if CONFIG_PATH.is_file():
        mode = stat.S_IMODE(CONFIG_PATH.stat().st_mode)
        if mode & 0o077:
            ok = False
            checks.append(f"FAIL config permissions are {mode:04o}; expected 0600")
        else:
            checks.append("ok config permissions")
        try:
            load_config()
            checks.append("ok optional Tether overrides are valid")
        except Exception as exc:
            ok = False
            checks.append(f"FAIL config: {exc}")
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
            elif poll_healthy is True:
                checks.append("WARN Socket Mode is disconnected; reply polling fallback is healthy")
            elif transport is False or poll_healthy is False:
                ok = False
                checks.append("FAIL no healthy Slack reply ingress path")
            else:
                checks.append("WARN Slack reply ingress health is not yet observed")
        except Exception as exc:
            ok = False
            checks.append(f"FAIL broker readiness check: {str(exc)[:160]}")
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
    try:
        hermes_checkout = HERMES_HOME / "hermes-agent"
        if hermes_checkout.is_dir() and str(hermes_checkout) not in sys.path:
            sys.path.insert(0, str(hermes_checkout))
        from plugins.platforms.slack.adapter import SlackAdapter
        if hasattr(SlackAdapter, "_handle_slack_message"):
            checks.append("ok Hermes Slack adapter compatibility surface")
        else:
            ok = False
            checks.append("FAIL Hermes Slack adapter is incompatible")
    except Exception:
        checks.append("WARN Hermes Slack adapter import unavailable outside gateway environment")
    return ok, checks
