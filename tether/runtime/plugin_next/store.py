"""The whole persistent model of Tether: which thread owns which session, and the
turns waiting for it.

This replaces the schema-18 durability core (domain_schema / domain_runtime /
domain_control) for the ``session`` driver. That core proved that a *forked*
``claude --print`` process delivered a turn: attempts, leases, receipts,
uncertain-state classification. With one long-lived session process per binding
the reply is a line we read off a pipe, so the model shrinks to four tables and
the same method names ActiveSlice already calls.

Error contract: every refusal is a :class:`StoreError` whose ``code`` is the
same string the old runtime used, so the broker surfaces identical codes.
"""

from __future__ import annotations

import json
import sqlite3
import threading
import time
import uuid
from pathlib import Path
from typing import Any

NO_REPLY_TOKEN = "NO_REPLY"  # nosec B105 - a reply marker, not a credential

_SCHEMA = """
CREATE TABLE IF NOT EXISTS endpoints(
  endpoint_id TEXT PRIMARY KEY,
  endpoint_key TEXT NOT NULL UNIQUE,
  endpoint_kind TEXT NOT NULL,
  source_kind TEXT NOT NULL,
  source_json TEXT NOT NULL,
  state TEXT NOT NULL DEFAULT 'ready',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS bindings(
  binding_id TEXT PRIMARY KEY,
  endpoint_id TEXT NOT NULL REFERENCES endpoints(endpoint_id),
  team_id TEXT NOT NULL,
  channel_id TEXT NOT NULL,
  thread_ts TEXT NOT NULL DEFAULT '',
  owner_user_id TEXT NOT NULL DEFAULT '',
  idempotency_key TEXT NOT NULL UNIQUE,
  state TEXT NOT NULL,              -- pending_root | active | closed
  generation INTEGER NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS bindings_thread ON bindings(team_id, channel_id, thread_ts, state);
CREATE TABLE IF NOT EXISTS turns(
  event_key TEXT PRIMARY KEY,
  binding_id TEXT NOT NULL REFERENCES bindings(binding_id),
  binding_generation INTEGER NOT NULL,
  ordered_at TEXT NOT NULL,
  payload_inline TEXT,
  state TEXT NOT NULL,              -- ready | running | completed | cancelled | failed
  attempt_id TEXT,
  error_code TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS turns_binding_state ON turns(binding_id, state, ordered_at);
CREATE TABLE IF NOT EXISTS attempts(
  attempt_id TEXT PRIMARY KEY,
  endpoint_id TEXT NOT NULL,
  binding_id TEXT NOT NULL,
  state TEXT NOT NULL,              -- accepted | completed_with_response | no_reply | failed
  response_ref TEXT,
  error_code TEXT,
  created_at TEXT NOT NULL,
  terminal_at TEXT
);
CREATE INDEX IF NOT EXISTS attempts_endpoint_state ON attempts(endpoint_id, state);
"""


class StoreError(RuntimeError):
    def __init__(self, code: str, message: str | None = None):
        super().__init__(message or code)
        self.code = code


def _now() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())


def _id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:24]}"


def is_no_reply(text: str) -> bool:
    """NO_REPLY as the whole message or as its last line means: do not post."""
    stripped = (text or "").strip()
    if not stripped:
        return False
    if stripped == NO_REPLY_TOKEN:
        return True
    lines = [line.strip() for line in stripped.splitlines() if line.strip()]
    return bool(lines) and lines[-1] == NO_REPLY_TOKEN and len(stripped) <= 2000


class Store:
    """SQLite-backed bindings, turns and attempts. Thread-safe (one lock)."""

    def __init__(self, path: Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._db = sqlite3.connect(self.path, timeout=30, check_same_thread=False)
        self._db.row_factory = sqlite3.Row
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA synchronous=NORMAL")
        self._db.execute("PRAGMA foreign_keys=ON")
        self._db.executescript(_SCHEMA)
        self._db.commit()
        try:
            self.path.chmod(0o600)
        except OSError:
            pass

    def close(self) -> None:
        with self._lock:
            self._db.close()

    # -- endpoints -----------------------------------------------------------------

    def register_endpoint(
        self,
        *,
        endpoint_key: str,
        endpoint_kind: str,
        source_kind: str,
        source_json: str,
        ref_version: int = 1,
        descriptor: Any = None,
        capabilities: tuple[str, ...] = (),
    ) -> dict[str, Any]:
        """Idempotent on ``endpoint_key``; re-registering repoints the source."""
        if not endpoint_key:
            raise StoreError("endpoint_key_required")
        with self._lock:
            row = self._db.execute(
                "SELECT * FROM endpoints WHERE endpoint_key=?", (endpoint_key,)
            ).fetchone()
            now = _now()
            if row is None:
                endpoint_id = _id("end")
                self._db.execute(
                    "INSERT INTO endpoints(endpoint_id,endpoint_key,endpoint_kind,source_kind,"
                    "source_json,state,created_at,updated_at) VALUES(?,?,?,?,?,'ready',?,?)",
                    (endpoint_id, endpoint_key, endpoint_kind, source_kind, source_json, now, now),
                )
            else:
                endpoint_id = row["endpoint_id"]
                if row["source_kind"] != source_kind:
                    raise StoreError("endpoint_identity_conflict")
                self._db.execute(
                    "UPDATE endpoints SET source_json=?, state='ready', updated_at=? WHERE endpoint_id=?",
                    (source_json, now, endpoint_id),
                )
            self._db.commit()
            return self._endpoint_view(self._db.execute(
                "SELECT * FROM endpoints WHERE endpoint_id=?", (endpoint_id,)).fetchone())

    def _endpoint_view(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "endpoint_id": row["endpoint_id"], "endpoint_key": row["endpoint_key"],
            "endpoint_kind": row["endpoint_kind"], "source_kind": row["source_kind"],
            "source": json.loads(row["source_json"] or "{}"), "state": row["state"],
        }

    # -- bindings -------------------------------------------------------------------

    def bind_thread(
        self,
        *,
        endpoint_id: str,
        team_id: str,
        channel_id: str,
        owner_user_id: str,
        idempotency_key: str,
        thread_ts: str | None = None,
    ) -> dict[str, Any]:
        if not idempotency_key:
            raise StoreError("idempotency_key_required")
        with self._lock:
            endpoint = self._db.execute(
                "SELECT * FROM endpoints WHERE endpoint_id=?", (endpoint_id,)).fetchone()
            if endpoint is None:
                raise StoreError("endpoint_unknown")
            existing = self._db.execute(
                "SELECT * FROM bindings WHERE idempotency_key=?", (idempotency_key,)).fetchone()
            if existing is not None:
                same = (existing["endpoint_id"], existing["team_id"], existing["channel_id"]) == (
                    endpoint_id, team_id, channel_id,
                ) and (not thread_ts or existing["thread_ts"] in ("", thread_ts))
                if same and existing["state"] != "closed":
                    # a retried notify arrives without a thread: the row already has it
                    return self._binding_view(existing)
                if existing["state"] != "closed":
                    raise StoreError("idempotency_conflict")
                # a closed binding with the same key: fall through and create a new generation
                idempotency_key = f"{idempotency_key}#{existing['generation'] + 1}"
            if thread_ts:
                live = self._db.execute(
                    "SELECT binding_id FROM bindings WHERE team_id=? AND channel_id=? AND thread_ts=? "
                    "AND state!='closed'", (team_id, channel_id, thread_ts)).fetchone()
                if live is not None:
                    raise StoreError("thread_claim_conflict")
            now = _now()
            binding_id = _id("bnd")
            self._db.execute(
                "INSERT INTO bindings(binding_id,endpoint_id,team_id,channel_id,thread_ts,owner_user_id,"
                "idempotency_key,state,generation,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,1,?,?)",
                (binding_id, endpoint_id, team_id, channel_id, thread_ts or "", owner_user_id,
                 idempotency_key, "active" if thread_ts else "pending_root", now, now),
            )
            self._db.commit()
            return self._binding_view(self._db.execute(
                "SELECT * FROM bindings WHERE binding_id=?", (binding_id,)).fetchone())

    def activate_binding(self, binding_id: str, thread_ts: str) -> dict[str, Any]:
        if not thread_ts:
            raise StoreError("thread_ts_required")
        with self._lock:
            row = self._db.execute(
                "SELECT * FROM bindings WHERE binding_id=?", (binding_id,)).fetchone()
            if row is None:
                raise StoreError("binding_unknown")
            if row["state"] == "active":
                if row["thread_ts"] != thread_ts:
                    raise StoreError("thread_claim_conflict")
                return self._binding_view(row)
            if row["state"] != "pending_root":
                raise StoreError("binding_not_claimable")
            self._db.execute(
                "UPDATE bindings SET thread_ts=?, state='active', updated_at=? WHERE binding_id=?",
                (thread_ts, _now(), binding_id))
            self._db.commit()
            return self._binding_view(self._db.execute(
                "SELECT * FROM bindings WHERE binding_id=?", (binding_id,)).fetchone())

    def find_active_binding(self, *, team_id: str, channel_id: str, thread_ts: str) -> dict[str, Any] | None:
        if not thread_ts:
            return None
        with self._lock:
            row = self._db.execute(
                "SELECT * FROM bindings WHERE team_id=? AND channel_id=? AND thread_ts=? AND state='active' "
                "ORDER BY created_at DESC LIMIT 1", (team_id, channel_id, thread_ts)).fetchone()
        return dict(row) if row else None

    def live_binding_for_thread(self, *, team_id: str, channel_id: str, thread_ts: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._db.execute(
                "SELECT * FROM bindings WHERE team_id=? AND channel_id=? AND thread_ts=? AND state!='closed' "
                "ORDER BY created_at DESC LIMIT 1", (team_id, channel_id, thread_ts)).fetchone()
        return self._binding_view(row) if row else None

    def binding_thread(self, binding_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._db.execute(
                "SELECT * FROM bindings WHERE binding_id=?", (binding_id,)).fetchone()
        return dict(row) if row and row["thread_ts"] else None

    def close_binding(self, binding_id: str) -> dict[str, Any]:
        """Refuses while ready or running turns exist: drain first, then close."""
        with self._lock:
            row = self._db.execute(
                "SELECT * FROM bindings WHERE binding_id=?", (binding_id,)).fetchone()
            if row is None:
                raise StoreError("binding_unknown")
            if row["state"] == "closed":
                return self._binding_view(row)
            busy = self._db.execute(
                "SELECT COUNT(*) FROM turns WHERE binding_id=? AND state IN ('ready','running')",
                (binding_id,)).fetchone()[0]
            if busy:
                raise StoreError("binding_has_ready_turns", f"{busy} turn(s) still ready or running")
            self._db.execute(
                "UPDATE bindings SET state='closed', generation=generation+1, updated_at=? WHERE binding_id=?",
                (_now(), binding_id))
            self._db.commit()
            return self._binding_view(self._db.execute(
                "SELECT * FROM bindings WHERE binding_id=?", (binding_id,)).fetchone())

    def _binding_view(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "binding_id": row["binding_id"], "endpoint_id": row["endpoint_id"],
            "state": row["state"], "generation": row["generation"], "team_id": row["team_id"],
            "channel_id": row["channel_id"], "thread_ts": row["thread_ts"],
            "owner_user_id": row["owner_user_id"],
            "active": row["state"] == "active", "pending_root": row["state"] == "pending_root",
        }

    # -- turns ----------------------------------------------------------------------

    def admit_turn(
        self,
        *,
        binding_id: str,
        event_key: str,
        ordered_at: str,
        mutation_kind: str = "create",
        mutation_target_key: str | None = None,
        payload_inline: str | None = None,
        payload_ref: str | None = None,
        payload_bytes: int | None = None,
    ) -> dict[str, Any]:
        if not event_key:
            raise StoreError("event_key_required")
        with self._lock:
            existing = self._db.execute(
                "SELECT * FROM turns WHERE event_key=?", (event_key,)).fetchone()
            if existing is not None:
                if existing["binding_id"] != binding_id:
                    raise StoreError("event_binding_conflict")
                return self._turn_view(existing)
            binding = self._db.execute(
                "SELECT * FROM bindings WHERE binding_id=?", (binding_id,)).fetchone()
            if binding is None:
                raise StoreError("binding_unknown")
            if binding["state"] != "active":
                raise StoreError("binding_not_admitting")
            now = _now()
            self._db.execute(
                "INSERT INTO turns(event_key,binding_id,binding_generation,ordered_at,payload_inline,state,"
                "created_at,updated_at) VALUES(?,?,?,?,?,'ready',?,?)",
                (event_key, binding_id, binding["generation"], ordered_at, payload_inline, now, now))
            self._db.commit()
            return self._turn_view(self._db.execute(
                "SELECT * FROM turns WHERE event_key=?", (event_key,)).fetchone())

    def _turn_view(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "event_key": row["event_key"], "binding_id": row["binding_id"],
            "binding_generation": row["binding_generation"], "state": row["state"],
            "ordered_at": row["ordered_at"], "error_code": row["error_code"],
            "payload_inline": row["payload_inline"],
        }

    def recent_turn_actors(self, binding_id: str, limit: int = 2) -> list[str]:
        with self._lock:
            rows = self._db.execute(
                "SELECT payload_inline FROM turns WHERE binding_id=? ORDER BY ordered_at DESC LIMIT ?",
                (binding_id, int(limit))).fetchall()
        actors: list[str] = []
        for row in rows:
            try:
                actors.append(str(json.loads(row["payload_inline"] or "{}").get("user") or ""))
            except ValueError:
                actors.append("")
        return actors

    # -- scheduling -----------------------------------------------------------------

    def endpoints_with_ready_turns(self) -> list[str]:
        with self._lock:
            rows = self._db.execute(
                "SELECT DISTINCT b.endpoint_id FROM turns t JOIN bindings b ON b.binding_id=t.binding_id "
                "WHERE t.state='ready' AND b.state='active' AND b.endpoint_id NOT IN "
                "(SELECT endpoint_id FROM attempts WHERE state='accepted') ORDER BY t.ordered_at").fetchall()
        return [row["endpoint_id"] for row in rows]

    def schedule_next(self, endpoint_id: str, **_: Any) -> dict[str, Any] | None:
        """Bundle every ready turn of the endpoint's oldest busy binding into one attempt."""
        with self._lock:
            running = self._db.execute(
                "SELECT attempt_id FROM attempts WHERE endpoint_id=? AND state='accepted'",
                (endpoint_id,)).fetchone()
            if running is not None:
                return None
            turn = self._db.execute(
                "SELECT t.* FROM turns t JOIN bindings b ON b.binding_id=t.binding_id "
                "WHERE b.endpoint_id=? AND t.state='ready' AND b.state='active' "
                "ORDER BY t.ordered_at LIMIT 1", (endpoint_id,)).fetchone()
            if turn is None:
                return None
            attempt_id = _id("att")
            now = _now()
            self._db.execute(
                "INSERT INTO attempts(attempt_id,endpoint_id,binding_id,state,created_at) "
                "VALUES(?,?,?,'accepted',?)", (attempt_id, endpoint_id, turn["binding_id"], now))
            self._db.execute(
                "UPDATE turns SET state='running', attempt_id=?, updated_at=? "
                "WHERE binding_id=? AND state='ready'", (attempt_id, now, turn["binding_id"]))
            self._db.commit()
            return {"attempt_id": attempt_id, "endpoint_id": endpoint_id,
                    "binding_id": turn["binding_id"], "state": "accepted"}

    def attempt_context(self, attempt_id: str) -> dict[str, Any]:
        with self._lock:
            attempt = self._db.execute(
                "SELECT * FROM attempts WHERE attempt_id=?", (attempt_id,)).fetchone()
            if attempt is None:
                raise StoreError("attempt_unknown")
            endpoint = self._db.execute(
                "SELECT * FROM endpoints WHERE endpoint_id=?", (attempt["endpoint_id"],)).fetchone()
            binding = self._db.execute(
                "SELECT * FROM bindings WHERE binding_id=?", (attempt["binding_id"],)).fetchone()
            turns = self._db.execute(
                "SELECT * FROM turns WHERE attempt_id=? ORDER BY ordered_at", (attempt_id,)).fetchall()
        return {
            "attempt_id": attempt_id, "state": attempt["state"], "error_code": attempt["error_code"],
            "response_ref": attempt["response_ref"],
            "source_kind": endpoint["source_kind"], "source": json.loads(endpoint["source_json"] or "{}"),
            "team_id": binding["team_id"], "channel_id": binding["channel_id"],
            "thread_ts": binding["thread_ts"], "owner_user_id": binding["owner_user_id"],
            "turns": [dict(t) for t in turns],
        }

    def finish_attempt(
        self, attempt_id: str, *, state: str, response_ref: str | None = None, error_code: str | None = None,
    ) -> dict[str, Any]:
        """Terminal transition. Turns complete on a reply or NO_REPLY; cancel on failure."""
        if state not in {"completed_with_response", "no_reply", "failed"}:
            raise StoreError("attempt_state_invalid", state)
        with self._lock:
            now = _now()
            self._db.execute(
                "UPDATE attempts SET state=?, response_ref=?, error_code=?, terminal_at=? WHERE attempt_id=?",
                (state, response_ref, error_code, now, attempt_id))
            turn_state = "completed" if state != "failed" else "cancelled"
            self._db.execute(
                "UPDATE turns SET state=?, error_code=?, updated_at=? WHERE attempt_id=?",
                (turn_state, error_code if state == "failed" else None, now, attempt_id))
            self._db.commit()
        return {"attempt_id": attempt_id, "state": state, "error_code": error_code}

    def uncertain_attempts(self) -> list[dict[str, Any]]:
        """Accepted attempts nobody is driving: only possible after a crash. Failed on sight."""
        with self._lock:
            rows = self._db.execute(
                "SELECT attempt_id FROM attempts WHERE state='accepted'").fetchall()
        return [dict(row) for row in rows]

    def fail_orphans(self) -> int:
        """At start-up: attempts left 'accepted' by a dead gateway are failed, turns cancelled."""
        with self._lock:
            rows = self._db.execute(
                "SELECT attempt_id FROM attempts WHERE state='accepted'").fetchall()
        for row in rows:
            self.finish_attempt(row["attempt_id"], state="failed", error_code="gateway_restarted")
        return len(rows)

    def import_legacy_bindings(self, domain_db: Path) -> int:
        """One-time import of active thread bindings from the schema-18 ``domain.db``.

        Idempotent: threads already bound here are skipped. Endpoints keep their
        key and source (session id, cwd). Returns the number of bindings added.
        """
        domain_db = Path(domain_db)
        if not domain_db.exists():
            return 0
        try:
            legacy = sqlite3.connect(f"file:{domain_db}?mode=ro", uri=True)
            legacy.row_factory = sqlite3.Row
            rows = legacy.execute(
                "SELECT b.team_id, b.channel_id, b.thread_ts, b.owner_user_id, e.endpoint_key, "
                "e.endpoint_kind, e.source_kind, e.source_json FROM thread_bindings b "
                "JOIN endpoints e ON e.endpoint_id=b.endpoint_id "
                "WHERE b.state='active' AND b.thread_ts!='' ORDER BY b.created_at"
            ).fetchall()
            legacy.close()
        except sqlite3.Error:
            return 0
        added = 0
        for row in rows:
            if self.find_active_binding(team_id=row["team_id"], channel_id=row["channel_id"],
                                        thread_ts=row["thread_ts"]) is not None:
                continue
            try:
                endpoint = self.register_endpoint(
                    endpoint_key=row["endpoint_key"], endpoint_kind=row["endpoint_kind"] or "detached_native",
                    source_kind=row["source_kind"], source_json=row["source_json"] or "{}",
                )
                self.bind_thread(
                    endpoint_id=endpoint["endpoint_id"], team_id=row["team_id"], channel_id=row["channel_id"],
                    owner_user_id=row["owner_user_id"] or "",
                    idempotency_key=f"bind:{row['team_id']}:{row['channel_id']}:{row['thread_ts']}",
                    thread_ts=row["thread_ts"],
                )
                added += 1
            except StoreError:
                continue
        return added

    def counts(self) -> dict[str, int]:
        with self._lock:
            ready = self._db.execute("SELECT COUNT(*) FROM turns WHERE state='ready'").fetchone()[0]
            running = self._db.execute("SELECT COUNT(*) FROM attempts WHERE state='accepted'").fetchone()[0]
        return {"ready_turns": int(ready), "running_attempts": int(running),
                "uncertain_attempts": 0, "rebind_required": 0}
