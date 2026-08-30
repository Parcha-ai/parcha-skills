"""The sole schema-18 domain runtime.

Drives the durable domain — endpoints, thread bindings, queued turns, fenced
endpoint leases, native attempts, and driver receipts — through single
`BEGIN IMMEDIATE` transactions. The schema's constraints are the state
machine; this module is the only writer path for schema 18 and adds no shadow
state. Completion is owned by driver receipts, never by a model instruction:
only an exact-turn receipt with the attempt's request identity and the live
lease fence can advance or terminalize an attempt, and possible execution is
never replayed.

Herdr and Zellij endpoints are admitted for bookkeeping but are ineligible
for automatic scheduling until their upstreams prove exact-turn lifecycle
receipts. The shipped schema-17 broker does not use this module; it is
exercised against v18 test stores and rehearsal copies only.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import os
import sqlite3
import time
from pathlib import Path
from typing import Any, Iterator

import domain_schema


SCHEMA_VERSION = domain_schema.SCHEMA_VERSION
# Only drivers that prove exact-turn lifecycle receipts may be auto-scheduled.
AUTO_SCHEDULABLE_ENDPOINT_KINDS = frozenset({"detached_native", "hermes_continuation"})
DRIVER_KIND_BY_ENDPOINT_KIND = {
    "zellij_pane": "zellij",
    "herdr_agent": "herdr",
    "detached_native": "detached_native",
    "hermes_continuation": "detached_native",
}
# The one silence token. Anything else — including empty output — is not silence.
NO_REPLY_TOKEN = "NO_REPLY"
TERMINAL_ATTEMPT_STATES = frozenset({
    "completed_with_response",
    "no_reply",
    "cancelled",
    "failed_before_start",
    "failed",
    "operator_completed",
    "operator_abandoned",
})
_SUBMIT_RECEIPT_STATES = frozenset({
    "not_started",
    "accepted",
    "running",
    "completed_with_response",
    "no_reply",
    "failed",
    "cancelled",
    "uncertain",
})
_CANCEL_RECEIPT_STATES = frozenset({"not_started", "cancelled", "uncertain"})
DEFAULT_LEASE_TTL_SECONDS = 1800
DEFAULT_MAX_TURNS_PER_ATTEMPT = 8


class DomainRuntimeError(RuntimeError):
    def __init__(self, code: str, message: str | None = None):
        super().__init__(message or code)
        self.code = code


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _sha256_json(value: Any) -> str:
    return _sha256_text(
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    )


def _now_text(offset_seconds: int = 0) -> str:
    return time.strftime(
        "%Y-%m-%d %H:%M:%S",
        time.gmtime(time.time() + offset_seconds),
    )


def is_no_reply(response_text: str) -> bool:
    return response_text.strip() == NO_REPLY_TOKEN


class DomainRuntime:
    def __init__(self, path: Path):
        self.path = Path(path)
        if not self.path.is_absolute():
            raise DomainRuntimeError(
                "database_path_not_absolute",
                "the domain database path must be absolute",
            )
        with self._transaction() as db:
            version = int(db.execute("PRAGMA user_version").fetchone()[0])
            if version != SCHEMA_VERSION:
                raise DomainRuntimeError(
                    "schema_unsupported",
                    f"DomainRuntime requires schema {SCHEMA_VERSION}, found {version}",
                )

    @contextlib.contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        db = sqlite3.connect(self.path, timeout=30)
        db.row_factory = sqlite3.Row
        try:
            db.execute("PRAGMA foreign_keys=ON")
            db.execute("PRAGMA busy_timeout=30000")
            db.execute("BEGIN IMMEDIATE")
            yield db
            db.commit()
        except BaseException:
            db.rollback()
            raise
        finally:
            db.close()

    # -- endpoint and binding registration ---------------------------------

    def register_endpoint(
        self,
        *,
        endpoint_key: str,
        endpoint_kind: str,
        source_kind: str,
        source_json: str,
        ref_version: int,
        descriptor: domain_schema.SecurityDomainDescriptor,
        capabilities: tuple[str, ...] = (),
    ) -> dict[str, Any]:
        descriptor.validate()
        if endpoint_kind not in DRIVER_KIND_BY_ENDPOINT_KIND:
            raise DomainRuntimeError("endpoint_kind_unknown")
        if not endpoint_key:
            raise DomainRuntimeError("endpoint_key_required")
        domain_id = descriptor.security_domain_id
        endpoint_id = "end_" + _sha256_text(f"{domain_id}:{endpoint_key}")[:24]
        with self._transaction() as db:
            existing = db.execute(
                "SELECT * FROM endpoints WHERE endpoint_id=?",
                (endpoint_id,),
            ).fetchone()
            if existing is not None:
                if (
                    existing["endpoint_key"] != endpoint_key
                    or existing["security_domain_id"] != domain_id
                    or existing["endpoint_kind"] != endpoint_kind
                ):
                    raise DomainRuntimeError("endpoint_identity_conflict")
                return self._endpoint_view(existing)
            db.execute(
                """
                INSERT INTO endpoints(
                  endpoint_id,endpoint_key,endpoint_kind,source_kind,source_json,
                  ref_version,incarnation,security_domain_id,instance_uid,
                  workspace_id,persona_id,authorized_owners_json,
                  authorized_owners_hash,policy_generation,capabilities_json,
                  state,next_lease_fence
                ) VALUES(?,?,?,?,?,?,1,?,?,?,?,?,?,?,?,'ready',0)
                """,
                (
                    endpoint_id,
                    endpoint_key,
                    endpoint_kind,
                    source_kind,
                    source_json,
                    ref_version,
                    domain_id,
                    descriptor.instance_uid,
                    descriptor.workspace_id,
                    descriptor.persona_id,
                    json.dumps(
                        list(descriptor.canonical_owner_ids),
                        separators=(",", ":"),
                    ),
                    descriptor.authorized_owners_hash,
                    descriptor.policy_generation,
                    json.dumps(sorted(capabilities), separators=(",", ":")),
                ),
            )
            for owner in descriptor.canonical_owner_ids:
                db.execute(
                    """
                    INSERT INTO endpoint_authorized_owners(
                      endpoint_id,security_domain_id,owner_user_id
                    ) VALUES(?,?,?)
                    """,
                    (endpoint_id, domain_id, owner),
                )
            created = db.execute(
                "SELECT * FROM endpoints WHERE endpoint_id=?",
                (endpoint_id,),
            ).fetchone()
            return self._endpoint_view(created)

    @staticmethod
    def _endpoint_view(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "endpoint_id": row["endpoint_id"],
            "endpoint_kind": row["endpoint_kind"],
            "state": row["state"],
            "incarnation": row["incarnation"],
            "security_domain_id": row["security_domain_id"],
        }

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
            raise DomainRuntimeError("idempotency_key_required")
        request_hash = _sha256_json({
            "endpoint_id": endpoint_id,
            "team_id": team_id,
            "channel_id": channel_id,
            "thread_ts": thread_ts or "",
            "owner_user_id": owner_user_id,
            "idempotency_key": idempotency_key,
        })
        with self._transaction() as db:
            existing = db.execute(
                "SELECT * FROM thread_bindings WHERE idempotency_key=?",
                (idempotency_key,),
            ).fetchone()
            if existing is not None:
                if existing["request_hash"] != request_hash:
                    raise DomainRuntimeError("idempotency_conflict")
                return self._binding_view(existing)
            endpoint = db.execute(
                "SELECT * FROM endpoints WHERE endpoint_id=?",
                (endpoint_id,),
            ).fetchone()
            if endpoint is None:
                raise DomainRuntimeError("endpoint_unknown")
            if endpoint["state"] != "ready":
                raise DomainRuntimeError("endpoint_not_ready")
            binding_id = "bnd_" + _sha256_text(request_hash)[:24]
            state = "active" if thread_ts else "pending_root"
            db.execute(
                """
                INSERT INTO thread_bindings(
                  binding_id,endpoint_id,security_domain_id,team_id,channel_id,
                  thread_ts,owner_user_id,idempotency_key,request_hash,
                  generation,state,thread_claim_generation
                ) VALUES(?,?,?,?,?,?,?,?,?,1,?,?)
                """,
                (
                    binding_id,
                    endpoint_id,
                    endpoint["security_domain_id"],
                    team_id,
                    channel_id,
                    thread_ts,
                    owner_user_id,
                    idempotency_key,
                    request_hash,
                    state,
                    1 if thread_ts else None,
                ),
            )
            created = db.execute(
                "SELECT * FROM thread_bindings WHERE binding_id=?",
                (binding_id,),
            ).fetchone()
            return self._binding_view(created)

    @staticmethod
    def _binding_view(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "binding_id": row["binding_id"],
            "endpoint_id": row["endpoint_id"],
            "state": row["state"],
            "generation": row["generation"],
            "team_id": row["team_id"],
            "channel_id": row["channel_id"],
            "thread_ts": row["thread_ts"],
        }

    def activate_binding(self, binding_id: str, thread_ts: str) -> dict[str, Any]:
        if not thread_ts:
            raise DomainRuntimeError("thread_ts_required")
        with self._transaction() as db:
            binding = db.execute(
                "SELECT * FROM thread_bindings WHERE binding_id=?",
                (binding_id,),
            ).fetchone()
            if binding is None:
                raise DomainRuntimeError("binding_unknown")
            if binding["state"] == "active":
                if binding["thread_ts"] != thread_ts:
                    raise DomainRuntimeError("thread_claim_conflict")
                return self._binding_view(binding)
            if binding["state"] != "pending_root":
                raise DomainRuntimeError("binding_not_claimable")
            db.execute(
                """
                UPDATE thread_bindings
                SET thread_ts=?,state='active',
                    thread_claim_generation=generation,
                    updated_at=CURRENT_TIMESTAMP
                WHERE binding_id=? AND state='pending_root'
                """,
                (thread_ts, binding_id),
            )
            updated = db.execute(
                "SELECT * FROM thread_bindings WHERE binding_id=?",
                (binding_id,),
            ).fetchone()
            return self._binding_view(updated)

    # -- turn admission -----------------------------------------------------

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
        payload_sha256: str | None = None,
        payload_bytes: int | None = None,
    ) -> dict[str, Any]:
        if not event_key:
            raise DomainRuntimeError("event_key_required")
        if mutation_kind in {"create", "edit"}:
            if payload_inline is not None and payload_sha256 is None:
                payload_sha256 = _sha256_text(payload_inline)
                payload_bytes = len(payload_inline.encode())
        with self._transaction() as db:
            existing = db.execute(
                "SELECT * FROM queued_turns WHERE event_key=?",
                (event_key,),
            ).fetchone()
            if existing is not None:
                if existing["binding_id"] != binding_id:
                    raise DomainRuntimeError("event_binding_conflict")
                return self._turn_view(existing)
            binding = db.execute(
                "SELECT * FROM thread_bindings WHERE binding_id=?",
                (binding_id,),
            ).fetchone()
            if binding is None:
                raise DomainRuntimeError("binding_unknown")
            if binding["state"] not in {"active", "pending_root"}:
                raise DomainRuntimeError("binding_not_admitting")
            db.execute(
                """
                INSERT INTO queued_turns(
                  event_key,binding_id,binding_generation,ordered_at,
                  mutation_kind,mutation_target_key,payload_inline,payload_ref,
                  payload_sha256,payload_bytes,state
                ) VALUES(?,?,?,?,?,?,?,?,?,?,'ready')
                """,
                (
                    event_key,
                    binding_id,
                    binding["generation"],
                    ordered_at,
                    mutation_kind,
                    mutation_target_key,
                    payload_inline,
                    payload_ref,
                    payload_sha256,
                    payload_bytes,
                ),
            )
            created = db.execute(
                "SELECT * FROM queued_turns WHERE event_key=?",
                (event_key,),
            ).fetchone()
            return self._turn_view(created)

    @staticmethod
    def _turn_view(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "event_key": row["event_key"],
            "binding_id": row["binding_id"],
            "binding_generation": row["binding_generation"],
            "state": row["state"],
            "ordered_at": row["ordered_at"],
            "error_code": row["error_code"],
        }

    # -- scheduling ----------------------------------------------------------

    def schedule_next(
        self,
        endpoint_id: str,
        *,
        lease_ttl_seconds: int = DEFAULT_LEASE_TTL_SECONDS,
        max_turns: int = DEFAULT_MAX_TURNS_PER_ATTEMPT,
    ) -> dict[str, Any] | None:
        """Claim the fairest ready work as one prepared attempt with a lease.

        Fairness: the eligible binding is the endpoint's active binding whose
        oldest ready turn is globally oldest; one attempt claims consecutive
        ready turns of that binding only. Returns None when there is nothing
        eligible or another attempt holds the endpoint lease.
        """
        if max_turns < 1:
            raise DomainRuntimeError("max_turns_invalid")
        with self._transaction() as db:
            endpoint = db.execute(
                "SELECT * FROM endpoints WHERE endpoint_id=?",
                (endpoint_id,),
            ).fetchone()
            if endpoint is None:
                raise DomainRuntimeError("endpoint_unknown")
            if endpoint["state"] != "ready":
                return None
            if endpoint["endpoint_kind"] not in AUTO_SCHEDULABLE_ENDPOINT_KINDS:
                # Fail closed: these drivers cannot prove exact-turn receipts.
                return None
            open_lease = db.execute(
                """
                SELECT attempt_id FROM endpoint_leases
                WHERE endpoint_id=? AND released_at IS NULL
                """,
                (endpoint_id,),
            ).fetchone()
            if open_lease is not None:
                return None
            head = db.execute(
                """
                SELECT turn.binding_id,MIN(turn.ordered_at) AS oldest,
                       MIN(turn.event_key) AS tiebreak
                FROM queued_turns AS turn
                JOIN thread_bindings AS binding
                  ON binding.binding_id=turn.binding_id
                WHERE binding.endpoint_id=? AND binding.state='active'
                  AND turn.state='ready'
                  AND turn.binding_generation=binding.generation
                GROUP BY turn.binding_id
                ORDER BY oldest,tiebreak
                LIMIT 1
                """,
                (endpoint_id,),
            ).fetchone()
            if head is None:
                return None
            binding_id = head["binding_id"]
            binding = db.execute(
                "SELECT * FROM thread_bindings WHERE binding_id=?",
                (binding_id,),
            ).fetchone()
            turns = db.execute(
                """
                SELECT * FROM queued_turns
                WHERE binding_id=? AND state='ready' AND binding_generation=?
                ORDER BY ordered_at,event_key
                LIMIT ?
                """,
                (binding_id, binding["generation"], max_turns),
            ).fetchall()
            fence = int(endpoint["next_lease_fence"]) + 1
            db.execute(
                """
                UPDATE endpoints SET next_lease_fence=?,updated_at=CURRENT_TIMESTAMP
                WHERE endpoint_id=? AND next_lease_fence=?
                """,
                (fence, endpoint_id, endpoint["next_lease_fence"]),
            )
            attempt_id = "att_" + os.urandom(16).hex()
            reply_token = os.urandom(32).hex()
            driver_request_id = "req_" + os.urandom(16).hex()
            request_hash = _sha256_json({
                "attempt_id": attempt_id,
                "binding_id": binding_id,
                "binding_generation": binding["generation"],
                "event_keys": [turn["event_key"] for turn in turns],
            })
            now = _now_text()
            db.execute(
                """
                INSERT INTO endpoint_leases(
                  attempt_id,endpoint_id,endpoint_incarnation,fence,
                  acquired_at,expires_at
                ) VALUES(?,?,?,?,?,?)
                """,
                (
                    attempt_id,
                    endpoint_id,
                    endpoint["incarnation"],
                    fence,
                    now,
                    _now_text(max(1, lease_ttl_seconds)),
                ),
            )
            db.execute(
                """
                INSERT INTO native_attempts(
                  attempt_id,endpoint_id,binding_id,binding_generation,
                  driver_kind,driver_request_id,driver_request_hash,
                  reply_token_hash,state
                ) VALUES(?,?,?,?,?,?,?,?,'prepared')
                """,
                (
                    attempt_id,
                    endpoint_id,
                    binding_id,
                    binding["generation"],
                    DRIVER_KIND_BY_ENDPOINT_KIND[endpoint["endpoint_kind"]],
                    driver_request_id,
                    request_hash,
                    _sha256_text(reply_token),
                ),
            )
            for ordinal, turn in enumerate(turns):
                db.execute(
                    """
                    INSERT INTO native_attempt_turns(
                      attempt_id,ordinal,event_key,binding_id,
                      turn_binding_generation
                    ) VALUES(?,?,?,?,?)
                    """,
                    (
                        attempt_id,
                        ordinal,
                        turn["event_key"],
                        binding_id,
                        turn["binding_generation"],
                    ),
                )
            return {
                "attempt_id": attempt_id,
                "endpoint_id": endpoint_id,
                "binding_id": binding_id,
                "binding_generation": binding["generation"],
                "lease_fence": fence,
                "driver_kind": DRIVER_KIND_BY_ENDPOINT_KIND[endpoint["endpoint_kind"]],
                "driver_request_id": driver_request_id,
                "driver_request_hash": request_hash,
                "reply_token": reply_token,
                "event_keys": [turn["event_key"] for turn in turns],
            }

    def mark_submitting(self, attempt_id: str) -> None:
        with self._transaction() as db:
            attempt = self._load_attempt(db, attempt_id)
            if attempt["state"] == "submitting":
                return
            if attempt["state"] != "prepared":
                raise DomainRuntimeError("attempt_not_prepared")
            db.execute(
                """
                UPDATE native_attempts
                SET state='submitting',submitted_at=?,updated_at=CURRENT_TIMESTAMP
                WHERE attempt_id=? AND state='prepared'
                """,
                (_now_text(), attempt_id),
            )

    def request_cancel(self, attempt_id: str, cancel_request_id: str) -> dict[str, Any]:
        if not cancel_request_id:
            raise DomainRuntimeError("cancel_request_id_required")
        with self._transaction() as db:
            attempt = self._load_attempt(db, attempt_id)
            if attempt["state"] in TERMINAL_ATTEMPT_STATES:
                raise DomainRuntimeError("attempt_terminal")
            existing = attempt["cancel_request_id"]
            if existing is not None:
                if existing != cancel_request_id:
                    raise DomainRuntimeError("cancel_identity_conflict")
                return {"attempt_id": attempt_id, "cancel_request_id": existing}
            db.execute(
                """
                UPDATE native_attempts
                SET cancel_request_id=?,cancel_request_hash=?,
                    updated_at=CURRENT_TIMESTAMP
                WHERE attempt_id=? AND cancel_request_id IS NULL
                """,
                (
                    cancel_request_id,
                    _sha256_json({
                        "attempt_id": attempt_id,
                        "cancel_request_id": cancel_request_id,
                    }),
                    attempt_id,
                ),
            )
            return {"attempt_id": attempt_id, "cancel_request_id": cancel_request_id}

    # -- driver receipts and completion --------------------------------------

    @staticmethod
    def _load_attempt(db: sqlite3.Connection, attempt_id: str) -> sqlite3.Row:
        attempt = db.execute(
            "SELECT * FROM native_attempts WHERE attempt_id=?",
            (attempt_id,),
        ).fetchone()
        if attempt is None:
            raise DomainRuntimeError("attempt_unknown")
        return attempt

    def record_driver_receipt(
        self,
        *,
        attempt_id: str,
        receipt_id: str,
        lease_fence: int,
        sequence: int,
        driver_incarnation: str,
        operation: str,
        request_id: str,
        watch_cursor: str,
        state: str,
        observed_at: str,
        response_ref: str | None = None,
        response_sha256: str | None = None,
        response_bytes: int | None = None,
        error_code: str | None = None,
    ) -> dict[str, Any]:
        """Apply one exact-turn driver observation; the only completion path."""
        if operation not in {"submit", "cancel"}:
            raise DomainRuntimeError("receipt_operation_unknown")
        allowed = (
            _SUBMIT_RECEIPT_STATES if operation == "submit" else _CANCEL_RECEIPT_STATES
        )
        if state not in allowed:
            raise DomainRuntimeError("receipt_state_invalid")
        if state == "completed_with_response" and (
            not response_ref or not response_sha256 or response_bytes is None
        ):
            raise DomainRuntimeError("response_evidence_missing")
        with self._transaction() as db:
            attempt = self._load_attempt(db, attempt_id)
            replay = db.execute(
                "SELECT * FROM driver_receipts WHERE receipt_id=?",
                (receipt_id,),
            ).fetchone()
            if replay is not None:
                # A replay is the SAME observation arriving twice, which is
                # safe to absorb. A different observation reusing the id is a
                # conflict and must never be swallowed: driver receipt ids are
                # derived deterministically from (attempt, sequence), so two
                # racing drivers can collide here, and silently discarding the
                # loser would drop a terminal outcome and wedge the endpoint.
                if (
                    replay["attempt_id"] != attempt_id
                    or replay["request_hash"]
                    != self._receipt_request_hash(attempt, operation)
                    or int(replay["sequence"]) != sequence
                    or str(replay["state"]) != state
                    or str(replay["operation"]) != operation
                    or str(replay["request_id"]) != request_id
                    or str(replay["watch_cursor"]) != watch_cursor
                ):
                    raise DomainRuntimeError("receipt_identity_conflict")
                return {"attempt_id": attempt_id, "state": attempt["state"], "replay": True}
            if attempt["state"] in TERMINAL_ATTEMPT_STATES:
                raise DomainRuntimeError("attempt_terminal")
            lease = db.execute(
                """
                SELECT * FROM endpoint_leases
                WHERE attempt_id=? AND released_at IS NULL
                """,
                (attempt_id,),
            ).fetchone()
            if lease is None:
                raise DomainRuntimeError("lease_not_open")
            if int(lease["fence"]) != int(lease_fence):
                raise DomainRuntimeError("stale_lease_fence")
            expected_request_id = (
                attempt["driver_request_id"]
                if operation == "submit"
                else attempt["cancel_request_id"]
            )
            if expected_request_id is None or request_id != expected_request_id:
                raise DomainRuntimeError("receipt_request_mismatch")
            if sequence != int(attempt["last_driver_sequence"]) + 1:
                raise DomainRuntimeError("receipt_sequence_gap")
            db.execute(
                """
                INSERT INTO driver_receipts(
                  receipt_id,attempt_id,endpoint_id,lease_fence,sequence,
                  driver_kind,driver_incarnation,operation,request_id,
                  request_hash,watch_cursor,state,response_ref,response_sha256,
                  error_code,observed_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    receipt_id,
                    attempt_id,
                    attempt["endpoint_id"],
                    lease_fence,
                    sequence,
                    attempt["driver_kind"],
                    driver_incarnation,
                    operation,
                    request_id,
                    self._receipt_request_hash(attempt, operation),
                    watch_cursor,
                    state,
                    response_ref if state == "completed_with_response" else None,
                    response_sha256 if state == "completed_with_response" else None,
                    error_code,
                    observed_at,
                ),
            )
            db.execute(
                """
                UPDATE native_attempts
                SET receipt_cursor=?,last_driver_receipt_id=?,
                    last_driver_sequence=?,updated_at=CURRENT_TIMESTAMP
                WHERE attempt_id=? AND last_driver_sequence=?
                """,
                (
                    watch_cursor,
                    receipt_id,
                    sequence,
                    attempt_id,
                    attempt["last_driver_sequence"],
                ),
            )
            new_state = self._apply_receipt_state(
                db,
                attempt,
                operation=operation,
                receipt_state=state,
                response_ref=response_ref,
                response_sha256=response_sha256,
                response_bytes=response_bytes,
                error_code=error_code,
            )
            return {"attempt_id": attempt_id, "state": new_state, "replay": False}

    @staticmethod
    def _receipt_request_hash(attempt: sqlite3.Row, operation: str) -> str:
        if operation == "submit":
            return attempt["driver_request_hash"]
        return attempt["cancel_request_hash"]

    # Legal hops mirror the schema's `native_attempt_forward_state` trigger.
    _FORWARD_HOPS = {
        "prepared": frozenset({"submitting", "failed_before_start", "cancelled"}),
        "submitting": frozenset({
            "accepted", "uncertain", "failed_before_start", "failed", "cancelled",
        }),
        "accepted": frozenset({
            "uncertain", "completed_with_response", "no_reply", "failed", "cancelled",
        }),
        "uncertain": frozenset({
            "accepted", "completed_with_response", "no_reply", "failed_before_start",
            "failed", "cancelled", "operator_completed", "operator_abandoned",
        }),
    }

    def _hops_to(self, current: str, target: str) -> list[str]:
        """Shortest legal transition path; a driver observation of a later
        state implies the intermediate ones occurred."""
        if current == target:
            return []
        if target in self._FORWARD_HOPS.get(current, frozenset()):
            return [target]
        for step in ("submitting", "accepted"):
            if step in self._FORWARD_HOPS.get(current, frozenset()):
                tail = self._hops_to(step, target)
                if tail is not None:
                    return [step, *tail]
        return None  # type: ignore[return-value]

    def _hop(
        self,
        db: sqlite3.Connection,
        attempt_id: str,
        to_state: str,
        *,
        error_code: str | None = None,
        response: tuple[str, str, int] | None = None,
    ) -> None:
        now = _now_text()
        needs_submitted = to_state not in {"prepared", "failed_before_start", "cancelled"}
        needs_accepted = to_state in {"accepted", "completed_with_response", "no_reply"}
        terminal = to_state in TERMINAL_ATTEMPT_STATES
        # Response evidence is CHECK-bound to the completed state, so it must
        # land in the same statement as the terminal transition.
        response_clause = (
            "response_ref=?,response_sha256=?,response_bytes=?," if response else ""
        )
        parameters: list[Any] = [to_state]
        if needs_submitted:
            parameters.append(now)
        if needs_accepted:
            parameters.append(now)
        if terminal:
            parameters.append(now)
        if response:
            parameters.extend(response)
        parameters.extend((error_code, attempt_id))
        db.execute(
            f"""
            UPDATE native_attempts
            SET state=?,
                submitted_at={"COALESCE(submitted_at,?)" if needs_submitted else "submitted_at"},
                accepted_at={"COALESCE(accepted_at,?)" if needs_accepted else "accepted_at"},
                terminal_at={"?" if terminal else "terminal_at"},
                {response_clause}
                error_code=?,
                updated_at=CURRENT_TIMESTAMP
            WHERE attempt_id=?
            """,
            tuple(parameters),
        )

    def _advance_attempt(
        self,
        db: sqlite3.Connection,
        attempt: sqlite3.Row,
        target: str,
        *,
        error_code: str | None,
        response: tuple[str, str, int] | None = None,
    ) -> None:
        hops = self._hops_to(attempt["state"], target)
        if hops is None:
            raise DomainRuntimeError(
                "receipt_state_contradiction",
                f"a {attempt['state']} attempt cannot reach {target}",
            )
        for hop in hops:
            self._hop(
                db,
                attempt["attempt_id"],
                hop,
                error_code=error_code if hop == target else attempt["error_code"],
                response=response if hop == target else None,
            )

    def _apply_receipt_state(
        self,
        db: sqlite3.Connection,
        attempt: sqlite3.Row,
        *,
        operation: str,
        receipt_state: str,
        response_ref: str | None,
        response_sha256: str | None,
        response_bytes: int | None,
        error_code: str | None,
    ) -> str:
        attempt_id = attempt["attempt_id"]
        current = attempt["state"]
        if receipt_state == "running":
            if current == "prepared":
                self._advance_attempt(db, attempt, "submitting", error_code=None)
                return "submitting"
            return current
        if receipt_state == "accepted":
            self._advance_attempt(db, attempt, "accepted", error_code=None)
            return "accepted"
        if receipt_state == "uncertain":
            # Possible execution: hold the lease open as the visible blocker
            # and never replay. Only driver reconciliation or an operator
            # resolution moves this attempt again.
            self._advance_attempt(
                db,
                attempt,
                "uncertain",
                error_code=error_code or "native_execution_uncertain",
            )
            return "uncertain"
        if receipt_state == "not_started":
            if operation == "cancel":
                return self._terminalize(
                    db,
                    attempt,
                    "cancelled",
                    turn_outcome="cancelled",
                    error_code=error_code,
                )
            if current == "accepted":
                raise DomainRuntimeError(
                    "receipt_state_contradiction",
                    "an accepted attempt cannot report not_started",
                )
            return self._terminalize(
                db,
                attempt,
                "failed_before_start",
                turn_outcome="requeue",
                error_code=error_code or "driver_not_started",
            )
        if receipt_state == "completed_with_response":
            if not response_ref or not response_sha256 or response_bytes is None:
                raise DomainRuntimeError("response_evidence_missing")
            return self._terminalize(
                db,
                attempt,
                "completed_with_response",
                turn_outcome="completed",
                error_code=None,
                response=(response_ref, response_sha256, response_bytes),
            )
        if receipt_state == "no_reply":
            return self._terminalize(
                db,
                attempt,
                "no_reply",
                turn_outcome="completed",
                error_code=None,
            )
        if receipt_state == "failed":
            return self._terminalize(
                db,
                attempt,
                "failed",
                turn_outcome="completed",
                error_code=error_code or "driver_failed",
            )
        if receipt_state == "cancelled":
            return self._terminalize(
                db,
                attempt,
                "cancelled",
                turn_outcome="cancelled",
                error_code=error_code,
            )
        raise DomainRuntimeError("receipt_state_invalid")

    def _terminalize(
        self,
        db: sqlite3.Connection,
        attempt: sqlite3.Row,
        terminal_state: str,
        *,
        turn_outcome: str,
        error_code: str | None,
        response: tuple[str, str, int] | None = None,
    ) -> str:
        """Terminal attempt state, turn outcomes, and lease release: one commit."""
        attempt_id = attempt["attempt_id"]
        now = _now_text()
        self._advance_attempt(
            db,
            attempt,
            terminal_state,
            error_code=error_code,
            response=response,
        )
        if turn_outcome == "requeue":
            # Nothing ran: the claimed turns were never taken out of 'ready',
            # so they stay schedulable. Membership rows remain as the dense
            # historical record of what this attempt had claimed.
            pass
        else:
            db.execute(
                """
                UPDATE queued_turns
                SET state=?,terminal_at=?,error_code=?,updated_at=CURRENT_TIMESTAMP
                WHERE state='ready' AND event_key IN (
                  SELECT event_key FROM native_attempt_turns WHERE attempt_id=?
                )
                """,
                (
                    "cancelled" if turn_outcome == "cancelled" else "completed",
                    now,
                    error_code,
                    attempt_id,
                ),
            )
        db.execute(
            """
            UPDATE endpoint_leases
            SET released_at=?,release_reason=?
            WHERE attempt_id=? AND released_at IS NULL
            """,
            (now, terminal_state, attempt_id),
        )
        return terminal_state

    # -- recovery -------------------------------------------------------------

    def mark_uncertain(self, attempt_id: str, error_code: str) -> None:
        """Driver recovery: proof of outcome was lost after possible execution."""
        if not error_code:
            raise DomainRuntimeError("error_code_required")
        with self._transaction() as db:
            attempt = self._load_attempt(db, attempt_id)
            if attempt["state"] in TERMINAL_ATTEMPT_STATES:
                raise DomainRuntimeError("attempt_terminal")
            if attempt["state"] == "prepared":
                raise DomainRuntimeError(
                    "attempt_not_submitted",
                    "a prepared attempt cannot be uncertain; nothing was spawned",
                )
            self._advance_attempt(db, attempt, "uncertain", error_code=error_code)

    # A prepared attempt that provably never spawned is terminalized by the
    # driver's recovery pass through record_driver_receipt(state='not_started'):
    # the schema's terminal-proof guard demands a fenced driver receipt for
    # every terminal transition, so no receipt-free recovery path exists.

    def attempt_status(self, attempt_id: str) -> dict[str, Any]:
        with self._transaction() as db:
            attempt = self._load_attempt(db, attempt_id)
            lease = db.execute(
                "SELECT fence,released_at FROM endpoint_leases WHERE attempt_id=?",
                (attempt_id,),
            ).fetchone()
            return {
                "attempt_id": attempt_id,
                "state": attempt["state"],
                "binding_id": attempt["binding_id"],
                "binding_generation": attempt["binding_generation"],
                "driver_kind": attempt["driver_kind"],
                "error_code": attempt["error_code"],
                "lease_fence": int(lease["fence"]) if lease else None,
                "lease_open": bool(lease and lease["released_at"] is None),
                "last_driver_sequence": attempt["last_driver_sequence"],
            }
