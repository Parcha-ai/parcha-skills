from __future__ import annotations

import hashlib
import json
import sqlite3
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any

from domain_schema import SCHEMA_VERSION, invariant_violations


@dataclass(frozen=True)
class BlockingCondition:
    condition_id: str
    revision: str
    category: str
    reason_code: str
    scope: str
    workspace_id: str
    security_domain_id: str
    endpoint_id: str | None
    binding_id: str | None
    attempt_id: str | None
    blocked_since: str
    age_seconds: int
    blocked_turn_count: int
    impacts_readiness: bool
    impacts_migration: bool
    operator_resolvable: bool
    allowed_actions: tuple[str, ...]
    next_action_code: str

    def as_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["allowed_actions"] = list(self.allowed_actions)
        return value


@dataclass(frozen=True)
class BlockingSnapshot:
    as_of: str
    snapshot_revision: str
    conditions: tuple[BlockingCondition, ...]

    @property
    def summary(self) -> dict[str, Any]:
        readiness = [condition for condition in self.conditions if condition.impacts_readiness]
        return {
            "condition_count": len(self.conditions),
            "readiness_blocker_count": len(readiness),
            "operator_resolvable_count": sum(
                1 for condition in self.conditions if condition.operator_resolvable
            ),
            "blocked_turn_count": sum(
                condition.blocked_turn_count for condition in self.conditions
            ),
            "ready": not readiness,
        }

    def as_dict(self) -> dict[str, Any]:
        return {
            "as_of": self.as_of,
            "snapshot_revision": self.snapshot_revision,
            "summary": self.summary,
            "conditions": [condition.as_dict() for condition in self.conditions],
        }


@dataclass(frozen=True)
class ControlCapabilities:
    operator_resolution: bool = False


@dataclass(frozen=True)
class OperatorResolutionRequest:
    condition_id: str
    expected_revision: str
    action: str
    authority_receipt_id: str
    operator_principal_hash: str
    evidence_ref: str
    evidence_sha256: str


@dataclass(frozen=True)
class OperatorResolutionResult:
    status: str
    attempt_id: str
    action: str
    authority_receipt_id: str

    def as_dict(self) -> dict[str, str]:
        return asdict(self)


class DomainControlError(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


AuthorityVerifier = Callable[
    [OperatorResolutionRequest, BlockingCondition], bool
]


def _sha256_json(value: Any) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def _condition_id(kind: str, *parts: Any) -> str:
    return "blk_" + _sha256_json([kind, *parts])[:24]


def _canonical_now(now: datetime | None) -> tuple[datetime, str]:
    value = now or datetime.now(UTC)
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    value = value.astimezone(UTC).replace(microsecond=0)
    return value, value.strftime("%Y-%m-%d %H:%M:%S")


def _parse_timestamp(value: str) -> datetime:
    candidate = value.strip()
    if candidate.endswith("Z"):
        candidate = candidate[:-1] + "+00:00"
    parsed = datetime.fromisoformat(candidate)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _age_seconds(now: datetime, blocked_since: str) -> int:
    try:
        return max(0, int((now - _parse_timestamp(blocked_since)).total_seconds()))
    except (TypeError, ValueError):
        return 0


def _require_schema(connection: sqlite3.Connection) -> None:
    observed = int(connection.execute("PRAGMA user_version").fetchone()[0])
    if observed != SCHEMA_VERSION:
        raise DomainControlError(
            "schema_incompatible",
            f"domain control requires schema {SCHEMA_VERSION}; observed {observed}",
        )


def _ready_turn_count(connection: sqlite3.Connection, endpoint_id: str) -> int:
    return int(
        connection.execute(
            """
            SELECT COUNT(*)
            FROM queued_turns AS turn
            JOIN thread_bindings AS binding ON binding.binding_id=turn.binding_id
            WHERE binding.endpoint_id=? AND turn.state='ready'
            """,
            (endpoint_id,),
        ).fetchone()[0]
    )


def _native_uncertainty_conditions(
    connection: sqlite3.Connection,
    now: datetime,
    capabilities: ControlCapabilities,
) -> list[BlockingCondition]:
    rows = connection.execute(
        """
        SELECT attempt.attempt_id,attempt.binding_id,attempt.binding_generation,
               attempt.state,attempt.error_code,attempt.submitted_at,
               attempt.created_at,attempt.last_driver_sequence,
               lease.endpoint_id,lease.fence,lease.endpoint_incarnation,
               lease.acquired_at,lease.expires_at,endpoint.workspace_id,
               endpoint.security_domain_id,endpoint.policy_generation
        FROM native_attempts AS attempt
        JOIN endpoint_leases AS lease ON lease.attempt_id=attempt.attempt_id
        JOIN endpoints AS endpoint ON endpoint.endpoint_id=lease.endpoint_id
        WHERE attempt.state='uncertain' AND lease.released_at IS NULL
        ORDER BY COALESCE(attempt.submitted_at,attempt.created_at),attempt.attempt_id
        """
    ).fetchall()
    conditions: list[BlockingCondition] = []
    for row in rows:
        blocked_since = str(row["submitted_at"] or row["created_at"])
        revision_material = {
            "attempt_id": row["attempt_id"],
            "state": row["state"],
            "lease_fence": row["fence"],
            "last_driver_sequence": row["last_driver_sequence"],
            "endpoint_incarnation": row["endpoint_incarnation"],
            "binding_generation": row["binding_generation"],
            "policy_generation": row["policy_generation"],
        }
        conditions.append(
            BlockingCondition(
                condition_id=_condition_id(
                    "native_execution_uncertain",
                    row["attempt_id"],
                    row["fence"],
                ),
                revision=_sha256_json(revision_material),
                category="native_execution",
                reason_code=str(row["error_code"] or "native_execution_uncertain"),
                scope="endpoint",
                workspace_id=str(row["workspace_id"]),
                security_domain_id=str(row["security_domain_id"]),
                endpoint_id=str(row["endpoint_id"]),
                binding_id=str(row["binding_id"]),
                attempt_id=str(row["attempt_id"]),
                blocked_since=blocked_since,
                age_seconds=_age_seconds(now, blocked_since),
                blocked_turn_count=_ready_turn_count(
                    connection,
                    str(row["endpoint_id"]),
                ),
                impacts_readiness=True,
                impacts_migration=True,
                operator_resolvable=capabilities.operator_resolution,
                allowed_actions=(
                    ("complete", "abandon")
                    if capabilities.operator_resolution
                    else ()
                ),
                next_action_code=(
                    "reconcile_or_resolve_native_execution"
                    if capabilities.operator_resolution
                    else "enable_isolated_operator_authority"
                ),
            )
        )
    return conditions


def _endpoint_conditions(
    connection: sqlite3.Connection,
    now: datetime,
) -> list[BlockingCondition]:
    rows = connection.execute(
        """
        SELECT endpoint_id,workspace_id,security_domain_id,incarnation,
               policy_generation,error_code,updated_at
        FROM endpoints
        WHERE state='rebind_required'
        ORDER BY updated_at,endpoint_id
        """
    ).fetchall()
    return [
        BlockingCondition(
            condition_id=_condition_id(
                "endpoint_rebind_required",
                row["endpoint_id"],
                row["incarnation"],
            ),
            revision=_sha256_json(
                {
                    "endpoint_id": row["endpoint_id"],
                    "incarnation": row["incarnation"],
                    "policy_generation": row["policy_generation"],
                    "error_code": row["error_code"],
                }
            ),
            category="endpoint",
            reason_code=str(row["error_code"] or "endpoint_rebind_required"),
            scope="endpoint",
            workspace_id=str(row["workspace_id"]),
            security_domain_id=str(row["security_domain_id"]),
            endpoint_id=str(row["endpoint_id"]),
            binding_id=None,
            attempt_id=None,
            blocked_since=str(row["updated_at"]),
            age_seconds=_age_seconds(now, str(row["updated_at"])),
            blocked_turn_count=_ready_turn_count(connection, str(row["endpoint_id"])),
            impacts_readiness=True,
            impacts_migration=True,
            operator_resolvable=False,
            allowed_actions=(),
            next_action_code="recapture_endpoint",
        )
        for row in rows
    ]


def _binding_conditions(
    connection: sqlite3.Connection,
    now: datetime,
) -> list[BlockingCondition]:
    rows = connection.execute(
        """
        SELECT binding.binding_id,binding.endpoint_id,binding.generation,
               binding.team_id,binding.security_domain_id,binding.error_code,
               binding.updated_at,COUNT(turn.event_key) AS ready_turn_count,
               endpoint.incarnation,endpoint.policy_generation
        FROM thread_bindings AS binding
        JOIN endpoints AS endpoint ON endpoint.endpoint_id=binding.endpoint_id
        LEFT JOIN queued_turns AS turn
          ON turn.binding_id=binding.binding_id AND turn.state='ready'
        WHERE binding.state='rebind_required' AND endpoint.state!='rebind_required'
        GROUP BY binding.binding_id
        ORDER BY binding.updated_at,binding.binding_id
        """
    ).fetchall()
    return [
        BlockingCondition(
            condition_id=_condition_id(
                "binding_rebind_required",
                row["binding_id"],
                row["generation"],
            ),
            revision=_sha256_json(
                {
                    "binding_id": row["binding_id"],
                    "generation": row["generation"],
                    "endpoint_incarnation": row["incarnation"],
                    "policy_generation": row["policy_generation"],
                    "error_code": row["error_code"],
                }
            ),
            category="binding",
            reason_code=str(row["error_code"] or "binding_rebind_required"),
            scope="binding",
            workspace_id=str(row["team_id"]),
            security_domain_id=str(row["security_domain_id"]),
            endpoint_id=str(row["endpoint_id"]),
            binding_id=str(row["binding_id"]),
            attempt_id=None,
            blocked_since=str(row["updated_at"]),
            age_seconds=_age_seconds(now, str(row["updated_at"])),
            blocked_turn_count=int(row["ready_turn_count"]),
            impacts_readiness=True,
            impacts_migration=True,
            operator_resolvable=False,
            allowed_actions=(),
            next_action_code="rebind_thread",
        )
        for row in rows
    ]


def blocking_snapshot(
    connection: sqlite3.Connection,
    *,
    now: datetime | None = None,
    capabilities: ControlCapabilities | None = None,
) -> BlockingSnapshot:
    _require_schema(connection)
    observed_now, as_of = _canonical_now(now)
    previous_row_factory = connection.row_factory
    connection.row_factory = sqlite3.Row
    connection.execute("SAVEPOINT tether_blocking_snapshot")
    try:
        effective_capabilities = capabilities or ControlCapabilities()
        conditions = [
            *_native_uncertainty_conditions(
                connection,
                observed_now,
                effective_capabilities,
            ),
            *_endpoint_conditions(connection, observed_now),
            *_binding_conditions(connection, observed_now),
        ]
        conditions.sort(key=lambda condition: (condition.blocked_since, condition.condition_id))
        revision = _sha256_json(
            [
                [condition.condition_id, condition.revision]
                for condition in conditions
            ]
        )
        return BlockingSnapshot(as_of, revision, tuple(conditions))
    finally:
        connection.execute("ROLLBACK TO tether_blocking_snapshot")
        connection.execute("RELEASE tether_blocking_snapshot")
        connection.row_factory = previous_row_factory


def _validate_resolution_request(request: OperatorResolutionRequest) -> None:
    if request.action not in {"complete", "abandon"}:
        raise DomainControlError(
            "resolution_action_not_allowed",
            "native execution uncertainty allows only complete or abandon",
        )
    for name, value in (
        ("condition_id", request.condition_id),
        ("expected_revision", request.expected_revision),
        ("authority_receipt_id", request.authority_receipt_id),
        ("operator_principal_hash", request.operator_principal_hash),
        ("evidence_ref", request.evidence_ref),
        ("evidence_sha256", request.evidence_sha256),
    ):
        if not value or len(value) > 4096 or any(ord(character) < 32 for character in value):
            raise DomainControlError("resolution_invalid", f"{name} is invalid")
    if len(request.operator_principal_hash) != 64 or len(request.evidence_sha256) != 64:
        raise DomainControlError(
            "resolution_invalid",
            "operator and evidence digests must be SHA-256 values",
        )


def _resolved_condition(
    connection: sqlite3.Connection,
    attempt_id: str,
    action: str,
) -> BlockingCondition:
    row = connection.execute(
        """
        SELECT attempt.attempt_id,attempt.binding_id,attempt.binding_generation,
               attempt.error_code,attempt.submitted_at,attempt.created_at,
               attempt.last_driver_sequence,lease.endpoint_id,lease.fence,
               lease.endpoint_incarnation,endpoint.workspace_id,
               endpoint.security_domain_id,endpoint.policy_generation
        FROM native_attempts AS attempt
        JOIN endpoint_leases AS lease ON lease.attempt_id=attempt.attempt_id
        JOIN endpoints AS endpoint ON endpoint.endpoint_id=lease.endpoint_id
        WHERE attempt.attempt_id=?
        """,
        (attempt_id,),
    ).fetchone()
    if row is None:
        raise DomainControlError(
            "authority_receipt_conflict",
            "authority receipt refers to missing domain state",
        )
    blocked_since = str(row["submitted_at"] or row["created_at"])
    revision = _sha256_json(
        {
            "attempt_id": row["attempt_id"],
            "state": "uncertain",
            "lease_fence": row["fence"],
            "last_driver_sequence": row["last_driver_sequence"],
            "endpoint_incarnation": row["endpoint_incarnation"],
            "binding_generation": row["binding_generation"],
            "policy_generation": row["policy_generation"],
        }
    )
    return BlockingCondition(
        condition_id=_condition_id(
            "native_execution_uncertain",
            row["attempt_id"],
            row["fence"],
        ),
        revision=revision,
        category="native_execution",
        reason_code="native_execution_resolved",
        scope="endpoint",
        workspace_id=str(row["workspace_id"]),
        security_domain_id=str(row["security_domain_id"]),
        endpoint_id=str(row["endpoint_id"]),
        binding_id=str(row["binding_id"]),
        attempt_id=str(row["attempt_id"]),
        blocked_since=blocked_since,
        age_seconds=0,
        blocked_turn_count=0,
        impacts_readiness=False,
        impacts_migration=False,
        operator_resolvable=False,
        allowed_actions=(action,),
        next_action_code="already_resolved",
    )


def _resolution_replay(
    connection: sqlite3.Connection,
    request: OperatorResolutionRequest,
    verify_authority: AuthorityVerifier,
) -> OperatorResolutionResult | None:
    row = connection.execute(
        """
        SELECT attempt_id,action,authority_receipt_id,operator_principal_hash,
               evidence_ref,evidence_sha256,resolved_at
        FROM operator_resolutions WHERE authority_receipt_id=?
        """,
        (request.authority_receipt_id,),
    ).fetchone()
    if row is None:
        return None
    condition = _resolved_condition(connection, str(row["attempt_id"]), str(row["action"]))
    expected = (
        request.action,
        request.operator_principal_hash,
        request.evidence_ref,
        request.evidence_sha256,
    )
    observed = tuple(str(row[key]) for key in (
        "action",
        "operator_principal_hash",
        "evidence_ref",
        "evidence_sha256",
    ))
    if (
        observed != expected
        or request.condition_id != condition.condition_id
        or request.expected_revision != condition.revision
    ):
        raise DomainControlError(
            "authority_receipt_conflict",
            "authority receipt was already used for a different resolution",
        )
    if not verify_authority(request, condition):
        raise DomainControlError(
            "operator_authority_denied",
            "the operator authority did not approve this resolution",
        )
    return OperatorResolutionResult(
        "already_applied",
        str(row["attempt_id"]),
        str(row["action"]),
        str(row["authority_receipt_id"]),
    )


def resolve_condition(
    connection: sqlite3.Connection,
    request: OperatorResolutionRequest,
    *,
    verify_authority: AuthorityVerifier,
    capabilities: ControlCapabilities | None = None,
) -> OperatorResolutionResult:
    """Resolve one blocker after transport-level authority verification.

    This function is the transactional domain handler, not an authentication
    boundary. The caller must run it only in the service-writer process and
    supply a bounded local verifier backed by the separate operator authority
    channel. The verifier is called again under the writer lock immediately
    before commit so expiry or revocation cannot race lock acquisition.
    """

    _require_schema(connection)
    _validate_resolution_request(request)
    if not (capabilities or ControlCapabilities()).operator_resolution:
        raise DomainControlError(
            "operator_resolution_unavailable",
            "operator resolution requires an attested isolated authority channel",
        )
    if connection.in_transaction:
        raise DomainControlError(
            "transaction_already_open",
            "resolution requires its own immediate transaction",
        )
    previous_row_factory = connection.row_factory
    connection.row_factory = sqlite3.Row
    try:
        replay = _resolution_replay(connection, request, verify_authority)
        if replay is not None:
            return replay
        observed = blocking_snapshot(connection, capabilities=capabilities)
        observed_condition = next(
            (
                item
                for item in observed.conditions
                if item.condition_id == request.condition_id
            ),
            None,
        )
        if observed_condition is None:
            raise DomainControlError(
                "condition_not_found",
                "the blocking condition no longer exists",
            )
        if observed_condition.revision != request.expected_revision:
            raise DomainControlError(
                "condition_revision_changed",
                "the blocking condition changed; fetch a new snapshot",
            )
        if (
            request.action not in observed_condition.allowed_actions
            or not observed_condition.attempt_id
        ):
            raise DomainControlError(
                "resolution_action_not_allowed",
                "the requested action is not allowed for this blocker",
            )
        if not verify_authority(request, observed_condition):
            raise DomainControlError(
                "operator_authority_denied",
                "the operator authority did not approve this resolution",
            )
        connection.execute("BEGIN IMMEDIATE")
        replay = _resolution_replay(connection, request, verify_authority)
        if replay is not None:
            connection.commit()
            return replay
        snapshot = blocking_snapshot(connection, capabilities=capabilities)
        condition = next(
            (
                item
                for item in snapshot.conditions
                if item.condition_id == request.condition_id
            ),
            None,
        )
        if condition is None or condition.revision != request.expected_revision:
            raise DomainControlError(
                "condition_revision_changed",
                "the blocking condition changed; fetch a new snapshot",
            )
        lease = connection.execute(
            """
            SELECT endpoint_id,fence FROM endpoint_leases
            WHERE attempt_id=? AND released_at IS NULL
            """,
            (condition.attempt_id,),
        ).fetchone()
        if lease is None:
            raise DomainControlError(
                "condition_revision_changed",
                "the endpoint lease is no longer open",
            )
        if not verify_authority(request, condition):
            raise DomainControlError(
                "operator_authority_denied",
                "the operator authority expired or was revoked before commit",
            )
        _, resolved_at = _canonical_now(None)
        connection.execute(
            """
            INSERT INTO operator_resolutions(
              attempt_id,endpoint_id,lease_fence,action,source_kind,
              authority_receipt_id,operator_principal_hash,evidence_ref,
              evidence_sha256,resolved_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?)
            """,
            (
                condition.attempt_id,
                str(lease["endpoint_id"]),
                int(lease["fence"]),
                request.action,
                "authority",
                request.authority_receipt_id,
                request.operator_principal_hash,
                request.evidence_ref,
                request.evidence_sha256,
                resolved_at,
            ),
        )
        target_attempt_state = (
            "operator_completed" if request.action == "complete" else "operator_abandoned"
        )
        target_turn_state = "completed" if request.action == "complete" else "cancelled"
        connection.execute(
            """
            UPDATE native_attempts
            SET state=?,terminal_at=?,updated_at=?
            WHERE attempt_id=? AND state='uncertain' AND terminal_at IS NULL
            """,
            (
                target_attempt_state,
                resolved_at,
                resolved_at,
                condition.attempt_id,
            ),
        )
        if connection.execute("SELECT changes()").fetchone()[0] != 1:
            raise DomainControlError(
                "condition_revision_changed",
                "the native attempt changed during resolution",
            )
        connection.execute(
            """
            UPDATE queued_turns
            SET state=?,terminal_at=?,error_code=?,updated_at=?
            WHERE state='ready' AND event_key IN (
              SELECT event_key FROM native_attempt_turns WHERE attempt_id=?
            )
            """,
            (
                target_turn_state,
                resolved_at,
                f"operator_{request.action}",
                resolved_at,
                condition.attempt_id,
            ),
        )
        connection.execute(
            """
            UPDATE endpoint_leases
            SET released_at=?,release_reason=?
            WHERE attempt_id=? AND released_at IS NULL
            """,
            (
                resolved_at,
                f"operator_{request.action}",
                condition.attempt_id,
            ),
        )
        if connection.execute("SELECT changes()").fetchone()[0] != 1:
            raise DomainControlError(
                "condition_revision_changed",
                "the endpoint lease changed during resolution",
            )
        violations = invariant_violations(connection)
        if violations:
            raise DomainControlError(
                "domain_invariant_failed",
                f"resolution would violate domain state: {','.join(violations)}",
            )
        connection.commit()
        return OperatorResolutionResult(
            "applied",
            condition.attempt_id,
            request.action,
            request.authority_receipt_id,
        )
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.row_factory = previous_row_factory
