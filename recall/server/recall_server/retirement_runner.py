"""Bounded maintenance for an explicitly enrolled, surviving parent cohort.

This is invocation-local scheduling over schema069, not a second proof or queue.
Catalog deletion also deletes its ledger: ordinary runs never enroll replacements.
"""

from collections import Counter
from dataclasses import asdict, dataclass, replace
import hashlib
import time

import orjson

from .chunk_retirement import (
    ChunkRetirementError,
    ParentRetirementLimits,
    _parent_deadline,
    require_retirement_owner,
    retire_parent_chunks,
)
from .logical_evidence import IDENTITY_RE
from .parent_chunk_proof import (
    manifest_identity,
    parent_retirement_plan,
    read_parent_catalog,
)


def _invalid():
    raise ChunkRetirementError("retirement_runner_request_invalid")


@dataclass(frozen=True)
class RetirementScope:
    tenant_id: str
    principal_id: str
    source_ids: tuple[str, ...]

    def __post_init__(self):
        if (
            type(self.source_ids) is not tuple
            or not 1 <= len(self.source_ids) <= 100
            or any(
                not isinstance(v, str) or not IDENTITY_RE.fullmatch(v)
                for v in (self.tenant_id, self.principal_id, *self.source_ids)
            )
            or len(set(self.source_ids)) != len(self.source_ids)
        ):
            _invalid()


@dataclass(frozen=True)
class RunnerLimits:
    max_parents: int = 10
    max_archive_bytes: int = 256 * 1024**2
    max_clear_bytes: int = 64 * 1024**2
    cooldown_seconds: int = 60

    def __post_init__(self):
        for value, maximum in (
            (self.max_parents, 1000),
            (self.max_archive_bytes, 64 * 1024**3),
            (self.max_clear_bytes, 1024**4),
            (self.cooldown_seconds, 86400),
        ):
            if type(value) is not int or not 1 <= value <= maximum:
                _invalid()


def _authorize(store, connection, scope, deadline_at):
    for source in sorted(scope.source_ids):
        require_retirement_owner(
            store,
            connection,
            (scope.tenant_id, source),
            scope.principal_id,
            deadline_at,
        )


def _digest(plan):
    return hashlib.sha256(orjson.dumps(plan, option=orjson.OPT_SORT_KEYS)).hexdigest()


def _cohort_entry(manifest):
    # A reviewed enrollment page must not survive deletion/recreation of its
    # catalog, even if deterministic archive content is byte-identical.
    return dict(
        source_id=manifest["source_id"],
        native_parent_id=manifest["native_parent_id"],
        catalog_created_at=manifest["created_at"],
        manifest=manifest_identity(manifest),
    )


def plan_cohort(store, *, scope, after=None, limit=100, deadline_at=None):
    """A bounded keyset page of existing catalog identities; no archive reads."""
    if (
        not isinstance(scope, RetirementScope)
        or type(limit) is not int
        or not 1 <= limit <= 100
        or after is not None
        and (
            type(after) not in (tuple, list)
            or len(after) != 2
            or any(
                not isinstance(v, str) or not IDENTITY_RE.fullmatch(v) for v in after
            )
        )
    ):
        _invalid()
    deadline_at = min(_parent_deadline(deadline_at), time.monotonic() + 5)
    with store.connect() as connection, connection.transaction():
        _authorize(store, connection, scope, deadline_at)
        rows = store._execute_bounded(
            connection,
            """SELECT to_jsonb(evidence) AS manifest
            FROM canonical_evidence_documents evidence
            WHERE tenant_id=%s AND source_id=ANY(%s) AND (source_id,native_parent_id)>(%s,%s)
            ORDER BY source_id,native_parent_id LIMIT %s""",
            (scope.tenant_id, list(scope.source_ids), *(after or ("", "")), limit + 1),
            deadline_at,
        ).fetchall()
    parents = [_cohort_entry(row["manifest"]) for row in rows[:limit]]
    plan = dict(
        contract="recall.retirement-cohort.v1",
        scope=asdict(scope),
        parents=parents,
        after=list(after) if after else None,
        more=len(rows) > limit,
        next_cursor=(
            [parents[-1]["source_id"], parents[-1]["native_parent_id"]]
            if parents
            else None
        ),
    )
    # JSON is the private plan's canonical representation, including source tuple.
    plan = orjson.loads(orjson.dumps(plan))
    plan["proof_sha256"] = _digest(plan)
    return plan


def enroll_cohort(store, *, scope, reviewed_plan, deadline_at=None):
    """Insert this reviewed existing page only. Never reset or enable a ledger row."""
    if not isinstance(scope, RetirementScope) or not isinstance(reviewed_plan, dict):
        _invalid()
    unsigned = {k: v for k, v in reviewed_plan.items() if k != "proof_sha256"}
    if (
        set(unsigned)
        != {"contract", "scope", "parents", "after", "more", "next_cursor"}
        or unsigned["contract"] != "recall.retirement-cohort.v1"
        or unsigned["scope"] != orjson.loads(orjson.dumps(asdict(scope)))
        or reviewed_plan.get("proof_sha256") != _digest(unsigned)
        or not isinstance(unsigned["parents"], list)
        or len(unsigned["parents"]) > 100
    ):
        _invalid()
    parents = unsigned["parents"]
    keys = [
        (p.get("source_id"), p.get("native_parent_id"))
        for p in parents
        if isinstance(p, dict)
    ]
    if (
        len(keys) != len(parents)
        or any(
            source not in scope.source_ids
            or not isinstance(parent, str)
            or not IDENTITY_RE.fullmatch(parent)
            for source, parent in keys
        )
        or len(set(keys)) != len(keys)
    ):
        _invalid()
    deadline_at = min(_parent_deadline(deadline_at), time.monotonic() + 5)
    inserted = 0
    with store.connect() as connection, connection.transaction():
        _authorize(store, connection, scope, deadline_at)
        for plan in sorted(
            parents, key=lambda p: (p["source_id"], p["native_parent_id"])
        ):
            key = scope.tenant_id, plan["source_id"], plan["native_parent_id"]
            catalog = read_parent_catalog(
                store, connection, key, deadline_at, lock=True
            )
            if _cohort_entry(catalog["manifest"]) != plan:
                raise ChunkRetirementError("retirement_cohort_changed")
            inserted += store._execute_bounded(
                connection,
                """INSERT INTO canonical_chunk_retirement_progress
                (tenant_id,source_id,native_parent_id,logical_document_id,enabled,status)
                VALUES(%s,%s,%s,%s,true,'pending') ON CONFLICT DO NOTHING""",
                (*key, catalog["manifest"]["logical_document_id"]),
                deadline_at,
            ).rowcount
    return dict(
        status="enrolled",
        reviewed_parents=len(parents),
        inserted_parents=inserted,
        preserved_parents=len(parents) - inserted,
    )


_ELIGIBLE = """progress.tenant_id=%s AND progress.source_id=ANY(%s) AND progress.enabled
    AND (progress.status<>'complete' OR progress.manifest_artifact_id IS DISTINCT FROM evidence.manifest_artifact_id)
    AND NOT EXISTS(SELECT 1 FROM canonical_evidence_document_queue queued
        WHERE queued.tenant_id=progress.tenant_id AND queued.source_id=progress.source_id
          AND queued.native_parent_id=progress.native_parent_id)"""
_JOIN = """FROM canonical_chunk_retirement_progress progress JOIN canonical_evidence_documents evidence
    USING(tenant_id,source_id,native_parent_id,logical_document_id)"""


def inspect_enabled(store, *, scope, limits, deadline_at):
    deadline_at = min(deadline_at, time.monotonic() + 5)
    with store.connect() as connection, connection.transaction():
        _authorize(store, connection, scope, deadline_at)
        row = store._execute_bounded(
            connection,
            "SELECT count(*) AS eligible_parents " + _JOIN + " WHERE " + _ELIGIBLE,
            (scope.tenant_id, list(scope.source_ids)),
            deadline_at,
        ).fetchone()
    # No claim of located documents: metadata page counts cannot prove body eligibility.
    return dict(row, limits=asdict(limits), body_eligibility="requires_parent_proof")


def _claim(store, *, scope, limits, deadline_at, attempted):
    with store.connect() as connection, connection.transaction():
        _authorize(store, connection, scope, deadline_at)
        row = store._execute_bounded(
            connection,
            """SELECT progress.source_id,progress.native_parent_id,
            progress.scope_epoch,to_jsonb(evidence) AS manifest """
            + _JOIN
            + " WHERE "
            + _ELIGIBLE
            + """
            AND progress.updated_at <= clock_timestamp() - (%s * interval '1 second')
            AND NOT EXISTS(SELECT 1 FROM unnest(%s::text[],%s::text[]) seen(source,parent)
                WHERE seen.source=progress.source_id AND seen.parent=progress.native_parent_id)
            ORDER BY progress.updated_at,progress.source_id,progress.native_parent_id LIMIT 1
            FOR UPDATE OF progress SKIP LOCKED""",
            (
                scope.tenant_id,
                list(scope.source_ids),
                limits.cooldown_seconds,
                [p[0] for p in attempted],
                [p[1] for p in attempted],
            ),
            deadline_at,
        ).fetchone()
        if row is None:
            return None
        # An epoch is a commit fence, not an exclusive lifetime lease. A later
        # invocation can supersede a slow proof; its old claimant cannot publish.
        updated = store._execute_bounded(
            connection,
            """UPDATE canonical_chunk_retirement_progress
            SET scope_epoch=scope_epoch+1,updated_at=clock_timestamp()
            WHERE tenant_id=%s AND source_id=%s AND native_parent_id=%s RETURNING scope_epoch""",
            (scope.tenant_id, row["source_id"], row["native_parent_id"]),
            deadline_at,
        ).fetchone()
        row["scope_epoch"] = updated["scope_epoch"]
        return row


class RunArchive:
    """Charge attempted catalog bytes across successful AND failed parent proofs."""

    def __init__(self, delegate, *, max_bytes):
        self.delegate, self.max_bytes = delegate, max_bytes
        self.gets = self.bytes = 0

    def read_raw(self, reference):
        size = reference["size_bytes"]
        if type(size) is not int or size < 0 or self.bytes + size > self.max_bytes:
            raise ChunkRetirementError("retirement_runner_archive_budget")
        self.gets += 1
        self.bytes += size
        return self.delegate.read_raw(reference)

    def read_raw_bounded(self, reference, *, deadline_at):
        # _VerifiedArchive calls this surface when available. Preserve transport
        # cancellation while sharing the exact same invocation-wide accounting.
        size = reference["size_bytes"]
        if type(size) is not int or size < 0 or self.bytes + size > self.max_bytes:
            raise ChunkRetirementError("retirement_runner_archive_budget")
        self.gets += 1
        self.bytes += size
        bounded = getattr(self.delegate, "read_raw_bounded", None)
        return (
            bounded(reference, deadline_at=deadline_at)
            if callable(bounded)
            else self.delegate.read_raw(reference)
        )


def run_retirement(
    store,
    archive,
    *,
    scope,
    apply=False,
    limits=None,
    parent_limits=None,
    deadline_at=None,
    should_stop=None,
):
    """Sequential enabled-only maintenance; fresh whole-parent proof once per claim."""
    limits = RunnerLimits() if limits is None else limits
    parent_limits = ParentRetirementLimits() if parent_limits is None else parent_limits
    if (
        not isinstance(scope, RetirementScope)
        or not isinstance(limits, RunnerLimits)
        or not isinstance(parent_limits, ParentRetirementLimits)
        or type(apply) is not bool
        or should_stop is not None
        and not callable(should_stop)
    ):
        _invalid()
    deadline_at = _parent_deadline(deadline_at)
    if not apply:
        return dict(
            inspect_enabled(store, scope=scope, limits=limits, deadline_at=deadline_at),
            status="dry_run",
        )
    meter = RunArchive(archive, max_bytes=limits.max_archive_bytes)
    report = dict(
        status="bounded",
        attempted_parents=0,
        completed_parent_proofs=0,
        failed_parents=0,
        partial_parents=0,
        batches=0,
        cleared_documents=0,
        cleared_chunks=0,
        cleared_utf8_bytes=0,
        eligible_documents=0,
        commit_outcome_unknown=False,
    )
    errors, excluded, attempted = Counter(), Counter(), []
    for _ in range(limits.max_parents):
        if time.monotonic() >= deadline_at or should_stop is not None and should_stop():
            report["status"] = "stopped"
            break
        remaining_bytes = limits.max_clear_bytes - report["cleared_utf8_bytes"]
        remaining_archive = limits.max_archive_bytes - meter.bytes
        if remaining_bytes <= 0 or remaining_archive <= 0:
            break
        try:
            candidate = _claim(
                store,
                scope=scope,
                limits=limits,
                deadline_at=min(deadline_at, time.monotonic() + 5),
                attempted=attempted,
            )
        except Exception as error:
            if not attempted:
                raise
            errors[
                error.error_code
                if isinstance(error, ChunkRetirementError)
                else "retirement_claim_unavailable"
            ] += 1
            report["status"] = "claim_refused"
            break
        if candidate is None:
            report["status"] = "no_ready_parents"
            break
        attempted.append((candidate["source_id"], candidate["native_parent_id"]))
        report["attempted_parents"] += 1
        key = scope.tenant_id, candidate["source_id"], candidate["native_parent_id"]
        try:
            result = retire_parent_chunks(
                store,
                meter,
                tenant_id=key[0],
                source_id=key[1],
                native_parent_id=key[2],
                apply=True,
                reviewed_plan=parent_retirement_plan(key, candidate["manifest"]),
                limits=replace(
                    parent_limits,
                    max_clear_bytes=min(parent_limits.max_clear_bytes, remaining_bytes),
                    max_archive_bytes=min(
                        parent_limits.max_archive_bytes, remaining_archive
                    ),
                ),
                deadline_at=deadline_at,
                required_scope_epoch=candidate["scope_epoch"],
                owner_principal_id=scope.principal_id,
                should_stop=should_stop,
            )
            report["completed_parent_proofs"] += int(result["complete"])
            report["partial_parents"] += int(not result["complete"])
            report["eligible_documents"] += result.get("eligible_documents", 0)
            excluded.update(result.get("excluded", {}))
        except ChunkRetirementError as error:
            report["failed_parents"] += 1
            errors[error.error_code] += 1
            result = getattr(error, "committed", {})
            if error.error_code == "parent_retirement_unavailable":
                # COMMIT may have succeeded before Python observed its counters.
                # Stop; a later explicit invocation resumes from the durable ledger.
                report["commit_outcome_unknown"] = True
                report["status"] = "commit_outcome_unknown"
        for field in (
            "batches",
            "cleared_documents",
            "cleared_chunks",
            "cleared_utf8_bytes",
        ):
            report[field] += result.get(field, 0)
        if report["commit_outcome_unknown"] or "retirement_owner_required" in errors:
            break
    return dict(
        report,
        errors=dict(errors),
        excluded=dict(excluded),
        archive_gets=meter.gets,
        archive_bytes=meter.bytes,
        body_eligibility="proved_targets_only",
        physical_reclaimed_bytes=None,
    )
