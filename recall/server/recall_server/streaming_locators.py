"""Publish exact existing-archive positions without clearing or enrolling bodies.

A sealed attempt-local metadata spool supplies short NULL-only transactions.
A discarded/partial attempt reruns proof; existing matching locators are its
checkpoint. Retirement must remain absent or disabled through each commit.
"""

from __future__ import annotations

import time
import hashlib

import orjson

import psycopg

from .chunk_bodies import _check_deadline
from .chunk_retirement import (
    ParentRetirementLimits,
    _parent_deadline,
    _parent_scope,
    _try_parent_native_locks,
    require_retirement_owner,
)
from .logical_evidence import IDENTITY_RE
from .locator_backfill_plan import LocatorPlanError
from .parent_chunk_proof import (
    manifest_identity,
    prove_parent_chunks,
    read_parent_catalog,
)


class LocatorPublicationError(LocatorPlanError):
    def __init__(self, code="locator_publication_unavailable"):
        super().__init__(code)
        self.committed = dict(batches=0, published_documents=0)
        self.commit_unknown = False


def _authority(store, connection, scope, principal, deadline_at, *, lock_ledger=False):
    require_retirement_owner(store, connection, scope[:2], principal, deadline_at)
    progress = store._execute_bounded(
        connection,
        """SELECT enabled FROM canonical_chunk_retirement_progress
        WHERE tenant_id=%s AND source_id=%s AND native_parent_id=%s"""
        + (" FOR UPDATE NOWAIT" if lock_ledger else ""),
        scope,
        deadline_at,
    ).fetchone()
    if progress and progress["enabled"]:
        raise LocatorPublicationError("locator_publication_retirement_enabled")


def _publish_batch(
    store, *, proof, rows, scope, principal, deadline_at, first, should_stop=None
):
    about_to_commit = False
    try:
        with store.connect() as connection, connection.transaction():

            def query(sql, values=()):
                return store._execute_bounded(connection, sql, values, deadline_at)

            require_retirement_owner(
                store, connection, scope[:2], principal, deadline_at
            )
            if not _try_parent_native_locks(query, scope, rows):
                raise LocatorPublicationError("locator_publication_lock_busy")
            ids = sorted(row["document_id"] for row in rows)
            current = query(
                """SELECT document.tenant_id,document.source_id,document.document_id,document.native_id,
                    document.revision,document.text_sha256,document.body_record_ordinal,document.body_record_count,event.kind,
                    artifact.media_type AS raw_media_type,
                    ARRAY[event.canonical_redacted->>'type',event.canonical_redacted #>> '{content,type}',
                          event.canonical_redacted #>> '{content,message,type}',event.canonical_redacted #>> '{content,payload,type}',
                          event.canonical_redacted #>> '{message,type}',event.canonical_redacted #>> '{payload,type}'] AS structural_types
                FROM canonical_documents document JOIN canonical_events event USING(tenant_id,source_id,event_id)
                JOIN raw_artifacts artifact ON artifact.tenant_id=event.tenant_id AND artifact.source_id=event.source_id
                    AND artifact.artifact_id=event.artifact_id
                WHERE document.tenant_id=%s AND document.source_id=%s AND document.document_id=ANY(%s)
                  AND COALESCE(event.native_parent_id,event.native_id)=%s AND document.is_current
                  AND document.deleted_at IS NULL AND NOT event.is_tombstone
                  AND NOT EXISTS(SELECT 1 FROM canonical_events later WHERE later.tenant_id=document.tenant_id
                      AND later.source_id=document.source_id AND later.native_id=document.native_id
                      AND later.revision>document.revision AND later.is_tombstone)
                ORDER BY document.document_id FOR UPDATE OF document NOWAIT FOR SHARE OF event,artifact NOWAIT""",
                (*scope[:2], ids, scope[2]),
            ).fetchall()
            desired = {row["document_id"]: row for row in rows}
            if len(desired) != len(rows) or len(current) != len(desired):
                raise LocatorPublicationError("locator_publication_document_changed")
            changes = []
            for row in current:
                expected = desired[row["document_id"]]
                if any(
                    row[key] != expected[key]
                    for key in row
                    if key not in {"body_record_ordinal", "body_record_count"}
                ):
                    raise LocatorPublicationError(
                        "locator_publication_document_changed"
                    )
                position = row["body_record_ordinal"], row["body_record_count"]
                if position == (None, None):
                    changes.append(expected)
                elif position != (expected["record_ordinal"], expected["record_count"]):
                    raise LocatorPublicationError(
                        "locator_publication_position_changed"
                    )
            catalog = read_parent_catalog(
                store, connection, scope, deadline_at, lock=True
            )
            if (
                manifest_identity(catalog["manifest"]) != proof["manifest"]
                or catalog["manifest"]["created_at"] != proof["catalog_created_at"]
            ):
                raise LocatorPublicationError("locator_publication_parent_changed")
            # Enrollment takes the same catalog lock before inserting a ledger;
            # FOR UPDATE on an absent ledger alone would not exclude that race.
            query(
                """SELECT 1 FROM canonical_evidence_documents WHERE tenant_id=%s AND source_id=%s
                AND native_parent_id=%s FOR UPDATE NOWAIT""",
                scope,
            )
            _authority(
                store, connection, scope, principal, deadline_at, lock_ledger=True
            )
            if first:
                parts = query(
                    """SELECT * FROM canonical_evidence_document_parts
                    WHERE tenant_id=%s AND source_id=%s AND logical_document_id=%s AND revision=%s
                    ORDER BY part_ordinal LIMIT %s""",
                    (
                        *scope[:2],
                        proof["manifest"]["logical_document_id"],
                        proof["manifest"]["revision"],
                        len(proof["parts"]) + 1,
                    ),
                ).fetchall()
                if parts != proof["parts"]:
                    raise LocatorPublicationError("locator_publication_parent_changed")
            chunks = query(
                """SELECT document_id,ordinal,receipt,text_sha256 FROM canonical_chunks
                WHERE tenant_id=%s AND source_id=%s AND document_id=ANY(%s) AND deleted_at IS NULL
                ORDER BY document_id,ordinal LIMIT %s FOR SHARE NOWAIT""",
                (*scope[:2], ids, sum(len(row["chunks"]) for row in rows) + 1),
            ).fetchall()
            expected_chunks = {
                (row["document_id"], chunk["ordinal"]): chunk
                for row in rows
                for chunk in row["chunks"]
            }
            if len(chunks) != len(expected_chunks) or any(
                (row["document_id"], row["ordinal"]) not in expected_chunks
                or any(
                    row[key]
                    != expected_chunks[(row["document_id"], row["ordinal"])][key]
                    for key in ("ordinal", "receipt", "text_sha256")
                )
                for row in chunks
            ):
                raise LocatorPublicationError("locator_publication_chunks_changed")
            if should_stop is not None and should_stop():
                raise LocatorPublicationError("locator_publication_interrupted")
            if changes:
                # Rows are already locked and unique. Retain every predicate
                # and reject a partial match before this transaction commits.
                placeholders = ",".join(
                    ["(%s::text,%s::integer,%s::text,%s::integer,%s::integer)"]
                    * len(changes)
                )
                if query(
                    """UPDATE canonical_documents AS document
                    SET body_record_ordinal=desired.record_ordinal,body_record_count=desired.record_count
                    FROM (VALUES """
                    + placeholders
                    + """) AS desired(
                        document_id,revision,text_sha256,record_ordinal,record_count)
                    WHERE document.tenant_id=%s AND document.source_id=%s
                      AND document.document_id=desired.document_id
                      AND document.is_current AND document.deleted_at IS NULL
                      AND document.revision=desired.revision AND document.text_sha256=desired.text_sha256
                      AND document.body_record_ordinal IS NULL AND document.body_record_count IS NULL""",
                    tuple(
                        row[key]
                        for row in changes
                        for key in (
                            "document_id",
                            "revision",
                            "text_sha256",
                            "record_ordinal",
                            "record_count",
                        )
                    )
                    + scope[:2],
                ).rowcount != len(changes):
                    raise LocatorPublicationError(
                        "locator_publication_document_changed"
                    )
            if should_stop is not None and should_stop():
                raise LocatorPublicationError("locator_publication_interrupted")
            _check_deadline(deadline_at)
            about_to_commit = True
        return len(changes)
    except Exception as error:
        failure = (
            error
            if isinstance(error, LocatorPublicationError)
            else LocatorPublicationError(
                "locator_publication_lock_busy"
                if isinstance(error, psycopg.errors.LockNotAvailable)
                else "locator_publication_batch_unavailable"
            )
        )
        failure.commit_unknown = about_to_commit
        raise failure from None


def publish_parent_locators(
    store,
    archive,
    *,
    tenant_id,
    source_id,
    native_parent_id,
    owner_principal_id,
    apply=False,
    reviewed_plan=None,
    limits=None,
    deadline_at=None,
    should_stop=None,
):
    """Verify once and publish bounded positions for one explicit parent.

    No saved plan is write authority. Pool acquisition/DNS/COMMIT are not hard
    cancelled; one cooperative deadline covers SQL/archive and precommit checks.
    After publication, wait at least 60 seconds before separately enabling clears.
    """
    totals = dict(batches=0, published_documents=0)
    try:
        scope = _parent_scope(tenant_id, source_id, native_parent_id)
        if (
            not isinstance(owner_principal_id, str)
            or not IDENTITY_RE.fullmatch(owner_principal_id)
            or type(apply) is not bool
            or should_stop is not None
            and not callable(should_stop)
        ):
            raise LocatorPublicationError("locator_publication_request_invalid")
        limits = ParentRetirementLimits() if limits is None else limits
        if not isinstance(limits, ParentRetirementLimits):
            raise LocatorPublicationError("locator_publication_request_invalid")
        deadline_at = _parent_deadline(deadline_at)
        if should_stop is not None and should_stop():
            raise LocatorPublicationError("locator_publication_interrupted")
        with store.connect() as connection, connection.transaction():
            _authority(
                store,
                connection,
                scope,
                owner_principal_id,
                min(deadline_at, time.monotonic() + 5),
            )
        with prove_parent_chunks(
            store,
            archive,
            scope=scope,
            limits=limits,
            deadline_at=deadline_at,
            purpose="locate",
        ) as proof:
            with store.connect() as connection, connection.transaction():
                _authority(
                    store,
                    connection,
                    scope,
                    owner_principal_id,
                    min(deadline_at, time.monotonic() + 5),
                )
            proposed = (
                proof["spool"]
                .index.execute(
                    "SELECT count(*) FROM documents WHERE proposed_count IS NOT NULL"
                )
                .fetchone()[0]
            )
            plan = dict(
                contract="recall.streaming-locators.v1",
                tenant_id=tenant_id,
                source_id=source_id,
                native_parent_id=native_parent_id,
                manifest=proof["manifest"],
                catalog_created_at=proof["catalog_created_at"],
            )
            # Portable report identity: saved JSON and fresh database datetimes
            # must compare without accepting serialized proof as authority.
            plan = orjson.loads(orjson.dumps(plan, default=str))
            plan["proof_sha256"] = hashlib.sha256(
                orjson.dumps(plan, option=orjson.OPT_SORT_KEYS, default=str)
            ).hexdigest()
            if reviewed_plan is not None and reviewed_plan != plan:
                raise LocatorPublicationError("locator_publication_review_changed")
            report = dict(
                plan=plan,
                status="dry_run",
                current_documents=proof["current_documents"],
                eligible_documents=proof["eligible_documents"],
                proposed_documents=proposed,
                excluded=proof["excluded"],
                archive_gets=proof["archive_gets"],
                archive_bytes=proof["archive_bytes"],
                complete=False,
                **totals,
            )
            if not apply:
                return dict(report, complete=True)
            rows, chunk_count = [], 0

            def flush():
                published = _publish_batch(
                    store,
                    proof=proof,
                    rows=rows,
                    scope=scope,
                    principal=owner_principal_id,
                    deadline_at=min(deadline_at, time.monotonic() + 5),
                    first=totals["batches"] == 0,
                    should_stop=should_stop,
                )
                totals["published_documents"] += published
                totals["batches"] += 1

            for row in proof["spool"].proposals():
                _check_deadline(deadline_at)
                if should_stop is not None and should_stop():
                    return dict(report, status="interrupted", **totals)
                if rows and (
                    len(rows) >= limits.batch_documents
                    or chunk_count + len(row["chunks"]) > limits.batch_chunks
                ):
                    flush()
                    rows = []
                    chunk_count = 0
                    if totals["batches"] >= limits.max_batches:
                        return dict(report, status="partial", **totals)
                rows.append(row)
                chunk_count += len(row["chunks"])
            if rows:
                flush()
            return dict(report, status="published", complete=True, **totals)
    except Exception as error:
        failure = (
            error
            if isinstance(error, LocatorPublicationError)
            else LocatorPublicationError()
        )
        failure.committed = dict(totals)
        raise failure from None
