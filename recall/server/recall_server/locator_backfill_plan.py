"""Verified body positions in existing immutable parent parts.

Saved plans are advisory. The opt-in apply API reruns the complete proof in
process, then fences the fresh metadata under queue/catalog/document locks. It
only fills proven NULL positions; it never uploads or changes source bodies.
"""
from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
import hashlib
import math
import time
from typing import Any

import orjson
import psycopg

from .chunk_bodies import (
    _EXCLUDED_TYPES, _VerifiedArchive, _check_deadline,
    _record_location, _verified_body,
)
from .db import SearchDeadlineExceeded
from .logical_body_proof import LogicalBodyProofError, iter_parent_bodies


class LocatorPlanError(ValueError):
    """Content-free failure. Partial proposed positions must never escape."""


@dataclass(frozen=True)
class PlanLimits:
    max_documents: int = 10_000
    max_chunks: int = 100_000
    max_parts: int = 4096
    max_records: int = 200_000
    max_bytes: int = 256 * 1024 * 1024

    def __post_init__(self):
        maxima = (20_000, 200_000, 8192, 400_000, 8 * 1024**3)
        values = (self.max_documents, self.max_chunks, self.max_parts, self.max_records, self.max_bytes)
        for value, maximum in zip(values, maxima):
            if type(value) is not int or not 1 <= value <= maximum:
                raise LocatorPlanError('locator_plan_request_invalid')


@dataclass(frozen=True)
class _ParentProof:
    report: dict[str, Any]
    snapshot: dict[str, Any]
    tenant: str
    source: str
    parent: str
    limits: PlanLimits
    deadline_at: float


_PARENT_SQL = """SELECT to_jsonb(evidence) AS manifest,
       (SELECT jsonb_build_object('generation',queued.generation,
                                 'changed_at',queued.changed_at,'reason',queued.reason)
          FROM canonical_evidence_document_queue queued
         WHERE queued.tenant_id=evidence.tenant_id AND queued.source_id=evidence.source_id
           AND queued.native_parent_id=evidence.native_parent_id) AS queue
    FROM canonical_evidence_documents evidence
   WHERE evidence.tenant_id=%s AND evidence.source_id=%s AND evidence.native_parent_id=%s"""
_DOCUMENTS_SQL = """SELECT document.tenant_id,document.source_id,document.document_id,
       document.native_id,document.revision,document.text_sha256,
       document.body_record_ordinal,document.body_record_count,event.kind,
       artifact.media_type AS raw_media_type,
       ARRAY[event.canonical_redacted->>'type',
             event.canonical_redacted #>> '{content,type}',
             event.canonical_redacted #>> '{content,message,type}',
             event.canonical_redacted #>> '{content,payload,type}',
             event.canonical_redacted #>> '{message,type}',
             event.canonical_redacted #>> '{payload,type}'] AS structural_types
  FROM canonical_documents document
  JOIN canonical_events event USING(tenant_id,source_id,event_id)
  JOIN raw_artifacts artifact
    ON artifact.tenant_id=event.tenant_id AND artifact.source_id=event.source_id
   AND artifact.artifact_id=event.artifact_id
 WHERE document.tenant_id=%s AND document.source_id=%s
   AND COALESCE(event.native_parent_id,event.native_id)=%s
   AND document.is_current AND document.deleted_at IS NULL AND NOT event.is_tombstone
   AND NOT EXISTS (SELECT 1 FROM canonical_events later
                    WHERE later.tenant_id=document.tenant_id AND later.source_id=document.source_id
                      AND later.native_id=document.native_id AND later.revision>document.revision
                      AND later.is_tombstone)
 ORDER BY document.document_id LIMIT %s"""


def _snapshot_on_connection(store, connection, tenant, source, parent, limits, deadline_at):
    """Read bounded metadata using the caller's existing transaction."""
    _check_deadline(deadline_at)
    def query(sql, values):
        return store._execute_bounded(connection, sql, values, deadline_at)

    catalog = query(_PARENT_SQL, (tenant, source, parent)).fetchone()
    if catalog is None:
        raise LocatorPlanError('locator_plan_manifest_missing')
    manifest = catalog['manifest']
    parts = query("""SELECT * FROM canonical_evidence_document_parts
        WHERE tenant_id=%s AND source_id=%s AND logical_document_id=%s AND revision=%s
        ORDER BY part_ordinal LIMIT %s""", (tenant, source, manifest['logical_document_id'],
            manifest['revision'], limits.max_parts + 1)).fetchall()
    documents = query(_DOCUMENTS_SQL, (tenant, source, parent, limits.max_documents + 1)).fetchall()
    if len(parts) > limits.max_parts or len(documents) > limits.max_documents:
        raise LocatorPlanError('locator_plan_metadata_budget_exceeded')
    by_id = {row['document_id']: row for row in documents}
    if len(by_id) != len(documents):
        raise LocatorPlanError('locator_plan_catalog_invalid')
    for row in documents:
        row.update(chunks=[], pending=catalog['queue'] is not None, pg_body_bytes=0)
    chunks = query("""SELECT document_id,ordinal,receipt,text_sha256,
               octet_length(text_redacted) AS pg_bytes
          FROM canonical_chunks WHERE tenant_id=%s AND source_id=%s
           AND document_id=ANY(%s) AND deleted_at IS NULL
         ORDER BY document_id,ordinal LIMIT %s""",
         (tenant, source, list(by_id), limits.max_chunks + 1)).fetchall()
    if len(chunks) > limits.max_chunks:
        raise LocatorPlanError('locator_plan_metadata_budget_exceeded')
    for chunk in chunks:
        row = by_id[chunk['document_id']]
        row['pg_body_bytes'] += chunk['pg_bytes']
        row['chunks'].append({key: chunk[key] for key in ('ordinal', 'receipt', 'text_sha256')})
    _check_deadline(deadline_at)
    return dict(manifest=manifest, queue=catalog['queue'], parts=parts, documents=documents)


def _snapshot(store, tenant, source, parent, limits, deadline_at):
    """One metadata-only MVCC view, with independent cardinality bounds."""
    with store.connect() as connection:
        with connection.transaction():
            connection.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
            return _snapshot_on_connection(store, connection, tenant, source, parent, limits, deadline_at)


def select_parents(store, *, tenant_id, source_id=None, after=None, limit=1, deadline_at=None):
    """Bounded keyset page; cursor belongs in the private report only."""
    if (not isinstance(tenant_id, str) or not tenant_id or type(limit) is not int or not 1 <= limit <= 100
            or (source_id is not None and (not isinstance(source_id, str) or not source_id))
            or (after is not None and (not isinstance(after, tuple) or len(after) != 2
                                      or any(not isinstance(value, str) for value in after)))):
        raise LocatorPlanError('locator_plan_request_invalid')
    after_source, after_parent = after or ('', '')
    with store.connect() as connection:
        with connection.transaction():
            connection.execute('SET TRANSACTION READ ONLY')
            rows = store._execute_bounded(connection, """SELECT source_id,native_parent_id
                 FROM canonical_evidence_documents
                WHERE tenant_id=%s AND (%s::text IS NULL OR source_id=%s)
                  AND (source_id,native_parent_id)>(%s,%s)
                ORDER BY source_id,native_parent_id LIMIT %s""",
                (tenant_id, source_id, source_id, after_source, after_parent, limit + 1), deadline_at).fetchall()
    return rows[:limit], len(rows) > limit


class _MeteredArchive:
    def __init__(self, archive, deadline_at):
        self.delegate = _VerifiedArchive(archive, deadline_at)
        self.gets, self.bytes = 0, 0

    def read_raw(self, reference):
        self.gets += 1
        # Charge attempted catalog bytes even if corruption or transport fails.
        self.bytes += reference["size_bytes"]
        payload = self.delegate.read_raw(reference)
        return payload


def _prove_parent(store: Any, archive: Any, *, tenant_id: str, source_id: str,
                native_parent_id: str, limits: PlanLimits | None = None,
                deadline_at: float | None = None) -> _ParentProof:
    """Verify one parent, returning metadata only after its fresh final fence.

    Fetches each existing part once, never uploads or holds a DB connection
    during archive I/O. Whole parent bytes are streamed, and only one eligible
    event (at most 8MB) is retained for the shared reader's exact body proof.
    Parent, metadata, record, result and wall-clock work are explicitly bounded.
    """
    if any(not isinstance(value, str) or not value or len(value) > 1024
           for value in (tenant_id, source_id, native_parent_id)):
        raise LocatorPlanError('locator_plan_request_invalid')
    limits = limits or PlanLimits()
    if not isinstance(limits, PlanLimits):
        raise LocatorPlanError('locator_plan_request_invalid')
    now = time.monotonic()
    if deadline_at is not None and (type(deadline_at) not in (int, float)
            or not math.isfinite(deadline_at) or deadline_at > now + 3600):
        raise LocatorPlanError('locator_plan_request_invalid')
    deadline_at = now + 60 if deadline_at is None else deadline_at
    meter = _MeteredArchive(archive, deadline_at)
    expected_bytes, expected_gets = None, None
    try:
        before = _snapshot(store, tenant_id, source_id, native_parent_id, limits, deadline_at)
        manifest, parts = before['manifest'], before['parts']
        if (manifest['tenant_id'] != tenant_id or manifest['source_id'] != source_id
                or manifest['native_parent_id'] != native_parent_id
                or not parts or len(parts) != manifest['part_count']
                or type(manifest['record_count']) is not int
                or manifest['record_count'] < 1):
            raise LocatorPlanError('locator_plan_catalog_invalid')
        expected_gets = len(parts)
        expected_bytes = sum(part['size_bytes'] for part in parts)
        if manifest['record_count'] > limits.max_records:
            raise LocatorPlanError('locator_plan_metadata_budget_exceeded')
        if expected_bytes > limits.max_bytes:
            raise LocatorPlanError('locator_plan_archive_budget_exceeded')
        documents = {row['native_id']: row for row in before['documents']}
        if len(documents) != len(before['documents']):
            raise LocatorPlanError('locator_plan_catalog_invalid')
        excluded, eligible, changes, unchanged = Counter(), set(), [], 0
        seen = set()
        try:
            for first, segments in iter_parent_bodies(meter, tenant_id=tenant_id, source_id=source_id,
                    native_parent_id=native_parent_id, manifest=manifest, parts=parts,
                    max_records=limits.max_records, max_bytes=limits.max_bytes, deadline_at=deadline_at):
                native = first.event_native_id
                if native in seen:
                    raise LocatorPlanError('locator_plan_part_invalid')
                seen.add(native)
                row = documents.get(native)
                reason = ('not_current' if row is None else
                          'oversized' if row['raw_media_type'] == 'application/vnd.recall.oversized-record+gzip' else
                          'structural' if _EXCLUDED_TYPES.intersection(row['structural_types']) else
                          'event_body_budget' if segments is None else None)
                if reason is None:
                    stored = _record_location(row)
                    location = (first.ordinal, first.ordinal + first.segment_count)
                    if stored is not None and stored != location:
                        raise LocatorPlanError('locator_plan_existing_position_invalid')
                    verified = _verified_body(row, segments, stored, ())
                    if verified is None:
                        reason = ('pending_revision' if list(first.receipts) != [c['receipt'] for c in row['chunks']]
                                  else 'historical_chunk_boundaries')
                    else:
                        eligible.add(native)
                        if stored is None:
                            changes.append(dict(document_id=row['document_id'], record_ordinal=first.ordinal,
                                                record_count=first.segment_count))
                        else:
                            unchanged += 1
                if reason is not None:
                    excluded[reason] += 1
        except LogicalBodyProofError as error:
            raise LocatorPlanError(str(error).replace('logical_body_', 'locator_plan_')) from None
        for native, row in documents.items():
            if native not in seen:
                if row['raw_media_type'] == 'application/vnd.recall.oversized-record+gzip':
                    excluded['oversized'] += 1
                elif _EXCLUDED_TYPES.intersection(row['structural_types']):
                    excluded['structural'] += 1
                elif row['pending'] and _record_location(row) is None:
                    excluded['pending_new_document'] += 1
                else:
                    raise LocatorPlanError('locator_plan_current_body_missing')
        if _snapshot(store, tenant_id, source_id, native_parent_id, limits, deadline_at) != before:
            raise LocatorPlanError('locator_plan_catalog_changed')
        report = dict(status='verified_dry_run', current_documents=len(documents), eligible_documents=len(eligible),
                    unchanged_locators=unchanged, excluded=dict(sorted(excluded.items())), changes=changes,
                    verified_current_pg_chunk_bytes=sum(documents[native]['pg_body_bytes'] for native in eligible),
                    archive_gets=meter.gets, archive_bytes=meter.bytes, expected_archive_bytes=expected_bytes,
                    pending=before['queue'] is not None,
                    snapshot_sha256=hashlib.sha256(orjson.dumps(before, option=orjson.OPT_SORT_KEYS, default=str)).hexdigest())
        return _ParentProof(report, before, tenant_id, source_id, native_parent_id, limits, deadline_at)
    except SearchDeadlineExceeded as error:
        error.archive_gets, error.archive_bytes = meter.gets, meter.bytes
        error.expected_archive_gets, error.expected_archive_bytes = expected_gets, expected_bytes
        raise
    except LocatorPlanError as error:
        error.archive_gets, error.archive_bytes = meter.gets, meter.bytes
        error.expected_archive_gets, error.expected_archive_bytes = expected_gets, expected_bytes
        raise
    except Exception:
        error = LocatorPlanError('locator_plan_evidence_unavailable')
        error.archive_gets, error.archive_bytes = meter.gets, meter.bytes
        error.expected_archive_gets, error.expected_archive_bytes = expected_gets, expected_bytes
        raise error from None


def plan_parent(store: Any, archive: Any, *, tenant_id: str, source_id: str,
                native_parent_id: str, limits: PlanLimits | None = None,
                deadline_at: float | None = None) -> dict[str, Any]:
    return _prove_parent(store, archive, tenant_id=tenant_id, source_id=source_id,
                         native_parent_id=native_parent_id, limits=limits, deadline_at=deadline_at).report


def _apply_proof(store, proof):
    """Publish only proven NULLs; caller cannot supply persisted plan data.

    Existing queue -> parent catalog -> changed current documents, all NOWAIT.
    No advisory lock is acquired. A late append can insert a new queue row after
    the final fence: it cannot change the locked old documents or publish a new
    catalog before this transaction commits, and its new document stays NULL.
    """
    changes = proof.report['changes']
    if not changes:
        return 0
    documents = {row['document_id']: row for row in proof.snapshot['documents']}
    scope = (proof.tenant, proof.source, proof.parent)
    with store.connect() as connection:
        with connection.transaction():
            connection.execute('SET TRANSACTION ISOLATION LEVEL READ COMMITTED')

            def query(sql, values=()):
                return store._execute_bounded(connection, sql, values, proof.deadline_at)

            query("""SELECT generation FROM canonical_evidence_document_queue
                WHERE tenant_id=%s AND source_id=%s AND native_parent_id=%s FOR UPDATE NOWAIT""", scope)
            current = query("""SELECT logical_document_id FROM canonical_evidence_documents
                WHERE tenant_id=%s AND source_id=%s AND native_parent_id=%s FOR UPDATE NOWAIT""", scope).fetchone()
            if current is None:
                raise LocatorPlanError('locator_plan_catalog_changed')
            query("""CREATE TEMP TABLE recall_body_locator_backfill (
                document_id text PRIMARY KEY, native_id text NOT NULL, revision integer NOT NULL,
                text_sha256 text NOT NULL, record_ordinal integer NOT NULL CHECK(record_ordinal>=0),
                record_count integer NOT NULL CHECK(record_count>=1)
            ) ON COMMIT DROP""")
            with connection.cursor() as cursor:
                with cursor.copy('COPY pg_temp.recall_body_locator_backfill FROM STDIN') as writer:
                    for change in changes:
                        _check_deadline(proof.deadline_at)
                        row = documents[change['document_id']]
                        writer.write_row((row['document_id'], row['native_id'], row['revision'], row['text_sha256'],
                                          change['record_ordinal'], change['record_count']))
            query('ANALYZE pg_temp.recall_body_locator_backfill')
            locked = query("""SELECT document.document_id
                FROM canonical_documents document
                JOIN pg_temp.recall_body_locator_backfill desired USING(document_id)
                JOIN canonical_events event USING(tenant_id,source_id,event_id)
                WHERE document.tenant_id=%s AND document.source_id=%s
                  AND COALESCE(event.native_parent_id,event.native_id)=%s
                  AND document.native_id=desired.native_id AND document.revision=desired.revision
                  AND document.text_sha256=desired.text_sha256 AND document.is_current
                  AND document.deleted_at IS NULL AND document.body_record_ordinal IS NULL
                  AND document.body_record_count IS NULL
                ORDER BY document.document_id FOR UPDATE OF document NOWAIT""", scope).fetchall()
            if len(locked) != len(changes):
                raise LocatorPlanError('locator_plan_catalog_changed')
            # READ COMMITTED here is deliberate: reuse this connection, with
            # current publication and changed-document rows already locked.
            current = _snapshot_on_connection(store, connection, *scope, proof.limits, proof.deadline_at)
            if current != proof.snapshot:
                raise LocatorPlanError('locator_plan_catalog_changed')
            updated = query("""UPDATE canonical_documents document
                SET body_record_ordinal=desired.record_ordinal,body_record_count=desired.record_count
                FROM pg_temp.recall_body_locator_backfill desired
                WHERE document.tenant_id=%s AND document.source_id=%s
                  AND document.document_id=desired.document_id AND document.native_id=desired.native_id
                  AND document.revision=desired.revision AND document.text_sha256=desired.text_sha256
                  AND document.is_current AND document.deleted_at IS NULL
                  AND document.body_record_ordinal IS NULL AND document.body_record_count IS NULL""",
                  (proof.tenant, proof.source)).rowcount
            if updated != len(changes):
                raise LocatorPlanError('locator_plan_catalog_changed')
            # Filling earlier NULL positions changes retirement eligibility
            # without changing the immutable parent manifest. Pause any old
            # attempt via its epoch and revisit this enabled parent. Schema68
            # and explicitly disabled scopes retain their prior behavior.
            from .chunk_retirement import invalidate_parent_retirement
            invalidate_parent_retirement(query, scope)
            _check_deadline(proof.deadline_at)
    return updated


def apply_parent(store: Any, archive: Any, *, tenant_id: str, source_id: str,
                 native_parent_id: str, limits: PlanLimits | None = None,
                 deadline_at: float | None = None) -> dict[str, Any]:
    """Rerun archive proof, then fill only exact NULL positions atomically."""
    proof = _prove_parent(store, archive, tenant_id=tenant_id, source_id=source_id,
                          native_parent_id=native_parent_id, limits=limits, deadline_at=deadline_at)
    try:
        count = _apply_proof(store, proof)
        return dict(proof.report, status='applied', applied_documents=count)
    except (SearchDeadlineExceeded, LocatorPlanError) as error:
        failure = error
    except psycopg.errors.LockNotAvailable:
        failure = LocatorPlanError('locator_plan_lock_busy')
    except psycopg.errors.QueryCanceled:
        failure = SearchDeadlineExceeded()
    except Exception:
        failure = LocatorPlanError('locator_plan_apply_unavailable')
    failure.archive_gets = proof.report['archive_gets']
    failure.archive_bytes = proof.report['archive_bytes']
    failure.expected_archive_gets = proof.report['archive_gets']
    failure.expected_archive_bytes = proof.report['expected_archive_bytes']
    raise failure from None
