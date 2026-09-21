"""Explicit, reviewed current-chunk retirement; archive proof never uses fallback."""
from __future__ import annotations

import hashlib
import math
import os
from pathlib import Path
import stat
import time

import orjson
import psycopg

from .chunk_bodies import chunk_catalog_snapshot, read_archived_chunks
from .canonical_text import MAX_CANONICAL_TEXT_BYTES
from .db import SearchDeadlineExceeded
from .evidence_projection import DOCUMENT_ID_RE
from .logical_evidence import IDENTITY_RE

MAX_DOCUMENTS = 8
MAX_PLAN_BYTES = 8 * 1024 * 1024
PLAN_CONTRACT = 'recall.chunk-retirement-plan.v1'
EMPTY_SHA256 = hashlib.sha256(b'').hexdigest()
_CHUNKS_SQL = '''SELECT document_id,ordinal,receipt,text_sha256,text_redacted
    FROM canonical_chunks WHERE tenant_id=%s AND source_id=%s AND document_id=ANY(%s)
    AND deleted_at IS NULL ORDER BY document_id,ordinal'''


class ChunkRetirementError(RuntimeError):
    def __init__(self, code='chunk_retirement_unavailable'):
        self.error_code = code
        super().__init__(code)


def _check(deadline_at):
    if time.monotonic() >= deadline_at:
        raise SearchDeadlineExceeded()


def _digest(value):
    return hashlib.sha256(orjson.dumps(value, option=orjson.OPT_SORT_KEYS)).hexdigest()


def _capture(store, *, tenant_id, source_id, document_ids, deadline_at,
             connection=None, lock_chunks=False):
    if connection is None:
        with store.connect() as opened:
            return _capture(store, tenant_id=tenant_id, source_id=source_id,
                document_ids=document_ids, deadline_at=deadline_at, connection=opened)
    catalog = chunk_catalog_snapshot(store, tenant_id=tenant_id, source_ids=(source_id,),
        document_ids=document_ids, deadline_at=deadline_at, connection=connection)
    if set(catalog) != {(source_id, document) for document in document_ids}:
        raise ChunkRetirementError('chunk_retirement_target_ineligible')
    for row in catalog.values():
        if (row['manifest'] is None or type(row['body_record_ordinal']) is not int
                or row['body_record_ordinal'] < 0 or type(row['body_record_count']) is not int
                or row['body_record_count'] < 1):
            raise ChunkRetirementError('chunk_retirement_locator_required')
    oversized = store._execute_bounded(connection, '''
        SELECT document_id FROM canonical_chunks
        WHERE tenant_id=%s AND source_id=%s AND document_id=ANY(%s) AND deleted_at IS NULL
        GROUP BY document_id HAVING sum(octet_length(text_redacted))>%s LIMIT 1
    ''', (tenant_id, source_id, list(document_ids), MAX_CANONICAL_TEXT_BYTES), deadline_at).fetchone()
    if oversized is not None:
        raise ChunkRetirementError('chunk_retirement_body_budget_exceeded')
    chunks = store._execute_bounded(connection, _CHUNKS_SQL +
        (' FOR UPDATE NOWAIT' if lock_chunks else ''),
        (tenant_id, source_id, list(document_ids)), deadline_at).fetchall()
    by_document = {document: [] for document in document_ids}
    for chunk in chunks:
        by_document[chunk['document_id']].append(chunk)
    for row in catalog.values():
        metadata = [{key: chunk[key] for key in ('ordinal', 'receipt', 'text_sha256')}
                    for chunk in by_document[row['document_id']]]
        if not metadata or metadata != row['chunks']:
            raise ChunkRetirementError('chunk_retirement_proof_changed')
    _check(deadline_at)
    return catalog, by_document


def _plan(catalog, chunks, profile, operation):
    documents, signatures = [], []
    for key, row in sorted(catalog.items()):
        current = []
        for chunk in chunks[key[1]]:
            encoded = chunk['text_redacted'].encode()
            digest = hashlib.sha256(encoded).hexdigest()
            if encoded and digest != chunk['text_sha256']:
                raise ChunkRetirementError('chunk_retirement_current_body_invalid')
            current.append(dict(ordinal=chunk['ordinal'], receipt=chunk['receipt'],
                text_sha256=chunk['text_sha256'], current_bytes=len(encoded), current_sha256=digest))
        documents.append(dict(document_id=row['document_id'], native_id=row['native_id'],
            revision=row['revision'], text_sha256=row['text_sha256'],
            body_record_ordinal=row['body_record_ordinal'], body_record_count=row['body_record_count'],
            chunks=current))
        signatures.append(current)
    first = next(iter(catalog.values()))
    return dict(contract=PLAN_CONTRACT, schema_version=1, tenant_id=first['tenant_id'],
        source_id=first['source_id'], runtime_profile=profile, operation=operation, documents=documents,
        proof_sha256=_digest(dict(catalog=[row for _, row in sorted(catalog.items())],
                                  current_chunks=signatures, runtime_profile=profile, operation=operation)))


def _apply_verified(store, *, catalog, plan, tenant_id, source_id, document_ids, deadline_at, restored=None):
    if os.environ.get('RECALL_CHUNK_BODY_READS', 'postgres') != 'archive':
        raise ChunkRetirementError('chunk_retirement_archive_reads_required')
    with store.connect() as connection, connection.transaction():
        # These are exactly the ingest/forget native locks. A busy writer or
        # another clearer causes a whole-batch refusal, never a partial clear.
        for key in sorted({f"v2\x1f{tenant_id}\x1f{source_id}\x1f{row['native_id']}" for row in catalog.values()}):
            locked = store._execute_bounded(connection,
                'SELECT pg_try_advisory_xact_lock(hashtextextended(%s,0)) AS locked',
                (key,), deadline_at).fetchone()['locked']
            if not locked:
                raise ChunkRetirementError('chunk_retirement_lock_busy')
        locked_docs = store._execute_bounded(connection, '''
            SELECT document_id FROM canonical_documents
            WHERE tenant_id=%s AND source_id=%s AND document_id=ANY(%s)
            AND is_current AND deleted_at IS NULL ORDER BY document_id FOR UPDATE NOWAIT
        ''', (tenant_id, source_id, list(document_ids)), deadline_at).fetchall()
        if len(locked_docs) != len(document_ids):
            raise ChunkRetirementError('chunk_retirement_proof_changed')
        parents = sorted({row['manifest']['logical_document_id'] for row in catalog.values()})
        locked_parents = store._execute_bounded(connection, '''
            SELECT logical_document_id FROM canonical_evidence_documents
            WHERE tenant_id=%s AND source_id=%s AND logical_document_id=ANY(%s)
            ORDER BY logical_document_id FOR SHARE NOWAIT
        ''', (tenant_id, source_id, parents), deadline_at).fetchall()
        if len(locked_parents) != len(parents):
            raise ChunkRetirementError('chunk_retirement_proof_changed')
        current_catalog, current_chunks = _capture(store, tenant_id=tenant_id, source_id=source_id,
            document_ids=document_ids, deadline_at=deadline_at, connection=connection, lock_chunks=True)
        if _plan(current_catalog, current_chunks, plan['runtime_profile'], plan['operation']) != plan:
            raise ChunkRetirementError('chunk_retirement_proof_changed')
        if plan['operation'] == 'restore':
            for document in plan['documents']:
                missing = {chunk['ordinal'] for chunk in document['chunks']
                    if chunk['current_bytes'] == 0 and chunk['text_sha256'] != EMPTY_SHA256}
                if not missing:
                    continue
                rows = [chunk for chunk in restored[(source_id, document['document_id'])]
                        if chunk['ordinal'] in missing]
                changed = store._execute_bounded(connection, """
                    UPDATE canonical_chunks AS chunk SET text_redacted=body.text
                    FROM unnest(%s::int[],%s::text[]) AS body(ordinal,text)
                    WHERE chunk.tenant_id=%s AND chunk.source_id=%s AND chunk.document_id=%s
                    AND chunk.ordinal=body.ordinal AND chunk.deleted_at IS NULL AND chunk.text_redacted=''
                """, ([row['ordinal'] for row in rows], [row['text_redacted'] for row in rows],
                      tenant_id, source_id, document['document_id']), deadline_at)
                if changed.rowcount != len(missing):
                    raise ChunkRetirementError('chunk_retirement_proof_changed')
        else:
            expected = sum(chunk['current_bytes'] > 0 for document in plan['documents'] for chunk in document['chunks'])
            changed = store._execute_bounded(connection, """
                UPDATE canonical_chunks SET text_redacted=''
                WHERE tenant_id=%s AND source_id=%s AND document_id=ANY(%s)
                AND deleted_at IS NULL AND text_redacted<>''
            """, (tenant_id, source_id, list(document_ids)), deadline_at)
            if changed.rowcount != expected:
                raise ChunkRetirementError('chunk_retirement_proof_changed')
        _check(deadline_at)


def retire_current_chunks(store, archive, *, tenant_id, source_id, document_ids,
                          apply=False, reviewed_plan=None, deadline_at=None, restore=False):
    """Dry-run by default; apply re-proves every byte and requires its saved plan.

    One cooperative deadline bounds SQL/archive work and is checked before commit.
    Pool acquisition, DNS and COMMIT are not forcibly deadline-cancelled.
    No archive IO occurs inside the write transaction; historical rows survive.
    Restore uses the same proof/locks. Restore ALL retired targets before switching
    reads back to PostgreSQL; changing the profile alone is not a rollback.
    """
    if (not isinstance(tenant_id, str) or not IDENTITY_RE.fullmatch(tenant_id)
            or not isinstance(source_id, str) or not IDENTITY_RE.fullmatch(source_id)
            or not isinstance(document_ids, tuple) or not 1 <= len(document_ids) <= MAX_DOCUMENTS
            or any(not isinstance(doc, str) or not DOCUMENT_ID_RE.fullmatch(doc) for doc in document_ids)
            or len(set(document_ids)) != len(document_ids)
            or type(apply) is not bool or type(restore) is not bool):
        raise ChunkRetirementError('chunk_retirement_targets_invalid')
    mode = os.environ.get('RECALL_CHUNK_BODY_READS', 'postgres')
    if mode not in {'postgres', 'archive'} or (apply and mode != 'archive'):
        raise ChunkRetirementError('chunk_retirement_archive_reads_required')
    if apply and not isinstance(reviewed_plan, dict):
        raise ChunkRetirementError('chunk_retirement_reviewed_plan_required')
    if deadline_at is None:
        deadline_at = time.monotonic() + 20
    if type(deadline_at) not in {float, int} or not math.isfinite(deadline_at):
        raise ChunkRetirementError('chunk_retirement_deadline_invalid')
    document_ids = tuple(sorted(document_ids))
    try:
        _check(deadline_at)
        with store.connect() as connection:
            schema = store._execute_bounded(connection,
                'SELECT 1 FROM schema_migrations WHERE version=68', (), deadline_at).fetchone()
            if schema is None:
                raise ChunkRetirementError('chunk_retirement_schema_required')
        catalog, current = _capture(store, tenant_id=tenant_id, source_id=source_id,
                                   document_ids=document_ids, deadline_at=deadline_at)
        archived = read_archived_chunks(store, archive, tenant_id=tenant_id, source_ids=(source_id,),
                                        document_ids=document_ids, deadline_at=deadline_at)
        if set(archived) != set(catalog):
            raise ChunkRetirementError('chunk_retirement_archive_proof_required')
        for key, chunks in archived.items():
            stored = current[key[1]]
            if len(stored) != len(chunks) or any(
                actual['ordinal'] != expected['ordinal'] or actual['receipt'] != expected['receipt']
                or hashlib.sha256(actual['text_redacted'].encode()).hexdigest() != expected['text_sha256']
                or (expected['text_redacted'] and actual['text_redacted'] != expected['text_redacted'])
                for actual, expected in zip(chunks, stored, strict=True)
            ):
                raise ChunkRetirementError('chunk_retirement_archive_proof_required')
        _check(deadline_at)
        after = chunk_catalog_snapshot(store, tenant_id=tenant_id, source_ids=(source_id,),
                                      document_ids=document_ids, deadline_at=deadline_at)
        if after != catalog:
            raise ChunkRetirementError('chunk_retirement_proof_changed')
        plan = _plan(catalog, current, dict(chunk_body_reads=mode, locator_schema=68),
                     'restore' if restore else 'clear')
        _check(deadline_at)
        affected = [actual for key, rows in archived.items()
                    for actual, old in zip(rows, current[key[1]], strict=True)
                    if (restore and not old['text_redacted'] and actual['text_redacted'])
                    or (not restore and old['text_redacted'])]
        count, byte_count = len(affected), sum(len(row['text_redacted'].encode()) for row in affected)
        # Clearing carries only metadata into the transaction. Restoration keeps
        # the already verified <=64MiB archive result, never performs network IO.
        if not restore:
            archived = None
        del current, chunks, stored, affected
        if apply:
            if plan != reviewed_plan:
                raise ChunkRetirementError('chunk_retirement_plan_changed')
            _apply_verified(store, catalog=catalog, plan=plan, tenant_id=tenant_id, source_id=source_id,
                            document_ids=document_ids, deadline_at=deadline_at, restored=archived)
        return dict(status='applied' if apply else 'dry_run', documents=len(plan['documents']),
            chunks=count, bytes=byte_count,
            proof_sha256=plan['proof_sha256'], plan=plan)
    except ChunkRetirementError:
        raise
    except SearchDeadlineExceeded:
        raise ChunkRetirementError('chunk_retirement_deadline_exceeded') from None
    except psycopg.errors.LockNotAvailable:
        raise ChunkRetirementError('chunk_retirement_lock_busy') from None
    except Exception:
        raise ChunkRetirementError() from None


def public_report(result):
    return {key: result[key] for key in ('status', 'documents', 'chunks', 'bytes', 'proof_sha256')}


def write_private_plan(path: Path, plan):
    try:
        payload = orjson.dumps(plan, option=orjson.OPT_SORT_KEYS | orjson.OPT_INDENT_2)
        if len(payload) > MAX_PLAN_BYTES:
            raise ChunkRetirementError('chunk_retirement_plan_invalid')
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        with os.fdopen(descriptor, 'wb') as output:
            output.write(payload)
            output.flush()
            os.fsync(output.fileno())
    except (OSError, ValueError, TypeError):
        raise ChunkRetirementError('chunk_retirement_plan_unavailable') from None


def read_private_plan(path: Path):
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
        with os.fdopen(descriptor, 'rb') as source:
            info = os.fstat(source.fileno())
            if not stat.S_ISREG(info.st_mode) or info.st_mode & 0o077 or info.st_uid != os.getuid():
                raise ChunkRetirementError('chunk_retirement_plan_private_required')
            payload = source.read(MAX_PLAN_BYTES + 1)
            if len(payload) > MAX_PLAN_BYTES:
                raise ChunkRetirementError('chunk_retirement_plan_invalid')
            plan = orjson.loads(payload)
            if not isinstance(plan, dict):
                raise ChunkRetirementError('chunk_retirement_plan_invalid')
            return plan
    except (OSError, ValueError, TypeError):
        raise ChunkRetirementError('chunk_retirement_plan_unavailable') from None
