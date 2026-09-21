"""Explicit, reviewed current-chunk retirement; archive proof never uses fallback."""
from __future__ import annotations

from dataclasses import dataclass

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
            # Restoration pauses later bulk work for these exact parents. The
            # ledger is optional for the older explicit schema68 command.
            ledger = store._execute_bounded(connection,
                "SELECT to_regclass('canonical_chunk_retirement_progress') AS ledger", (), deadline_at).fetchone()['ledger']
            if ledger is not None:
                store._execute_bounded(connection, '''INSERT INTO canonical_chunk_retirement_progress
                    (tenant_id,source_id,native_parent_id,logical_document_id,enabled,status)
                    SELECT tenant_id,source_id,native_parent_id,logical_document_id,false,'disabled'
                    FROM canonical_evidence_documents
                    WHERE tenant_id=%s AND source_id=%s AND logical_document_id=ANY(%s)
                    ON CONFLICT(tenant_id,source_id,native_parent_id) DO UPDATE SET
                    enabled=false,scope_epoch=canonical_chunk_retirement_progress.scope_epoch+1,
                    status='disabled',updated_at=clock_timestamp()''',
                    (tenant_id, source_id, parents), deadline_at)
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


# Parent jobs share the mutation owner above, but carry metadata proof instead
# of a retained map of archive bodies. They do not run from a worker loop yet.
@dataclass(frozen=True)
class ParentRetirementLimits:
    batch_documents: int = 64
    batch_chunks: int = 4096
    hash_bytes: int = 8 * 1024**2
    max_batches: int = 1000
    max_clear_bytes: int = 1024**3
    max_documents: int = 2_000_000
    max_chunks: int = 8_000_000
    max_parts: int = 8192
    max_records: int = 4_000_000
    max_archive_bytes: int = 8 * 1024**3
    max_spool_bytes: int = 1024**3

    def __post_init__(self):
        maxima = dict(batch_documents=256, batch_chunks=8192, hash_bytes=32 * 1024**2,
            max_batches=100_000, max_clear_bytes=1024**4, max_documents=4_000_000, max_chunks=32_000_000,
            max_parts=32_768, max_records=8_000_000, max_archive_bytes=64 * 1024**3,
            max_spool_bytes=8 * 1024**3)
        if any(type(getattr(self, key)) is not int or not 1 <= getattr(self, key) <= maximum
               for key, maximum in maxima.items()) or self.max_spool_bytes < 128 * 1024:
            raise ChunkRetirementError('parent_retirement_limits_invalid')


def _parent_scope(tenant_id, source_id, native_parent_id):
    scope = tenant_id, source_id, native_parent_id
    if any(not isinstance(value, str) or not IDENTITY_RE.fullmatch(value) for value in scope):
        raise ChunkRetirementError('parent_retirement_scope_invalid')
    return scope


def _parent_deadline(deadline_at):
    now = time.monotonic()
    if deadline_at is None:
        return now + 300
    if type(deadline_at) not in (int, float) or not math.isfinite(deadline_at) or deadline_at > now + 3600:
        raise ChunkRetirementError('parent_retirement_deadline_invalid')
    if now >= deadline_at:
        raise ChunkRetirementError('parent_retirement_deadline_exceeded')
    return deadline_at


def require_retirement_owner(store, connection, scope, principal_id, deadline_at):
    """Lock the current source/grant through a bounded body transaction."""
    found = store._execute_bounded(connection, """SELECT source.source_id
        FROM canonical_sources source JOIN canonical_source_grants grant_row USING(tenant_id,source_id)
        WHERE source.tenant_id=%s AND source.source_id=%s AND source.owner_principal_id=%s
          AND grant_row.principal_id=%s AND grant_row.permission='owner'
        FOR SHARE OF source,grant_row NOWAIT""", (*scope, principal_id, principal_id), deadline_at).fetchone()
    if found is None:
        raise ChunkRetirementError('retirement_owner_required')


def _parent_progress(store, connection, scope, deadline_at, *, lock=False):
    sql = '''SELECT * FROM canonical_chunk_retirement_progress
             WHERE tenant_id=%s AND source_id=%s AND native_parent_id=%s'''
    return store._execute_bounded(connection, sql + (' FOR UPDATE NOWAIT' if lock else ''), scope, deadline_at).fetchone()


def set_parent_retirement_enabled(store, *, tenant_id, source_id, native_parent_id, enabled, deadline_at=None):
    """Explicit exact-parent switch. Enabling resets the scheduling cursor only."""
    from .parent_chunk_proof import read_parent_catalog
    scope = _parent_scope(tenant_id, source_id, native_parent_id)
    if type(enabled) is not bool:
        raise ChunkRetirementError('parent_retirement_scope_invalid')
    deadline_at = _parent_deadline(deadline_at)
    with store.connect() as connection, connection.transaction():
        catalog = read_parent_catalog(store, connection, scope, deadline_at, lock=True)
        store._execute_bounded(connection, '''INSERT INTO canonical_chunk_retirement_progress
            (tenant_id,source_id,native_parent_id,logical_document_id,enabled,status)
            VALUES(%s,%s,%s,%s,%s,%s)
            ON CONFLICT(tenant_id,source_id,native_parent_id) DO UPDATE SET
              enabled=excluded.enabled,scope_epoch=canonical_chunk_retirement_progress.scope_epoch+1,
              status=excluded.status,manifest_artifact_id=NULL,
              last_record_ordinal=-1,updated_at=clock_timestamp()''',
            (*scope, catalog['manifest']['logical_document_id'], enabled, 'pending' if enabled else 'disabled'), deadline_at)
        _check(deadline_at)



def invalidate_parent_retirement(query, scope):
    """Call with the parent catalog already locked; never enable a scope."""
    ledger = query("SELECT to_regclass('canonical_chunk_retirement_progress') AS ledger", ()).fetchone()['ledger']
    if ledger is not None:
        query("""UPDATE canonical_chunk_retirement_progress
            SET status='pending',scope_epoch=scope_epoch+1,manifest_artifact_id=NULL,
                last_record_ordinal=-1,updated_at=clock_timestamp()
            WHERE tenant_id=%s AND source_id=%s AND native_parent_id=%s AND enabled""", scope)


def _try_parent_native_locks(query, scope, rows):
    keys = sorted(f'v2\x1f{scope[0]}\x1f{scope[1]}\x1f{row["native_id"]}' for row in rows)
    if not keys:
        return True
    # Preserve Python's existing key order regardless of database collation.
    # PostgreSQL evaluates this volatile output after ORDER BY ordinality.
    locks = query('''SELECT ordinality,
        pg_try_advisory_xact_lock(hashtextextended(lock_key,0)) AS locked
        FROM unnest(%s::text[]) WITH ORDINALITY AS keys(lock_key,ordinality)
        ORDER BY ordinality''', (keys,)).fetchall()
    return len(locks) == len(keys) and all(
        row['ordinality'] == ordinal and row['locked'] is True
        for ordinal, row in enumerate(locks, 1)
    )


def _retire_parent_batch(store, *, proof, rows, scope, limits, deadline_at, complete, remaining_bytes, owner_principal_id=None):
    from .parent_chunk_proof import manifest_identity, read_parent_catalog
    if os.environ.get('RECALL_CHUNK_BODY_READS', 'postgres') != 'archive':
        raise ChunkRetirementError('chunk_retirement_archive_reads_required')
    manifest = proof['manifest']
    documents = {row['document_id']: row for row in rows}
    document_ids = sorted(documents)
    hashed_bytes = cleared_bytes = cleared_chunks = cleared_documents = 0
    hash_ms = 0.0
    started = time.monotonic()
    with store.connect() as connection, connection.transaction():
        def query(sql, values=()):
            return store._execute_bounded(connection, sql, values, deadline_at)
        if owner_principal_id is not None:
            require_retirement_owner(store, connection, scope[:2], owner_principal_id, deadline_at)
        if not _try_parent_native_locks(query, scope, rows):
            raise ChunkRetirementError('parent_retirement_lock_busy')
        locked = query('''SELECT document.tenant_id,document.source_id,document.document_id,document.native_id,
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
                ORDER BY document.document_id FOR UPDATE OF document NOWAIT FOR SHARE OF event,artifact NOWAIT''',
                (*scope[:2], document_ids, scope[2])).fetchall()
        if len(locked) != len(documents) or any(any(row[key] != documents[row['document_id']][key] for key in row) for row in locked):
            raise ChunkRetirementError('parent_retirement_document_changed')
        current = read_parent_catalog(store, connection, scope, deadline_at, lock=True)
        if manifest_identity(current['manifest']) != manifest:
            raise ChunkRetirementError('parent_retirement_parent_changed')
        progress = _parent_progress(store, connection, scope, deadline_at, lock=True)
        if not progress or not progress['enabled'] or progress['scope_epoch'] != proof['scope_epoch']:
            raise ChunkRetirementError('parent_retirement_disabled')
        chunks = query('''SELECT document_id,ordinal,receipt,text_sha256,octet_length(text_redacted) AS pg_bytes
            FROM canonical_chunks WHERE tenant_id=%s AND source_id=%s AND document_id=ANY(%s)
              AND deleted_at IS NULL ORDER BY document_id,ordinal FOR UPDATE NOWAIT''', (*scope[:2], document_ids)).fetchall()
        expected = {(row['document_id'], chunk['ordinal']): chunk for row in rows for chunk in row['chunks']}
        if len(chunks) != len(expected) or any(
            (chunk['document_id'], chunk['ordinal']) not in expected
            or any(chunk[key] != expected[(chunk['document_id'], chunk['ordinal'])][key] for key in ('ordinal','receipt','text_sha256'))
            for chunk in chunks):
            raise ChunkRetirementError('parent_retirement_chunk_changed')
        hashed_bytes = sum(chunk['pg_bytes'] for chunk in chunks)
        if len(chunks) > limits.batch_chunks or hashed_bytes > min(limits.hash_bytes, remaining_bytes):
            raise ChunkRetirementError('parent_retirement_hash_budget')
        hash_started = time.monotonic()
        actual = query('''SELECT document_id,ordinal,encode(sha256(convert_to(text_redacted,'UTF8')),'hex') AS actual_sha
            FROM canonical_chunks WHERE tenant_id=%s AND source_id=%s AND document_id=ANY(%s)
              AND deleted_at IS NULL AND text_redacted<>'' ORDER BY document_id,ordinal''', (*scope[:2], document_ids)).fetchall()
        hash_ms = (time.monotonic() - hash_started) * 1000
        if any(row['actual_sha'] != expected[(row['document_id'], row['ordinal'])]['text_sha256'] for row in actual):
            raise ChunkRetirementError('parent_retirement_current_body_invalid')
        cleared_chunks, cleared_documents = len(actual), len({row['document_id'] for row in actual})
        changed = query('''UPDATE canonical_chunks SET text_redacted=''
            WHERE tenant_id=%s AND source_id=%s AND document_id=ANY(%s) AND deleted_at IS NULL AND text_redacted<>'' ''',
            (*scope[:2], document_ids)).rowcount
        if changed != cleared_chunks:
            raise ChunkRetirementError('parent_retirement_chunk_changed')
        cleared_bytes = hashed_bytes
        cursor = max((row['body_record_ordinal'] + row['body_record_count'] - 1 for row in rows), default=-1)
        if progress['manifest_artifact_id'] == manifest['manifest_artifact_id']:
            cursor = max(cursor, progress['last_record_ordinal'])
        query('''UPDATE canonical_chunk_retirement_progress SET manifest_artifact_id=%s,last_record_ordinal=%s,status=%s,
                cumulative_cleared_documents=cumulative_cleared_documents+%s,
                cumulative_cleared_chunks=cumulative_cleared_chunks+%s,
                cumulative_cleared_utf8_bytes=cumulative_cleared_utf8_bytes+%s,updated_at=clock_timestamp()
            WHERE tenant_id=%s AND source_id=%s AND native_parent_id=%s''',
            (manifest['manifest_artifact_id'], cursor, 'complete' if complete else 'partial',
             cleared_documents, cleared_chunks, cleared_bytes, *scope))
        _check(deadline_at)
    return dict(cleared_documents=cleared_documents, cleared_chunks=cleared_chunks,
                cleared_utf8_bytes=cleared_bytes, hashed_utf8_bytes=hashed_bytes,
                hash_ms=hash_ms, sql_ms=(time.monotonic() - started) * 1000)


def retire_parent_chunks(store, archive, *, tenant_id, source_id, native_parent_id,
                         apply=False, reviewed_plan=None, limits=None, deadline_at=None,
                         required_scope_epoch=None, owner_principal_id=None, should_stop=None):
    """Dry-run one exact parent; complete archive proof precedes bounded commits.

    Only attempt-local proof authorizes body updates. The persistent cursor is
    scheduling metadata; a restart proves every parent part again exactly once.
    """
    from .parent_chunk_proof import prove_parent_chunks
    scope = _parent_scope(tenant_id, source_id, native_parent_id)
    limits = ParentRetirementLimits() if limits is None else limits
    if not isinstance(limits, ParentRetirementLimits) or type(apply) is not bool:
        raise ChunkRetirementError('parent_retirement_limits_invalid')
    if (required_scope_epoch is not None and (type(required_scope_epoch) is not int or required_scope_epoch < 1)
            or owner_principal_id is not None and (not isinstance(owner_principal_id, str) or not IDENTITY_RE.fullmatch(owner_principal_id))
            or should_stop is not None and not callable(should_stop)):
        raise ChunkRetirementError('parent_retirement_scope_invalid')
    deadline_at = _parent_deadline(deadline_at)
    totals = dict(cleared_documents=0, cleared_chunks=0, cleared_utf8_bytes=0, hashed_utf8_bytes=0,
                  hash_ms=0.0, sql_ms=0.0, batches=0)
    try:
        with store.connect() as connection:
            schema = store._execute_bounded(connection, 'SELECT 1 FROM schema_migrations WHERE version=69', (), deadline_at).fetchone()
            if schema is None:
                raise ChunkRetirementError('parent_retirement_schema_required')
            progress = _parent_progress(store, connection, scope, deadline_at) if apply else None
        if apply:
            if os.environ.get('RECALL_CHUNK_BODY_READS', 'postgres') != 'archive':
                raise ChunkRetirementError('chunk_retirement_archive_reads_required')
            if not isinstance(reviewed_plan, dict):
                raise ChunkRetirementError('chunk_retirement_reviewed_plan_required')
            if (not progress or not progress['enabled']
                    or required_scope_epoch is not None and progress['scope_epoch'] != required_scope_epoch):
                raise ChunkRetirementError('parent_retirement_disabled')
        with prove_parent_chunks(store, archive, scope=scope, limits=limits, deadline_at=deadline_at) as proof:
            report = {key: value for key, value in proof.items() if key not in {'spool','manifest'}}
            if not apply:
                return dict(report, **totals, status='dry_run', complete=False)
            if proof['plan'] != reviewed_plan:
                raise ChunkRetirementError('chunk_retirement_plan_changed')
            proof['scope_epoch'] = progress['scope_epoch']
            cursor = (progress['last_record_ordinal'] if progress['manifest_artifact_id'] == proof['manifest']['manifest_artifact_id'] else -1)
            rows, chunk_count, byte_count = [], 0, 0
            def commit(complete=False):
                result = _retire_parent_batch(store, proof=proof, rows=rows, scope=scope,
                    limits=limits, deadline_at=min(deadline_at, time.monotonic() + 5), complete=complete,
                    remaining_bytes=limits.max_clear_bytes-totals['cleared_utf8_bytes'], owner_principal_id=owner_principal_id)
                for key, value in result.items():
                    totals[key] += value
                totals['batches'] += 1
            for row in proof['spool'].verified():
                _check(deadline_at)
                if should_stop is not None and should_stop():
                    return dict(report, **totals, status='stopped', complete=False)
                if row['body_record_ordinal'] <= cursor and row['pg_body_bytes'] == 0:
                    continue
                if row['pg_body_bytes'] > limits.hash_bytes:
                    raise ChunkRetirementError('parent_retirement_hash_budget')
                if rows and (len(rows) == limits.batch_documents or chunk_count + len(row['chunks']) > limits.batch_chunks
                             or byte_count + row['pg_body_bytes'] > limits.hash_bytes):
                    commit()
                    rows, chunk_count, byte_count = [], 0, 0
                    if totals['batches'] >= limits.max_batches or totals['cleared_utf8_bytes'] >= limits.max_clear_bytes:
                        return dict(report, **totals, status='partial', complete=False)
                if totals['cleared_utf8_bytes'] + byte_count + row['pg_body_bytes'] > limits.max_clear_bytes:
                    if rows:
                        commit()
                    return dict(report, **totals, status='partial', complete=False)
                rows.append(row)
                chunk_count += len(row['chunks'])
                byte_count += row['pg_body_bytes']
            if should_stop is not None and should_stop():
                return dict(report, **totals, status='stopped', complete=False)
            commit(complete=True)
            return dict(report, **totals, status='applied', complete=True)
    except Exception as error:
        if isinstance(error, ChunkRetirementError):
            failure = error
        elif isinstance(error, SearchDeadlineExceeded):
            failure = ChunkRetirementError('parent_retirement_deadline_exceeded')
        elif isinstance(error, psycopg.errors.LockNotAvailable):
            failure = ChunkRetirementError('parent_retirement_lock_busy')
        else:
            failure = ChunkRetirementError('parent_retirement_unavailable')
        failure.committed = totals
        raise failure from None
