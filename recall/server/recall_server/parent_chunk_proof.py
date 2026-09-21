"""Private metadata-only parent proof; no persisted plan is accepted as proof."""
from __future__ import annotations

from collections import Counter
from contextlib import contextmanager
import hashlib
import os
from pathlib import Path
import shutil
import sqlite3
import tempfile

import orjson

from .chunk_bodies import _EXCLUDED_TYPES, _check_deadline, _record_location, _verified_body
from .locator_backfill_plan import _DOCUMENTS_SQL, _MeteredArchive, _PARENT_SQL
from .logical_body_proof import iter_parent_bodies

_METADATA_FIELDS = frozenset({'tenant_id', 'source_id', 'document_id', 'native_id', 'revision',
    'text_sha256', 'body_record_ordinal', 'body_record_count', 'kind', 'raw_media_type',
    'structural_types', 'chunks', 'pending', 'pg_body_bytes'})


def _error(code='parent_retirement_proof_unavailable'):
    # Keep the operation's error boundary and mutations in chunk_retirement.
    from .chunk_retirement import ChunkRetirementError
    return ChunkRetirementError(code)


class ParentMetadataSpool:
    """A disposable indexed metadata spool, never source bodies or durable proof."""
    def __init__(self, *, max_bytes):
        self.directory = tempfile.TemporaryDirectory(prefix='recall-parent-proof-')
        self.index = None
        try:
            if shutil.disk_usage(self.directory.name).free < max_bytes + 64 * 1024**2:
                raise _error('parent_retirement_spool_space')
            path = Path(self.directory.name) / 'metadata.sqlite'
            self.index = sqlite3.connect(path)
            os.chmod(path, 0o600)
            self.index.execute('PRAGMA cache_size=-2048')
            self.index.execute('PRAGMA journal_mode=OFF')  # Attempt-local, discarded on any failure.
            self.index.execute('PRAGMA temp_store=FILE')
            self.index.execute(f'PRAGMA max_page_count={max_bytes // 4096}')
            self.index.execute('CREATE TABLE documents(native TEXT PRIMARY KEY,document TEXT UNIQUE,payload BLOB NOT NULL,ordinal INTEGER,verified INTEGER NOT NULL DEFAULT 0,proposed_count INTEGER)')
            self.index.execute('CREATE INDEX documents_position ON documents(verified,ordinal,document)')
            self.index.execute('CREATE TABLE seen(native TEXT PRIMARY KEY)')
        except BaseException:
            self.close()
            raise

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def close(self):
        if self.index is not None:
            self.index.close()
            self.index = None
        self.directory.cleanup()

    def add_document(self, row):
        if set(row) != _METADATA_FIELDS or any(set(chunk) != {'ordinal', 'receipt', 'text_sha256', 'pg_bytes'} for chunk in row['chunks']):
            raise _error('parent_retirement_metadata_invalid')
        payload = orjson.dumps(row)
        if len(payload) > 1024**2:
            raise _error('parent_retirement_metadata_budget')
        try:
            self.index.execute('INSERT INTO documents(native,document,payload,ordinal) VALUES(?,?,?,?)',
                               (row['native_id'], row['document_id'], payload, row['body_record_ordinal']))
        except sqlite3.Error:
            raise _error('parent_retirement_metadata_invalid') from None

    def claim_native(self, native):
        try:
            self.index.execute('INSERT INTO seen VALUES(?)', (native,))
        except sqlite3.Error:
            raise _error('parent_retirement_metadata_invalid') from None

    def get_document(self, native):
        found = self.index.execute('SELECT payload FROM documents WHERE native=?', (native,)).fetchone()
        return None if found is None else orjson.loads(found[0])

    def mark_verified(self, row, location=None):
        if self.index.execute('UPDATE documents SET verified=1,ordinal=coalesce(?,ordinal),proposed_count=? WHERE native=? AND verified=0',
                              (None if location is None else location[0], None if location is None else location[1], row['native_id'])).rowcount != 1:
            raise _error('parent_retirement_metadata_invalid')

    def unseen(self):
        for (payload,) in self.index.execute('SELECT payload FROM documents LEFT JOIN seen USING(native) WHERE seen.native IS NULL'):
            yield orjson.loads(payload)

    def verified(self):
        for (payload,) in self.index.execute('SELECT payload FROM documents WHERE verified=1 ORDER BY ordinal,document'):
            yield orjson.loads(payload)


    def proposals(self):
        for payload, ordinal, count in self.index.execute('SELECT payload,ordinal,proposed_count FROM documents WHERE proposed_count IS NOT NULL ORDER BY ordinal,document'):
            yield dict(orjson.loads(payload), record_ordinal=ordinal, record_count=count)


def manifest_identity(manifest):
    fields = {'tenant_id', 'source_id', 'logical_document_id', 'native_parent_id', 'revision',
              'evidence_id', 'document_content_sha256', 'record_count', 'receipt_count', 'part_count'}
    return {key: value for key, value in manifest.items() if key in fields or key.startswith('manifest_')}


def parent_retirement_plan(scope, manifest):
    """Same exact manifest plan for manual review and explicitly enabled policy."""
    plan = dict(contract='recall.parent-chunk-retirement-plan.v1', tenant_id=scope[0], source_id=scope[1],
                native_parent_id=scope[2], manifest=manifest_identity(manifest), operation='clear')
    plan['proof_sha256'] = hashlib.sha256(orjson.dumps(plan, option=orjson.OPT_SORT_KEYS)).hexdigest()
    return plan


def read_parent_catalog(store, connection, scope, deadline_at, *, lock=False):
    sql = _PARENT_SQL + (' FOR SHARE OF evidence NOWAIT' if lock else '')
    found = store._execute_bounded(connection, sql, scope, deadline_at).fetchone()
    if found is None:
        raise _error('parent_retirement_parent_missing')
    return found


def _capture(store, spool, scope, limits, deadline_at):
    with store.connect() as connection, connection.transaction():
        connection.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
        catalog = read_parent_catalog(store, connection, scope, deadline_at)
        manifest = catalog['manifest']
        parts = store._execute_bounded(connection, '''SELECT * FROM canonical_evidence_document_parts
            WHERE tenant_id=%s AND source_id=%s AND logical_document_id=%s AND revision=%s
            ORDER BY part_ordinal LIMIT %s''', (*scope[:2], manifest['logical_document_id'], manifest['revision'], limits.max_parts + 1), deadline_at).fetchall()
        if len(parts) > limits.max_parts:
            raise _error('parent_retirement_metadata_budget')
        sql = '''SELECT selected.*,items.chunks FROM (''' + _DOCUMENTS_SQL + ''') selected
            CROSS JOIN LATERAL (SELECT coalesce(jsonb_agg(to_jsonb(item) ORDER BY item.ordinal),'[]'::jsonb) AS chunks
                FROM (SELECT ordinal,receipt,text_sha256,octet_length(text_redacted) AS pg_bytes
                      FROM canonical_chunks WHERE tenant_id=selected.tenant_id AND source_id=selected.source_id
                        AND document_id=selected.document_id AND deleted_at IS NULL ORDER BY ordinal LIMIT %s) item) items'''
        count, chunk_count = 0, 0
        with connection.cursor(name='retirement_metadata') as cursor:
            store._set_statement_deadline(connection, deadline_at)
            cursor.execute(sql, (*scope, limits.max_documents + 1, limits.batch_chunks + 1))
            while True:
                store._set_statement_deadline(connection, deadline_at)
                rows = cursor.fetchmany(32)
                if not rows:
                    break
                for row in rows:
                    count += 1
                    chunk_count += len(row['chunks'])
                    if count > limits.max_documents or chunk_count > limits.max_chunks or len(row['chunks']) > limits.batch_chunks:
                        raise _error('parent_retirement_metadata_budget')
                    row.update(pending=catalog['queue'] is not None,
                               pg_body_bytes=sum(chunk['pg_bytes'] for chunk in row['chunks']))
                    spool.add_document(row)
                    _check_deadline(deadline_at)
        spool.index.commit()
    return catalog, parts, count, chunk_count


@contextmanager
def prove_parent_chunks(store, archive, *, scope, limits, deadline_at, purpose="retire"):
    """Yield a complete sealed metadata proof only after all bytes are verified."""
    if purpose not in {"retire", "locate"}:
        raise _error("parent_proof_purpose_invalid")
    with ParentMetadataSpool(max_bytes=limits.max_spool_bytes) as spool:
        catalog, parts, count, chunk_count = _capture(store, spool, scope, limits, deadline_at)
        manifest = catalog['manifest']
        meter = _MeteredArchive(archive, deadline_at)
        excluded, eligible, eligible_bytes, groups = Counter(), 0, 0, 0
        for first, segments in iter_parent_bodies(meter, tenant_id=scope[0], source_id=scope[1],
                native_parent_id=scope[2], manifest=manifest, parts=parts, max_records=limits.max_records,
                max_bytes=limits.max_archive_bytes, deadline_at=deadline_at):
            groups += 1
            spool.claim_native(first.event_native_id)
            row = spool.get_document(first.event_native_id)
            reason = ('not_current' if row is None else
                      'oversized' if row['raw_media_type'] == 'application/vnd.recall.oversized-record+gzip' else
                      'structural' if _EXCLUDED_TYPES.intersection(row['structural_types']) else
                      'unlocated' if purpose == 'retire' and _record_location(row) is None else
                      'event_body_budget' if segments is None else None)
            if reason is None:
                if _verified_body(row, segments, _record_location(row), ()) is None:
                    if purpose == 'retire':
                        raise _error('parent_retirement_archive_proof_required')
                    excluded['pending_revision' if list(first.receipts) != [chunk['receipt'] for chunk in row['chunks']]
                             else 'historical_chunk_boundaries'] += 1
                    continue
                proposed = ((first.ordinal, first.segment_count)
                            if purpose == 'locate' and _record_location(row) is None else None)
                spool.mark_verified(row, proposed)
                eligible += 1
                eligible_bytes += row['pg_body_bytes']
            else:
                excluded[reason] += 1
        if groups == 0:
            raise _error('parent_retirement_archive_proof_required')
        first = segments = None
        for row in spool.unseen():
            if row['raw_media_type'] == 'application/vnd.recall.oversized-record+gzip':
                excluded['oversized'] += 1
            elif _EXCLUDED_TYPES.intersection(row['structural_types']):
                excluded['structural'] += 1
            elif _record_location(row) is None and (purpose == 'retire' or row['pending']):
                excluded['unlocated' if purpose == 'retire' else 'pending_new_document'] += 1
            else:
                raise _error('parent_retirement_archive_proof_required')
        spool.index.commit()
        identity = manifest_identity(manifest)
        with store.connect() as connection:
            current = read_parent_catalog(store, connection, scope, deadline_at)
            if (manifest_identity(current['manifest']) != identity or purpose == 'locate'
                    and current['manifest']['created_at'] != manifest['created_at']):
                raise _error('parent_retirement_parent_changed')
        _check_deadline(deadline_at)
        plan = parent_retirement_plan(scope, identity)
        extra = (dict(parts=parts, catalog_created_at=manifest['created_at']) if purpose == 'locate' else {})
        yield dict(spool=spool, manifest=identity, plan=plan, current_documents=count, current_chunks=chunk_count, eligible_documents=eligible,
                   eligible_utf8_bytes=eligible_bytes, excluded=dict(excluded), archive_gets=meter.gets,
                   archive_bytes=meter.bytes, **extra)
