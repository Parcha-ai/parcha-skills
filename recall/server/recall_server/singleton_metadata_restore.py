"""Explicit, bounded reverse path for the inactive singleton metadata contract."""
from __future__ import annotations

import hashlib
import math
import time

import orjson

from .canonical import _opaque
from .canonical_history import restore_outgoing, stage_history_rows
from .chunk_bodies import chunk_catalog_snapshot
from .chunk_retirement import (
    ChunkRetirementError, _try_parent_native_locks,
    disable_parent_retirement_after_restore, require_retirement_owner,
)
from .db import V2_AUTHORITY_RE
from .parent_chunk_proof import read_parent_catalog

MAX_DOCUMENTS = 32
_DOCUMENTS = '''SELECT document_id,native_id,revision,text_sha256,created_at::text AS created_at
    FROM canonical_documents WHERE tenant_id=%s AND source_id=%s AND document_id=ANY(%s)
      AND is_current AND deleted_at IS NULL AND chunk_receipt_profile=1
    ORDER BY document_id'''


def _check(deadline_at):
    if time.monotonic() >= deadline_at:
        raise ChunkRetirementError('singleton_restore_deadline')


def _validate(tenant_id, source_id, principal_id, document_ids, deadline_at):
    if (any(not isinstance(value, str) or not V2_AUTHORITY_RE.fullmatch(value)
            for value in (tenant_id, source_id, principal_id))
            or not isinstance(document_ids, tuple) or not 1 <= len(document_ids) <= MAX_DOCUMENTS
            or len(set(document_ids)) != len(document_ids)
            or any(not isinstance(value, str) or not V2_AUTHORITY_RE.fullmatch(value) for value in document_ids)
            or type(deadline_at) not in (int, float) or not math.isfinite(deadline_at)):
        raise ChunkRetirementError('singleton_restore_scope_invalid')
    _check(deadline_at)


def restore_singleton_metadata(store, archive, *, tenant_id, source_id, principal_id,
                               document_ids, apply=False, reviewed_plan=None, deadline_at=None):
    """Restore exact verified bodies and metadata, dry-run unless explicitly applied.

    No profile is issued here. Preparation releases SQL before archive reads;
    apply rechecks source authority and the exact catalog under existing locks.
    An exception, including a lost COMMIT acknowledgment, returns no counters.
    """
    deadline_at = time.monotonic() + 25 if deadline_at is None else deadline_at
    _validate(tenant_id, source_id, principal_id, document_ids, deadline_at)
    if type(apply) is not bool or not getattr(store, 'verified_singleton_receipts', False):
        raise ChunkRetirementError('singleton_restore_profile_reader_required')
    scope = (tenant_id, source_id)
    params = (*scope, list(document_ids))
    with store.connect() as connection, connection.transaction():
        connection.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
        require_retirement_owner(store, connection, scope, principal_id, deadline_at, lock=False)
        documents = store._execute_bounded(connection, _DOCUMENTS, params, deadline_at).fetchall()
        before = chunk_catalog_snapshot(store, tenant_id=tenant_id, source_ids=(source_id,),
            document_ids=document_ids, connection=connection, deadline_at=deadline_at)
    if len(documents) != len(document_ids) or len(before) != len(document_ids):
        raise ChunkRetirementError('singleton_restore_documents_changed')
    fingerprint = hashlib.sha256(orjson.dumps(list(before.values()), option=orjson.OPT_SORT_KEYS)).hexdigest()
    rows = list(before.values())
    with stage_history_rows(store, archive, tenant_id=tenant_id, rows=rows, deadline_at=deadline_at) as staged:
        plan_documents = []
        for document in documents:
            row = before[(source_id, document['document_id'])]
            chunks = staged.read(row)
            if len(chunks) != 1 or chunks[0]['ordinal'] != 0:
                raise ChunkRetirementError('singleton_restore_metadata_invalid')
            plan_documents.append(dict(document, chunk_id=_opaque('chk', tenant_id, source_id, document['document_id'], '0'), receipt=chunks[0]['receipt'],
                body_bytes=len(chunks[0]['text_redacted'].encode())))
        plan = dict(version=1, tenant_id=tenant_id, source_id=source_id,
                    documents=plan_documents, catalog_sha256=fingerprint)
        if not apply:
            return dict(status='dry_run', plan=plan, restored_documents=0,
                        eligible_documents=len(documents), eligible_utf8_bytes=sum(d['body_bytes'] for d in plan_documents))
        if plan != reviewed_plan:
            raise ChunkRetirementError('singleton_restore_plan_changed')
        with store.connect() as connection, connection.transaction():
            connection.execute("SET LOCAL lock_timeout='250ms'")
            require_retirement_owner(store, connection, scope, principal_id, deadline_at)
            def query(sql, values=()):
                return store._execute_bounded(connection, sql, values, deadline_at)
            if not _try_parent_native_locks(query, scope, documents):
                raise ChunkRetirementError('singleton_restore_lock_busy')
            locked = query(_DOCUMENTS + ' FOR UPDATE NOWAIT', params).fetchall()
            if locked != documents:
                raise ChunkRetirementError('singleton_restore_documents_changed')
            parents = sorted({row['native_parent_id'] or row['native_id'] for row in rows})
            logical_ids = []
            for parent in parents:
                catalog = read_parent_catalog(store, connection, (*scope, parent), deadline_at, lock=True)
                logical_ids.append(catalog['manifest']['logical_document_id'])
            after = chunk_catalog_snapshot(store, tenant_id=tenant_id, source_ids=(source_id,),
                document_ids=document_ids, connection=connection, deadline_at=deadline_at)
            if after != before:
                raise ChunkRetirementError('singleton_restore_catalog_changed')
            restore_outgoing(connection, staged, tenant_id=tenant_id, source_id=source_id,
                native_ids=[document['native_id'] for document in documents], store=store, deadline_at=deadline_at)
            disable_parent_retirement_after_restore(store, connection, tenant_id=tenant_id,
                source_id=source_id, parents=logical_ids, deadline_at=deadline_at)
            _check(deadline_at)
        return dict(status='restored', plan=plan, restored_documents=len(documents),
                    restored_utf8_bytes=sum(d['body_bytes'] for d in plan_documents))


def inspect_singleton_restoration(store, *, principal_id, reviewed_plan, deadline_at=None):
    """Observe an exact original plan without retrying its mutation or upgrading its ACK."""
    deadline_at = time.monotonic() + 5 if deadline_at is None else deadline_at
    tenant_id, source_id = reviewed_plan['tenant_id'], reviewed_plan['source_id']
    documents = reviewed_plan['documents']
    document_ids = tuple(document['document_id'] for document in documents)
    _validate(tenant_id, source_id, principal_id, document_ids, deadline_at)
    with store.connect() as connection, connection.transaction():
        connection.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
        require_retirement_owner(store, connection, (tenant_id, source_id), principal_id, deadline_at, lock=False)
        rows = store._execute_bounded(connection, '''SELECT document.document_id,document.native_id,
            document.revision,document.text_sha256,document.created_at::text AS created_at,
            document.chunk_receipt_profile,chunk.chunk_id,chunk.ordinal,chunk.receipt,chunk.text_sha256 AS chunk_sha256,
            octet_length(chunk.text_redacted) AS body_bytes,
            encode(sha256(convert_to(chunk.text_redacted,'UTF8')),'hex') AS actual_sha256
            FROM canonical_documents document LEFT JOIN canonical_chunks chunk
              USING(tenant_id,source_id,document_id)
            WHERE document.tenant_id=%s AND document.source_id=%s AND document.document_id=ANY(%s)
              AND document.is_current AND document.deleted_at IS NULL AND chunk.deleted_at IS NULL
            ORDER BY document.document_id,chunk.ordinal''', (tenant_id, source_id, list(document_ids)), deadline_at).fetchall()
    expected = {document['document_id']: document for document in documents}
    counts = {document: 0 for document in expected}
    for row in rows:
        counts[row['document_id']] += 1
    restored = sum(counts[row['document_id']] == 1 and row['chunk_receipt_profile'] is None
                   and row['ordinal'] == 0 and all(row[key] == expected[row['document_id']][key]
                     for key in ('native_id','revision','text_sha256','created_at','chunk_id','receipt','body_bytes'))
                   and row['chunk_sha256'] == row['actual_sha256'] == row['text_sha256'] for row in rows)
    return dict(original_commit_outcome='unknown', restored_documents=restored,
                requested_documents=len(documents), mutation_replayed=False)
