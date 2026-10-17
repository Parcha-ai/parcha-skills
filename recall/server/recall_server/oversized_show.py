"""Paginate the verified source behind an oversized canonical receipt."""
from __future__ import annotations

import hashlib
import json
import re
import time

from .canonical import _identity_sha256
from .chunk_bodies import _VerifiedArchive
from .db import SearchDeadlineExceeded
from .logical_evidence_projection import restore_oversized_record, OVERSIZED_MEDIA_TYPE
from .parent_membership_enrollment import _authorized
from .projectors import item_receipt


def _require(ok):
    if not ok:
        raise ValueError('oversized source unavailable for current receipt')


def _snapshot(reader, target, deadline):
    store = reader.store
    def query(c, sql, args):
        return store._execute_bounded(c, sql, args, deadline)
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise SearchDeadlineExceeded()
    with store.connect(timeout=remaining) as c:
        c.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
        anchor = reader._receipt_event(c, target, deadline_at=deadline)
        _require(anchor is not None)
        scope = dict(tenant_id=reader.tenant_id, principal_id=reader.principal_id,
                     source_id=anchor['source_id'])
        _require(_authorized(store, c, scope, deadline))
        row = query(c, '''SELECT d.document_id,d.text_sha256 AS projection_text_sha256,
                   e.tenant_id,e.source_id,e.native_id,e.native_parent_id,e.event_id,e.revision,
                   e.kind,e.occurred_at,e.observed_at,e.content_sha256 AS projection_content_sha256,
                   e.canonical_redacted->'content' AS oversized_content,
                   a.artifact_id AS raw_artifact_id,a.storage_backend AS raw_storage_backend,
                   a.object_key AS raw_object_key,a.content_sha256 AS raw_content_sha256,
                   a.size_bytes AS raw_size_bytes,a.media_type AS raw_media_type,
                   a.encryption AS raw_encryption,a.version_id AS raw_version_id,
                   a.created_at AS raw_created_at
              FROM canonical_documents d
              JOIN canonical_events e USING(tenant_id,source_id,event_id)
              JOIN raw_artifacts a ON a.tenant_id=e.tenant_id AND a.source_id=e.source_id
                   AND a.artifact_id=e.artifact_id
              JOIN canonical_ingest_jobs j ON j.tenant_id=e.tenant_id AND j.source_id=e.source_id
                   AND j.job_id=e.job_id
             WHERE d.tenant_id=%s AND d.source_id=%s AND d.document_id=%s
               AND d.is_current AND d.deleted_at IS NULL AND NOT e.is_tombstone
               AND a.state='live' AND j.status='committed'
               AND NOT EXISTS (SELECT 1 FROM canonical_chunks removed
                   WHERE removed.tenant_id=d.tenant_id AND removed.source_id=d.source_id
                     AND removed.document_id=d.document_id AND removed.deleted_at IS NOT NULL)''',
            (reader.tenant_id, anchor['source_id'], anchor['document_id'])).fetchone()
        _require(row is not None and row['raw_media_type'] == OVERSIZED_MEDIA_TYPE)
        row = dict(row)
        identities = [_identity_sha256(reader.tenant_id, row['source_id'], row['native_id'])]
        if row['native_parent_id']:
            identities.append(_identity_sha256(reader.tenant_id, row['source_id'], row['native_parent_id']))
        forgotten = query(c, '''SELECT EXISTS(SELECT 1 FROM forget_tombstones
            WHERE tenant_id=%s AND source_id=%s AND target_identity_sha256=ANY(%s)) AS present''',
            (reader.tenant_id, row['source_id'], identities)).fetchone()['present']
        _require(not forgotten)
        row['receipt'] = item_receipt(row['source_id'], row['native_id'], row['revision'], anchor['anchor_ordinal'])
        return row


def show_oversized(reader, target, *, cursor=None, page_bytes=None, deadline_at=None):
    from .mcp import MAX_MCP_RESPONSE_BYTES, SEARCH_RESULT_BUDGET_BYTES, _encoded_result_size
    from .canonical_retrieval import _timestamp

    page_bytes = 32_768 if page_bytes is None else page_bytes
    _require(type(page_bytes) is int and 0 < page_bytes <= MAX_MCP_RESPONSE_BYTES)
    page_bytes = min(page_bytes, SEARCH_RESULT_BUDGET_BYTES)
    _require(cursor is None or (isinstance(cursor, str) and
             re.fullmatch(r'[0-9a-f]{64}:[0-9]+', cursor) is not None))
    _require(reader.archive is not None)
    deadline = deadline_at if deadline_at is not None else time.monotonic() + reader.store.search_deadline_ms / 1000
    before = _snapshot(reader, target, deadline)
    pin = hashlib.sha256(json.dumps([reader.tenant_id, reader.principal_id, before],
        default=str, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    start = 0
    if cursor is not None:
        expected, position = cursor.split(':')
        _require(expected == pin)
        start = int(position)
    # Use the same restoration contract as logical source publication. This is
    # deliberately not canonical chunk hydration: chunks commit the envelope.
    text = restore_oversized_record(_VerifiedArchive(reader.archive, deadline), before, '')
    _require(0 <= start < len(text))
    source_bytes = before['oversized_content']['full_size_bytes']
    result = dict(target=target, scope='full_source_record',
        event={k: (_timestamp(before[k]) if k in ('occurred_at', 'observed_at') else before[k])
               for k in ('source_id', 'native_id', 'revision', 'kind', 'occurred_at', 'observed_at')},
        projection_text_sha256=before['projection_text_sha256'],
        source_text_sha256=before['oversized_content']['full_content_sha256'],
        source_bytes=source_bytes, selection_sha256=pin,
        chunks=[], opened_receipts=[before['receipt']], next_cursor=None, complete=False)
    byte_start = len(text[:start].encode())
    def render(end):
        result['chunks'] = [dict(receipt=before['receipt'], text=text[start:end],
            content_start=start, content_end=end, content_length=len(text),
            byte_start=byte_start, byte_end=byte_start+len(text[start:end].encode()),
            byte_length=source_bytes, content_complete=start == 0 and end == len(text))]
        result['complete'] = end == len(text)
        result['next_cursor'] = None if result['complete'] else f'{pin}:{end}'
    low, high = start, min(len(text), start + page_bytes)
    while low < high:
        end = (low + high + 1) // 2
        render(end)
        if _encoded_result_size(result) <= page_bytes:
            low = end
        else:
            high = end - 1
    render(low)
    _require(low > start and _encoded_result_size(result) <= page_bytes)
    _require(_snapshot(reader, target, deadline) == before)
    if time.monotonic() >= deadline:
        raise SearchDeadlineExceeded()
    return result
