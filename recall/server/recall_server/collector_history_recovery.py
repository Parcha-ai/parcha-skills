"""Explicit correction of one sealed, obsolete filesystem cleanup marker.

This is not ordinary ingest and never changes historical deletion flags. The
actual company administrator authorizes and audits a new collector generation.
"""
from __future__ import annotations

import hashlib
import json
import math
import time
import uuid

from contracts.v2 import IDENTITY_RE, validate_contract

from .canonical import _identity_sha256, _opaque, _source_ordinal
from .chunk_retirement import require_retirement_owner
from .control import ControlPlane
from .logical_evidence_projection import _validate_source_body
from .projectors import canonical_json, item_receipt, validate_envelope


class CollectorHistoryRecoveryError(RuntimeError):
    pass


def _refuse(code):
    raise CollectorHistoryRecoveryError('collector_history_' + code)


def _read(plane, connection, tenant_id, principal_id, expected, deadline, *, lock):
    store = plane.store
    source, native = expected['source_id'], expected['native_id']
    scope = (tenant_id, source)
    require_retirement_owner(store, connection, scope, principal_id, deadline, lock=lock)
    # Unlike ordinary exact-owner maintenance, this exceptional correction
    # always requires the explicit existing company-admin predicate.
    ControlPlane._brain_admin(connection, principal_id=principal_id, tenant_id=tenant_id, lock=lock)
    def query(sql, params=()):
        return store._execute_bounded(connection, sql, params, deadline)
    if query('''SELECT 1 FROM forget_tombstones WHERE tenant_id=%s AND source_id=%s
        AND target_identity_sha256=%s''', (*scope, _identity_sha256(*scope, native))).fetchone():
        _refuse('forgotten')
    source_row = query('SELECT owner_principal_id FROM canonical_sources WHERE tenant_id=%s AND source_id=%s', scope).fetchone()
    rows = query('''SELECT event_id,revision,is_tombstone,native_parent_id,kind,
        content_sha256,artifact_id,occurred_at,observed_at,source_ordinal,canonical_redacted
        FROM canonical_events WHERE tenant_id=%s AND source_id=%s AND native_id=%s
        ORDER BY revision DESC LIMIT 3''', (*scope, native)).fetchall()
    if [(r['event_id'], r['revision'], r['is_tombstone']) for r in rows] != [
        (expected['marker_event_id'], 2, True), (expected['original_event_id'], 1, False)
    ]:
        _refuse('history_changed')
    marker, original = rows
    parents = sorted({native} | {r['native_parent_id'] for r in rows if r['native_parent_id']})
    if lock:
        for identity in parents:
            if not query('SELECT pg_try_advisory_xact_lock(hashtextextended(%s,0)) AS locked',
                         (f'v2\x1f{tenant_id}\x1f{source}\x1f{identity}',)).fetchone()['locked']:
                _refuse('native_busy')
        locked = query('''SELECT event_id,revision,is_tombstone,native_parent_id,kind,
            content_sha256,artifact_id,occurred_at,observed_at,source_ordinal,canonical_redacted FROM canonical_events
            WHERE tenant_id=%s AND source_id=%s AND native_id=%s
            ORDER BY revision DESC LIMIT 3 FOR UPDATE NOWAIT''', (*scope, native)).fetchall()
        if locked != rows:
            _refuse('history_changed')
    hashes = [_identity_sha256(*scope, identity) for identity in parents]
    if query('''SELECT 1 FROM forget_tombstones WHERE tenant_id=%s AND source_id=%s
        AND target_identity_sha256=ANY(%s) LIMIT 1''', (*scope, hashes)).fetchone():
        _refuse('forgotten')
    if query('''SELECT 1 FROM canonical_events WHERE tenant_id=%s AND source_id=%s
        AND native_id=ANY(%s) AND native_id<>%s AND is_tombstone LIMIT 1''', (*scope, parents, native)).fetchone():
        _refuse('parent_deleted')
    payload = marker['canonical_redacted']
    marker_content = payload.get('content', {})
    deletion_id = marker_content.get('deletion_id')
    if (marker['kind'] != 'tombstone'
            or payload.get('principal_id') != source_row['owner_principal_id']
            or payload.get('provenance', {}).get('connector_id') != 'claude.jsonl'
            or payload.get('content', {}).get('target_native_id') != native
            or not isinstance(deletion_id, str) or not deletion_id
            or hashlib.sha256(canonical_json(marker_content)).hexdigest() != marker['content_sha256']
            or _opaque('evt', tenant_id, source, native, marker['content_sha256']) != expected['marker_event_id']):
        _refuse('marker_refused')
    documents = query('''SELECT document_id,revision,is_current,deleted_at,text_sha256,content_sha256
        FROM canonical_documents WHERE tenant_id=%s AND source_id=%s AND native_id=%s
        ORDER BY revision LIMIT 2''' + (' FOR UPDATE NOWAIT' if lock else ''), (*scope, native)).fetchall()
    if (len(documents) != 1 or documents[0]['document_id'] != expected['document_id']
            or documents[0]['revision'] != 1 or documents[0]['is_current']
            or documents[0]['deleted_at'] is None):
        _refuse('document_changed')
    chunks = query('''SELECT ordinal,receipt,text_sha256,deleted_at,
        octet_length(text_redacted) AS text_bytes,
        CASE WHEN octet_length(text_redacted)<=65536 THEN text_redacted END AS text_redacted
        FROM canonical_chunks WHERE tenant_id=%s AND source_id=%s AND document_id=%s
        ORDER BY ordinal LIMIT 2''' + (' FOR UPDATE NOWAIT' if lock else ''),
        (*scope, expected['document_id'])).fetchall()
    if (len(chunks) != 1 or chunks[0]['ordinal'] != 0 or chunks[0]['deleted_at'] is None
            or chunks[0]['text_bytes'] != expected['text_bytes']
            or chunks[0]['text_sha256'] != expected['text_sha256']
            or documents[0]['text_sha256'] != expected['text_sha256']
            or chunks[0]['receipt'] != item_receipt(source, native, 1, 0)):
        _refuse('chunk_changed')
    chunk = chunks[0]
    _validate_source_body(dict(event_text=chunk['text_redacted'], document_text_sha256=expected['text_sha256'],
        source_chunks=[dict(ordinal=0, size_bytes=expected['text_bytes'], text_sha256=expected['text_sha256'])],
        chunk_receipts=[chunk['receipt']], chunk_count=1))
    content = json.loads(chunk['text_redacted'])
    if (not isinstance(content, dict) or canonical_json(content).decode() != chunk['text_redacted']
            or original['content_sha256'] != expected['text_sha256']
            or documents[0]['content_sha256'] != expected['text_sha256']):
        _refuse('content_changed')
    metadata = original['canonical_redacted']
    if (original['kind'] != 'transcript_record'
            or metadata.get('principal_id') != source_row['owner_principal_id']
            or metadata.get('provenance', {}).get('connector_id') != 'claude.jsonl'):
        _refuse('lineage_changed')
    artifact = query('''SELECT tenant_id,source_id,artifact_id,storage_backend,object_key,
        content_sha256,size_bytes,media_type,encryption,version_id,created_at,state
        FROM raw_artifacts WHERE tenant_id=%s AND source_id=%s AND artifact_id=%s''',
        (*scope, original['artifact_id'])).fetchone()
    if artifact is None or artifact.pop('state') != 'live':
        _refuse('raw_unavailable')
    artifact['created_at'] = artifact['created_at'].isoformat()
    artifact.update(contract='recall.artifact-ref.v1', schema_version=1)
    validate_contract(artifact, expected='recall.artifact-ref.v1')
    return dict(original=original, marker=marker, document=documents[0], chunk=chunk,
                artifact=artifact, content=content)


def recover_collector_history(plane, *, tenant_id, principal_id, expected, operation_id,
                              deadline_at=None):
    """Recover one exactly reviewed record; exceptions never acknowledge a write."""
    fields = {'source_id','native_id','original_event_id','marker_event_id','document_id','text_sha256','text_bytes'}
    if (not isinstance(expected, dict) or set(expected) != fields
            or any(not isinstance(v, str) or not IDENTITY_RE.fullmatch(v)
                   for v in (tenant_id, principal_id, *(expected[k] for k in fields - {'text_bytes','text_sha256'})))
            or type(expected['text_bytes']) is not int or not 0 < expected['text_bytes'] <= 65536
            or not isinstance(expected['text_sha256'], str) or len(expected['text_sha256']) != 64):
        _refuse('scope_invalid')
    operation_id = str(uuid.UUID(operation_id))
    deadline = time.monotonic() + 25 if deadline_at is None else deadline_at
    if type(deadline) not in (int, float) or not math.isfinite(deadline) or not time.monotonic() < deadline <= time.monotonic() + 25:
        _refuse('deadline')
    with plane.store.connect() as conn, conn.transaction():
        conn.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
        before = _read(plane, conn, tenant_id, principal_id, expected, deadline, lock=False)
    # Reuse the source's original immutable raw lineage. Generation is canonical
    # metadata, as in collector restoration; no new upload or body claim is made.
    plane.archive.verify_raw_bounded(before['artifact'], deadline_at=deadline)
    original = before['original']
    content = dict(before['content'])
    generation = content.get('_recall_collector_generation', 0)
    if type(generation) is not int or generation < 0:
        _refuse('generation_invalid')
    content['_recall_collector_generation'] = generation + 1
    metadata = original['canonical_redacted']
    event = dict(schema_version=1, source_id=expected['source_id'], native_id=expected['native_id'],
        kind=original['kind'], occurred_at=original['occurred_at'].isoformat(),
        observed_at=original['observed_at'].isoformat(), principal_id=metadata['principal_id'],
        visibility=metadata['visibility'], content_type='application/json', content=content,
        provenance={**metadata['provenance'], 'artifact_ref': before['artifact']},
        content_sha256=hashlib.sha256(canonical_json(content)).hexdigest())
    if original['native_parent_id'] is not None:
        event['native_parent_id'] = original['native_parent_id']
    validate_envelope(event)
    if _source_ordinal(event) != original['source_ordinal']:
        _refuse('source_order_changed')
    target_sha256 = hashlib.sha256(canonical_json(dict(tenant_id=tenant_id, expected=expected))).hexdigest()
    with plane.store.connect() as conn, conn.transaction():
        conn.execute("SET LOCAL lock_timeout='250ms'")
        after = _read(plane, conn, tenant_id, principal_id, expected, deadline, lock=True)
        if after != before:
            _refuse('snapshot_changed')
        result = plane._write_authorized_document(conn, tenant_id=tenant_id, connector_id='claude.jsonl',
            artifact=before['artifact'], event=event, text_redacted=canonical_json(content).decode())
        if result['revision'] != 3 or result['inserted'] != 1:
            _refuse('revision_changed')
        conn.execute('''INSERT INTO control_audit_events(id,principal_id,operation,status,target_sha256)
            VALUES(%s,%s,'collector.history.recover','success',%s)''', (operation_id, principal_id, target_sha256))
        if time.monotonic() >= deadline:
            _refuse('deadline')
    return dict(result, operation_id=operation_id, target_sha256=target_sha256)


def inspect_collector_history_recovery(store, *, tenant_id, principal_id, expected,
                                       operation_id, deadline_at=None):
    """Observe the original atomic audit without replaying or upgrading its ACK.

    An absent audit is not permission to retry: later edits or forgets can change
    current state. The original caller must retain its unknown outcome.
    """
    operation_id = str(uuid.UUID(operation_id))
    deadline = time.monotonic() + 5 if deadline_at is None else deadline_at
    target = hashlib.sha256(canonical_json(dict(tenant_id=tenant_id, expected=expected))).hexdigest()
    with store.connect() as conn, conn.transaction():
        conn.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
        require_retirement_owner(store, conn, (tenant_id, expected['source_id']), principal_id, deadline, lock=False)
        ControlPlane._brain_admin(conn, principal_id=principal_id, tenant_id=tenant_id)
        row = store._execute_bounded(conn, '''SELECT principal_id,operation,status,target_sha256
            FROM control_audit_events WHERE id=%s''', (operation_id,), deadline).fetchone()
        matched = row == dict(principal_id=principal_id, operation='collector.history.recover',
                              status='success', target_sha256=target)
        if row is not None and not matched:
            _refuse('audit_conflict')
    return dict(status='recorded_applied' if matched else 'not_recorded',
                original_commit_outcome='unknown', mutation_replayed=False,
                operation_id=operation_id, target_sha256=target)
