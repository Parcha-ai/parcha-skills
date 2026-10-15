"""Attempt-local contiguous retirement discovery; full completion stays separate.

The selected archive reader and full parent verifier remain unchanged.
Discovery metadata never authorizes a clear.
"""
from __future__ import annotations

import hashlib
from io import BytesIO
import time

from .canonical import _identity_sha256
from .canonical_text import MAX_CANONICAL_TEXT_BYTES
from .chunk_bodies import (
    MAX_DOCUMENTS, MAX_RESULT_BYTES, _EXCLUDED_TYPES, _VerifiedArchive, _check_deadline,
    chunk_catalog_snapshot, read_archived_chunks,
)
from .evidence_projection import CanonicalEvidenceProjector
from .logical_evidence import LogicalEvidenceProjectionStore, MAX_PART_BYTES
from .parent_chunk_proof import manifest_identity, parent_retirement_plan, read_parent_catalog
from .passage_projection import decode_logical_record


_WITNESSES = """WITH current_documents AS MATERIALIZED (
    SELECT tenant_id,source_id,document_id,native_id,revision,text_sha256,
           body_record_ordinal,body_record_count,event_id
    FROM canonical_documents
    WHERE tenant_id=%s AND source_id=%s AND native_id=ANY(%s)
      AND is_current AND deleted_at IS NULL /*document_lock*/
    ) SELECT document.document_id,document.native_id,document.revision,
    document.text_sha256,document.body_record_ordinal,document.body_record_count,
    event.event_id,event.native_parent_id,event.kind,event.is_tombstone,
    artifact.media_type AS raw_media_type,
    ARRAY[event.canonical_redacted->>'type',event.canonical_redacted #>> '{content,type}',
          event.canonical_redacted #>> '{content,message,type}',event.canonical_redacted #>> '{content,payload,type}',
          event.canonical_redacted #>> '{message,type}',event.canonical_redacted #>> '{payload,type}'] AS structural_types
    FROM current_documents document
    JOIN LATERAL (SELECT event_id,native_parent_id,kind,is_tombstone,artifact_id,canonical_redacted
        FROM canonical_events WHERE tenant_id=document.tenant_id AND source_id=document.source_id
          AND event_id=document.event_id LIMIT 1 /*event_lock*/) event ON true
    JOIN LATERAL (SELECT media_type FROM raw_artifacts
        WHERE tenant_id=document.tenant_id AND source_id=document.source_id
          AND artifact_id=event.artifact_id LIMIT 1 /*artifact_lock*/) artifact ON true
    ORDER BY document.document_id"""


def _fail(code):
    from .chunk_retirement import ChunkRetirementError
    raise ChunkRetirementError('parent_retirement_window_' + code)


def _witnesses(store, connection, scope, natives, deadline, *, lock=False):
    query = lambda sql, values: store._execute_bounded(connection, sql, values, deadline)
    identities = sorted(set(natives) | {scope[2]})
    hashes = [_identity_sha256(*scope[:2], native) for native in identities]
    if query('''SELECT 1 FROM forget_tombstones WHERE tenant_id=%s AND source_id=%s
        AND target_identity_sha256=ANY(%s) LIMIT 1''', (*scope[:2], hashes)).fetchone():
        _fail('forgotten')
    if query('''SELECT 1 FROM canonical_events WHERE tenant_id=%s AND source_id=%s
        AND native_id=%s AND is_tombstone LIMIT 1''', scope).fetchone():
        _fail('parent_deleted')
    sql = _WITNESSES
    if lock:
        sql = sql.replace('/*document_lock*/','FOR UPDATE NOWAIT').replace(
            '/*event_lock*/','FOR SHARE NOWAIT').replace('/*artifact_lock*/','FOR SHARE NOWAIT')
    rows = query(sql, (*scope[:2], list(natives))).fetchall()
    result = dict.fromkeys(natives)
    for row in rows:
        native = row['native_id']
        if native not in result or result[native] is not None:
            _fail('current_identity')
        result[native] = row
    return result


def _reason(event, row, scope):
    if row is None:
        return 'not_current'
    if row['is_tombstone'] or (row['native_parent_id'] or row['native_id']) != scope[2]:
        _fail('current_identity')
    if row['raw_media_type'] == 'application/vnd.recall.oversized-record+gzip':
        return 'oversized'
    if _EXCLUDED_TYPES.intersection(row['structural_types']):
        return 'structural'
    if row['body_record_ordinal'] is None and row['body_record_count'] is None:
        return 'unlocated'
    if (type(row['body_record_ordinal']) is not int or type(row['body_record_count']) is not int
            or row['body_record_ordinal'] != event['first'] or row['body_record_count'] != event['count']
            or row['kind'] != event['kind']):
        _fail('locator')
    return 'event_body_budget' if event['body_bytes'] > MAX_CANONICAL_TEXT_BYTES else None


def _discover(archive, scope, manifest, parts, start, limits, deadline):
    if (not parts or len(parts) != manifest['part_count'] or len(parts) > limits.max_parts
            or type(manifest['record_count']) is not int or not 1 <= manifest['record_count'] <= limits.max_records):
        _fail('catalog')
    ordinal = 0
    for index, part in enumerate(parts):
        if (part['part_ordinal'] != index or part['first_record_ordinal'] != ordinal
                or type(part['last_record_ordinal']) is not int or part['last_record_ordinal'] < ordinal
                or type(part['size_bytes']) is not int or not 0 < part['size_bytes'] <= MAX_PART_BYTES
                or part['tenant_id'] != scope[0] or part['source_id'] != scope[1]
                or part['logical_document_id'] != manifest['logical_document_id'] or part['revision'] != manifest['revision']):
            _fail('catalog')
        ordinal = part['last_record_ordinal'] + 1
    if ordinal != manifest['record_count'] or not 0 <= start <= ordinal:
        _fail('catalog')
    projection = LogicalEvidenceProjectionStore(_VerifiedArchive(archive, deadline))
    events, seen, active = [], set(), None
    consumed = gets = 0
    for part in parts:
        if part['last_record_ordinal'] < start:
            continue
        # A selected reader may reread every discovery part. Reserve both reads
        # before I/O, even if later metadata will exclude some events.
        if 2 * (consumed + part['size_bytes']) > limits.max_archive_bytes:
            if events:
                active = None  # Its first ordinal is not covered by this window.
                break
            _fail('archive_budget')
        payload = projection.read_part(CanonicalEvidenceProjector._reference(part),
            tenant_id=scope[0], source_id=scope[1])
        consumed += len(payload); gets += 1
        if not payload.endswith(b'\n'):
            _fail('part')
        ordinal, receipts = part['first_record_ordinal'], 0
        full = False
        for line in BytesIO(payload):
            _check_deadline(deadline)
            record = decode_logical_record(line, source_id=scope[1])
            if record.ordinal != ordinal:
                _fail('part')
            ordinal += 1; receipts += len(record.receipts)
            if record.ordinal < start or full:
                continue
            if active is None:
                if record.segment_ordinal != 0 or not record.receipts or record.event_native_id in seen:
                    _fail('event_boundary')
                active = dict(native_id=record.event_native_id,first=record.ordinal,count=record.segment_count,
                    kind=record.event_kind,body_bytes=0)
                segment = 0
                shape = record.occurred_at, record.roles, record.actor_links
            if (record.event_native_id != active['native_id'] or record.event_kind != active['kind']
                    or record.segment_count != active['count'] or record.segment_ordinal != segment
                    or (record.occurred_at,record.roles,record.actor_links) != shape
                    or segment > 0 and record.receipts):
                _fail('event_boundary')
            active['body_bytes'] += len(record.text.encode())
            segment += 1
            if segment == active['count']:
                seen.add(active['native_id']); events.append(active); active = None
                full = len(events) == min(MAX_DOCUMENTS, limits.batch_documents)
        if ordinal - 1 != part['last_record_ordinal'] or receipts != part['receipt_count']:
            _fail('part')
        del payload, line, record
        if full:
            break
    if active is not None:
        _fail('event_boundary')
    return events, dict(discovery_archive_bytes=consumed,discovery_archive_gets=gets)


def prepare_window(store, archive, *, scope, principal_id, limits, deadline_at, remaining_bytes=None, required_scope_epoch=None):
    from .chunk_retirement import ChunkRetirementError, _parent_progress, require_retirement_owner
    remaining_bytes = limits.max_clear_bytes if remaining_bytes is None else remaining_bytes
    if type(remaining_bytes) is not int or not 0 <= remaining_bytes <= limits.max_clear_bytes:
        _fail('clear_budget')
    with store.connect() as c, c.transaction():
        c.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
        require_retirement_owner(store,c,scope[:2],principal_id,deadline_at,lock=False)
        _witnesses(store,c,scope,(),deadline_at)
        catalog = read_parent_catalog(store,c,scope,deadline_at)
        progress = _parent_progress(store,c,scope,deadline_at)
        if (not progress or not progress['enabled'] or required_scope_epoch is not None
                and progress['scope_epoch'] != required_scope_epoch):
            raise ChunkRetirementError('parent_retirement_disabled')
        if catalog['queue'] is not None:
            _fail('pending')
        manifest = catalog['manifest']
        parts = store._execute_bounded(c,'''SELECT * FROM canonical_evidence_document_parts
            WHERE tenant_id=%s AND source_id=%s AND logical_document_id=%s AND revision=%s
            ORDER BY part_ordinal LIMIT %s''',(*scope[:2],manifest['logical_document_id'],manifest['revision'],limits.max_parts+1),deadline_at).fetchall()
    identity = manifest_identity(manifest)
    cursor = progress['last_record_ordinal'] if progress['manifest_artifact_id'] == identity['manifest_artifact_id'] else -1
    events, cost = _discover(archive,scope,manifest,parts,cursor+1,limits,deadline_at)
    plan = parent_retirement_plan(scope,identity)
    if not events:
        return dict(handoff_full_parent=True,plan=plan,cost=cost,scope_epoch=progress['scope_epoch'])
    natives = tuple(event['native_id'] for event in events)
    with store.connect() as c, c.transaction():
        c.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
        require_retirement_owner(store,c,scope[:2],principal_id,deadline_at,lock=False)
        witnesses = _witnesses(store,c,scope,natives,deadline_at)
        reasons = {event['native_id']:_reason(event,witnesses[event['native_id']],scope) for event in events}
        ids = tuple(witnesses[native]['document_id'] for native in natives if reasons[native] is None)
        # Count bounded physical metadata before constructing chunk JSON. This
        # keeps a large later document from blocking a valid earlier prefix.
        stats = store._execute_bounded(c,'''SELECT requested.document_id,
            count(chunk.ordinal) AS chunks,coalesce(sum(chunk.bytes),0)::bigint AS bytes
            FROM unnest(%s::text[]) requested(document_id)
            LEFT JOIN LATERAL (SELECT ordinal,octet_length(text_redacted) AS bytes
                FROM canonical_chunks WHERE tenant_id=%s AND source_id=%s
                  AND document_id=requested.document_id AND deleted_at IS NULL
                ORDER BY ordinal LIMIT %s) chunk ON true
            GROUP BY requested.document_id''', (list(ids),*scope[:2],limits.batch_chunks+1),deadline_at).fetchall() if ids else []
        stats = {row['document_id']:row for row in stats}
        chunk_count = hash_bytes = result_bytes = 0
        prefix = []
        clear_budget_limited = False
        for event in events:
            native = event['native_id']
            if reasons[native] is None:
                count = stats[witnesses[native]['document_id']]
                # An admitted singleton has no physical row, but still one
                # receipt. The unchanged reader proves that admission below.
                next_chunks = chunk_count + max(1,count['chunks'])
                next_hash = hash_bytes + count['bytes']
                next_result = result_bytes + event['body_bytes']
                if (next_chunks > limits.batch_chunks or next_hash > min(limits.hash_bytes,remaining_bytes)
                        or next_result > MAX_RESULT_BYTES):
                    clear_budget_limited = (next_hash > remaining_bytes and count['bytes'] <= limits.hash_bytes
                        and max(1,count['chunks']) <= limits.batch_chunks and event['body_bytes'] <= MAX_RESULT_BYTES)
                    if not prefix:
                        _fail('clear_budget_hold' if clear_budget_limited else 'individual_budget_hold')
                    break
                chunk_count,hash_bytes,result_bytes = next_chunks,next_hash,next_result
            prefix.append(event)
        events = prefix
        natives = tuple(event['native_id'] for event in events)
        witnesses = {native:witnesses[native] for native in natives}
        reasons = {native:reasons[native] for native in natives}
        ids = tuple(witnesses[native]['document_id'] for native in natives if reasons[native] is None)
        selected = chunk_catalog_snapshot(store,tenant_id=scope[0],source_ids=(scope[1],),
            document_ids=ids,deadline_at=deadline_at,connection=c,located_only=True) if ids else {}
        physical = store._execute_bounded(c,'''SELECT document_id,ordinal,receipt,text_sha256,
            octet_length(text_redacted) AS pg_bytes FROM canonical_chunks
            WHERE tenant_id=%s AND source_id=%s AND document_id=ANY(%s)
              AND deleted_at IS NULL ORDER BY document_id,ordinal''',(*scope[:2],list(ids)),deadline_at).fetchall() if ids else []
    if set(selected) != {(scope[1],document) for document in ids}:
        _fail('selected_missing')
    rows, selected_parts = [], {}
    for row in selected.values():
        witness = witnesses[row['native_id']]
        if ((row['native_parent_id'] or row['native_id']) != scope[2] or manifest_identity(row['manifest']) != identity
                or any(row[key] != witness[key] for key in ('document_id','revision','text_sha256','body_record_ordinal','body_record_count'))):
            _fail('selected_changed')
        first,count = row['body_record_ordinal'],row['body_record_count']
        for part in row['parts']:
            if first <= part['last_record_ordinal'] and first+count > part['first_record_ordinal']:
                selected_parts[part['artifact_id']] = part
        lengths = {(r['ordinal'],r['receipt'],r['text_sha256']):r['pg_bytes'] for r in physical if r['document_id']==row['document_id']}
        chunks = [dict(ch,pg_bytes=lengths.get((ch['ordinal'],ch['receipt'],ch['text_sha256']),0)) for ch in row['chunks']]
        rows.append(dict(row,chunks=chunks,pg_body_bytes=sum(ch['pg_bytes'] for ch in chunks)))
    proof_bytes = sum(part['size_bytes'] for part in selected_parts.values())
    if cost['discovery_archive_bytes'] + proof_bytes > limits.max_archive_bytes:
        _fail('archive_budget')
    if (sum(len(row['chunks']) for row in rows) > limits.batch_chunks
            or sum(row['pg_body_bytes'] for row in rows) > min(limits.hash_bytes,remaining_bytes)):
        _fail('hash_budget')
    archived = read_archived_chunks(store,archive,tenant_id=scope[0],source_ids=(scope[1],),
        document_ids=ids,located_only=True,deadline_at=deadline_at) if ids else {}
    if set(archived) != set(selected):
        _fail('selected_missing')
    for row in rows:
        actual = archived[(scope[1],row['document_id'])]
        if len(actual)!=len(row['chunks']) or any(a['ordinal']!=b['ordinal'] or a['receipt']!=b['receipt']
                or hashlib.sha256(a['text_redacted'].encode()).hexdigest()!=b['text_sha256']
                for a,b in zip(actual,row['chunks'])):
            _fail('selected_changed')
    if ids and chunk_catalog_snapshot(store,tenant_id=scope[0],source_ids=(scope[1],),
            document_ids=ids,deadline_at=deadline_at,located_only=True)!=selected:
        _fail('selected_changed')
    coverage = dict(start_cursor=progress['last_record_ordinal'],previous_manifest=progress['manifest_artifact_id'],
        events=events,witnesses=witnesses,reasons=reasons,covered_through=events[-1]['first']+events[-1]['count']-1)
    return dict(handoff_full_parent=False,plan=plan,proof=dict(manifest=identity,scope_epoch=progress['scope_epoch']),
        rows=rows,coverage=coverage,clear_budget_limited=clear_budget_limited,cost=dict(cost,proof_archive_bytes=proof_bytes,proof_archive_gets=len(selected_parts)))


def coverage_natives(coverage, rows, limits):
    events = coverage['events']
    natives = [event['native_id'] for event in events]
    if (not 1 <= len(events) <= min(MAX_DOCUMENTS,limits.batch_documents) or len(set(natives))!=len(natives)
            or set(coverage['witnesses'])!=set(natives) or set(coverage['reasons'])!=set(natives)
            or {row['native_id'] for row in rows}!={n for n in natives if coverage['reasons'][n] is None}):
        _fail('coverage')
    return [dict(native_id=native) for native in natives]


def recheck_coverage(store, connection, scope, coverage, progress, manifest, deadline):
    if (coverage['start_cursor']!=progress['last_record_ordinal']
            or coverage['previous_manifest']!=progress['manifest_artifact_id']):
        _fail('cursor')
    cursor = progress['last_record_ordinal'] if progress['manifest_artifact_id']==manifest['manifest_artifact_id'] else -1
    for event in coverage['events']:
        if (type(event['first']) is not int or type(event['count']) is not int
                or event['first']!=cursor+1 or event['count']<1):
            _fail('coverage')
        cursor += event['count']
    if type(coverage['covered_through']) is not int or cursor!=coverage['covered_through'] or cursor>=manifest['record_count']:
        _fail('coverage')
    current = _witnesses(store,connection,scope,tuple(coverage['witnesses']),deadline,lock=True)
    if current!=coverage['witnesses'] or any(_reason(event,current[event['native_id']],scope)!=coverage['reasons'][event['native_id']]
            for event in coverage['events']):
        _fail('witness_changed')


def retire_window(store, archive, *, scope, principal_id, limits, deadline_at, reviewed_plan,
                  metadata_mode='keep', remaining_bytes=None, should_stop=None, required_scope_epoch=None):
    from .chunk_retirement import _retire_parent_batch, _require_singleton_archive_runtime, retire_parent_chunks
    if metadata_mode not in ('keep','singleton'):
        _fail('metadata_mode')
    if metadata_mode == 'singleton':
        _require_singleton_archive_runtime(store)
    if remaining_bytes == 0:
        return dict(status='stopped',complete=False)
    if should_stop is not None and should_stop():
        return dict(status='stopped',complete=False)
    window = prepare_window(store,archive,scope=scope,principal_id=principal_id,limits=limits,
        deadline_at=deadline_at,remaining_bytes=remaining_bytes,required_scope_epoch=required_scope_epoch)
    if window['plan']!=reviewed_plan:
        _fail('plan_changed')
    if window['handoff_full_parent']:
        from dataclasses import replace
        # The caller supplies remaining attempt limits, never a fresh budget.
        completion_limits = limits if remaining_bytes is None else replace(limits,max_clear_bytes=remaining_bytes)
        result = retire_parent_chunks(store,archive,tenant_id=scope[0],source_id=scope[1],native_parent_id=scope[2],
            limits=completion_limits,deadline_at=deadline_at,apply=True,reviewed_plan=reviewed_plan,
            owner_principal_id=principal_id,metadata_mode=metadata_mode,should_stop=should_stop,
            required_scope_epoch=window['scope_epoch'])
        return dict(result,completion_proof='original_full_parent')
    if should_stop is not None and should_stop():
        return dict(status='stopped',complete=False,**window['cost'])
    result = _retire_parent_batch(store,proof=window['proof'],rows=window['rows'],scope=scope,limits=limits,
        deadline_at=min(deadline_at,time.monotonic()+5),complete=False,
        remaining_bytes=limits.max_clear_bytes if remaining_bytes is None else remaining_bytes,
        owner_principal_id=principal_id,coverage=window['coverage'],metadata_mode=metadata_mode)
    return dict(result,status='partial',complete=False,batches=1,clear_budget_limited=window['clear_budget_limited'],**window['cost'])


def retire_parent_windows(store, archive, *, tenant_id, source_id, native_parent_id,
                          apply, reviewed_plan, limits, deadline_at, required_scope_epoch,
                          owner_principal_id, should_stop=None, metadata_mode='keep'):
    """Use one claimed attempt budget; only the original EOF proof completes it."""
    from dataclasses import replace
    import psycopg
    from .chunk_retirement import ChunkRetirementError, _parent_scope, _parent_deadline
    from .db import SearchDeadlineExceeded
    from .retirement_runner import RunArchive
    if not apply or type(required_scope_epoch) is not int or required_scope_epoch < 1:
        _fail('claim')
    scope = _parent_scope(tenant_id,source_id,native_parent_id)
    deadline_at = _parent_deadline(deadline_at)
    if reviewed_plan['manifest']['record_count'] <= min(MAX_DOCUMENTS,limits.batch_documents):
        # Small catalogs avoid discovery/selected-reader rereads. The original
        # owner still validates the exact plan, current manifest and authority.
        from .chunk_retirement import retire_parent_chunks
        return retire_parent_chunks(store,archive,tenant_id=tenant_id,source_id=source_id,
            native_parent_id=native_parent_id,apply=apply,reviewed_plan=reviewed_plan,
            limits=limits,deadline_at=deadline_at,required_scope_epoch=required_scope_epoch,
            owner_principal_id=owner_principal_id,should_stop=should_stop,metadata_mode=metadata_mode)
    meter = RunArchive(archive,max_bytes=limits.max_archive_bytes)
    totals = dict(retired_metadata_chunks=0,cleared_documents=0,cleared_chunks=0,
        cleared_utf8_bytes=0,hashed_utf8_bytes=0,hash_ms=0.0,sql_ms=0.0,batches=0)
    result = dict(status='partial',complete=False)
    try:
        with store.connect() as connection:
            if store._execute_bounded(connection,'SELECT 1 FROM schema_migrations WHERE version=69',(),deadline_at).fetchone() is None:
                raise ChunkRetirementError('parent_retirement_schema_required')
        while totals['batches'] < limits.max_batches:
            if time.monotonic() >= deadline_at or should_stop is not None and should_stop():
                result = dict(status='stopped',complete=False)
                break
            clear_left = limits.max_clear_bytes-totals['cleared_utf8_bytes']
            archive_left = limits.max_archive_bytes-meter.bytes
            if clear_left <= 0 or archive_left <= 0:
                break
            try:
                result = retire_window(store,meter,scope=scope,principal_id=owner_principal_id,
                    limits=replace(limits,max_batches=limits.max_batches-totals['batches'],
                        max_clear_bytes=clear_left,max_archive_bytes=archive_left),
                    remaining_bytes=clear_left,deadline_at=deadline_at,reviewed_plan=reviewed_plan,
                    required_scope_epoch=required_scope_epoch,metadata_mode=metadata_mode,should_stop=should_stop)
            except ChunkRetirementError as error:
                clear_limited = error.error_code == 'parent_retirement_window_clear_budget_hold'
                archive_limited = (error.error_code == 'parent_retirement_window_archive_budget' and totals['batches'] > 0)
                if not clear_limited and not archive_limited:
                    raise
                result = dict(status='partial',complete=False,clear_budget_limited=clear_limited,archive_budget_limited=archive_limited)
                break
            for key in totals:
                totals[key] += result.get(key,0)
            if result['complete'] or result['status']=='stopped' or result.get('clear_budget_limited'):
                break
        return {**result,**totals}
    except Exception as error:
        # Retain the original owner's conservative COMMIT classification and
        # any acknowledged windows preceding a later failure or lost ACK.
        for key in totals:
            totals[key] += getattr(error,'committed',{}).get(key,0)
        if isinstance(error,ChunkRetirementError):
            failure = error
        elif isinstance(error,SearchDeadlineExceeded):
            failure = ChunkRetirementError('parent_retirement_deadline_exceeded')
        elif isinstance(error,psycopg.errors.LockNotAvailable):
            failure = ChunkRetirementError('parent_retirement_lock_busy')
        else:
            failure = ChunkRetirementError('parent_retirement_unavailable')
        if not hasattr(failure,'cause_details'):
            label = type(error).__name__
            failure.cause_details = dict(error_class=label if len(label)<=80 and label.isidentifier() else 'UnknownError',
                cause_error_code=None,phase='maintenance')
        failure.committed = totals
        raise failure from None
