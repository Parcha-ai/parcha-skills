"""One bounded local Slack attribution slice under the existing managed lease."""
from __future__ import annotations

import hashlib
import json
import time
from collections import Counter

from connectors.sdk import ConnectorRecordV2, MAX_PAGE_BYTES, MAX_PAGE_RECORDS, MAX_RECORD_BYTES

from .control import ControlError
from .canonical import _identity_sha256
from .archive import FilesystemArchiveStore

KEY = 'slack_captured_attribution'
DEBT = KEY + ':unresolved:'
DEBT_END = DEBT + '~'
RETRY_TURN = KEY + ':retry_turn'
_NOT_A_CONTACT_SKIP = object()
_DEBT_ERROR_TYPES = frozenset({
    'ArchiveError', 'ArchiveNotFound', 'ValueError', 'ConnectorContractError',
    'JSONDecodeError', 'TimeoutError', 'SearchDeadlineExceeded',
    'StructuredEnvelopeValidationError',
})
_CAPTURED_FAILURE_CODES = frozenset({
    'captured_archive_unavailable', 'captured_archive_corrupt',
    'captured_record_invalid', 'captured_commitment_changed',
})
_REPORTED_DEBT_REASONS = (_DEBT_ERROR_TYPES | _CAPTURED_FAILURE_CODES
                          | {'archive_unavailable', 'current_record_changed'})
_DEBT_SAMPLE_LIMIT = 32
_DEBT_EXAMPLE_LIMIT = 8
_DEBT_EXAMPLES_PER_REASON = 2


class _CapturedArchive:
    """One slice's actual archive bytes, including shared logical parts."""
    def __init__(self, archive):
        self.archive = archive
        self.consumed = 0

    def read_raw_bounded(self, reference, *, deadline_at):
        size = reference.get('size_bytes')
        if type(size) is not int or size <= 0 or self.consumed + size > MAX_PAGE_BYTES:
            raise ValueError('captured_archive_unavailable')
        if time.monotonic() >= deadline_at:
            raise TimeoutError()
        # A failed or uncertain read still consumes its reserved byte allowance.
        self.consumed += size
        bounded = getattr(self.archive, 'read_raw_bounded', None)
        if callable(bounded):
            return bounded(reference, deadline_at=deadline_at)
        if isinstance(self.archive, FilesystemArchiveStore):
            return self.archive.read_raw(reference)
        raise ValueError('captured_archive_unavailable')


def _debt_reason(error):
    # Only exact codes already raised by this owner may leave the exception.
    # Arbitrary messages (including codes embedded in text) remain private.
    if (type(error) is ValueError and len(error.args) == 1
            and type(error.args[0]) is str and error.args[0] in _CAPTURED_FAILURE_CODES):
        return error.args[0]
    name = type(error).__name__
    return name if name in _DEBT_ERROR_TYPES else 'archive_unavailable'


def _epoch(worker, row):
    with worker.store.connect() as c, c.transaction():
        c.execute("SELECT set_config('statement_timeout',%s,true)", (str(worker.store.search_deadline_ms),))
        worker._require_slack_page_authority(c, row)
        # This is the small tenant identity directory, never message bodies.
        identities = c.execute('''SELECT connector_id,namespace,subject_hmac_sha256,actor_id
            FROM brain_actor_external_identities WHERE tenant_id=%s
            ORDER BY connector_id,namespace,subject_hmac_sha256,actor_id''', (row['tenant_id'],)).fetchall()
    return hashlib.sha256(json.dumps(['slack-captured-attribution-v1', str(row['id']), row['revision'], str(row['connection_id']),
        row['tenant_id'], row['source_id'], row['privacy_mode'], row['provider_subject_id'],
        sorted(row['granted_scopes']), row['selectors'], identities], sort_keys=True).encode()).hexdigest()


def _save(db, state, *, retry_next=None):
    db.execute('INSERT OR REPLACE INTO meta(key,value) VALUES (?,?)', (KEY, json.dumps(state)))
    if retry_next is not None:
        # Keep the v1 checkpoint readable by the prior runtime. The scheduling
        # bit commits with acknowledged progress, never as a separate cursor.
        db.execute('INSERT OR REPLACE INTO meta(key,value) VALUES (?,?)',
            (RETRY_TURN, '1' if retry_next else '0'))
    db.commit()


def progress_report(row, runner):
    """Observe progress and a bounded debt sample in one read-only snapshot."""
    saved = dict(runner.db.execute("""SELECT key,value FROM meta WHERE key IN (?,?)
        UNION ALL
        SELECT key,value FROM (
            SELECT key,value FROM meta WHERE key>=? AND key<?
            ORDER BY key LIMIT ?)""",
        (KEY, RETRY_TURN, DEBT, DEBT_END, _DEBT_SAMPLE_LIMIT)))
    if KEY not in saved:
        raise ValueError('captured_progress_invalid')
    state = json.loads(saved.pop(KEY))
    turn = saved.pop(RETRY_TURN, None)
    has_debt = bool(saved)
    if (state['unresolved'] > 0) != has_debt:
        raise ValueError('captured_progress_invalid')
    reasons = Counter()
    examples = []
    example_reasons = Counter()
    for key, value in saved.items():
        try:
            debt = json.loads(value)
            reason = debt.get('reason') if isinstance(debt, dict) else None
            label = reason if isinstance(reason, str) and reason in _REPORTED_DEBT_REASONS else 'other'
            native_id = debt.get('native_id') if isinstance(debt, dict) else None
            # Diagnose actual persisted debt, never arbitrary exception text or
            # malformed rows. Examples share the existing bounded snapshot.
            if (label in _REPORTED_DEBT_REASONS and set(debt) == {'native_id', 'reason'}
                    and isinstance(native_id, str) and 0 < len(native_id) <= 512
                    and native_id.isascii() and all(33 <= ord(char) <= 126 for char in native_id)
                    and key == DEBT + hashlib.sha256(native_id.encode()).hexdigest()
                    and len(examples) < _DEBT_EXAMPLE_LIMIT
                    and example_reasons[label] < _DEBT_EXAMPLES_PER_REASON):
                examples.append(dict(native_id=native_id, reason=label))
                example_reasons[label] += 1
        except (ValueError, TypeError):
            label = 'invalid_debt_record'
        reasons[label] += 1
    return dict(installation_id=str(row['id']), source_id=row['source_id'],
        checkpoint_version=state['version'], authority_epoch=state['epoch'],
        frontier_sha256=hashlib.sha256(json.dumps(state['upper']).encode()).hexdigest(),
        forward_cursor_sha256=hashlib.sha256(state['after'].encode()).hexdigest(),
        retry_cursor_sha256=hashlib.sha256(state['retry_after'].encode()).hexdigest(),
        retry_turn=turn == '1' if turn in ('0', '1') else None,
        debt_reason_sample=dict(count=len(saved), limit=_DEBT_SAMPLE_LIMIT, reasons=dict(reasons)),
        debt_examples=examples, debt_examples_limit=_DEBT_EXAMPLE_LIMIT,
        phase=state['phase'], examined=state['examined'], unresolved=state['unresolved'],
        unresolved_entries_present=has_debt,
        status='unresolved' if has_debt else 'complete' if state['phase'] == 'complete' else 'in_progress')


def _page(worker, row, after, upper, limit, native_ids=None):
    with worker.store.connect() as c, c.transaction():
        c.execute("SELECT set_config('statement_timeout',%s,true)", (str(worker.store.search_deadline_ms),))
        worker._require_slack_page_authority(c, row)
        if c.execute('SELECT 1 FROM canonical_sources WHERE tenant_id=%s AND source_id=%s',
                (row['tenant_id'], row['source_id'])).fetchone() is None:
            return []
        worker.plane._require_existing_source_owner(c, tenant_id=row['tenant_id'],
            principal_id=row['principal_id'], source_id=row['source_id'])
        return c.execute('''SELECT event.event_id,event.native_id,event.content_sha256,
            event.native_parent_id,event.kind,event.occurred_at,event.observed_at,
            event.body_location,document.document_id,
            event.canonical_redacted#>>'{content,kind}' AS content_kind,
            artifact.artifact_id,artifact.storage_backend,artifact.object_key,
            artifact.content_sha256 AS raw_sha256,artifact.size_bytes,artifact.media_type,
            artifact.encryption,artifact.version_id,artifact.created_at,artifact.state
            FROM canonical_events event JOIN canonical_documents document
              USING(tenant_id,source_id,event_id)
            LEFT JOIN raw_artifacts artifact ON artifact.tenant_id=event.tenant_id
              AND artifact.source_id=event.source_id AND artifact.artifact_id=event.artifact_id
            WHERE event.tenant_id=%s AND event.source_id=%s
              AND event.native_id>%s AND (%s::text IS NULL OR event.native_id<=%s)
              AND (%s::text[] IS NULL OR event.native_id=ANY(%s))
              AND document.is_current AND document.deleted_at IS NULL AND NOT event.is_tombstone
              AND event.canonical_redacted#>>'{provenance,connector_id}'='slack.messages'
            ORDER BY event.native_id LIMIT %s''', (row['tenant_id'], row['source_id'], after,
                upper, upper, native_ids, native_ids, limit)).fetchall()


def _record(worker, row, selected, deadline, *, contacts_only=False, skip_only=False,
            archive_budget=None):
    archive_budget = archive_budget or _CapturedArchive(worker.archive)
    thinned = selected.get('body_location') == 'raw'
    with worker.store.connect() as c, c.transaction():
        c.execute("SELECT set_config('statement_timeout',%s,true)", (str(worker.store.search_deadline_ms),))
        worker._require_slack_page_authority(c, row)
        worker.plane._require_existing_source_owner(c, tenant_id=row['tenant_id'],
            principal_id=row['principal_id'], source_id=row['source_id'])
        if c.execute('''SELECT 1 FROM forget_tombstones WHERE tenant_id=%s AND source_id=%s
                AND target_identity_sha256=%s''', (row['tenant_id'], row['source_id'],
                _identity_sha256(row['tenant_id'], row['source_id'], selected['native_id']))).fetchone():
            return None
        document = selected['content_kind'] == 'document.v1' and not contacts_only
        event = worker._require_captured_slack_event(c, row, selected['native_id'],
            {selected['native_id']: (selected['event_id'], selected['content_sha256'])},
            **({'captured_document': True} if document or thinned else {}))
        if skip_only:
            # The native current-event check holds the event lock. Thinning may
            # have changed its representation since the page nominated a skip.
            kind = c.execute('''SELECT canonical_redacted#>>'{content,kind}' AS kind
                FROM canonical_events WHERE tenant_id=%s AND source_id=%s AND event_id=%s''',
                (row['tenant_id'], row['source_id'], selected['event_id'])).fetchone()['kind']
            return None if kind in {'communication_message.v1', 'document.v1'} else _NOT_A_CONTACT_SKIP
        if document and not thinned:
            # Validate accepted typed content before it leaves the read boundary.
            # Pure malformed-input failures become debt, never write failures.
            from .projectors import validate_envelope
            return validate_envelope(event)
    if thinned:
        from .chunk_bodies import read_archived_chunks
        from .projectors import validate_envelope
        bodies = read_archived_chunks(worker.store, archive_budget,
            tenant_id=row['tenant_id'], source_ids=(row['source_id'],),
            document_ids=(selected['document_id'],), deadline_at=deadline,
            located_only=True)
        chunks = bodies.get((row['source_id'], selected['document_id']))
        if not chunks:
            raise ValueError('captured_archive_unavailable')
        content = json.loads(''.join(chunk['text_redacted'] for chunk in chunks))
        restored = dict(
            {key: event[key] for key in ('principal_id', 'visibility', 'content_type', 'provenance')},
            schema_version=1, source_id=row['source_id'],
            native_id=selected['native_id'], kind=selected['kind'],
            occurred_at=selected['occurred_at'].isoformat(),
            observed_at=selected['observed_at'].isoformat(),
            content=content, content_sha256=selected['content_sha256'])
        if selected['native_parent_id'] is not None:
            restored['native_parent_id'] = selected['native_parent_id']
        validate_envelope(restored)
        with worker.store.connect() as c, c.transaction():
            c.execute("SELECT set_config('statement_timeout',%s,true)", (str(worker.store.search_deadline_ms),))
            worker._require_slack_page_authority(c, row)
            worker.plane._require_existing_source_owner(c, tenant_id=row['tenant_id'],
                principal_id=row['principal_id'], source_id=row['source_id'])
            if c.execute('''SELECT 1 FROM forget_tombstones WHERE tenant_id=%s AND source_id=%s
                    AND target_identity_sha256=%s''', (row['tenant_id'], row['source_id'],
                    _identity_sha256(row['tenant_id'], row['source_id'], selected['native_id']))).fetchone():
                return None
            worker._require_captured_slack_event(c, row, selected['native_id'],
                {selected['native_id']: (selected['event_id'], selected['content_sha256'])})
        if content.get('kind') in {'communication_message.v1', 'document.v1'}:
            # These are already privacy-filtered canonical commitments. Contact
            # identity enrollment still needs the original SDK input below.
            return None if contacts_only else restored
    if contacts_only and selected['content_kind'] in {'communication_message.v1', 'document.v1'}:
        return None  # The messages phase owns non-contact attribution.
    if (selected['state'] != 'live' or selected['media_type'] != 'application/json'
            or not isinstance(selected['size_bytes'], int)
            or not 0 < selected['size_bytes'] <= MAX_RECORD_BYTES + 100_000):
        raise ValueError('captured_archive_unavailable')
    reference = dict(contract='recall.artifact-ref.v1', schema_version=1,
        tenant_id=row['tenant_id'], source_id=row['source_id'],
        **{key: selected[key] for key in ('artifact_id', 'storage_backend', 'object_key',
            'size_bytes', 'media_type', 'encryption', 'version_id')},
        content_sha256=selected['raw_sha256'], created_at=selected['created_at'].isoformat())
    if time.monotonic() >= deadline:
        raise TimeoutError()
    payload = archive_budget.read_raw_bounded(reference, deadline_at=deadline)
    if time.monotonic() >= deadline:
        raise TimeoutError()
    if len(payload) != reference['size_bytes'] or hashlib.sha256(payload).hexdigest() != selected['raw_sha256']:
        raise ValueError('captured_archive_corrupt')
    raw = json.loads(payload)
    if not isinstance(raw, dict) or set(raw) != {'schema_version', 'native_id', 'native_parent_id',
            'occurred_at', 'content', 'provenance', 'deleted'}:
        raise ValueError('captured_record_invalid')
    record = ConnectorRecordV2(**raw)
    if record.schema_version != 2 or record.deleted or record.native_id != selected['native_id']:
        raise ValueError('captured_record_invalid')
    event = worker._slack_record_event(row, record)
    if event is None:
        return None
    if event['content_sha256'] != selected['content_sha256']:
        raise ValueError('captured_commitment_changed')
    return record


def repair_slice(worker, row, runner, *, deadline_at):
    """SQL ACK precedes spool progress; an interrupted cross-store ACK repeats safely."""
    db = runner.db
    epoch = _epoch(worker, row)
    saved = db.execute('SELECT value FROM meta WHERE key=?', (KEY,)).fetchone()
    state = json.loads(saved[0]) if saved else None
    if state is not None and (not isinstance(state, dict) or set(state) != {
            'version', 'epoch', 'phase', 'after', 'upper', 'examined', 'unresolved', 'retry_after'}
            or type(state['version']) is not int or state['version'] != 1 or state['phase'] not in {'contacts', 'messages', 'complete'}
            or any(type(state[key]) is not int or state[key] < 0 for key in ('examined', 'unresolved'))
            or any(not isinstance(state[key], str) for key in ('epoch', 'after', 'retry_after'))
            or state['upper'] is not None and not isinstance(state['upper'], str)):
        raise ValueError('captured_progress_invalid')
    if state is None or state['epoch'] != epoch:
        state = dict(version=1, epoch=epoch, phase='contacts', after='', upper=None,
            examined=0, unresolved=0, retry_after='')
        # Re-examine the full source under the new epoch, including prior debt.
        db.execute('DELETE FROM meta WHERE key>=? AND key<?', (DEBT, DEBT_END))
        _save(db, state, retry_next=True)
    if state['phase'] == 'complete' and not state['unresolved']:
        if db.execute('SELECT 1 FROM meta WHERE key>=? AND key<? LIMIT 1', (DEBT, DEBT_END)).fetchone():
            raise ValueError('captured_progress_invalid')
        return state
    now = time.monotonic()
    deadline = min(now + worker.interval_seconds, now + max(0, deadline_at - now) / 2)
    turn = db.execute('SELECT value FROM meta WHERE key=?', (RETRY_TURN,)).fetchone()
    if turn is not None and turn[0] not in ('0', '1'):
        raise ValueError('captured_progress_invalid')
    retry = state['phase'] == 'complete' or bool(state['unresolved'] and (turn is None or turn[0] == '1'))
    debts = db.execute('SELECT key,value FROM meta WHERE key>=? AND key<? AND key>? ORDER BY key LIMIT ?',
        (DEBT, DEBT_END, state['retry_after'], MAX_PAGE_RECORDS)).fetchall() if retry else []
    if retry and not debts:
        debts = db.execute('SELECT key,value FROM meta WHERE key>=? AND key<? ORDER BY key LIMIT ?',
            (DEBT, DEBT_END, MAX_PAGE_RECORDS)).fetchall()
    ids = [json.loads(item[1])['native_id'] for item in debts] if retry else None
    if retry and not ids:
        raise ValueError('captured_progress_invalid')
    page = _page(worker, row, '' if retry else state['after'], state['upper'], MAX_PAGE_RECORDS, ids)
    # Bound this pass's historical native-key frontier. New normal ingest still
    # owns attribution; an identity epoch change schedules a new complete pass.
    if state['upper'] is None:
        with worker.store.connect() as c, c.transaction():
            c.execute("SELECT set_config('statement_timeout',%s,true)", (str(worker.store.search_deadline_ms),))
            worker._require_slack_page_authority(c, row)
            top = c.execute('''SELECT native_id FROM canonical_events
                WHERE tenant_id=%s AND source_id=%s ORDER BY native_id DESC LIMIT 1''',
                (row['tenant_id'], row['source_id'])).fetchone()
        state['upper'] = top['native_id'] if top else ''
    if retry:
        present = {item['native_id'] for item in page}
        # The current indexed query also supplies terminal deletion/supersession
        # dispositions; absence is not an archive or author-eligibility claim.
        removed = 0
        for key, value in debts:
            if json.loads(value)['native_id'] not in present:
                removed += db.execute('DELETE FROM meta WHERE key=?', (key,)).rowcount
        state['unresolved'] -= removed
        page.sort(key=lambda item: hashlib.sha256(item['native_id'].encode()).hexdigest())
        _save(db, state, retry_next=False if removed else None)
    archive_budget = _CapturedArchive(worker.archive)
    for selected in page:
        contacts_only = state['phase'] == 'contacts' and not retry
        no_raw_read = (selected.get('body_location') == 'raw' or
            selected['content_kind'] == 'document.v1' or
            contacts_only and selected['content_kind'] == 'communication_message.v1')
        charge = 0 if no_raw_read else min(max(0, selected['size_bytes'] or 0), MAX_RECORD_BYTES + 100_000)
        if time.monotonic() >= deadline or archive_budget.consumed + charge > MAX_PAGE_BYTES:
            return state
        reason = None
        skip = contacts_only and selected['content_kind'] in {'communication_message.v1', 'document.v1'}
        if skip:
            debt_key = DEBT + hashlib.sha256(selected['native_id'].encode()).hexdigest()
            existing_debt = db.execute('SELECT 1 FROM meta WHERE key=?', (debt_key,)).fetchone()
        try:
            record = _record(worker, row, selected, deadline,
                contacts_only=contacts_only, archive_budget=archive_budget,
                **({'skip_only': True} if skip else {}))
            if record is _NOT_A_CONTACT_SKIP:
                return state  # Reconsider the changed representation on the next normal page.
        except ControlError:
            raise
        except Exception as error:
            # Read failures remain debt. Effectful errors below must propagate.
            reason = _debt_reason(error)
            record = None
        if not skip:
            debt_key = DEBT + hashlib.sha256(selected['native_id'].encode()).hexdigest()
            existing_debt = db.execute('SELECT 1 FROM meta WHERE key=?', (debt_key,)).fetchone()
        content = record.get('content') if isinstance(record, dict) else record.content if record is not None else None
        relevant = record is not None and (retry or existing_debt or
            (content.get('kind') == 'contact_identity.v1') == (state['phase'] == 'contacts'))
        if relevant:
            expected = {selected['native_id']: (selected['event_id'], selected['content_sha256'])}
            try:
                if isinstance(record, dict):
                    worker._reconcile_slack_page(row, (), expected_events=expected, captured_documents=(record,))
                else:
                    worker._reconcile_slack_page(row, (record,), expected_events=expected)
            except ControlError as error:
                if error.code != 'connector_record_changed':
                    raise
                reason = 'current_record_changed'
        if not (skip and reason is None):
            # Recheck authority even for a missing archive or a privacy exclusion.
            with worker.store.connect() as c, c.transaction():
                c.execute("SELECT set_config('statement_timeout',%s,true)", (str(worker.store.search_deadline_ms),))
                worker._require_slack_page_authority(c, row)
                worker.plane._require_existing_source_owner(c, tenant_id=row['tenant_id'],
                    principal_id=row['principal_id'], source_id=row['source_id'])
        if reason:
            state['unresolved'] += int(not existing_debt)
            db.execute('INSERT OR REPLACE INTO meta(key,value) VALUES (?,?)',
                (debt_key, json.dumps(dict(native_id=selected['native_id'], reason=reason))))
        else:
            state['unresolved'] -= db.execute('DELETE FROM meta WHERE key=?', (debt_key,)).rowcount
        if retry:
            state['retry_after'] = debt_key
        state['examined'] += 1
        state['after'] = selected['native_id'] if not retry else state['after']
        _save(db, state, retry_next=not retry)
    if not retry and len(page) < MAX_PAGE_RECORDS:
        if _epoch(worker, row) != state['epoch']:
            # Includes our own contact derivation. One harmless repeated pass
            # avoids blessing an unrelated enrollment committed mid-scan.
            return state
        state['phase'] = 'messages' if state['phase'] == 'contacts' else 'complete'
        state['after'] = ''
        _save(db, state, retry_next=True)
    return state
