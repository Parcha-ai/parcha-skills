"""A few archived diagnostic pages within one retirement-child time slice."""
from dataclasses import asdict
import json
import time

import psycopg

from .archive import ArchiveError, ArchiveRequest, ArtifactReference
from .audit_retirement import AuditRetirement, _cutoff
from .chunk_retirement import ChunkRetirementError
from .db import SearchDeadlineExceeded
from .retirement_campaign import canonical, digest
from .retirement_runner import RetirementScope, _authorize

PAGE_ROWS = 5000
MAX_PAGES = 3
JOURNAL_BYTES = 65536


def _require(condition):
    if not condition:
        raise ChunkRetirementError('audit_checkpoint_invalid')


def _checkpoint(value):
    _require(isinstance(value, dict) and set(value) == {
        'version', 'cutoff', 'after', 'pending', 'exhausted', 'journal'})
    _require(len(canonical(value)) <= JOURNAL_BYTES)
    _require(value['version'] == 1 and type(value['version']) is int)
    _require(isinstance(value['cutoff'], str))
    _cutoff(value['cutoff'])
    _require(value['after'] is None or isinstance(value['after'], str))
    _require(type(value['exhausted']) is bool)
    _require(not value['exhausted'] or value['pending'] is None)
    _require(value['pending'] is None or isinstance(value['pending'], dict))
    _require(value['journal'] is None or isinstance(value['journal'], dict))
    return value


def retire_audit_pages(store, archive, source, *, deadline_at):
    """A persisted receipt fences every clear; exceptions never advance the keyset."""
    report = dict(audit_status='disabled', audit_cleared_rows=0,
                  audit_scanned_rows=0, audit_archived_bytes=0, audit_reconciled_rows=0,
                  audit_commit_outcome_unknown=False, audit_completed_pages=0)
    current = source.get('audit_checkpoint')
    if current is None or not source['enabled']:
        return report
    phase = 'checkpoint'
    clearing = False
    key = source['tenant_id'], source['source_id']
    scope = RetirementScope(source['tenant_id'], source['principal_id'], (source['source_id'],))

    def remaining():
        value = deadline_at - time.monotonic()
        if value <= 0:
            raise SearchDeadlineExceeded('audit maintenance deadline exceeded')
        return value

    def query(c, sql, params=()):
        remaining()
        return store._execute_bounded(c, sql, params, deadline_at)

    def guard(c, receipt=None):
        _authorize(store, c, scope, deadline_at)
        row = query(c, 'SELECT * FROM canonical_retirement_source_policy '
            'WHERE tenant_id=%s AND source_id=%s FOR UPDATE NOWAIT', key).fetchone()
        _require(row is not None and row['enabled'] and row['capture_complete']
                 and row['policy_epoch'] == source['policy_epoch']
                 and row['maintenance_epoch'] == source['maintenance_epoch']
                 and row['audit_checkpoint'] == current)
        if receipt is not None:
            _require(current['pending']['receipt'] == receipt)

    def save(value):
        nonlocal current
        with store.connect(timeout=remaining()) as c, c.transaction():
            guard(c)
            query(c, 'UPDATE canonical_retirement_source_policy SET audit_checkpoint=%s::jsonb '
                'WHERE tenant_id=%s AND source_id=%s', (json.dumps(value), *key))
        current = value

    owner = AuditRetirement(store, archive, deadline_at=deadline_at, before_clear=guard)
    try:
        current = _checkpoint(current)
        if current['exhausted']:
            return dict(report, audit_status='exhausted')
        for _ in range(MAX_PAGES):
            remaining()
            reconciling = current['pending'] is not None
            if not reconciling:
                phase = 'archive'
                receipt = owner.archive_page(tenant_id=key[0], source_id=key[1],
                    cutoff=current['cutoff'], after=current['after'], limit=PAGE_ROWS)
                report['audit_archived_bytes'] += receipt['raw_bytes']
                phase = 'journal'
                record = dict(contract='recall.audit-maintenance-page.v1',
                              previous=current['journal'], receipt=receipt)
                raw = canonical(record)
                _require(len(raw) <= JOURNAL_BYTES)
                reference = owner.put(ArchiveRequest(*key, 'audit-maintenance:' + digest(record),
                                                    'application/json', raw))
                _require(owner.read(reference, tenant_id=key[0], source_id=key[1]) == raw)
                phase = 'intent'
                save(dict(current, pending=dict(receipt=receipt, journal=asdict(reference))))
            else:
                phase = 'reconcile'
                pending = current['pending']
                _require(set(pending) == {'receipt', 'journal'})
                reference = ArtifactReference(**pending['journal'])
                _require(reference.size_bytes <= JOURNAL_BYTES)
                raw = owner.read(reference, tenant_id=key[0], source_id=key[1])
                expected = dict(contract='recall.audit-maintenance-page.v1',
                                previous=current['journal'], receipt=pending['receipt'])
                _require(raw == canonical(expected))
            receipt = current['pending']['receipt']
            _require(receipt['tenant_id'] == key[0] and receipt['source_id'] == key[1]
                     and receipt['cutoff'] == _cutoff(current['cutoff']).isoformat()
                     and receipt['after'] == current['after']
                     and 0 <= receipt['row_count'] <= receipt['scanned'] <= PAGE_ROWS
                     and (receipt['next_after'] == current['after'] if receipt['scanned'] == 0
                          else isinstance(receipt['next_after'], str)
                          and (current['after'] is None or receipt['next_after'] > current['after'])))
            # Reconcile earlier attempts before advancing. A fresh page goes directly
            # through the same native clear proof without a redundant archive read.
            phase = 'inspect'
            state = owner.inspect(receipt) if reconciling else dict(status='present')
            _require(state['status'] in {'present', 'absent'})
            if state['status'] == 'present' and receipt['row_count']:
                phase = 'clear'
                clearing = True
                outcome = owner.clear(receipt)
                clearing = False
                _require(outcome['status'] in {'cleared', 'absent'})
                report['audit_cleared_rows'] += outcome['rows']
            elif receipt['row_count']:
                report['audit_reconciled_rows'] += receipt['row_count']
            phase = 'advance'
            save(dict(current, after=receipt['next_after'], pending=None,
                      exhausted=receipt['exhausted'], journal=current['pending']['journal']))
            report['audit_scanned_rows'] += receipt['scanned']
            report['audit_completed_pages'] += 1
            if receipt['exhausted']:
                return dict(report, audit_status='exhausted')
        return dict(report, audit_status='page_budget')
    except (ChunkRetirementError, SearchDeadlineExceeded, ArchiveError, psycopg.Error,
            ValueError, TypeError, KeyError, TimeoutError) as error:
        return dict(report, audit_status='unknown' if clearing else 'deferred',
                    audit_commit_outcome_unknown=clearing, audit_phase=phase,
                    audit_error_class=type(error).__name__)
