"""Owner-approved future enrollment at the existing logical publication boundary."""
from dataclasses import asdict
import hashlib
import time

import orjson
import psycopg

from .chunk_retirement import ChunkRetirementError
from .db import SearchDeadlineExceeded
from .parent_chunk_proof import read_parent_catalog
from .retirement_runner import RetirementScope, _authorize, _bind_reviewed_parent, _cohort_entry, _digest


def _key(scope):
    if not isinstance(scope, RetirementScope) or len(scope.source_ids) != 1:
        raise ChunkRetirementError('retirement_policy_scope_invalid')
    return scope.tenant_id, scope.source_ids[0]


def _deadline(deadline_at):
    return min(time.monotonic() + 5, deadline_at) if deadline_at is not None else time.monotonic() + 5


def _authorize_policy(store, connection, scope, deadline_at):
    markers = store._execute_bounded(connection,
        'SELECT version FROM schema_migrations WHERE version IN (75,76)', (), deadline_at).fetchall()
    if {row['version'] for row in markers} != {75, 76}:
        raise ChunkRetirementError('retirement_policy_schema_missing')
    _authorize(store, connection, scope, deadline_at)


def _row(store, connection, scope, deadline_at, *, lock=False):
    row = store._execute_bounded(connection, '''SELECT * FROM canonical_retirement_source_policy
        WHERE tenant_id=%s AND source_id=%s''' + (' FOR UPDATE NOWAIT' if lock else ''),
        _key(scope), deadline_at).fetchone()
    if row is None or row['principal_id'] != scope.principal_id:
        raise ChunkRetirementError('retirement_policy_missing')
    return row


def start_policy(store, *, scope, allow_detached_rebind=False, deadline_at=None):
    """Start one disabled capture. Replays retain the same cutoff and cursor."""
    if type(allow_detached_rebind) is not bool:
        raise ChunkRetirementError('retirement_policy_scope_invalid')
    key, deadline_at = _key(scope), _deadline(deadline_at)
    with store.connect() as c, c.transaction():
        _authorize_policy(store, c, scope, deadline_at)
        store._execute_bounded(c, '''INSERT INTO canonical_retirement_source_policy
            (tenant_id,source_id,principal_id,allow_detached_rebind)
            VALUES(%s,%s,%s,%s) ON CONFLICT DO NOTHING''',
            (*key, scope.principal_id, allow_detached_rebind), deadline_at)
        row = _row(store, c, scope, deadline_at)
        if row['allow_detached_rebind'] != allow_detached_rebind:
            raise ChunkRetirementError('retirement_policy_scope_changed')
        return _plan(scope, row)


# Walk distinct indexed parent keys without scanning every event of a hot parent.
_BASELINE_PAGE = """WITH RECURSIVE parents(native_parent_id,step) AS (
    (SELECT native_parent_id,1 FROM (
        (SELECT COALESCE(native_parent_id,native_id) AS native_parent_id FROM canonical_events
         WHERE tenant_id=%s AND source_id=%s
           AND (%s::text IS NULL OR COALESCE(native_parent_id,native_id)>%s)
         ORDER BY COALESCE(native_parent_id,native_id) LIMIT 1)
        UNION ALL
        (SELECT native_parent_id FROM canonical_evidence_documents
         WHERE tenant_id=%s AND source_id=%s AND (%s::text IS NULL OR native_parent_id>%s)
         ORDER BY native_parent_id LIMIT 1)
    ) first_keys ORDER BY native_parent_id LIMIT 1)
    UNION ALL
    SELECT next_parent.native_parent_id,parents.step+1 FROM parents
    CROSS JOIN LATERAL (
        SELECT native_parent_id FROM (
            (SELECT COALESCE(event.native_parent_id,event.native_id) AS native_parent_id
             FROM canonical_events event WHERE event.tenant_id=%s AND event.source_id=%s
               AND COALESCE(event.native_parent_id,event.native_id)>parents.native_parent_id
             ORDER BY COALESCE(event.native_parent_id,event.native_id) LIMIT 1)
            UNION ALL
            (SELECT evidence.native_parent_id FROM canonical_evidence_documents evidence
             WHERE evidence.tenant_id=%s AND evidence.source_id=%s
               AND evidence.native_parent_id>parents.native_parent_id
             ORDER BY evidence.native_parent_id LIMIT 1)
        ) next_keys ORDER BY native_parent_id LIMIT 1
    ) next_parent WHERE parents.step<%s
) SELECT native_parent_id FROM parents ORDER BY native_parent_id"""


def _baseline_values(key, cursor, limit):
    return (*key, cursor, cursor, *key, cursor, cursor, *key, *key, limit)


def capture_baseline(store, *, scope, limit=100, deadline_at=None):
    """Commit one bounded page and its cursor together; never overwrite intent."""
    if type(limit) is not int or not 1 <= limit <= 500:
        raise ChunkRetirementError('retirement_policy_page_invalid')
    key, deadline_at = _key(scope), _deadline(deadline_at)
    with store.connect() as c, c.transaction():
        _authorize_policy(store, c, scope, deadline_at)
        row = _row(store, c, scope, deadline_at, lock=True)
        if row['capture_complete']:
            return dict(complete=True, captured_parents=row['captured_parents'])
        cursor = row['capture_cursor']
        parents = store._execute_bounded(c, _BASELINE_PAGE,
            _baseline_values(key, cursor, limit), deadline_at).fetchall()
        names = [p['native_parent_id'] for p in parents]
        if names:
            store._execute_bounded(c, '''INSERT INTO canonical_chunk_retirement_progress
                (tenant_id,source_id,native_parent_id,enabled,status)
                SELECT %s,%s,parent,false,'baseline' FROM unnest(%s::text[]) AS parent
                ON CONFLICT DO NOTHING''', (*key, names), deadline_at)
        complete = len(names) < limit
        store._execute_bounded(c, '''UPDATE canonical_retirement_source_policy
            SET capture_cursor=%s,capture_complete=%s,captured_parents=captured_parents+%s,
                eligible_after=CASE WHEN %s THEN clock_timestamp() ELSE eligible_after END,
                policy_epoch=policy_epoch+1,updated_at=clock_timestamp()
            WHERE tenant_id=%s AND source_id=%s''',
            (names[-1] if names else cursor, complete, len(names), complete, *key), deadline_at)
        return dict(complete=complete, captured_parents=row['captured_parents'] + len(names))


def _plan(scope, row):
    value = dict(contract='recall.retirement-policy.v1', scope=asdict(scope),
        policy_epoch=row['policy_epoch'], enabled=row['enabled'],
        allow_detached_rebind=row['allow_detached_rebind'],
        eligible_after=row['eligible_after'].isoformat() if row['eligible_after'] else None,
        capture_started_at=row['capture_started_at'].isoformat(),
        capture_complete=row['capture_complete'], captured_parents=row['captured_parents'])
    value = orjson.loads(orjson.dumps(value))
    value['proof_sha256'] = hashlib.sha256(orjson.dumps(value, option=orjson.OPT_SORT_KEYS)).hexdigest()
    return value


def policy_plan(store, *, scope, deadline_at=None):
    deadline_at = _deadline(deadline_at)
    with store.connect() as c, c.transaction():
        _authorize_policy(store, c, scope, deadline_at)
        return _plan(scope, _row(store, c, scope, deadline_at))


def set_policy_enabled(store, *, scope, reviewed_plan, enabled, deadline_at=None):
    if type(enabled) is not bool:
        raise ChunkRetirementError('retirement_policy_state_invalid')
    key, deadline_at = _key(scope), _deadline(deadline_at)
    with store.connect() as c, c.transaction():
        _authorize_policy(store, c, scope, deadline_at)
        row = _row(store, c, scope, deadline_at, lock=True)
        if reviewed_plan != _plan(scope, row) or (enabled and not row['capture_complete']):
            raise ChunkRetirementError('retirement_policy_review_changed')
        store._execute_bounded(c, '''UPDATE canonical_retirement_source_policy
            SET enabled=%s,policy_epoch=policy_epoch+1,updated_at=clock_timestamp()
            WHERE tenant_id=%s AND source_id=%s''', (enabled, *key), deadline_at)
        return dict(enabled=enabled, policy_epoch=row['policy_epoch'] + 1)


def enroll_published_parent(store, connection, key):
    """Optional bounded metadata work; a refusal cannot roll back publication."""
    old_timeout = connection.execute("SELECT current_setting('statement_timeout') AS value").fetchone()['value']
    try:
        with connection.transaction():
            deadline_at = time.monotonic() + 0.15
            def execute(sql, values=()):
                return store._execute_bounded(connection, sql, values, deadline_at)
            if execute('SELECT 1 FROM schema_migrations WHERE version=76').fetchone() is None:
                return 'schema_absent'
            policy = execute('''SELECT * FROM canonical_retirement_source_policy
                WHERE tenant_id=%s AND source_id=%s AND enabled FOR SHARE NOWAIT''', key[:2]).fetchone()
            if policy is None:
                return 'off'
            scope = RetirementScope(key[0], policy['principal_id'], (key[1],))
            _authorize(store, connection, scope, deadline_at)
            progress = execute('''SELECT * FROM canonical_chunk_retirement_progress
                WHERE tenant_id=%s AND source_id=%s AND native_parent_id=%s''', key).fetchone()
            if progress:
                if progress['status'] != 'detached' or not policy['allow_detached_rebind']:
                    return 'represented'
                catalog = read_parent_catalog(store, connection, key, deadline_at, lock=True)
                plan = dict(contract='recall.retirement-cohort.v1', scope=asdict(scope),
                    parents=[_cohort_entry(catalog['manifest'])], after=None, more=False,
                    next_cursor=[key[1], key[2]])
                plan = orjson.loads(orjson.dumps(plan))
                plan['proof_sha256'] = _digest(plan)
                _bind_reviewed_parent(store, scope=scope, reviewed_plan=plan,
                    expected_scope_epoch=progress['scope_epoch'], required_status='detached',
                    deadline_at=deadline_at, connection=connection)
                return 'rebound'
            # The cutoff also excludes pre-capture events committed after a page
            # passed their key; the frozen ledger preserves exclusions on rebuild.
            if execute('''SELECT 1 FROM canonical_events WHERE tenant_id=%s AND source_id=%s
                AND COALESCE(native_parent_id,native_id)=%s AND created_at<=%s LIMIT 1''',
                (*key, policy['eligible_after'])).fetchone():
                execute('''INSERT INTO canonical_chunk_retirement_progress
                    (tenant_id,source_id,native_parent_id,enabled,status)
                    VALUES(%s,%s,%s,false,'baseline') ON CONFLICT DO NOTHING''', key)
                return 'before_policy'
            catalog = execute('''SELECT logical_document_id FROM canonical_evidence_documents
                WHERE tenant_id=%s AND source_id=%s AND native_parent_id=%s FOR SHARE NOWAIT''', key).fetchone()
            if catalog is None:
                return 'catalog_absent'
            inserted = execute('''INSERT INTO canonical_chunk_retirement_progress
                (tenant_id,source_id,native_parent_id,logical_document_id,enabled,status)
                VALUES(%s,%s,%s,%s,true,'pending') ON CONFLICT DO NOTHING''',
                (*key, catalog['logical_document_id'])).rowcount
            return 'enrolled' if inserted else 'represented'
    except (ChunkRetirementError, SearchDeadlineExceeded, psycopg.Error):
        return 'deferred'
    finally:
        connection.execute("SELECT set_config('statement_timeout',%s,true)", (old_timeout,))
