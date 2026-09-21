#!/usr/bin/env python3
"""Actual locator/clear callers keep native conflicts, rollback and partial counts."""
from pathlib import Path
import json
import os
import sys
import tempfile
import time
import uuid
from unittest.mock import patch

import psycopg
from psycopg.conninfo import conninfo_to_dict, make_conninfo

SERVER = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(SERVER.parent), str(SERVER)]
from e2e_archive_reprojection import fixture  # noqa: E402
from e2e_logical_source_integrity import TrackedStore  # noqa: E402
from recall_server import chunk_retirement as retirement  # noqa: E402
from recall_server import streaming_locators as locators  # noqa: E402


def snapshot(store, scope):
    with store.connect() as conn:
        args = scope['tenant_id'], scope['source_id']
        documents = conn.execute('''SELECT d.native_id,d.body_record_ordinal,
            d.body_record_count,c.ordinal,c.text_redacted FROM canonical_documents d
            JOIN canonical_chunks c USING(tenant_id,source_id,document_id)
            WHERE d.tenant_id=%s AND d.source_id=%s ORDER BY d.native_id,c.ordinal''', args).fetchall()
        ledger = conn.execute('''SELECT * FROM canonical_chunk_retirement_progress
            WHERE tenant_id=%s AND source_id=%s''', args).fetchone()
        return documents, ledger


def case(store, root, mode, failure):
    count = 64 if failure == 'none' else 6
    tenant, source, archive, _, _, _ = fixture(store, root, count=count)
    scope = dict(tenant_id=tenant, source_id=source, native_parent_id='session')
    principal = 'principal:reprojection'
    with store.connect() as conn:
        conn.execute("INSERT INTO canonical_source_grants VALUES(%s,%s,%s,'owner',now())", (tenant, principal, source))
        if mode == 'locate':
            conn.execute('''UPDATE canonical_documents SET body_record_ordinal=NULL,
                body_record_count=NULL WHERE tenant_id=%s AND source_id=%s''', (tenant, source))
    retirement.set_parent_retirement_enabled(store, **scope, enabled=mode == 'clear')
    initial = snapshot(store, scope)
    operation = retirement.retire_parent_chunks if mode == 'clear' else locators.publish_parent_locators
    error_type = retirement.ChunkRetirementError if mode == 'clear' else locators.LocatorPublicationError
    limits = retirement.ParentRetirementLimits(batch_documents=64 if failure == 'none' else 3)

    def apply():
        preview = operation(store, archive, **scope, owner_principal_id=principal, limits=limits)
        return operation(store, archive, **scope, owner_principal_id=principal, limits=limits,
                         apply=True, reviewed_plan=preview['plan'], deadline_at=time.monotonic()+30)

    execute = store._execute_bounded
    arrays, timed_out = [], []
    def observe(conn, sql, values, deadline_at):
        result = execute(conn, sql, values, deadline_at)
        if 'WITH ORDINALITY AS keys' in sql:
            arrays.append(values[0])
        update = ("UPDATE canonical_chunks SET text_redacted=''" if mode == 'clear'
                  else 'UPDATE canonical_documents SET body_record_ordinal=')
        if failure == 'deadline' and update in sql and not timed_out:
            timed_out.append(True)
            # Real SQL timeout after a real UPDATE; the caller must roll it back.
            execute(conn, 'SELECT pg_sleep(0.2)', (), time.monotonic()+0.03)
        return result

    if failure in ('conflict', 'partial'):
        blocked = 1 if failure == 'conflict' else 4
        key = f'v2\x1f{tenant}\x1f{source}\x1fevent-{blocked:04}'
        with psycopg.connect(store.dsn) as competing:
            assert competing.execute('SELECT pg_try_advisory_xact_lock(hashtextextended(%s,0))', (key,)).fetchone()[0]
            with patch.object(store, '_execute_bounded', side_effect=observe):
                try:
                    apply()
                    raise AssertionError('native conflict accepted')
                except error_type as error:
                    expected = 0 if failure == 'conflict' else 3
                    assert error.committed['batches'] == expected//3
                    field = 'cleared_documents' if mode == 'clear' else 'published_documents'
                    assert error.committed[field] == expected
                    assert str(error) == ('parent_retirement_lock_busy' if mode == 'clear' else 'locator_publication_lock_busy')
                    if mode == 'locate':
                        assert not error.commit_unknown
            current = snapshot(store, scope)
            if failure == 'conflict':
                assert current == initial, 'conflict advanced bodies, locators or ledger'
            elif mode == 'clear':
                assert sum(row['text_redacted'] == '' for row in current[0]) == 3
                assert current[1]['cumulative_cleared_documents'] == 3
            else:
                assert sum(row['body_record_ordinal'] is not None for row in current[0]) == 3
                assert current[1] == initial[1]
                assert [row['text_redacted'] for row in current[0]] == [row['text_redacted'] for row in initial[0]]
            # The failed transaction may have acquired keys on both sides of
            # the conflict. Its rollback must release every one of them.
            with psycopg.connect(store.dsn) as observer:
                for n in range(count):
                    other = f'v2\x1f{tenant}\x1f{source}\x1fevent-{n:04}'
                    acquired = observer.execute('SELECT pg_try_advisory_xact_lock(hashtextextended(%s,0))', (other,)).fetchone()[0]
                    assert acquired == (n != blocked)
        assert len(arrays) == (1 if failure == 'conflict' else 2)
        resumed = apply()
        field = 'cleared_documents' if mode == 'clear' else 'published_documents'
        assert resumed['complete'] and resumed[field] == count-expected
    elif failure == 'deadline':
        with patch.object(store, '_execute_bounded', side_effect=observe):
            try:
                apply()
                raise AssertionError('SQL deadline accepted')
            except error_type as error:
                assert error.committed['batches'] == 0
                if mode == 'locate':
                    assert not error.commit_unknown
        assert timed_out and len(arrays) == 1 and snapshot(store, scope) == initial
        resumed = apply()
        assert resumed['complete']
    else:
        with patch.object(store, '_execute_bounded', side_effect=observe):
            result = apply()
        assert result['complete'] and result['batches'] == 1 and len(arrays) == 1
        assert arrays[0] == sorted(f'v2\x1f{tenant}\x1f{source}\x1fevent-{n:04}' for n in range(count))
    final = snapshot(store, scope)
    if mode == 'clear':
        assert all(row['text_redacted'] == '' for row in final[0])
        assert final[1]['cumulative_cleared_documents'] == count
    else:
        assert all(row['body_record_ordinal'] is not None for row in final[0])
        assert final[1] == initial[1]
        assert [row['text_redacted'] for row in final[0]] == [row['text_redacted'] for row in initial[0]]


def main():
    admin_dsn = os.environ['RECALL_DATABASE_URL']
    database = 'recall_native_locks_' + uuid.uuid4().hex
    with psycopg.connect(admin_dsn, autocommit=True) as admin:
        admin.execute(f'CREATE DATABASE "{database}"')
    store = TrackedStore(make_conninfo(**(conninfo_to_dict(admin_dsn) | {'dbname': database})))
    store.search_deadline_ms = 30000
    try:
        store.migrate()
        with tempfile.TemporaryDirectory() as folder, patch.dict(os.environ, RECALL_CHUNK_BODY_READS='archive'):
            for mode in ('clear', 'locate'):
                for failure in ('none', 'conflict', 'partial', 'deadline'):
                    case(store, Path(folder), mode, failure)
        print(json.dumps(dict(status='pass', scenarios=8, one_lock_query_per_batch=True,
                              native_conflicts_and_rollback=True, partial_commits_exact=True)))
    finally:
        store.close()
        with psycopg.connect(admin_dsn, autocommit=True) as admin:
            admin.execute(f'DROP DATABASE "{database}" WITH (FORCE)')


if __name__ == '__main__':
    main()
