#!/usr/bin/env python3
"""Disposable PG proof of NULL-only locator publication and ingest races."""
from __future__ import annotations
import json
import os
from pathlib import Path
import sys
import tempfile
import threading
import time
import uuid
from unittest.mock import patch

import psycopg
from psycopg.conninfo import conninfo_to_dict, make_conninfo

SERVER = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SERVER))
from e2e_archive_reprojection import fixture, mark_dirty
from e2e_locator_backfill_plan import state
from e2e_logical_evidence_projection import insert_record
from e2e_logical_source_integrity import TrackedStore
from recall_server import locator_backfill_plan as planner
from recall_server.locator_backfill_plan import apply_parent, LocatorPlanError
from recall_server.logical_evidence_projection import mark_logical_evidence_dirty


def prepared(store, root):
    tenant, source, archive, projection, projector, texts = fixture(store, root)
    with store.connect() as connection:
        connection.execute("UPDATE canonical_documents SET body_record_ordinal=NULL,body_record_count=NULL WHERE tenant_id=%s AND source_id=%s AND native_id <> 'event-0002'", (tenant, source))
    return tenant, source, archive, projector


def invoke(store, tenant, source, archive):
    return apply_parent(store, archive, tenant_id=tenant, source_id=source, native_parent_id='session')


def basic(store, root):
    tenant, source, archive, projector = prepared(store, root)
    before, uploads = state(store, tenant, source), archive.uploads
    result = invoke(store, tenant, source, archive)
    assert result['status'] == 'applied' and result['applied_documents'] == 2, result
    after = state(store, tenant, source)
    for old, new in zip(before, after):
        assert old['texts'] == new['texts'] and old['document_text'] == new['document_text']
        assert old['is_current'] == new['is_current']
        if old['body_record_ordinal'] is not None:
            assert old == new, 'valid existing locator was overwritten or rewritten'
        else:
            assert new['body_record_ordinal'] is not None and new['body_record_count'] == 1
    assert archive.uploads == uploads
    assert invoke(store, tenant, source, archive)['applied_documents'] == 0
    assert state(store, tenant, source) == after


def append_race(store, root, *, after_fence):
    tenant, source, archive, projector = prepared(store, root)
    original = planner._snapshot_on_connection
    calls = 0

    def append():
        # This is a second real connection while apply holds its row locks.
        with store.connect() as connection:
            connection.execute('SET LOCAL statement_timeout=3000')
            insert_record(connection, tenant=tenant, source=source, parent='session',
                          native='late-append', text='new unarchived body', role='assistant', byte_start=999)
            mark_logical_evidence_dirty(connection, tenant_id=tenant, source_id=source,
                                        native_ids=['late-append'], reason='ingest')

    def snapshot(*args):
        nonlocal calls
        calls += 1
        if calls == 3 and not after_fence:
            append()
        value = original(*args)
        if calls == 3 and after_fence:
            append()
        return value

    with patch.object(planner, '_snapshot_on_connection', side_effect=snapshot):
        if after_fence:
            assert invoke(store, tenant, source, archive)['applied_documents'] == 2
        else:
            try:
                invoke(store, tenant, source, archive)
            except LocatorPlanError as error:
                assert str(error) == 'locator_plan_catalog_changed', error
            else:
                raise AssertionError('append before the final fence was accepted')
    with store.connect() as connection:
        documents = connection.execute('SELECT native_id,body_record_ordinal FROM canonical_documents WHERE tenant_id=%s AND source_id=%s', (tenant, source)).fetchall()
        by_native = {row['native_id']: row['body_record_ordinal'] for row in documents}
        assert by_native['late-append'] is None
        assert (by_native['event-0000'] is not None) == after_fence
        assert (by_native['event-0001'] is not None) == after_fence
        assert by_native['event-0002'] == 2
        assert connection.execute('SELECT count(*) AS n FROM canonical_evidence_document_queue WHERE tenant_id=%s AND source_id=%s', (tenant, source)).fetchone()['n'] == 1
    # A fresh proof still recognizes the old exact bodies, and cannot propose
    # the unarchived append. No current catalog was changed by the backfill.
    report = planner.plan_parent(store, archive, tenant_id=tenant, source_id=source, native_parent_id='session')
    assert report['eligible_documents'] == 3 and report['excluded']['pending_new_document'] == 1


def revision(connection, tenant, source):
    connection.execute("UPDATE canonical_documents SET is_current=false WHERE tenant_id=%s AND source_id=%s AND native_id='event-0000' AND is_current", (tenant, source))
    receipt = insert_record(connection, tenant=tenant, source=source, parent='session', native='replacement',
                            text='revision two', role='assistant', byte_start=0)
    connection.execute("UPDATE canonical_events SET native_id='event-0000',revision=2 WHERE tenant_id=%s AND source_id=%s AND native_id='replacement'", (tenant, source))
    connection.execute("UPDATE canonical_documents SET native_id='event-0000',revision=2 WHERE tenant_id=%s AND source_id=%s AND native_id='replacement'", (tenant, source))
    connection.execute('UPDATE canonical_chunks SET receipt=%s WHERE receipt=%s', (f'recall://{source}/event-0000?rev=2#item=0', receipt))


def ingest_owns_document_first(store, root):
    tenant, source, archive, projector = prepared(store, root)
    mark_dirty(store, tenant, source)
    doc_locked, queue_locked, queue_attempted = threading.Event(), threading.Event(), threading.Event()
    errors = []

    def ingest():
        try:
            with store.connect() as connection:
                connection.execute('SET LOCAL statement_timeout=5000')
                revision(connection, tenant, source)
                doc_locked.set()
                assert queue_locked.wait(5)
                queue_attempted.set()
                mark_logical_evidence_dirty(connection, tenant_id=tenant, source_id=source,
                                            native_ids=['event-0000'], reason='ingest')
        except BaseException as error:
            errors.append(error)

    original = store._execute_bounded

    def query(connection, sql, values, deadline):
        value = original(connection, sql, values, deadline)
        if 'SELECT generation FROM canonical_evidence_document_queue' in sql:
            queue_locked.set()
            assert queue_attempted.wait(5)
        return value

    worker = threading.Thread(target=ingest)
    worker.start()
    assert doc_locked.wait(5)
    started = time.monotonic()
    with patch.object(store, '_execute_bounded', side_effect=query):
        try:
            invoke(store, tenant, source, archive)
        except LocatorPlanError as error:
            assert str(error) == 'locator_plan_lock_busy', error
        else:
            raise AssertionError('apply waited for the ingest document lock')
    worker.join(5)
    assert not worker.is_alive() and not errors and time.monotonic() - started < 3, errors
    with store.connect() as connection:
        documents = connection.execute("SELECT revision,is_current,body_record_ordinal FROM canonical_documents WHERE tenant_id=%s AND source_id=%s AND native_id='event-0000' ORDER BY revision", (tenant, source)).fetchall()
        assert documents == [dict(revision=1,is_current=False,body_record_ordinal=None),
                             dict(revision=2,is_current=True,body_record_ordinal=None)]
        assert connection.execute('SELECT attempts FROM canonical_evidence_document_queue WHERE tenant_id=%s AND source_id=%s', (tenant, source)).fetchone()['attempts'] == 0


def apply_owns_document_first(store, root):
    tenant, source, archive, projector = prepared(store, root)
    original = planner._snapshot_on_connection
    calls, errors, pid = 0, [], []
    started = threading.Event()

    def ingest():
        try:
            with store.connect() as connection:
                connection.execute('SET LOCAL statement_timeout=5000')
                pid.append(connection.info.backend_pid)
                started.set()
                revision(connection, tenant, source)
                mark_logical_evidence_dirty(connection, tenant_id=tenant, source_id=source,
                                            native_ids=['event-0000'], reason='ingest')
        except BaseException as error:
            errors.append(error)

    worker = threading.Thread(target=ingest)

    def snapshot(*args):
        nonlocal calls
        calls += 1
        value = original(*args)
        if calls == 3:
            worker.start()
            assert started.wait(5)
            until = time.monotonic() + 3
            while time.monotonic() < until:
                with store.connect() as connection:
                    waiting = connection.execute('SELECT wait_event_type FROM pg_stat_activity WHERE pid=%s', (pid[0],)).fetchone()
                if waiting and waiting['wait_event_type'] == 'Lock':
                    break
                time.sleep(0.01)
            else:
                raise AssertionError('concurrent revision did not wait on the proven document')
        return value

    with patch.object(planner, '_snapshot_on_connection', side_effect=snapshot):
        assert invoke(store, tenant, source, archive)['applied_documents'] == 2
    worker.join(5)
    assert not worker.is_alive() and not errors, errors
    with store.connect() as connection:
        documents = connection.execute("SELECT revision,is_current,body_record_ordinal FROM canonical_documents WHERE tenant_id=%s AND source_id=%s AND native_id='event-0000' ORDER BY revision", (tenant, source)).fetchall()
        assert documents == [dict(revision=1,is_current=False,body_record_ordinal=0),
                             dict(revision=2,is_current=True,body_record_ordinal=None)]


def rollback_and_busy_catalog(store, root):
    tenant, source, archive, projector = prepared(store, root)
    before = state(store, tenant, source)
    locked, release = threading.Event(), threading.Event()
    errors = []

    def hold_catalog():
        try:
            with store.connect() as blocker:
                blocker.execute('SELECT 1 FROM canonical_evidence_documents WHERE tenant_id=%s AND source_id=%s FOR UPDATE', (tenant, source))
                locked.set()
                assert release.wait(5)
        except BaseException as error:
            errors.append(error)

    worker = threading.Thread(target=hold_catalog)
    worker.start()
    assert locked.wait(5)
    started = time.monotonic()
    try:
        try:
            invoke(store, tenant, source, archive)
        except LocatorPlanError as error:
            assert str(error) == 'locator_plan_lock_busy', error
        else:
            raise AssertionError('busy catalog was accepted')
        assert time.monotonic() - started < 2
    finally:
        release.set()
        worker.join(5)
    assert not worker.is_alive() and not errors, errors
    original = store._execute_bounded

    def fail_after_update(connection, sql, values, deadline):
        value = original(connection, sql, values, deadline)
        if sql.startswith('UPDATE canonical_documents document'):
            assert value.rowcount == 2
            raise LocatorPlanError('synthetic_after_update_failure')
        return value

    with patch.object(store, '_execute_bounded', side_effect=fail_after_update):
        try:
            invoke(store, tenant, source, archive)
        except LocatorPlanError as error:
            assert str(error) == 'synthetic_after_update_failure', error
        else:
            raise AssertionError('injected failure disappeared')
    assert state(store, tenant, source) == before
    assert invoke(store, tenant, source, archive)['applied_documents'] == 2


def main():
    admin = os.environ['RECALL_DATABASE_URL']
    database = 'recall_locator_apply_' + uuid.uuid4().hex
    settings = conninfo_to_dict(admin); settings['dbname'] = database
    with psycopg.connect(admin, autocommit=True) as connection:
        connection.execute(f'CREATE DATABASE "{database}"')
    store = TrackedStore(make_conninfo(**settings))
    try:
        store.migrate()
        with tempfile.TemporaryDirectory(prefix='recall-locator-apply-') as directory:
            root = Path(directory)
            basic(store, root)
            append_race(store, root, after_fence=False)
            append_race(store, root, after_fence=True)
            ingest_owns_document_first(store, root)
            apply_owns_document_first(store, root)
            rollback_and_busy_catalog(store, root)
        print(json.dumps(dict(status='pass', null_only_apply=True, append_before_and_after_fence=True,
                              revision_both_lock_orders=True, whole_parent_rollback=True)))
    finally:
        store.close()
        with psycopg.connect(admin, autocommit=True) as connection:
            connection.execute(f'DROP DATABASE "{database}" WITH (FORCE)')


if __name__ == '__main__':
    main()
