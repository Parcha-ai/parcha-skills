#!/usr/bin/env python3
"""Disposable PG proof: existing-part planning never publishes or changes bodies."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile
import uuid

import psycopg
from psycopg.conninfo import conninfo_to_dict, make_conninfo

SERVER = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SERVER))
from e2e_archive_reprojection import fixture, mark_dirty
from e2e_logical_evidence_projection import insert_record
from e2e_logical_source_integrity import TrackedStore
from recall_server.locator_backfill_plan import LocatorPlanError, PlanLimits, plan_parent, select_parents


def check_failed(code, call):
    try:
        call()
    except LocatorPlanError as error:
        assert str(error) == code, str(error)
    else:
        raise AssertionError('invalid dry-run returned a plan')


def state(store, tenant, source):
    with store.connect() as connection:
        return connection.execute('''SELECT document.document_id, document.is_current,
            document.body_record_ordinal,document.body_record_count,document.xmin::text AS xmin,
            min(document.text_redacted) AS document_text,
            array_agg(chunk.text_redacted ORDER BY chunk.ordinal) AS texts
            FROM canonical_documents document JOIN canonical_chunks chunk USING(tenant_id,source_id,document_id)
            WHERE document.tenant_id=%s AND document.source_id=%s
            GROUP BY document.document_id,document.is_current,document.body_record_ordinal,
                     document.body_record_count,document.xmin::text ORDER BY document.document_id''', (tenant, source)).fetchall()


def cases(store, root):
    tenant, source, archive, projection, projector, texts = fixture(store, root)
    with store.connect() as connection:
        connection.execute('UPDATE canonical_documents SET body_record_ordinal=NULL,body_record_count=NULL WHERE tenant_id=%s AND source_id=%s', (tenant, source))
    before, uploads = state(store, tenant, source), archive.uploads

    def plan(**options):
        return plan_parent(store, archive, tenant_id=tenant, source_id=source, native_parent_id='session', **options)

    result = plan()
    assert len(result['changes']) == result['eligible_documents'] == 3
    assert result['archive_gets'] == len(archive.reads) and set(archive.reads.values()) == {1}
    assert state(store, tenant, source) == before and archive.uploads == uploads
    assert not any(text in json.dumps(result) for text in texts.values())
    parents, more = select_parents(store, tenant_id=tenant, source_id=source, limit=1)
    assert parents == [dict(source_id=source, native_parent_id='session')] and not more
    assert select_parents(store, tenant_id=tenant, after=(source, 'session'), limit=1) == ([], False)
    check_failed('locator_plan_manifest_missing', lambda: plan_parent(store, archive,
        tenant_id='tenant:other', source_id=source, native_parent_id='session'))
    reads = sum(archive.reads.values())
    check_failed('locator_plan_metadata_budget_exceeded', lambda: plan(limits=PlanLimits(max_documents=2)))
    check_failed('locator_plan_archive_budget_exceeded', lambda: plan(limits=PlanLimits(max_bytes=1)))
    assert sum(archive.reads.values()) == reads

    with store.connect() as connection:
        part = connection.execute('SELECT object_key FROM canonical_evidence_document_parts WHERE tenant_id=%s AND source_id=%s ORDER BY part_ordinal LIMIT 1', (tenant, source)).fetchone()
    path = archive.delegate.root / part['object_key'] / 'data'
    payload = path.read_bytes()
    path.write_bytes(b'corrupt')
    check_failed('locator_plan_evidence_unavailable', plan)
    path.unlink()
    check_failed('locator_plan_evidence_unavailable', plan)
    path.write_bytes(payload)
    path.chmod(0o600)

    # Appended B is absent from the current parts; unchanged A positions remain provable.
    with store.connect() as connection:
        insert_record(connection, tenant=tenant, source=source, parent='session', native='pending-new',
                      text='new source body', role='assistant', byte_start=100)
    mark_dirty(store, tenant, source)
    result = plan()
    assert len(result['changes']) == 3 and result['excluded']['pending_new_document'] == 1

    # A new revision excludes its historical predecessor from current candidates.
    with store.connect() as connection:
        connection.execute("UPDATE canonical_documents SET is_current=false WHERE tenant_id=%s AND source_id=%s AND native_id='event-0000'", (tenant, source))
        old = connection.execute("SELECT document_id FROM canonical_documents WHERE tenant_id=%s AND source_id=%s AND native_id='event-0000'", (tenant, source)).fetchone()['document_id']
        receipt = insert_record(connection, tenant=tenant, source=source, parent='session', native='replacement',
                               text='revision two', role='assistant', byte_start=0)
        connection.execute("UPDATE canonical_events SET native_id='event-0000',revision=2 WHERE tenant_id=%s AND source_id=%s AND native_id='replacement'", (tenant, source))
        connection.execute("UPDATE canonical_documents SET native_id='event-0000',revision=2 WHERE tenant_id=%s AND source_id=%s AND native_id='replacement'", (tenant, source))
        connection.execute('UPDATE canonical_chunks SET receipt=%s WHERE receipt=%s', (f'recall://{source}/event-0000?rev=2#item=0', receipt))
    mark_dirty(store, tenant, source)
    result = plan()
    assert result['excluded']['pending_revision'] == 1 and len(result['changes']) == 2
    assert old not in [change['document_id'] for change in result['changes']]
    with store.connect() as connection:
        connection.execute("UPDATE canonical_documents SET is_current=false,deleted_at=now() WHERE tenant_id=%s AND source_id=%s AND native_id='event-0001'", (tenant, source))
    mark_dirty(store, tenant, source)
    assert plan()['eligible_documents'] == 1

    def append_during_read():
        with store.connect() as connection:
            insert_record(connection, tenant=tenant, source=source, parent='session', native='racing-new',
                          text='racing source body', role='assistant', byte_start=200)
        mark_dirty(store, tenant, source)

    archive.on_read = append_during_read
    check_failed('locator_plan_catalog_changed', plan)
    assert plan()['excluded']['pending_new_document'] == 2
    assert archive.uploads == uploads


def publication_race(store, root):
    tenant, source, archive, projection, projector, texts = fixture(store, root)
    with store.connect() as connection:
        connection.execute('UPDATE canonical_documents SET body_record_ordinal=NULL,body_record_count=NULL WHERE tenant_id=%s AND source_id=%s', (tenant, source))
    mark_dirty(store, tenant, source)
    archive.on_read = lambda: projector.project_pending(tenant_id=tenant, batch_size=1, max_batches=1)
    call = lambda: plan_parent(store, archive, tenant_id=tenant, source_id=source, native_parent_id='session')
    check_failed('locator_plan_catalog_changed', call)
    result = call()
    assert result['changes'] == [] and result['unchanged_locators'] == 3


def unsupported_and_mismatch(store, root):
    tenant, source, archive, projection, projector, texts = fixture(store, root)
    with store.connect() as connection:
        row = connection.execute("SELECT * FROM canonical_chunks WHERE tenant_id=%s AND source_id=%s AND receipt LIKE '%%/event-0000?%%'", (tenant, source)).fetchone()
        text = row['text_redacted']
        first, last = text[:5], text[5:]
        connection.execute('UPDATE canonical_chunks SET text_redacted=%s,text_sha256=%s WHERE chunk_id=%s',
                           (first, hashlib.sha256(first.encode()).hexdigest(), row['chunk_id']))
        connection.execute('''INSERT INTO canonical_chunks(tenant_id,source_id,chunk_id,document_id,ordinal,
            text_redacted,text_sha256,receipt) VALUES(%s,%s,%s,%s,1,%s,%s,%s)''',
            (tenant, source, 'chk_' + uuid.uuid4().hex, row['document_id'], last,
             hashlib.sha256(last.encode()).hexdigest(), row['receipt'].replace('#item=0', '#item=1')))
    mark_dirty(store, tenant, source)
    report = projector.project_pending(tenant_id=tenant, batch_size=1, max_batches=1)
    assert report['failed'] == 0, report
    with store.connect() as connection:
        connection.execute('UPDATE canonical_documents SET body_record_ordinal=NULL,body_record_count=NULL WHERE tenant_id=%s AND source_id=%s', (tenant, source))
        connection.execute("UPDATE raw_artifacts SET media_type='application/vnd.recall.oversized-record+gzip' WHERE artifact_id=(SELECT artifact_id FROM canonical_events WHERE tenant_id=%s AND source_id=%s AND native_id='event-0001')", (tenant, source))

    def plan():
        return plan_parent(store, archive, tenant_id=tenant, source_id=source, native_parent_id='session')

    result = plan()
    assert result['excluded'] == {'historical_chunk_boundaries': 1, 'oversized': 1}, result
    assert len(result['changes']) == 1
    with store.connect() as connection:
        connection.execute("UPDATE canonical_documents SET text_sha256=%s WHERE tenant_id=%s AND source_id=%s AND native_id='event-0002'", ('0' * 64, tenant, source))
    check_failed('locator_plan_evidence_unavailable', plan)


def main():
    admin = os.environ['RECALL_DATABASE_URL']
    database = 'recall_locator_plan_' + uuid.uuid4().hex
    settings = conninfo_to_dict(admin)
    settings['dbname'] = database
    with psycopg.connect(admin, autocommit=True) as connection:
        connection.execute(f'CREATE DATABASE "{database}"')
    store = TrackedStore(make_conninfo(**settings))
    try:
        store.migrate()
        with tempfile.TemporaryDirectory(prefix='recall-locator-plan-') as directory:
            cases(store, Path(directory))
            publication_race(store, Path(directory))
            unsupported_and_mismatch(store, Path(directory))
        print(json.dumps(dict(status='pass', read_only_plan=True, archive_reads_once=True,
                              catalog_races_rejected=True, revisions_and_exclusions=True)))
    finally:
        store.close()
        with psycopg.connect(admin, autocommit=True) as connection:
            connection.execute(f'DROP DATABASE "{database}" WITH (FORCE)')


if __name__ == '__main__':
    main()
