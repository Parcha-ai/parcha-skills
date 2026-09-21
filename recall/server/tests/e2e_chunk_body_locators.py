#!/usr/bin/env python3
"""Exact archived reads survive parent growth and atomic locator publication."""
from __future__ import annotations

import json
import os
import sys
import tempfile
import threading
import time
import uuid
from pathlib import Path
from unittest.mock import patch

import psycopg
from psycopg.conninfo import conninfo_to_dict, make_conninfo

SERVER = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SERVER))
from e2e_archive_reprojection import fixture, mark_dirty  # noqa: E402
from e2e_logical_evidence_projection import insert_record, insert_source  # noqa: E402
from e2e_logical_source_integrity import TrackedStore  # noqa: E402
from recall_server.archive import FilesystemArchiveStore  # noqa: E402
from recall_server.canonical_retrieval import BoundCanonicalRetrieval  # noqa: E402
from recall_server.chunk_bodies import ChunkBodyError, read_archived_chunks  # noqa: E402
from recall_server.logical_evidence import LogicalEvidenceProjectionStore  # noqa: E402
from recall_server.logical_evidence_projection import CanonicalLogicalEvidenceProjector, mark_logical_evidence_dirty  # noqa: E402


def rows(store, tenant, source):
    with store.connect() as connection:
        return connection.execute("""SELECT document_id,native_id,body_record_ordinal,
            body_record_count,xmin::text AS xmin FROM canonical_documents
            WHERE tenant_id=%s AND source_id=%s AND is_current ORDER BY native_id""", (tenant, source)).fetchall()


def hydrate(store, archive, tenant, source):
    documents = rows(store, tenant, source)
    return read_archived_chunks(store, archive, tenant_id=tenant, source_ids=(source,),
                                document_ids=tuple(r['document_id'] for r in documents))


def unavailable(callback):
    try:
        callback()
    except ChunkBodyError as error:
        assert str(error) == 'archived_chunk_body_unavailable'
    else:
        raise AssertionError('unverified locator returned a body')


def project(projector, tenant):
    report = projector.project_pending(tenant_id=tenant, batch_size=2, max_batches=1, upload_concurrency=1)
    assert report['failed'] == 0, report
    return report


def small_cases(store, root):
    tenant, source, archive, projection, projector, texts = fixture(store, root)
    old = rows(store, tenant, source)
    assert [(r['body_record_ordinal'], r['body_record_count']) for r in old] == [(0, 1), (1, 1), (2, 1)]
    with store.connect() as connection:
        connection.execute("UPDATE canonical_chunks SET text_redacted='' WHERE tenant_id=%s AND source_id=%s", (tenant, source))
        insert_record(connection, tenant=tenant, source=source, parent='session', native='new',
                      text='new event', role='assistant', byte_start=99)
    mark_dirty(store, tenant, source)
    assert project(projector, tenant)['documents'] == 1
    after = {r['document_id']: r for r in rows(store, tenant, source)}
    assert all(after[r['document_id']] == r for r in old), 'append rewrote unchanged locator prefix'
    recovered = hydrate(store, archive, tenant, source)
    assert all(recovered[(source, r['document_id'])][0]['text_redacted'] == texts[r['native_id']] for r in old)

    # Unchanged-object repair backfills NULL positions without inventing a new
    # logical revision; a subsequent no-op repair must not rewrite any row.
    with store.connect() as connection:
        revision = connection.execute('SELECT revision FROM canonical_evidence_documents WHERE tenant_id=%s AND source_id=%s', (tenant, source)).fetchone()['revision']
        connection.execute('UPDATE canonical_documents SET body_record_ordinal=NULL,body_record_count=NULL WHERE document_id=%s', (old[0]['document_id'],))
    mark_dirty(store, tenant, source)
    assert project(projector, tenant)['repaired'] == 1
    with store.connect() as connection:
        assert connection.execute('SELECT revision FROM canonical_evidence_documents WHERE tenant_id=%s AND source_id=%s', (tenant, source)).fetchone()['revision'] == revision
    repaired = rows(store, tenant, source)
    mark_dirty(store, tenant, source)
    assert project(projector, tenant)['repaired'] == 1
    assert rows(store, tenant, source) == repaired
    assert hydrate(store, archive, tenant, source)

    # New canonical revision has NULL positions until its own publication.
    with store.connect() as connection:
        connection.execute("UPDATE canonical_documents SET is_current=false WHERE document_id=%s", (old[0]['document_id'],))
        temporary = insert_record(connection, tenant=tenant, source=source, parent='session', native='temporary',
                                  text='replacement body', role='assistant', byte_start=0)
        connection.execute("UPDATE canonical_events SET native_id='event-0000',revision=2 WHERE tenant_id=%s AND source_id=%s AND native_id='temporary'", (tenant, source))
        connection.execute("UPDATE canonical_documents SET native_id='event-0000',revision=2 WHERE tenant_id=%s AND source_id=%s AND native_id='temporary'", (tenant, source))
        receipt = f'recall://{source}/event-0000?rev=2#item=0'
        connection.execute('UPDATE canonical_chunks SET receipt=%s WHERE receipt=%s', (receipt, temporary))
        connection.execute("UPDATE canonical_documents SET is_current=false,deleted_at=now() WHERE document_id=%s", (old[1]['document_id'],))
    mark_dirty(store, tenant, source)
    reader = BoundCanonicalRetrieval(store, tenant_id=tenant, principal_id='principal:reprojection',
                                     authorized_sources=(source,), chunk_body_archive=archive)
    assert reader.show(f'recall://{source}/event-0000?rev=1#item=0') is None
    assert reader.show(receipt)['chunks'][0]['text'] == 'replacement body'
    assert project(projector, tenant)['documents'] == 1
    assert reader.show(receipt)['chunks'][0]['text'] == 'replacement body'
    assert (source, old[1]['document_id']) not in hydrate(store, archive, tenant, source)

    # Publishing after catalog capture changes the locator/manifest fence.
    with store.connect() as connection:
        insert_record(connection, tenant=tenant, source=source, parent='session', native='race-append',
                      text='race append', role='user', byte_start=999)
    mark_dirty(store, tenant, source)
    archive.on_read = lambda: project(projector, tenant)
    unavailable(lambda: reader.show(receipt))
    assert reader.show(receipt)['chunks'][0]['text'] == 'replacement body'

    # An incorrect or unavailable locator is an error, never inline fallback.
    current = rows(store, tenant, source)[0]
    with store.connect() as connection:
        connection.execute('UPDATE canonical_documents SET body_record_ordinal=99999 WHERE document_id=%s', (current['document_id'],))
    unavailable(lambda: hydrate(store, archive, tenant, source))
    with store.connect() as connection:
        connection.execute('UPDATE canonical_documents SET body_record_ordinal=%s WHERE document_id=%s', (current['body_record_ordinal'], current['document_id']))
        part = connection.execute('SELECT * FROM canonical_evidence_document_parts WHERE tenant_id=%s AND source_id=%s ORDER BY part_ordinal LIMIT 1', (tenant, source)).fetchone()
    path = archive.delegate.root / part['object_key'] / 'data'
    payload = path.read_bytes()
    path.write_bytes(b'broken')
    unavailable(lambda: hydrate(store, archive, tenant, source))
    path.unlink()
    unavailable(lambda: hydrate(store, archive, tenant, source))
    path.write_bytes(payload)
    path.chmod(0o600)


def lock_contention(store, root):
    tenant, source, archive, projection, projector, texts = fixture(store, root)
    prior = rows(store, tenant, source)
    with store.connect() as connection:
        connection.execute("UPDATE canonical_events SET source_ordinal=CASE WHEN native_id='event-0000' THEN 999 ELSE 0 END WHERE tenant_id=%s AND source_id=%s", (tenant, source))
    mark_dirty(store, tenant, source)
    candidate = projector._pending(tenant_id=tenant, limit=1)[0]
    upload, = projector._prepare_batch_and_upload((candidate,))
    doc_locked, queue_locked, ingest_waiting = threading.Event(), threading.Event(), threading.Event()
    errors, statuses = [], []
    original = projector._publish_body_locators

    def publishing(connection, selected, locators):
        queue_locked.set()
        assert ingest_waiting.wait(5)
        return original(connection, selected, locators)

    def ingest():
        try:
            with store.connect() as connection:
                connection.execute('SET LOCAL statement_timeout=5000')
                connection.execute('UPDATE canonical_documents SET text_redacted=text_redacted WHERE document_id=%s', (prior[0]['document_id'],))
                doc_locked.set()
                assert queue_locked.wait(5)
                ingest_waiting.set()
                mark_logical_evidence_dirty(connection, tenant_id=tenant, source_id=source,
                                            native_ids=['event-0000'], reason='ingest')
        except BaseException as error:
            errors.append(error)

    thread = threading.Thread(target=ingest)
    thread.start()
    assert doc_locked.wait(5)
    started = time.monotonic()
    with patch.object(projector, '_publish_body_locators', side_effect=publishing):
        statuses.append(projector._commit_upload(candidate, upload))
    thread.join(5)
    assert not thread.is_alive() and not errors, errors
    assert statuses == ['stale'] and time.monotonic() - started < 3, statuses
    after = rows(store, tenant, source)
    assert [(r['body_record_ordinal'], r['body_record_count']) for r in after] == [(r['body_record_ordinal'], r['body_record_count']) for r in prior]
    with store.connect() as connection:
        queued = connection.execute('SELECT attempts,next_attempt_at,generation FROM canonical_evidence_document_queue WHERE tenant_id=%s AND source_id=%s', (tenant, source)).fetchone()
        assert queued['attempts'] == 0 and queued['next_attempt_at'] is None and queued['generation'] > candidate.generation
        cleanup = connection.execute('SELECT artifact_id FROM canonical_evidence_cleanup_queue WHERE tenant_id=%s AND source_id=%s', (tenant, source)).fetchall()
        assert {reference['artifact_id'] for reference in upload.cleanup_references} <= {row['artifact_id'] for row in cleanup}
    recovered = hydrate(store, archive, tenant, source)
    assert all(recovered[(source, r['document_id'])][0]['text_redacted'] == texts[r['native_id']] for r in prior)
    assert project(projector, tenant)['documents'] == 1
    recovered = hydrate(store, archive, tenant, source)
    assert all(recovered[(source, r['document_id'])][0]['text_redacted'] == texts[r['native_id']] for r in prior)


def growth(store, root):
    nonce = uuid.uuid4().hex
    tenant, source = 'tenant:growth:' + nonce, 'codex:growth:' + nonce
    archive = FilesystemArchiveStore(root / nonce, namespace_key=b'g' * 32)
    projector = CanonicalLogicalEvidenceProjector(store, LogicalEvidenceProjectionStore(archive), bound_tenant_id=tenant)
    with store.connect() as connection:
        insert_source(connection, tenant, 'principal:growth', source)
        for parent in ('left', 'right'):
            insert_record(connection, tenant=tenant, source=source, parent=parent, native=parent + '-old',
                          text=parent + ' exact old body', role='assistant', byte_start=0)
        connection.execute('UPDATE canonical_events SET source_ordinal=0 WHERE tenant_id=%s AND source_id=%s', (tenant, source))
    projector.seed_backfill(tenant_id=tenant)
    assert project(projector, tenant)['documents'] == 2
    old = rows(store, tenant, source)
    with store.connect() as connection:
        connection.execute("UPDATE canonical_chunks SET text_redacted='' WHERE tenant_id=%s AND source_id=%s", (tenant, source))
    body = 'growth body ' * 600_000  # 7.2 MB, within the canonical event bound.
    for round_number, parents in enumerate((('left', 'right'), ('left',))):
        with store.connect() as connection:
            for parent in parents:
                for index in range(5):
                    native = f'{parent}-{round_number}-{index}'
                    insert_record(connection, tenant=tenant, source=source, parent=parent, native=native,
                                  text=body, role='assistant', byte_start=10 + round_number * 50 + index)
                    mark_logical_evidence_dirty(connection, tenant_id=tenant, source_id=source, native_ids=[native], reason='ingest')
            connection.execute("UPDATE canonical_events SET source_ordinal=(canonical_redacted #>> '{provenance,byte_start}')::bigint WHERE tenant_id=%s AND source_id=%s AND source_ordinal IS NULL", (tenant, source))
        assert project(projector, tenant)['documents'] == len(parents)
        with store.connect() as connection:
            sizes = connection.execute('SELECT logical_document_id,sum(size_bytes) AS bytes FROM canonical_evidence_document_parts WHERE tenant_id=%s AND source_id=%s GROUP BY logical_document_id', (tenant, source)).fetchall()
            assert sum(row['bytes'] for row in sizes) > 64 * 1024 * 1024
            if round_number:
                assert max(row['bytes'] for row in sizes) > 64 * 1024 * 1024
            assert connection.execute("SELECT count(*) AS n FROM canonical_chunks WHERE tenant_id=%s AND source_id=%s AND text_redacted=''", (tenant, source)).fetchone()['n'] == 2
        recovered = read_archived_chunks(store, archive, tenant_id=tenant, source_ids=(source,), document_ids=tuple(r['document_id'] for r in old))
        assert [recovered[(source, r['document_id'])][0]['text_redacted'] for r in old] == [r['native_id'].removesuffix('-old') + ' exact old body' for r in old]
        after = {r['document_id']: r for r in rows(store, tenant, source)}
        assert all(after[r['document_id']] == r for r in old), 'growth rewrote stable prefix positions'


def main():
    admin = os.environ['RECALL_DATABASE_URL']
    database = 'recall_body_locators_' + uuid.uuid4().hex
    settings = conninfo_to_dict(admin)
    settings['dbname'] = database
    with psycopg.connect(admin, autocommit=True) as connection:
        connection.execute(f'CREATE DATABASE "{database}"')
    store = TrackedStore(make_conninfo(**settings))
    try:
        report = store.migrate()
        assert 68 in report['applied'] and 67 in report['deferred'] and report['postgres_vector_plane'] == 'present'
        with tempfile.TemporaryDirectory(prefix='recall-locators-') as directory:
            root = Path(directory)
            small_cases(store, root)
            lock_contention(store, root)
            growth(store, root)
        print(json.dumps({'status': 'pass', 'parent_and_cumulative_growth_over_64mib': True,
                          'unchanged_prefix_xmin_preserved': True, 'nowait_ingest_race_retried': True,
                          'repair_revision_and_corruption_fences': True}, sort_keys=True))
    finally:
        store.close()
        with psycopg.connect(admin, autocommit=True) as connection:
            connection.execute(f'DROP DATABASE "{database}" WITH (FORCE)')


if __name__ == '__main__':
    main()
