#!/usr/bin/env python3
"""Disposable PostgreSQL proof of exact, tenant-scoped archived receipt reads."""
import json
import os
import sys
import tempfile
import uuid
from contextlib import contextmanager
from pathlib import Path

import psycopg
from psycopg.conninfo import conninfo_to_dict, make_conninfo

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from e2e_logical_evidence_projection import insert_record, insert_source
from recall_server.archive import FilesystemArchiveStore
from recall_server.chunk_bodies import ChunkBodyError
from recall_server.db import BrainStore
from recall_server.logical_evidence import LogicalEvidenceProjectionStore
from recall_server.logical_evidence_projection import CanonicalLogicalEvidenceProjector


def main():
    admin_dsn = os.environ['RECALL_DATABASE_URL']
    database = 'recall_archived_resolve_' + uuid.uuid4().hex
    settings = conninfo_to_dict(admin_dsn)
    settings['dbname'] = database
    with psycopg.connect(admin_dsn, autocommit=True) as admin:
        admin.execute(f'CREATE DATABASE "{database}"')
    store = BrainStore(make_conninfo(**settings))
    try:
        store.migrate()
        tenant, other, principal, source = 'tenant:resolve', 'tenant:other', 'principal:resolve', 'source:shared'
        with tempfile.TemporaryDirectory(prefix='recall-resolve-') as temporary:
            archive = FilesystemArchiveStore(Path(temporary) / 'archive', namespace_key=b'a' * 32)
            with store.connect() as conn:
                for owner in (other, tenant):
                    insert_source(conn, owner, principal, source)
                    receipt = insert_record(conn, tenant=owner, source=source, parent='session',
                                            native='native', text='historical α' if owner == tenant else 'other tenant secret',
                                            role='assistant', byte_start=0)
                conn.execute('UPDATE canonical_documents SET is_current=false WHERE tenant_id=%s', (tenant,))
                new_receipt = insert_record(conn, tenant=tenant, source=source, parent='session', native='new',
                                            text='current β 🧠\n' * 5000, role='assistant', byte_start=10)
                conn.execute("UPDATE canonical_events SET native_id='native',revision=2 WHERE tenant_id=%s AND native_id='new'", (tenant,))
                conn.execute("UPDATE canonical_documents SET native_id='native',revision=2 WHERE tenant_id=%s AND native_id='new'", (tenant,))
                conn.execute("UPDATE canonical_chunks SET receipt=replace(receipt,'/new?rev=1','/native?rev=2') WHERE tenant_id=%s AND receipt LIKE %s", (tenant, '%/new?rev=1%'))
                new_receipt = new_receipt.replace('/new?rev=1', '/native?rev=2')
            scope = dict(tenant_id=tenant, authorized_sources=(source,))
            historical = store.resolve(receipt, **scope)
            current = store.resolve(new_receipt, **scope)
            assert historical['items'][0]['text_redacted'] == 'historical α'
            assert len(current['items']) > 2
            assert set(current['items'][0]) == {'ordinal', 'occurred_at', 'role', 'surface', 'text_redacted', 'receipt'}
            assert store.resolve(new_receipt, chunk_body_archive=archive, **scope) == current, 'unprojected fallback'
            projector = CanonicalLogicalEvidenceProjector(store, LogicalEvidenceProjectionStore(archive), bound_tenant_id=tenant)
            projector.seed_backfill(tenant_id=tenant)
            report = projector.project_pending(tenant_id=tenant, batch_size=10, max_batches=1, upload_concurrency=1)
            assert report['documents'] == 1, report
            with store.connect() as conn:
                conn.execute('''UPDATE canonical_chunks chunk SET text_redacted=''
                                FROM canonical_documents document
                                WHERE chunk.tenant_id=document.tenant_id AND chunk.source_id=document.source_id
                                  AND chunk.document_id=document.document_id AND document.tenant_id=%s
                                  AND document.is_current''', (tenant,))
            connected = [0]
            original_connect = store.connect
            @contextmanager
            def tracked_connect():
                with original_connect() as conn:
                    connected[0] += 1
                    try:
                        yield conn
                    finally:
                        connected[0] -= 1
            store.connect = tracked_connect
            original_read = archive.read_raw
            reads = []
            def tracked_read(reference):
                assert connected[0] == 0, 'archive read held a pooled database connection'
                reads.append(reference)
                return original_read(reference)
            archive.read_raw = tracked_read
            assert store.resolve(new_receipt, chunk_body_archive=archive, **scope) == current
            reads.clear()
            assert store.resolve(receipt, chunk_body_archive=archive, **scope) == historical
            assert reads == [], 'historical revision must retain inline body'
            assert store.resolve(receipt, tenant_id=other, authorized_sources=(source,))['items'][0]['text_redacted'] == 'other tenant secret'
            assert store.resolve(new_receipt, tenant_id=other, authorized_sources=(source,), chunk_body_archive=archive) is None
            assert store.resolve(new_receipt, tenant_id=tenant, authorized_sources=(), chunk_body_archive=archive) is None
            assert reads == []
            def broken(_):
                raise OSError('private object location')
            archive.read_raw = broken
            try:
                store.resolve(new_receipt, chunk_body_archive=archive, **scope)
            except ChunkBodyError:
                pass
            else:
                raise AssertionError('corrupt archive silently used thinned PG body')
            archive.read_raw = tracked_read
            with store.connect() as conn:
                conn.execute('''INSERT INTO canonical_events(
                     tenant_id,source_id,event_id,native_id,native_parent_id,artifact_id,job_id,
                     kind,content_sha256,revision,occurred_at,observed_at,is_tombstone,canonical_redacted)
                     SELECT tenant_id,source_id,%s,native_id,native_parent_id,artifact_id,job_id,
                            kind,%s,3,occurred_at,observed_at,true,'{}'::jsonb
                       FROM canonical_events WHERE tenant_id=%s AND revision=2''',
                             ('evt_' + uuid.uuid4().hex, 'f' * 64, tenant))
            reads.clear()
            assert store.resolve(new_receipt, chunk_body_archive=archive, **scope) is None
            assert store.resolve(receipt, chunk_body_archive=archive, **scope) is None
            assert not reads
        print(json.dumps({'status': 'pass', 'current_archive_exact': True, 'historical_inline_exact': True,
                          'tenant_collision_and_empty_grants_denied': True, 'tombstones_denied': True,
                          'no_pooled_connection_during_archive_read': True, 'corrupt_archive_refused': True}))
    finally:
        store.close()
        with psycopg.connect(admin_dsn, autocommit=True) as admin:
            admin.execute(f'DROP DATABASE "{database}"')


if __name__ == '__main__':
    main()
