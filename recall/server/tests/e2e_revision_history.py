#!/usr/bin/env python3
"""Disposable PostgreSQL proof of exact history after current-body retirement."""
import copy
import hashlib
import json
import os
import sys
import tempfile
import uuid
from contextlib import contextmanager
from pathlib import Path
from unittest import mock

import psycopg
from psycopg.conninfo import conninfo_to_dict, make_conninfo

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from recall_server.archive import ArchiveNotFound, FilesystemArchiveStore
from recall_server.canonical import CanonicalArchiveGateway, CanonicalLifecycleError, CanonicalPlane
from recall_server.db import BrainStore
from recall_server.legacy_plane import LegacyIngestBridge
from recall_server.logical_evidence import LogicalEvidenceProjectionStore
from recall_server.logical_evidence_projection import CanonicalLogicalEvidenceProjector
from recall_server.projectors import canonical_json


def main():
    admin_dsn = os.environ['RECALL_DATABASE_URL']
    database = 'recall_revision_history_' + uuid.uuid4().hex
    settings = conninfo_to_dict(admin_dsn)
    settings['dbname'] = database
    with psycopg.connect(admin_dsn, autocommit=True) as admin:
        admin.execute(f'CREATE DATABASE "{database}"')
    store = BrainStore(make_conninfo(**settings))
    try:
        store.migrate()
        tenant, principal, source = 'tenant:history', 'principal:history', 'source:history'
        with tempfile.TemporaryDirectory(prefix='recall-history-') as temporary:
            archive = FilesystemArchiveStore(Path(temporary) / 'archive', namespace_key=b'h' * 32)
            projector = CanonicalLogicalEvidenceProjector(store, LogicalEvidenceProjectionStore(archive),
                                                         bound_tenant_id=tenant, raw_archive=archive)
            plane = CanonicalPlane(store, archive, evidence_projector=projector, chunk_body_archive=archive)
            gateway = CanonicalArchiveGateway(store, archive, tenant_id=tenant, principal_id=principal)
            bridge = LegacyIngestBridge(store, plane, archive)
            active = [0]
            reads = []
            original_connect, original_read = store.connect, archive.read_raw
            @contextmanager
            def tracked_connect():
                with original_connect() as connection:
                    active[0] += 1
                    try:
                        yield connection
                    finally:
                        active[0] -= 1
            def tracked_read(reference):
                assert not active[0], 'archive IO held a pooled database connection'
                reads.append(reference)
                return original_read(reference)
            store.connect, archive.read_raw = tracked_connect, tracked_read

            def event(native, revision, *, tombstone=False):
                content = {'target_native_id': native} if tombstone else {'text': (f'{native} version {revision}: α 🧠\n' * 2000), 'role': 'user'}
                artifact = gateway.put_raw(tenant_id=tenant, source_id=source, native_id=native,
                    payload=canonical_json(content), media_type='application/json', created_at='2026-09-21T00:00:00Z')
                return dict(schema_version=1, source_id=source, native_id=native, native_parent_id=native,
                    kind='tombstone' if tombstone else 'connector_record', occurred_at='2026-09-21T00:00:00Z',
                    observed_at='2026-09-21T00:00:00Z', principal_id=principal, visibility='private',
                    content_type='application/json', content=content,
                    provenance={'connector_id': 'synthetic.history', 'artifact_ref': artifact},
                    content_sha256=hashlib.sha256(canonical_json(content)).hexdigest())

            def write(envelope, path='batch', target=plane):
                if path == 'document':
                    return target.ingest_document(tenant_id=tenant, principal_id=envelope['principal_id'],
                        connector_id='synthetic.history', artifact_ref=envelope['provenance']['artifact_ref'],
                        envelope=envelope, text_redacted='' if envelope['kind'] == 'tombstone' else canonical_json(envelope['content']).decode())
                if path == 'legacy':
                    return bridge._ingest_canonical([envelope], principal={'tenant_id': tenant},
                        raw_payload=None, media_type='application/json')
                events = [envelope, envelope] if path == 'mixed' else [envelope]
                return target.ingest_batch(tenant_id=tenant, principal_id=envelope['principal_id'], events=events)

            def resolve(native, revision=1):
                return store.resolve(f'recall://{source}/{native}?rev={revision}#item=0',
                    tenant_id=tenant, authorized_sources=(source,), chunk_body_archive=archive)

            def project():
                projector.project_pending(tenant_id=tenant, batch_size=100, max_batches=10, upload_concurrency=1)

            def clear(native):
                # Simulate the future clearer: same native lock, current-document fence.
                with store.connect() as connection, connection.transaction():
                    connection.execute('SELECT pg_advisory_xact_lock(hashtextextended(%s,0))',
                                       (f'v2\x1f{tenant}\x1f{source}\x1f{native}',))
                    connection.execute('''UPDATE canonical_documents SET text_redacted='',body_location='chunks'
                        WHERE tenant_id=%s AND source_id=%s AND native_id=%s
                        AND is_current AND deleted_at IS NULL''', (tenant, source, native))
                    connection.execute('''UPDATE canonical_chunks chunk SET text_redacted=''
                        FROM canonical_documents document WHERE chunk.tenant_id=document.tenant_id
                        AND chunk.source_id=document.source_id AND chunk.document_id=document.document_id
                        AND document.tenant_id=%s AND document.source_id=%s AND document.native_id=%s
                        AND document.is_current AND document.deleted_at IS NULL''', (tenant, source, native))

            def expect_retry(callback):
                try:
                    callback()
                except CanonicalLifecycleError as error:
                    assert error.error_code == 'canonical_history_unavailable', error.error_code
                else:
                    raise AssertionError('unsafe revision was acknowledged')

            for path in ('document', 'batch', 'mixed', 'legacy'):
                first, second = event(path, 1), event(path, 2)
                write(first, path)
                baseline = resolve(path)
                project()
                clear(path)
                reads.clear()
                assert write(first, path)['replay'], 'replay changed history'
                assert not reads, 'replay must not hydrate history'
                write(second, path)
                assert reads, 'outgoing cleared body was not restored'
                outgoing_parts = tuple(reads)
                project()
                # Cleanup is a lifecycle operation outside the writer's IO lease proof.
                archive.read_raw = original_read
                projector.drain_cleanup(tenant_id=tenant)
                archive.read_raw = tracked_read
                for reference in outgoing_parts:
                    try:
                        original_read(reference)
                    except ArchiveNotFound:
                        pass
                    else:
                        raise AssertionError('old logical part was not actually deleted')
                reads.clear()
                assert resolve(path) == baseline, path
                assert not reads, 'historical resolution depended on retired logical parts'
                assert resolve(path, 2)['items']

            first, second = event('race', 1), event('race', 2)
            write(first)
            baseline = resolve('race')
            project()
            original_prepare = plane.prepare_history
            @contextmanager
            def clear_after_preflight(**kwargs):
                with original_prepare(**kwargs) as staged:
                    clear('race')
                    yield staged
            with mock.patch.object(plane, 'prepare_history', clear_after_preflight):
                expect_retry(lambda: write(second))
            assert resolve('race', 2) is None
            assert resolve('race') == baseline
            write(second)
            assert resolve('race') == baseline

            # A competing writer can replace the staged revision before this
            # transaction locks it. Never attach the old bytes to the new PK.
            first, second, third = (event('stale', revision) for revision in (1, 2, 3))
            write(first)
            baseline = resolve('stale')
            project()
            clear('stale')
            sibling = CanonicalPlane(store, archive, chunk_body_archive=archive)
            @contextmanager
            def supersede_after_preflight(**kwargs):
                with original_prepare(**kwargs) as staged:
                    write(second, target=sibling)
                    project()
                    clear('stale')
                    yield staged
            with mock.patch.object(plane, 'prepare_history', supersede_after_preflight):
                expect_retry(lambda: write(third))
            assert resolve('stale', 3) is None
            assert resolve('stale') == baseline
            second_baseline = resolve('stale', 2)
            write(third)
            assert resolve('stale', 2) == second_baseline

            shared_first = [event('shared-a', 1), event('shared-b', 1)]
            shared_second = [event('shared-a', 2), event('shared-b', 2)]
            for envelope in shared_first + shared_second:
                envelope['native_parent_id'] = 'shared-parent'
            plane.ingest_batch(tenant_id=tenant, principal_id=principal, events=shared_first)
            shared_baselines = [resolve(envelope['native_id']) for envelope in shared_first]
            project()
            for envelope in shared_first:
                clear(envelope['native_id'])
            reads.clear()
            plane.ingest_batch(tenant_id=tenant, principal_id=principal, events=shared_second)
            assert len(reads) == 1, 'shared immutable part was read more than once'
            assert [resolve(envelope['native_id']) for envelope in shared_first] == shared_baselines

            first, second = event('unavailable', 1), event('unavailable', 2)
            write(first)
            project()
            clear('unavailable')
            expect_retry(lambda: write(second, target=CanonicalPlane(store, archive)))
            with mock.patch.object(archive, 'read_raw', side_effect=OSError('private archive location')):
                expect_retry(lambda: write(second))
            assert resolve('unavailable', 2) is None
            denied = copy.deepcopy(second)
            denied['principal_id'] = 'principal:intruder'
            reads.clear()
            try:
                write(denied)
            except CanonicalLifecycleError as error:
                assert error.error_code == 'canonical_authority_forbidden'
            else:
                raise AssertionError('source owner bypass')
            assert not reads
            with plane.prepare_history(tenant_id='tenant:other', principal_id=principal, events=[second]):
                pass
            assert not reads, 'tenant collision read another tenant archive'
            reads.clear()
            write(event('unavailable', 3, tombstone=True))
            assert not reads, 'tombstone resurrected a body'
            assert resolve('unavailable') is None

            first, second = event('forget', 1), event('forget', 2)
            write(first)
            project()
            clear('forget')
            request = dict(contract='recall.forget-request.v1', schema_version=1, tenant_id=tenant,
                principal_id=principal, source_id=source,
                target_receipt=f'recall://{source}/forget?rev=1#item=0', mode='explicit_forget', reason='owner_requested',
                requested_at='2026-09-21T00:01:00Z', idempotency_key='history-forget')
            @contextmanager
            def forget_after_preflight(**kwargs):
                with original_prepare(**kwargs) as staged:
                    plane.forget(request)
                    yield staged
            with mock.patch.object(plane, 'prepare_history', forget_after_preflight):
                try:
                    write(second)
                except CanonicalLifecycleError as error:
                    assert error.error_code == 'canonical_identity_forgotten', error.error_code
                else:
                    raise AssertionError('forget was resurrected')
            assert resolve('forget') is None and resolve('forget', 2) is None
        print(json.dumps(dict(status='pass', historical_exact_after_cleanup=True,
            all_writer_paths=True, retirement_race_retry=True, unavailable_retry=True,
            unauthorized_zero_archive_io=True, tombstone_and_forget_not_restored=True,
            no_database_connection_during_archive_read=True)))
    finally:
        store.close()
        with psycopg.connect(admin_dsn, autocommit=True) as admin:
            admin.execute(f'DROP DATABASE "{database}"')


if __name__ == '__main__':
    main()
