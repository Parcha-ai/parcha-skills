#!/usr/bin/env python3
"""Disposable PG: exact current-body retirement, retained history, and race refusal."""
import copy
import hashlib
import json
import os
import sys
import tempfile
import time
import uuid
from pathlib import Path
from unittest import mock

import psycopg
from psycopg.conninfo import conninfo_to_dict, make_conninfo

SERVER = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(SERVER.parent), str(SERVER)]
from e2e_archive_reprojection import ReadCountingArchive
from e2e_logical_source_integrity import TrackedStore
from recall_server.archive import FilesystemArchiveStore
from recall_server.canonical import CanonicalArchiveGateway, CanonicalPlane
from recall_server.canonical_retrieval import BoundCanonicalRetrieval
from recall_server.chunk_retirement import ChunkRetirementError, retire_current_chunks
from recall_server.evidence_projection import CanonicalEvidenceProjector, EvidenceProjectionStore
from recall_server.logical_evidence import LogicalEvidenceError, LogicalEvidenceProjectionStore
from recall_server.logical_evidence_projection import CanonicalLogicalEvidenceProjector
from recall_server.projectors import canonical_json
from recall_server import chunk_retirement


def refused(callback):
    try:
        callback()
    except ChunkRetirementError:
        return
    raise AssertionError('unsafe retirement succeeded')


def main():
    admin_dsn = os.environ['RECALL_DATABASE_URL']
    database = 'recall_chunk_retirement_' + uuid.uuid4().hex
    settings = conninfo_to_dict(admin_dsn)
    settings['dbname'] = database
    with psycopg.connect(admin_dsn, autocommit=True) as admin:
        admin.execute(f'CREATE DATABASE "{database}"')
    store = TrackedStore(make_conninfo(**settings))
    try:
        store.migrate()
        tenant, principal, source = 'tenant:retire', 'principal:retire', 'source:retire'
        with tempfile.TemporaryDirectory() as temporary, \
             mock.patch.dict(os.environ, {'RECALL_CHUNK_BODY_READS': 'archive'}):
            archive = ReadCountingArchive(FilesystemArchiveStore(Path(temporary), namespace_key=b'r' * 32), store)
            logical = CanonicalLogicalEvidenceProjector(store, LogicalEvidenceProjectionStore(archive),
                                                       bound_tenant_id=tenant, raw_archive=archive)
            # This is the production app's forget wiring; logical rebuilds run separately.
            plane = CanonicalPlane(store, archive,
                evidence_projector=CanonicalEvidenceProjector(store, EvidenceProjectionStore(archive)),
                chunk_body_archive=archive)
            gateway = CanonicalArchiveGateway(store, archive, tenant_id=tenant, principal_id=principal)

            def event(native, revision=1, parent='session', tombstone=False):
                native = 'event-' + native
                content = {'target_native_id': native} if tombstone else {
                    'role': 'assistant', 'text': f'{native} revision {revision} α 🧠\n' * 2500}
                reference = gateway.put_raw(tenant_id=tenant, source_id=source, native_id=native,
                    payload=canonical_json(content), media_type='application/json', created_at='2026-09-21T00:00:00Z')
                return dict(schema_version=1, source_id=source, native_id=native, native_parent_id=parent,
                    kind='tombstone' if tombstone else 'connector_record', principal_id=principal,
                    occurred_at='2026-09-21T00:00:00Z', observed_at='2026-09-21T00:00:00Z',
                    visibility='private', content_type='application/json', content=content,
                    content_sha256=hashlib.sha256(canonical_json(content)).hexdigest(),
                    provenance={'connector_id': 'synthetic.retirement', 'artifact_ref': reference})

            def write(envelope, standalone=False):
                if standalone:
                    return plane.ingest_document(tenant_id=tenant, principal_id=principal,
                        connector_id='synthetic.retirement', artifact_ref=envelope['provenance']['artifact_ref'],
                        envelope=envelope, text_redacted=canonical_json(envelope['content']).decode())
                return plane.ingest_batch(tenant_id=tenant, principal_id=principal, events=[envelope])

            def project():
                report = logical.project_pending(tenant_id=tenant, batch_size=100, max_batches=2, upload_concurrency=1)
                assert report['failed'] == 0, report

            def docs(natives):
                natives = ['event-' + native for native in natives]
                with store.connect() as connection:
                    return tuple(row['document_id'] for row in connection.execute('''
                        SELECT document_id FROM canonical_documents WHERE tenant_id=%s AND source_id=%s
                        AND native_id=ANY(%s) AND is_current ORDER BY native_id''', (tenant, source, natives)))

            def bodies(document_ids):
                with store.connect() as connection:
                    return connection.execute('''SELECT document_id,ordinal,receipt,text_sha256,text_redacted
                        FROM canonical_chunks WHERE tenant_id=%s AND source_id=%s AND document_id=ANY(%s)
                        ORDER BY document_id,ordinal''', (tenant, source, list(document_ids))).fetchall()

            def retire(document_ids, **kwargs):
                return retire_current_chunks(store, archive, tenant_id=tenant, source_id=source,
                                             document_ids=document_ids, **kwargs)

            def resolve(native, revision=1):
                return store.resolve(f'recall://{source}/event-{native}?rev={revision}#item=0', tenant_id=tenant,
                                     authorized_sources=(source,), chunk_body_archive=archive)

            for native in ('a', 'b', 'c', 'd'):
                write(event(native))
            project()
            targets = docs(['a', 'b', 'c', 'd'])
            original = bodies(targets)
            target_receipt = f'recall://{source}/event-b?rev=1#item=1'
            bound = dict(tenant_id=tenant, principal_id=principal, authorized_sources=(source,))
            pg_reader = BoundCanonicalRetrieval(store, **bound)
            archive_reader = BoundCanonicalRetrieval(store, chunk_body_archive=archive, **bound)
            expected = (pg_reader.show(target_receipt), pg_reader.session_context(target_receipt, before=1, after=1),
                        pg_reader.related(limit=20))
            old_responses = {native: resolve(native) for native in ('a', 'b', 'c', 'd')}
            preview = retire(targets)
            assert preview['status'] == 'dry_run' and preview['bytes'] > 0
            assert bodies(targets) == original, 'dry-run changed PG bodies'
            fake_plan = copy.deepcopy(preview['plan'])
            fake_plan['proof_sha256'] = '0' * 64
            refused(lambda: retire(targets, apply=True, reviewed_plan=fake_plan))
            with mock.patch.object(archive, 'read_raw', side_effect=OSError('private archive key')):
                refused(lambda: retire(targets, apply=True, reviewed_plan=preview['plan']))
            assert bodies(targets) == original, 'proof failure partially cleared a batch'
            # Witness the actual UPDATE inside the transaction, then inject a
            # failure or let the shared deadline expire before COMMIT. Both
            # cases must restore every target's original PG bytes.
            def assert_update_rollback(reviewed, restoring=False):
                before_failure = bodies(targets)
                execute = store._execute_bounded
                check = chunk_retirement._check
                for failure in ('failure', 'deadline'):
                    updated = []
                    def execute_then_fail(connection, sql, params, deadline_at):
                        cursor = execute(connection, sql, params, deadline_at)
                        if 'UPDATE canonical_chunks' in sql:
                            assert cursor.rowcount > 0
                            visible = connection.execute("SELECT count(*) AS n FROM canonical_chunks WHERE tenant_id=%s AND text_redacted<>''", (tenant,)).fetchone()
                            assert bool(visible['n']) == restoring
                            updated.append(True)
                            if failure == 'failure':
                                raise RuntimeError('injected failure after update')
                        return cursor
                    def check_after_update(deadline_at):
                        check(time.monotonic() - 1 if updated and failure == 'deadline' else deadline_at)
                    with mock.patch.object(store, '_execute_bounded', side_effect=execute_then_fail), \
                         mock.patch.object(chunk_retirement, '_check', side_effect=check_after_update):
                        try:
                            retire(targets, apply=True, reviewed_plan=reviewed, restore=restoring)
                        except ChunkRetirementError as error:
                            assert error.error_code == ('chunk_retirement_deadline_exceeded' if failure == 'deadline' else 'chunk_retirement_unavailable')
                        else:
                            raise AssertionError('post-update failure committed')
                    assert updated and bodies(targets) == before_failure, 'post-update failure did not roll back every body'
            assert_update_rollback(preview['plan'])
            applied = retire(targets, apply=True, reviewed_plan=preview['plan'])
            assert applied['bytes'] == preview['bytes']
            assert all(row['text_redacted'] == '' for row in bodies(targets))
            assert (archive_reader.show(target_receipt), archive_reader.session_context(target_receipt, before=1, after=1),
                    archive_reader.related(limit=20)) == expected
            assert {native: resolve(native) for native in old_responses} == old_responses
            refused(lambda: retire(targets, apply=True, reviewed_plan=preview['plan']))
            restore_preview = retire(targets, restore=True)
            assert restore_preview['bytes'] == preview['bytes']
            assert restore_preview['plan']['operation'] == 'restore'
            refused(lambda: retire(targets, restore=True, apply=True, reviewed_plan=preview['plan']))
            assert_update_rollback(restore_preview['plan'], restoring=True)
            corrupt_restore = copy.deepcopy(restore_preview['plan'])
            corrupt_restore['proof_sha256'] = 'f' * 64
            refused(lambda: retire(targets, restore=True, apply=True, reviewed_plan=corrupt_restore))
            with mock.patch.object(archive, 'read_raw', side_effect=OSError('private archive key')):
                refused(lambda: retire(targets, restore=True, apply=True, reviewed_plan=restore_preview['plan']))
            retire(targets, restore=True, apply=True, reviewed_plan=restore_preview['plan'])
            assert bodies(targets) == original, 'rollback did not restore exact PostgreSQL bodies'
            refused(lambda: retire(targets, restore=True, apply=True, reviewed_plan=restore_preview['plan']))
            assert (pg_reader.show(target_receipt), pg_reader.session_context(target_receipt, before=1, after=1),
                    pg_reader.related(limit=20)) == expected
            # Restoration must be complete before reverting the archive-read
            # profile. Re-clear with a freshly reviewed plan for lifecycle tests.
            fresh_preview = retire(targets)
            retire(targets, apply=True, reviewed_plan=fresh_preview['plan'])

            write(event('a', 2), standalone=True)
            write(event('b', 2))
            write(event('e'))
            project()
            logical.drain_cleanup(tenant_id=tenant)
            assert resolve('a') == old_responses['a'] and resolve('b') == old_responses['b']
            assert resolve('d') == old_responses['d'], 'append/reprojection lost a thinned survivor'
            archive.reads.clear()
            refused(lambda: retire(targets))  # Includes historical targets now.
            refused(lambda: retire(targets, restore=True, apply=True, reviewed_plan=restore_preview['plan']))
            refused(lambda: retire_current_chunks(store, archive, tenant_id='tenant:other', source_id=source, document_ids=docs(['d'])))
            refused(lambda: retire_current_chunks(store, archive, tenant_id=tenant, source_id='source:other', document_ids=docs(['d'])))
            assert not archive.reads, 'denied target performed archive IO'

            plane.forget(dict(contract='recall.forget-request.v1', schema_version=1, tenant_id=tenant,
                principal_id=principal, source_id=source, target_receipt=f'recall://{source}/event-c?rev=1#item=0',
                mode='explicit_forget', reason='owner_requested', requested_at='2026-09-21T00:01:00Z',
                idempotency_key='retirement-forget'))
            assert resolve('c') is None and resolve('d') == old_responses['d']
            project()
            assert resolve('c') is None and resolve('d') == old_responses['d']
            write(event('d', 2, tombstone=True))
            assert resolve('d') is None

            for native in ('race-a', 'race-b'):
                write(event(native, parent='race'))
            project()
            race_targets = docs(['race-a', 'race-b'])
            race_original = bodies(race_targets)
            race_preview = retire(race_targets)
            # Competing clearer/writer native lock and publisher row lock both
            # refuse immediately, with every target body left intact.
            with psycopg.connect(store.dsn) as contender, contender.transaction():
                contender.execute('SELECT pg_advisory_xact_lock(hashtextextended(%s,0))',
                                  (f'v2\x1f{tenant}\x1f{source}\x1fevent-race-a',))
                refused(lambda: retire(race_targets, apply=True, reviewed_plan=race_preview['plan']))
            assert bodies(race_targets) == race_original
            with psycopg.connect(store.dsn) as contender, contender.transaction():
                contender.execute('''SELECT logical_document_id FROM canonical_evidence_documents
                    WHERE tenant_id=%s AND source_id=%s AND native_parent_id='race' FOR UPDATE''', (tenant, source))
                refused(lambda: retire(race_targets, apply=True, reviewed_plan=race_preview['plan']))
            assert bodies(race_targets) == race_original

            original_apply = chunk_retirement._apply_verified
            def replace_after_proof(*args, **kwargs):
                write(event('race-a', 2, parent='race'))
                return original_apply(*args, **kwargs)
            with mock.patch.object(chunk_retirement, '_apply_verified', side_effect=replace_after_proof):
                refused(lambda: retire(race_targets, apply=True, reviewed_plan=race_preview['plan']))
            assert bodies(race_targets) == race_original, 'revision race cleared old bodies'
            project()
            race_targets = docs(['race-a', 'race-b'])
            race_original = bodies(race_targets)
            race_preview = retire(race_targets)
            def publish_after_proof(*args, **kwargs):
                write(event('race-c', parent='race'))
                project()
                return original_apply(*args, **kwargs)
            with mock.patch.object(chunk_retirement, '_apply_verified', side_effect=publish_after_proof):
                refused(lambda: retire(race_targets, apply=True, reviewed_plan=race_preview['plan']))
            assert bodies(race_targets) == race_original, 'publication race partially cleared a batch'
            with store.connect() as connection:
                connection.execute('UPDATE canonical_documents SET body_record_ordinal=NULL,body_record_count=NULL WHERE document_id=%s', (race_targets[0],))
            archive.reads.clear()
            refused(lambda: retire(race_targets))
            assert not archive.reads, 'unlocated target performed archive IO'

            for native in ('guard-a', 'guard-b'):
                write(event(native, parent='guard'))
            project()
            guard_targets = docs(['guard-a', 'guard-b'])
            guard_original = bodies(guard_targets)
            # The alternate whole-parent delete API must serialize with the
            # clearer's shared publication lock before inspecting survivors.
            with psycopg.connect(store.dsn) as contender, contender.transaction():
                contender.execute('''SELECT logical_document_id FROM canonical_evidence_documents
                    WHERE tenant_id=%s AND source_id=%s AND native_parent_id='guard' FOR SHARE''', (tenant, source))
                try:
                    logical.delete_native_ids(tenant_id=tenant, source_id=source, native_ids=['event-guard-a'])
                except LogicalEvidenceError:
                    pass
                else:
                    raise AssertionError('whole-parent delete bypassed retirement publication lock')
            assert bodies(guard_targets) == guard_original
            guard_preview = retire(guard_targets)
            guard_response = resolve('guard-b')
            retire(guard_targets, apply=True, reviewed_plan=guard_preview['plan'])
            try:
                logical.delete_native_ids(tenant_id=tenant, source_id=source, native_ids=['event-guard-a'])
            except LogicalEvidenceError as error:
                assert str(error) == 'logical_evidence_survivor_body_required'
            else:
                raise AssertionError('whole-parent delete destroyed a thinned survivor')
            assert resolve('guard-b') == guard_response
        print(json.dumps(dict(status='pass', dry_run_no_changes=True, all_reads_exact=True, post_update_failure_and_expiry_rollback=True, exact_restore_rollback=True,
            history_and_survivors_retained=True, forget_and_tombstones_denied=True,
            native_lock_and_publication_races_refused=True, explicit_targets_and_locators_required=True,
            no_archive_io_during_database_lease=True)))
    finally:
        store.close()
        with psycopg.connect(admin_dsn, autocommit=True) as admin:
            admin.execute(f'DROP DATABASE "{database}" WITH (FORCE)')


if __name__ == '__main__':
    main()
