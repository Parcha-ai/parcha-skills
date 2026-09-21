#!/usr/bin/env python3
"""Real PG: read each parent part once, clear bounded batches, resume safely."""
from pathlib import Path
import errno
import hashlib
import json
import os
import sys
import tempfile
import uuid
from unittest.mock import patch

import psycopg
from psycopg.conninfo import conninfo_to_dict, make_conninfo

SERVER = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(SERVER.parent), str(SERVER)]
from e2e_archive_reprojection import fixture, mark_dirty
from e2e_logical_evidence_projection import insert_record
from e2e_logical_source_integrity import SmallPartProjection, TrackedStore
from recall_server import chunk_retirement as retirement
from recall_server.canonical import CanonicalArchiveGateway, CanonicalPlane
from recall_server.canonical_retrieval import BoundCanonicalRetrieval
from recall_server.projectors import canonical_json
from recall_server.chunk_bodies import read_archived_chunks
from recall_server.logical_evidence_projection import mark_logical_evidence_dirty
from recall_server.chunk_retirement import (ChunkRetirementError, ParentRetirementLimits,
    retire_current_chunks, retire_parent_chunks, set_parent_retirement_enabled)
from recall_server.logical_evidence import LogicalEvidenceProjectionStore
from recall_server.locator_backfill_plan import apply_parent
from recall_server.parent_chunk_proof import ParentMetadataSpool


def refused(callback):
    try:
        callback()
    except ChunkRetirementError:
        return
    raise AssertionError('unsafe parent retirement succeeded')


def fault_cases(store, root):
    tenant, source, archive, _, projector, _ = fixture(store, root, count=10)
    scope = dict(tenant_id=tenant, source_id=source, native_parent_id='session')
    args = (tenant, source)
    def run(**kwargs):
        return retire_parent_chunks(store, archive, **scope, **kwargs)
    def state():
        with store.connect() as connection:
            bodies = connection.execute("SELECT document_id,ordinal,text_redacted FROM canonical_chunks WHERE tenant_id=%s AND source_id=%s ORDER BY document_id,ordinal", args).fetchall()
            progress = connection.execute("SELECT * FROM canonical_chunk_retirement_progress WHERE tenant_id=%s AND source_id=%s", args).fetchone()
        return bodies, progress
    preview = run()
    set_parent_retirement_enabled(store, **scope, enabled=True)
    original, initial_progress = state()
    # All parts must finish before any clear: damage the last immutable part.
    with store.connect() as connection:
        last = connection.execute('SELECT artifact_id FROM canonical_evidence_document_parts WHERE tenant_id=%s AND source_id=%s ORDER BY part_ordinal DESC LIMIT 1', args).fetchone()['artifact_id']
    read = archive.read_raw
    def corrupt_last(reference):
        payload = read(reference)
        return b'x' * len(payload) if reference['artifact_id'] == last else payload
    with patch.object(archive, 'read_raw', side_effect=corrupt_last):
        refused(lambda: run(apply=True, reviewed_plan=preview['plan']))
    assert state() == (original, initial_progress)
    # A valid archive cannot authorize deletion of a different live PG copy.
    with store.connect() as connection:
        connection.execute('UPDATE canonical_chunks SET text_redacted=%s WHERE document_id=%s AND ordinal=0', ('corrupt inline body', original[0]['document_id']))
    corrupted = state()
    refused(lambda: run(apply=True, reviewed_plan=preview['plan']))
    assert state() == corrupted
    with store.connect() as connection:
        connection.execute('UPDATE canonical_chunks SET text_redacted=%s WHERE document_id=%s AND ordinal=0', (original[0]['text_redacted'], original[0]['document_id']))
    refused(lambda: run(apply=True, reviewed_plan=preview['plan'], limits=ParentRetirementLimits(hash_bytes=1)))
    assert state() == (original, initial_progress)
    execute = store._execute_bounded
    updated = []
    def fail_after_update(connection, sql, values, deadline_at):
        cursor = execute(connection, sql, values, deadline_at)
        if "UPDATE canonical_chunks SET text_redacted=''" in sql:
            assert cursor.rowcount > 0
            updated.append(True)
            raise RuntimeError('synthetic SQL failure after actual body update')
        return cursor
    with patch.object(store, '_execute_bounded', side_effect=fail_after_update):
        refused(lambda: run(apply=True, reviewed_plan=preview['plan']))
    assert updated and state() == (original, initial_progress), 'body update and ledger did not roll back together'
    # Expiration after UPDATE must also roll back the progress write and bodies.
    from recall_server.db import SearchDeadlineExceeded
    check = retirement._check
    expired = []
    def expire_after_update(connection, sql, values, deadline_at):
        cursor = execute(connection, sql, values, deadline_at)
        if "UPDATE canonical_chunks SET text_redacted=''" in sql:
            expired.append(True)
        return cursor
    def expired_check(deadline_at):
        if expired:
            raise SearchDeadlineExceeded('search deadline exceeded')
        return check(deadline_at)
    with patch.object(store, '_execute_bounded', side_effect=expire_after_update), patch.object(retirement, '_check', side_effect=expired_check):
        refused(lambda: run(apply=True, reviewed_plan=preview['plan']))
    assert expired and state() == (original, initial_progress)

    # Eligibility can change after the complete archive proof, before locks.
    mutate = retirement._retire_parent_batch
    for target in ('structure', 'media'):
        changed_metadata = []
        def change_eligibility(*values, **options):
            from psycopg.types.json import Jsonb
            with store.connect() as connection:
                row = connection.execute('SELECT event.event_id,event.canonical_redacted,artifact.artifact_id,artifact.media_type FROM canonical_documents document JOIN canonical_events event USING(tenant_id,source_id,event_id) JOIN raw_artifacts artifact ON artifact.tenant_id=event.tenant_id AND artifact.source_id=event.source_id AND artifact.artifact_id=event.artifact_id WHERE document.document_id=%s', (options['rows'][0]['document_id'],)).fetchone()
                changed_metadata.append(row)
                if target == 'structure':
                    connection.execute('UPDATE canonical_events SET canonical_redacted=%s WHERE event_id=%s', (Jsonb(dict(row['canonical_redacted'], type='token_count')), row['event_id']))
                else:
                    connection.execute('UPDATE raw_artifacts SET media_type=%s WHERE artifact_id=%s', ('application/vnd.recall.oversized-record+gzip', row['artifact_id']))
            return mutate(*values, **options)
        with patch.object(retirement, '_retire_parent_batch', side_effect=change_eligibility):
            refused(lambda: run(apply=True, reviewed_plan=preview['plan']))
        assert state() == (original, initial_progress)
        from psycopg.types.json import Jsonb
        with store.connect() as connection:
            row = changed_metadata[0]
            connection.execute('UPDATE canonical_events SET canonical_redacted=%s WHERE event_id=%s', (Jsonb(row['canonical_redacted']), row['event_id']))
            connection.execute('UPDATE raw_artifacts SET media_type=%s WHERE artifact_id=%s', (row['media_type'], row['artifact_id']))
    # A commit is durable even if the process stops before it can return a result.
    mutate = retirement._retire_parent_batch
    def interrupt_after_commit(*values, **options):
        mutate(*values, **options)
        raise KeyboardInterrupt()
    with patch.object(retirement, '_retire_parent_batch', side_effect=interrupt_after_commit):
        try:
            run(apply=True, reviewed_plan=preview['plan'], limits=ParentRetirementLimits(batch_documents=3))
        except KeyboardInterrupt:
            pass
        else:
            raise AssertionError('injected interruption did not stop job')
    bodies, progress = state()
    assert sum(not row['text_redacted'] for row in bodies) == 3 and progress['cumulative_cleared_documents'] == 3
    # Actual publication after the next valid batch stops the remaining stale
    # proof. Previously committed bodies remain available through the new parent.
    committed = []
    def publish_after_batch(*values, **options):
        result = mutate(*values, **options)
        if not committed:
            committed.append(True)
            with store.connect() as connection:
                insert_record(connection, tenant=tenant, source=source, parent='session', native='new-parent-tail',
                              text='new tail retained', role='assistant', byte_start=999)
                mark_logical_evidence_dirty(connection, tenant_id=tenant, source_id=source,
                                           native_ids=['new-parent-tail'], reason='ingest')
            report = projector.project_pending(tenant_id=tenant, batch_size=1, max_batches=1, upload_concurrency=1)
            assert report['failed'] == 0 and report['documents'] == 1, report
        return result
    with patch.object(retirement, '_retire_parent_batch', side_effect=publish_after_batch):
        refused(lambda: run(apply=True, reviewed_plan=preview['plan'], limits=ParentRetirementLimits(batch_documents=3)))
    bodies, progress = state()
    assert sum(not row['text_redacted'] for row in bodies) == 6 and progress['cumulative_cleared_documents'] == 6
    assert progress['status'] == 'pending'
    assert progress['last_record_ordinal'] == -1 and progress['manifest_artifact_id'] is None
    assert progress['scope_epoch'] == initial_progress['scope_epoch'] + 1
    fresh = run()
    # Disable/re-enable changes scope epoch: an already running job cannot use
    # its old cursor after an operator resets that scope.
    toggled = []
    def toggle_after_batch(*values, **options):
        result = mutate(*values, **options)
        if not toggled:
            toggled.append(True)
            set_parent_retirement_enabled(store, **scope, enabled=False)
            set_parent_retirement_enabled(store, **scope, enabled=True)
        return result
    with patch.object(retirement, '_retire_parent_batch', side_effect=toggle_after_batch):
        refused(lambda: run(apply=True, reviewed_plan=fresh['plan'], limits=ParentRetirementLimits(batch_documents=2)))
    assert state()[1]['last_record_ordinal'] == -1
    finished = run(apply=True, reviewed_plan=fresh['plan'])
    assert finished['complete'] and all(not row['text_redacted'] for row in state()[0])


def writer_cases(store, root):
    for standalone in (False, True):
        tenant, source, archive, _, projector, _ = fixture(store, root, count=6)
        scope = dict(tenant_id=tenant, source_id=source, native_parent_id='session')
        principal = 'principal:reprojection'
        plane = CanonicalPlane(store, archive, chunk_body_archive=archive)
        gateway = CanonicalArchiveGateway(store, archive, tenant_id=tenant, principal_id=principal)
        receipt = f'recall://{source}/event-0000?rev=1#item=0'
        before = store.resolve(receipt, tenant_id=tenant, authorized_sources=(source,), chunk_body_archive=archive)
        assert before is not None
        preview = retire_parent_chunks(store, archive, **scope)
        set_parent_retirement_enabled(store, **scope, enabled=True)
        mutate = retirement._retire_parent_batch
        revised = []
        def revise_after_commit(*values, **options):
            result = mutate(*values, **options)
            if not revised:
                revised.append(True)
                content = {'role': 'assistant', 'text': 'new current revision'}
                payload = canonical_json(content)
                reference = gateway.put_raw(tenant_id=tenant, source_id=source, native_id='event-0000',
                    payload=payload, media_type='application/json', created_at='2026-09-21T00:00:00Z')
                envelope = dict(schema_version=1, source_id=source, native_id='event-0000', native_parent_id='session',
                    kind='transcript_record', principal_id=principal, occurred_at='2026-09-21T00:00:00Z',
                    observed_at='2026-09-21T00:00:00Z', visibility='private', content_type='application/json',
                    content=content, content_sha256=hashlib.sha256(payload).hexdigest(),
                    provenance={'connector_id': 'synthetic.parent-retirement', 'artifact_ref': reference})
                if standalone:
                    plane.ingest_document(tenant_id=tenant, principal_id=principal,
                        connector_id='synthetic.parent-retirement', artifact_ref=reference,
                        envelope=envelope, text_redacted=payload.decode())
                else:
                    plane.ingest_batch(tenant_id=tenant, principal_id=principal, events=[envelope])
            return result
        with patch.object(retirement, '_retire_parent_batch', side_effect=revise_after_commit):
            result = retire_parent_chunks(store, archive, **scope, apply=True, reviewed_plan=preview['plan'],
                                          limits=ParentRetirementLimits(batch_documents=2))
        assert result['complete']
        assert store.resolve(receipt, tenant_id=tenant, authorized_sources=(source,), chunk_body_archive=archive) == before
        projected = projector.project_pending(tenant_id=tenant, batch_size=1, max_batches=1, upload_concurrency=1)
        assert projected['failed'] == 0 and projected['documents'] == 1, projected
        projector.drain_cleanup(tenant_id=tenant)
        assert store.resolve(receipt, tenant_id=tenant, authorized_sources=(source,), chunk_body_archive=archive) == before


def excluded_parent(store, root):
    tenant, source, archive, _, _, _ = fixture(store, root, count=3)
    scope = dict(tenant_id=tenant, source_id=source, native_parent_id='session')
    with store.connect() as connection:
        connection.execute("UPDATE raw_artifacts SET media_type='application/vnd.recall.oversized-record+gzip' WHERE tenant_id=%s AND source_id=%s AND artifact_id IN(SELECT artifact_id FROM canonical_documents WHERE tenant_id=%s AND source_id=%s AND native_id='event-0000')", (tenant, source, tenant, source))
        connection.execute("UPDATE canonical_events SET canonical_redacted=jsonb_set(canonical_redacted,'{type}','\"token_count\"') WHERE tenant_id=%s AND source_id=%s AND native_id='event-0001'", (tenant, source))
        connection.execute("UPDATE canonical_documents SET body_record_ordinal=NULL,body_record_count=NULL WHERE tenant_id=%s AND source_id=%s AND native_id='event-0002'", (tenant, source))
    plan = retire_parent_chunks(store, archive, **scope)
    assert plan['eligible_documents'] == 0 and plan['excluded'] == {'oversized': 1, 'structural': 1, 'unlocated': 1}
    set_parent_retirement_enabled(store, **scope, enabled=True)
    report = retire_parent_chunks(store, archive, **scope, apply=True, reviewed_plan=plan['plan'])
    assert report['complete'] and report['cleared_documents'] == 0 and report['cleared_utf8_bytes'] == 0
    with store.connect() as connection:
        assert connection.execute("SELECT count(*) AS n FROM canonical_chunks WHERE tenant_id=%s AND source_id=%s AND text_redacted<>''", (tenant, source)).fetchone()['n'] == 3
    archive.reads.clear()
    refused(lambda: retire_parent_chunks(store, archive, tenant_id='tenant:wrong', source_id=source, native_parent_id='session'))
    refused(lambda: retire_parent_chunks(store, archive, tenant_id=tenant, source_id='source:wrong', native_parent_id='session'))
    assert not archive.reads


def newly_located_prefix(store, root, *, projector_repair=False):
    tenant, source, archive, _, projector, _ = fixture(store, root, count=4)
    scope = dict(tenant_id=tenant, source_id=source, native_parent_id='session')
    def progress():
        with store.connect() as connection:
            return connection.execute('SELECT * FROM canonical_chunk_retirement_progress WHERE tenant_id=%s AND source_id=%s', (tenant, source)).fetchone()
    with store.connect() as connection:
        connection.execute("UPDATE canonical_documents SET body_record_ordinal=NULL,body_record_count=NULL WHERE tenant_id=%s AND source_id=%s AND native_id='event-0000'", (tenant, source))
    plan = retire_parent_chunks(store, archive, **scope)
    set_parent_retirement_enabled(store, **scope, enabled=True)
    first = retire_parent_chunks(store, archive, **scope, apply=True, reviewed_plan=plan['plan'])
    assert first['complete'] and first['cleared_documents'] == 3
    old = progress()
    assert old['last_record_ordinal'] == 3
    def locate():
        if not projector_repair:
            assert apply_parent(store, archive, **scope)['applied_documents'] == 1
        else:
            mark_dirty(store, tenant, source)
            report = projector.project_pending(tenant_id=tenant, batch_size=1, max_batches=1, upload_concurrency=1)
            assert report['failed'] == 0 and report['repaired'] == 1, report
    locate()
    changed = progress()
    assert changed['enabled'] and changed['status'] == 'pending' and changed['last_record_ordinal'] == -1
    assert changed['scope_epoch'] == old['scope_epoch'] + 1
    resumed = retire_parent_chunks(store, archive, **scope, apply=True, reviewed_plan=plan['plan'])
    assert resumed['complete'] and resumed['cleared_documents'] == 1
    # An explicitly disabled scope must remain disabled when a locator is filled.
    set_parent_retirement_enabled(store, **scope, enabled=False)
    with store.connect() as connection:
        connection.execute("UPDATE canonical_documents SET body_record_ordinal=NULL,body_record_count=NULL WHERE tenant_id=%s AND source_id=%s AND native_id='event-0001'", (tenant, source))
    old = progress()
    locate()
    assert progress() == old




def head_schema_capability(store):
    from psycopg import sql
    from recall_server import SCHEMA_VERSION
    from recall_server.capabilities import CapabilityError, probe_database
    role, password = 'retirement_runtime_' + uuid.uuid4().hex[:12], uuid.uuid4().hex
    identifier = sql.Identifier(role)
    with store.connect() as connection:
        port, database = connection.info.port, connection.info.dbname
        connection.execute(sql.SQL('CREATE ROLE {} LOGIN PASSWORD {}').format(identifier, sql.Literal(password)))
        connection.execute(sql.SQL('GRANT USAGE ON SCHEMA public TO {}').format(identifier))
        connection.execute(sql.SQL('GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA public TO {}').format(identifier))
        connection.execute(sql.SQL('REVOKE INSERT,UPDATE,DELETE ON schema_migrations FROM {}').format(identifier))
        connection.execute(sql.SQL('GRANT USAGE,SELECT ON ALL SEQUENCES IN SCHEMA public TO {}').format(identifier))
    dsn = f"postgresql://{role}:{password}@127.0.0.1:{port}/{database}"
    try:
        assert probe_database(dsn, profile='local-fixture')['schema_version'] == SCHEMA_VERSION
        with store.connect() as connection:
            connection.execute(sql.SQL('REVOKE SELECT ON canonical_chunk_retirement_progress FROM {}').format(identifier))
        try:
            probe_database(dsn, profile='local-fixture')
        except CapabilityError as error:
            assert error.code == 'role_privilege_insufficient'
        else:
            raise AssertionError('runtime accepted missing retirement ledger permission')
    finally:
        with store.connect() as connection:
            connection.execute(sql.SQL('DROP OWNED BY {}').format(identifier))
            connection.execute(sql.SQL('DROP ROLE {}').format(identifier))


def schema68_compatibility(store, root):
    tenant, source, archive, _, _, texts = fixture(store, root, count=3)
    scope = dict(tenant_id=tenant, source_id=source, native_parent_id='session')
    with store.connect() as connection:
        connection.execute('DROP TABLE canonical_chunk_retirement_progress')
        connection.execute('DELETE FROM schema_migrations WHERE version=69')
        connection.execute("UPDATE canonical_documents SET body_record_ordinal=NULL,body_record_count=NULL WHERE tenant_id=%s AND source_id=%s", (tenant, source))
    assert apply_parent(store, archive, **scope)['applied_documents'] == 3
    with store.connect() as connection:
        document = connection.execute('SELECT document_id,native_id FROM canonical_documents WHERE tenant_id=%s AND source_id=%s ORDER BY native_id LIMIT 1', (tenant, source)).fetchone()
    targets = dict(tenant_id=tenant, source_id=source, document_ids=(document['document_id'],))
    clear = retire_current_chunks(store, archive, **targets)
    retire_current_chunks(store, archive, **targets, apply=True, reviewed_plan=clear['plan'])
    restore = retire_current_chunks(store, archive, **targets, restore=True)
    retire_current_chunks(store, archive, **targets, restore=True, apply=True, reviewed_plan=restore['plan'])
    with store.connect() as connection:
        body = connection.execute('SELECT text_redacted FROM canonical_chunks WHERE document_id=%s', (document['document_id'],)).fetchone()['text_redacted']
    assert body == texts[document['native_id']]


def main():
    admin_dsn = os.environ['RECALL_DATABASE_URL']
    database = 'recall_parent_retirement_' + uuid.uuid4().hex
    settings = conninfo_to_dict(admin_dsn) | {'dbname': database}
    with psycopg.connect(admin_dsn, autocommit=True) as admin:
        admin.execute(f'CREATE DATABASE "{database}"')
    store = TrackedStore(make_conninfo(**settings))
    # This checks exact read parity, not latency under shared-host load.
    store.search_deadline_ms = 30_000
    try:
        store.migrate()
        assert store.migrate()['applied'] == [], 'additive migration is not idempotent'
        head_schema_capability(store)
        with tempfile.TemporaryDirectory() as temporary, patch.dict(os.environ, RECALL_CHUNK_BODY_READS='archive'):
            with patch.object(SmallPartProjection, 'put_records', LogicalEvidenceProjectionStore.put_records):
                tenant, source, archive, _, projector, texts = fixture(store, Path(temporary), count=1000)
            scope = dict(tenant_id=tenant, source_id=source, native_parent_id='session')
            def run(**kwargs):
                return retire_parent_chunks(store, archive, **scope, **kwargs)
            def remaining():
                with store.connect() as connection:
                    return connection.execute('SELECT count(*) AS n FROM canonical_chunks WHERE tenant_id=%s AND source_id=%s AND text_redacted<>\'\'', (tenant, source)).fetchone()['n']
            def ledger():
                with store.connect() as connection:
                    return connection.execute('SELECT * FROM canonical_chunk_retirement_progress WHERE tenant_id=%s AND source_id=%s', (tenant, source)).fetchone()
            with store.connect() as connection:
                part_count = connection.execute('SELECT count(*) AS n FROM canonical_evidence_document_parts WHERE tenant_id=%s AND source_id=%s', (tenant, source)).fetchone()['n']
            assert part_count == 1
            bound = dict(tenant_id=tenant, principal_id='principal:reprojection', authorized_sources=(source,))
            pg_reader = BoundCanonicalRetrieval(store, **bound)
            archive_reader = BoundCanonicalRetrieval(store, chunk_body_archive=archive, **bound)
            anchor = f'recall://{source}/event-0500?rev=1#item=0'
            expected = (pg_reader.show(anchor), pg_reader.session_context(anchor, before=1, after=1),
                        pg_reader.related(limit=20), store.resolve(anchor, tenant_id=tenant, authorized_sources=(source,)))
            preview = run()
            assert preview['eligible_documents'] == 1000 and remaining() == 1000 and ledger() is None
            assert sum(archive.reads.values()) == part_count and all(v == 1 for v in archive.reads.values())
            archive.reads.clear()
            refused(lambda: run(apply=True, reviewed_plan=preview['plan']))
            assert not archive.reads and remaining() == 1000
            refused(lambda: run(limits=ParentRetirementLimits(max_chunks=999)))
            assert not archive.reads and remaining() == 1000
            set_parent_retirement_enabled(store, **scope, enabled=True)
            # A full proof that cannot be staged must never reach a body UPDATE.
            mark = ParentMetadataSpool.mark_verified
            calls = 0
            def full_spool(*args, **kwargs):
                nonlocal calls
                calls += 1
                if calls == 900:
                    raise OSError(errno.ENOSPC, 'synthetic full metadata spool')
                return mark(*args, **kwargs)
            with patch.object(ParentMetadataSpool, 'mark_verified', side_effect=full_spool, autospec=True):
                refused(lambda: run(apply=True, reviewed_plan=preview['plan']))
            assert remaining() == 1000 and ledger()['last_record_ordinal'] == -1
            archive.reads.clear()
            first = run(apply=True, reviewed_plan=preview['plan'], limits=ParentRetirementLimits(batch_documents=17, max_batches=2))
            assert first['cleared_documents'] == 34 and not first['complete'] and remaining() == 966
            assert ledger()['last_record_ordinal'] == 33 and ledger()['cumulative_cleared_documents'] == 34
            assert sum(archive.reads.values()) == part_count and all(v == 1 for v in archive.reads.values())
            archive.reads.clear()
            resumed = run(apply=True, reviewed_plan=preview['plan'])
            assert resumed['cleared_documents'] == 966 and resumed['complete'] and remaining() == 0
            assert ledger()['cumulative_cleared_documents'] == 1000 and ledger()['status'] == 'complete'
            assert sum(archive.reads.values()) == part_count and all(v == 1 for v in archive.reads.values())
            assert (archive_reader.show(anchor), archive_reader.session_context(anchor, before=1, after=1),
                    archive_reader.related(limit=20), store.resolve(anchor, tenant_id=tenant, authorized_sources=(source,), chunk_body_archive=archive)) == expected
            with store.connect() as connection:
                docs = connection.execute('SELECT document_id,native_id FROM canonical_documents WHERE tenant_id=%s AND source_id=%s ORDER BY native_id LIMIT 2', (tenant, source)).fetchall()
            bodies = read_archived_chunks(store, archive, tenant_id=tenant, source_ids=(source,), document_ids=tuple(row['document_id'] for row in docs))
            assert all(bodies[(source, row['document_id'])][0]['text_redacted'] == texts[row['native_id']] for row in docs)
            # Exact recovery disables future retirement for this whole parent.
            targets = tuple(row['document_id'] for row in docs)
            recovery = retire_current_chunks(store, archive, tenant_id=tenant, source_id=source, document_ids=targets, restore=True)
            retire_current_chunks(store, archive, tenant_id=tenant, source_id=source, document_ids=targets, restore=True,
                                  apply=True, reviewed_plan=recovery['plan'])
            assert remaining() == 2 and not ledger()['enabled']
            archive.reads.clear()
            refused(lambda: run(apply=True, reviewed_plan=preview['plan']))
            assert not archive.reads and remaining() == 2
            fault_cases(store, Path(temporary))
            assert remaining() == 2, 'another source job touched restored bodies'
            writer_cases(store, Path(temporary))
            excluded_parent(store, Path(temporary))
            newly_located_prefix(store, Path(temporary))
            newly_located_prefix(store, Path(temporary), projector_repair=True)
            schema68_compatibility(store, Path(temporary))
        print(json.dumps(dict(status='pass', thousand_documents=True, part_read_once=True,
            bounded_commits_and_resume=True, spool_full_before_first_clear=True,
            current_reads_exact=True, restoration_disables_parent=True, late_corruption_and_actual_pg_hash_refused=True,
            body_and_ledger_rollback_atomic=True, interruption_and_publication_resume=True, scope_epoch_fenced=True, both_writers_preserve_history_after_bulk=True, all_read_routes_exact=True, unsupported_retained=True, newly_located_prefix_revisited=True)))
    finally:
        store.close()
        with psycopg.connect(admin_dsn, autocommit=True) as admin:
            admin.execute(f'DROP DATABASE "{database}" WITH (FORCE)')


if __name__ == '__main__':
    main()
