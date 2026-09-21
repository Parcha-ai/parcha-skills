#!/usr/bin/env python3
"""Fresh PG: interactive NULL locators use exact inline copies without S3 scans."""
from pathlib import Path
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
from e2e_archive_reprojection import fixture
from e2e_logical_source_integrity import TrackedStore
from recall_server.canonical import CanonicalArchiveGateway, CanonicalPlane
from recall_server.canonical_retrieval import BoundCanonicalRetrieval
from recall_server.chunk_bodies import ChunkBodyError, read_archived_chunks
from recall_server.chunk_retirement import retire_current_chunks
from recall_server.projectors import canonical_json


def main():
    admin_dsn = os.environ['RECALL_DATABASE_URL']
    database = 'recall_located_public_' + uuid.uuid4().hex
    settings = conninfo_to_dict(admin_dsn) | {'dbname': database}
    with psycopg.connect(admin_dsn, autocommit=True) as admin:
        admin.execute(f'CREATE DATABASE "{database}"')
    store = TrackedStore(make_conninfo(**settings))
    store.search_deadline_ms = 30_000  # Functional parity, independent of shared-host latency.
    try:
        store.migrate()
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, RECALL_CHUNK_BODY_READS='archive'):
            tenant, source, archive, _, _, _ = fixture(store, Path(directory), count=6)
            principal = 'principal:reprojection'
            scope = dict(tenant_id=tenant, principal_id=principal, authorized_sources=(source,))
            pg = BoundCanonicalRetrieval(store, **scope)
            reader = BoundCanonicalRetrieval(store, chunk_body_archive=archive, **scope)
            anchor = f'recall://{source}/event-0002?rev=1#item=0'
            def responses(retrieval, archived=False):
                return (retrieval.show(anchor), retrieval.session_context(anchor, before=2, after=2),
                        retrieval.related(limit=20), store.resolve(anchor, tenant_id=tenant,
                        authorized_sources=(source,), chunk_body_archive=archive if archived else None))
            expected = responses(pg)
            with store.connect() as connection:
                docs = connection.execute('SELECT document_id,native_id,body_record_ordinal,body_record_count FROM canonical_documents WHERE tenant_id=%s AND source_id=%s ORDER BY native_id', (tenant, source)).fetchall()
                parts = connection.execute('SELECT * FROM canonical_evidence_document_parts WHERE tenant_id=%s AND source_id=%s ORDER BY part_ordinal', (tenant, source)).fetchall()
                connection.execute('UPDATE canonical_documents SET body_record_ordinal=NULL,body_record_count=NULL WHERE tenant_id=%s AND source_id=%s', (tenant, source))
            assert len(parts) == 6
            archive.reads.clear()
            assert responses(reader, True) == expected
            assert not archive.reads, 'interactive NULL position performed archive I/O'
            # Recovery still explicitly supports the transitional full-parent read.
            recovered = read_archived_chunks(store, archive, tenant_id=tenant, source_ids=(source,),
                                              document_ids=(docs[2]['document_id'],))
            assert (source, docs[2]['document_id']) in recovered
            assert sum(archive.reads.values()) == len(parts)
            # A NULL neighbor must not expand a located anchor into a parent scan.
            with store.connect() as connection:
                connection.execute('UPDATE canonical_documents SET body_record_ordinal=%s,body_record_count=%s WHERE document_id=%s',
                                   (docs[2]['body_record_ordinal'], docs[2]['body_record_count'], docs[2]['document_id']))
            archive.reads.clear()
            assert responses(reader, True) == expected
            assert set(archive.reads) == {parts[2]['artifact_id']}
            # The one located body can be retired while all unlocated copies remain.
            targets = dict(tenant_id=tenant, source_id=source, document_ids=(docs[2]['document_id'],))
            plan = retire_current_chunks(store, archive, **targets)
            retire_current_chunks(store, archive, **targets, apply=True, reviewed_plan=plan['plan'])
            archive.reads.clear()
            assert responses(reader, True) == expected
            assert set(archive.reads) == {parts[2]['artifact_id']}
            # Corrupt archive for a located body cannot become blank inline fallback.
            raw = archive.read_raw
            def corrupt(reference):
                payload = raw(reference)
                return b'x' * len(payload)
            with patch.object(archive, 'read_raw', side_effect=corrupt):
                try:
                    reader.show(anchor)
                except ChunkBodyError:
                    pass
                else:
                    raise AssertionError('located corruption fell back')
            # Unlocated copies still require their exact retained chunk hash.
            unlocated = f'recall://{source}/event-0000?rev=1#item=0'
            with store.connect() as connection:
                connection.execute("UPDATE canonical_chunks SET text_redacted='' WHERE document_id=%s", (docs[0]['document_id'],))
            archive.reads.clear()
            try:
                reader.show(unlocated)
            except ChunkBodyError:
                pass
            else:
                raise AssertionError('missing unlocated PG body was accepted')
            assert not archive.reads
            # Every public route keeps the same tenant/grant fences.
            denied = BoundCanonicalRetrieval(store, chunk_body_archive=archive, **(scope | {'authorized_sources': ()}))
            other_tenant = BoundCanonicalRetrieval(store, chunk_body_archive=archive, **(scope | {'tenant_id': 'tenant:denied'}))
            for bound in (denied, other_tenant):
                assert bound.show(anchor) is None and bound.session_context(anchor) is None
                assert not bound.related()['results']
            assert store.resolve(anchor, tenant_id=tenant, authorized_sources=(), chunk_body_archive=archive) is None
            assert not archive.reads
            # A real tombstone hides an unlocated document before fallback.
            native = 'event-0001'
            content = {'target_native_id': native}
            gateway = CanonicalArchiveGateway(store, archive, tenant_id=tenant, principal_id=principal)
            reference = gateway.put_raw(tenant_id=tenant, source_id=source, native_id=native,
                payload=canonical_json(content), media_type='application/json', created_at='2026-09-21T00:00:00Z')
            envelope = dict(schema_version=1, source_id=source, native_id=native, native_parent_id='session',
                kind='tombstone', principal_id=principal, occurred_at='2026-09-21T00:00:00Z',
                observed_at='2026-09-21T00:00:00Z', visibility='private', content_type='application/json',
                content=content, content_sha256=hashlib.sha256(canonical_json(content)).hexdigest(),
                provenance={'connector_id': 'synthetic.located', 'artifact_ref': reference})
            CanonicalPlane(store, archive, chunk_body_archive=archive).ingest_batch(
                tenant_id=tenant, principal_id=principal, events=[envelope])
            hidden = f'recall://{source}/{native}?rev=1#item=0'
            archive.reads.clear()
            assert reader.show(hidden) is None and reader.session_context(hidden) is None
            assert store.resolve(hidden, tenant_id=tenant, authorized_sources=(source,), chunk_body_archive=archive) is None
            assert not archive.reads
        print(json.dumps(dict(status='pass', null_public_zero_gets=True, mixed_only_located_part=True,
            low_level_full_scan_preserved=True, all_read_routes_exact=True, retired_located_exact=True,
            archive_and_inline_corruption_refused=True, tenant_grants_and_tombstones_preserved=True)))
    finally:
        store.close()
        with psycopg.connect(admin_dsn, autocommit=True) as admin:
            admin.execute(f'DROP DATABASE "{database}" WITH (FORCE)')


if __name__ == '__main__':
    main()
