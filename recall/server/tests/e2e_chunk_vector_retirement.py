#!/usr/bin/env python3
"""Optional derived-column retirement and retained routes on real PostgreSQL."""
from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
import uuid
from unittest.mock import patch

import psycopg
from psycopg.conninfo import conninfo_to_dict, make_conninfo

SERVER = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SERVER.parent))
sys.path.insert(0, str(SERVER))
from e2e_logical_evidence_projection import insert_record, insert_source  # noqa: E402
from recall_server.canonical_retrieval import BoundCanonicalRetrieval  # noqa: E402
from recall_server.capabilities import probe_database  # noqa: E402
from recall_server.db import BrainStore  # noqa: E402
from recall_server import storage_retirement as retirement  # noqa: E402


def has_vector(store):
    with store.connect() as connection:
        return connection.execute("""SELECT EXISTS(SELECT 1 FROM pg_attribute
            WHERE attrelid='canonical_chunks'::regclass AND attname='search_vector'
              AND NOT attisdropped) AS present""").fetchone()['present']


def refused(store, reason):
    try:
        retirement.retire_chunk_search_vector(store, apply=True)
    except ValueError as error:
        assert reason in str(error)
    else:
        raise AssertionError('unsafe vector retirement accepted')
    assert has_vector(store)


def boundary():
    admin_dsn = os.environ['RECALL_DATABASE_URL']
    database = 'recall_vector_' + uuid.uuid4().hex
    settings = conninfo_to_dict(admin_dsn)
    settings['dbname'] = database
    reader_role = 'vector_reader_' + uuid.uuid4().hex
    reader_password = uuid.uuid4().hex
    with psycopg.connect(admin_dsn, autocommit=True) as admin:
        admin.execute(f'CREATE DATABASE "{database}"')
        admin.execute(psycopg.sql.SQL('CREATE ROLE {} LOGIN PASSWORD {}').format(
            psycopg.sql.Identifier(reader_role), psycopg.sql.Literal(reader_password)))
    store = BrainStore(make_conninfo(**settings))
    try:
        store.migrate()
        tenant, source, principal = 'tenant:vector', 'source:vector', 'principal:vector'
        with store.connect() as connection:
            insert_source(connection, tenant, principal, source)
            receipt = insert_record(connection, tenant=tenant, source=source, parent='session',
                native='native', text='synthetic vector retirement exact body', role='assistant', byte_start=0)
        bound = BoundCanonicalRetrieval(store, tenant_id=tenant, principal_id=principal, authorized_sources=(source,))
        legacy = bound._legacy_chunk_search_for_eval('synthetic vector retirement')
        assert legacy['results'], legacy
        before = bound.show(receipt)
        refused(store, 'turbopuffer')
        store.search_plane = 'turbopuffer'
        refused(store, '067')
        store.migrate(retire_postgres_plane=True)
        refused(store, 'search index')
        retirement.retire_chunk_search_index(store, apply=True)
        assert retirement.retire_chunk_search_vector(store)['status'] == 'preview'
        assert has_vector(store)
        # Same-table dependencies would disappear automatically even with RESTRICT.
        for create, remove in [
            ('CREATE INDEX vector_extra ON canonical_chunks USING gin(search_vector)', 'DROP INDEX vector_extra'),
            ('ALTER TABLE canonical_chunks ADD CONSTRAINT vector_extra CHECK(search_vector IS NOT NULL)', 'ALTER TABLE canonical_chunks DROP CONSTRAINT vector_extra'),
            ('CREATE VIEW vector_extra AS SELECT search_vector FROM canonical_chunks', 'DROP VIEW vector_extra'),
            ('CREATE STATISTICS vector_extra ON search_vector,chunk_id FROM canonical_chunks', 'DROP STATISTICS vector_extra'),
        ]:
            with store.connect() as connection:
                connection.execute(create)
            refused(store, 'dependencies')
            with store.connect() as connection:
                connection.execute(remove)
        for definition in ("tsvector", "tsvector GENERATED ALWAYS AS (to_tsvector('english',text_redacted)) STORED"):
            with store.connect() as connection:
                connection.execute('ALTER TABLE canonical_chunks DROP COLUMN search_vector')
                connection.execute('ALTER TABLE canonical_chunks ADD COLUMN search_vector ' + definition)
            refused(store, 'definition')
        with store.connect() as connection:
            connection.execute('ALTER TABLE canonical_chunks DROP COLUMN search_vector')
            connection.execute("ALTER TABLE canonical_chunks ADD COLUMN search_vector tsvector GENERATED ALWAYS AS (to_tsvector('simple',text_redacted)) STORED")
        with psycopg.connect(store.dsn) as blocker:
            blocker.execute('LOCK TABLE canonical_chunks IN ACCESS SHARE MODE')
            try:
                retirement.retire_chunk_search_vector(store, apply=True)
            except psycopg.errors.LockNotAvailable:
                pass
            else:
                raise AssertionError('busy table accepted')
        assert has_vector(store)
        original = retirement._vector_snapshot
        calls = 0
        def after_drop(connection):
            nonlocal calls
            calls += 1
            if calls == 3:
                raise RuntimeError('synthetic post-DDL failure')
            return original(connection)
        with patch.object(retirement, '_vector_snapshot', side_effect=after_drop):
            try:
                retirement.retire_chunk_search_vector(store, apply=True)
            except RuntimeError:
                pass
            else:
                raise AssertionError('injected failure was ignored')
        assert has_vector(store), 'DDL failure must roll back the entire transaction'
        env = dict(os.environ, PYTHONPATH=str(SERVER), RECALL_SEARCH_PLANE='turbopuffer',
                   RECALL_TPUF_API_KEY='synthetic', RECALL_TPUF_CLIENT_FACTORY='tests.central_brain.fake_turbopuffer:factory')
        # The command's default is a real read-only preview, not application.
        preview = subprocess.run([sys.executable, '-m', 'recall_server.cli', '--dsn', store.dsn,
            'storage-retire-chunk-search-vector'], env=env, cwd=SERVER.parent, text=True, capture_output=True)
        assert preview.returncode == 0, 'vector CLI preview failed'
        assert json.loads(preview.stdout)['status'] == 'preview' and has_vector(store)
        with store.connect() as connection:
            versions = connection.execute('SELECT version FROM schema_migrations ORDER BY version').fetchall()
        result = retirement.retire_chunk_search_vector(store, apply=True)
        assert result['status'] == 'retired' and result['physical_bytes_reclaimed'] == 0
        assert not has_vector(store) and bound.show(receipt) == before
        assert retirement.retire_chunk_search_vector(store, apply=True)['status'] == 'already_absent'
        store.migrate()
        with store.connect() as connection:
            connection.execute(psycopg.sql.SQL('GRANT USAGE ON SCHEMA public TO {}').format(psycopg.sql.Identifier(reader_role)))
            connection.execute(psycopg.sql.SQL('GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA public TO {}').format(psycopg.sql.Identifier(reader_role)))
            connection.execute(psycopg.sql.SQL('REVOKE INSERT,UPDATE,DELETE ON schema_migrations FROM {}').format(psycopg.sql.Identifier(reader_role)))
            connection.execute(psycopg.sql.SQL('GRANT USAGE,SELECT ON ALL SEQUENCES IN SCHEMA public TO {}').format(psycopg.sql.Identifier(reader_role)))
        reader_dsn = (f"postgresql://{reader_role}:{reader_password}@"
                      f"{settings.get('host', '127.0.0.1')}:{settings.get('port', '5432')}/{database}?sslmode=disable")
        assert probe_database(reader_dsn, profile='local-fixture')['status'] == 'fixture-ready'
        assert not has_vector(store), 'migration 028 recreated the retired vector'
        with store.connect() as connection:
            assert connection.execute('SELECT version FROM schema_migrations ORDER BY version').fetchall() == versions
            assert connection.execute("SELECT to_regclass('canonical_chunks_search_idx') AS idx").fetchone()['idx'] is None
        with store.connect() as connection:
            store.verify_search_plane(connection)
    finally:
        store.close()
        with psycopg.connect(admin_dsn, autocommit=True) as admin:
            admin.execute(f'DROP DATABASE "{database}" WITH (FORCE)')
            admin.execute(psycopg.sql.SQL('DROP ROLE {}').format(psycopg.sql.Identifier(reader_role)))


def retained_routes():
    # Reuse the unchanged assertion suites on a new database variant: all their
    # fixture migrations now explicitly retire 067 and this derived column first.
    import e2e_archived_chunk_reads
    import e2e_revision_history
    import e2e_chunk_retirement
    import e2e_parent_scoped_retrieval
    migrate = BrainStore.migrate
    def without_vector(store, **kwargs):
        store.search_plane = 'turbopuffer'
        result = migrate(store, retire_postgres_plane=True)
        retirement.retire_chunk_search_index(store, apply=True)
        retirement.retire_chunk_search_vector(store, apply=True)
        assert not has_vector(store)
        return result
    with patch.object(BrainStore, 'migrate', without_vector):
        e2e_archived_chunk_reads.main()
        e2e_revision_history.main()
        e2e_chunk_retirement.main()
    # The parent suite itself tests GIN removal. Retire the vector immediately
    # after that real operation, preserving its before/after retrieval assertions.
    old_index = retirement.retire_chunk_search_index
    def index_then_vector(store, **kwargs):
        if kwargs.get('apply'):
            migrate(store, retire_postgres_plane=True)
        result = old_index(store, **kwargs)
        if kwargs.get('apply'):
            retirement.retire_chunk_search_vector(store, apply=True)
            assert not has_vector(store)
        return result
    parent_lookup = BoundCanonicalRetrieval._parent_scoped_receipts
    search_results = {}
    def lookup_with_public_search(bound, **kwargs):
        present = has_vector(bound.store)
        if present not in search_results:
            # Run the public passage/TPUF path, including canonical time clipping,
            # on both sides of DDL; ignore diagnostics, preserve result equality.
            result = bound.search('migration', filters={'since': '2026-07-27T00:30:00Z'}, limit=5)
            assert result['results'], result
            search_results[present] = result['results']
        return parent_lookup(bound, **kwargs)
    with patch.object(e2e_parent_scoped_retrieval, 'retire_chunk_search_index', index_then_vector), \
         patch.object(BoundCanonicalRetrieval, '_parent_scoped_receipts', lookup_with_public_search):
        e2e_parent_scoped_retrieval.main()
    assert set(search_results) == {False, True} and search_results[False] == search_results[True]



def main():
    boundary()
    retained_routes()
    print(json.dumps(dict(status='pass', optional_vector_retirement=True, default_postgres_preserved=True,
        guards_and_rollback=True, no_schema_version_change=True, retired_current_and_historical_routes=True)))


if __name__ == '__main__':
    main()
