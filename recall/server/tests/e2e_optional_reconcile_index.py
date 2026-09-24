#!/usr/bin/env python3
"""A least-privilege runtime serves required schema before optional index070 is applied."""
from pathlib import Path
import json
import os
import sys
import tempfile
import uuid
from unittest.mock import patch

import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo

SERVER = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(SERVER.parent), str(SERVER)]
from e2e_archive_reprojection import fixture  # noqa: E402
from e2e_logical_source_integrity import TrackedStore  # noqa: E402
from recall_server import SCHEMA_VERSION  # noqa: E402
from recall_server.capabilities import CapabilityError, probe_database  # noqa: E402
from recall_server.canonical_retrieval import BoundCanonicalRetrieval  # noqa: E402
from recall_server.db import BrainStore  # noqa: E402


def scenario(admin_dsn, root, retired):
    database = 'recall_optional_index_' + uuid.uuid4().hex
    role = 'optional_index_runtime_' + uuid.uuid4().hex[:12]
    password = uuid.uuid4().hex
    identifier = sql.Identifier(role)
    with psycopg.connect(admin_dsn, autocommit=True) as admin:
        admin.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(database)))
    dsn = make_conninfo(**(conninfo_to_dict(admin_dsn) | {'dbname': database}))
    store, runtime = TrackedStore(dsn), None
    try:
        store.migrate(retire_postgres_plane=retired)
        tenant, source, _, _, _, _ = fixture(store, root, count=2)
        with store.connect() as conn:
            port = conn.info.port
            conn.execute('DROP INDEX canonical_passages_reconcile_idx')
            conn.execute('DELETE FROM schema_migrations WHERE version=70')
            initial_versions = [row['version'] for row in conn.execute('SELECT version FROM schema_migrations ORDER BY version')]
            assert 71 in initial_versions and 70 not in initial_versions
            conn.execute(sql.SQL('CREATE ROLE {} LOGIN PASSWORD {}').format(identifier, sql.Literal(password)))
            conn.execute(sql.SQL('GRANT USAGE ON SCHEMA public TO {}').format(identifier))
            conn.execute(sql.SQL('GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA public TO {}').format(identifier))
            conn.execute(sql.SQL('REVOKE INSERT,UPDATE,DELETE ON schema_migrations FROM {}').format(identifier))
            conn.execute(sql.SQL('GRANT USAGE,SELECT ON ALL SEQUENCES IN SCHEMA public TO {}').format(identifier))
            conn.execute("INSERT INTO canonical_source_grants VALUES(%s,'principal:reprojection',%s,'owner',now())", (tenant, source))
        app_dsn = f'postgresql://{role}:{password}@127.0.0.1:{port}/{database}'
        result = probe_database(app_dsn, profile='local-fixture')
        assert result['schema_version'] == SCHEMA_VERSION
        assert result['postgres_vector_plane'] == ('retired' if retired else 'present')
        runtime = BrainStore(app_dsn, search_deadline_ms=30000)
        reader = BoundCanonicalRetrieval(runtime, tenant_id=tenant, principal_id='principal:reprojection', authorized_sources=(source,))
        receipt = f'recall://{source}/event-0000?rev=1#item=0'
        def reads():
            return (reader.show(receipt), reader.session_context(receipt, before=0, after=0),
                    runtime.resolve(receipt, tenant_id=tenant, authorized_sources=(source,)))
        before = reads()
        with store.connect() as conn:
            # Serving/probing did not apply migrations or create the index.
            assert conn.execute("SELECT to_regclass('canonical_passages_reconcile_idx') AS value").fetchone()['value'] is None
            assert [row['version'] for row in conn.execute('SELECT version FROM schema_migrations ORDER BY version')] == initial_versions
            store._migrate_concurrently(conn, (SERVER/'schema/070b_reconcile_passage_ids_concurrent.sql').read_text())
            conn.execute((SERVER/'schema/070_reconcile_passage_ids.sql').read_text())
            index = conn.execute("SELECT indisvalid,indisready FROM pg_index WHERE indexrelid='canonical_passages_reconcile_idx'::regclass").fetchone()
            assert index['indisvalid'] and index['indisready']
            assert [row['version'] for row in conn.execute('SELECT version FROM schema_migrations ORDER BY version')] == sorted(initial_versions + [70])
        assert probe_database(app_dsn, profile='local-fixture')['schema_version'] == SCHEMA_VERSION
        assert reads() == before, 'optional index changed exact reads'
        with psycopg.connect(app_dsn) as app:
            try:
                app.execute('DELETE FROM schema_migrations WHERE version=70')
            except psycopg.errors.InsufficientPrivilege:
                app.rollback()
            else:
                raise AssertionError('runtime can mutate migration markers')
        with store.connect() as conn:
            conn.execute(sql.SQL('REVOKE SELECT ON canonical_chunk_retirement_progress FROM {}').format(identifier))
        try:
            probe_database(app_dsn, profile='local-fixture')
        except CapabilityError as error:
            assert error.code == 'role_privilege_insufficient'
        else:
            raise AssertionError('bridge relaxed required ledger privileges')
    finally:
        if runtime is not None:
            runtime.close()
        store.close()
        with psycopg.connect(admin_dsn, autocommit=True) as admin:
            admin.execute(sql.SQL('DROP DATABASE {} WITH (FORCE)').format(sql.Identifier(database)))
            admin.execute(sql.SQL('DROP ROLE IF EXISTS {}').format(identifier))


def main():
    with tempfile.TemporaryDirectory() as folder:
        root = Path(folder)
        for retired in (False, True):
            with patch.dict(os.environ, RECALL_SEARCH_PLANE='turbopuffer' if retired else 'postgres',
                            RECALL_TPUF_API_KEY='synthetic',
                            RECALL_TPUF_CLIENT_FACTORY='tests.central_brain.fake_turbopuffer:factory',
                            RECALL_TPUF_FAKE_STATE=str(root/'fake-turbopuffer.json')):
                scenario(os.environ['RECALL_DATABASE_URL'], root, retired)
    print(json.dumps(dict(status='pass', required_schema_before_index=True, optional70_after_explicit_migration=True,
                          both_search_planes=True, least_privilege_preserved=True, exact_read_pairs=6)))


if __name__ == '__main__':
    main()
