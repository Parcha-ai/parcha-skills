#!/usr/bin/env python3
"""Original runtime probes, reads and publishes before/after additive schema89.

The future columns below are a disposable fixture, not a shipped migration.
Both vector-plane states use isolated databases and a least-privilege app role.
"""
import os
from pathlib import Path
import tempfile
import uuid
from unittest.mock import patch

import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo

import e2e_optional_reconcile_index as original
from recall_server.capabilities import CapabilityError, probe_database
from recall_server.canonical_retrieval import BoundCanonicalRetrieval
from recall_server.db import BrainStore
from recall_server.passage_index import CanonicalPassageProjector
from recall_server.passage_projection import DEFAULT_PASSAGE_POLICY


def scenario(admin_dsn, root, retired):
    database = 'recall_tool_compat_' + uuid.uuid4().hex
    role = 'tool_compat_' + uuid.uuid4().hex[:12]
    password = uuid.uuid4().hex
    identifier = sql.Identifier(role)
    with psycopg.connect(admin_dsn, autocommit=True) as admin:
        admin.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(database)))
    dsn = make_conninfo(**(conninfo_to_dict(admin_dsn) | {'dbname': database}))
    store, runtime = original.TrackedStore(dsn), None
    try:
        store.migrate(retire_postgres_plane=retired)
        tenant, source, _, projection, _, _ = original.fixture(store, root, count=2)
        with store.connect() as connection:
            port = connection.info.port
            # Isolate88→89 even after a future publisher ships migration089.
            connection.execute('ALTER TABLE canonical_passage_documents DROP COLUMN IF EXISTS requested_extraction_version, DROP COLUMN IF EXISTS applied_extraction_version, DROP COLUMN IF EXISTS extraction_coverage')
            connection.execute('DELETE FROM schema_migrations WHERE version>88')
            connection.execute(sql.SQL('CREATE ROLE {} LOGIN PASSWORD {}').format(identifier, sql.Literal(password)))
            connection.execute(sql.SQL('GRANT USAGE ON SCHEMA public TO {}').format(identifier))
            connection.execute(sql.SQL('GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA public TO {}').format(identifier))
            connection.execute(sql.SQL('REVOKE INSERT,UPDATE,DELETE ON schema_migrations FROM {}').format(identifier))
            connection.execute(sql.SQL('GRANT USAGE,SELECT ON ALL SEQUENCES IN SCHEMA public TO {}').format(identifier))
            connection.execute("INSERT INTO canonical_source_grants VALUES(%s,'principal:reprojection',%s,'owner',now())", (tenant, source))
        app_dsn = f'postgresql://{role}:{password}@127.0.0.1:{port}/{database}'
        assert probe_database(app_dsn, profile='local-fixture')['schema_version'] == 88
        runtime = BrainStore(app_dsn, search_deadline_ms=30000)
        reader = BoundCanonicalRetrieval(runtime, tenant_id=tenant, principal_id='principal:reprojection', authorized_sources=(source,))
        receipt = f'recall://{source}/event-0000?rev=1#item=0'
        before = reader.show(receipt)
        with store.connect() as connection:
            connection.execute("""ALTER TABLE canonical_passage_documents
                ADD COLUMN requested_extraction_version text NOT NULL DEFAULT 'legacy'
                    CHECK(requested_extraction_version IN ('legacy','tool_groups_v1')),
                ADD COLUMN applied_extraction_version text NOT NULL DEFAULT 'legacy'
                    CHECK(applied_extraction_version IN ('legacy','tool_groups_v1')),
                ADD COLUMN extraction_coverage jsonb""")
            connection.execute('INSERT INTO schema_migrations(version) VALUES(89)')
        assert probe_database(app_dsn, profile='local-fixture')['schema_version'] == 89
        assert reader.show(receipt) == before
        # Real original queue hydration/preparation/commit with the new columns.
        projector = CanonicalPassageProjector(runtime, projection, policy=DEFAULT_PASSAGE_POLICY,
                                             bound_tenant_id=tenant)
        report = projector.project_pending(tenant_id=tenant, batch_size=1, max_batches=1, concurrency=1)
        assert report['documents'] == 1 and report['unavailable'] == 0, report
        with store.connect() as connection:
            state = connection.execute('SELECT requested_extraction_version,applied_extraction_version,extraction_coverage FROM canonical_passage_documents WHERE tenant_id=%s', (tenant,)).fetchone()
            assert state == dict(requested_extraction_version='legacy', applied_extraction_version='legacy', extraction_coverage=None)
            connection.execute('INSERT INTO schema_migrations(version) VALUES(91)')
        try:
            probe_database(app_dsn, profile='local-fixture')
        except CapabilityError as error:
            assert error.code == 'schema_drift'
        else:
            raise AssertionError('unknown schema91 accepted')
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
                            RECALL_TPUF_FAKE_STATE=str(root / 'vendor-fixture.json')):
                scenario(os.environ['RECALL_DATABASE_URL'], root, retired)
    print('PASS original runtime: schema88/89 startup, exact reads, legacy publication, both planes, unknown91 refusal')


if __name__ == '__main__':
    main()
