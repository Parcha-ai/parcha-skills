#!/usr/bin/env python3
"""Schema90 compatibility before activation, using disposable native PostgreSQL.

The fixture reconstructs schema89 and applies the separately reviewed role/version
DDL. Old-writer publication is explicit even after the new default ships.
"""
import json
import os
from pathlib import Path
import tempfile
import uuid
from unittest.mock import patch

import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo

import e2e_optional_reconcile_index as original
from recall_server import SCHEMA_VERSION
from recall_server.capabilities import CapabilityError, probe_database
from recall_server.canonical_retrieval import BoundCanonicalRetrieval
from recall_server.db import BrainStore
from recall_server.passage_index import CanonicalPassageProjector
from recall_server.passage_projection import DEFAULT_PASSAGE_POLICY

FUTURE_DDL = Path(__file__).parent / 'fixtures' / 'codex_extraction_090.sql'
TOOL_UNIT = 'codex.tool-result.string.v1#/payload/output'


def rejected_update(connection, query, params):
    try:
        with connection.transaction():
            connection.execute(query, params)
    except psycopg.errors.CheckViolation:
        return
    raise AssertionError('unsupported role or extraction version accepted')


def scenario(admin_dsn, root, retired):
    database = 'recall_codex_compat_' + uuid.uuid4().hex
    role = 'codex_compat_' + uuid.uuid4().hex[:12]
    password = uuid.uuid4().hex
    identifier = sql.Identifier(role)
    with psycopg.connect(admin_dsn, autocommit=True) as admin:
        admin.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(database)))
    dsn = make_conninfo(**(conninfo_to_dict(admin_dsn) | {'dbname': database}))
    store, runtime = original.TrackedStore(dsn), None
    try:
        store.migrate(retire_postgres_plane=retired)
        # Isolate the original89→90 compatibility witness after activation ships.
        # No tool rows exist yet. This is disposable fixture DDL only.
        with store.connect() as connection:
            connection.execute("""ALTER TABLE canonical_passages
                DROP CONSTRAINT canonical_passages_roles_check1,
                ADD CONSTRAINT canonical_passages_roles_check1
                    CHECK(roles <@ ARRAY['assistant','user']::text[])""")
            for field in ('requested_extraction_version','applied_extraction_version'):
                connection.execute(sql.SQL("ALTER TABLE canonical_passage_documents DROP CONSTRAINT {}, ADD CONSTRAINT {} CHECK ({} IN ('legacy','tool_groups_v1'))").format(
                    sql.Identifier('canonical_passage_documents_'+field+'_check'),
                    sql.Identifier('canonical_passage_documents_'+field+'_check'),sql.Identifier(field)))
            connection.execute('DELETE FROM schema_migrations WHERE version>=90')
        tenant, source, _, projection, _, _ = original.fixture(store, root, count=2)
        with store.connect() as connection:
            port = connection.info.port
            connection.execute(sql.SQL('CREATE ROLE {} LOGIN PASSWORD {}').format(identifier, sql.Literal(password)))
            connection.execute(sql.SQL('GRANT USAGE ON SCHEMA public TO {}').format(identifier))
            connection.execute(sql.SQL('GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA public TO {}').format(identifier))
            connection.execute(sql.SQL('REVOKE INSERT,UPDATE,DELETE ON schema_migrations FROM {}').format(identifier))
            connection.execute(sql.SQL('GRANT USAGE,SELECT ON ALL SEQUENCES IN SCHEMA public TO {}').format(identifier))
            connection.execute("INSERT INTO canonical_source_grants VALUES(%s,'principal:reprojection',%s,'owner',now())", (tenant, source))
        app_dsn = f'postgresql://{role}:{password}@127.0.0.1:{port}/{database}'
        assert probe_database(app_dsn, profile='local-fixture')['schema_version'] == 89
        runtime = BrainStore(app_dsn, search_deadline_ms=30000)
        reader = BoundCanonicalRetrieval(runtime, tenant_id=tenant, principal_id='principal:reprojection', authorized_sources=(source,))
        receipt = f'recall://{source}/event-0000?rev=1#item=0'
        before = reader.show(receipt)
        projector = CanonicalPassageProjector(runtime, projection, policy=DEFAULT_PASSAGE_POLICY,
                                             bound_tenant_id=tenant)
        with patch('recall_server.passage_index._document_extraction_version', return_value='legacy'):
            report = projector.project_pending(tenant_id=tenant, batch_size=1, max_batches=1, concurrency=1)
        assert report['documents'] == 1 and report['unavailable'] == 0, report
        with runtime.connect() as connection:
            row = connection.execute('SELECT * FROM canonical_passages WHERE tenant_id=%s ORDER BY ordinal LIMIT 1', (tenant,)).fetchone()
            key = (tenant, source, row['passage_id'])
            update = 'UPDATE canonical_passages SET roles=%s,spans=%s WHERE tenant_id=%s AND source_id=%s AND passage_id=%s'
            typed = [dict(span, source_unit=TOOL_UNIT) for span in row['spans']]
            rejected_update(connection, update, (['tool'], json.dumps(typed), *key))
        with store.connect() as connection:
            connection.execute(FUTURE_DDL.read_text())
        assert probe_database(app_dsn, profile='local-fixture')['schema_version'] == 90
        assert reader.show(receipt) == before
        with runtime.connect() as connection:
            # Storage contract only: these are synthetic metadata mutations,
            # not a claim the legacy fixture has a provider tool result.
            connection.execute(update, (['tool'], json.dumps(typed), *key))
            actual = connection.execute('SELECT roles,spans FROM canonical_passages WHERE tenant_id=%s AND source_id=%s AND passage_id=%s', key).fetchone()
            assert actual == dict(roles=['tool'], spans=typed)
            invalid = [(['tool'], []), (['tool'], [{}]),
                       (['tool'], [dict(typed[0], source_unit=None)]),
                       (['tool'], [dict(typed[0], source_unit=TOOL_UNIT + '/forged')]),
                       (['tool'], [dict(typed[0], source_unit='codex.tool-input.string.v1#/payload/arguments')]),
                       (['tool'], typed + [row['spans'][0]]),
                       (['assistant', 'tool'], typed), (['user', 'tool'], typed),
                       (['system'], typed), (['analysis'], typed), ([], typed)]
            for roles, spans in invalid:
                rejected_update(connection, update, (roles, json.dumps(spans), *key))
            rejected_update(connection,
                'UPDATE canonical_passages SET source_id=%s WHERE tenant_id=%s AND source_id=%s AND passage_id=%s',
                ('claude:wrong-provider', *key))
            connection.execute(update, (row['roles'], json.dumps(row['spans']), *key))
            for field in ['requested_extraction_version', 'applied_extraction_version']:
                statement = sql.SQL('UPDATE canonical_passage_documents SET {}=%s WHERE tenant_id=%s').format(sql.Identifier(field))
                connection.execute(statement, ('codex_tool_groups_v1', tenant))
                rejected_update(connection, statement, ('future_unknown', tenant))
                connection.execute(statement, ('legacy', tenant))
        assert reader.show(receipt) == before
        # A new legacy document still follows the existing native queue/write path.
        tenant2, _, _, projection2, _, _ = original.fixture(store, root, count=1)
        with patch('recall_server.passage_index._document_extraction_version', return_value='legacy'):
            report = CanonicalPassageProjector(runtime, projection2, policy=DEFAULT_PASSAGE_POLICY,
                bound_tenant_id=tenant2).project_pending(tenant_id=tenant2, batch_size=1, max_batches=1, concurrency=1)
        assert report['documents'] == 1 and report['unavailable'] == 0, report
        with store.connect() as connection:
            state = connection.execute('SELECT requested_extraction_version,applied_extraction_version FROM canonical_passage_documents WHERE tenant_id=%s', (tenant2,)).fetchone()
            assert state == dict(requested_extraction_version='legacy', applied_extraction_version='legacy')
        store.migrate(retire_postgres_plane=retired)
        assert probe_database(app_dsn, profile='local-fixture')['schema_version'] == SCHEMA_VERSION
        assert reader.show(receipt) == before
        with store.connect() as connection:
            connection.execute('INSERT INTO schema_migrations(version) VALUES(%s)', (SCHEMA_VERSION + 1,))
        try:
            probe_database(app_dsn, profile='local-fixture')
        except CapabilityError as error:
            assert error.code == 'schema_drift'
        else:
            raise AssertionError(f'unknown schema{SCHEMA_VERSION + 1} accepted')
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
    print(f'PASS schema89/90 native startup, exact reads, legacy publication, strict typed-role/version constraints, both planes, unknown{SCHEMA_VERSION + 1} refusal')


if __name__ == '__main__':
    main()
