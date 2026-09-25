#!/usr/bin/env python3
"""Optional timestamp index bounds event work without changing schema/readiness."""
from pathlib import Path
import json
import os
import sys
import uuid
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo

SERVER = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(SERVER.parent), str(SERVER)]
from e2e_logical_evidence_projection import insert_record, insert_source  # noqa:E402
from recall_server.capabilities import probe_database  # noqa:E402
from recall_server.canonical_retrieval import BoundCanonicalRetrieval  # noqa:E402
from recall_server.context_time_index import ensure_context_time_index  # noqa:E402
from recall_server.db import BrainStore  # noqa:E402


def measured_context(store, reader, receipt):
    captured = []
    execute = store._execute_bounded
    def record(connection, query, args, deadline):
        if 'JOIN LATERAL' in query and 'ORDER BY event.occurred_at' in query:
            captured.append((query, args))
        return execute(connection, query, args, deadline)
    with patch.object(store, '_execute_bounded', record):
        response = reader.session_context(receipt, before=2, after=2)
    assert len(captured) == 2
    work = []
    def scan(node):
        if node.get('Relation Name') == 'canonical_events' and node.get('Alias') == 'canonical_events':
            work.append((node.get('Actual Rows', 0) + node.get('Rows Removed by Filter', 0)) * node.get('Actual Loops', 1))
        for child in node.get('Plans', []):
            scan(child)
    with store.connect() as connection:
        for query, args in captured:
            plan = connection.execute('EXPLAIN(ANALYZE,FORMAT JSON,TIMING OFF) '+query, args).fetchone()['QUERY PLAN'][0]
            scan(plan['Plan'])
    return response, max(work)


def main():
    admin_dsn = os.environ['RECALL_DATABASE_URL']
    database = 'context_index_' + uuid.uuid4().hex
    role = 'context_reader_' + uuid.uuid4().hex[:12]
    password = uuid.uuid4().hex
    with psycopg.connect(admin_dsn, autocommit=True) as admin:
        admin.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(database)))
    dsn = make_conninfo(**(conninfo_to_dict(admin_dsn) | {'dbname': database}))
    store, app = BrainStore(dsn, search_deadline_ms=30000), None
    try:
        store.migrate()
        tenant, source, owner = 'tenant:context-index', 'source:context-index', 'principal:context-index'
        with store.connect() as connection:
            insert_source(connection, tenant, owner, source)
            connection.execute("INSERT INTO canonical_source_grants VALUES(%s,%s,%s,'owner',now())", (tenant, owner, source))
            for number in range(1201):
                insert_record(connection, tenant=tenant, source=source, parent='session',
                              native=f'event-{number:04}', text=f'exact synthetic text {number}',
                              role='assistant', byte_start=number)
            connection.execute('UPDATE canonical_events SET source_ordinal=1201-substring(native_id from 7)::int')
            for table in ('canonical_events', 'canonical_documents', 'canonical_chunks'):
                connection.execute('ANALYZE '+table)
            connection.execute('DROP INDEX canonical_passages_reconcile_idx')
            # Isolate the historical 069→070 proof from later optional markers.
            connection.execute('DELETE FROM schema_migrations WHERE version IN (70,71,72,73,74)')
            connection.execute(sql.SQL('CREATE ROLE {} LOGIN PASSWORD {}').format(sql.Identifier(role), sql.Literal(password)))
            connection.execute(sql.SQL('GRANT USAGE ON SCHEMA public TO {}').format(sql.Identifier(role)))
            connection.execute(sql.SQL('GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA public TO {}').format(sql.Identifier(role)))
            connection.execute(sql.SQL('REVOKE INSERT,UPDATE,DELETE ON schema_migrations FROM {}').format(sql.Identifier(role)))
            connection.execute(sql.SQL('GRANT USAGE,SELECT ON ALL SEQUENCES IN SCHEMA public TO {}').format(sql.Identifier(role)))
            app_port = connection.info.port
        app_dsn = f'postgresql://{role}:{password}@127.0.0.1:{app_port}/{database}'
        app = BrainStore(app_dsn, search_deadline_ms=30000)
        reader = BoundCanonicalRetrieval(app, tenant_id=tenant, principal_id=owner, authorized_sources=(source,))
        receipt = f'recall://{source}/event-0600?rev=1#item=0'
        assert probe_database(app_dsn, profile='local-fixture')['schema_version'] == 69
        assert ensure_context_time_index(app_dsn)['status'] == 'absent'
        denied = ensure_context_time_index(app_dsn, apply=True, timeout_seconds=5)
        assert denied['status'] == 'inspect_required' and denied['error_class'] == 'InsufficientPrivilege', denied
        # A same-name Btree is not sufficient: collation/opclass changes can
        # prevent it from providing the query's ordinary text ordering.
        for last_key in ('native_id COLLATE "C"', 'native_id text_pattern_ops'):
            with psycopg.connect(dsn, autocommit=True) as admin:
                admin.execute('CREATE INDEX CONCURRENTLY canonical_events_context_time_idx '
                              'ON public.canonical_events(tenant_id,source_id,'
                              '(COALESCE(native_parent_id,native_id)),occurred_at,'+last_key+')')
            incompatible = ensure_context_time_index(dsn, apply=True)
            assert incompatible['status'] == 'incompatible' and incompatible['action'] == 'refused', incompatible
            assert not incompatible['ddl_attempted']
            with psycopg.connect(dsn, autocommit=True) as admin:
                admin.execute('DROP INDEX CONCURRENTLY public.canonical_events_context_time_idx')
        before, old_work = measured_context(app, reader, receipt)
        built = ensure_context_time_index(dsn, apply=True, timeout_seconds=10)
        assert built['status'] == 'ready' and built['action'] == 'created', built
        after, new_work = measured_context(app, reader, receipt)
        assert before == after and old_work >= 500 and new_work <= 10, (old_work, new_work)
        assert ensure_context_time_index(dsn, apply=True)['action'] == 'already_ready'
        assert probe_database(app_dsn, profile='local-fixture')['schema_version'] == 69
        with psycopg.connect(dsn, autocommit=True) as admin:
            admin.execute('DROP INDEX CONCURRENTLY public.canonical_events_context_time_idx')
        # A prior writer forces the concurrent build to wait after publishing its
        # invalid catalog entry. Timeout leaves it for explicit inspected cleanup.
        with psycopg.connect(dsn) as writer, ThreadPoolExecutor(max_workers=1) as executor:
            writer.execute("UPDATE canonical_events SET kind=kind WHERE native_id='event-0000'")
            future = executor.submit(ensure_context_time_index, dsn, apply=True, timeout_seconds=1)
            assert reader.session_context(receipt, before=2, after=2) == before
            canceled = future.result(timeout=5)
            writer.rollback()
        assert canceled['status'] == 'inspect_required' and canceled['ddl_attempted'] and not canceled['ddl_acknowledged'], canceled
        assert canceled['error_class'] in ('QueryCanceled', 'LockNotAvailable'), canceled
        invalid = ensure_context_time_index(dsn)
        assert invalid['status'] == 'invalid', invalid
        refused = ensure_context_time_index(dsn, apply=True)
        assert refused['status'] == 'invalid' and refused['action'] == 'refused' and not refused['ddl_attempted']
        # Fixture-only manual cleanup: inspect exact invalid identity first;
        # production runbook requires separate operator admission to do this.
        with psycopg.connect(dsn, autocommit=True) as admin:
            state = admin.execute("SELECT indexrelid,indisvalid FROM pg_index WHERE indexrelid='public.canonical_events_context_time_idx'::regclass").fetchone()
            assert state == (invalid['index_oid'], False)
            admin.execute('DROP INDEX CONCURRENTLY public.canonical_events_context_time_idx')
        assert ensure_context_time_index(dsn)['status'] == 'absent'
        rebuilt = ensure_context_time_index(dsn, apply=True, timeout_seconds=10)
        assert rebuilt['status'] == 'ready' and rebuilt['index_oid'] != invalid['index_oid']
        with store.connect() as connection:
            store._migrate_concurrently(connection, (SERVER/'schema/070b_reconcile_passage_ids_concurrent.sql').read_text())
            connection.execute((SERVER/'schema/070_reconcile_passage_ids.sql').read_text())
        assert probe_database(app_dsn, profile='local-fixture')['schema_version'] == 70
        assert reader.session_context(receipt, before=2, after=2) == before
        print(json.dumps(dict(status='pass', old_event_rows=old_work, indexed_event_rows=new_work,
                              exact_timestamp_and_native_order=True, schema69_and70_unchanged=True,
                              least_privilege_build_refused=True, wrong_collation_and_opclass_refused=True,
                              serving_continues_during_build=True,
                              canceled_build_invalid_then_refused=True, explicit_cleanup_then_rebuild=True)))
    finally:
        if app is not None:
            app.close()
        store.close()
        with psycopg.connect(admin_dsn, autocommit=True) as admin:
            admin.execute(sql.SQL('DROP DATABASE {} WITH(FORCE)').format(sql.Identifier(database)))
            admin.execute(sql.SQL('DROP ROLE IF EXISTS {}').format(sql.Identifier(role)))


if __name__ == '__main__':
    main()
