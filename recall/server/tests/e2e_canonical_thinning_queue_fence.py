#!/usr/bin/env python3
"""PG parity for the queue existence fence, including locks and rollback."""
from __future__ import annotations

import hashlib
import json
import os
import sys
import uuid
from contextlib import contextmanager
from pathlib import Path

import psycopg
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from psycopg.rows import dict_row

SERVER = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(SERVER), str(SERVER.parent)]
from recall_server.canonical_thinning import thin_canonical_bodies  # noqa: E402
from e2e_canonical_body_thinning import insert_document  # noqa: E402

FENCE = (
    '                                -- Keep the full parent key correlated; a source-only\n'
    '                                -- anti-merge can repeatedly scan unrelated parents.\n'
    '                                OFFSET 0\n'
)
BASE_SQL_SHA = 'b3802fedd04b8b4d51d8ed8ae0da209eb6c44fff83785c1854d2f602bbb1607a'
TENANT = 'tenant:fence'
SOURCE = 'source:fence'


class Store:
    def __init__(self, connection, *, legacy=False, fail=False):
        self.connection = connection
        self.legacy = legacy
        self.fail = fail
        self.sql = None
        self.parameters = None

    @contextmanager
    def connect(self):
        yield self

    def execute(self, statement, parameters=None, **kwargs):
        assert kwargs == {}  # preserve original preparation policy/settings
        assert statement.count(FENCE) == 1
        old = statement.replace(FENCE, '')
        assert hashlib.sha256(old.encode()).hexdigest() == BASE_SQL_SHA
        self.sql, self.parameters = old if self.legacy else statement, parameters
        cursor = self.connection.execute(self.sql, parameters)
        if self.fail:
            self.connection.execute('SELECT 1/0')
        return cursor


def snapshot(connection):
    return {
        table: connection.execute('SELECT to_jsonb(t) AS row FROM '+table+' t ORDER BY to_jsonb(t)::text').fetchall()
        for table in ('canonical_documents', 'canonical_events', 'canonical_chunks',
                      'canonical_evidence_documents', 'canonical_evidence_document_queue', 'raw_artifacts')
    }


def paired(connection, *, expected, batch_size=50):
    before = snapshot(connection)
    results = []
    for legacy in (True, False):
        with connection.transaction(force_rollback=True):
            store = Store(connection, legacy=legacy)
            result = thin_canonical_bodies(store, tenant_id=TENANT, batch_size=batch_size)
            assert result['documents'] == result['events'] == len(expected), result
            changed = connection.execute("SELECT document_id FROM canonical_documents WHERE body_location='chunks' AND text_redacted='' ORDER BY document_id").fetchall()
            assert {row['document_id'] for row in changed} == expected
            results.append((result, snapshot(connection)))
            plan = connection.execute('EXPLAIN(FORMAT JSON) '+store.sql, store.parameters).fetchone()['QUERY PLAN'][0]
            nodes = []
            def walk(node):
                nodes.append(node)
                for child in node.get('Plans', []):
                    walk(child)
            walk(plan['Plan'])
            if legacy:
                assert any(n.get('Join Type') == 'Anti' for n in nodes)
            else:
                assert any(str(n.get('Subplan Name', '')).startswith('SubPlan') for n in nodes)
                assert not any(n.get('Join Type') == 'Anti' for n in nodes)
                probes = [n for n in nodes if n.get('Relation Name') == 'canonical_evidence_document_queue']
                assert len(probes) == 1
                assert probes[0]['Node Type'] in ('Index Scan', 'Index Only Scan')
                assert probes[0]['Plan Rows'] <= 1
                condition = probes[0].get('Index Cond', '')
                assert all(key in condition for key in ('tenant_id', 'source_id', 'native_parent_id', 'COALESCE'))
        assert snapshot(connection) == before
    assert results[0] == results[1]


def main():
    admin_dsn = os.environ['RECALL_DATABASE_URL']
    database = 'thin_queue_fence_' + uuid.uuid4().hex
    settings = conninfo_to_dict(admin_dsn)
    settings['dbname'] = database
    dsn = make_conninfo(**settings)
    with psycopg.connect(admin_dsn, autocommit=True) as admin:
        admin.execute(f'CREATE DATABASE "{database}"')
    try:
        with psycopg.connect(dsn, autocommit=True, row_factory=dict_row) as connection:
            for migration in sorted((SERVER / 'schema').glob('*.sql')):
                if int(migration.name[:3]) <= 69:
                    connection.execute(migration.read_text().replace('CONCURRENTLY ', ''))
            connection.execute("SET statement_timeout='5s'")
            connection.execute("SET lock_timeout='500ms'")
            cases = {}
            for suffix in ('good', 'queued', 'no-chunks', 'deleted-chunks', 'raw-filesystem',
                           'raw-deleted', 'manifest-filesystem', 'document-deleted',
                           'already-thin', 'null-parent-queued', 'null-parent-ready', 'oversized'):
                cases[suffix] = insert_document(connection, tenant=TENANT, principal='principal:fence',
                                                source=SOURCE, suffix=suffix, text='Synthetic 🐙 body '*40,
                                                omit_chunks=suffix == 'no-chunks')
            event, document = cases['deleted-chunks']
            connection.execute('UPDATE canonical_chunks SET deleted_at=now() WHERE document_id=%s', (document,))
            event, document = cases['raw-filesystem']
            connection.execute("UPDATE raw_artifacts SET storage_backend='filesystem',encryption='filesystem-owner-only' WHERE artifact_id=(SELECT artifact_id FROM canonical_documents WHERE document_id=%s)", (document,))
            event, document = cases['raw-deleted']
            connection.execute("UPDATE raw_artifacts SET state='deleted',deleted_at=now() WHERE artifact_id=(SELECT artifact_id FROM canonical_documents WHERE document_id=%s)", (document,))
            connection.execute("UPDATE canonical_evidence_documents SET manifest_storage_backend='filesystem',manifest_encryption='filesystem-owner-only' WHERE native_parent_id='session:manifest-filesystem'")
            connection.execute('UPDATE canonical_documents SET deleted_at=now(),is_current=false WHERE document_id=%s', (cases['document-deleted'][1],))
            connection.execute("UPDATE canonical_documents SET body_location='chunks' WHERE document_id=%s", (cases['already-thin'][1],))
            connection.execute("UPDATE canonical_events SET native_parent_id=NULL WHERE event_id=ANY(%s)", ([cases['null-parent-queued'][0], cases['null-parent-ready'][0]],))
            for suffix in ('null-parent-queued', 'null-parent-ready'):
                connection.execute('UPDATE canonical_evidence_documents SET native_parent_id=%s WHERE native_parent_id=%s', ('native:'+suffix, 'session:'+suffix))
            connection.execute("INSERT INTO canonical_evidence_document_queue(tenant_id,source_id,native_parent_id,reason) VALUES(%s,%s,'session:queued','ingest'),(%s,%s,'native:null-parent-queued','ingest')", (TENANT, SOURCE, TENANT, SOURCE))
            # Enough unrelated queued keys for a real full-key lookup, not an
            # empty-queue proof. These rows do not change eligible parent data.
            connection.execute("INSERT INTO canonical_evidence_document_queue(tenant_id,source_id,native_parent_id,reason) SELECT %s,%s,'unrelated:'||i,'ingest' FROM generate_series(1,10000)i", (TENANT, SOURCE))
            other_event, other_document = insert_document(connection, tenant=TENANT, principal='principal:fence', source='source:other', suffix='other-source', text='Other synthetic body')
            connection.execute("UPDATE canonical_events SET native_parent_id='session:queued' WHERE event_id=%s", (other_event,))
            connection.execute("UPDATE canonical_evidence_documents SET native_parent_id='session:queued' WHERE native_parent_id='session:other-source'")
            insert_document(connection, tenant='tenant:other', principal='principal:other', source=SOURCE, suffix='other-tenant', text='Separate synthetic tenant')
            pointer = {'contract':'recall.oversized-projection.v1','schema_version':1,
                       'full_record_available':True,'full_content_sha256':'a'*64,
                       'full_size_bytes':2000,'archive_encoding':'gzip','head':'synthetic head','tail':'synthetic tail'}
            connection.execute("UPDATE canonical_events SET canonical_redacted=jsonb_set(canonical_redacted,'{content}',%s::jsonb) WHERE event_id=%s", (json.dumps(pointer), cases['oversized'][0]))
            for table in ('canonical_documents', 'canonical_events', 'canonical_chunks',
                          'canonical_evidence_documents', 'canonical_evidence_document_queue', 'raw_artifacts'):
                connection.execute('ANALYZE '+table)
            settings_before = connection.execute("SELECT current_setting('jit') AS jit,current_setting('plan_cache_mode') AS mode").fetchone()
            expected = {cases[k][1] for k in ('good', 'null-parent-ready', 'oversized')} | {other_document}
            paired(connection, expected=expected)
            order = connection.execute('SELECT document_id FROM canonical_documents WHERE document_id=ANY(%s) ORDER BY source_id,document_id LIMIT 1', (list(expected),)).fetchone()['document_id']
            paired(connection, expected={order}, batch_size=1)
            for relation, key, value in [('canonical_documents','document_id',cases['good'][1]),
                                         ('canonical_events','event_id',cases['good'][0])]:
                with psycopg.connect(dsn) as locker:
                    locker.execute('SELECT 1 FROM '+relation+' WHERE '+key+'=%s FOR UPDATE', (value,))
                    paired(connection, expected=expected-{cases['good'][1]})
            before = snapshot(connection)
            try:
                with connection.transaction():
                    thin_canonical_bodies(Store(connection, fail=True), tenant_id=TENANT, batch_size=50)
            except psycopg.errors.DivisionByZero:
                pass
            else:
                raise AssertionError('post-update failure not propagated')
            assert snapshot(connection) == before
            assert connection.info.transaction_status == psycopg.pq.TransactionStatus.IDLE
            assert connection.execute("SELECT current_setting('jit') AS jit,current_setting('plan_cache_mode') AS mode").fetchone() == settings_before
            assert connection.prepare_threshold == 5
            paired(connection, expected=expected)
        print(json.dumps({'status':'pass','parity_cases':5,'queue_keys':10002,
                          'authority_and_null_parent_guards':True,'skip_locked_both_tables':True,
                          'rollback_and_reuse':True,'settings_unchanged':True}))
    finally:
        with psycopg.connect(admin_dsn, autocommit=True) as admin:
            admin.execute(f'DROP DATABASE "{database}" WITH(FORCE)')


if __name__ == '__main__':
    main()
