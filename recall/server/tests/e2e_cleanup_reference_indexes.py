#!/usr/bin/env python3
"""Exact cleanup authority uses selective artifact probes on a skewed catalog."""
from __future__ import annotations

import ast
import inspect
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
import uuid

import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo

RECALL = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(RECALL), str(RECALL / 'server')]
from e2e_archive_reprojection import fixture  # noqa: E402
from e2e_logical_evidence_projection import insert_source  # noqa: E402
from e2e_logical_source_integrity import TrackedStore  # noqa: E402
from recall_server import logical_evidence_projection as owner  # noqa: E402

INDEXES = {
    'canonical_evidence_documents': ('canonical_evidence_documents_manifest_artifact_idx', 'manifest_artifact_id'),
    'canonical_evidence_document_parts': ('canonical_evidence_document_parts_artifact_idx', 'artifact_id'),
    'canonical_parquet_scan_shards': ('canonical_parquet_scan_shards_artifact_idx', 'artifact_id'),
}
SCALE = 20_000


def claim_sql():
    statements = [node.value for node in ast.walk(ast.parse(inspect.getsource(owner)))
                  if isinstance(node, ast.Constant) and isinstance(node.value, str)
                  and 'AS removable' in node.value and 'FOR UPDATE SKIP LOCKED' in node.value]
    assert len(statements) == 1, 'must exercise the actual cleanup owner SQL'
    return statements[0]


def nodes(plan):
    yield plan
    for child in plan.get('Plans', ()):
        yield from nodes(child)


class CleanupReferenceIndexes(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.admin_dsn = os.environ['RECALL_DATABASE_URL']
        cls.database = 'recall_cleanup_indexes_' + uuid.uuid4().hex
        with psycopg.connect(cls.admin_dsn, autocommit=True) as admin:
            admin.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(cls.database)))
        cls.store = TrackedStore(make_conninfo(**(conninfo_to_dict(cls.admin_dsn) | {'dbname': cls.database})))
        cls.store.migrate()
        cls.temporary = tempfile.TemporaryDirectory(prefix='recall-cleanup-indexes-')
        cls.tenant, cls.source, cls.archive, cls.projection, cls.projector, _ = fixture(
            cls.store, Path(cls.temporary.name), count=1)
        with cls.store.connect() as connection:
            document = connection.execute('SELECT to_jsonb(d) AS value FROM canonical_evidence_documents d WHERE tenant_id=%s', (cls.tenant,)).fetchone()['value']
            part = connection.execute('SELECT to_jsonb(p) AS value FROM canonical_evidence_document_parts p WHERE tenant_id=%s LIMIT 1', (cls.tenant,)).fetchone()['value']
            # Full real catalog rows with identical tenant/source prefixes. The
            # old indexes cannot narrow negative probes by artifact identity.
            connection.execute('''INSERT INTO canonical_evidence_documents
                SELECT (jsonb_populate_record(NULL::canonical_evidence_documents,
                    %s::jsonb || jsonb_build_object(
                      'logical_document_id','ldoc_'||md5('document-'||n),
                      'native_parent_id','parent-'||n,
                      'evidence_id','evd_'||md5('evidence-'||n),
                      'manifest_artifact_id','art_'||md5('manifest-'||n),
                      'manifest_object_key','objects/00/'||md5('manifest-key-'||n)||md5('manifest-key2-'||n)
                    ))).* FROM generate_series(1,%s) n''', (json.dumps(document), SCALE))
            connection.execute('''INSERT INTO canonical_evidence_document_parts
                SELECT (jsonb_populate_record(NULL::canonical_evidence_document_parts,
                    %s::jsonb || jsonb_build_object(
                      'logical_document_id','ldoc_'||md5('document-'||n),
                      'artifact_id','art_'||md5('part-'||n),
                      'object_key','objects/00/'||md5('part-key-'||n)||md5('part-key2-'||n)
                    ))).* FROM generate_series(1,%s) n''', (json.dumps(part), SCALE))
            connection.execute('''INSERT INTO canonical_parquet_scan_shards(
                tenant_id,source_id,bucket_start,dataset,shard_index,generation_sha256,
                artifact_id,storage_backend,object_key,content_sha256,size_bytes,
                media_type,encryption,version_id,row_count,created_at)
                SELECT %s,%s,'2026-09-01','records',n,repeat('a',64),
                    'art_'||md5('shard-'||n),'s3',
                    'objects/00/'||md5('shard-key-'||n)||md5('shard-key2-'||n),
                    repeat('b',64),1,'application/vnd.apache.parquet','sse-s3','v1',1,now()
                FROM generate_series(1,%s) n''', (cls.tenant, cls.source, SCALE))
        cls.references = [cls.archive.delegate.put_raw(
            tenant_id=cls.tenant, source_id=cls.source, native_id='cleanup-'+str(n),
            payload=('obsolete-'+str(n)).encode(), media_type='application/json',
            created_at='2026-09-25T00:00:00Z') for n in range(8)]
        with cls.store.connect() as connection:
            cls.projector._enqueue_cleanup(connection, tuple(cls.references))
            for table in INDEXES:
                connection.execute(sql.SQL('ANALYZE {}').format(sql.Identifier(table)))
            connection.execute('ANALYZE canonical_evidence_cleanup_queue')
        cls.query = claim_sql()
        cls.params = (cls.tenant, cls.tenant, [], [], [], 8)

    @classmethod
    def tearDownClass(cls):
        cls.store.close()
        cls.temporary.cleanup()
        with psycopg.connect(cls.admin_dsn, autocommit=True) as admin:
            admin.execute(sql.SQL('DROP DATABASE {} WITH (FORCE)').format(sql.Identifier(cls.database)))

    def explain(self, connection):
        return connection.execute('EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) '+self.query,
                                  self.params).fetchone()['QUERY PLAN'][0]

    def decisions(self, connection):
        return {row['artifact_id']: row['removable'] for row in
                connection.execute(self.query, self.params).fetchall()}

    def test_selective_artifact_probes_preserve_exact_claims(self):
        with self.store.connect() as connection:
            with connection.transaction():
                indexed = self.explain(connection)
                candidate_decisions = self.decisions(connection)
                self.assertEqual(candidate_decisions, {r['artifact_id']: True for r in self.references})
                probes = {node.get('Relation Name'): node for node in nodes(indexed['Plan'])
                          if node.get('Relation Name') in INDEXES}
                for table, (index, artifact) in INDEXES.items():
                    with self.subTest(table=table):
                        self.assertIn(table, probes)
                        self.assertEqual(probes[table].get('Index Name'), index,
                                         'cleanup still scans the source catalog for an artifact')
                        self.assertIn(artifact, probes[table].get('Index Cond', ''))
                        self.assertEqual(probes[table].get('Rows Removed by Filter', 0), 0)
                if any(probes.get(table, {}).get('Index Name') != index
                       for table, (index, _) in INDEXES.items()):
                    return  # Subtests above already recorded behavioral failures.
                # Index removal lives in a rolled-back savepoint; compare stock
                # planner behavior, without disabling sequential/index scans.
                class RestoreIndexes(Exception):
                    pass
                try:
                    with connection.transaction():
                        for index, _ in INDEXES.values():
                            connection.execute(sql.SQL('DROP INDEX {}').format(sql.Identifier(index)))
                        baseline = self.explain(connection)
                        self.assertEqual(self.decisions(connection), candidate_decisions)
                        raise RestoreIndexes()
                except RestoreIndexes:
                    pass
                def facts(plan):
                    return dict(execution_ms=plan['Execution Time'],
                                shared_hit_blocks=plan['Plan'].get('Shared Hit Blocks', 0),
                                shared_read_blocks=plan['Plan'].get('Shared Read Blocks', 0),
                                filtered_rows=sum(node.get('Rows Removed by Filter', 0)*node.get('Actual Loops', 1)
                                                  for node in nodes(plan['Plan'])))
                self.assertGreater(facts(baseline)['filtered_rows'], SCALE)
                print(json.dumps(dict(scale=SCALE, cleanup_rows=8, baseline=facts(baseline), indexed=facts(indexed))), flush=True)

    def test_marker_refuses_missing_wrong_and_invalid_existing_indexes(self):
        migration = (RECALL / 'server/schema/074_record_cleanup_reference_indexes.sql').read_text()
        # Keep the real DO guard and INSERT, under a test-owned transaction so
        # each catalog fault rolls back and cannot leak into the scale proof.
        body = migration[migration.index('DO $$'):migration.rindex('COMMIT;')]
        index = next(iter(INDEXES.values()))[0]
        class RestoreFixture(Exception):
            pass
        for fault in ('missing', 'wrong_columns', 'invalid', 'not_ready'):
            with self.subTest(fault=fault), self.store.connect() as connection:
                try:
                    with connection.transaction():
                        connection.execute('DELETE FROM schema_migrations WHERE version=74')
                        if fault in ('missing', 'wrong_columns'):
                            connection.execute(sql.SQL('DROP INDEX {}').format(sql.Identifier(index)))
                            if fault == 'wrong_columns':
                                connection.execute(sql.SQL('CREATE INDEX {} ON canonical_evidence_documents (tenant_id, source_id, logical_document_id)').format(sql.Identifier(index)))
                        else:
                            # Disposable superuser fixture models an interrupted
                            # concurrent build; the runtime never edits pg_index.
                            flag = 'indisvalid' if fault == 'invalid' else 'indisready'
                            connection.execute(sql.SQL('UPDATE pg_index SET {}=false WHERE indexrelid=%s::regclass').format(sql.Identifier(flag)), (index,))
                        with self.assertRaisesRegex(psycopg.errors.RaiseException, 'cleanup reference indexes unavailable'):
                            with connection.transaction():
                                connection.execute(body)
                        self.assertIsNone(connection.execute('SELECT version FROM schema_migrations WHERE version=74').fetchone())
                        raise RestoreFixture()
                except RestoreFixture:
                    pass
        # Valid indexes allow the exact marker repeatedly, without rebuilding or
        # altering the mandatory-version contract.
        with self.store.connect() as connection:
            connection.execute(body)
            connection.execute(body)
            self.assertEqual(connection.execute('SELECT count(*) AS n FROM schema_migrations WHERE version=74').fetchone()['n'], 1)

    def test_each_live_reference_protects_exact_scoped_artifact(self):
        # Roll back fixture changes: the performance test always sees eight
        # negative lookups, regardless of unittest method ordering.
        class RestoreFixture(Exception):
            pass
        with self.store.connect() as connection:
            try:
                with connection.transaction():
                    for n, (table, (_, artifact)) in enumerate(INDEXES.items()):
                        connection.execute(sql.SQL('UPDATE {} SET {}=%s WHERE ctid=(SELECT ctid FROM {} WHERE tenant_id=%s AND source_id=%s LIMIT 1)').format(
                            sql.Identifier(table), sql.Identifier(artifact), sql.Identifier(table)),
                            (self.references[n]['artifact_id'], self.tenant, self.source))
                    # The guard deliberately protects the artifact even though
                    # this catalog row has a different storage descriptor/version.
                    expected = {r['artifact_id']: n >= 3 for n, r in enumerate(self.references)}
                    self.assertEqual(self.decisions(connection), expected)
                    for tenant, source in ((self.tenant, 'source:foreign'), ('tenant:foreign', self.source)):
                        insert_source(connection, tenant, 'principal:foreign', source)
                        connection.execute('''INSERT INTO canonical_evidence_documents
                            SELECT (jsonb_populate_record(NULL::canonical_evidence_documents,
                                to_jsonb(d)||jsonb_build_object('tenant_id',%s::text,'source_id',%s::text,
                                  'manifest_artifact_id',%s::text,'manifest_object_key',%s::text))).*
                            FROM canonical_evidence_documents d WHERE tenant_id=%s AND source_id=%s LIMIT 1''',
                            (tenant, source, self.references[3]['artifact_id'],
                             'objects/00/'+uuid.uuid4().hex+uuid.uuid4().hex, self.tenant, self.source))
                    self.assertEqual(self.decisions(connection), expected)
                    raise RestoreFixture()
            except RestoreFixture:
                pass


if __name__ == '__main__':
    unittest.main()
