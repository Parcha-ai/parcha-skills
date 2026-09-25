#!/usr/bin/env python3
"""Real PostgreSQL ordering oracle and bounded aggregate-work regression."""
from contextlib import nullcontext
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest

import psycopg
from psycopg.rows import dict_row

SERVER = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(SERVER), str(SERVER.parent)]
from recall_server.passage_index import CanonicalPassageProjector  # noqa: E402
from recall_server.passage_projection import DEFAULT_PASSAGE_POLICY  # noqa: E402


def nodes(plan):
    yield plan
    for child in plan.get('Plans', ()):
        yield from nodes(child)


class SetAdmission(unittest.TestCase):
    def setUp(self):
        self.connection = psycopg.connect(os.environ['RECALL_DATABASE_URL'], row_factory=dict_row)
        self.addCleanup(self.connection.close)
        self.connection.execute("SET statement_timeout='30s'")
        self.connection.execute('''CREATE TEMP TABLE canonical_passage_projection_queue (
            tenant_id text,source_id text,logical_document_id text,revision integer,
            generation bigint,changed_at timestamptz,notification_queued_at timestamptz,
            PRIMARY KEY(tenant_id,source_id,logical_document_id))''')
        self.connection.execute('''CREATE INDEX ON canonical_passage_projection_queue
            (tenant_id,changed_at,source_id,logical_document_id)''')
        self.connection.execute('''CREATE TEMP TABLE canonical_evidence_documents (
            tenant_id text,source_id text,logical_document_id text,revision integer,
            document_content_sha256 text DEFAULT 'document',manifest_artifact_id text DEFAULT 'manifest',
            manifest_storage_backend text DEFAULT 's3',manifest_object_key text DEFAULT 'manifest/key',
            manifest_content_sha256 text DEFAULT 'hash',manifest_size_bytes bigint DEFAULT 10,
            manifest_media_type text DEFAULT 'application/json',manifest_encryption text DEFAULT 'sse-s3',
            manifest_version_id text DEFAULT 'v1',created_at timestamptz DEFAULT now(),
            PRIMARY KEY(tenant_id,source_id,logical_document_id),
            UNIQUE(tenant_id,source_id,logical_document_id,revision))''')
        self.connection.execute('''CREATE TEMP TABLE canonical_evidence_document_parts (
            tenant_id text,source_id text,logical_document_id text,revision integer,part_ordinal integer,
            artifact_id text,storage_backend text DEFAULT 's3',object_key text DEFAULT 'part/key',
            content_sha256 text DEFAULT 'hash',size_bytes bigint,media_type text DEFAULT 'application/jsonl',
            encryption text DEFAULT 'sse-s3',version_id text DEFAULT 'v1',created_at timestamptz DEFAULT now(),
            PRIMARY KEY(tenant_id,source_id,logical_document_id,revision,part_ordinal))''')
        self.projector = CanonicalPassageProjector(
            SimpleNamespace(connect=lambda: nullcontext(self.connection)), None,
            policy=DEFAULT_PASSAGE_POLICY)
        self.now = datetime.now(timezone.utc)
        self.expected = []

    def add(self, name, sizes, *, tenant='a', source='x', age=60, notified=None,
            revision=1, evidence_revision=None, evidence=True):
        changed = self.now - timedelta(seconds=age)
        notification = None if notified is None else self.now - timedelta(seconds=notified)
        key = (tenant, source, name)
        self.connection.execute('''INSERT INTO canonical_passage_projection_queue VALUES
            (%s,%s,%s,%s,7,%s,%s)''', (*key, revision, changed, notification))
        er = revision if evidence_revision is None else evidence_revision
        if evidence:
            self.connection.execute('''INSERT INTO canonical_evidence_documents
                (tenant_id,source_id,logical_document_id,revision) VALUES (%s,%s,%s,%s)''', (*key, er))
        for ordinal, size in enumerate(sizes):
            self.connection.execute('''INSERT INTO canonical_evidence_document_parts
                (tenant_id,source_id,logical_document_id,revision,part_ordinal,artifact_id,size_bytes)
                VALUES (%s,%s,%s,%s,%s,%s,%s)''', (*key, er, ordinal, f'{name}-{ordinal}', size))
        self.expected.append(dict(key=key, revision=revision, changed=changed,
            notification=notification, size=sum(sizes) if er == revision else 0,
            visible=evidence and er == revision and bool(sizes), parts=tuple(sizes)))

    def oracle(self, tenant, limit, notified):
        rows = [r for r in self.expected if tenant is None or r['key'][0] == tenant]
        def priority(r):
            stamp = r['notification'] if notified else None
            return (stamp is None, stamp or self.now, not (r['changed'] < self.now-timedelta(minutes=5)),
                    r['size'], r['changed'], *r['key'])
        # Missing evidence/parts consume admission capacity, just as the original inner LIMIT.
        return [r for r in sorted(rows, key=priority)[:limit] if r['visible']]

    def assert_selection(self, *, tenant, limit, notified):
        self.projector._prefer_notification_admission = notified
        got = self.projector._pending(tenant_id=tenant, limit=limit)
        expected = self.oracle(tenant, limit, notified)
        self.assertEqual([(r.tenant_id,r.source_id,r.logical_document_id) for r in got],
                         [r['key'] for r in expected])
        for actual, wanted in zip(got, expected):
            self.assertEqual(actual.revision, wanted['revision'])
            self.assertEqual(actual.generation, 7)
            self.assertEqual(actual.changed_at, wanted['changed'])
            self.assertEqual(tuple(p['size_bytes'] for p in actual.part_references), wanted['parts'])
            self.assertEqual(tuple(p['artifact_id'] for p in actual.part_references),
                             tuple(f"{wanted['key'][2]}-{n}" for n in range(len(wanted['parts']))))
            self.assertEqual(actual.manifest_reference['artifact_id'], 'manifest')

    def test_frozen_age_notification_ties_scope_multipart_and_limits(self):
        self.add('aged-large', [700,900], age=900)
        self.add('young-tiny', [1], age=60)
        self.add('notify-first', [300], notified=300, age=30)
        self.add('notify-later', [1], notified=100, age=30)
        self.add('tie-a', [4,6], age=600)
        self.add('tie-b', [10], age=600)
        self.add('tie-a', [10], source='y', age=600)
        self.add('tie-a', [1], tenant='b', source='x', age=600)
        for tenant in ('a','b',None,'absent'):
            for notified in (False,True):
                for limit in (1,3,10):
                    with self.subTest(tenant=tenant, notified=notified, limit=limit):
                        self.assert_selection(tenant=tenant,limit=limit,notified=notified)

    def test_missing_revision_parts_and_evidence_keep_prejoin_limit(self):
        self.add('no-parts', [], age=800)
        self.add('wrong-revision', [1], revision=2, evidence_revision=1, age=700)
        self.add('no-evidence', [2], evidence=False, age=600)
        self.add('valid', [10,20], age=600)
        for limit in (1,2,3,4,10):
            for notified in (False,True):
                self.assert_selection(tenant='a',limit=limit,notified=notified)
        self.assertEqual(self.projector._pending(tenant_id='a',limit=3), ())

    def test_70000_queue_scoring_avoids_per_parent_subplans(self):
        self.connection.execute('''INSERT INTO canonical_passage_projection_queue
            SELECT 'scale','s','p-'||n,1,1,now()-interval '1 day',NULL
            FROM generate_series(1,70000)n''')
        self.connection.execute('''INSERT INTO canonical_evidence_documents
            (tenant_id,source_id,logical_document_id,revision)
            SELECT tenant_id,source_id,logical_document_id,revision
            FROM canonical_passage_projection_queue''')
        self.connection.execute('''INSERT INTO canonical_evidence_document_parts
            (tenant_id,source_id,logical_document_id,revision,part_ordinal,artifact_id,size_bytes)
            SELECT tenant_id,source_id,logical_document_id,revision,0,'artifact-'||logical_document_id,100
            FROM canonical_passage_projection_queue''')
        # Unrelated parts must not alter size totals or be joined into the admitted metadata.
        self.connection.execute('''INSERT INTO canonical_evidence_document_parts
            (tenant_id,source_id,logical_document_id,revision,part_ordinal,artifact_id,size_bytes)
            SELECT 'unrelated','s','p-'||n,1,0,'unrelated-'||n,500
            FROM generate_series(1,140000)n''')
        for table in ('canonical_passage_projection_queue','canonical_evidence_documents','canonical_evidence_document_parts'):
            self.connection.execute('ANALYZE '+table)
        query = next(v for v in CanonicalPassageProjector._pending.__code__.co_consts
                     if isinstance(v,str) and 'SELECT queue.tenant_id' in v)
        repetitions = []
        for notified in (False,True):
            params=(notified,'scale','scale',10)
            # Warm reads, then retain actual plan attribution without a fragile time threshold.
            self.connection.execute(query,params).fetchall()
            result=self.connection.execute('EXPLAIN(ANALYZE,BUFFERS,FORMAT JSON) '+query,params).fetchone()['QUERY PLAN'][0]
            repeated=max((n['Actual Loops'] for n in nodes(result['Plan'])
                          if n.get('Parent Relationship')=='SubPlan'),default=0)
            print(json.dumps(dict(notification=notified, execution_ms=result['Execution Time'],
                shared_hits=result['Plan'].get('Shared Hit Blocks',0),
                local_hits=result['Plan'].get('Local Hit Blocks',0),
                repeated_subplan_loops=repeated)),flush=True)
            repetitions.append(repeated)
            self.projector._prefer_notification_admission=notified
            got=self.projector._pending(tenant_id='scale',limit=10)
            self.assertEqual([r.logical_document_id for r in got],
                             sorted('p-'+str(n) for n in range(1,70001))[:10])
            self.assertTrue(all(len(r.part_references)==1 for r in got))
        self.assertLess(max(repetitions),100, 'queue-wide correlated aggregation repeats before LIMIT10')


if __name__=='__main__':
    unittest.main()
