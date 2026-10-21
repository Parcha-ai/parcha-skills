#!/usr/bin/env python3
"""Exact admission semantics and access cost with a selective optional index."""
import ast
import hashlib
import json
import time
import unittest
from pathlib import Path

from e2e_worker_typed_admission import WorkerTypedAdmission
from recall_server import passage_index


METHOD = next(n for n in ast.walk(ast.parse(Path(passage_index.__file__).read_text()))
              if isinstance(n, ast.FunctionDef) and n.name == 'admit_typed_pending')
NATIVE = next(n.value for n in ast.walk(METHOD) if isinstance(n, ast.Constant)
              and isinstance(n.value, str) and n.value.startswith('WITH outstanding'))
CAPACITY = NATIVE.split('), candidates AS (')[0] + ') SELECT count(*) AS n FROM outstanding'
INDEX = 'canonical_passage_documents_upgrade_idx'
DDL = f'''CREATE INDEX {INDEX}
    ON canonical_passage_documents(tenant_id,source_id,logical_document_id)
    WHERE requested_extraction_version<>applied_extraction_version'''


def nodes(plan):
    yield plan
    for child in plan.get('Plans', []):
        yield from nodes(child)


class TypedUpgradeCapacityIndex(WorkerTypedAdmission):
    def seed(self, total=12000):
        self.legacy()
        with self.store.connect() as c:
            c.execute('''INSERT INTO canonical_sources
                SELECT (jsonb_populate_record(NULL::canonical_sources,
                    to_jsonb(s)||jsonb_build_object('source_id','claude:'||s.source_id))).*
                FROM canonical_sources s WHERE tenant_id=%s AND source_id=%s''',
                (self.tenant,self.source))
            c.execute('''CREATE TEMP TABLE capacity_seed AS
                SELECT i,'ldoc_'||md5(i::text) AS logical_document_id
                FROM generate_series(1,%s::integer) i''', (total,))
            c.execute('''INSERT INTO canonical_evidence_documents
                SELECT (jsonb_populate_record(NULL::canonical_evidence_documents,
                    to_jsonb(e)||jsonb_build_object(
                        'source_id',CASE WHEN s.i%%3=0 THEN 'claude:'||e.source_id ELSE e.source_id END,
                        'logical_document_id',s.logical_document_id,
                        'native_parent_id','capacity-parent-'||s.i,
                        'evidence_id','evd_'||md5(s.i::text),
                        'manifest_version_id','capacity-version-'||s.i))).*
                FROM capacity_seed s CROSS JOIN LATERAL (
                    SELECT * FROM canonical_evidence_documents WHERE tenant_id=%s LIMIT 1
                ) e''', (self.tenant,))
            c.execute('''INSERT INTO canonical_passage_documents
                SELECT (jsonb_populate_record(NULL::canonical_passage_documents,
                    to_jsonb(p)||jsonb_build_object(
                        'source_id',CASE WHEN s.i%%3=0 THEN 'claude:'||p.source_id ELSE p.source_id END,
                        'logical_document_id',s.logical_document_id,
                        'policy_fingerprint',CASE WHEN s.i%%2=0 THEN repeat('d',64) ELSE p.policy_fingerprint END,
                        'passage_count',0,'append_checkpoint',NULL))).*
                FROM capacity_seed s CROSS JOIN LATERAL (
                    SELECT * FROM canonical_passage_documents WHERE tenant_id=%s LIMIT 1
                ) p''', (self.tenant,))
            c.execute('''INSERT INTO canonical_passage_projection_queue(
                tenant_id,source_id,logical_document_id,revision,generation,reason)
                SELECT p.tenant_id,p.source_id,p.logical_document_id,p.revision,1,
                    CASE WHEN s.i<=3840 THEN 'backfill' ELSE 'certification' END
                FROM capacity_seed s JOIN canonical_passage_documents p USING(logical_document_id)
                WHERE p.tenant_id=%s AND s.i<=8000''', (self.tenant,))
            c.execute('DROP TABLE capacity_seed')

    def shape(self, qualifying):
        with self.store.connect() as c:
            c.execute('''UPDATE canonical_passage_documents
                SET requested_extraction_version='legacy',applied_extraction_version='legacy'
                WHERE tenant_id=%s''', (self.tenant,))
            # Some nonqueued and certification-only documents also need an
            # upgrade. The queue predicate must still exclude them.
            c.execute('''UPDATE canonical_passage_documents
                SET requested_extraction_version=CASE WHEN policy_fingerprint=repeat('d',64)
                    THEN 'tool_groups_v1' ELSE 'codex_tool_groups_v1' END
                WHERE tenant_id=%s AND logical_document_id=ANY(%s::text[])''',
                (self.tenant, ['ldoc_'+hashlib.md5(str(i).encode(), usedforsecurity=False).hexdigest() for i in
                              list(range(1, qualifying+1))+list(range(4000,4050))+list(range(9000,9050))]))
            for table in ('canonical_passage_documents','canonical_passage_projection_queue',
                          'canonical_evidence_documents','canonical_sources','canonical_source_grants'):
                c.execute('ANALYZE '+table)

    def capacity(self, c, limit=100):
        return self.store._execute_bounded(c,CAPACITY,(self.tenant,limit),time.monotonic()+3).fetchone()['n']

    def full(self, c, limit=100):
        args=(self.tenant,limit,self.passages.policy.fingerprint,limit,self.tenant,None,None,limit)
        return self.store._execute_bounded(c,NATIVE,args,time.monotonic()+3).fetchall()

    def profile(self, c, statement, args):
        p=self.store._execute_bounded(c,'EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) '+statement,
                                      args,time.monotonic()+3).fetchone()['QUERY PLAN'][0]
        return dict(execution_ms=p['Execution Time'],planning_ms=p['Planning Time'],
            nodes=[{k:n[k] for k in ('Node Type','Relation Name','Index Name','Actual Rows',
                'Actual Loops','Rows Removed by Filter','Shared Hit Blocks','Shared Read Blocks') if k in n}
                for n in nodes(p['Plan'])])

    def install_index(self):
        with self.store.connect() as c:
            self.assertIsNone(c.execute('SELECT to_regclass(%s) AS r',(INDEX,)).fetchone()['r'])
            c.execute(DDL)
        self.addCleanup(self.drop_index)

    def drop_index(self):
        with self.store.connect() as c:
            c.execute('DROP INDEX '+INDEX)

    def test_original_capacity_and_full_nomination_parity_dense_sparse(self):
        self.seed()
        before={}
        records=[]
        args=(self.tenant,100,self.passages.policy.fingerprint,100,self.tenant,None,None,100)
        for state,count in (('sparse',49),('dense',3000),('empty',0)):
            self.shape(count)
            with self.store.connect() as c:
                before[state]=self.full(c)
                self.assertEqual(self.capacity(c),min(100,count))
                records.append(dict(index=False,state=state,
                    capacity=self.profile(c,CAPACITY,(self.tenant,100)),
                    full_nomination=self.profile(c,NATIVE,args)))
        self.install_index()
        for state,count in (('sparse',49),('dense',3000),('empty',0)):
            self.shape(count)
            with self.store.connect() as c:
                self.assertEqual(self.full(c),before[state])
                self.assertEqual(self.capacity(c),min(100,count))
                measured=self.profile(c,CAPACITY,(self.tenant,100))
                records.append(dict(index=True,state=state,capacity=measured,
                    full_nomination=self.profile(c,NATIVE,args)))
                if state=='sparse':
                    self.assertIn(INDEX,{n.get('Index Name') for n in measured['nodes']})
        print('CAPACITY_PROFILE '+json.dumps(records,sort_keys=True),flush=True)

    def test_counts_and_index_membership_follow_requested_applied_updates(self):
        self.seed(total=10000)
        self.install_index()
        for count in (0,1,99,100,101):
            self.shape(count)
            with self.store.connect() as c:
                for limit in (1,100):
                    self.assertEqual(self.capacity(c,limit),min(limit,count))
                # Completion changes the predicate immediately, not through a
                # separately maintained queue counter.
                c.execute('''UPDATE canonical_passage_documents SET applied_extraction_version=requested_extraction_version
                    WHERE tenant_id=%s''',(self.tenant,))
                self.assertEqual(self.capacity(c),0)


if __name__=='__main__':
    suite=unittest.TestSuite(TypedUpgradeCapacityIndex(n) for n in TypedUpgradeCapacityIndex.__dict__ if n.startswith('test_'))
    raise SystemExit(not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful())
