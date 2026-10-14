#!/usr/bin/env python3
"""Real PostgreSQL/COPY/archive proofs for retiring diagnostic success rows."""
import os
import json
import time
from pathlib import Path
import tempfile
import sys
import unittest
import uuid

RECALL = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(RECALL), str(RECALL / 'server')]

from recall_server.archive import ArchiveCorruption, FilesystemArchiveStore
from recall_server.audit_retirement import AuditRetirement
from recall_server.db import BrainStore


class AuditRetirementTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.store = BrainStore(os.environ['RECALL_DATABASE_URL'])
        cls.store.migrate()

    @classmethod
    def tearDownClass(cls):
        cls.store.close()

    def setUp(self):
        suffix = uuid.uuid4().hex
        self.tenant, self.source = 'tenant:audit:' + suffix, 'source:audit:' + suffix
        self.cutoff = '2026-10-13T00:00:00+00:00'
        directory = tempfile.TemporaryDirectory(prefix='recall-audit-retirement-')
        self.addCleanup(directory.cleanup)
        self.archive = FilesystemArchiveStore(Path(directory.name), namespace_key=b'a' * 32)
        self.owner = AuditRetirement(self.store, self.archive)
        with self.store.connect() as c:
            for tenant, source in [(self.tenant, self.source), (self.tenant, self.source + ':other'),
                                   (self.tenant + ':other', self.source)]:
                c.execute('INSERT INTO brain_tenants(tenant_id) VALUES (%s) ON CONFLICT DO NOTHING', (tenant,))
                c.execute('INSERT INTO brain_principals(tenant_id,principal_id) VALUES (%s,%s) ON CONFLICT DO NOTHING', (tenant, 'principal:fixture'))
                c.execute('INSERT INTO canonical_sources(tenant_id,source_id,owner_principal_id) VALUES (%s,%s,%s)', (tenant, source, 'principal:fixture'))

    def add(self, audit_id, *, operation='ingest.commit', status='success', created='2026-10-12T00:00:00Z', tenant=None, source=None, duration=0.12345678912345678):
        with self.store.connect() as c:
            c.execute('''INSERT INTO canonical_audit_events
                (tenant_id,source_id,audit_id,operation,status,subject_sha256,item_count,byte_count,duration_ms,created_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''',
                (tenant or self.tenant, source or self.source, audit_id, operation, status,
                 'a' * 64, 1, 9223372036854775807, duration, created))

    def rows(self):
        with self.store.connect() as c:
            return c.execute('SELECT * FROM canonical_audit_events WHERE tenant_id=ANY(%s) ORDER BY tenant_id,source_id,audit_id', ([self.tenant, self.tenant + ':other'],)).fetchall()

    def prepare(self, **kwargs):
        return self.owner.archive_page(tenant_id=self.tenant, source_id=self.source, cutoff=self.cutoff, **kwargs)

    def test_single_copy_matches_old_csv_and_receipt_at_5k_and_50k(self):
        import csv
        import hashlib
        import io
        from contextlib import contextmanager
        from unittest.mock import patch
        from recall_server.audit_retirement import _copy, _select
        from recall_server.retirement_campaign import digest
        # Quoted identifiers and nullable CSV fields must remain byte-identical.
        with self.store.connect() as c:
            c.execute("""INSERT INTO canonical_audit_events
                (tenant_id,source_id,audit_id,operation,status,subject_sha256,
                 item_count,byte_count,duration_ms,created_at)
                SELECT %s,%s,'audit:'||lpad(n::text,8,'0')||E',"\\n',
                CASE WHEN n%%100=0 THEN 'forget.commit' ELSE 'ingest.commit' END,
                CASE WHEN n%%101=0 THEN 'failed' ELSE 'success' END,
                NULL,NULL,NULL,NULL,
                CASE WHEN n%%103=0 THEN '2026-10-14'::timestamptz
                     ELSE '2026-10-12'::timestamptz END
                FROM generate_series(1,50000)n""", (self.tenant,self.source))
        original_connection = self.owner._connection
        for limit in (5000, 50000):
            with self.subTest(limit=limit):
                with original_connection(snapshot=True) as c:
                    keys = [r['audit_id'] for r in c.execute(
                        'SELECT audit_id FROM canonical_audit_events WHERE tenant_id=%s '
                        'AND source_id=%s ORDER BY audit_id LIMIT %s',
                        (self.tenant,self.source,limit)).fetchall()]
                    eligible = [r['audit_id'] for r in c.execute(
                        "SELECT audit_id FROM canonical_audit_events WHERE tenant_id=%s "
                        "AND source_id=%s AND audit_id=ANY(%s) AND operation='ingest.commit' "
                        "AND status='success' AND created_at<%s ORDER BY audit_id",
                        (self.tenant,self.source,keys,self.cutoff)).fetchall()]
                    q,params = _select(self.tenant,self.source,eligible)
                    expected_raw = _copy(c,q,params)
                queries = []
                class Observed:
                    def __init__(inner, connection): inner.connection = connection
                    def execute(inner, query, params=()):
                        queries.append(str(query)); return inner.connection.execute(query,params)
                    def __getattr__(inner, key): return getattr(inner.connection,key)
                @contextmanager
                def observed(**kwargs):
                    with original_connection(**kwargs) as c: yield Observed(c)
                with patch.object(self.owner,'_connection',observed):
                    receipt = self.prepare(limit=limit)
                raw,actual_keys = self.owner._load(receipt)
                self.assertEqual(raw,expected_raw)
                self.assertEqual(actual_keys,eligible)
                self.assertEqual(len(list(csv.reader(io.StringIO(raw.decode(),newline='')))),len(eligible))
                expected = dict(contract='recall.ingest-audit-retirement.v1',
                    tenant_id=self.tenant,source_id=self.source,cutoff=self.cutoff,after=None,
                    next_after=keys[-1],scanned=limit,exhausted=False,row_count=len(eligible),
                    raw_bytes=len(expected_raw),raw_sha256=hashlib.sha256(expected_raw).hexdigest(),
                    artifact=receipt['artifact'])
                self.assertEqual(receipt,dict(expected,sha256=digest(expected)))
                self.assertEqual(sum('SELECT audit_id FROM canonical_audit_events' in q for q in queries),1,
                                 'archive must not fetch eligibility rows before copying them again')

    def test_single_copy_keeps_discovery_snapshot_across_concurrent_change(self):
        from contextlib import contextmanager
        from unittest.mock import patch
        self.add('audit:01'); self.add('audit:02',status='failed')
        expected = self.prepare()
        expected_raw,_ = self.owner._load(expected)
        original_connection = self.owner._connection
        changed = []
        class Observed:
            def __init__(inner, connection): inner.connection = connection
            def execute(inner, query, params=()):
                cursor = inner.connection.execute(query,params)
                if 'SELECT audit_id FROM canonical_audit_events WHERE' in str(query) and not changed:
                    class Discovery:
                        def fetchall(self_cursor):
                            rows = cursor.fetchall()
                            with self.store.connect() as writer:
                                writer.execute("UPDATE canonical_audit_events SET duration_ms=9,status='success' "
                                    "WHERE tenant_id=%s AND source_id=%s",(self.tenant,self.source))
                            changed.append(True)
                            return rows
                    return Discovery()
                return cursor
            def __getattr__(inner,key): return getattr(inner.connection,key)
        @contextmanager
        def observed(**kwargs):
            with original_connection(**kwargs) as c: yield Observed(c)
        with patch.object(self.owner,'_connection',observed): receipt = self.prepare()
        self.assertEqual(changed,[True])
        self.assertEqual(self.owner._load(receipt)[0],expected_raw)
        self.assertEqual(receipt['row_count'],1)
        with self.assertRaisesRegex(ValueError,'audit_rows_changed_or_partial'):
            self.owner.clear(receipt)

    def test_archive_clear_restore_preserves_all_other_rows(self):
        self.add('audit:ordinary')
        self.add('audit:null', duration=None)
        self.add('audit:failed', status='failed')
        self.add('audit:rejected', status='rejected')
        self.add('audit:forget', operation='forget.commit')
        self.add('audit:suppression', operation='ingest.slack_suppressed_deleted')
        self.add('audit:unknown', operation='future.unknown')
        self.add('audit:at-cutoff', created=self.cutoff)
        self.add('audit:later', created='2026-10-14T00:00:00Z')
        self.add('audit:source', source=self.source + ':other')
        self.add('audit:tenant', tenant=self.tenant + ':other')
        before = self.rows()
        receipt = self.prepare()
        self.assertEqual(receipt['row_count'], 2)
        self.assertEqual(receipt['scanned'], 9)
        self.assertEqual(self.rows(), before, 'archive must not clear database rows')
        result = self.owner.clear(receipt)
        self.assertEqual(result, {'status': 'cleared', 'rows': 2})
        retained = [r for r in before if r['audit_id'] not in ('audit:ordinary', 'audit:null')]
        self.assertEqual(self.rows(), retained)
        self.assertEqual(self.owner.inspect(receipt), {'status': 'absent', 'rows': 0, 'expected': 2})
        self.assertEqual(self.owner.restore(receipt), {'status': 'restored', 'rows': 2})
        self.assertEqual(self.rows(), before)
        self.assertEqual(self.owner.restore(receipt), {'status': 'restored', 'rows': 0})
        self.assertEqual(self.rows(), before)

    def test_unreviewed_schema_and_delete_side_effects_refuse_before_clear(self):
        self.add('audit:schema')
        receipt = self.prepare()
        with self.store.connect() as c:
            c.execute('ALTER TABLE canonical_audit_events ADD COLUMN future_payload text')
        try:
            with self.assertRaisesRegex(ValueError, 'audit_schema_changed'):
                self.owner.clear(receipt)
            self.assertEqual(len(self.rows()), 1)
        finally:
            with self.store.connect() as c:
                c.execute('ALTER TABLE canonical_audit_events DROP COLUMN future_payload')
        with self.store.connect() as c:
            c.execute('CREATE TABLE test_audit_child (tenant_id text,source_id text,audit_id text, '
                      'FOREIGN KEY(tenant_id,source_id,audit_id) REFERENCES canonical_audit_events ON DELETE CASCADE)')
            c.execute('INSERT INTO test_audit_child VALUES (%s,%s,%s)', (self.tenant,self.source,'audit:schema'))
        try:
            with self.assertRaisesRegex(ValueError, 'audit_delete_side_effects'):
                self.owner.clear(receipt)
            self.assertEqual(len(self.rows()), 1)
            with self.store.connect() as c:
                self.assertEqual(c.execute('SELECT count(*) AS n FROM test_audit_child').fetchone()['n'], 1)
        finally:
            with self.store.connect() as c:
                c.execute('DROP TABLE test_audit_child')

    def test_changed_partial_or_corrupt_page_cannot_clear_and_restore_is_atomic(self):
        self.add('audit:a')
        self.add('audit:b')
        receipt = self.prepare()
        with self.store.connect() as c:
            c.execute("UPDATE canonical_audit_events SET item_count=7 WHERE tenant_id=%s AND audit_id='audit:b'", (self.tenant,))
        changed = self.rows()
        with self.assertRaisesRegex(ValueError, 'audit_rows_changed_or_partial'):
            self.owner.clear(receipt)
        self.assertEqual(self.rows(), changed)
        with self.store.connect() as c:
            c.execute("DELETE FROM canonical_audit_events WHERE tenant_id=%s AND audit_id='audit:a'", (self.tenant,))
        partial = self.rows()
        with self.assertRaisesRegex(ValueError, 'audit_rows_changed_or_partial'):
            self.owner.clear(receipt)
        with self.assertRaisesRegex(ValueError, 'audit_restore_conflict'):
            self.owner.restore(receipt)
        self.assertEqual(self.rows(), partial, 'restoration of a must roll back when b differs')
        data = self.archive.root / receipt['artifact']['object_key'] / 'data'
        data.write_bytes(b'corrupt-copy')
        with self.assertRaises(ArchiveCorruption):
            self.owner.clear(receipt)
        self.assertEqual(self.rows(), partial)

    def test_sparse_pages_advance_over_retained_rows_and_restart_from_receipt(self):
        self.add('audit:01', operation='forget.commit')
        self.add('audit:02', status='failed')
        self.add('audit:03')
        self.add('audit:04', created=self.cutoff)
        first = self.prepare(limit=2)
        self.assertEqual((first['scanned'], first['row_count'], first['exhausted']), (2, 0, False))
        self.assertEqual(first['next_after'], 'audit:02')
        restarted = AuditRetirement(self.store, self.archive)
        second = restarted.archive_page(tenant_id=self.tenant, source_id=self.source,
                                         cutoff=self.cutoff, after=first['next_after'], limit=2)
        self.assertEqual((second['row_count'], second['exhausted']), (1, False))
        restarted.clear(second)
        self.assertEqual([r['audit_id'] for r in self.rows()], ['audit:01','audit:02','audit:04'])
        end = self.prepare(after=second['next_after'], limit=2)
        self.assertEqual((end['scanned'],end['exhausted']), (0,True))
        self.assertEqual(restarted.inspect(second), {'status':'absent','rows':0,'expected':1})
        self.assertEqual(restarted.clear(second), {'status':'absent','rows':0})

    def test_physical_reclamation_preserves_every_retained_row(self):
        # The pilot is an explicitly disposable relation, never a production command.
        # Retain a realistic 1% exceptional tail rather than using empty-table truncation.
        import psycopg
        from psycopg import sql
        from psycopg.rows import dict_row
        schema = 'audit_pilot_' + uuid.uuid4().hex
        with self.store.connect() as c:
            c.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(schema)))
            c.execute(sql.SQL('CREATE TABLE {}.canonical_audit_events '
                             '(LIKE public.canonical_audit_events INCLUDING ALL)').format(sql.Identifier(schema)))
        self.addCleanup(self.drop_schema, schema)
        dsn = os.environ['RECALL_DATABASE_URL']
        class PilotStore:
            def connect(inner):
                return psycopg.connect(dsn, options='-c search_path=' + schema, row_factory=dict_row)
        pilot = PilotStore()
        owner = AuditRetirement(pilot, self.archive)
        with pilot.connect() as c:
            c.execute("""INSERT INTO canonical_audit_events
                (tenant_id,source_id,audit_id,operation,status,subject_sha256,item_count,byte_count,duration_ms,created_at)
                SELECT %s,%s,'audit:'||lpad(n::text,8,'0'),
                CASE WHEN n%%100=0 THEN 'forget.commit' ELSE 'ingest.commit' END,
                'success',md5(n::text)||md5((n+1)::text),1,1000,0.12345678912345678,'2026-10-12'::timestamptz
                FROM generate_series(1,30000) AS n""", (self.tenant,self.source))
            c.execute('ANALYZE canonical_audit_events')
            retained = c.execute("SELECT * FROM canonical_audit_events WHERE operation='forget.commit' ORDER BY audit_id").fetchall()
            before = c.execute("SELECT pg_total_relation_size('canonical_audit_events') AS bytes,pg_current_wal_insert_lsn() AS wal").fetchone()
        started = time.monotonic()
        receipt = owner.archive_page(tenant_id=self.tenant,source_id=self.source,cutoff=self.cutoff,limit=50000)
        self.assertEqual(receipt['row_count'], 29700)
        self.assertEqual(owner.clear(receipt), {'status':'cleared','rows':29700})
        elapsed = time.monotonic()-started
        with pilot.connect() as c:
            logical = c.execute("SELECT pg_total_relation_size('canonical_audit_events') AS bytes,pg_wal_lsn_diff(pg_current_wal_insert_lsn(),%s) AS wal_bytes", (before['wal'],)).fetchone()
            self.assertEqual(c.execute('SELECT * FROM canonical_audit_events ORDER BY audit_id').fetchall(),retained)
        # AUTOCOMMIT is required by PostgreSQL VACUUM; this test supplies an isolated schema.
        with psycopg.connect(dsn, options='-c search_path=' + schema, autocommit=True, row_factory=dict_row) as c:
            c.execute('VACUUM (FULL, ANALYZE) canonical_audit_events')
            after = c.execute("SELECT pg_total_relation_size('canonical_audit_events') AS bytes").fetchone()['bytes']
            self.assertEqual(c.execute('SELECT * FROM canonical_audit_events ORDER BY audit_id').fetchall(),retained)
        self.assertGreaterEqual(logical['bytes'],before['bytes'] * 0.9,
                                'logical deletion must not be mistaken for physical reclamation')
        self.assertLess(after,before['bytes'] * 0.15)
        print(json.dumps(dict(pilot='audit_physical_reclamation', rows=30000, retired=29700,retained=300,
                              before_bytes=before['bytes'],after_delete_bytes=logical['bytes'],after_rewrite_bytes=after,
                              archive_bytes=receipt['artifact']['size_bytes'],uncompressed_bytes=receipt['raw_bytes'],
                              delete_wal_bytes=int(logical['wal_bytes']),archive_and_clear_seconds=elapsed)),flush=True)
        owner.restore(receipt)
        with pilot.connect() as c:
            self.assertEqual(c.execute('SELECT count(*) AS n FROM canonical_audit_events').fetchone()['n'],30000)


    def test_uncommitted_clear_rolls_back_and_lost_committed_result_is_observed(self):
        from contextlib import contextmanager
        self.add('audit:crash')
        receipt = self.prepare()
        store = self.store
        class InterruptedStore:
            @contextmanager
            def connect(inner):
                with store.connect() as c:
                    yield c
                    raise ConnectionError('synthetic disconnect before commit')
        with self.assertRaises(ConnectionError):
            AuditRetirement(InterruptedStore(),self.archive).clear(receipt)
        self.assertEqual(self.owner.inspect(receipt),{'status':'present','rows':1,'expected':1})
        self.owner.clear(receipt)  # Simulate losing this return value after COMMIT.
        restarted = AuditRetirement(self.store,self.archive)
        self.assertEqual(restarted.inspect(receipt),{'status':'absent','rows':0,'expected':1})
        self.assertEqual(self.rows(),[])

    def test_concurrent_restores_are_identical_without_overwriting_another_source(self):
        from concurrent.futures import ThreadPoolExecutor
        import threading
        self.add('audit:concurrent')
        self.add('audit:concurrent',source=self.source+':other',duration=0.5)
        before = self.rows()
        receipt = self.prepare()
        self.owner.clear(receipt)
        barrier = threading.Barrier(2)
        def restore(_):
            barrier.wait(timeout=5)
            return AuditRetirement(self.store,self.archive).restore(receipt)
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(restore,range(2)))
        self.assertEqual(sum(r['rows'] for r in results),1)
        self.assertEqual(self.rows(),before)


    def test_first_page_does_not_skip_a_valid_empty_primary_key(self):
        # The existing SQL contract allows empty audit_id, even though ordinary
        # writers use hashes. Completion cannot silently assume a stronger schema.
        self.add('')
        receipt = self.prepare(limit=1)
        self.assertEqual(receipt['row_count'],1)
        self.assertEqual(receipt['next_after'],'')
        self.owner.clear(receipt)
        self.assertEqual(self.rows(),[])


    def test_assembled_job_boots_real_store_and_runs_archive_clear_inspect_restore(self):
        import hashlib
        import importlib
        import runpy
        import subprocess
        from recall_server.retirement_jobs import decode_result
        operator = runpy.run_path(str(RECALL/'scripts/retire_ingest_audits.py'))
        self.add('audit:packaged')
        before = self.rows()
        config = dict(commit='a'*40,service_id='srv:synthetic',cutoff_sealed=True,
                      scope=dict(tenant_id=self.tenant,source_id=self.source,cutoff=self.cutoff),
                      owner_modules={})
        for name in ('audit_retirement','archive','archive_runtime','db'):
            module = importlib.import_module('recall_server.'+name)
            config['owner_modules'][module.__name__] = hashlib.sha256(Path(module.__file__).read_bytes()).hexdigest()
        environment = dict(PATH=os.environ['PATH'],PYTHONPATH=str(RECALL)+os.pathsep+str(RECALL/'server'),
                           RECALL_DATABASE_URL=os.environ['RECALL_DATABASE_URL'],
                           RENDER_GIT_COMMIT=config['commit'],RENDER_SERVICE_ID=config['service_id'],
                           RECALL_SEARCH_PLANE='turbopuffer',RECALL_TPUF_API_KEY='synthetic-ci-key')
        # Only the external object-storage factory is replaced with a real local
        # archive. The complete packaged body and BrainStore bootstrap execute.
        bootstrap = ('from pathlib import Path\n'
                     'from recall_server.archive import FilesystemArchiveStore\n'
                     'import recall_server.archive_runtime as runtime\n'
                     'runtime.build_evidence_archive_store=lambda **kwargs: FilesystemArchiveStore('
                     + 'Path(' + repr(str(self.archive.root)) + '),namespace_key=b"a"*32)\n')
        def run(payload):
            body,pin = operator['package_body'](config,payload)
            result = subprocess.run([sys.executable,'-c',bootstrap+body],env=environment,
                                    capture_output=True,text=True,timeout=30)
            self.assertEqual(result.returncode,0,result.stderr)
            return decode_result([json.loads(line) for line in result.stdout.splitlines()],pin)
        receipt = run(dict(operation='archive',after=None,limit=10))
        self.assertEqual(receipt['row_count'],1)
        self.assertEqual(run(dict(operation='clear',receipt=receipt)),{'status':'cleared','rows':1})
        self.assertEqual(run(dict(operation='inspect',receipt=receipt)),{'status':'absent','rows':0,'expected':1})
        self.assertEqual(run(dict(operation='restore',receipt=receipt)),{'status':'restored','rows':1})
        self.assertEqual(self.rows(),before)

    def drop_schema(self, schema):
        from psycopg import sql
        with self.store.connect() as c:
            c.execute(sql.SQL('DROP SCHEMA {} CASCADE').format(sql.Identifier(schema)))


if __name__ == '__main__':
    unittest.main()
