#!/usr/bin/env python3
"""Actual cursor transport parity, bounded metadata, and cancellation."""
# ruff: noqa: E402
from contextlib import nullcontext
import hashlib
import json
import os
import resource
from pathlib import Path
import sys
import tempfile
import time
import unittest
import weakref
from unittest.mock import patch

import psycopg
from psycopg import sql

SERVER = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(SERVER.parent), str(SERVER)]
from e2e_archive_reprojection import fixture
from e2e_logical_source_integrity import TrackedStore
from recall_server import parent_chunk_proof as proof
from recall_server.chunk_retirement import ParentRetirementLimits, ChunkRetirementError
from recall_server.db import SearchDeadlineExceeded


class CaptureBatching(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.store = TrackedStore(os.environ['RECALL_DATABASE_URL'])
        cls.store.migrate()
        cls.root = tempfile.TemporaryDirectory()
        cls.tenant, cls.source, cls.archive, *_ = fixture(cls.store, Path(cls.root.name), count=257)
        cls.scope = (cls.tenant, cls.source, 'session')

    @classmethod
    def tearDownClass(cls):
        cls.store.close()
        cls.root.cleanup()

    def capture(self, *, batch=None, deadline=True, delay=0, cancel=False, limits=None, timeout=30):
        fetches = []
        original_execute = psycopg.Cursor.execute
        original_fetchmany = psycopg.ServerCursor.fetchmany
        def execute(cursor, query, params=None, **kwargs):
            rendered = query.as_string(cursor.connection) if isinstance(query, sql.Composable) else str(query)
            if 'FETCH FORWARD ' in rendered and 'retirement_metadata' in rendered:
                fetches.append(rendered)
                time.sleep(delay)  # Explicit simulated network RTT; not production measurement.
                if cancel:
                    query = rendered.replace('FETCH FORWARD ', 'SELECT pg_sleep(1); FETCH FORWARD ')
            return original_execute(cursor, query, params, **kwargs)
        def fetchmany(cursor, size=0):
            if cursor.name == 'retirement_metadata':
                fetches.append(size)
            return original_fetchmany(cursor, size)
        started = time.monotonic()
        with patch.object(proof, '_CAPTURE_FETCH_ROWS', batch, create=True) if batch else nullcontext():
            with patch.object(psycopg.Cursor, 'execute', execute), patch.object(psycopg.ServerCursor, 'fetchmany', fetchmany):
                with proof.ParentMetadataSpool(max_bytes=256 * 1024**2) as spool:
                    result = proof._capture(self.store, spool, self.scope, limits or ParentRetirementLimits(),
                                            time.monotonic() + (.03 if cancel else timeout) if deadline else None)
                    digest = hashlib.sha256()
                    payload_bytes = 0
                    for (payload,) in spool.index.execute('SELECT payload FROM documents ORDER BY document'):
                        digest.update(payload)
                        payload_bytes += len(payload)
                    rows = result[2:]
        return dict(digest=digest.hexdigest(), rows=rows, payload_bytes=payload_bytes,
                    peak_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
                    fetches=len(fetches), elapsed_ms=(time.monotonic() - started) * 1000)

    def test_257_rows_use_two_data_fetches_plus_eof(self):
        result = self.capture()
        self.assertEqual(result['rows'], (257, 257))
        self.assertEqual(result['fetches'], 3)

    def test_same_spool_with_and_without_deadline_and_old_batch(self):
        results = [self.capture(batch=32), self.capture(batch=128), self.capture(), self.capture(deadline=False)]
        self.assertEqual([r['fetches'] for r in results], [10, 4, 3, 3])
        self.assertEqual(len({r['digest'] for r in results}), 1)
        self.assertEqual(len({r['payload_bytes'] for r in results}), 1)
        self.assertFalse(self.archive.reads)

    def test_large_bodies_do_not_enter_metadata_spool(self):
        with self.store.connect() as connection:
            connection.execute("UPDATE canonical_chunks SET text_redacted=repeat('private body sentinel ',16384) WHERE tenant_id=%s AND document_id=(SELECT min(document_id) FROM canonical_documents WHERE tenant_id=%s)", (self.tenant, self.tenant))
        try:
            old, new = self.capture(batch=32), self.capture()
            self.assertEqual(old['digest'], new['digest'])
            self.assertLess(new['payload_bytes'], 512 * 1024)
        finally:
            with self.store.connect() as connection:
                connection.execute("UPDATE canonical_chunks c SET text_redacted=d.text_redacted FROM canonical_documents d WHERE c.tenant_id=d.tenant_id AND c.source_id=d.source_id AND c.document_id=d.document_id AND c.tenant_id=%s", (self.tenant,))

    def test_wide_metadata_parity_and_existing_row_budget(self):
        # Near the existing 1MiB row limit, without hydrating any body text.
        def widen(size):
            with self.store.connect() as connection:
                connection.execute("UPDATE canonical_events SET canonical_redacted=jsonb_set(canonical_redacted,'{type}',to_jsonb(repeat('w',%s))) WHERE tenant_id=%s AND native_id IN (SELECT native_id FROM canonical_documents WHERE tenant_id=%s ORDER BY native_id LIMIT 129)", (size, self.tenant, self.tenant))
        try:
            widen(900 * 1024)
            old, new = self.capture(batch=32), self.capture()
            self.assertEqual(old['digest'], new['digest'])
            self.assertGreater(new['payload_bytes'], 129 * 900 * 1024)
            print(json.dumps(dict(contract='capture-wide-metadata.v1', wide_rows=129,
                                  type_bytes=900 * 1024, old=old, new=new), sort_keys=True))
            widen(1024 * 1024)
            for batch in (32, 128, 256):
                with self.subTest(batch=batch), self.assertRaises(ChunkRetirementError):
                    self.capture(batch=batch)
        finally:
            with self.store.connect() as connection:
                connection.execute("UPDATE canonical_events SET canonical_redacted=jsonb_set(canonical_redacted,'{type}',to_jsonb('assistant'::text)) WHERE tenant_id=%s", (self.tenant,))
        self.assertFalse(self.archive.reads)

    def test_real_fetch_timeout_rolls_back_for_both_batches(self):
        for batch in (32, 128, 256):
            with self.subTest(batch=batch), self.assertRaises((SearchDeadlineExceeded, psycopg.errors.QueryCanceled)):
                self.capture(batch=batch, cancel=True)
            self.assertEqual(self.store.active_connections, 0)
        self.assertFalse(self.archive.reads)
        self.assertEqual(self.capture()['rows'], (257, 257))

    def test_chunk_dense_complete_metadata_and_archive_proof(self):
        from e2e_receipt_membership import ReceiptMembershipTests
        fixture = ReceiptMembershipTests('test_exact_metadata_survives_verified_singleton_row_removal_only_when_enabled')
        fixture.store = self.store
        fixture.setUp()
        previous_scope, previous_archive = self.scope, self.archive
        try:
            # Actual canonical writer boundaries and long valid receipts. Handmade
            # boundaries are correctly excluded by the full archive proof.
            for index in range(257):
                fixture.ingest('dense/'+'p'*120+str(index),'x'*(512*1024))
            report = fixture.projector.project_pending(tenant_id=fixture.tenant,batch_size=1,max_batches=1,upload_concurrency=1)
            self.assertEqual(report['failed'],0)
            with self.store.connect() as connection:
                for table in ('canonical_events','canonical_documents','canonical_chunks'):
                    connection.execute('ANALYZE '+table)
            self.scope = fixture.tenant, fixture.source, 'session'
            self.archive = fixture.archive
            with patch.object(self.store,'verified_singleton_receipts',True):
                old = self.capture(batch=128,timeout=60)
                new = self.capture(timeout=60)
                self.assertEqual(old['digest'],new['digest'])
                self.assertEqual(new['rows'][0],257)
                self.assertGreater(new['rows'][1],4000)
                with proof.prove_parent_chunks(self.store,self.archive,scope=self.scope,limits=ParentRetirementLimits(),deadline_at=time.monotonic()+60) as complete:
                    self.assertEqual(complete['eligible_documents'],257)
            print(json.dumps(dict(contract='capture-dense-metadata.v1',old=old,new=new),sort_keys=True))
        finally:
            self.scope, self.archive = previous_scope, previous_archive
            fixture.doCleanups()

    def test_consumed_batch_released_before_next_real_fetch(self):
        from recall_server import receipt_membership
        class Members(dict):
            pass
        class Rows(list):
            pass
        references = []
        fetch_cursors = set()
        original_execute, original_fetchall = psycopg.Cursor.execute, psycopg.Cursor.fetchall
        original_member = receipt_membership.verified_document_chunks
        def member(*args, **kwargs):
            result = Members(original_member(*args, **kwargs))
            references.append(weakref.ref(result))
            return result
        def execute(cursor, query, params=None, **kwargs):
            rendered = query.as_string(cursor.connection) if isinstance(query, sql.Composable) else str(query)
            if 'FETCH FORWARD ' in rendered and 'retirement_metadata' in rendered:
                self.assertTrue(all(ref() is None for ref in references),
                                'previous decoded batch survives into the next FETCH')
                fetch_cursors.add(id(cursor))
            return original_execute(cursor, query, params, **kwargs)
        def fetchall(cursor):
            rows = original_fetchall(cursor)
            if id(cursor) in fetch_cursors:
                fetch_cursors.remove(id(cursor))
                rows = Rows(rows)
                references.append(weakref.ref(rows))
            return rows
        with patch.object(psycopg.Cursor, 'execute', execute), patch.object(psycopg.Cursor, 'fetchall', fetchall), patch.object(self.store, 'verified_singleton_receipts', True), patch.object(receipt_membership, 'verified_document_chunks', member):
            self.assertEqual(self.capture()['rows'], (257, 257))
        self.assertTrue(all(ref() is None for ref in references))

    def test_document_budget_still_refuses_without_archive_reads(self):
        for batch in (32, 128, 256):
            with self.subTest(batch=batch), self.assertRaises(ChunkRetirementError):
                self.capture(batch=batch, limits=ParentRetirementLimits(max_documents=256))
        self.assertFalse(self.archive.reads)

    def test_controlled_rtt_same_rows_and_spool(self):
        old, new = self.capture(batch=32, delay=.02), self.capture(delay=.02)
        self.assertEqual(old['digest'], new['digest'])
        self.assertEqual((old['fetches'], new['fetches']), (10, 3))
        print(json.dumps(dict(contract='capture-batch-rtt.v1', simulated_rtt_ms=20, old=old, new=new), sort_keys=True))


    @unittest.skipUnless(os.environ.get('RECALL_CAPTURE_SCALE') == '1', 'opt-in 46k capture measurement')
    def test_scale_46001_same_capture_under_simulated_rtt(self):
        # Expand metadata using real indexed/FK-constrained tables. This is a
        # capture-only experiment; these synthetic rows are not archived proof.
        with self.store.connect() as c:
            c.execute("SET LOCAL statement_timeout='60s'")
            c.execute("ANALYZE canonical_events")
            c.execute("ANALYZE canonical_documents")
            c.execute("ANALYZE canonical_chunks")
            c.execute("""INSERT INTO canonical_events(tenant_id,source_id,event_id,native_id,native_parent_id,
                artifact_id,job_id,kind,content_sha256,revision,occurred_at,observed_at,canonical_redacted,source_ordinal)
                SELECT e.tenant_id,e.source_id,'evt_batch_'||n,'batch_'||n,'session',e.artifact_id,e.job_id,
                    e.kind,e.content_sha256,1,e.occurred_at,e.observed_at,e.canonical_redacted,n
                FROM (SELECT * FROM canonical_events WHERE tenant_id=%s LIMIT 1) e
                CROSS JOIN generate_series(257,46000) n""", (self.tenant,))
            c.execute("ANALYZE canonical_events")
            c.execute("""INSERT INTO canonical_documents(tenant_id,source_id,document_id,event_id,artifact_id,native_id,
                content_sha256,revision,is_current,text_redacted,text_sha256)
                SELECT d.tenant_id,d.source_id,'doc_batch_'||n,'evt_batch_'||n,d.artifact_id,'batch_'||n,
                    d.content_sha256,1,true,d.text_redacted,d.text_sha256
                FROM (SELECT * FROM canonical_documents WHERE tenant_id=%s LIMIT 1) d
                CROSS JOIN generate_series(257,46000) n""", (self.tenant,))
            c.execute("ANALYZE canonical_documents")
            c.execute("""INSERT INTO canonical_chunks(tenant_id,source_id,chunk_id,document_id,ordinal,receipt,text_redacted,text_sha256)
                SELECT x.tenant_id,x.source_id,'chk_batch_'||n,'doc_batch_'||n,0,
                    'recall://'||x.source_id||'/batch_'||n||'?rev=1#item=0',x.text_redacted,x.text_sha256
                FROM (SELECT * FROM canonical_chunks WHERE tenant_id=%s LIMIT 1) x
                CROSS JOIN generate_series(257,46000) n""", (self.tenant,))
            c.execute("ANALYZE canonical_chunks")
        old = self.capture(batch=32, delay=.01, timeout=90)
        new = self.capture(delay=.01, timeout=90)
        self.assertEqual(old['rows'], (46001, 46001))
        self.assertEqual(old['digest'], new['digest'])
        self.assertEqual((old['fetches'], new['fetches']), (1439, 181))
        print(json.dumps(dict(contract='capture-scale-rtt.v1', simulated_rtt_ms=10, old=old, new=new), sort_keys=True))


if __name__ == '__main__':
    unittest.main()
