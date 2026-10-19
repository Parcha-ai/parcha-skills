#!/usr/bin/env python3
"""Actual stream chunk parity, bounded metadata, and cancellation."""
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
        streams = []
        original_stream = psycopg.Cursor.stream
        def stream(cursor, query, *args, **kwargs):
            if "SELECT selected.*,items.chunks FROM (" in query:
                streams.append(kwargs['size'])
                time.sleep(delay)  # Explicit simulated query RTT; not production measurement.
                if cancel:
                    query = query.replace(
                        "SELECT selected.*,items.chunks FROM (",
                        "SELECT selected.*,items.chunks FROM (SELECT pg_sleep(1)) pause CROSS JOIN (",
                    )
            yield from original_stream(cursor, query, *args, **kwargs)
        started = time.monotonic()
        with patch.object(proof, '_CAPTURE_FETCH_ROWS', batch, create=True) if batch else nullcontext():
            with patch.object(psycopg.Cursor, 'stream', stream):
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
                    streams=len(streams), result_chunk_sizes=streams, elapsed_ms=(time.monotonic() - started) * 1000)

    def test_257_rows_use_one_stream_with_bounded_result_chunks(self):
        result = self.capture()
        self.assertEqual(result['rows'], (257, 257))
        self.assertEqual(result['streams'], 1)
        self.assertEqual(result['result_chunk_sizes'], [256])

    def test_same_spool_with_and_without_deadline_and_different_result_chunks(self):
        results = [self.capture(batch=32), self.capture(batch=128), self.capture(), self.capture(deadline=False)]
        self.assertEqual([r['streams'] for r in results], [1, 1, 1, 1])
        self.assertEqual([r['result_chunk_sizes'] for r in results], [[32], [128], [256], [256]])
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

    def test_real_stream_timeout_rolls_back_for_all_result_chunk_sizes(self):
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

    def test_consumed_membership_batch_released_before_next_batch(self):
        from recall_server import receipt_membership
        class Members(dict):
            pass
        references = []
        original_member = receipt_membership.receipt_metadata_from_rows
        calls = 0
        def member(*args, **kwargs):
            nonlocal calls
            self.assertTrue(all(ref() is None for ref in references),
                            'previous decoded membership survives into the next streamed document')
            result = Members(original_member(*args, **kwargs))
            references.append(weakref.ref(result))
            calls += 1
            return result
        with patch.object(self.store, 'verified_singleton_receipts', True), patch.object(receipt_membership, 'receipt_metadata_from_rows', member):
            self.assertEqual(self.capture()['rows'], (257, 257))
        self.assertEqual(calls, 257)
        self.assertTrue(all(ref() is None for ref in references))

    def test_document_budget_still_refuses_without_archive_reads(self):
        for batch in (32, 128, 256):
            with self.subTest(batch=batch), self.assertRaises(ChunkRetirementError):
                self.capture(batch=batch, limits=ParentRetirementLimits(max_documents=256))
        self.assertFalse(self.archive.reads)

    def test_controlled_rtt_same_rows_and_spool(self):
        old, new = self.capture(batch=32, delay=.02), self.capture(delay=.02)
        self.assertEqual(old['digest'], new['digest'])
        self.assertEqual((old['streams'], new['streams']), (1, 1))
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
        self.assertEqual((old['streams'], new['streams']), (1, 1))
        print(json.dumps(dict(contract='capture-scale-rtt.v1', simulated_rtt_ms=10, old=old, new=new), sort_keys=True))


if __name__ == '__main__':
    unittest.main()
