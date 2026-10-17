"""Thinning must make newly eligible historical metadata reachable by retirement."""
from dataclasses import replace
import os
from pathlib import Path
import sys
import time
import unittest
from unittest.mock import patch

RECALL = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(RECALL), str(RECALL/'server')]
import e2e_singleton_metadata_admission as original
from recall_server import chunk_retirement as retirement
from recall_server import retirement_runner as runner
from recall_server.canonical_thinning import thin_canonical_bodies
from recall_server.db import BrainStore


class ThinningRetirement(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.store = BrainStore(os.environ['RECALL_DATABASE_URL'])
        cls.store.migrate()

    @classmethod
    def tearDownClass(cls):
        cls.store.close()

    def setUp(self):
        self.fixture = original.SingletonMetadataAdmission('runTest')
        self.fixture.store = self.store
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.archive = self.fixture.archive
        self.tenant, self.source, self.principal = self.fixture.tenant, self.fixture.source, self.fixture.principal
        self.receipts = [self.fixture.ingest('turn/'+name, 'Native historical inline evidence '+name)['receipt']
                         for name in ('one', 'two')]
        # Historical inline rows can coexist with durable raw artifacts after
        # archive migration. Preserve the exact original text and all hashes.
        with self.store.connect() as connection:
            connection.execute("""UPDATE canonical_documents document SET body_location='inline',
                text_redacted=chunk.text_redacted FROM canonical_chunks chunk
                WHERE document.tenant_id=%s AND chunk.tenant_id=document.tenant_id
                  AND chunk.source_id=document.source_id AND chunk.document_id=document.document_id
                  AND chunk.ordinal=0""", (self.tenant,))
        result = self.fixture.projector.project_pending(tenant_id=self.tenant, batch_size=10,
                                                       max_batches=1, upload_concurrency=1)
        self.assertEqual(result['failed'], 0)
        self.env = patch.dict(os.environ, RECALL_CHUNK_BODY_READS='archive')
        self.env.start(); self.addCleanup(self.env.stop)
        self.store.verified_singleton_receipts = True
        self.store.search_plane = 'turbopuffer'
        retirement.set_parent_retirement_enabled(self.store, tenant_id=self.tenant,
            source_id=self.source, native_parent_id='session', enabled=True)

    def run_parent(self, batches):
        with self.store.connect() as connection:
            connection.execute("UPDATE canonical_chunk_retirement_progress SET updated_at=now()-interval '2 minutes' WHERE tenant_id=%s", (self.tenant,))
        return runner.run_retirement(self.store, self.archive,
            scope=runner.RetirementScope(self.tenant, self.principal, (self.source,)), apply=True,
            limits=runner.RunnerLimits(max_parents=1), metadata_mode='singleton',
            parent_limits=replace(retirement.ParentRetirementLimits(), batch_documents=1, max_batches=batches),
            deadline_at=time.monotonic()+20)

    def ledger(self):
        with self.store.connect() as connection:
            return connection.execute('SELECT * FROM canonical_chunk_retirement_progress WHERE tenant_id=%s',
                                      (self.tenant,)).fetchone()

    def test_thinning_after_eof_reaches_newly_eligible_metadata(self):
        first = self.run_parent(2)
        self.assertEqual(first['cleared_documents'], 2)
        self.assertEqual(first['retired_metadata_chunks'], 0)
        before = self.ledger()
        self.assertEqual(before['last_record_ordinal'], 1)
        self.assertEqual(before['status'], 'partial')
        thinned = thin_canonical_bodies(self.store, tenant_id=self.tenant, batch_size=10, max_batches=1)
        self.assertEqual(thinned['documents'], 2)
        after = self.ledger()
        self.assertLess(after['last_record_ordinal'], before['last_record_ordinal'])
        self.assertGreater(after['scope_epoch'], before['scope_epoch'])
        second = self.run_parent(2)
        self.assertEqual(second['retired_metadata_chunks'], 2)
        self.assertEqual(second['cleared_utf8_bytes'], 0)
        for receipt in self.receipts:
            self.assertIsNotNone(self.store.resolve(receipt, tenant_id=self.tenant,
                authorized_sources=(self.source,), chunk_body_archive=self.archive))


if __name__ == '__main__':
    unittest.main(verbosity=2)
