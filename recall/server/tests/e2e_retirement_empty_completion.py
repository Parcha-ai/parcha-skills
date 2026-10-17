"""Native retirement completion after every singleton has already been retired."""
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
from recall_server.db import BrainStore


class EmptyCompletion(unittest.TestCase):
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
        self.tenant = self.fixture.tenant
        self.source = self.fixture.source
        self.principal = self.fixture.principal
        self.receipts = [self.fixture.ingest('turn/'+name, 'Actual native singleton '+name)['receipt']
                         for name in ('one', 'two')]
        projected = self.fixture.projector.project_pending(
            tenant_id=self.tenant, batch_size=10, max_batches=1, upload_concurrency=1)
        self.assertEqual(projected['failed'], 0)
        self.env = patch.dict(os.environ, RECALL_CHUNK_BODY_READS='archive')
        self.env.start()
        self.addCleanup(self.env.stop)
        self.store.verified_singleton_receipts = True
        self.store.search_plane = 'turbopuffer'
        retirement.set_parent_retirement_enabled(self.store, tenant_id=self.tenant,
            source_id=self.source, native_parent_id='session', enabled=True)

    def run_parent(self, batches):
        with self.store.connect() as connection:
            connection.execute("""UPDATE canonical_chunk_retirement_progress
                SET updated_at=now()-interval '2 minutes' WHERE tenant_id=%s""", (self.tenant,))
        return runner.run_retirement(self.store, self.archive,
            scope=runner.RetirementScope(self.tenant, self.principal, (self.source,)),
            apply=True, limits=runner.RunnerLimits(max_parents=1),
            parent_limits=replace(retirement.ParentRetirementLimits(), batch_documents=1, max_batches=batches),
            metadata_mode='singleton', deadline_at=time.monotonic()+20)

    def test_drained_parent_finishes_without_reopening_archive_or_claiming_full_proof(self):
        first = self.run_parent(2)
        self.assertEqual(first['cleared_documents'], 2)
        self.assertEqual(first['retired_metadata_chunks'], 2)
        self.assertEqual(first['completed_parent_proofs'], 0)
        with self.store.connect() as connection:
            before = connection.execute("""SELECT status,last_record_ordinal,
                cumulative_cleared_utf8_bytes FROM canonical_chunk_retirement_progress
                WHERE tenant_id=%s""", (self.tenant,)).fetchone()
            chunks = connection.execute('SELECT count(*) AS n FROM canonical_chunks WHERE tenant_id=%s',
                                        (self.tenant,)).fetchone()['n']
        self.assertEqual(chunks, 0)
        self.assertEqual(before['status'], 'partial')
        with patch.object(self.archive, 'read_raw', side_effect=AssertionError('already drained archive reopened')) as read:
            resumed = self.run_parent(1)
        self.assertFalse(resumed['commit_outcome_unknown'])
        self.assertEqual(resumed['failed_parents'], 0)
        self.assertEqual(resumed['cleared_utf8_bytes'], 0)
        self.assertEqual(resumed['retired_metadata_chunks'], 0)
        self.assertEqual(resumed['completed_parent_proofs'], 0)
        self.assertEqual(resumed['completed_noop_parents'], 1)
        read.assert_not_called()
        with self.store.connect() as connection:
            after = connection.execute("""SELECT status,last_record_ordinal,
                cumulative_cleared_utf8_bytes FROM canonical_chunk_retirement_progress
                WHERE tenant_id=%s""", (self.tenant,)).fetchone()
        self.assertEqual(after, dict(before, status='complete'))
        self.assertEqual(self.run_parent(1)['attempted_parents'], 0)
        for receipt in self.receipts:
            self.assertIsNotNone(self.store.resolve(receipt, tenant_id=self.tenant,
                authorized_sources=(self.source,), chunk_body_archive=self.archive))


if __name__ == '__main__':
    unittest.main(verbosity=2)
