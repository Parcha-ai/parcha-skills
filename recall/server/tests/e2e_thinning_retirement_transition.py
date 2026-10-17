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
from recall_server import canonical_thinning as thinning
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
        self.assertEqual(after['scope_epoch'], before['scope_epoch'] + 1)
        second = self.run_parent(2)
        self.assertEqual(second['retired_metadata_chunks'], 2)
        self.assertEqual(second['cleared_utf8_bytes'], 0)
        for receipt in self.receipts:
            self.assertIsNotNone(self.store.resolve(receipt, tenant_id=self.tenant,
                authorized_sources=(self.source,), chunk_body_archive=self.archive))

    def test_disabled_parent_stays_disabled_after_thinning(self):
        self.run_parent(2)
        retirement.set_parent_retirement_enabled(self.store, tenant_id=self.tenant,
            source_id=self.source, native_parent_id='session', enabled=False)
        before = self.ledger()
        self.assertEqual(thin_canonical_bodies(self.store, tenant_id=self.tenant,
            batch_size=10, max_batches=1)['documents'], 2)
        self.assertEqual(self.ledger(), before)

    def test_background_thinner_rearms_once_and_preserves_cumulative_work(self):
        self.run_parent(2)
        before = self.ledger()
        result = thinning.CanonicalBodyThinner(self.store, tenant_id=self.tenant).thin(batch_size=10)
        self.assertEqual(result['documents'], 2)
        after = self.ledger()
        self.assertEqual(after['scope_epoch'], before['scope_epoch'] + 1)
        self.assertEqual(after['last_record_ordinal'], -1)
        for field in ('cumulative_cleared_documents', 'cumulative_cleared_chunks',
                      'cumulative_cleared_utf8_bytes'):
            self.assertEqual(after[field], before[field])
        self.assertEqual(self.run_parent(2)['retired_metadata_chunks'], 2)

    def test_pending_append_keeps_original_thinning_refusal(self):
        self.run_parent(2)
        self.fixture.ingest('turn/new', 'New captured evidence awaiting logical publication')
        before = self.ledger()
        self.assertEqual(thin_canonical_bodies(self.store, tenant_id=self.tenant,
            batch_size=10, max_batches=1)['documents'], 0)
        self.assertEqual(self.ledger(), before)

    def test_other_parent_intent_is_unchanged(self):
        other = original.SingletonMetadataAdmission('runTest')
        other.store = self.store
        other.setUp()
        self.addCleanup(other.doCleanups)
        other.ingest('turn/other', 'Unrelated parent evidence')
        other.projector.project_pending(tenant_id=other.tenant, batch_size=10,
                                       max_batches=1, upload_concurrency=1)
        retirement.set_parent_retirement_enabled(self.store, tenant_id=other.tenant,
            source_id=other.source, native_parent_id='session', enabled=True)
        def other_ledger():
            with self.store.connect() as connection:
                return connection.execute('SELECT * FROM canonical_chunk_retirement_progress WHERE tenant_id=%s',
                                          (other.tenant,)).fetchone()
        before = other_ledger()
        self.run_parent(2)
        self.assertEqual(thin_canonical_bodies(self.store, tenant_id=self.tenant,
            batch_size=10, max_batches=1)['documents'], 2)
        self.assertEqual(other_ledger(), before)

    def test_thinning_ahead_of_cursor_does_not_restart_prior_work(self):
        with self.store.connect() as connection:
            connection.execute("""UPDATE canonical_documents SET body_location='chunks',text_redacted=''
                WHERE tenant_id=%s AND native_id='turn/one'""", (self.tenant,))
        self.assertEqual(self.run_parent(1)['retired_metadata_chunks'], 1)
        before = self.ledger()
        self.assertEqual(before['last_record_ordinal'], 0)
        self.assertEqual(thin_canonical_bodies(self.store, tenant_id=self.tenant,
            batch_size=10, max_batches=1)['documents'], 1)
        self.assertEqual(self.ledger(), before)
        self.assertEqual(self.run_parent(1)['retired_metadata_chunks'], 1)

    def test_catalog_lock_skips_transition_until_owner_releases(self):
        self.run_parent(2)
        before = self.ledger()
        with self.store.connect() as locked, locked.transaction():
            locked.execute("""SELECT 1 FROM canonical_evidence_documents
                WHERE tenant_id=%s FOR UPDATE""", (self.tenant,)).fetchall()
            self.assertEqual(thin_canonical_bodies(self.store, tenant_id=self.tenant,
                batch_size=10, max_batches=1)['documents'], 0)
            self.assertEqual(self.ledger(), before)
        self.assertEqual(thin_canonical_bodies(self.store, tenant_id=self.tenant,
            batch_size=10, max_batches=1)['documents'], 2)
        self.assertEqual(self.ledger()['scope_epoch'], before['scope_epoch'] + 1)

    def test_transition_and_rearm_roll_back_together(self):
        self.run_parent(2)
        before = self.ledger()
        apply = thinning._thin_selected
        def interrupt(*args, **kwargs):
            apply(*args, **kwargs)
            raise RuntimeError('injected before COMMIT')
        with patch.object(thinning, '_thin_selected', side_effect=interrupt):
            with self.assertRaisesRegex(RuntimeError, 'injected'):
                thin_canonical_bodies(self.store, tenant_id=self.tenant, batch_size=10, max_batches=1)
        self.assertEqual(self.ledger(), before)
        with self.store.connect() as connection:
            rows = connection.execute('SELECT body_location FROM canonical_documents WHERE tenant_id=%s',
                                      (self.tenant,)).fetchall()
        self.assertEqual([row['body_location'] for row in rows], ['inline', 'inline'])


if __name__ == '__main__':
    unittest.main(verbosity=2)
