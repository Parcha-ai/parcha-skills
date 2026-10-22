"""Native cleanup must reach the coordinator's reserved SQL capacity."""
import os
from pathlib import Path
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

from psycopg_pool import PoolTimeout

import e2e_logical_admission_sharing as fixture
from recall_server.worker_connections import WorkerWriteStore
from recall_server.db import BrainStore


class CleanupSQLAdmission(unittest.TestCase):
    setUpClass = classmethod(fixture.AdmissionSharing.setUpClass.__func__)
    tearDownClass = classmethod(fixture.AdmissionSharing.tearDownClass.__func__)
    setUp = fixture.AdmissionSharing.setUp
    projector = fixture.AdmissionSharing.projector
    add = fixture.AdmissionSharing.add

    def test_cleanup_can_reach_reserved_pool_while_background_sql_is_active(self):
        self.store.prepare_pool(4)
        view = WorkerWriteStore(self.store, publication_reserve=2)
        self.logical.store = view
        self.assertEqual(view._capacity, 2)
        barrier = threading.Barrier(3)
        owner_results = []

        def background_sql_owner():
            with view.connect() as connection:
                self.assertEqual(connection.execute('SELECT 7 AS n').fetchone()['n'], 7)
                barrier.wait(timeout=5)
                connection.execute('SELECT pg_sleep(6.0)').fetchone()
                owner_results.append(True)

        with ThreadPoolExecutor(max_workers=2) as executor:
            owners = [executor.submit(background_sql_owner) for _ in range(2)]
            barrier.wait(timeout=5)
            # The underlying native pool still has the original two reserved
            # slots. This must work while both bounded background slots are held.
            before = time.monotonic()
            with self.store.connect() as connection:
                self.assertEqual(connection.execute('SELECT 11 AS n').fetchone()['n'], 11)
            self.assertLess(time.monotonic() - before, 1.0)
            self.assertFalse(any(owner.done() for owner in owners))
            try:
                # Actual unchanged cleanup owner, including its queue selection
                # and final pending count. No fake cursor, pool or admission clock.
                with self.assertRaisesRegex(PoolTimeout, 'worker SQL admission'):
                    self.logical.drain_cleanup(tenant_id=self.tenant, limit=1)
                result = self.logical.drain_cleanup(tenant_id=self.tenant, limit=1,
                                                    _coordinator=True)
                self.assertEqual(result['pending'], 0)
                self.assertFalse(any(owner.done() for owner in owners),
                                 'cleanup waited for background SQL instead of its reserve')
            finally:
                for owner in owners:
                    owner.result(timeout=8)
        self.assertEqual(owner_results, [True, True])
        self.assertEqual(view._available, view._capacity)
        with self.store.connect() as connection:
            self.assertEqual(connection.execute('SELECT 13 AS n').fetchone()['n'], 13)


    def test_underlying_full_pool_still_reports_native_timeout(self):
        self.store.prepare_pool(4)
        view = WorkerWriteStore(self.store)
        barrier, release = threading.Barrier(5), threading.Event()
        def holder():
            with self.store.connect() as connection:
                self.assertEqual(connection.execute('SELECT 17 AS n').fetchone()['n'], 17)
                barrier.wait(timeout=5)
                self.assertTrue(release.wait(8))
        with ThreadPoolExecutor(max_workers=4) as executor:
            owners = [executor.submit(holder) for _ in range(4)]
            barrier.wait(timeout=5)
            try:
                with self.assertRaises(PoolTimeout) as raised:
                    with view.coordinator_connect():
                        self.fail('full underlying pool admitted an extra connection')
                self.assertNotIn('worker SQL admission', str(raised.exception))
                self.assertFalse(any(owner.done() for owner in owners))
                self.assertEqual(view._available, view._capacity)
                self.assertFalse(view._local.active)
            finally:
                release.set()
                for owner in owners:
                    owner.result(timeout=2)
        with view.coordinator_connect() as connection:
            self.assertEqual(connection.execute('SELECT 19 AS n').fetchone()['n'], 19)

    def test_coordinator_ownership_nested_refusal_and_exception_restore(self):
        for reserve, pool_size in ((2, 4), (4, 6)):
            store = BrainStore(os.environ['RECALL_DATABASE_URL'], pool_max_size=pool_size)
            self.addCleanup(store.close)
            view = WorkerWriteStore(store, publication_reserve=reserve)
            before = view._available
            with view.coordinator_connect() as connection:
                self.assertTrue(view._local.active)
                self.assertEqual(view._available, before)
                self.assertEqual(view._execute_bounded(connection, 'SELECT 23 AS n', (), None).fetchone()['n'], 23)
                for checkout in (view.connect, view.coordinator_connect):
                    with self.assertRaisesRegex(RuntimeError, 'nested worker'):
                        with checkout():
                            self.fail('nested checkout admitted')
            with self.assertRaisesRegex(RuntimeError, 'admitted connection'):
                view._set_statement_deadline(connection, None)
            with self.assertRaises(KeyboardInterrupt):
                with view.coordinator_connect():
                    raise KeyboardInterrupt('synthetic coordinator interruption')
            self.assertFalse(view._local.active)
            self.assertIsNone(view._local.connection)
            self.assertEqual(view._available, before)
            with view.connect():
                with self.assertRaisesRegex(RuntimeError, 'nested worker'):
                    with view.coordinator_connect():
                        self.fail('coordinator nested inside preparation')
            self.assertEqual(view._available, before)

    def test_nonempty_cleanup_keeps_success_ack_and_failed_reference(self):
        self.add('source-seed', age=100)
        references = [self.archive.put_raw(tenant_id=self.tenant, source_id=self.source,
            native_id='cleanup-'+str(i), payload=('owned cleanup '+str(i)).encode(),
            media_type='application/json', created_at='2026-10-22T00:00:00Z') for i in range(2)]
        with self.store.connect() as connection:
            self.logical._enqueue_cleanup(connection, tuple(references))
        view = WorkerWriteStore(self.store)
        self.logical.store = view
        original_delete = self.projection.delete_reference
        seen = []
        def delete(reference):
            seen.append(reference['artifact_id'])
            if reference['artifact_id'] == references[1]['artifact_id']:
                raise OSError('synthetic original archive confirmation failure')
            return original_delete(reference)
        with patch.object(self.projection, 'delete_reference', side_effect=delete):
            result = self.logical.drain_cleanup(tenant_id=self.tenant, limit=2, _coordinator=True)
        self.assertEqual((result['completed'], result['deleted'], result['failures'], result['pending']), (1, 1, 1, 1))
        self.assertEqual(set(seen), {r['artifact_id'] for r in references})
        with self.store.connect() as connection:
            queued = connection.execute('SELECT artifact_id,attempts FROM canonical_evidence_cleanup_queue WHERE tenant_id=%s', (self.tenant,)).fetchall()
        self.assertEqual(queued, [dict(artifact_id=references[1]['artifact_id'], attempts=1)])
        self.assertEqual(self.archive.read_raw(references[1]), b'owned cleanup 1')
        self.assertFalse((Path(self.archive.root) / references[0]['object_key'] / 'data').exists())
        self.assertEqual(view._available, view._capacity)



if __name__ == '__main__':
    unittest.main()
