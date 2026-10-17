"""Legacy cursor reconciliation cannot revive an explicitly forgotten event."""
import json
from pathlib import Path
import tempfile
import unittest

import e2e_slack_deletion_ordering as fixture
from collector.collector import Collector, CollectorRuntimeError
from recall_server.canonical import CanonicalArchiveGateway


class LegacyEofForget(unittest.TestCase):
    setUpClass = classmethod(fixture.SlackDeletionOrdering.setUpClass.__func__)
    tearDownClass = classmethod(fixture.SlackDeletionOrdering.tearDownClass.__func__)
    setUp = fixture.SlackDeletionOrdering.setUp

    def test_missing_local_ledger_reconciliation_still_refuses_native_forget(self):
        plane, tenant, principal = self.plane, self.tenant, self.principal

        class Writer:
            def ingest(self, events):
                return plane.ingest_batch(tenant_id=tenant, principal_id=principal, events=events)

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / 'source'
            root.mkdir()
            path = root / 'session.jsonl'
            path.write_text(json.dumps(dict(type='user', message=dict(content='Explicitly forgotten fixture'),
                                           timestamp='2026-10-14T00:00:00Z')) + '\n')
            gateway = CanonicalArchiveGateway(self.store, self.archive, tenant_id=tenant,
                                               principal_id=principal)
            collector = Collector(root=root, harness='claude', source_id=self.source,
                                  spool_path=Path(tmp) / 'spool/state.db', endpoint='http://127.0.0.1:1',
                                  token='fixture', principal_id=principal, tenant_id=tenant,
                                  archive=gateway, brain_writer=Writer(), archive_workers=1)
            try:
                self.assertEqual(collector.scan()['records_queued'], 1)
                self.assertEqual(collector.flush()['acked'], 1)
                row = collector.db.execute('SELECT * FROM active_records').fetchone()
                request = dict(contract='recall.forget-request.v1', schema_version=1, tenant_id=tenant,
                               principal_id=principal, source_id=self.source, target_receipt=row['receipt'],
                               mode='explicit_forget', reason='owner_requested',
                               requested_at='2026-10-17T00:00:00Z', idempotency_key='legacy-fixture')
                self.assertEqual(plane.forget(request)['status'], 'deleted')
                for table in ('active_records', 'record_generations', 'outbox'):
                    collector.db.execute(f'DELETE FROM {table}')
                collector.db.execute('UPDATE files SET snapshot_sha256=NULL,snapshot_stat=NULL')
                collector.db.commit()
                with self.assertRaisesRegex(CollectorRuntimeError, 'archive_unavailable'):
                    collector.scan()
                self.assertEqual(collector.pending_envelopes(), [])
                self.assertIsNone(collector.db.execute('SELECT snapshot_sha256 FROM files').fetchone()[0])
                with self.store.connect() as connection:
                    self.assertEqual(connection.execute('SELECT count(*) AS n FROM canonical_documents WHERE tenant_id=%s AND source_id=%s AND is_current AND deleted_at IS NULL',
                                                        (tenant, self.source)).fetchone()['n'], 0)
                    self.assertGreater(connection.execute('SELECT count(*) AS n FROM forget_tombstones WHERE tenant_id=%s AND source_id=%s',
                                                          (tenant, self.source)).fetchone()['n'], 0)
            finally:
                collector.close()


if __name__ == '__main__':
    unittest.main()
