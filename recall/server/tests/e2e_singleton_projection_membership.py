#!/usr/bin/env python3
"""Current receipt authority during streamed Parquet publication and repair."""
import unittest
from unittest.mock import patch

from e2e_singleton_passage_reads import SingletonPassageReads
from recall_server.logical_evidence import LogicalEvidenceProjectionStore
from recall_server.parquet_scan import CanonicalParquetScanProjector, ScanCandidate


class SingletonProjectionMembership(SingletonPassageReads):
    def scan_fixture(self):
        self.prepare_passages()
        self.scan = CanonicalParquetScanProjector(self.store, LogicalEvidenceProjectionStore(self.archive))
        with self.store.connect() as connection:
            row = connection.execute('SELECT * FROM canonical_parquet_scan_queue WHERE tenant_id=%s LIMIT 1',
                                     (self.tenant,)).fetchone()
        self.candidate = ScanCandidate(self.tenant, self.source, row['bucket_start'], row['generation'], row['changed_at'])
        self.documents = [self.args['logical_document_id']]

    def receipt_fanout_fixture(self, *, valid_first=False):
        from psycopg import sql
        from recall_server.projectors import item_receipt
        self.scan_fixture()
        self.fanout_document = self.baseline(self.receipt)['document_id']
        self.admit_fixture(self.fanout_document)
        with self.store.connect() as connection:
            baseline = connection.execute('SELECT * FROM canonical_passages WHERE tenant_id=%s LIMIT 1', (self.tenant,)).fetchone()
            columns = [row['attname'] for row in connection.execute("SELECT attname FROM pg_attribute WHERE attrelid='canonical_passages'::regclass AND attnum>0 AND NOT attisdropped AND attgenerated='' ORDER BY attnum").fetchall()]
            overrides = dict(passage_id='psg_' + 'f' * 32, ordinal=baseline['ordinal'] + 1)
            selected = [sql.Placeholder() if key in overrides else sql.Identifier(key) for key in columns]
            statement = sql.SQL('INSERT INTO canonical_passages ({}) SELECT {} FROM canonical_passages WHERE tenant_id=%s AND passage_id=%s').format(sql.SQL(',').join(map(sql.Identifier, columns)), sql.SQL(',').join(selected))
            connection.execute(statement, tuple(overrides[key] for key in columns if key in overrides) + (self.tenant, baseline['passage_id']))
            missing = [item_receipt(self.source, f'missing-{i}', 1, 0) for i in range(1001)]
            receipts = ([self.receipt] + missing) if valid_first else (missing + [self.receipt])
            connection.execute('UPDATE canonical_passages SET receipts=%s WHERE tenant_id=%s AND passage_id=%s', (receipts, self.tenant, baseline['passage_id']))
        return overrides['passage_id']

    def test_projection_receipt_slices_share_budget_and_preserve_native_union(self):
        from recall_server import receipt_membership, parent_membership_read
        survivor = self.receipt_fanout_fixture()
        native = receipt_membership.lookup_current_search_membership
        attempts = []
        def record(*args, **kwargs):
            attempts.append((len(kwargs['receipts']), kwargs['deadline_at'], id(kwargs['connection'])))
            return native(*args, **kwargs)
        with patch.object(self.store, 'verified_singleton_receipts', True), patch.object(parent_membership_read.ReadSettings, 'from_env', return_value=parent_membership_read.ReadSettings()), patch.object(receipt_membership, 'lookup_current_search_membership', record):
            rows = list(self.scan._passages(self.candidate, self.documents))
        self.assertEqual([row['passage_id'] for row in rows], [survivor])
        self.assertEqual([row['receipts'] for row in rows], [[self.receipt]])
        self.assertEqual([n for n, _, _ in attempts], [500, 500, 2])
        self.assertEqual(len({deadline for _, deadline, _ in attempts}), 1)
        self.assertEqual(len({connection for _, _, connection in attempts}), 1)

    def test_projection_receipt_slices_keep_snapshot_then_next_request_refuses(self):
        from recall_server import receipt_membership, parent_membership_read
        survivor = self.receipt_fanout_fixture()
        document = self.fanout_document
        native = receipt_membership.lookup_current_search_membership
        attempts = []
        def change_after_first(*args, **kwargs):
            members = native(*args, **kwargs)
            attempts.append(len(kwargs['receipts']))
            if len(attempts) == 1:
                with self.store.connect() as other:
                    other.execute('UPDATE canonical_documents SET is_current=false,deleted_at=now() WHERE tenant_id=%s AND document_id=%s', (self.tenant, document))
            return members
        with patch.object(self.store, 'verified_singleton_receipts', True), patch.object(parent_membership_read.ReadSettings, 'from_env', return_value=parent_membership_read.ReadSettings()):
            with patch.object(receipt_membership, 'lookup_current_search_membership', change_after_first):
                rows = list(self.scan._passages(self.candidate, self.documents))
            self.assertEqual([row['passage_id'] for row in rows], [survivor])
            self.assertEqual(attempts, [500, 500, 2])
            self.assertEqual(list(self.scan._passages(self.candidate, self.documents)), [])

    def test_projection_later_slice_cancellation_yields_nothing_and_keeps_queue(self):
        from recall_server import parent_membership_read
        from recall_server.db import BrainStore, SearchDeadlineExceeded
        self.receipt_fanout_fixture(valid_first=True)
        original = self.store._execute_bounded
        attempts = []
        def slow(connection, statement, parameters, deadline):
            if statement.lstrip().startswith('WITH requested'):
                attempts.append(deadline)
                BrainStore._execute_bounded(connection, 'SELECT pg_sleep(.25)', (), deadline)
            return original(connection, statement, parameters, deadline)
        with self.store.connect() as connection:
            before = connection.execute('SELECT generation,changed_at FROM canonical_parquet_scan_queue WHERE tenant_id=%s', (self.tenant,)).fetchall()
        with patch.object(self.store, 'verified_singleton_receipts', True), patch.object(self.store, 'search_deadline_ms', 450), patch.object(parent_membership_read.ReadSettings, 'from_env', return_value=parent_membership_read.ReadSettings()), patch.object(self.store, '_execute_bounded', slow):
            iterator = self.scan._passages(self.candidate, self.documents)
            with self.assertRaises(SearchDeadlineExceeded):
                next(iterator)
        self.assertEqual(len(attempts), 2)
        self.assertEqual(len(set(attempts)), 1)
        with self.store.connect() as connection:
            after = connection.execute('SELECT generation,changed_at FROM canonical_parquet_scan_queue WHERE tenant_id=%s', (self.tenant,)).fetchall()
        self.assertEqual(before, after)

    def test_projection_stream_preserves_exact_passage_after_admission(self):
        self.scan_fixture()
        baseline = list(self.scan._passages(self.candidate, self.documents))
        self.assertEqual(len(baseline), 1)
        self.admit_fixture(self.baseline(self.receipt)['document_id'])
        with patch.object(self.store, 'verified_singleton_receipts', True):
            self.assertEqual(list(self.scan._passages(self.candidate, self.documents)), baseline)
    def test_projection_repair_does_not_requeue_a_valid_admitted_receipt(self):
        from e2e_passage_receipt_repair import Repair
        from recall_server.passage_index import CanonicalPassageProjector
        from recall_server.passage_projection import DEFAULT_PASSAGE_POLICY

        self.scan_fixture()
        self.digest = Repair.digest
        target = {key: self.args[key] for key in ('source_id', 'logical_document_id')}
        target['passage_id'] = self.args['passage_ids'][0]
        plan = Repair.plan(self, target)
        self.admit_fixture(self.baseline(self.receipt)['document_id'])
        owner = CanonicalPassageProjector(self.store, LogicalEvidenceProjectionStore(self.archive),
                                         bound_tenant_id=self.tenant, policy=DEFAULT_PASSAGE_POLICY)
        with patch.object(self.store, 'verified_singleton_receipts', True):
            with self.assertRaisesRegex(ValueError, 'witness_changed'):
                owner.requeue_receipt_repair(plan=plan, principal_id=self.principal)
        with self.store.connect() as connection:
            self.assertEqual(connection.execute('SELECT count(*) AS n FROM canonical_passage_projection_queue WHERE tenant_id=%s',
                                                (self.tenant,)).fetchone()['n'], 0)

    def test_projection_stream_advances_after_a_whole_filtered_batch(self):
        from psycopg import sql
        self.scan_fixture()
        with self.store.connect() as connection:
            baseline = connection.execute('SELECT * FROM canonical_passages WHERE tenant_id=%s LIMIT 1', (self.tenant,)).fetchone()
            columns = [row['attname'] for row in connection.execute("SELECT attname FROM pg_attribute WHERE attrelid='canonical_passages'::regclass AND attnum>0 AND NOT attisdropped AND attgenerated='' ORDER BY attnum").fetchall()]
            selected = [sql.Placeholder() if key in ('passage_id', 'ordinal', 'receipts') else sql.Identifier(key) for key in columns]
            statement = sql.SQL('INSERT INTO canonical_passages ({}) SELECT {} FROM canonical_passages WHERE tenant_id=%s AND passage_id=%s').format(
                sql.SQL(',').join(map(sql.Identifier, columns)), sql.SQL(',').join(selected))
            values = []
            for ordinal in range(2000):
                overrides = dict(passage_id=f'psg_{ordinal:032x}', ordinal=ordinal + 1,
                                 receipts=[self.receipt.replace('turn/passage', 'missing/native')])
                values.append(tuple(overrides[key] for key in columns if key in overrides) + (self.tenant, baseline['passage_id']))
            with connection.cursor() as cursor:
                cursor.executemany(statement, values)
        self.admit_fixture(self.baseline(self.receipt)['document_id'])
        with patch.object(self.store, 'verified_singleton_receipts', True):
            rows = list(self.scan._passages(self.candidate, self.documents))
        self.assertEqual([row['passage_id'] for row in rows], [baseline['passage_id']])

    def test_projection_stream_keeps_one_snapshot_and_next_request_denies(self):
        from recall_server import receipt_membership
        self.scan_fixture()
        doc = self.baseline(self.receipt)['document_id']
        self.admit_fixture(doc)
        original = receipt_membership.lookup_current_search_membership
        def delete_then_prove(*args, **kwargs):
            with self.store.connect() as other:
                other.execute('UPDATE canonical_documents SET is_current=false,deleted_at=now() WHERE tenant_id=%s AND document_id=%s',
                              (self.tenant, doc))
            return original(*args, **kwargs)
        with patch.object(self.store, 'verified_singleton_receipts', True):
            with patch.object(receipt_membership, 'lookup_current_search_membership', delete_then_prove):
                self.assertEqual(len(list(self.scan._passages(self.candidate, self.documents))), 1)
            self.assertEqual(list(self.scan._passages(self.candidate, self.documents)), [])

    def test_projection_invalid_profile_never_publishes_and_retains_queue(self):
        from recall_server.receipt_membership import ReceiptMembershipError
        self.scan_fixture()
        self.admit_fixture(self.baseline(self.receipt)['document_id'])
        with self.store.connect() as connection:
            connection.execute('UPDATE canonical_documents SET chunk_receipt_profile=2 WHERE tenant_id=%s', (self.tenant,))
        with patch.object(self.store, 'verified_singleton_receipts', True):
            with self.assertRaises(ReceiptMembershipError):
                self.scan.project_pending(tenant_id=self.tenant, batch_size=1, max_batches=1)
        with self.store.connect() as connection:
            self.assertEqual(connection.execute('SELECT count(*) AS n FROM canonical_parquet_scan_shards WHERE tenant_id=%s',
                                                (self.tenant,)).fetchone()['n'], 0)
            self.assertEqual(connection.execute('SELECT count(*) AS n FROM canonical_parquet_scan_queue WHERE tenant_id=%s',
                                                (self.tenant,)).fetchone()['n'], 1)

    def test_projection_full_publication_preserves_archived_passage_receipt(self):
        import pyarrow as pa
        import pyarrow.parquet as pq
        from e2e_parquet_scan_fragments import live_parts, reference
        self.scan_fixture()
        self.admit_fixture(self.baseline(self.receipt)['document_id'])
        with patch.object(self.store, 'verified_singleton_receipts', True):
            result = self.scan.project_pending(tenant_id=self.tenant, batch_size=1, max_batches=1)
        self.assertEqual(result['shards'], 1)
        parts = [row for row in live_parts(self.store, self.tenant, self.source) if row['dataset'] == 'passages']
        self.assertEqual(len(parts), 1)
        rows = pq.read_table(pa.BufferReader(self.archive.read_raw(reference(parts[0])))).to_pylist()
        self.assertEqual([row['receipts'] for row in rows], [[self.receipt]])

    def test_projection_default_still_works_without_profile_column(self):
        self.scan_fixture()
        baseline = list(self.scan._passages(self.candidate, self.documents))
        with self.store.connect() as connection:
            connection.execute('ALTER TABLE canonical_documents DROP COLUMN chunk_receipt_profile')
        try:
            self.assertEqual(list(self.scan._passages(self.candidate, self.documents)), baseline)
        finally:
            with self.store.connect() as connection:
                connection.execute('ALTER TABLE canonical_documents ADD COLUMN chunk_receipt_profile smallint')

    def test_projection_repair_still_queues_a_genuinely_superseded_receipt(self):
        from e2e_passage_receipt_repair import Repair
        from recall_server.passage_index import CanonicalPassageProjector
        from recall_server.passage_projection import DEFAULT_PASSAGE_POLICY
        self.scan_fixture()
        old_receipt = self.receipt
        self.ingest('turn/passage', 'A revised source body with a different receipt')
        self.projector.project_pending(tenant_id=self.tenant, batch_size=10, max_batches=1, upload_concurrency=1)
        owner = CanonicalPassageProjector(self.store, LogicalEvidenceProjectionStore(self.archive),
                                         bound_tenant_id=self.tenant, policy=DEFAULT_PASSAGE_POLICY)
        owner.project_pending(tenant_id=self.tenant, batch_size=10, max_batches=1, concurrency=1)
        with self.store.connect() as connection:
            target = connection.execute('UPDATE canonical_passages SET receipts=%s WHERE tenant_id=%s RETURNING source_id,logical_document_id,passage_id',
                                        ([old_receipt], self.tenant)).fetchone()
        self.digest = Repair.digest
        plan = Repair.plan(self, target)
        with patch.object(self.store, 'verified_singleton_receipts', True):
            result = owner.requeue_receipt_repair(plan=plan, principal_id=self.principal)
        self.assertEqual(result['status'], 'queued')
        self.assertFalse(result['repair_complete'])


if __name__ == '__main__':
    names = [name for name in dir(SingletonProjectionMembership) if name.startswith('test_projection_')]
    result = unittest.TextTestRunner(verbosity=2).run(unittest.TestSuite(SingletonProjectionMembership(name) for name in names))
    raise SystemExit(not result.wasSuccessful())
