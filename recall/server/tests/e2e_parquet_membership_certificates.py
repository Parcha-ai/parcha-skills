#!/usr/bin/env python3
"""Parquet reuses exact membership certificates inside its native stream snapshot."""
import os
import time
import unittest
from unittest.mock import patch

from psycopg import sql

from e2e_parent_membership_enrollment import ParentEnrollmentTests
from recall_server import parent_membership_read as reader
from recall_server.logical_evidence import LogicalEvidenceProjectionStore
from recall_server.parquet_scan import CanonicalParquetScanProjector
from recall_server.receipt_membership import lookup_current_search_membership


class ParquetCertificateTests(ParentEnrollmentTests):
    def setUp(self):
        super().setUp()
        self.store.verified_singleton_receipts = True
        self.store.search_deadline_ms = 6000
        self.scan = CanonicalParquetScanProjector(self.store, LogicalEvidenceProjectionStore(self.archive))
        self.candidate = self.scan._pending(tenant_id=self.tenant, limit=1)[0]
        with self.store.connect() as connection:
            self.documents = [row['logical_document_id'] for row in connection.execute(
                'SELECT DISTINCT logical_document_id FROM canonical_passages WHERE tenant_id=%s',
                (self.tenant,)).fetchall()]

    def stream(self, enabled=True, epoch='synthetic-epoch-a'):
        with patch.dict(os.environ, RECALL_PARENT_MEMBERSHIP_READ='1' if enabled else '0',
                        RECALL_PARENT_MEMBERSHIP_EPOCH=epoch):
            return list(self.scan._passages(self.candidate, self.documents))

    def test_certified_stream_matches_disabled_without_native_receipt_query(self):
        self.enroll()
        before = self.stream(False)
        self.assertTrue(before)
        with patch('recall_server.receipt_membership.lookup_current_search_membership',
                   wraps=lookup_current_search_membership) as native:
            after = self.stream()
        self.assertEqual(after, before)
        native.assert_not_called()

    def test_disabled_does_not_query_certificates(self):
        self.enroll()
        with patch.object(reader, 'partition', side_effect=AssertionError('disabled')):
            with patch('recall_server.receipt_membership.lookup_current_search_membership',
                       wraps=lookup_current_search_membership) as native:
                self.assertTrue(self.stream(False))
        self.assertEqual(native.call_count, 1)
        self.assertEqual(set(native.call_args.kwargs['receipts']), {self.receipt, self.physical_receipt})

    def test_full_scan_build_and_publication_preserve_native_objects(self):
        self.enroll()
        with patch.dict(os.environ, RECALL_PARENT_MEMBERSHIP_READ='0'):
            baseline = self.scan._build(self.candidate)
        with patch.dict(os.environ, RECALL_PARENT_MEMBERSHIP_READ='1',
                        RECALL_PARENT_MEMBERSHIP_EPOCH='synthetic-epoch-a'):
            with patch('recall_server.receipt_membership.lookup_current_search_membership',
                       wraps=lookup_current_search_membership) as native:
                certified = self.scan._build(self.candidate)
        native.assert_not_called()
        self.assertEqual(certified.references, baseline.references)
        self.assertEqual(certified.row_counts, baseline.row_counts)
        self.assertEqual(certified.members, baseline.members)
        self.assertEqual(certified.generation_sha256, baseline.generation_sha256)
        self.assertEqual(self.scan._commit(self.candidate, certified), 'committed')
        with self.store.connect() as c:
            self.assertEqual(c.execute('SELECT count(*) AS n FROM canonical_parquet_scan_queue '
                                       'WHERE tenant_id=%s', (self.tenant,)).fetchone()['n'], 0)
            self.assertEqual(c.execute('SELECT count(DISTINCT dataset) AS n FROM canonical_parquet_scan_shards '
                                       'WHERE tenant_id=%s', (self.tenant,)).fetchone()['n'], 4)

    def test_missing_and_wrong_epoch_keep_exact_native_fallback(self):
        baseline = self.stream(False)
        for epoch in ('synthetic-epoch-a', 'wrong-epoch'):
            with self.subTest(epoch=epoch):
                with patch('recall_server.receipt_membership.lookup_current_search_membership',
                           wraps=lookup_current_search_membership) as native:
                    self.assertEqual(self.stream(epoch=epoch), baseline)
                self.assertEqual(native.call_count, 1)
                self.assertEqual(set(native.call_args.kwargs['receipts']), {self.receipt, self.physical_receipt})
            self.enroll()

    def clone_passages(self, count, receipt):
        with self.store.connect() as c:
            columns = [row['attname'] for row in c.execute(
                "SELECT attname FROM pg_attribute WHERE attrelid='canonical_passages'::regclass "
                "AND attnum>0 AND NOT attisdropped AND attgenerated='' ORDER BY attnum").fetchall()]
            # New rows deliberately carry no certificates, even when the source is enrolled.
            overrides = {'passage_id', 'ordinal', 'receipts', 'membership_generation', 'membership_binding_sha256'}
            selected = [sql.Placeholder() if key in overrides else sql.Identifier(key) for key in columns]
            statement = sql.SQL('INSERT INTO canonical_passages ({}) SELECT {} FROM canonical_passages '
                                'WHERE tenant_id=%s AND passage_id=%s').format(
                sql.SQL(',').join(map(sql.Identifier, columns)), sql.SQL(',').join(selected))
            values = []
            for ordinal in range(count):
                data = dict(passage_id=f'psg_{ordinal:032x}', ordinal=ordinal + 100,
                            receipts=[receipt], membership_generation=None, membership_binding_sha256=None)
                values.append(tuple(data[key] for key in columns if key in overrides) + (self.tenant, self.ids[0]))
            with c.cursor() as cursor:
                cursor.executemany(statement, values)

    def test_mixed_certificates_verify_only_original_fallback_receipts(self):
        self.enroll()
        self.clone_passages(2, self.physical_receipt)
        baseline = self.stream(False)
        with patch('recall_server.receipt_membership.lookup_current_search_membership',
                   wraps=lookup_current_search_membership) as native:
            self.assertEqual(self.stream(), baseline)
        self.assertEqual(native.call_count, 1)
        self.assertEqual(native.call_args.kwargs['receipts'], (self.physical_receipt,))

    def test_changed_exact_binding_fields_always_use_native(self):
        changes = (
            ('canonical_passages', 'text_sha256', 'a' * 64),
            ('canonical_passages', 'receipts', ['recall://' + self.source + '/missing?rev=1#item=0']),
            ('canonical_evidence_documents', 'manifest_content_sha256', 'b' * 64),
            ('canonical_passage_documents', 'source_document_sha256', 'c' * 64),
        )
        for table, column, changed in changes:
            with self.subTest(column=column):
                self.enroll()
                with self.store.connect() as c:
                    old = c.execute(sql.SQL('SELECT {} FROM {} WHERE tenant_id=%s LIMIT 1').format(
                        sql.Identifier(column), sql.Identifier(table)), (self.tenant,)).fetchone()[column]
                    update = sql.SQL('UPDATE {} SET {}=%s WHERE tenant_id=%s').format(
                        sql.Identifier(table), sql.Identifier(column))
                    c.execute(update, (changed, self.tenant))
                baseline = self.stream(False)
                with patch('recall_server.receipt_membership.lookup_current_search_membership',
                           wraps=lookup_current_search_membership) as native:
                    self.assertEqual(self.stream(), baseline)
                self.assertEqual(native.call_count, 1)
                with self.store.connect() as c:
                    c.execute(update, (old, self.tenant))

    def test_deleted_publication_cannot_leave_certified_scan_rows(self):
        self.enroll()
        with self.store.connect() as c:
            c.execute('DELETE FROM canonical_passage_documents WHERE tenant_id=%s', (self.tenant,))
        baseline = self.stream(False)
        self.assertEqual(baseline, [])
        with patch('recall_server.receipt_membership.lookup_current_search_membership',
                   wraps=lookup_current_search_membership) as native:
            self.assertEqual(self.stream(), baseline)
        native.assert_not_called()

    def test_incomplete_optional_metadata_keeps_native_passage(self):
        self.enroll()
        baseline = self.stream(False)
        execute = self.store._execute_bounded
        def missing(connection, statement, parameters, deadline):
            if statement == reader._METADATA_SQL:
                statement = 'SELECT * FROM (' + statement + ') optional_metadata WHERE false'
            return execute(connection, statement, parameters, deadline)
        with patch.object(self.store, '_execute_bounded', side_effect=missing):
            with patch('recall_server.receipt_membership.lookup_current_search_membership',
                       wraps=lookup_current_search_membership) as native:
                self.assertEqual(self.stream(), baseline)
        self.assertEqual(native.call_count, 1)

    def test_metadata_timeout_preserves_named_cursor_and_later_batch(self):
        self.enroll()
        self.clone_passages(2000, 'recall://' + self.source + '/missing?rev=1#item=0')
        baseline = self.stream(False)
        self.assertEqual(len(baseline), len(self.ids))
        execute = self.store._execute_bounded
        metadata_calls = []
        native_deadlines = []
        def delayed(connection, statement, parameters, deadline):
            if statement == reader._METADATA_SQL:
                metadata_calls.append(deadline)
                if len(metadata_calls) == 1:
                    return execute(connection, 'SELECT pg_sleep(0.2)', (), deadline)
            elif statement.startswith('WITH requested AS MATERIALIZED'):
                native_deadlines.append(deadline)
            return execute(connection, statement, parameters, deadline)
        with patch.object(self.store, '_execute_bounded', side_effect=delayed):
            self.assertEqual(self.stream(), baseline)
        self.assertEqual(len(metadata_calls), 2)
        self.assertEqual(len(native_deadlines), 1)
        # The100ms optional phase shares the original6s deadline; it cannot renew it.
        self.assertGreater(native_deadlines[0] - metadata_calls[0], 5.8)
        self.assertLessEqual(native_deadlines[0] - metadata_calls[0], 5.91)

    def test_concurrent_edit_keeps_snapshot_then_next_stream_rejects(self):
        self.enroll()
        baseline = self.stream(False)
        original = reader.partition
        def changed(*args, **kwargs):
            self.edit()
            return original(*args, **kwargs)
        with patch.object(reader, 'partition', side_effect=changed):
            self.assertEqual(self.stream(), baseline)
        self.assertEqual(self.stream(), [])

    def test_partial_forget_falls_back_and_matches_native_before_cleanup(self):
        self.enroll()
        # The existing native Forget test proves invalidation before archive cleanup,
        # including actual canonical mutation rather than a fake generation update.
        super().test_forget_revocation_precedes_archive_cleanup_failure()
        with patch('recall_server.receipt_membership.lookup_current_search_membership',
                   wraps=lookup_current_search_membership) as native:
            self.assertEqual(self.stream(), self.stream(False))
        self.assertEqual(native.call_count, 2)
        self.assertEqual(self.stream(), [])

    def test_source_deletion_recreation_does_not_reuse_certificates(self):
        super().test_actual_source_deletion_recreation_retains_generation_and_loses_certificate()
        self.assertEqual(self.stream(), self.stream(False))
        self.assertEqual(self.stream(), [])

    def test_retirement_invalidates_then_reenrollment_uses_same_receipts(self):
        self.enroll()
        baseline = self.stream(False)
        for receipt in (self.receipt, self.physical_receipt):
            self.admit_fixture(self.baseline(receipt)['document_id'])
        with patch('recall_server.receipt_membership.lookup_current_search_membership',
                   wraps=lookup_current_search_membership) as native:
            self.assertEqual(self.stream(), baseline)
        self.assertEqual(native.call_count, 1)
        self.enroll()
        with patch('recall_server.receipt_membership.lookup_current_search_membership',
                   wraps=lookup_current_search_membership) as native:
            self.assertEqual(self.stream(), baseline)
        native.assert_not_called()

    def test_publication_revision_change_uses_native_without_dropping_passage(self):
        self.enroll()
        with self.store.connect() as c:
            c.execute('UPDATE canonical_passage_documents SET revision=revision+1 WHERE tenant_id=%s',
                      (self.tenant,))
        baseline = self.stream(False)
        self.assertTrue(baseline)
        with patch('recall_server.receipt_membership.lookup_current_search_membership',
                   wraps=lookup_current_search_membership) as native:
            self.assertEqual(self.stream(), baseline)
        self.assertEqual(native.call_count, 1)

    def test_no_certificate_overhead_is_one_optional_query_per_batch(self):
        original = self.store._execute_bounded
        statements = []
        def counted(connection, statement, parameters, deadline):
            statements.append(statement)
            return original(connection, statement, parameters, deadline)
        start = time.monotonic()
        with patch.object(self.store, '_execute_bounded', side_effect=counted):
            enabled = self.stream()
        enabled_ms = (time.monotonic() - start) * 1000
        self.assertEqual(sum(statement == reader._METADATA_SQL for statement in statements), 1)
        self.assertEqual(sum(statement.startswith('WITH requested AS MATERIALIZED') for statement in statements), 1)
        start = time.monotonic()
        disabled = self.stream(False)
        disabled_ms = (time.monotonic() - start) * 1000
        self.assertEqual(enabled, disabled)
        print({'local_no_certificate_enabled_ms': round(enabled_ms, 3),
               'local_disabled_ms': round(disabled_ms, 3)}, flush=True)


if __name__ == '__main__':
    suite = unittest.TestSuite(ParquetCertificateTests(name) for name in ParquetCertificateTests.__dict__
                              if name.startswith('test_'))
    raise SystemExit(not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful())
