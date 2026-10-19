#!/usr/bin/env python3
"""Native publication can reuse exact current membership certificates."""
import os
import hashlib
import time
import unittest
from unittest.mock import patch

from e2e_parent_membership_enrollment import ParentEnrollmentTests
from recall_server import parent_membership_read, receipt_membership
from recall_server.db import SearchDeadlineExceeded
from recall_server.turbopuffer_plane import TurbopufferSettings
from recall_server.turbopuffer_projection import TurbopufferProjector


class PublicationCertificates(ParentEnrollmentTests):
    def setUp(self):
        super().setUp()
        # Match the worker's finite publication budget, not the unit fixture's
        # short search default (which intentionally reserves no certificate time).
        previous = self.store.search_deadline_ms
        self.addCleanup(setattr, self.store, 'search_deadline_ms', previous)
        self.store.search_deadline_ms = 3000

    def publisher(self, *, enabled=True, epoch='synthetic-epoch-a'):
        publisher = TurbopufferProjector(
            self.store, TurbopufferSettings(api_key='synthetic-local-only'), client=self.client,
        )
        publisher.parent_membership_read = parent_membership_read.ReadSettings(
            enabled=enabled, binding_epoch=epoch,
        )
        return publisher

    def publication_rows(self, publisher=None):
        with patch.object(self.store, 'verified_singleton_receipts', True):
            with self.store.connect() as connection, connection.transaction():
                connection.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
                return (publisher or self.publisher()).passages_by_id(connection, self.tenant, list(self.ids))

    def test_certified_current_page_keeps_rows_without_repeating_receipt_proof(self):
        self.assertEqual(self.enroll()['status'], 'enrolled')
        with patch.dict(os.environ, {
            'RECALL_PARENT_MEMBERSHIP_READ': '1',
            'RECALL_PARENT_MEMBERSHIP_EPOCH': self.scope['binding_epoch'],
        }), patch.object(self.store, 'verified_singleton_receipts', True):
            publisher = TurbopufferProjector(
                self.store, TurbopufferSettings(api_key='synthetic-local-only'),
                client=self.client,
            )
            deadline = time.monotonic() + 3
            with self.store.connect() as connection, connection.transaction():
                connection.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
                # Use the actual native catalog/body owner, not synthetic rows.
                rows = publisher.passages_by_id(connection, self.tenant, list(self.ids))
                self.assertEqual({row['passage_id'] for row in rows}, set(self.ids))
                certified, report = parent_membership_read.partition(
                    self.store, connection,
                    selected=[dict(row, tenant_id=self.tenant) for row in rows],
                    settings=parent_membership_read.ReadSettings.from_env(),
                    deadline_at=deadline,
                )
                self.assertEqual(report['status'], 'partitioned')
                self.assertEqual(certified, {(self.source, passage_id) for passage_id in self.ids})
                with patch.object(receipt_membership, 'lookup_current_search_membership',
                                  wraps=receipt_membership.lookup_current_search_membership) as proof:
                    actual = publisher.passages_by_id(connection, self.tenant, list(self.ids))
                self.assertEqual(actual, rows)
                self.assertEqual(proof.call_count, 0,
                                 'already certified current page repeats native receipt proof')

    def test_disabled_missing_wrong_epoch_and_binding_tamper_keep_native_rows(self):
        before = self.publication_rows(self.publisher(enabled=False))
        for mode in ('missing', 'disabled', 'epoch', 'binding', 'null_generation', 'null_binding'):
            with self.subTest(mode=mode):
                if mode != 'missing':
                    self.assertEqual(self.enroll()['status'], 'enrolled')
                if mode == 'binding':
                    with self.store.connect() as connection:
                        connection.execute('UPDATE canonical_passages SET membership_binding_sha256=%s WHERE tenant_id=%s',
                                           ('0' * 64, self.tenant))
                if mode in ('null_generation', 'null_binding'):
                    field = 'membership_generation' if mode == 'null_generation' else 'membership_binding_sha256'
                    with self.store.connect() as connection:
                        connection.execute(f'UPDATE canonical_passages SET {field}=NULL WHERE tenant_id=%s',
                                           (self.tenant,))
                publisher = self.publisher(enabled=mode != 'disabled',
                                           epoch='other-epoch' if mode == 'epoch' else 'synthetic-epoch-a')
                execute = self.store._execute_bounded
                optional = []
                def recorded(connection, sql, values, deadline):
                    if sql == parent_membership_read._METADATA_SQL:
                        optional.append(values)
                    return execute(connection, sql, values, deadline)
                with patch.object(self.store, '_execute_bounded', side_effect=recorded), patch.object(
                                  receipt_membership, 'lookup_current_search_membership',
                                  wraps=receipt_membership.lookup_current_search_membership) as proof:
                    actual = self.publication_rows(publisher)
                self.assertEqual(actual, before)
                self.assertEqual(proof.call_count, 1)
                self.assertEqual(set(proof.call_args.kwargs['receipts']), {self.receipt, self.physical_receipt})
                self.assertEqual(len(optional), int(mode in ('epoch', 'binding')))

    def test_mixed_page_proves_only_deduplicated_uncertified_receipts(self):
        clones = tuple('psg_' + hashlib.sha256((self.ids[0] + suffix).encode()).hexdigest()[:32]
                       for suffix in ('a', 'b'))
        with self.store.connect() as connection:
            connection.execute('UPDATE canonical_passages SET receipts=ARRAY[%s] WHERE tenant_id=%s',
                               (self.receipt, self.tenant))
            for index, passage_id in enumerate(clones, 1):
                connection.execute('''INSERT INTO canonical_passages(
                    tenant_id,source_id,logical_document_id,revision,passage_id,ordinal,
                    policy_fingerprint,target_tokens,overlap_tokens,token_count,
                    first_occurred_at,last_occurred_at,roles,receipts,spans,text_redacted,text_sha256)
                    SELECT tenant_id,source_id,logical_document_id,revision,%s,ordinal+%s,
                           policy_fingerprint,target_tokens,overlap_tokens,token_count,
                           first_occurred_at,last_occurred_at,roles,ARRAY[%s],spans,text_redacted,text_sha256
                    FROM canonical_passages WHERE tenant_id=%s AND passage_id=%s''',
                                   (passage_id, index * 100, self.physical_receipt, self.tenant, self.ids[0]))
        self.assertEqual(self.enroll()['status'], 'enrolled')
        self.ids = (*self.ids, *clones)
        before = self.publication_rows(self.publisher(enabled=False))
        execute = self.store._execute_bounded
        optional = []
        def recorded(connection, sql, values, deadline):
            if sql == parent_membership_read._METADATA_SQL:
                optional.append(values[2])
            return execute(connection, sql, values, deadline)
        with patch.object(self.store, '_execute_bounded', side_effect=recorded), patch.object(
                          receipt_membership, 'lookup_current_search_membership',
                          wraps=receipt_membership.lookup_current_search_membership) as proof:
            actual = self.publication_rows()
        self.assertEqual(actual, before)
        self.assertEqual(proof.call_count, 1)
        self.assertEqual(proof.call_args.kwargs['receipts'], (self.physical_receipt,))
        self.assertEqual(optional, [list(self.scope['passage_ids'])])

    def test_edit_invalidates_current_certificate_and_publication_rows(self):
        self.assertEqual(self.enroll()['status'], 'enrolled')
        self.assertTrue(self.publication_rows())
        self.edit()
        with patch.object(receipt_membership, 'lookup_current_search_membership',
                          wraps=receipt_membership.lookup_current_search_membership) as proof:
            self.assertEqual(self.publication_rows(), [])
        self.assertEqual(proof.call_count, 1)

    def test_unknown_singleton_profile_still_refuses_publication(self):
        for receipt in (self.receipt, self.physical_receipt):
            self.admit_fixture(self.baseline(receipt)['document_id'])
        self.assertEqual(self.enroll()['status'], 'enrolled')
        with self.store.connect() as connection:
            connection.execute('UPDATE canonical_documents SET chunk_receipt_profile=2 WHERE tenant_id=%s',
                               (self.tenant,))
        with self.assertRaises(receipt_membership.ReceiptMembershipError):
            self.publication_rows()

    def test_forget_refuses_publication_even_if_archive_cleanup_failed(self):
        super().test_forget_revocation_precedes_archive_cleanup_failure()
        self.assertEqual(self.publication_rows(), [])

    def test_optional_sql_failure_preserves_transaction_and_native_fallback(self):
        self.assertEqual(self.enroll()['status'], 'enrolled')
        before = self.publication_rows(self.publisher(enabled=False))
        execute = self.store._execute_bounded
        def failed(connection, sql, values, deadline):
            if sql == parent_membership_read._METADATA_SQL:
                return connection.execute('SELECT * FROM deliberately_missing_publication_certificate_table')
            return execute(connection, sql, values, deadline)
        with patch.object(self.store, '_execute_bounded', side_effect=failed), patch.object(
                receipt_membership, 'lookup_current_search_membership',
                wraps=receipt_membership.lookup_current_search_membership) as proof:
            self.assertEqual(self.publication_rows(), before)
        self.assertEqual(proof.call_count, 1)

    def test_optional_timeout_preserves_original_deadline_and_native_fallback(self):
        self.assertEqual(self.enroll()['status'], 'enrolled')
        rows = self.publication_rows(self.publisher(enabled=False))
        execute = self.store._execute_bounded
        attempted = []
        def delayed(connection, sql, values, deadline):
            if sql == parent_membership_read._METADATA_SQL:
                attempted.append(True)
                return execute(connection, 'SELECT pg_sleep(0.2)', (), deadline)
            return execute(connection, sql, values, deadline)
        publisher = self.publisher()
        deadline = time.monotonic() + 3
        with patch.object(self.store, 'verified_singleton_receipts', True), \
                self.store.connect() as connection, connection.transaction():
            connection.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
            page = publisher.passage_page(connection,
                dict(tenant_id=self.tenant, source_id=self.source, month=rows[0]['first_occurred_at'].date()),
                since=None, after=None, deadline_at=deadline)
            with patch.object(self.store, '_execute_bounded', side_effect=delayed), patch.object(
                    receipt_membership, 'lookup_current_search_membership',
                    wraps=receipt_membership.lookup_current_search_membership) as proof:
                self.assertEqual(publisher._authorized_publication_rows(
                    connection, self.tenant, page, deadline_at=deadline), rows)
            self.assertEqual(proof.call_count, 1)
            self.assertEqual(attempted, [True])
            self.assertEqual(proof.call_args.kwargs['deadline_at'], deadline)
            self.assertEqual(connection.execute('SELECT 1 AS healthy').fetchone()['healthy'], 1)

    def test_expired_deadline_dispatches_no_membership_proof(self):
        self.assertEqual(self.enroll()['status'], 'enrolled')
        publisher = self.publisher()
        with patch.object(self.store, 'search_deadline_ms', 0), patch.object(
                receipt_membership, 'lookup_current_search_membership') as proof:
            with self.assertRaises(SearchDeadlineExceeded):
                self.publication_rows(publisher)
        proof.assert_not_called()

    def test_concurrent_edit_uses_one_snapshot_then_next_publication_refuses(self):
        self.assertEqual(self.enroll()['status'], 'enrolled')
        before = self.publication_rows()
        partition = parent_membership_read.partition
        def edited(*args, **kwargs):
            self.edit()
            return partition(*args, **kwargs)
        with patch.object(parent_membership_read, 'partition', side_effect=edited):
            self.assertEqual(self.publication_rows(), before)
        self.assertEqual(self.publication_rows(), [])


if __name__ == '__main__':
    suite = unittest.TestSuite(PublicationCertificates(name) for name in PublicationCertificates.__dict__
                               if name.startswith('test_'))
    raise SystemExit(not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful())
