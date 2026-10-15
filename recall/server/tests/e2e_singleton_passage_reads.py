#!/usr/bin/env python3
"""Exact passage metadata and source-opening parity after singleton admission."""
import unittest
from unittest.mock import patch
import e2e_receipt_membership as fixtures
from recall_server.canonical_retrieval import BoundCanonicalRetrieval
from recall_server.logical_evidence import LogicalEvidenceProjectionStore
from recall_server.passage_index import CanonicalPassageProjector
from recall_server.passage_projection import DEFAULT_PASSAGE_POLICY


class SingletonPassageReads(fixtures.ReceiptMembershipTests):
    def prepare_passages(self, *, mixed=False):
        self.receipt = self.ingest('turn/passage', 'Exact archived passage α 🧠 ' * 16)['receipt']
        if mixed:
            self.physical_receipt = self.ingest('turn/physical', 'Physical passage body ' * 16)['receipt']
        self.projector.project_pending(tenant_id=self.tenant, batch_size=10, max_batches=1, upload_concurrency=1)
        CanonicalPassageProjector(self.store, LogicalEvidenceProjectionStore(self.archive),
            bound_tenant_id=self.tenant, policy=DEFAULT_PASSAGE_POLICY).project_pending(tenant_id=self.tenant, batch_size=10, max_batches=1, concurrency=1)
        with self.store.connect() as connection:
            evidence = connection.execute('SELECT logical_document_id,revision,manifest_content_sha256 FROM canonical_evidence_documents WHERE tenant_id=%s', (self.tenant,)).fetchone()
            passage = connection.execute('SELECT passage_id FROM canonical_passages WHERE tenant_id=%s ORDER BY ordinal LIMIT 1', (self.tenant,)).fetchone()
        self.args = dict(source_id=self.source, **evidence, passage_ids=[passage['passage_id']])
        self.bound = BoundCanonicalRetrieval(self.store, tenant_id=self.tenant, principal_id=self.principal,
            authorized_sources=(self.source,), chunk_body_archive=self.archive)

    def opening_target(self):
        with self.store.connect() as connection:
            connection.execute("INSERT INTO brain_organizations VALUES (%s,'company','Synthetic opening',now())", (self.tenant,))
            connection.execute("INSERT INTO brain_spaces(tenant_id,organization_id,brain_kind,slug) VALUES (%s,%s,'company','synthetic')", (self.tenant, self.tenant))
            connection.execute("INSERT INTO brain_access_grants(tenant_id,principal_id,permission) VALUES (%s,%s,'read')", (self.tenant, self.principal))
        target = {key: value for key, value in self.args.items() if key != 'passage_ids'}
        target['passage_id'] = self.args['passage_ids'][0]
        return target

    def test_passage_metadata_preserves_exact_receipts_and_digest(self):
        self.prepare_passages()
        baseline = self.bound.passage_metadata(**self.args)
        self.admit_fixture(self.baseline(self.receipt)['document_id'])
        with self.assertRaisesRegex(ValueError, 'passage_metadata_unavailable'):
            self.bound.passage_metadata(**self.args)
        with patch.object(self.store, 'verified_singleton_receipts', True):
            self.assertEqual(self.bound.passage_metadata(**self.args), baseline)

    def test_passage_source_opening_preserves_exact_pages_and_resume_cursor(self):
        from recall_server.mcp import _encoded_result_size

        self.prepare_passages()
        target = self.opening_target()
        baseline = self.bound.show(target)
        page_bytes = _encoded_result_size(baseline) - 128
        first = self.bound.show(target, page_bytes=page_bytes)
        self.assertFalse(first['complete'])
        second = self.bound.show(target, page_bytes=page_bytes, cursor=first['next_cursor'])
        self.admit_fixture(self.baseline(self.receipt)['document_id'])
        with patch.object(self.store, 'verified_singleton_receipts', True):
            self.assertEqual(self.bound.show(target), baseline)
            self.assertEqual(self.bound.show(target, page_bytes=page_bytes), first)
            self.assertEqual(self.bound.show(target, page_bytes=page_bytes, cursor=first['next_cursor']), second)

    def test_passage_mixed_source_membership_and_physical_deletion_remain_exact(self):
        self.prepare_passages(mixed=True)
        target = self.opening_target()
        baseline = self.bound.show(target)
        self.assertEqual(set(baseline['opened_receipts']), {self.receipt, self.physical_receipt})
        self.admit_fixture(self.baseline(self.receipt)['document_id'])
        with patch.object(self.store, 'verified_singleton_receipts', True):
            self.assertEqual(self.bound.show(target), baseline)
            with self.store.connect() as connection:
                connection.execute('UPDATE canonical_chunks SET deleted_at=now() WHERE tenant_id=%s AND receipt=%s',
                                   (self.tenant, self.physical_receipt))
            with self.assertRaises(ValueError):
                self.bound.show(target)

    def test_passage_profile_opening_requires_current_brain_and_source_grants(self):
        self.prepare_passages()
        target = self.opening_target()
        self.admit_fixture(self.baseline(self.receipt)['document_id'])
        with patch.object(self.store, 'verified_singleton_receipts', True):
            for table in ('brain_access_grants', 'canonical_source_grants'):
                with self.store.connect() as connection:
                    rows = connection.execute(f'DELETE FROM {table} WHERE tenant_id=%s AND principal_id=%s RETURNING *',
                                              (self.tenant, self.principal)).fetchall()
                self.assertTrue(rows)
                with self.assertRaises(ValueError):
                    self.bound.show(target)
                with self.store.connect() as connection:
                    for row in rows:
                        columns = list(row)
                        connection.execute(f"INSERT INTO {table} ({','.join(columns)}) VALUES ({','.join(['%s'] * len(columns))})", tuple(row.values()))
                self.assertTrue(self.bound.show(target)['complete'])

    def test_passage_metadata_rejects_wrong_parent_and_later_tombstone(self):
        self.prepare_passages()
        self.admit_fixture(self.baseline(self.receipt)['document_id'])
        with patch.object(self.store, 'verified_singleton_receipts', True):
            with self.store.connect() as connection:
                connection.execute("UPDATE canonical_events SET native_parent_id='other-parent' WHERE tenant_id=%s", (self.tenant,))
            with self.assertRaises(ValueError):
                self.bound.passage_metadata(**self.args)
            with self.store.connect() as connection:
                connection.execute("UPDATE canonical_events SET native_parent_id='session' WHERE tenant_id=%s", (self.tenant,))
            self.ingest('turn/passage', '', tombstone=True)
            with self.assertRaises(ValueError):
                self.bound.passage_metadata(**self.args)

    def test_passage_profile_corruption_and_missing_archive_fail_closed(self):
        from recall_server.receipt_membership import ReceiptMembershipError
        self.prepare_passages()
        target = self.opening_target()
        document = self.baseline(self.receipt)
        self.admit_fixture(document['document_id'])
        with patch.object(self.store, 'verified_singleton_receipts', True):
            with patch.object(self.bound, 'chunk_body_archive', None):
                with self.assertRaises(ValueError):
                    self.bound.show(target)
            with self.store.connect() as connection:
                connection.execute('UPDATE canonical_documents SET chunk_receipt_profile=2 WHERE tenant_id=%s', (self.tenant,))
            with self.assertRaises(ReceiptMembershipError):
                self.bound.show(target)

    def test_passage_metadata_has_one_authority_snapshot_and_next_request_denies(self):
        self.prepare_passages()
        document = self.baseline(self.receipt)
        baseline = self.bound.passage_metadata(**self.args)
        self.admit_fixture(document['document_id'])
        execute, deleted = self.store._execute_bounded, []
        def delete_after_catalog(connection, statement, params, deadline):
            cursor = execute(connection, statement, params, deadline)
            if 'SELECT passage.passage_id' in statement and not deleted:
                deleted.append(True)
                with self.store.connect() as writer:
                    writer.execute('UPDATE canonical_documents SET is_current=false,deleted_at=now() WHERE tenant_id=%s AND document_id=%s',
                                   (self.tenant, document['document_id']))
            return cursor
        with patch.object(self.store, 'verified_singleton_receipts', True):
            with patch.object(self.store, '_execute_bounded', delete_after_catalog):
                self.assertEqual(self.bound.passage_metadata(**self.args), baseline)
            with self.assertRaises(ValueError):
                self.bound.passage_metadata(**self.args)
        self.assertEqual(deleted, [True])

    def test_passage_grant_revocation_during_archive_read_prevents_response(self):
        self.prepare_passages()
        target = self.opening_target()
        self.admit_fixture(self.baseline(self.receipt)['document_id'])
        read, revoked = self.archive.read_raw, []
        def revoke_after_read(reference):
            payload = read(reference)
            if not revoked:
                revoked.append(True)
                with self.store.connect() as connection:
                    connection.execute('DELETE FROM brain_access_grants WHERE tenant_id=%s AND principal_id=%s',
                                       (self.tenant, self.principal))
            return payload
        with patch.object(self.store, 'verified_singleton_receipts', True):
            with patch.object(self.archive, 'read_raw', revoke_after_read):
                with self.assertRaises(ValueError):
                    self.bound.show(target)
        self.assertEqual(revoked, [True])


if __name__ == '__main__':
    names = [name for name in dir(SingletonPassageReads) if name.startswith('test_passage_')]
    result = unittest.TextTestRunner(verbosity=2).run(unittest.TestSuite(SingletonPassageReads(name) for name in names))
    raise SystemExit(not result.wasSuccessful())
