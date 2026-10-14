#!/usr/bin/env python3
"""Search authority parity with real canonical data and a stale local vendor."""
import unittest
import os
from contextlib import contextmanager
from unittest.mock import patch
import psycopg
import e2e_receipt_membership as fixtures
from recall_server.logical_evidence import LogicalEvidenceProjectionStore
from recall_server.passage_index import CanonicalPassageProjector
from recall_server.passage_projection import DEFAULT_PASSAGE_POLICY
from recall_server.turbopuffer_plane import TurbopufferSettings, passage_row
from recall_server.turbopuffer_retrieval import TurbopufferHintRetrieval
from tests.central_brain.fake_turbopuffer import FakeTurbopuffer


class SingletonSearchTests(fixtures.ReceiptMembershipTests):
    def prepare_search(self, *, mixed=False):
        self.receipt = self.ingest('turn/search', 'Synthetic exact deployment phrase')['receipt']
        if mixed:
            self.physical_receipt = self.ingest('turn/physical', 'Another exact deployment phrase')['receipt']
        self.projector.project_pending(tenant_id=self.tenant, batch_size=10, max_batches=1, upload_concurrency=1)
        projector = CanonicalPassageProjector(self.store, LogicalEvidenceProjectionStore(self.archive),
            policy=DEFAULT_PASSAGE_POLICY, bound_tenant_id=self.tenant)
        report = projector.project_pending(tenant_id=self.tenant, batch_size=10, max_batches=1)
        self.assertEqual(report['documents'], 1)
        with self.store.connect() as connection:
            rows = connection.execute("""SELECT passage.*,evidence.native_parent_id,evidence.manifest_object_key,
                evidence.manifest_content_sha256,evidence.first_occurred_at AS doc_first_occurred_at,
                evidence.last_occurred_at AS doc_last_occurred_at,'[]'::jsonb AS actors
                FROM canonical_passages passage JOIN canonical_evidence_documents evidence
                USING(tenant_id,source_id,logical_document_id)
                WHERE passage.tenant_id=%s AND passage.source_id=%s""", (self.tenant, self.source)).fetchall()
        self.assertTrue(rows)
        settings = TurbopufferSettings(api_key='synthetic-local-only')
        self.client = FakeTurbopuffer()
        self.namespace = self.client.namespace(settings.namespace(self.tenant))
        self.namespace.write(upsert_rows=[passage_row(row) for row in rows])
        self.retrieval = TurbopufferHintRetrieval(self.store, tenant_id=self.tenant,
            sources=[self.source], policy_fingerprint=DEFAULT_PASSAGE_POLICY.fingerprint,
            settings=settings, client=self.client)

    def search(self):
        return self.retrieval.search('exact deployment phrase', lexical_query='deployment phrase',
                                    since=None, until=None, limit=10, include_arms=True)

    def test_search_admitted_receipt_retains_exact_canonical_authority(self):
        self.prepare_search()
        original = self.search()
        self.assertTrue(original['results'])
        self.admit_fixture(self.baseline(self.receipt)['document_id'])
        self.assertEqual(self.search()['results'], [])
        with patch.object(self.store, 'verified_singleton_receipts', True):
            result = self.search()
        self.assertEqual(result['diagnostics']['authority_status'], 'ok')
        self.assertEqual(result['results'], original['results'])
        self.assertEqual(result['arms'], original['arms'])

    def test_search_mixed_profile_keeps_physical_deletion_fence(self):
        self.prepare_search(mixed=True)
        self.admit_fixture(self.baseline(self.receipt)['document_id'])
        with patch.object(self.store, 'verified_singleton_receipts', True):
            self.assertTrue(self.search()['results'])
            with self.store.connect() as connection:
                connection.execute('UPDATE canonical_chunks SET deleted_at=now() WHERE tenant_id=%s AND receipt=%s',
                                   (self.tenant, self.physical_receipt))
            result = self.search()
        self.assertEqual(result['diagnostics']['authority_status'], 'ok')
        self.assertEqual(result['results'], [])
        self.assertTrue(all(not rows for rows in result['arms'].values()))

    def test_search_atomic_admission_snapshot_has_physical_or_profile_proof(self):
        from recall_server.chunk_retirement import retire_current_chunks
        self.prepare_search()
        original = self.search()
        document = self.baseline(self.receipt)
        with patch.dict(os.environ, {'RECALL_CHUNK_BODY_READS': 'archive'}):
            args = dict(tenant_id=self.tenant, source_id=self.source, document_ids=(document['document_id'],))
            preview = retire_current_chunks(self.store, self.archive, **args)
            retired = retire_current_chunks(self.store, self.archive, **args, apply=True, reviewed_plan=preview['plan'])
            self.assertEqual(retired['status'], 'applied')
        # Fixture simulation of the future atomic admission owner; immutable
        # body proof above is the unchanged independent retirement verifier.
        with patch.object(self.store, 'verified_singleton_receipts', True):
            with self.store.connect() as connection, connection.transaction():
                connection.execute('UPDATE canonical_documents SET chunk_receipt_profile=1 WHERE tenant_id=%s AND document_id=%s',
                                   (self.tenant, document['document_id']))
                self.assertEqual(self.search()['results'], original['results'])
                connection.execute('DELETE FROM canonical_chunks WHERE tenant_id=%s AND document_id=%s',
                                   (self.tenant, document['document_id']))
                self.assertEqual(self.search()['results'], original['results'])
            self.assertEqual(self.search()['results'], original['results'])

    def test_search_independent_live_physical_proof_precedes_profile(self):
        from recall_server.receipt_membership import lookup_receipt_metadata, ReceiptMembershipError
        self.prepare_search()
        original = self.search()
        with self.store.connect() as connection:
            connection.execute('UPDATE canonical_documents SET chunk_receipt_profile=1 WHERE tenant_id=%s', (self.tenant,))
        with patch.object(self.store, 'verified_singleton_receipts', True):
            result = self.search()
        self.assertEqual(result['results'], original['results'])
        self.assertEqual(result['diagnostics']['authority_status'], 'ok')
        # This search-only precedence does not change the stronger full owner.
        with self.assertRaises(ReceiptMembershipError):
            lookup_receipt_metadata(self.store, tenant_id=self.tenant,
                authorized_sources=(self.source,), receipts=(self.receipt,), allow_verified_singletons=True)

    def test_search_profile_cannot_override_deleted_physical_row(self):
        self.prepare_search()
        with self.store.connect() as connection:
            connection.execute('UPDATE canonical_documents SET chunk_receipt_profile=1 WHERE tenant_id=%s', (self.tenant,))
            connection.execute('UPDATE canonical_chunks SET deleted_at=now() WHERE tenant_id=%s', (self.tenant,))
        self.namespace.queries.clear()
        with patch.object(self.store, 'verified_singleton_receipts', True):
            result = self.search()
        self.assertEqual(result['diagnostics']['authority_status'], 'unavailable')
        self.assertEqual(result['results'], [])
        self.assertTrue(all(not rows for rows in result['arms'].values()))
        self.assertFalse(any(query['rank_by'] == ('id', 'asc') for query in self.namespace.queries))

    def test_search_profile_cannot_override_wrong_physical_ordinal(self):
        self.prepare_search()
        with self.store.connect() as connection:
            connection.execute('UPDATE canonical_documents SET chunk_receipt_profile=1 WHERE tenant_id=%s', (self.tenant,))
            connection.execute("UPDATE canonical_chunks SET ordinal=1,receipt=replace(receipt,'#item=0','#item=1') WHERE tenant_id=%s", (self.tenant,))
        with patch.object(self.store, 'verified_singleton_receipts', True):
            result = self.search()
        self.assertEqual(result['results'], [])
        self.assertEqual(result['diagnostics']['authority_status'], 'unavailable')

    def edit(self, *, batch=False, tombstone=False, native='turn/search'):
        def ingest_batch(**kwargs):
            return self.plane.ingest_batch(tenant_id=self.tenant, principal_id=self.principal,
                                          events=[kwargs['envelope']])
        if batch:
            with patch.object(self.plane, 'ingest_document', ingest_batch):
                return self.ingest(native, '' if tombstone else 'Changed exact deployment phrase', tombstone=tombstone)
        return self.ingest(native, '' if tombstone else 'Changed exact deployment phrase', tombstone=tombstone)

    def assert_writer_hides_old_receipt(self, *, batch=False, tombstone=False):
        self.prepare_search()
        self.admit_fixture(self.baseline(self.receipt)['document_id'])
        with patch.object(self.store, 'verified_singleton_receipts', True):
            self.assertTrue(self.search()['results'])
            replay = self.ingest('turn/search', 'Synthetic exact deployment phrase')
            self.assertTrue(replay['replay'])
            self.assertTrue(self.search()['results'])
            self.edit(batch=batch, tombstone=tombstone, native='session' if tombstone else 'turn/search')
            # The unchanged stale vendor still nominates the old receipt.
            self.assertEqual(self.search()['results'], [])
        with self.store.connect() as connection:
            old = connection.execute('SELECT is_current,deleted_at FROM canonical_documents WHERE tenant_id=%s AND revision=1', (self.tenant,)).fetchone()
        self.assertFalse(old['is_current'])
        self.assertEqual(old['deleted_at'] is not None, tombstone)

    def test_search_scalar_edit_and_replay_preserve_current_authority(self):
        self.assert_writer_hides_old_receipt()

    def test_search_batch_edit_and_replay_preserve_current_authority(self):
        self.assert_writer_hides_old_receipt(batch=True)

    def test_search_scalar_parent_tombstone_hides_linked_profile(self):
        self.assert_writer_hides_old_receipt(tombstone=True)

    def test_search_batch_parent_tombstone_hides_linked_profile(self):
        self.assert_writer_hides_old_receipt(batch=True, tombstone=True)

    def test_search_forget_failure_and_retry_keep_profile_hidden(self):
        from recall_server.canonical import CanonicalLifecycleError
        self.prepare_search()
        self.admit_fixture(self.baseline(self.receipt)['document_id'])
        request = dict(contract='recall.forget-request.v1', schema_version=1,
            tenant_id=self.tenant, principal_id=self.principal, source_id=self.source,
            target_receipt=self.receipt, mode='explicit_forget', reason='owner_requested',
            requested_at='2026-10-14T00:01:00Z', idempotency_key='search-forget')
        with patch.object(self.store, 'verified_singleton_receipts', True):
            with patch.object(self.archive, 'delete_internal_raw_bounded', side_effect=OSError('synthetic archive failure')):
                with self.assertRaisesRegex(CanonicalLifecycleError, 'archive_delete_failed'):
                    self.plane.forget(request)
            self.assertEqual(self.search()['results'], [])
            self.assertEqual(self.plane.forget(request)['status'], 'deleted')
            self.assertEqual(self.search()['results'], [])

    def test_search_lost_edit_commit_ack_does_not_reauthorize_old_receipt(self):
        self.prepare_search()
        self.admit_fixture(self.baseline(self.receipt)['document_id'])
        connect, restore = self.store.connect, self.plane._restore_history
        restored = [False]
        def track_restore(*args, **kwargs):
            restore(*args, **kwargs)
            restored[0] = True
        @contextmanager
        def lost_ack():
            with connect() as connection:
                yield connection
            if restored[0]:
                raise psycopg.OperationalError('synthetic acknowledgment lost after COMMIT')
        with patch.object(self.store, 'verified_singleton_receipts', True):
            with patch.object(self.plane, '_restore_history', track_restore), patch.object(self.store, 'connect', lost_ack):
                with self.assertRaises(psycopg.OperationalError):
                    self.edit()
            self.assertEqual(self.search()['results'], [])
            self.assertTrue(self.edit()['replay'])
            self.assertEqual(self.search()['results'], [])

    def test_search_one_snapshot_does_not_cache_authority_across_requests(self):
        from recall_server.turbopuffer_retrieval import _SELECTED_AUTHORITY_SQL
        self.prepare_search()
        document = self.baseline(self.receipt)
        self.admit_fixture(document['document_id'])
        execute = self.store._execute_bounded
        observed = []
        def delete_after_catalog(connection, statement, values, deadline):
            result = execute(connection, statement, values, deadline)
            if statement == _SELECTED_AUTHORITY_SQL and not observed:
                observed.append(connection.execute("SELECT current_setting('transaction_isolation') AS isolation,current_setting('transaction_read_only') AS readonly").fetchone())
                with self.store.connect() as other:
                    other.execute('UPDATE canonical_documents SET is_current=false,deleted_at=now() WHERE tenant_id=%s AND document_id=%s',
                                  (self.tenant, document['document_id']))
            return result
        with patch.object(self.store, 'verified_singleton_receipts', True):
            with patch.object(self.store, '_execute_bounded', delete_after_catalog):
                result = self.search()
            self.assertTrue(result['results'])
            self.assertEqual(observed, [dict(isolation='repeatable read', readonly='on')])
            next_request = self.search()
        self.assertEqual(next_request['results'], [])
        self.assertTrue(all(not rows for rows in next_request['arms'].values()))


if __name__ == '__main__':
    names = [name for name in dir(SingletonSearchTests) if name.startswith('test_search_')]
    result = unittest.TextTestRunner(verbosity=2).run(unittest.TestSuite(SingletonSearchTests(name) for name in names))
    raise SystemExit(not result.wasSuccessful())
