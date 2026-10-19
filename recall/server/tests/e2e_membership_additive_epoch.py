"""Native membership stability across additive and destructive canonical writes."""
import os
import hashlib
import json
from pathlib import Path
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import psycopg
import time
import unittest
from unittest.mock import patch

import e2e_receipt_membership as receipt_fixture
from e2e_compact_canonical_bodies import DurableArchiveFixture
from e2e_membership_remainder import MembershipRemainderTests
from recall_server import parent_membership_enrollment as enrollment
from recall_server import membership_remainder as remainder
from recall_server.chunk_retirement import retire_current_chunks, retire_singleton_metadata


class AdditiveEpochTests(MembershipRemainderTests):
    def setUp(self):
        with patch.object(receipt_fixture, 'FilesystemArchiveStore', DurableArchiveFixture):
            super().setUp()

    def rows(self):
        with self.store.connect() as connection:
            return connection.execute(enrollment._SELECT,
                (self.tenant, self.source, 'session', list(self.scope['passage_ids']))).fetchall()

    def generation(self):
        return self.rows()[0]['generation']

    def publish(self):
        self.assertEqual(self.projector.project_pending(tenant_id=self.tenant,
            batch_size=10, max_batches=1, upload_concurrency=1)['failed'], 0)
        result = self.publisher(self.settings(enabled=False)).project_pending(
            tenant_id=self.tenant, batch_size=10, max_batches=1)
        self.assertEqual(result['documents'], 1)

    def admit(self, receipt):
        document = self.baseline(receipt)['document_id']
        scope = dict(tenant_id=self.tenant, source_id=self.source, document_ids=(document,))
        with patch.dict(os.environ, RECALL_CHUNK_BODY_READS='archive'), \
                patch.object(self.store, 'verified_singleton_receipts', True), \
                patch.object(self.store, 'search_plane', 'turbopuffer'):
            preview = retire_current_chunks(self.store, self.archive, **scope)
            retire_current_chunks(self.store, self.archive, **scope,
                apply=True, reviewed_plan=preview['plan'])
            preview = retire_singleton_metadata(self.store, self.archive,
                **scope, principal_id=self.principal)
            applied = retire_singleton_metadata(self.store, self.archive,
                **scope, principal_id=self.principal, apply=True, reviewed_plan=preview['plan'])
        self.assertEqual(applied['retired_chunks'], 1)
        return document

    def test_distinct_native_append_and_publication_preserve_exact_prefix(self):
        self.ingest('turn/long-prefix', 'Exact immutable prefix with independent receipts. ' * 400)
        self.publish()
        self.refresh_scope()
        self.assertEqual(self.enroll()['status'], 'enrolled')
        before = {row['passage_id']: row for row in self.rows()}
        generation = self.generation()
        self.ingest('zzzz/later-tail', 'A genuinely new native event.')
        self.publish()
        retained = [row for row in self.rows() if row['text_sha256'] == before[row['passage_id']]['text_sha256']
                    and row['receipts'] == before[row['passage_id']]['receipts']]
        self.assertTrue(retained)
        self.scope['passage_ids'] = tuple(row['passage_id'] for row in retained)
        self.assertEqual(self.compare()['native_allowed_passages'], len(retained))
        self.assertEqual(self.generation(), generation)
        self.assertEqual(self.compare()['certified_passages'], len(retained))
        self.assertEqual(self.compare()['disagreements'], 0)

    def test_additive_append_between_capture_and_stamp_preserves_native_proof(self):
        original = enrollment._capture

        def append_after_capture(*args, **kwargs):
            captured = original(*args, **kwargs)
            self.ingest('zzzz/after-capture', 'New independent evidence after proof capture.')
            return captured

        with patch.object(enrollment, '_capture', side_effect=append_after_capture):
            self.assertEqual(self.enroll()['status'], 'enrolled')
        self.assertEqual(self.compare()['certified_passages'], len(self.scope['passage_ids']))
        self.assertEqual(self.compare()['disagreements'], 0)

    def test_profile_one_wrong_ordinal_insert_invalidates_existing_certificate(self):
        document = self.admit(self.receipt)
        self.assertEqual(self.enroll()['status'], 'enrolled')
        generation = self.generation()
        with self.store.connect() as connection:
            connection.execute('''INSERT INTO canonical_chunks(
                tenant_id,source_id,chunk_id,document_id,ordinal,receipt,text_redacted,text_sha256)
                SELECT tenant_id,source_id,document_id||'-wrong-ordinal',document_id,1,%s,'',text_sha256
                FROM canonical_documents WHERE tenant_id=%s AND document_id=%s''',
                (self.receipt.rsplit('#item=', 1)[0] + '#item=1', self.tenant, document))
        self.assertGreater(self.generation(), generation)
        from recall_server.receipt_membership import ReceiptMembershipError
        with self.assertRaises(ReceiptMembershipError):
            self.compare()

    def test_new_malformed_foreign_physical_receipt_is_rejected(self):
        self.admit(self.receipt)
        other_receipt = self.ingest('zzzz/foreign-document', 'Separate native physical owner.')['receipt']
        other = self.baseline(other_receipt)
        with self.store.connect() as connection:
            connection.execute("UPDATE canonical_events SET native_parent_id='foreign-parent' "
                'WHERE tenant_id=%s AND native_id=%s', (self.tenant, other['native_id']))
        self.assertEqual(self.enroll()['status'], 'enrolled')
        generation = self.generation()
        with self.assertRaisesRegex(psycopg.errors.CheckViolation, 'canonical_physical_receipt_identity_invalid'):
            with self.store.connect() as connection:
                connection.execute('''INSERT INTO canonical_chunks(
                    tenant_id,source_id,chunk_id,document_id,ordinal,receipt,text_redacted,text_sha256)
                    SELECT tenant_id,source_id,chunk_id||'-foreign-receipt',document_id,1,%s,'',text_sha256
                    FROM canonical_chunks WHERE tenant_id=%s AND receipt=%s''',
                    (self.receipt, self.tenant, other_receipt))
        self.assertEqual(self.generation(), generation)
        self.assertEqual(self.enroll()['status'], 'enrolled')
        self.assertEqual(self.compare()['disagreements'], 0)

    @contextmanager
    def legacy_collector(self):
        # Historical fixture uses the exact old producer contract. Restore the
        # candidate before any enrollment, transition or assertion under test.
        schema = Path(__file__).resolve().parents[1] / 'schema'
        old = (schema/'082_parent_authority_generation.sql').read_text()
        function = old[old.index('CREATE OR REPLACE FUNCTION recall_collect_parent_authority()'):
                       old.index('-- Transition tables')]
        with self.store.connect() as connection:
            connection.execute(function)
        try:
            yield
        finally:
            with self.store.connect() as connection:
                connection.execute((schema/'092_additive_membership_generation.sql').read_text())

    def test_exact_native_singleton_admission_preserves_membership_certificate(self):
        self.assertEqual(self.enroll()['status'], 'enrolled')
        generation = self.generation()
        self.admit(self.receipt)
        self.assertEqual(self.compare()['native_allowed_passages'], len(self.scope['passage_ids']))
        self.assertEqual(self.generation(), generation)
        self.assertEqual(self.compare()['certified_passages'], len(self.scope['passage_ids']))
        self.assertEqual(self.compare()['disagreements'], 0)

    def test_inactive_foreign_receipt_document_activation_invalidates_derived_owner(self):
        self.admit(self.receipt)
        foreign_receipt = self.ingest('zzzz/inactive-foreign', 'Inactive foreign physical owner.')['receipt']
        foreign = self.baseline(foreign_receipt)
        with self.legacy_collector(), self.store.connect() as connection:
            connection.execute("UPDATE canonical_events SET native_parent_id='foreign-parent' "
                'WHERE tenant_id=%s AND native_id=%s', (self.tenant, foreign['native_id']))
            connection.execute('UPDATE canonical_documents SET is_current=false '
                'WHERE tenant_id=%s AND document_id=%s', (self.tenant, foreign['document_id']))
            connection.execute('''INSERT INTO canonical_chunks(
                tenant_id,source_id,chunk_id,document_id,ordinal,receipt,text_redacted,text_sha256)
                SELECT tenant_id,source_id,chunk_id||'-inactive-collision',document_id,1,%s,'',text_sha256
                FROM canonical_chunks WHERE tenant_id=%s AND receipt=%s''',
                (self.receipt, self.tenant, foreign_receipt))
        self.assertEqual(self.enroll()['status'], 'enrolled')
        generation = self.generation()
        with self.assertRaisesRegex(psycopg.errors.CheckViolation, 'canonical_physical_receipt_identity_invalid'):
            with self.store.connect() as connection:
                connection.execute('UPDATE canonical_documents SET is_current=true '
                    'WHERE tenant_id=%s AND document_id=%s', (self.tenant, foreign['document_id']))
        self.assertEqual(self.enroll()['status'], 'enrolled')
        self.assertEqual(self.generation(), generation)
        self.assertEqual(self.compare()['certified_passages'], len(self.scope['passage_ids']))

    def test_pending_queue_proves_prefix_while_append_and_retirement_remain_pending(self):
        self.interrupted_publication()
        with self.store.connect() as connection:
            connection.execute('''INSERT INTO canonical_chunk_retirement_progress(
                tenant_id,source_id,native_parent_id,logical_document_id,enabled,status)
                SELECT tenant_id,source_id,native_parent_id,logical_document_id,true,'pending'
                FROM canonical_evidence_documents WHERE tenant_id=%s''', (self.tenant,))
        self.ingest('zzzz/still-arriving', 'New independent event while proof queue is pending.')
        queued = self.queued()
        self.assertEqual(len(queued), 1)
        with self.store.connect() as connection:
            scope = connection.execute(remainder.CURRENT,
                (self.tenant, self.source, queued[0]['logical_document_id'])).fetchone()
        self.assertTrue(scope['logical_pending'])
        self.assertTrue(scope['retirement_pending'])
        self.assertFalse(scope['retirement_intent'])
        result = remainder.resume(self.store, queued[0], settings=self.settings(),
            deadline_at=time.monotonic()+3)
        self.assertEqual(result['status'], 'enrolled')
        self.assertGreater(result['certified_passages'], 0)
        self.assertEqual(self.queued(), [])
        before = self.compare()['certified_passages']
        from recall_server.chunk_retirement import ChunkRetirementError
        # Native admission itself still refuses a publication-pending parent.
        with self.assertRaisesRegex(ChunkRetirementError, 'singleton_metadata_publication_pending'):
            self.admit(self.receipt)
        self.assertEqual(self.compare()['certified_passages'], before)
        self.assertEqual(self.compare()['disagreements'], 0)
        self.edit()
        self.assertEqual(self.compare()['certified_passages'], 0)

    def test_v2_v3_mixed_readers_use_native_fallback_until_recertified(self):
        # Exact pre-v3 binding fields/domain; neither reader accepts the other
        # version's certificate merely because the parent generation matches.
        legacy_fields = (
            'tenant_id', 'source_id', 'native_parent_id', 'logical_document_id',
            'passage_id', 'policy_fingerprint', 'text_sha256', 'receipts',
            'manifest_content_sha256', 'manifest_revision', 'projected_revision',
            'passage_revision', 'source_document_sha256',
        )

        def legacy(row, scope):
            payload = ['recall.passage-membership-binding.v2', scope['binding_epoch'],
                       *[row[field] for field in legacy_fields]]
            return hashlib.sha256(json.dumps(payload, ensure_ascii=False,
                separators=(',', ':'), allow_nan=False).encode()).hexdigest()

        with patch.object(enrollment, '_binding', legacy):
            self.assertEqual(self.enroll()['status'], 'enrolled')
            self.assertEqual(self.compare()['certified_passages'], len(self.ids))
        self.assertEqual(self.compare()['certified_passages'], 0)
        self.assertEqual(self.compare()['native_allowed_passages'], len(self.ids))
        self.assertEqual(self.enroll()['status'], 'enrolled')
        with patch.object(enrollment, '_binding', legacy):
            self.assertEqual(self.compare()['certified_passages'], 0)
            self.assertEqual(self.compare()['native_allowed_passages'], len(self.ids))
        self.assertEqual(self.compare()['disagreements'], 0)

    def test_metadata_admission_between_capture_and_stamp_preserves_same_proof(self):
        capture = enrollment._capture

        def admit_after_capture(*args, **kwargs):
            captured = capture(*args, **kwargs)
            self.admit(self.receipt)
            return captured

        with patch.object(enrollment, '_capture', side_effect=admit_after_capture):
            self.assertEqual(self.enroll()['status'], 'enrolled')
        self.assertEqual(self.compare()['certified_passages'], len(self.ids))
        self.assertEqual(self.compare()['disagreements'], 0)

    def test_old_schema_keeps_pending_barrier(self):
        self.interrupted_publication()
        with self.store.connect() as connection:
            connection.execute('''INSERT INTO canonical_chunk_retirement_progress(
                tenant_id,source_id,native_parent_id,logical_document_id,enabled,status)
                SELECT tenant_id,source_id,native_parent_id,logical_document_id,true,'pending'
                FROM canonical_evidence_documents WHERE tenant_id=%s''', (self.tenant,))
            connection.execute('DELETE FROM schema_migrations WHERE version=92')
        try:
            with self.legacy_collector():
                result = remainder.resume(self.store, self.queued()[0], settings=self.settings(),
                    deadline_at=time.monotonic()+3)
                self.assertEqual(result['status'], 'busy')
                self.assertEqual(result['certified_passages'], 0)
        finally:
            with self.store.connect() as connection:
                connection.execute('INSERT INTO schema_migrations(version) VALUES(92) ON CONFLICT DO NOTHING')

    def test_batched_insert_cost_uses_same_original_deferred_owner(self):
        reports = []
        statement = '''EXPLAIN(ANALYZE,BUFFERS,FORMAT JSON)
            INSERT INTO canonical_chunks(
                tenant_id,source_id,chunk_id,document_id,ordinal,receipt,text_redacted,text_sha256)
            SELECT chunk.tenant_id,chunk.source_id,chunk.chunk_id||'-cost-'||n,
                chunk.document_id,n,split_part(chunk.receipt,'#item=',1)||'#item='||n,
                '',chunk.text_sha256 FROM canonical_chunks chunk
            CROSS JOIN generate_series(1,1000)n
            WHERE chunk.tenant_id=%s AND chunk.receipt=%s'''

        def measure(name):
            with self.store.connect() as connection:
                started = time.monotonic()
                plan = connection.execute(statement, (self.tenant, self.receipt)).fetchone()['QUERY PLAN'][0]
                rows = connection.execute('SELECT count(*) AS n FROM canonical_chunks '
                    'WHERE tenant_id=%s AND ordinal>0', (self.tenant,)).fetchone()['n']
                self.assertEqual(rows, 1000)
                connection.execute('SET CONSTRAINTS ALL IMMEDIATE')
                elapsed = (time.monotonic()-started)*1000
                connection.rollback()
            reports.append(dict(owner=name, inserted=rows, statement_and_flush_ms=elapsed,
                execution_ms=plan['Execution Time'], planning_ms=plan['Planning Time'],
                triggers=plan.get('Triggers', [])))

        # Same synthetic 1,000-row native SQL, rollback after each execution.
        # Both orders expose warmth effects; these are not production timings.
        for name in ('original082', 'candidate092', 'candidate092', 'original082'):
            if name == 'original082':
                with self.legacy_collector():
                    measure(name)
            else:
                measure(name)
        path = Path(os.environ['DIAGNOSIS_OUTPUT']) / 'batch-cost.json'
        path.write_text(json.dumps(dict(reports=reports, new_indexes=0, new_tables=0), indent=2)+'\n')

    def test_changed_chunk_primary_key_cannot_hide_new_malformed_identity(self):
        self.assertEqual(self.enroll()['status'], 'enrolled')
        generation = self.generation()
        with self.assertRaisesRegex(psycopg.errors.CheckViolation, 'canonical_physical_receipt_identity_invalid'):
            with self.store.connect() as connection:
                connection.execute("UPDATE canonical_chunks SET chunk_id=chunk_id||'-renamed',"
                    "receipt=%s WHERE tenant_id=%s AND receipt=%s",
                    (self.receipt.rsplit('#item=', 1)[0]+'#item=7', self.tenant, self.receipt))
        self.assertEqual(self.generation(), generation)
        self.assertEqual(self.compare()['certified_passages'], len(self.ids))

    def test_concurrent_legacy_identity_repairs_cannot_commit_mismatched_pair(self):
        document = self.baseline(self.receipt)
        renamed = 'turn/renamed-document'
        foreign_receipt = f'recall://{self.source}/{renamed}?rev=1#item=0'
        with self.legacy_collector(), self.store.connect() as connection:
            connection.execute('UPDATE canonical_chunks SET receipt=%s WHERE tenant_id=%s AND receipt=%s',
                (foreign_receipt, self.tenant, self.receipt))
        start = Barrier(2)

        def repair_document():
            try:
                with self.store.connect() as connection:
                    connection.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ')
                    connection.execute('SELECT 1 FROM canonical_documents WHERE tenant_id=%s', (self.tenant,))
                    start.wait(timeout=2)
                    connection.execute('UPDATE canonical_documents SET native_id=%s WHERE tenant_id=%s AND document_id=%s',
                        (renamed, self.tenant, document['document_id']))
                return 'committed'
            except psycopg.errors.CheckViolation as error:
                self.assertIn('canonical_document_identity_immutable', str(error))
                return 'rejected'

        def repair_chunk():
            with self.store.connect() as connection:
                start.wait(timeout=2)
                connection.execute('UPDATE canonical_chunks SET receipt=%s WHERE tenant_id=%s AND receipt=%s',
                    (self.receipt, self.tenant, foreign_receipt))
            return 'committed'

        with ThreadPoolExecutor(max_workers=2) as pool:
            document_future = pool.submit(repair_document)
            chunk_future = pool.submit(repair_chunk)
            self.assertEqual(document_future.result(timeout=4), 'rejected')
            self.assertEqual(chunk_future.result(timeout=4), 'committed')
        with self.store.connect() as connection:
            row = connection.execute('SELECT document.native_id,chunk.receipt FROM canonical_documents document '
                'JOIN canonical_chunks chunk USING(tenant_id,source_id,document_id) '
                'WHERE document.tenant_id=%s AND document.document_id=%s',
                (self.tenant, document['document_id'])).fetchone()
        self.assertEqual(row, dict(native_id=document['native_id'], receipt=self.receipt))

    def test_old_malformed_cleanup_is_allowed_and_native_owner_is_reproved(self):
        with self.legacy_collector(), self.store.connect() as connection:
            connection.execute('UPDATE canonical_chunks SET receipt=%s WHERE tenant_id=%s AND receipt=%s',
                ('recall://'+self.source+'/old-malformed?rev=1#item=0', self.tenant, self.receipt))
        self.assertEqual(self.enroll()['status'], 'membership-unproved')
        with self.store.connect() as connection:
            connection.execute("UPDATE canonical_chunks SET deleted_at=clock_timestamp(),text_redacted='' "
                'WHERE tenant_id=%s AND receipt=%s',
                (self.tenant, 'recall://'+self.source+'/old-malformed?rev=1#item=0'))
            connection.execute('DELETE FROM canonical_chunks WHERE tenant_id=%s AND receipt=%s',
                (self.tenant, 'recall://'+self.source+'/old-malformed?rev=1#item=0'))
        self.assertEqual(self.compare()['certified_passages'], 0)


    def test_legacy_malformed_owned_receipt_cleanup_invalidates_actual_parent(self):
        other_receipt = self.ingest('zzzz/legacy-owner', 'Old physical receipt owner.')['receipt']
        with self.legacy_collector(), self.store.connect() as connection:
            connection.execute('DELETE FROM canonical_chunks WHERE tenant_id=%s AND receipt=%s',
                (self.tenant, self.receipt))
            connection.execute("""INSERT INTO canonical_chunks(
                tenant_id,source_id,chunk_id,document_id,ordinal,receipt,text_redacted,text_sha256)
                SELECT tenant_id,source_id,chunk_id||'-legacy',document_id,1,%s,'',text_sha256
                FROM canonical_chunks WHERE tenant_id=%s AND receipt=%s""",
                (self.receipt, self.tenant, other_receipt))
        self.assertEqual(self.enroll()['status'], 'enrolled')
        generation = self.generation()
        with self.store.connect() as connection:
            connection.execute('DELETE FROM canonical_chunks WHERE tenant_id=%s AND receipt=%s',
                (self.tenant, self.receipt))
        self.assertGreater(self.generation(), generation)
        self.assertEqual(self.compare()['certified_passages'], 0)
        self.assertLess(self.compare()['native_allowed_passages'], len(self.ids))
        self.assertEqual(self.enroll()['status'], 'membership-unproved')

    def test_active_maintenance_intent_still_blocks_schema92_proof(self):
        self.interrupted_publication()
        with self.store.connect() as connection:
            connection.execute("""INSERT INTO canonical_retirement_source_policy(
                tenant_id,source_id,principal_id,maintenance_intent)
                VALUES(%s,%s,%s,'{"parent":"session"}'::jsonb)""",
                (self.tenant,self.source,self.principal))
        before = self.queued()
        with patch.object(enrollment, 'enroll_parent_passages',
                          side_effect=AssertionError('active intent cannot prove')):
            result = remainder.resume(self.store, before[0], settings=self.settings(),
                deadline_at=time.monotonic()+3)
        self.assertEqual(result['status'], 'busy')
        self.assertEqual(result['certified_passages'], 0)
        self.assertEqual(self.queued(), before)

    def test_profile_conversion_and_canonical_insert_cannot_keep_false_certificate(self):
        from e2e_parent_membership_fast_read import ParentMembershipFastReadTests
        from recall_server.parent_membership_read import ReadSettings
        document = self.baseline(self.receipt)['document_id']
        scope = dict(tenant_id=self.tenant,source_id=self.source,document_ids=(document,))
        # Original archive/body retirement proof first. Only the independently
        # accepted metadata transition is interleaved below; no verifier mock.
        with patch.dict(os.environ, RECALL_CHUNK_BODY_READS='archive'):
            preview = retire_current_chunks(self.store,self.archive,**scope)
            retired = retire_current_chunks(self.store,self.archive,**scope,
                apply=True,reviewed_plan=preview['plan'])
        self.assertEqual(retired['status'],'applied')
        self.assertEqual(self.enroll()['status'],'enrolled')
        generation = self.generation()
        with psycopg.connect(self.store.dsn) as converting:
            converting.execute("SET LOCAL statement_timeout='3s'")
            converting.execute('UPDATE canonical_documents SET chunk_receipt_profile=1 '
                'WHERE tenant_id=%s AND document_id=%s', (self.tenant,document))
            converting.execute('DELETE FROM canonical_chunks WHERE tenant_id=%s AND document_id=%s',
                (self.tenant,document))
            # A different connection still sees profileNULL. It writes a valid
            # receipt for ordinal1; no malformed or foreign identity is involved.
            with psycopg.connect(self.store.dsn) as inserting:
                inserting.execute("SET LOCAL statement_timeout='3s'")
                inserting.execute("""INSERT INTO canonical_chunks(
                    tenant_id,source_id,chunk_id,document_id,ordinal,receipt,text_redacted,text_sha256)
                    SELECT tenant_id,source_id,document_id||'-concurrent-ordinal',document_id,1,%s,'',text_sha256
                    FROM canonical_documents WHERE tenant_id=%s AND document_id=%s""",
                    (self.receipt.rsplit('#item=',1)[0]+'#item=1',self.tenant,document))
            # Both real transactions commit; the second physical ordinal now
            # suppresses the formerly exact derived singleton receipt.
        with patch.object(self.store,'verified_singleton_receipts',True):
            self.retrieval.parent_membership_read = ReadSettings(enabled=False)
            native = ParentMembershipFastReadTests.search(self)
            self.retrieval.parent_membership_read = ReadSettings(enabled=True,
                binding_epoch=self.scope['binding_epoch'])
            certified = ParentMembershipFastReadTests.search(self)
        observed=dict(test=self._testMethodName,generation_before=generation,
            generation_after=self.generation(),native_results=len(native['results']),
            certified_results=len(certified['results']),
            native_authority_status=native['diagnostics']['authority_status'],
            certified_authority_status=certified['diagnostics']['authority_status'])
        print(json.dumps(observed,sort_keys=True),flush=True)
        Path(os.environ['DIAGNOSIS_OUTPUT'],'profile-insert-race.json').write_text(
            json.dumps(observed,indent=2)+'\n')
        self.assertEqual(certified['results'],native['results'])
        self.assertEqual(certified['arms'],native['arms'])

    def test_conversion_epoch_bump_cannot_hide_later_insert_commit(self):
        from e2e_parent_membership_fast_read import ParentMembershipFastReadTests
        from recall_server.parent_membership_read import ReadSettings
        document = self.baseline(self.receipt)['document_id']
        scope = dict(tenant_id=self.tenant,source_id=self.source,document_ids=(document,))
        with patch.dict(os.environ, RECALL_CHUNK_BODY_READS='archive'):
            preview = retire_current_chunks(self.store,self.archive,**scope)
            self.assertEqual(retire_current_chunks(self.store,self.archive,**scope,
                apply=True,reviewed_plan=preview['plan'])['status'],'applied')
        self.assertEqual(self.enroll()['status'],'enrolled')
        before = self.generation()
        with psycopg.connect(self.store.dsn) as inserting:
            inserting.execute("SET LOCAL statement_timeout='3s'")
            inserting.execute("""INSERT INTO canonical_chunks(
                tenant_id,source_id,chunk_id,document_id,ordinal,receipt,text_redacted,text_sha256)
                SELECT tenant_id,source_id,document_id||'-late-ordinal',document_id,1,%s,'',text_sha256
                FROM canonical_documents WHERE tenant_id=%s AND document_id=%s""",
                (self.receipt.rsplit('#item=',1)[0]+'#item=1',self.tenant,document))
            with psycopg.connect(self.store.dsn) as converting:
                converting.execute("SET LOCAL statement_timeout='3s'")
                converting.execute('UPDATE canonical_documents SET chunk_receipt_profile=1 '
                    'WHERE tenant_id=%s AND document_id=%s', (self.tenant,document))
                converting.execute('DELETE FROM canonical_chunks WHERE tenant_id=%s AND document_id=%s',
                    (self.tenant,document))
                # Model the proposed conservative conversion invalidation without
                # changing runtime. A fresh proof can run after this commits but
                # before the earlier INSERT reaches its deferred commit boundary.
                converting.execute('UPDATE canonical_parent_authority SET generation=generation+1 '
                    'WHERE tenant_id=%s AND source_id=%s', (self.tenant,self.source))
            self.assertGreater(self.generation(),before)
            self.assertEqual(self.enroll()['status'],'enrolled')
            stamped = self.generation()
        with patch.object(self.store,'verified_singleton_receipts',True):
            self.retrieval.parent_membership_read = ReadSettings(enabled=False)
            native = ParentMembershipFastReadTests.search(self)
            self.retrieval.parent_membership_read = ReadSettings(enabled=True,
                binding_epoch=self.scope['binding_epoch'])
            certified = ParentMembershipFastReadTests.search(self)
        observed=dict(test=self._testMethodName,generation_before=before,
            stamped_generation=stamped,generation_after=self.generation(),
            native_results=len(native['results']),certified_results=len(certified['results']),
            native_authority_status=native['diagnostics']['authority_status'],
            certified_authority_status=certified['diagnostics']['authority_status'])
        print(json.dumps(observed,sort_keys=True),flush=True)
        Path(os.environ['DIAGNOSIS_OUTPUT'],'profile-late-insert-race.json').write_text(
            json.dumps(observed,indent=2)+'\n')
        self.assertEqual(certified['results'],native['results'])
        self.assertEqual(certified['arms'],native['arms'])

    def test_competing_canonical_physical_inserts_preserve_unique_receipt(self):
        document = self.admit(self.receipt)
        start = Barrier(2)

        def insert(index):
            try:
                with self.store.connect() as connection:
                    start.wait(timeout=2)
                    connection.execute("""INSERT INTO canonical_chunks(
                        tenant_id,source_id,chunk_id,document_id,ordinal,receipt,text_redacted,text_sha256)
                        SELECT tenant_id,source_id,document_id||%s,document_id,0,%s,'',text_sha256
                        FROM canonical_documents WHERE tenant_id=%s AND document_id=%s""",
                        ('-restore-'+str(index),self.receipt,self.tenant,document))
                return 'committed'
            except psycopg.errors.UniqueViolation:
                return 'duplicate-refused'

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(insert, (1,2)))
        self.assertCountEqual(results, ['committed','duplicate-refused'])
        self.assertEqual(self.compare()['native_allowed_passages'], len(self.ids))


    def test_recreated_source_cannot_reuse_certificate_with_retained_epoch(self):
        from psycopg import sql
        self.enroll()
        with self.store.connect() as c:
            old=c.execute('SELECT generation FROM canonical_parent_authority WHERE tenant_id=%s AND source_id=%s',
                          (self.tenant,self.source)).fetchone()['generation']
            # Disposable test database only: exercise real FK ordering and normal
            # triggers, never disable them or use TRUNCATE to simulate cleanup.
            for table in ('canonical_evidence_documents','canonical_evidence_objects',
                          'canonical_chunks','canonical_documents','canonical_events',
                          'canonical_ingest_jobs','raw_artifacts','canonical_source_grants',
                          'canonical_audit_events','canonical_evidence_document_queue',
                          'canonical_evidence_cleanup_queue','canonical_sources'):
                c.execute(sql.SQL('DELETE FROM {} WHERE tenant_id=%s AND source_id=%s').format(sql.Identifier(table)),
                          (self.tenant,self.source))
        with self.store.connect() as c:
            retained=c.execute('SELECT generation FROM canonical_parent_authority WHERE tenant_id=%s AND source_id=%s',
                               (self.tenant,self.source)).fetchone()['generation']
            self.assertGreater(retained,old)
            c.execute('INSERT INTO canonical_sources(tenant_id,source_id,owner_principal_id) VALUES(%s,%s,%s)',
                      (self.tenant,self.source,self.principal))
            c.execute("INSERT INTO canonical_source_grants(tenant_id,source_id,principal_id,permission) VALUES(%s,%s,%s,'owner')",
                      (self.tenant,self.source,self.principal))
        self.ingest('turn/search','Synthetic recreated source')
        with self.store.connect() as c:
            current=c.execute('SELECT generation FROM canonical_parent_authority WHERE tenant_id=%s AND source_id=%s',
                              (self.tenant,self.source)).fetchone()['generation']
        self.assertEqual(current,retained)
        self.assertEqual(self.compare()['status'],'incomplete')
        self.assertEqual(self.search()['results'],[])


    def test_late_epoch_writer_invalidates_already_initialized_certificate(self):
        from e2e_parent_membership_enrollment import ParentEnrollmentTests
        self.assertEqual(self.enroll()['status'], 'enrolled')
        # Reuse the original native lock/interleave/assertions with a real epoch
        # row; additive insertion no longer creates one speculatively.
        ParentEnrollmentTests.test_enrollment_publication_does_not_wait_for_authority_row_lock(self)
        self.assertEqual(self.compare()['native_allowed_passages'], len(self.ids))
        self.assertEqual(self.compare()['disagreements'], 0)

    def test_edit_between_prefixes_refuses_stale_proof_and_new_publication_recovers(self):
        ids = self.receipt_weighted_parent()
        original = remainder.change_cursor
        changed = []

        def edited(*args, **kwargs):
            result = original(*args, **kwargs)
            if kwargs.get('after') == ids[0] and not changed:
                changed.append(True)
                self.edit()
            return result

        with patch.object(remainder, 'change_cursor', side_effect=edited):
            outcome = self.run_cycle()
        self.assertEqual(outcome['membership_certified_passages'], 1)
        self.assertEqual(self.compare()['certified_passages'], 0)
        self.assertEqual(self.compare()['disagreements'], 0)
        self.assertEqual(self.queued(), [])
        with self.store.connect() as connection:
            self.assertEqual(connection.execute('SELECT count(*) AS count '
                'FROM canonical_evidence_document_queue WHERE tenant_id=%s',
                (self.tenant,)).fetchone()['count'], 1)
        projected = self.projector.project_pending(tenant_id=self.tenant,
            batch_size=10,max_batches=1,upload_concurrency=1)
        self.assertEqual(projected['failed'], 0)
        published = self.run_cycle()
        self.assertEqual(published['documents'], 1)
        self.refresh_scope()
        self.assertEqual(self.compare()['certified_passages'], len(self.scope['passage_ids']))
        self.assertEqual(self.compare()['disagreements'], 0)



    def test_stable_manifest_binding_retains_exact_native_search_result(self):
        from e2e_parent_membership_fast_read import ParentMembershipFastReadTests
        from recall_server.parent_membership_read import ReadSettings
        self.assertEqual(self.enroll()['status'], 'enrolled')
        with self.store.connect() as connection:
            connection.execute('UPDATE canonical_evidence_documents SET manifest_content_sha256=%s '
                'WHERE tenant_id=%s', ('a'*64,self.tenant))
        compared = self.compare()
        self.assertEqual(compared['native_allowed_passages'], len(self.ids))
        self.assertEqual(compared['certified_passages'], len(self.ids))
        self.assertEqual(compared['disagreements'], 0)
        with patch.object(self.store, 'verified_singleton_receipts', True):
            self.retrieval.parent_membership_read = ReadSettings(enabled=False)
            native = ParentMembershipFastReadTests.search(self)
            self.retrieval.parent_membership_read = ReadSettings(enabled=True,
                binding_epoch=self.scope['binding_epoch'])
            certified = ParentMembershipFastReadTests.search(self)
        self.assertTrue(native['results'])
        self.assertEqual(certified['results'], native['results'])
        self.assertEqual(certified['arms'], native['arms'])
        self.assertEqual(certified['diagnostics']['authority_stages']['native_receipts'], 0)


    def insert_existing_ordinal(self, connection, *, ordinal=1):
        document = self.baseline(self.receipt)['document_id']
        connection.execute("""INSERT INTO canonical_chunks(
            tenant_id,source_id,chunk_id,document_id,ordinal,receipt,text_redacted,text_sha256)
            SELECT tenant_id,source_id,document_id||'-owned-'||%s,document_id,%s,%s,'',text_sha256
            FROM canonical_documents WHERE tenant_id=%s AND document_id=%s""",
            (str(ordinal),ordinal,self.receipt.rsplit('#item=',1)[0]+'#item='+str(ordinal),
             self.tenant,document))

    def assert_native_search_equal(self):
        from e2e_parent_membership_fast_read import ParentMembershipFastReadTests
        from recall_server.parent_membership_read import ReadSettings
        self.retrieval.parent_membership_read = ReadSettings(enabled=False)
        native = ParentMembershipFastReadTests.search(self)
        self.retrieval.parent_membership_read = ReadSettings(enabled=True,
            binding_epoch=self.scope['binding_epoch'])
        certified = ParentMembershipFastReadTests.search(self)
        self.assertEqual(certified['results'],native['results'])
        self.assertEqual(certified['arms'],native['arms'])

    def test_existing_document_insert_invalidates_without_tuple_ownership(self):
        self.assertEqual(self.enroll()['status'],'enrolled')
        generation = self.generation()
        with self.store.connect() as connection:
            self.insert_existing_ordinal(connection)
        self.assertGreater(self.generation(),generation)
        self.assertEqual(self.compare()['certified_passages'],0)
        self.assert_native_search_equal()

    def test_existing_document_noop_update_owns_tuple_and_blocks_conversion(self):
        self.assertEqual(self.enroll()['status'],'enrolled')
        generation = self.generation()
        document = self.baseline(self.receipt)['document_id']
        with self.store.connect() as connection:
            connection.execute('UPDATE canonical_documents SET body_location=body_location '
                'WHERE tenant_id=%s AND document_id=%s',(self.tenant,document))
            row = connection.execute('SELECT xmin::text AS xmin,pg_current_xact_id()::text AS xid '
                'FROM canonical_documents WHERE tenant_id=%s AND document_id=%s',
                (self.tenant,document)).fetchone()
            self.assertEqual(row['xmin'],row['xid'])
            self.insert_existing_ordinal(connection)
            with self.assertRaises(psycopg.errors.LockNotAvailable):
                with psycopg.connect(self.store.dsn) as converting:
                    converting.execute("SET LOCAL lock_timeout='100ms'")
                    converting.execute('UPDATE canonical_documents SET chunk_receipt_profile=1 '
                        'WHERE tenant_id=%s AND document_id=%s',(self.tenant,document))
        self.assertEqual(self.generation(),generation)
        self.assertEqual(self.compare()['certified_passages'],len(self.scope['passage_ids']))
        self.assert_native_search_equal()

    def test_subtransaction_tuple_and_rollback_fall_back_conservatively(self):
        document = self.baseline(self.receipt)['document_id']
        for rollback in (False,True):
            with self.subTest(rollback=rollback):
                self.assertEqual(self.enroll()['status'],'enrolled')
                generation = self.generation()
                with self.store.connect() as connection:
                    connection.execute('SELECT pg_current_xact_id()')
                    with connection.transaction(force_rollback=rollback):
                        connection.execute('UPDATE canonical_documents SET body_location=body_location '
                            'WHERE tenant_id=%s AND document_id=%s',(self.tenant,document))
                    row = connection.execute('SELECT xmin::text AS xmin,pg_current_xact_id()::text AS xid '
                        'FROM canonical_documents WHERE tenant_id=%s AND document_id=%s',
                        (self.tenant,document)).fetchone()
                    self.assertNotEqual(row['xmin'],row['xid'])
                    self.insert_existing_ordinal(connection,ordinal=2 if rollback else 1)
                self.assertGreater(self.generation(),generation)
                self.assertEqual(self.compare()['certified_passages'],0)
                self.assert_native_search_equal()

    def test_top_owned_tuple_with_subtransaction_insert_and_rollback(self):
        self.assertEqual(self.enroll()['status'],'enrolled')
        generation = self.generation()
        document = self.baseline(self.receipt)['document_id']
        with self.store.connect() as connection:
            connection.execute('UPDATE canonical_documents SET body_location=body_location '
                'WHERE tenant_id=%s AND document_id=%s',(self.tenant,document))
            with connection.transaction(force_rollback=True):
                self.insert_existing_ordinal(connection,ordinal=2)
            with connection.transaction():
                self.insert_existing_ordinal(connection)
        self.assertEqual(self.generation(),generation)
        with self.store.connect() as connection:
            ordinals=[row['ordinal'] for row in connection.execute(
                'SELECT ordinal FROM canonical_chunks WHERE tenant_id=%s AND document_id=%s ORDER BY ordinal',
                (self.tenant,document))]
        self.assertEqual(ordinals,[0,1])
        self.assert_native_search_equal()
        with self.store.connect() as connection,connection.transaction(force_rollback=True):
            self.insert_existing_ordinal(connection,ordinal=2)
        self.assertEqual(self.generation(),generation)
        self.assert_native_search_equal()

    def test_xid_comparison_never_truncates_epoch_or_frozen_identity(self):
        # No synthetic wraparound is claimed. The exact SQL representation keeps
        # high xid8 bits; mismatches conservatively reject the exemption.
        with self.store.connect() as connection:
            row=connection.execute("SELECT '1'::xid::text='4294967297'::xid8::text AS wrapped, "
                "'2'::xid::text=pg_current_xact_id()::text AS frozen, "
                "pg_typeof(pg_current_xact_id())::text AS kind").fetchone()
        self.assertFalse(row['wrapped'])
        self.assertFalse(row['frozen'])
        self.assertEqual(row['kind'],'xid8')

    def test_native_set_batch_append_preserves_existing_certificate(self):
        self.ingest('turn/set-long-prefix','Immutable set-batch prefix evidence. ' * 400)
        self.publish()
        self.refresh_scope()
        self.assertEqual(self.enroll()['status'],'enrolled')
        before={row['passage_id']:row for row in self.rows()}
        generation=self.generation()
        events=[]
        for index in range(2):
            native='zzzz/set-append-'+str(index)
            content={'text':'Independent set batch evidence '+str(index),'role':'user'}
            payload=receipt_fixture.canonical_json(content)
            ref=self.gateway.put_raw(tenant_id=self.tenant,source_id=self.source,native_id=native,
                payload=payload,media_type='application/json',created_at='2026-10-14T00:00:00Z')
            events.append(dict(schema_version=1,source_id=self.source,native_id=native,
                native_parent_id='session',kind='connector_record',occurred_at='2026-10-14T00:00:00Z',
                observed_at='2026-10-14T00:00:00Z',principal_id=self.principal,visibility='private',
                content_type='application/json',content=content,provenance=dict(
                    connector_id='synthetic.membership',artifact_ref=ref),
                content_sha256=hashlib.sha256(payload).hexdigest()))
        with patch.object(self.plane,'_ingest_set_batch',wraps=self.plane._ingest_set_batch) as native_set:
            result=self.plane.ingest_batch(tenant_id=self.tenant,principal_id=self.principal,events=events)
        self.assertEqual(native_set.call_count,1)
        self.assertEqual(result['inserted'],2)
        self.publish()
        retained=[row for row in self.rows() if row['text_sha256']==before[row['passage_id']]['text_sha256']
                  and row['receipts']==before[row['passage_id']]['receipts']]
        self.assertTrue(retained)
        self.scope['passage_ids']=tuple(row['passage_id'] for row in retained)
        self.assertEqual(self.generation(),generation)
        self.assertEqual(self.compare()['certified_passages'],len(self.scope['passage_ids']))
        self.assert_native_search_equal()

    def test_owned_tuple_does_not_bypass_receipt_identity_or_fk(self):
        self.assertEqual(self.enroll()['status'],'enrolled')
        generation=self.generation()
        document=self.baseline(self.receipt)['document_id']
        with self.assertRaisesRegex(psycopg.errors.CheckViolation,'canonical_physical_receipt_identity_invalid'):
            with self.store.connect() as connection:
                connection.execute('UPDATE canonical_documents SET body_location=body_location '
                    'WHERE tenant_id=%s AND document_id=%s',(self.tenant,document))
                connection.execute("""INSERT INTO canonical_chunks(
                    tenant_id,source_id,chunk_id,document_id,ordinal,receipt,text_redacted,text_sha256)
                    SELECT tenant_id,source_id,document_id||'-malformed',document_id,1,
                      'recall://'||source_id||'/wrong-native?rev=1#item=1','',text_sha256
                    FROM canonical_documents WHERE tenant_id=%s AND document_id=%s""",
                    (self.tenant,document))
        with self.assertRaises(psycopg.errors.ForeignKeyViolation):
            with self.store.connect() as connection:
                connection.execute("""INSERT INTO canonical_chunks(
                    tenant_id,source_id,chunk_id,document_id,ordinal,receipt,text_redacted,text_sha256)
                    SELECT tenant_id,source_id,document_id||'-absent',document_id||'-absent',1,
                      'recall://'||source_id||'/absent-native?rev=1#item=1','',text_sha256
                    FROM canonical_documents WHERE tenant_id=%s AND document_id=%s""",
                    (self.tenant,document))
        self.assertEqual(self.generation(),generation)
        self.assert_native_search_equal()


if __name__ == '__main__':
    suite = unittest.TestSuite(AdditiveEpochTests(name) for name in AdditiveEpochTests.__dict__
                               if name.startswith('test_'))
    raise SystemExit(not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful())
