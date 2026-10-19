#!/usr/bin/env python3
"""Captured Slack history converges through the ordinary managed worker cycle."""
import json
import sqlite3
import unittest
from unittest.mock import patch

from e2e_managed_slack_history_actors import ManagedSlackHistoryActors
from connectors.sdk import ConnectorPage
from recall_server import logical_evidence_projection


class CapturedSlackActors(ManagedSlackHistoryActors):
    def captured(self, *, thin=False, enroll=True):
        worker = self.managed()
        factory = worker.connector_factory
        _, self.spool = factory()
        self.assertEqual(self.step(worker)['failed'], 0)
        self.assertEqual(self.step(worker)['failed'], 0)
        self.assertEqual(self.counts(), (0, 0))
        if thin:
            self.thin_history()
        self.project()
        self.assertEqual(self.scoped(), [])
        source = self.source
        calls = []

        class LiveOnly:
            source_id = source
            connector_id = 'slack.messages'

            def pull(self, cursor):
                # No directory/history record is ever returned after enrollment.
                calls.append(cursor)
                return ConnectorPage((), 'live-only-' + str(len(calls)), False)

        worker.connector_factory = lambda *_: (LiveOnly(), self.spool)
        if enroll:
            self.enroll()
        return worker, calls

    def converge(self, worker):
        reports = [self.step(worker) for _ in range(6)]
        self.assertTrue(all(report['failed'] == 0 for report in reports), reports)
        self.assertEqual(self.counts(), (1, 1))
        with sqlite3.connect(self.spool) as c:
            state = json.loads(c.execute("SELECT value FROM meta WHERE key='slack_captured_attribution' ").fetchone()[0])
        self.assertEqual(state['phase'], 'complete')
        self.assertEqual(state['unresolved'], 0)
        return reports

    def test_progress_reports_bind_two_actual_installations(self):
        reports = []
        for fixture in (self, type(self)(self._testMethodName)):
            if fixture is not self:
                fixture.setUp()
                self.addCleanup(fixture.doCleanups)
            worker, _ = fixture.captured()
            fixture.converge(worker)
            report = fixture.step(worker)['captured_attribution']
            reports.append((fixture, report))
            with fixture.store.connect() as c:
                c.execute("UPDATE connector_installations SET state='paused' WHERE id=%s", (fixture.installation,))
        for fixture, report in reports:
            self.assertEqual(report['installation_id'], fixture.installation)
            self.assertEqual(report['source_id'], fixture.source)
            self.assertEqual(report['status'], 'complete')
            self.assertEqual(report['phase'], 'complete')
            self.assertEqual(report['unresolved'], 0)
            self.assertFalse(report['unresolved_entries_present'])
            self.assertEqual(len(report['authority_epoch']), 64)
            self.assertEqual(len(report['frontier_sha256']), 64)
            encoded = json.dumps(report)
            self.assertNotIn(fixture.message.native_id, encoded)
            self.assertNotIn(fixture.email, encoded)
        self.assertNotEqual(reports[0][1]['installation_id'], reports[1][1]['installation_id'])
        self.assertNotEqual(reports[0][1]['source_id'], reports[1][1]['source_id'])

    def test_progress_distinguishes_unresolved_from_failed_mutation(self):
        from recall_server.archive import ArchiveNotFound
        worker, _ = self.captured(thin=True)
        self.message_phase(worker)
        with patch.object(worker.archive, 'read_raw', side_effect=ArchiveNotFound('synthetic')):
            result = self.step(worker)
        self.assertEqual(result['failed'], 0)
        report = result['captured_attribution']
        self.assertEqual(report['status'], 'unresolved')
        self.assertTrue(report['unresolved_entries_present'])
        self.assertGreater(report['unresolved'], 0)
        with patch.object(logical_evidence_projection, 'mark_logical_evidence_dirty',
                          side_effect=RuntimeError('synthetic queue failure')):
            result = self.step(worker)
        self.assertEqual(result['failed'], 1)
        self.assertNotIn('captured_attribution', result)
        self.assertEqual(self.counts(), (1, 0))

    def test_progress_rejects_zero_counter_with_persisted_debt(self):
        from recall_server import slack_captured_attribution as captured
        worker, _ = self.captured()
        self.converge(worker)
        original = captured.repair_slice
        def corrupt(*args, **kwargs):
            result = original(*args, **kwargs)
            db = args[2].db
            db.execute('INSERT INTO meta(key,value) VALUES (?,?)',
                (captured.DEBT + 'synthetic', json.dumps(dict(native_id='synthetic', reason='ArchiveNotFound'))))
            db.commit()
            return result
        with patch.object(captured, 'repair_slice', side_effect=corrupt):
            result = self.step(worker)
        self.assertEqual(result['failed'], 1)
        self.assertNotIn('captured_attribution', result)

    def test_contacts_skip_intact_message_archive(self):
        worker, _ = self.captured()
        with self.store.connect() as c:
            target = c.execute('SELECT artifact_id FROM canonical_events WHERE tenant_id=%s AND source_id=%s AND native_id=%s',
                (self.tenant, self.source, self.message.native_id)).fetchone()['artifact_id']
        original = worker.archive.read_raw
        reads = []
        def read(reference):
            reads.append(reference['artifact_id'])
            return original(reference)
        with patch.object(worker.archive, 'read_raw', side_effect=read):
            self.message_phase(worker)
        self.assertNotIn(target, reads)
        self.assertEqual(self.counts(), (1, 0))
        self.converge(worker)

    def test_message_reuses_locked_current_row(self):
        worker, _ = self.captured()
        self.message_phase(worker)
        original = worker._require_captured_slack_event
        with patch.object(worker, '_require_captured_slack_event', wraps=original) as checked:
            self.assertEqual(self.step(worker)['failed'], 0)
        message_checks = [call for call in checked.call_args_list
                          if call.args[2] == self.message.native_id]
        # One pre-archive check; the writer uses its existing locked row.
        self.assertEqual(len(message_checks), 1)
        self.assertEqual(self.counts(), (1, 1))
        self.project()
        self.assertEqual(len(self.scoped()), 1)

    def test_captured_current_message_repairs_without_history_revisit(self):
        worker, calls = self.captured()
        before, acks = self.immutable(), self.ack_rows()
        self.converge(worker)
        self.assertEqual(len(calls), 6)
        self.assertEqual(self.immutable(), before)
        self.assertEqual(self.ack_rows(), acks)
        self.project()
        self.assertEqual(len(self.scoped()), 1)

    def test_thinned_captured_payload_repairs_without_provider_history(self):
        worker, _ = self.captured(thin=True)
        before = self.immutable()
        self.converge(worker)
        self.assertEqual(self.immutable(), before)
        self.project()
        self.assertEqual(len(self.scoped()), 1)

    def test_late_enrollment_restarts_completed_local_frontier(self):
        worker, _ = self.captured(enroll=False)
        for _ in range(6):
            self.assertEqual(self.step(worker)['failed'], 0)
        self.assertEqual(self.counts(), (0, 0))
        self.enroll()
        self.converge(worker)
        self.project()
        self.assertEqual(len(self.scoped()), 1)

    def test_queue_failure_rolls_back_links_and_local_progress(self):
        worker, _ = self.captured()
        # The contact phase can commit an existing-email identity; message
        # attribution and logical dirtiness must remain a single transaction.
        for _ in range(3):
            self.assertEqual(self.step(worker)['failed'], 0)
            with sqlite3.connect(self.spool) as c:
                state = json.loads(c.execute("SELECT value FROM meta WHERE key='slack_captured_attribution'").fetchone()[0])
            if state['phase'] == 'messages':
                break
        self.assertEqual(state['phase'], 'messages')
        with patch.object(logical_evidence_projection, 'mark_logical_evidence_dirty',
                          side_effect=RuntimeError('synthetic queue failure')):
            self.assertEqual(self.step(worker)['failed'], 1)
            with sqlite3.connect(self.spool) as c:
                before = c.execute("SELECT value FROM meta WHERE key='slack_captured_attribution'").fetchone()
            state = json.loads(before[0])
            self.assertLess(state['after'], self.message.native_id)
            self.assertEqual(self.step(worker)['failed'], 1)
        self.assertEqual(self.counts(), (1, 0))
        with sqlite3.connect(self.spool) as c:
            self.assertEqual(c.execute("SELECT value FROM meta WHERE key='slack_captured_attribution'").fetchone(), before)
        self.converge(worker)

    def message_phase(self, worker):
        for _ in range(4):
            self.assertEqual(self.step(worker)['failed'], 0)
            with sqlite3.connect(self.spool) as c:
                state = json.loads(c.execute("SELECT value FROM meta WHERE key='slack_captured_attribution'").fetchone()[0])
            if state['phase'] == 'messages':
                return
        self.fail('contact phase did not finish')

    def test_missing_archive_remains_unresolved_then_recovers_without_full_replay(self):
        from recall_server.archive import ArchiveNotFound
        worker, _ = self.captured(thin=True)
        self.message_phase(worker)
        with patch.object(worker.archive, 'read_raw', side_effect=ArchiveNotFound('synthetic')):
            self.assertEqual(self.step(worker)['failed'], 0)
        self.assertEqual(self.counts(), (1, 0))
        with sqlite3.connect(self.spool) as c:
            state = json.loads(c.execute("SELECT value FROM meta WHERE key='slack_captured_attribution'").fetchone()[0])
        self.assertEqual(state['phase'], 'complete')
        self.assertGreater(state['unresolved'], 0)
        self.converge(worker)

    def test_revoked_source_during_archive_read_refuses_attribution(self):
        worker, _ = self.captured()
        self.message_phase(worker)
        original = worker.archive.read_raw
        def revoke(reference):
            payload = original(reference)
            with self.store.connect() as c:
                c.execute('DELETE FROM canonical_source_grants WHERE tenant_id=%s AND source_id=%s AND principal_id=%s',
                    (self.tenant, self.source, self.principal))
            return payload
        with patch.object(worker.archive, 'read_raw', side_effect=revoke):
            self.assertEqual(self.step(worker)['failed'], 1)
        self.assertEqual(self.counts(), (1, 0))

    def test_native_forget_during_archive_read_is_not_undone(self):
        import uuid
        worker, _ = self.captured()
        self.message_phase(worker)
        original = worker.archive.read_raw
        def forget(reference):
            payload = original(reference)
            if reference['artifact_id'] == target:
                self.plane.forget(dict(contract='recall.forget-request.v1', schema_version=1,
                    tenant_id=self.tenant, principal_id=self.principal, source_id=self.source,
                    target_receipt=receipt, mode='explicit_forget', reason='owner_requested',
                    requested_at='2026-10-19T09:00:00Z', idempotency_key=uuid.uuid4().hex))
            return payload
        with self.store.connect() as c:
            target = c.execute('SELECT artifact_id FROM canonical_events WHERE tenant_id=%s AND source_id=%s AND native_id=%s',
                (self.tenant,self.source,self.message.native_id)).fetchone()['artifact_id']
            receipt = c.execute('SELECT receipt FROM canonical_chunks chunk JOIN canonical_documents document USING(tenant_id,source_id,document_id) WHERE document.tenant_id=%s AND document.source_id=%s AND document.native_id=%s',
                (self.tenant,self.source,self.message.native_id)).fetchone()['receipt']
        with patch.object(worker.archive, 'read_raw', side_effect=forget):
            self.assertEqual(self.step(worker)['failed'], 0)
        self.assertEqual(self.counts(), (1, 0))
        self.project()
        self.assertEqual(self.scoped(), [])

    def test_one_record_slices_resume_to_whole_source_completion(self):
        worker, _ = self.captured()
        from recall_server import slack_captured_attribution
        with patch.object(slack_captured_attribution, 'MAX_PAGE_RECORDS', 1):
            for _ in range(24):
                self.assertEqual(self.step(worker)['failed'], 0)
        self.converge(worker)
        self.project()
        self.assertEqual(len(self.scoped()), 1)

    def test_unknown_repair_ack_repeats_without_duplicate_dirty_work(self):
        worker, _ = self.captured()
        self.message_phase(worker)
        original = worker._reconcile_slack_page
        def unknown(row, records, **kwargs):
            result = original(row, records, **kwargs)
            if any(record.native_id == self.message.native_id for record in records):
                raise RuntimeError('synthetic lost acknowledgement after commit')
            return result
        with patch.object(worker, '_reconcile_slack_page', side_effect=unknown):
            self.assertEqual(self.step(worker)['failed'], 1)
        self.assertEqual(self.counts(), (1, 1))
        with self.store.connect() as c:
            before = c.execute('SELECT * FROM canonical_evidence_document_queue WHERE tenant_id=%s AND source_id=%s ORDER BY native_parent_id',
                (self.tenant,self.source)).fetchall()
        self.converge(worker)
        with self.store.connect() as c:
            self.assertEqual(c.execute('SELECT * FROM canonical_evidence_document_queue WHERE tenant_id=%s AND source_id=%s ORDER BY native_parent_id',
                (self.tenant,self.source)).fetchall(), before)

    def test_local_timeout_and_provider_failure_do_not_starve_other_lane(self):
        from recall_server.archive import ArchiveDeadlineExceeded
        worker, calls = self.captured()
        self.message_phase(worker)
        prior = len(calls)
        deadlines = []
        def unavailable(reference, *, deadline_at):
            deadlines.append(deadline_at)
            raise ArchiveDeadlineExceeded()
        with patch.object(worker.archive, 'read_raw_bounded', unavailable, create=True):
            self.assertEqual(self.step(worker)['failed'], 0)
        self.assertTrue(deadlines)
        self.assertEqual(len(calls), prior + 1)
        self.assertEqual(self.counts(), (1, 0))
        factory = worker.connector_factory
        connector, spool = factory()
        with patch.object(connector, 'pull', side_effect=RuntimeError('synthetic provider failure')):
            worker.connector_factory = lambda *_: (connector, spool)
            self.assertEqual(self.step(worker)['failed'], 1)
        worker.connector_factory = factory
        # Local reconciliation was ACKed before the failed provider call.
        self.assertEqual(self.counts(), (1, 1))
        self.converge(worker)
        with self.store.connect() as c:
            self.assertIsNone(c.execute('SELECT lease_owner FROM connector_installations WHERE id=%s',
                (self.installation,)).fetchone()['lease_owner'])

    def test_expired_lease_during_archive_read_refuses_native_write(self):
        worker, _ = self.captured()
        self.message_phase(worker)
        original = worker.archive.read_raw
        def expire(reference):
            payload = original(reference)
            with self.store.connect() as c:
                c.execute("UPDATE connector_installations SET lease_expires_at=now()-interval '1 second' WHERE id=%s",
                    (self.installation,))
            return payload
        with patch.object(worker.archive, 'read_raw', side_effect=expire):
            self.assertEqual(self.step(worker)['failed'], 1)
        self.assertEqual(self.counts(), (1, 0))

    def test_corrupt_checkpoint_refuses_false_completion(self):
        worker, _ = self.captured()
        with sqlite3.connect(self.spool) as c:
            c.execute("UPDATE meta SET value=? WHERE key='slack_captured_attribution'", ('{"phase":"complete"}',))
        self.assertEqual(self.step(worker)['failed'], 1)
        self.assertEqual(self.counts(), (0, 0))

    def test_unreadable_retry_prefix_does_not_starve_later_eligible_record(self):
        from recall_server.archive import ArchiveNotFound
        from recall_server import slack_captured_attribution
        worker, _ = self.captured()
        self.message_phase(worker)
        with patch.object(worker.archive, 'read_raw', side_effect=ArchiveNotFound('synthetic')):
            self.assertEqual(self.step(worker)['failed'], 0)
        with self.store.connect() as c:
            rows = c.execute("""SELECT event.native_id,artifact.artifact_id,artifact.size_bytes
                FROM canonical_events event JOIN raw_artifacts artifact USING(tenant_id,source_id,artifact_id)
                WHERE event.tenant_id=%s AND event.source_id=%s
                  AND event.canonical_redacted#>>'{provenance,connector_id}'='slack.messages'
                ORDER BY event.native_id""", (self.tenant,self.source)).fetchall()
        target = next(row for row in rows if row['native_id'] == self.message.native_id)
        budget = max(row['size_bytes'] for row in rows)
        self.assertGreater(rows[0]['size_bytes'] + target['size_bytes'], budget)
        original = worker.archive.read_raw
        def only_message(reference):
            if reference['artifact_id'] != target['artifact_id']:
                raise ArchiveNotFound('synthetic')
            return original(reference)
        with patch.object(worker.archive, 'read_raw', side_effect=only_message), patch.object(
                slack_captured_attribution, 'MAX_PAGE_BYTES', budget):
            for _ in range(6):
                self.assertEqual(self.step(worker)['failed'], 0)
        self.assertEqual(self.counts(), (1, 1))
        with sqlite3.connect(self.spool) as c:
            state = json.loads(c.execute("SELECT value FROM meta WHERE key='slack_captured_attribution'").fetchone()[0])
        self.assertEqual(state['unresolved'], len(rows) - 1)

    def test_archived_old_author_cannot_cross_current_revision(self):
        from connectors.slack_source import normalize_slack_message
        from connectors.sdk import ConnectorRunner
        from privacy.policy import PrivacyPolicy
        worker, _ = self.captured()
        self.message_phase(worker)
        original = worker.archive.read_raw
        with self.store.connect() as c:
            target = c.execute('SELECT artifact_id FROM canonical_events WHERE tenant_id=%s AND source_id=%s AND native_id=%s',
                (self.tenant,self.source,self.message.native_id)).fetchone()['artifact_id']
        def revise(reference):
            payload = original(reference)
            if reference['artifact_id'] == target:
                record = normalize_slack_message(workspace_id='T123',channel_id='C123',
                    value={'ts':'1789981200.000001','user':'U999','text':'Synthetic question'})
                decision = PrivacyPolicy(mode='scrub').apply({'content':record.content,
                    'provenance':PrivacyPolicy(mode='scrub').apply(record.provenance).value})
                artifact = self.gateway.put_raw(tenant_id=self.tenant,source_id=self.source,
                    native_id=record.native_id,payload=ConnectorRunner._raw_payload(record),
                    media_type='application/json',created_at=record.occurred_at)
                from client.mac import canonical_envelope
                event = canonical_envelope(source_id=self.source,native_id=record.native_id,
                    kind='connector_record',content=decision.value['content'],principal_id=self.principal,
                    visibility='private',occurred_at=record.occurred_at,
                    provenance={**decision.value['provenance'],'connector_id':'slack.messages',
                        'connector_schema_version':2,'artifact_ref':artifact},parent=record.native_parent_id)
                self.plane.ingest_batch(tenant_id=self.tenant,principal_id=self.principal,events=[event])
            return payload
        with patch.object(worker.archive, 'read_raw', side_effect=revise):
            self.assertEqual(self.step(worker)['failed'], 0)
        self.assertEqual(self.counts(), (1, 0))
        self.project()
        self.assertEqual(self.scoped(), [])
        with self.store.connect() as c:
            self.assertEqual(c.execute('SELECT revision FROM canonical_documents WHERE tenant_id=%s AND source_id=%s AND native_id=%s AND is_current',
                (self.tenant,self.source,self.message.native_id)).fetchone()['revision'], 2)


if __name__ == '__main__':
    names = [name for name in CapturedSlackActors.__dict__ if name.startswith('test_')]
    result = unittest.TextTestRunner(verbosity=2).run(unittest.TestSuite(CapturedSlackActors(name) for name in names))
    raise SystemExit(not result.wasSuccessful())
