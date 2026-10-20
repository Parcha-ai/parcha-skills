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

    def captured_file(self):
        import hashlib
        from connectors.sdk import ConnectorRecordV2
        worker, _ = self.captured(enroll=False)
        live_factory = worker.connector_factory
        payload = b"%PDF-1.4\nSynthetic binary attachment\x00\xff" + b"x" * (2 * 1024 * 1024)
        record = ConnectorRecordV2(schema_version=2, native_id='slack-file:T123:F123',
            native_parent_id=self.message.native_id, occurred_at=self.message.occurred_at,
            content=dict(kind='document.v1', content_fidelity='complete',
                document_id='slack-file:T123:F123', mime_type='application/pdf',
                name='Synthetic attachment', parent_id=self.message.native_id,
                surface='slack', artifact_content_sha256=hashlib.sha256(payload).hexdigest(),
                text='Synthetic captured attachment', owner_ids=['slack:T123:U123']),
            provenance=dict(uri='connector://slack/files/slack-file:T123:F123'),
            archive_payload=payload, archive_media_type='application/pdf')
        source = self.source
        class FilePage:
            source_id = source
            connector_id = 'slack.messages'
            def pull(self, cursor):
                return ConnectorPage((record,), 'captured-file', False)
        worker.connector_factory = lambda *_: (FilePage(), self.spool)
        self.assertEqual(self.step(worker)['failed'], 0)
        worker.connector_factory = live_factory
        with self.store.connect() as c:
            event = c.execute("SELECT event_id,artifact_id FROM canonical_events WHERE tenant_id=%s AND source_id=%s AND native_id=%s",
                (self.tenant, self.source, record.native_id)).fetchone()
            raw = c.execute('SELECT media_type,size_bytes FROM raw_artifacts WHERE tenant_id=%s AND source_id=%s AND artifact_id=%s',
                (self.tenant, self.source, event['artifact_id'])).fetchone()
        self.assertEqual(raw, dict(media_type='application/pdf', size_bytes=len(payload)))
        with self.store.connect() as c:
            sdk_bytes = c.execute('SELECT sum(size_bytes) AS size FROM raw_artifacts WHERE tenant_id=%s AND source_id=%s AND artifact_id<>%s',
                (self.tenant, self.source, event['artifact_id'])).fetchone()['size']
        self.assertLess(sdk_bytes, len(payload))
        self.project()
        self.enroll()
        return worker, record, event, sdk_bytes

    def test_binary_attachment_repairs_existing_owner_without_reading_file(self):
        worker, record, event, sdk_bytes = self.captured_file()
        before = self.immutable()
        reads = []
        original = worker.archive.read_raw
        def observed(reference):
            reads.append(reference['artifact_id'])
            return original(reference)
        with patch.object(worker.archive, 'read_raw', side_effect=observed):
            self.message_phase(worker)
            # Only actual SDK reads consume the page byte allowance. The
            # binary file alone exceeds this allowance but is not downloaded.
            with patch('recall_server.slack_captured_attribution.MAX_PAGE_BYTES', sdk_bytes):
                self.assertEqual(self.step(worker)['failed'], 0)
            with self.store.connect() as c:
                self.assertTrue(c.execute("SELECT 1 FROM canonical_event_actors WHERE tenant_id=%s AND source_id=%s AND event_id=%s AND relation='owner'",
                    (self.tenant, self.source, event['event_id'])).fetchone())
            for _ in range(6):
                self.assertEqual(self.step(worker)['failed'], 0)
        with self.store.connect() as c:
            owners = c.execute("SELECT actor_id FROM canonical_event_actors WHERE tenant_id=%s AND source_id=%s AND event_id=%s AND relation='owner'",
                (self.tenant, self.source, event['event_id'])).fetchall()
            queued = c.execute('SELECT 1 FROM canonical_evidence_document_queue WHERE tenant_id=%s AND source_id=%s AND native_parent_id=%s',
                (self.tenant, self.source, record.native_parent_id)).fetchone()
        self.assertEqual(owners, [dict(actor_id=self.actor)])
        self.assertIsNotNone(queued)
        self.assertNotIn(event['artifact_id'], reads)
        self.assertEqual(self.immutable(), before)
        report = self.step(worker)['captured_attribution']
        self.assertEqual(report['phase'], 'complete')
        self.assertEqual(report['unresolved'], 0)
        self.project()
        with self.store.connect() as c:
            self.assertTrue(c.execute("SELECT 1 FROM canonical_evidence_document_actors WHERE tenant_id=%s AND source_id=%s AND actor_id=%s AND relation='owner'",
                (self.tenant, self.source, self.actor)).fetchone())

    def unfinished_document_debt(self):
        from connectors.slack_source import normalize_slack_message
        from recall_server import slack_captured_attribution as captured
        worker, record, event, _ = self.captured_file()
        live_factory = worker.connector_factory
        source = self.source
        # Real SDK intake supplies a finite forward frontier; none of these
        # unrelated records can attribute the attachment's enrolled owner.
        tails = tuple(normalize_slack_message(workspace_id='T123', channel_id='C999',
            value={'ts': f'1789981300.{index:06d}', 'user': 'U999', 'text': 'Synthetic later record'})
            for index in range(24))
        contacts = self.contacts
        class TailPage:
            source_id = source
            connector_id = 'slack.messages'
            def pull(self, cursor):
                return ConnectorPage((*contacts, *tails), 'finite-retry-fixture', False)
        worker.connector_factory = lambda *_: (TailPage(), self.spool)
        # Establish the ordinary email→Slack identity before freezing a scan
        # epoch. This is real forward intake, not a hand-written actor mapping.
        with patch.object(captured, 'repair_slice', return_value=None):
            self.assertEqual(self.step(worker)['failed'], 0)
        worker.connector_factory = live_factory
        # Populate normal body metadata for every tail before taking the
        # immutable snapshot; later projection should change attribution only.
        self.project()
        self.project()
        before = self.immutable()
        original = captured._record
        def old_binary_refusal(worker, row, selected, deadline, **kwargs):
            if selected['native_id'] == record.native_id:
                # The pre-1020 SDK-JSON-only boundary refused this real PDF.
                self.assertEqual(selected['media_type'], 'application/pdf')
                raise ValueError('captured_archive_unavailable')
            return original(worker, row, selected, deadline, **kwargs)
        def checkpoint():
            with sqlite3.connect(self.spool) as c:
                return json.loads(c.execute('SELECT value FROM meta WHERE key=?', (captured.KEY,)).fetchone()[0])
        with patch.object(captured, 'MAX_PAGE_RECORDS', 2):
            with patch.object(captured, '_record', side_effect=old_binary_refusal):
                self.assertEqual(self.step(worker)['failed'], 0)
            old = checkpoint()
            self.assertEqual(old['phase'], 'contacts')
            self.assertEqual(old['after'], record.native_id)
            self.assertEqual(old['unresolved'], 1)
            with self.store.connect() as c:
                self.assertIsNone(c.execute('SELECT 1 FROM canonical_event_actors WHERE tenant_id=%s AND source_id=%s AND event_id=%s',
                    (self.tenant, self.source, event['event_id'])).fetchone())
        return worker, record, event, before, old

    def captured_checkpoint(self):
        from recall_server import slack_captured_attribution as captured
        with sqlite3.connect(self.spool) as c:
            return json.loads(c.execute('SELECT value FROM meta WHERE key=?', (captured.KEY,)).fetchone()[0])

    def captured_scheduling_rows(self):
        from recall_server import slack_captured_attribution as captured
        with sqlite3.connect(self.spool) as c:
            return c.execute('SELECT key,value FROM meta WHERE key IN (?,?) ORDER BY key',
                (captured.KEY, captured.RETRY_TURN)).fetchall()

    def test_repairable_document_debt_runs_before_full_contact_and_message_scans(self):
        from recall_server import slack_captured_attribution as captured
        worker, record, event, before, old = self.unfinished_document_debt()
        with patch.object(captured, 'MAX_PAGE_RECORDS', 2):
            # The current native typed-document path is now available. A fair
            # two-lane schedule must service debt within two bounded turns,
            # without waiting for either full-source traversal to finish.
            reports = [self.step(worker) for _ in range(2)]
            self.assertTrue(all(report['failed'] == 0 for report in reports), reports)
            current = self.captured_checkpoint()
            self.assertEqual(current['epoch'], old['epoch'])
            self.assertEqual(current['upper'], old['upper'])
            self.assertEqual(current['phase'], 'contacts')
            self.assertGreater(current['after'], old['after'])
            self.assertLessEqual(current['examined'] - old['examined'], 4)
            self.assertEqual(self.immutable(), before)
            with self.store.connect() as c:
                owners = c.execute("SELECT actor_id FROM canonical_event_actors WHERE tenant_id=%s AND source_id=%s AND event_id=%s AND relation='owner'",
                    (self.tenant, self.source, event['event_id'])).fetchall()
            self.assertEqual(owners, [dict(actor_id=self.actor)])
            self.assertEqual(current['unresolved'], 0)
        self.project()
        from recall_server.canonical_retrieval import BoundCanonicalRetrieval
        result = BoundCanonicalRetrieval(self.store, tenant_id=self.tenant, principal_id=self.principal,
            authorized_sources=(self.source,)).scope_documents(
                filters={'person': 'Synthetic', 'person_relation': 'owner'}, limit=10)
        self.assertTrue(result['complete'])
        self.assertTrue(result['documents'])
        self.assertEqual(self.immutable(), before)

    def test_retry_turn_survives_restart_and_old_checkpoint_without_companion(self):
        from recall_server import slack_captured_attribution as captured
        from recall_server.managed_worker import ManagedConnectorWorker
        worker, record, event, before, old = self.unfinished_document_debt()
        self.assertEqual(set(old), {'version', 'epoch', 'phase', 'after', 'upper',
                                   'examined', 'unresolved', 'retry_after'})
        with sqlite3.connect(self.spool) as c:
            # A deployed v1 checkpoint has no companion scheduling key.
            c.execute('DELETE FROM meta WHERE key=?', (captured.RETRY_TURN,))
        restarted = ManagedConnectorWorker(self.store, self.archive, self.box,
            state_root=worker.state_root, worker_id='synthetic-restarted-retry',
            connector_factory=worker.connector_factory)
        restarted.retrieval.embed_pending = lambda **_: {}
        with patch.object(captured, 'MAX_PAGE_RECORDS', 2):
            self.assertEqual(self.step(restarted)['failed'], 0)
            repaired = self.captured_checkpoint()
            self.assertEqual(repaired['unresolved'], 0)
            self.assertEqual(repaired['after'], old['after'])
            self.assertEqual(repaired['phase'], 'contacts')
            with sqlite3.connect(self.spool) as c:
                self.assertEqual(c.execute('SELECT value FROM meta WHERE key=?',
                    (captured.RETRY_TURN,)).fetchone()[0], '0')
            self.assertEqual(self.step(restarted)['failed'], 0)
            self.assertGreater(self.captured_checkpoint()['after'], old['after'])
        self.assertEqual(self.immutable(), before)
        with self.store.connect() as c:
            self.assertTrue(c.execute('SELECT 1 FROM canonical_event_actors WHERE tenant_id=%s AND source_id=%s AND event_id=%s',
                (self.tenant, self.source, event['event_id'])).fetchone())

    def test_retry_dirty_failure_preserves_checkpoint_turn_and_restarts(self):
        from recall_server import slack_captured_attribution as captured
        worker, record, event, before, old = self.unfinished_document_debt()
        saved = self.captured_scheduling_rows()
        with patch.object(captured, 'MAX_PAGE_RECORDS', 2):
            with patch.object(logical_evidence_projection, 'mark_logical_evidence_dirty',
                              side_effect=RuntimeError('synthetic dirty failure')):
                self.assertEqual(self.step(worker)['failed'], 1)
            self.assertEqual(self.captured_scheduling_rows(), saved)
            self.assertEqual(self.immutable(), before)
            with self.store.connect() as c:
                self.assertIsNone(c.execute('SELECT 1 FROM canonical_event_actors WHERE tenant_id=%s AND source_id=%s AND event_id=%s',
                    (self.tenant, self.source, event['event_id'])).fetchone())
            self.assertEqual(self.step(worker)['failed'], 0)
            self.assertEqual(self.captured_checkpoint()['unresolved'], 0)
            self.assertEqual(self.captured_checkpoint()['after'], old['after'])
        self.project()
        with self.store.connect() as c:
            self.assertTrue(c.execute("SELECT 1 FROM canonical_evidence_document_actors WHERE tenant_id=%s AND source_id=%s AND actor_id=%s AND relation='owner'",
                (self.tenant, self.source, self.actor)).fetchone())

    def test_fixed_capture_code_persists_and_retry_recovery_clears_it(self):
        from recall_server import slack_captured_attribution as captured
        worker, record, event, before, old = self.unfinished_document_debt()
        with sqlite3.connect(self.spool) as c:
            debt = json.loads(c.execute("SELECT value FROM meta WHERE key>=? AND key<?",
                (captured.DEBT, captured.DEBT_END)).fetchone()[0])
        self.assertEqual(debt["reason"], "captured_archive_unavailable")
        with sqlite3.connect(self.spool) as c:
            report = captured.progress_report({"id": "synthetic", "source_id": self.source},
                type("Runner", (), {"db": c})())
        self.assertEqual(report["debt_reason_sample"]["reasons"],
                         {"captured_archive_unavailable": 1})
        with patch.object(captured, "MAX_PAGE_RECORDS", 2):
            self.assertEqual(self.step(worker)["failed"], 0)
        self.assertEqual(self.captured_checkpoint()["unresolved"], 0)
        self.assertEqual(self.immutable(), before)

    def test_arbitrary_value_error_text_never_enters_debt_or_report(self):
        from recall_server import slack_captured_attribution as captured
        worker, record, event, before, old = self.unfinished_document_debt()
        original = captured._record
        def unavailable(worker, row, selected, deadline, **kwargs):
            if selected["native_id"] == record.native_id:
                raise ValueError("private@example.com captured_archive_corrupt")
            return original(worker, row, selected, deadline, **kwargs)
        with patch.object(captured, "MAX_PAGE_RECORDS", 2), patch.object(
                captured, "_record", side_effect=unavailable):
            report = self.step(worker)["captured_attribution"]
        with sqlite3.connect(self.spool) as c:
            values = c.execute("SELECT value FROM meta WHERE key>=? AND key<?",
                               (captured.DEBT, captured.DEBT_END)).fetchall()
        self.assertEqual(len(values), 1)
        self.assertEqual(json.loads(values[0][0])["reason"], "ValueError")
        self.assertNotIn("private@example.com", json.dumps([values, report]))
        self.assertEqual(self.immutable(), before)

    def test_persistent_retry_debt_cannot_starve_forward_scan(self):
        from recall_server import slack_captured_attribution as captured
        worker, record, event, before, old = self.unfinished_document_debt()
        original = captured._record
        retries = []
        def unavailable(worker, row, selected, deadline, **kwargs):
            if selected['native_id'] == record.native_id:
                retries.append(deadline)
                raise ValueError('still unavailable')
            return original(worker, row, selected, deadline, **kwargs)
        states = [old]
        with patch.object(captured, 'MAX_PAGE_RECORDS', 2), patch.object(captured, '_record', side_effect=unavailable):
            for _ in range(6):
                self.assertEqual(self.step(worker)['failed'], 0)
                states.append(self.captured_checkpoint())
        self.assertEqual(len(retries), 3)
        for index, state in enumerate(states[1:], 1):
            self.assertEqual(state['phase'], 'contacts')
            self.assertEqual(state['unresolved'], 1)
            self.assertEqual(state['epoch'], old['epoch'])
            self.assertLessEqual(state['examined'] - states[index-1]['examined'], 2)
            if index % 2:
                self.assertEqual(state['after'], states[index-1]['after'])
            else:
                self.assertGreater(state['after'], states[index-1]['after'])
        self.assertEqual(self.immutable(), before)
        # Once the record becomes available it is serviced on the next debt turn.
        with patch.object(captured, 'MAX_PAGE_RECORDS', 2):
            self.assertEqual(self.step(worker)['failed'], 0)
        self.assertEqual(self.captured_checkpoint()['unresolved'], 0)

    def test_retry_preserves_original_deadline_and_raw_byte_budget(self):
        import time
        from types import SimpleNamespace
        from recall_server import slack_captured_attribution as captured
        worker, record, event, before, old = self.unfinished_document_debt()
        saved = self.captured_scheduling_rows()
        original_page = captured._page
        expired = False
        def page(*args, **kwargs):
            nonlocal expired
            result = original_page(*args, **kwargs)
            expired = True
            return result
        clock = SimpleNamespace(monotonic=lambda: time.monotonic() + (1000 if expired else 0))
        with patch.object(captured, 'MAX_PAGE_RECORDS', 2):
            with patch.object(captured, '_page', side_effect=page), patch.object(captured, 'time', clock), \
                    patch.object(captured, '_record', side_effect=AssertionError('deadline must prevent a record attempt')):
                self.assertEqual(self.step(worker)['failed'], 0)
            self.assertEqual(self.captured_scheduling_rows(), saved)
            self.assertEqual(self.step(worker)['failed'], 0)
            self.assertEqual(self.captured_checkpoint()['unresolved'], 0)
            # Create SDK-message debt through the ordinary disposition owner.
            # Unlike typed-document retry, it needs an actual archive read.
            original_record = captured._record
            def unavailable_message(worker, row, selected, deadline, **kwargs):
                if selected['native_id'] == self.message.native_id:
                    raise ValueError('synthetic prior SDK failure')
                return original_record(worker, row, selected, deadline, **kwargs)
            with patch.object(captured, 'MAX_PAGE_RECORDS', 500), \
                    patch.object(captured, '_record', side_effect=unavailable_message):
                self.assertEqual(self.step(worker)['failed'], 0)
            self.assertEqual(self.captured_checkpoint()['unresolved'], 1)
            saved = self.captured_scheduling_rows()
            with patch.object(captured, 'MAX_PAGE_BYTES', 0), \
                    patch.object(worker.archive, 'read_raw', side_effect=AssertionError('zero raw budget')):
                self.assertEqual(self.step(worker)['failed'], 0)
            self.assertEqual(self.captured_scheduling_rows(), saved)
        self.assertEqual(self.immutable(), before)

    def test_retry_revocation_preserves_progress_and_forget_cannot_resurrect(self):
        import uuid
        from recall_server import slack_captured_attribution as captured
        for boundary in ('grant', 'forget'):
            with self.subTest(boundary=boundary):
                fixture = type(self)(self._testMethodName)
                fixture.setUp()
                try:
                    worker, record, event, _, old = fixture.unfinished_document_debt()
                    saved = fixture.captured_scheduling_rows()
                    if boundary == 'grant':
                        with fixture.store.connect() as c:
                            c.execute('DELETE FROM canonical_source_grants WHERE tenant_id=%s AND source_id=%s AND principal_id=%s',
                                (fixture.tenant, fixture.source, fixture.principal))
                    else:
                        with fixture.store.connect() as c:
                            receipt = c.execute('''SELECT chunk.receipt FROM canonical_chunks chunk
                                JOIN canonical_documents document USING(tenant_id,source_id,document_id)
                                WHERE document.tenant_id=%s AND document.source_id=%s AND document.native_id=%s
                                  AND document.is_current ORDER BY chunk.ordinal LIMIT 1''',
                                (fixture.tenant, fixture.source, record.native_id)).fetchone()['receipt']
                        fixture.plane.forget(dict(contract='recall.forget-request.v1', schema_version=1,
                            tenant_id=fixture.tenant, principal_id=fixture.principal, source_id=fixture.source,
                            target_receipt=receipt, mode='explicit_forget', reason='owner_requested',
                            requested_at='2026-10-19T09:00:00Z', idempotency_key=uuid.uuid4().hex))
                    before = fixture.immutable()
                    with patch.object(captured, 'MAX_PAGE_RECORDS', 2):
                        report = fixture.step(worker)
                    fixture.assertEqual(report['failed'], int(boundary == 'grant'))
                    if boundary == 'grant':
                        fixture.assertEqual(fixture.captured_scheduling_rows(), saved)
                    else:
                        state = fixture.captured_checkpoint()
                        fixture.assertEqual(state['after'], old['after'])
                        fixture.assertEqual(state['phase'], 'contacts')
                        fixture.assertEqual(state['unresolved'], 0)
                    fixture.assertEqual(fixture.immutable(), before)
                    with fixture.store.connect() as c:
                        fixture.assertIsNone(c.execute('SELECT 1 FROM canonical_event_actors WHERE tenant_id=%s AND source_id=%s AND event_id=%s',
                            (fixture.tenant, fixture.source, event['event_id'])).fetchone())
                finally:
                    fixture.doCleanups()

    def test_captured_file_revocation_and_dirty_failure_cannot_commit_links(self):
        for boundary in ('revocation', 'dirty_failure'):
            with self.subTest(boundary=boundary):
                fixture = type(self)(self._testMethodName)
                fixture.setUp()
                try:
                    worker, record, event, _ = fixture.captured_file()
                    fixture.message_phase(worker)
                    original = worker._reconcile_slack_page
                    def interleave(row, records, **kwargs):
                        if kwargs.get('captured_documents'):
                            if boundary == 'revocation':
                                with fixture.store.connect() as c:
                                    c.execute('DELETE FROM canonical_source_grants WHERE tenant_id=%s AND source_id=%s AND principal_id=%s',
                                        (fixture.tenant, fixture.source, fixture.principal))
                            else:
                                with patch.object(logical_evidence_projection, 'mark_logical_evidence_dirty',
                                                  side_effect=RuntimeError('synthetic dirty failure')):
                                    return original(row, records, **kwargs)
                        return original(row, records, **kwargs)
                    with patch.object(worker, '_reconcile_slack_page', side_effect=interleave):
                        fixture.assertEqual(fixture.step(worker)['failed'], 1)
                    with fixture.store.connect() as c:
                        fixture.assertIsNone(c.execute('SELECT 1 FROM canonical_event_actors WHERE tenant_id=%s AND source_id=%s AND event_id=%s',
                            (fixture.tenant, fixture.source, event['event_id'])).fetchone())
                    with sqlite3.connect(fixture.spool) as c:
                        state = json.loads(c.execute("SELECT value FROM meta WHERE key='slack_captured_attribution'").fetchone()[0])
                    fixture.assertLess(state['after'], record.native_id)
                    if boundary == 'dirty_failure':
                        for _ in range(6):
                            fixture.assertEqual(fixture.step(worker)['failed'], 0)
                        with fixture.store.connect() as c:
                            fixture.assertTrue(c.execute("SELECT 1 FROM canonical_event_actors WHERE tenant_id=%s AND source_id=%s AND event_id=%s AND relation='owner'",
                                (fixture.tenant, fixture.source, event['event_id'])).fetchone())
                finally:
                    fixture.doCleanups()

    def test_captured_file_forget_and_current_revision_block_stale_owner(self):
        import uuid
        from client.mac import canonical_envelope
        from recall_server import slack_captured_attribution as captured
        for boundary in ('forget', 'revision'):
            with self.subTest(boundary=boundary):
                fixture = type(self)(self._testMethodName)
                fixture.setUp()
                try:
                    worker, record, event, _ = fixture.captured_file()
                    fixture.message_phase(worker)
                    original = captured._record
                    def interleave(*args, **kwargs):
                        result = original(*args, **kwargs)
                        if isinstance(result, dict) and result['native_id'] == record.native_id:
                            if boundary == 'forget':
                                with fixture.store.connect() as c:
                                    receipt = c.execute('SELECT receipt FROM canonical_chunks chunk JOIN canonical_documents document USING(tenant_id,source_id,document_id) WHERE document.tenant_id=%s AND document.source_id=%s AND document.native_id=%s AND document.is_current',
                                        (fixture.tenant, fixture.source, record.native_id)).fetchone()['receipt']
                                fixture.plane.forget(dict(contract='recall.forget-request.v1', schema_version=1,
                                    tenant_id=fixture.tenant, principal_id=fixture.principal, source_id=fixture.source,
                                    target_receipt=receipt, mode='explicit_forget', reason='owner_requested',
                                    requested_at='2026-10-19T09:00:00Z', idempotency_key=uuid.uuid4().hex))
                            else:
                                changed = canonical_envelope(source_id=fixture.source, native_id=record.native_id,
                                    kind='connector_record', content={**result['content'], 'owner_ids': ['slack:T123:U999']},
                                    principal_id=fixture.principal, visibility='private', occurred_at=record.occurred_at,
                                    provenance=result['provenance'], parent=record.native_parent_id)
                                fixture.plane.ingest_batch(tenant_id=fixture.tenant, principal_id=fixture.principal, events=[changed])
                        return result
                    with patch.object(captured, '_record', side_effect=interleave):
                        fixture.assertEqual(fixture.step(worker)['failed'], 0)
                    with fixture.store.connect() as c:
                        fixture.assertIsNone(c.execute("SELECT 1 FROM canonical_event_actors actor JOIN canonical_events event USING(tenant_id,source_id,event_id) WHERE event.tenant_id=%s AND event.source_id=%s AND event.native_id=%s AND actor.relation='owner'",
                            (fixture.tenant, fixture.source, record.native_id)).fetchone())
                        if boundary == 'revision':
                            fixture.assertEqual(c.execute('SELECT revision FROM canonical_documents WHERE tenant_id=%s AND source_id=%s AND native_id=%s AND is_current',
                                (fixture.tenant, fixture.source, record.native_id)).fetchone()['revision'], 2)
                finally:
                    fixture.doCleanups()

    def test_captured_file_invalid_commitment_becomes_debt_then_recovers(self):
        from psycopg.types.json import Jsonb
        worker, record, event, _ = self.captured_file()
        self.message_phase(worker)
        with self.store.connect() as c:
            original = c.execute('SELECT canonical_redacted FROM canonical_events WHERE tenant_id=%s AND source_id=%s AND event_id=%s',
                (self.tenant, self.source, event['event_id'])).fetchone()['canonical_redacted']
            c.execute("UPDATE canonical_events SET canonical_redacted=jsonb_set(canonical_redacted,'{content,owner_ids}',%s) WHERE tenant_id=%s AND source_id=%s AND event_id=%s",
                (Jsonb(['slack:T123:U999']), self.tenant, self.source, event['event_id']))
        report = self.step(worker)
        self.assertEqual(report['failed'], 0)
        self.assertEqual(report['captured_attribution']['phase'], 'complete')
        self.assertEqual(report['captured_attribution']['unresolved'], 1)
        # A bad document cannot block the later eligible message or provider lane.
        self.assertEqual(self.counts(), (1, 1))
        with self.store.connect() as c:
            self.assertIsNone(c.execute('SELECT 1 FROM canonical_event_actors WHERE tenant_id=%s AND source_id=%s AND event_id=%s',
                (self.tenant, self.source, event['event_id'])).fetchone())
            c.execute('UPDATE canonical_events SET canonical_redacted=%s WHERE tenant_id=%s AND source_id=%s AND event_id=%s',
                (Jsonb(original), self.tenant, self.source, event['event_id']))
        report = self.step(worker)
        self.assertEqual(report['failed'], 0)
        self.assertEqual(report['captured_attribution']['unresolved'], 0)
        with self.store.connect() as c:
            self.assertTrue(c.execute("SELECT 1 FROM canonical_event_actors WHERE tenant_id=%s AND source_id=%s AND event_id=%s AND relation='owner'",
                (self.tenant, self.source, event['event_id'])).fetchone())

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
