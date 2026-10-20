#!/usr/bin/env python3
"""Captured binary attachment attribution survives native body thinning."""
import json
import time
import unittest
import uuid
from unittest.mock import patch

from e2e_managed_slack_captured_actors import CapturedSlackActors
from recall_server.logical_evidence import LogicalEvidenceProjectionStore
from recall_server import slack_captured_attribution as captured


class ThinnedCapturedDocument(CapturedSlackActors):
    def managed(self, **kwargs):
        worker = super().managed(**kwargs)
        # This fixture intentionally stores raw and logical objects together.
        worker.plane.chunk_body_archive = self.archive
        return worker

    def captured_file(self):
        worker, record, event, sdk_bytes = super().captured_file()
        with self.store.connect() as c:
            before = c.execute("""SELECT event.canonical_redacted,document.text_redacted,
                    chunk.receipt FROM canonical_events event
                JOIN canonical_documents document USING(tenant_id,source_id,event_id)
                JOIN canonical_chunks chunk USING(tenant_id,source_id,document_id)
                WHERE event.tenant_id=%s AND event.source_id=%s AND event.event_id=%s
                ORDER BY chunk.ordinal LIMIT 1""",
                (self.tenant, self.source, event['event_id'])).fetchone()
        targets = self.projector.targets_for_receipts(tenant_id=self.tenant,
            source_ids=(self.source,), receipts=(before['receipt'],), limit=10)
        projection = LogicalEvidenceProjectionStore(self.archive)
        archived = [
            row
            for target in targets
            for row in map(json.loads, projection.read_part(target['reference'],
                tenant_id=self.tenant, source_id=self.source).splitlines())
            if row['event_native_id'] == record.native_id
        ]
        self.assertEqual(len(archived), 1)
        self.original_file_envelope = before['canonical_redacted']
        self.thin_history()
        with self.store.connect() as c:
            after = c.execute("""SELECT canonical_redacted,body_location
                FROM canonical_events WHERE tenant_id=%s AND source_id=%s AND event_id=%s""",
                (self.tenant, self.source, event['event_id'])).fetchone()
        self.assertEqual(after['body_location'], 'raw')
        self.observation = dict(
            original_content_keys=sorted(before['canonical_redacted']['content']),
            thinned_content_keys=sorted(after['canonical_redacted']['content']),
            archived_record_keys=sorted(archived[0]),
            archived_has_owner_ids='owner_ids' in json.dumps(archived),
            archived_has_actor_links=bool(archived[0].get('actor_links')),
            archived_text_equals_document=archived[0].get('text') == before['text_redacted'],
            native_failures=[],
        )
        return worker, record, event, sdk_bytes

    def test_native_binary_attachment_owner_survives_actual_thinning(self):
        original = captured._record

        def observed(*args, **kwargs):
            try:
                return original(*args, **kwargs)
            except ValueError as error:
                self.observation['native_failures'].append(str(error))
                raise

        try:
            worker, record, event, _ = self.captured_file()
            before = self.immutable()
            reads = []
            read_raw = worker.archive.read_raw

            def read(reference):
                reads.append(reference['artifact_id'])
                return read_raw(reference)

            with patch.object(captured, '_record', side_effect=observed):
                with patch.object(worker.archive, 'read_raw', side_effect=read):
                    self.message_phase(worker)
                    for _ in range(6):
                        self.assertEqual(self.step(worker)['failed'], 0)
            with self.store.connect() as c:
                owners = c.execute("""SELECT actor_id FROM canonical_event_actors
                    WHERE tenant_id=%s AND source_id=%s AND event_id=%s AND relation='owner'""",
                    (self.tenant, self.source, event['event_id'])).fetchall()
            self.assertEqual(owners, [dict(actor_id=self.actor)])
            self.assertNotIn(event['artifact_id'], reads)
            self.assertEqual(self.immutable(), before)
            self.assertEqual(self.captured_checkpoint()['unresolved'], 0)
        finally:
            print('THINNED_ATTACHMENT_OBSERVATION=' + json.dumps(
                getattr(self, 'observation', {}), sort_keys=True), flush=True)

    def test_thinned_message_uses_canonical_content_without_sdk_raw_reparse(self):
        worker, _ = self.captured(thin=True)
        with self.store.connect() as c:
            artifact = c.execute("""SELECT artifact_id FROM canonical_events
                WHERE tenant_id=%s AND source_id=%s AND native_id=%s""",
                (self.tenant, self.source, self.message.native_id)).fetchone()['artifact_id']
        original = worker.archive.read_raw
        reads = []

        def read(reference):
            reads.append(reference['artifact_id'])
            if reference['artifact_id'] == artifact:
                raise ValueError('synthetic historical SDK format cannot be reparsed')
            return original(reference)

        before = self.immutable()
        with patch.object(worker.archive, 'read_raw', side_effect=read):
            self.converge(worker)
        self.assertNotIn(artifact, reads)
        self.assertEqual(self.immutable(), before)

    def test_distinct_raw_and_evidence_stores_use_the_configured_owner(self):
        import tempfile
        from recall_server.archive import FilesystemArchiveStore
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        evidence = FilesystemArchiveStore(directory.name, namespace_key=b'e' * 32)
        self.projector.projection = LogicalEvidenceProjectionStore(evidence)
        original_managed = self.managed
        def configured(*args, **kwargs):
            worker = original_managed(*args, **kwargs)
            worker.plane.chunk_body_archive = evidence
            return worker
        with patch.object(self, 'managed', side_effect=configured):
            worker, _ = self.captured(thin=True)
        self.message_phase(worker)
        raw_read, evidence_read = worker.archive.read_raw, evidence.read_raw
        reads = {'raw': 0, 'evidence': 0}
        def raw(reference):
            reads['raw'] += 1
            return raw_read(reference)
        def logical(reference):
            reads['evidence'] += 1
            return evidence_read(reference)
        with patch.object(worker.archive, 'read_raw', side_effect=raw), patch.object(
            evidence, 'read_raw', side_effect=logical):
            self.assertEqual(self.step(worker)['failed'], 0)
        self.assertEqual(self.counts(), (1, 1))
        self.assertGreater(reads['evidence'], 0)
        self.assertEqual(self.captured_checkpoint()['unresolved'], 0)

    def shared_part_messages(self, *, large=False):
        from connectors.sdk import ConnectorPage
        from connectors.slack_source import normalize_slack_message
        worker = self.managed()
        connector, self.spool = worker.connector_factory()
        count = 12 if large else 4
        records = tuple(normalize_slack_message(workspace_id='T123', channel_id='C123',
            value={'ts': f'1789981200.{index + 1:06d}', 'thread_ts': '1789981200.000001',
                   'user': 'U123', 'text': ('x' * 800000 if large else 'Synthetic shared part')})
            for index in range(count))
        connector.pages['message'] = ConnectorPage(records[:6], 'second', True)
        connector.pages['second'] = ConnectorPage(records[6:], 'contacts-again', True)
        for _ in range(3):
            self.assertEqual(self.step(worker)['failed'], 0)
        put = self.projector.projection.put_records
        def sized(*args, **kwargs):
            return put(*args, **dict(kwargs, part_bytes=16 * 1024 * 1024))
        with patch.object(self.projector.projection, 'put_records', side_effect=sized):
            self.project()
        # The large fixture exceeds one ten-document thinning batch.
        self.thin_history(max_batches=2 if large else 1)
        self.enroll()
        source = self.source
        class LiveOnly:
            source_id = source
            connector_id = 'slack.messages'
            def pull(self, cursor):
                return ConnectorPage((), 'live-only', False)
        worker.connector_factory = lambda *_: (LiveOnly(), self.spool)
        self.message_phase(worker)
        reads = []
        original = worker.archive.read_raw
        def read(reference):
            reads.append((reference['artifact_id'], reference['size_bytes']))
            return original(reference)
        with patch.object(worker.archive, 'read_raw', side_effect=read):
            self.assertEqual(self.step(worker)['failed'], 0)
        self.assertEqual(self.counts(), (1, count))
        self.assertEqual(self.captured_checkpoint()['unresolved'], 0)
        self.assertTrue(reads)
        self.assertEqual(len(reads), len({artifact for artifact, _ in reads}), reads)
        if large:
            self.assertGreater(max(size for _, size in reads), 8000000)

    def test_shared_logical_part_read_once_per_native_slice(self):
        self.shared_part_messages()

    def test_valid_large_logical_part_uses_native_read_allowance(self):
        self.shared_part_messages(large=True)

    def claimed_file(self):
        worker, record, event, _ = self.captured_file()
        with self.store.connect() as c:
            c.execute('UPDATE connector_installations SET run_after=now() WHERE id=%s',
                (self.installation,))
        row = worker._claim()
        self.assertIsNotNone(row)
        selected = captured._page(worker, row, '', None, 1, [record.native_id])
        self.assertEqual(len(selected), 1)
        return worker, row, selected[0]

    def test_hydration_rechecks_source_grant_after_archive_read(self):
        from recall_server.canonical import CanonicalLifecycleError
        worker, row, selected = self.claimed_file()
        original = worker.archive.read_raw
        revoked = False

        def read(reference):
            nonlocal revoked
            payload = original(reference)
            if not revoked:
                revoked = True
                with self.store.connect() as c:
                    c.execute('DELETE FROM canonical_source_grants WHERE tenant_id=%s AND source_id=%s',
                        (self.tenant, self.source))
            return payload

        with patch.object(worker.archive, 'read_raw', side_effect=read):
            with self.assertRaises(CanonicalLifecycleError):
                captured._record(worker, row, selected, time.monotonic() + 10)
        self.assertTrue(revoked)

    def test_hydration_forget_during_archive_read_never_returns_content(self):
        worker, row, selected = self.claimed_file()
        with self.store.connect() as c:
            receipt = c.execute("""SELECT receipt FROM canonical_chunks
                WHERE tenant_id=%s AND source_id=%s AND document_id=%s ORDER BY ordinal LIMIT 1""",
                (self.tenant, self.source, selected['document_id'])).fetchone()['receipt']
        original = worker.archive.read_raw
        forgotten = False

        def read(reference):
            nonlocal forgotten
            payload = original(reference)
            if not forgotten:
                forgotten = True
                self.plane.forget(dict(contract='recall.forget-request.v1', schema_version=1,
                    tenant_id=self.tenant, principal_id=self.principal, source_id=self.source,
                    target_receipt=receipt, mode='explicit_forget', reason='owner_requested',
                    requested_at='2026-10-20T17:00:00Z', idempotency_key=uuid.uuid4().hex))
            return payload

        with patch.object(worker.archive, 'read_raw', side_effect=read):
            try:
                result = captured._record(worker, row, selected, time.monotonic() + 10)
            except ValueError:
                result = None
        self.assertTrue(forgotten)
        self.assertIsNone(result)
        with self.store.connect() as c:
            self.assertFalse(c.execute("""SELECT 1 FROM canonical_event_actors
                WHERE tenant_id=%s AND source_id=%s AND event_id=%s""",
                (self.tenant, self.source, selected['event_id'])).fetchone())

    def test_hydration_checks_content_commitment_and_archive_bytes(self):
        worker, row, selected = self.claimed_file()
        original = worker.archive.read_raw
        before = self.immutable()
        with patch.object(worker.archive, 'read_raw',
                side_effect=lambda reference: original(reference) + b' '):
            with self.assertRaises(ValueError):
                captured._record(worker, row, selected, time.monotonic() + 10)
        self.assertEqual(self.immutable(), before)
        with self.store.connect() as c:
            c.execute('UPDATE canonical_events SET content_sha256=%s WHERE tenant_id=%s AND source_id=%s AND event_id=%s',
                ('0' * 64, self.tenant, self.source, selected['event_id']))
        selected = dict(selected, content_sha256='0' * 64)
        with self.assertRaisesRegex(ValueError, 'content_sha256 mismatch'):
            captured._record(worker, row, selected, time.monotonic() + 10)

    def test_hydration_byte_refusal_keeps_native_target_recoverable(self):
        worker, row, selected = self.claimed_file()
        before = self.immutable()
        with patch('recall_server.chunk_bodies.MAX_READ_BYTES', 1):
            with patch.object(worker.archive, 'read_raw', side_effect=AssertionError('must not read')) as read:
                with self.assertRaises(ValueError):
                    captured._record(worker, row, selected, time.monotonic() + 10)
                read.assert_not_called()
        self.assertEqual(self.immutable(), before)
        result = captured._record(worker, row, selected, time.monotonic() + 10)
        self.assertEqual(result['content']['kind'], 'document.v1')
        self.assertEqual(result['content']['owner_ids'], ['slack:T123:U123'])
        self.assertEqual(self.immutable(), before)

    def test_hydration_rechecks_current_event_commitment_after_read(self):
        from recall_server.control import ControlError
        worker, row, selected = self.claimed_file()
        original = worker.archive.read_raw
        changed = False

        def read(reference):
            nonlocal changed
            payload = original(reference)
            if not changed:
                changed = True
                with self.store.connect() as c:
                    c.execute("""UPDATE canonical_events SET content_sha256=%s
                        WHERE tenant_id=%s AND source_id=%s AND event_id=%s""",
                        ('0' * 64, self.tenant, self.source, selected['event_id']))
            return payload

        with patch.object(worker.archive, 'read_raw', side_effect=read):
            with self.assertRaisesRegex(ControlError, 'connector_record_changed'):
                captured._record(worker, row, selected, time.monotonic() + 10)
        self.assertTrue(changed)

    def test_inline_nomination_thinned_before_read_refuses_then_recovers(self):
        worker, record, _, _ = CapturedSlackActors.captured_file(self)
        with self.store.connect() as c:
            c.execute('UPDATE connector_installations SET run_after=now() WHERE id=%s',
                (self.installation,))
        row = worker._claim()
        selected = captured._page(worker, row, '', None, 1, [record.native_id])[0]
        self.assertEqual(selected['body_location'], 'inline')
        self.thin_history()
        before = self.immutable()
        with self.assertRaises(ValueError):
            captured._record(worker, row, selected, time.monotonic() + 10)
        current = captured._page(worker, row, '', None, 1, [record.native_id])[0]
        self.assertEqual(current['body_location'], 'raw')
        restored = captured._record(worker, row, current, time.monotonic() + 10)
        self.assertEqual(restored['content']['owner_ids'], ['slack:T123:U123'])
        self.assertEqual(self.immutable(), before)

    def test_native_new_revision_during_hydration_refuses_old_content(self):
        from recall_server.projectors import canonical_json, content_sha256
        worker, row, selected = self.claimed_file()
        original = worker.archive.read_raw
        changed = False

        def read(reference):
            nonlocal changed
            payload = original(reference)
            if not changed:
                changed = True
                event = dict(self.original_file_envelope)
                event['content'] = dict(event['content'], text='Synthetic updated attachment text')
                event['content_sha256'] = content_sha256(event)
                self.plane.ingest_document(tenant_id=self.tenant, principal_id=self.principal,
                    connector_id='slack.messages', artifact_ref=event['provenance']['artifact_ref'],
                    envelope=event, text_redacted=canonical_json(event['content']).decode())
            return payload

        with patch.object(worker.archive, 'read_raw', side_effect=read):
            with self.assertRaises(ValueError):
                captured._record(worker, row, selected, time.monotonic() + 10)
        self.assertTrue(changed)
        with self.store.connect() as c:
            current = c.execute("""SELECT revision,event_id FROM canonical_documents
                WHERE tenant_id=%s AND source_id=%s AND native_id=%s AND is_current""",
                (self.tenant, self.source, selected['native_id'])).fetchone()
        self.assertEqual(current['revision'], 2)
        self.assertNotEqual(current['event_id'], selected['event_id'])


if __name__ == '__main__':
    unittest.main(defaultTest=[
        'ThinnedCapturedDocument.test_distinct_raw_and_evidence_stores_use_the_configured_owner',
        'ThinnedCapturedDocument.test_shared_logical_part_read_once_per_native_slice',
        'ThinnedCapturedDocument.test_valid_large_logical_part_uses_native_read_allowance',
        'ThinnedCapturedDocument.test_native_binary_attachment_owner_survives_actual_thinning',
        'ThinnedCapturedDocument.test_thinned_message_uses_canonical_content_without_sdk_raw_reparse',
        'ThinnedCapturedDocument.test_hydration_rechecks_source_grant_after_archive_read',
        'ThinnedCapturedDocument.test_hydration_forget_during_archive_read_never_returns_content',
        'ThinnedCapturedDocument.test_hydration_checks_content_commitment_and_archive_bytes',
        'ThinnedCapturedDocument.test_hydration_byte_refusal_keeps_native_target_recoverable',
        'ThinnedCapturedDocument.test_hydration_rechecks_current_event_commitment_after_read',
        'ThinnedCapturedDocument.test_inline_nomination_thinned_before_read_refuses_then_recovers',
        'ThinnedCapturedDocument.test_native_new_revision_during_hydration_refuses_old_content',
    ], verbosity=2)
