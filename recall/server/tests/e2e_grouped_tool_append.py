#!/usr/bin/env python3
"""Grouped append retains exact full-publication output with bounded suffix reads."""
import json
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import e2e_tool_group_publication as fixture
from recall_server.passage_projection import canonical_spans_json


class GroupedToolAppend(unittest.TestCase):
    ingest = fixture.ToolGroupPublication.ingest
    project = fixture.ToolGroupPublication.project
    projected = fixture.ToolGroupPublication.projected
    document = fixture.ToolGroupPublication.document
    enroll = fixture.ToolGroupPublication.enroll
    grant_and_open = fixture.ToolGroupPublication.grant_and_open

    def setUp(self):
        fixture.ToolGroupPublication.setUp(self)
        put = self.logical.projection.put_records
        self.parts = patch.object(self.logical.projection, 'put_records',
            side_effect=lambda **kw: put(**dict(kw, part_bytes=4096)))
        self.parts.start()
        self.addCleanup(self.parts.stop)

    def value(self, i, *, visible=True, tool=True):
        blocks = []
        if visible:
            blocks.append(dict(type='text', text=(f'Visible café evidence {i}. ' * 65)))
        if tool:
            blocks.append(dict(type='tool_use', id=f'call-{i}', name='Read',
                input=dict(path=f'/synthetic/{i}', detail='Read this exact evidence. ' * 25)))
        return dict(type='assistant', message=dict(role='assistant', content=blocks))

    def seed(self, *, visible=True):
        for i in range(16):
            self.ingest(f'event-{i:02}', second=i, value=self.value(i, visible=visible))
        self.project()
        self.enroll()
        self.project()

    def append(self, i=16, *, visible=True, tool=True):
        self.ingest(f'event-{i:02}', second=i, value=self.value(i, visible=visible, tool=tool))
        self.logical.project_pending(tenant_id=self.tenant, batch_size=1, max_batches=1,
                                     upload_concurrency=1)
        return self.passages._pending(tenant_id=self.tenant, limit=1)[0]

    def compare_and_commit(self, candidate, actual):
        oracle = self.passages._prepare(candidate, incremental=False)
        self.addCleanup(actual.close)
        self.addCleanup(oracle.close)
        self.assertEqual([(p.passage_id, p.ordinal, p.receipts) for p in actual.passages],
                         [(p.passage_id, p.ordinal, p.receipts) for p in oracle.passages])
        self.assertEqual(actual.extraction_coverage, oracle.extraction_coverage)
        self.assertEqual((actual.dense_message_count, actual.dense_message_bytes),
                         (oracle.dense_message_count, oracle.dense_message_bytes))
        self.assertEqual(self.passages._commit(actual)['status'], 'committed')
        after = self.projected()
        self.assertEqual(len(after), len(oracle.passages))
        for row, expected in zip(after, oracle.passages):
            self.assertEqual(row['revision'], candidate.revision)
            for key in ('passage_id', 'ordinal', 'text_sha256', 'token_count'):
                self.assertEqual(row[key], getattr(expected, key))
            self.assertEqual(row['text_redacted'], expected.text)
            self.assertEqual(row['receipts'], list(expected.receipts))
            self.assertEqual(row['roles'], list(expected.roles))
            self.assertEqual(row['spans'], json.loads(canonical_spans_json(expected.spans)))

    def test_mixed_append_matches_full_publication_without_reading_sealed_history(self):
        self.seed()
        candidate = self.append()
        self.assertGreater(len(candidate.part_references), 4)
        reads = []
        read = self.passages.logical_projection.read_part
        def observed(reference, **kw):
            body = read(reference, **kw)
            reads.append((reference['artifact_id'], len(body)))
            return body
        with patch.object(self.passages.logical_projection, 'read_part', side_effect=observed):
            actual = self.passages._prepare(candidate, incremental=True)
        full_bytes = sum(r['size_bytes'] for r in candidate.part_references)
        self.compare_and_commit(candidate, actual)
        print(dict(full_archive_bytes=full_bytes, actual_archive_bytes=sum(n for _, n in reads),
                   parts_read=len(reads), full_parts=len(candidate.part_references)))
        self.assertLess(sum(n for _, n in reads), full_bytes / 2,
                        'grouped append reread sealed immutable history')

    def test_successive_tool_only_visible_only_and_segmented_group_appends(self):
        # Use one segmentation contract for the old publication and every append.
        # Changing it halfway through would correctly fail the raw-prefix proof.
        with patch('recall_server.logical_evidence_projection.TEXT_SEGMENT_BYTES', 512):
            self.seed()
            for i, visible, tool in [(16, False, True), (17, True, False), (18, True, True)]:
                candidate = self.append(i, visible=visible, tool=tool)
                actual = self.passages._prepare(candidate, incremental=True)
                self.assertIsNotNone(actual.previous_checkpoint, i)
                self.compare_and_commit(candidate, actual)
        self.assertTrue(self.grant_and_open(self.projected()[-1])['complete'])

    def test_tool_only_history_falls_back_until_visible_overlap_exists(self):
        self.seed(visible=False)
        self.assertIsNone(self.document()['append_checkpoint'])
        candidate = self.append(16, visible=False)
        actual = self.passages._prepare(candidate, incremental=True)
        self.assertIsNone(actual.previous_checkpoint)
        self.compare_and_commit(candidate, actual)
        candidate = self.append(17, tool=False)
        actual = self.passages._prepare(candidate, incremental=True)
        self.assertIsNone(actual.previous_checkpoint)
        self.compare_and_commit(candidate, actual)
        self.assertIsNotNone(self.document()['append_checkpoint'])
        candidate = self.append(18)
        actual = self.passages._prepare(candidate, incremental=True)
        self.assertIsNotNone(actual.previous_checkpoint)
        self.compare_and_commit(candidate, actual)

    def test_changed_prefix_and_malformed_group_checkpoint_use_full_owner(self):
        self.seed()
        self.ingest('event-03', second=3, value=self.value('changed'))
        self.logical.project_pending(tenant_id=self.tenant, batch_size=1, max_batches=1,
                                     upload_concurrency=1)
        candidate = self.passages._pending(tenant_id=self.tenant, limit=1)[0]
        actual = self.passages._prepare(candidate, incremental=True)
        self.assertIsNone(actual.previous_checkpoint)
        self.compare_and_commit(candidate, actual)
        with self.store.connect() as c:
            c.execute("""UPDATE canonical_passage_documents SET append_checkpoint=
                jsonb_set(append_checkpoint,'{tool_groups,visible_passages}','-1'::jsonb)
                WHERE tenant_id=%s""", (self.tenant,))
        candidate = self.append()
        actual = self.passages._prepare(candidate, incremental=True)
        self.assertIsNone(actual.previous_checkpoint)
        self.compare_and_commit(candidate, actual)

    def test_current_corruption_refuses_and_competing_revision_cannot_ack(self):
        from recall_server.logical_evidence import LogicalEvidenceError
        self.seed()
        candidate = self.append()
        with patch.object(self.passages.logical_projection, 'read_part',
                          side_effect=LogicalEvidenceError('logical_evidence_corrupt')):
            with self.assertRaises(LogicalEvidenceError):
                self.passages._prepare(candidate, incremental=True)
        prepared = self.passages._prepare(candidate, incremental=True)
        self.addCleanup(prepared.close)
        self.assertIsNotNone(prepared.previous_checkpoint)
        newer = self.append(17)
        self.assertEqual(self.passages._commit(prepared)['status'], 'stale')
        actual = self.passages._prepare(newer, incremental=True)
        self.compare_and_commit(newer, actual)

    def test_checkpoint_rollback_and_forget_keep_native_authority(self):
        self.seed()
        candidate = self.append()
        actual = self.passages._prepare(candidate, incremental=True)
        self.addCleanup(actual.close)
        before = self.document()['append_checkpoint']
        rows = self.projected()
        with patch('recall_server.passage_index.enqueue_search_outbox',
                   side_effect=RuntimeError('synthetic transaction failure')):
            with self.assertRaisesRegex(RuntimeError, 'synthetic transaction failure'):
                self.passages._commit(actual)
        self.assertEqual(self.document()['append_checkpoint'], before)
        self.assertEqual(self.projected(), rows)
        self.compare_and_commit(candidate, actual)
        receipt = next(r for row in rows for r in row['receipts'] if '/event-03?' in r)
        request = dict(contract='recall.forget-request.v1', schema_version=1,
            tenant_id=self.tenant, principal_id=self.principal, source_id=self.source,
            target_receipt=receipt, mode='explicit_forget', reason='owner_requested',
            requested_at='2026-10-01T13:00:00Z', idempotency_key='forget-group-prefix')
        self.assertEqual(self.plane.forget(request)['status'], 'deleted')
        self.logical.project_pending(tenant_id=self.tenant, batch_size=1, max_batches=1,
                                     upload_concurrency=1)
        candidate = self.passages._pending(tenant_id=self.tenant, limit=1)[0]
        actual = self.passages._prepare(candidate, incremental=True)
        self.assertIsNone(actual.previous_checkpoint)
        self.compare_and_commit(candidate, actual)
        self.assertNotIn(receipt, [r for row in self.projected() for r in row['receipts']])

    def test_codex_default_group_append_matches_native_full_output(self):
        self.source = 'codex:' + self.source
        for i in range(16):
            value = (dict(type='response_item', payload=dict(type='message', role='user',
                content=[dict(type='input_text', text=f'Visible finding {i}. ' * 80)]))
                if i % 2 == 0 else dict(type='response_item', payload=dict(
                    type='function_call_output', call_id=f'call-{i}',
                    output=f'Native output {i}. ' * 100)))
            self.ingest(f'event-{i:02}', second=i, value=value)
        self.project()
        self.assertEqual(self.document()['applied_extraction_version'], 'codex_tool_groups_v1')
        for i, value in [(16, dict(type='response_item', payload=dict(type='function_call',
                name='Read', call_id='call-new', arguments='{"path":"/synthetic"}'))),
            (17, dict(type='response_item', payload=dict(type='message', role='assistant',
                content=[dict(type='output_text', text='New visible café conclusion. ' * 40)])))]:
            self.ingest(f'event-{i:02}', second=i, value=value)
            self.logical.project_pending(tenant_id=self.tenant, batch_size=1, max_batches=1,
                                         upload_concurrency=1)
            candidate = self.passages._pending(tenant_id=self.tenant, limit=1)[0]
            actual = self.passages._prepare(candidate, incremental=True)
            self.assertIsNotNone(actual.previous_checkpoint)
            self.compare_and_commit(candidate, actual)

    def test_raw_only_intervening_event_and_unavailable_new_part_preserve_tools(self):
        from recall_server.logical_evidence import LogicalEvidenceError
        self.seed()
        old_tools = {row['passage_id']: row['ordinal'] for row in self.projected()
                     if any(span.get('source_unit', '').startswith('claude.tool-')
                            for span in row['spans'])}
        self.assertTrue(old_tools)
        # A captured but unsupported result stays raw-only in both native paths.
        self.ingest('event-16', second=16, value=dict(type='user', message=dict(
            role='user', content=[dict(type='tool_result', tool_use_id='unknown', content=17)])))
        value = self.value(17, tool=False)
        value['message']['content'][0]['text'] *= 12
        self.ingest('event-17', second=17, value=value)
        self.logical.project_pending(tenant_id=self.tenant, batch_size=1, max_batches=1,
                                     upload_concurrency=1)
        candidate = self.passages._pending(tenant_id=self.tenant, limit=1)[0]
        actual = self.passages._prepare(candidate, incremental=True)
        self.assertIsNotNone(actual.previous_checkpoint)
        oracle = self.passages._prepare(candidate, incremental=False)
        self.addCleanup(oracle.close)
        self.assertEqual(actual.append_checkpoint, oracle.append_checkpoint)
        self.compare_and_commit(candidate, actual)
        current = {row['passage_id']: row['ordinal'] for row in self.projected()}
        self.assertTrue(all(current[key] > ordinal for key, ordinal in old_tools.items()))
        self.assertEqual(actual.extraction_coverage['dispositions']['unsupported_result_content'], 1)
        candidate = self.append(18)
        before = self.projected()
        self.assertTrue(self.archive.delete_raw(candidate.part_references[-1]))
        with self.assertRaisesRegex(LogicalEvidenceError, 'logical_evidence_not_found'):
            self.passages._prepare(candidate, incremental=True)
        self.assertEqual(self.projected(), before)
        with self.store.connect() as c:
            self.assertEqual(c.execute('SELECT count(*) AS n FROM canonical_passage_projection_queue '
                'WHERE tenant_id=%s AND reason<>%s', (self.tenant, 'certification')).fetchone()['n'], 1)


if __name__ == '__main__':
    unittest.main(verbosity=2)
