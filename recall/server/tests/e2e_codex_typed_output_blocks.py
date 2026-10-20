#!/usr/bin/env python3
"""Captured Codex text-block outputs through the original publication owners."""
import unittest
from unittest.mock import patch

from e2e_codex_tool_publication import CodexToolPublication
from contracts.source_units import resolve_codex_tool_source_unit
from evals.passage_index_audit import _exact_sample, _span_coverage
from recall_server import passage_append, tool_passage_projection
from recall_server.passage_projection import PassagePolicy


class CodexTypedOutputBlocks(CodexToolPublication):
    def old_projection(self, *, visible):
        self.ingest('old-string', result=True)
        self.ingest('typed-result', second=1, value=dict(type='response_item', payload=dict(
            type='custom_tool_call_output', output=[
                dict(type='input_text', text='The complete newly supported Sapphire result.')])))
        if visible:
            self.ingest('visible', second=2, value=dict(type='response_item', payload=dict(
                type='message', role='user', content=[
                    dict(type='input_text', text='Original visible Sapphire context.')])))

        def old_resolve(raw, unit):
            import json
            if isinstance(json.loads(raw)['payload'].get('output'), list):
                raise ValueError('tool_source_unit_invalid')
            return resolve_codex_tool_source_unit(raw, unit)

        # Capture the real predecessor behavior, including its absence of a
        # reusable checkpoint for a tool-only parent.
        original_checkpoint = passage_append.make_checkpoint
        def old_checkpoint(*args, **kwargs):
            if not args[2] or not args[3]:
                return None
            value = original_checkpoint(*args, **kwargs)
            return dict(value, version=3) if value else value
        with patch.object(tool_passage_projection, 'resolve_codex_tool_source_unit', old_resolve), \
                patch.object(passage_append, 'make_checkpoint', old_checkpoint):
            self.assertEqual(self.project()['documents'], 1)
        old = self.projected()
        self.assertFalse(any('newly supported' in row['text_redacted'] for row in old))
        return old

    def test_finished_old_codex_is_rebuilt_once_including_tool_only_history(self):
        for visible in (True, False):
            with self.subTest(visible=visible):
                if not visible:
                    self.setUp()
                old = self.old_projection(visible=visible)
                self.assertEqual(self.passages.seed_backfill(tenant_id=self.tenant), 1)
                self.assertEqual(self.project()['documents'], 1)
                current = self.projected()
                self.assertTrue(any('newly supported' in row['text_redacted'] for row in current))
                for prior in old:
                    self.assertTrue(any(all(prior[k] == row[k] for k in
                        ('passage_id', 'text_redacted', 'receipts', 'spans')) for row in current))
                checkpoint = self.document()['append_checkpoint']
                self.assertEqual(checkpoint['version'], 4)
                if not visible:
                    self.assertTrue(checkpoint['full_rebuild_only'])
                self.assertEqual(self.passages.seed_backfill(tenant_id=self.tenant), 0)
                self.assertEqual(self.project()['documents'], 0)

    def test_old_codex_append_cannot_retain_incomplete_typed_prefix(self):
        self.old_projection(visible=True)
        self.ingest('later-visible', second=3, value=dict(type='response_item', payload=dict(
            type='message', role='user', content=[dict(type='input_text', text='Later context.')])))
        self.logical.project_pending(tenant_id=self.tenant, batch_size=1,
                                     max_batches=1, upload_concurrency=1)
        candidate = self.passages._pending(tenant_id=self.tenant, limit=1)[0]
        prepared = self.passages._prepare(candidate, incremental=True)
        oracle = self.passages._prepare(candidate, incremental=False)
        try:
            self.assertIsNone(prepared.previous_checkpoint)
            self.assertEqual(tuple(prepared.passages), tuple(oracle.passages))
            self.assertTrue(any('newly supported' in row.text for row in prepared.passages))
            self.assertEqual(self.passages._commit(prepared)['status'], 'committed')
        finally:
            prepared.close()
            oracle.close()
        # Once rebuilt, a later typed-block arrival uses the original append
        # owner and still equals the complete native publication oracle.
        self.ingest('later-blocks', second=4, value=dict(type='response_item', payload=dict(
            type='custom_tool_call_output', output=[
                dict(type='input_text', text='First later block.'),
                dict(type='input_text', text='Last later café block.')])))
        self.logical.project_pending(tenant_id=self.tenant, batch_size=1,
                                     max_batches=1, upload_concurrency=1)
        candidate = self.passages._pending(tenant_id=self.tenant, limit=1)[0]
        prepared = self.passages._prepare(candidate, incremental=True)
        self.assertIsNotNone(prepared.previous_checkpoint)
        from e2e_grouped_tool_append import GroupedToolAppend
        GroupedToolAppend.compare_and_commit(self, candidate, prepared)

    def test_unsupported_completed_codex_acknowledges_epoch_without_fake_append(self):
        self.ingest('unknown', value=dict(type='response_item', payload=dict(
            type='custom_tool_call_output', output=[dict(type='image', image_url='fixture')])))
        with patch.object(passage_append, 'make_checkpoint', return_value=None):
            self.assertEqual(self.project()['documents'], 1)
        self.assertEqual(self.projected(), [])
        self.assertEqual(self.passages.seed_backfill(tenant_id=self.tenant), 1)
        self.assertEqual(self.project()['documents'], 1)
        checkpoint = self.document()['append_checkpoint']
        self.assertEqual(checkpoint['version'], 4)
        self.assertTrue(checkpoint['full_rebuild_only'])
        self.assertFalse(passage_append.proves_append(checkpoint, self.document(), None, None, None))
        self.assertEqual(self.document()['extraction_coverage']['dispositions'],
                         {'source_unit_refused': 1})
        self.assertEqual(self.passages.seed_backfill(tenant_id=self.tenant), 0)
        self.assertEqual(self.project()['documents'], 0)

    def test_complete_typed_output_keeps_receipts_coordinates_and_vendor_rows(self):
        self.store.search_plane = 'turbopuffer'
        self.passages.policy = PassagePolicy(8, 2)
        texts = [
            'Café 🧠 first result: the original check failed.',
            'Second result keeps every captured input_text block.',
            '',
            'Repeated literal result.',
            'Repeated literal result.',
        ]
        expected = (
            'Café 🧠 first result: the original check failed.\n'
            'Second result keeps every captured input_text block.\n\n'
            'Repeated literal result.\nRepeated literal result.'
        )
        raw = self.ingest('typed-result', value=dict(type='response_item', payload=dict(
            type='custom_tool_call_output', call_id='call-blocks',
            output=[dict(type='input_text', text=text) for text in texts])))
        self.assertEqual(self.project()['documents'], 1)
        rows = self.projected()
        self.assertGreater(len(rows), 1, 'the complete captured output must be searchable')
        self.assertEqual(resolve_codex_tool_source_unit(
            raw, 'codex.tool-result.string.v1#/payload/output')[0], expected)
        covered = set()
        for row in rows:
            opened = self.grant_and_open(row)
            self.assertTrue(opened['complete'])
            self.assertEqual(opened['chunks'][0]['text'], raw)
            self.assertEqual(row['roles'], ['tool'])
            for span in row['spans']:
                self.assertEqual(span['source_unit'],
                                 'codex.tool-result.string.v1#/payload/output')
                self.assertEqual(span['record_count'], 1)
                self.assertEqual(expected.encode()[span['source_byte_start']:span['source_byte_end']],
                                 row['text_redacted'].encode()[span['passage_byte_start']:span['passage_byte_end']])
                covered.update(range(span['source_byte_start'], span['source_byte_end']))
        self.assertTrue(all(index in covered for index, byte in enumerate(expected.encode())
                            if chr(byte).strip()))
        self.assertEqual(_exact_sample(self.store, self.passages.logical_projection, rows,
            policy=self.passages.policy, tenant_id=self.tenant), len(rows))
        self.assertEqual(_span_coverage([self.document()], rows)['uncovered_bytes'], 0)
        self.writer.drain(tenant_id=self.tenant, max_months=10)
        self.assertEqual(set(self.client.namespace(self.settings.namespace(self.tenant)).rows),
                         {row['passage_id'] for row in rows})


if __name__ == '__main__':
    suite = unittest.TestSuite(CodexTypedOutputBlocks(name)
        for name in CodexTypedOutputBlocks.__dict__ if name.startswith('test_'))
    raise SystemExit(not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful())
