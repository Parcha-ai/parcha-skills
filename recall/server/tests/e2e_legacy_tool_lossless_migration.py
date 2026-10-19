#!/usr/bin/env python3
"""Lossless native migration of historical mixed conversational/tool byte domains."""
import unittest
from dataclasses import replace
from unittest.mock import patch

from recall_server import passage_append, passage_index, passage_projection
from recall_server.logical_evidence import LogicalEvidenceError

from e2e_legacy_tool_migration import LegacyToolMigration


class LosslessLegacyMigration(LegacyToolMigration):
    def test_mixed_historical_domain_preserves_normal_and_typed_result(self):
        self.seed(mixed=True)
        before = self.projected()
        self.assertEqual(len(before), 1)
        self.assertEqual(before[0]['text_redacted'],
                         'Original human request.\nVerified historical result.')
        prepared = self.append_and_prepare()
        try:
            self.assertEqual(self.passages._commit(prepared)['status'], 'committed')
        finally:
            prepared.close()
        current = self.projected()
        fragments = []
        for row in current:
            self.grant_and_open(row)
            for span in row['spans']:
                fragment = row['text_redacted'].encode()[
                    span['passage_byte_start']:span['passage_byte_end']].decode()
                if fragment == 'Original human request.':
                    self.assertIsNone(span.get('source_unit'))
                    fragments.append(fragment)
                if fragment == 'Verified historical result.':
                    self.assertTrue(span['source_unit'].startswith('claude.tool-result.string.v1#'))
                    self.assertIn('not verified fact or human speech', row['header_redacted'])
                    self.assertEqual(row['receipts'], before[0]['receipts'])
                    fragments.append(fragment)
        self.assertCountEqual(fragments, ['Original human request.', 'Verified historical result.'])
        self.assertEqual(self.document()['applied_extraction_version'], 'tool_groups_v1')
        self.assertEqual(self.pending(), 0)

    def seed_blocks(self, blocks):
        self.ingest_blocks('result', blocks)
        native = passage_projection._visible_message
        with patch.object(passage_index, '_document_extraction_version', return_value='legacy'):
            with patch.object(passage_append, 'CHECKPOINT_VERSION', 1), patch.object(
                passage_projection, '_visible_message',
                side_effect=lambda text: native(text, include_tool_results=True)
            ):
                self.project()
        self.original_receipts = self.projected()[0]['receipts']

    def assert_domains(self, expected):
        covered = {unit: bytearray(len(text.encode())) for unit, text in expected.items()}
        rows = self.projected()
        ordinals = {span['record_ordinal'] for row in rows for span in row['spans']
                    if span.get('source_unit') is not None and span['source_unit'] in expected}
        self.assertEqual(len(ordinals), 1)
        record_ordinal = next(iter(ordinals))
        for row in rows:
            self.grant_and_open(row)
            for span in row['spans']:
                unit = span.get('source_unit')
                if span['record_ordinal'] != record_ordinal or unit not in expected:
                    continue
                if unit is not None:
                    self.assertEqual(row['receipts'], self.original_receipts)
                start, end = span['source_byte_start'], span['source_byte_end']
                actual = row['text_redacted'].encode()[
                    span['passage_byte_start']:span['passage_byte_end']]
                self.assertEqual(actual, expected[unit].encode()[start:end])
                covered[unit][start:end] = b'1' * (end-start)
        for unit, mask in covered.items():
            self.assertEqual(mask, b'1' * len(mask), unit)

    def test_unicode_multi_window_and_multi_leaf_domains(self):
        normal = 'Demande café 日本語.'
        result = ' '.join(f'résultat{i}🙂' for i in range(1400))
        tail = 'Deuxième résultat exact.'
        self.seed_blocks([
            {'type': 'text', 'text': normal},
            {'type': 'tool_result', 'content': [
                {'type': 'text', 'text': result}, {'type': 'text', 'text': tail}]},
        ])
        old = self.projected()
        self.assertGreater(len(old), 1)
        prepared = self.append_and_prepare(native='zz-arrival')
        try:
            self.assertEqual(self.passages._commit(prepared)['status'], 'committed')
        finally:
            prepared.close()
        self.assert_domains({None: normal,
            'claude.tool-result.string.v1#/message/content/1/content/0/text': result,
            'claude.tool-result.string.v1#/message/content/1/content/1/text': tail})

    def test_duplicate_text_keeps_each_native_leaf_and_normal_domain(self):
        text, other = 'Same exact text.', 'Distinct second result.'
        self.seed_blocks([
            {'type': 'tool_result', 'content': [
                {'type': 'text', 'text': text}, {'type': 'text', 'text': text}]},
            {'type': 'text', 'text': text},
            {'type': 'tool_result', 'content': other},
        ])
        self.assertEqual(self.projected()[0]['text_redacted'], text + '\n' + other)
        self.assertEqual(self.document()['append_checkpoint']['version'], 1)
        prepared = self.append_and_prepare(native='zz-arrival')
        self.assertIsNotNone(prepared.legacy_migration)
        try:
            self.assertEqual(self.passages._commit(prepared)['status'], 'committed')
        finally:
            prepared.close()
        self.assert_domains({None: text,
            'claude.tool-result.string.v1#/message/content/0/content/0/text': text,
            'claude.tool-result.string.v1#/message/content/0/content/1/text': text,
            'claude.tool-result.string.v1#/message/content/2/content': other})

    def test_partial_tool_byte_omission_refuses_without_mutation(self):
        self.seed(mixed=True)
        before = self.published_rows()
        native = passage_index.iter_passages

        def omit_last_byte(**kwargs):
            messages = kwargs['messages']
            if isinstance(messages, tuple) and messages and messages[0].source_unit:
                kwargs['messages'] = (replace(messages[0], text=messages[0].text[:-1]), *messages[1:])
            return native(**kwargs)

        with patch.object(passage_index, 'iter_passages', side_effect=omit_last_byte):
            with self.assertRaisesRegex(LogicalEvidenceError, 'legacy_tool_migration_required'):
                self.append_and_prepare()
        self.assertEqual(self.published_rows(), before)
        self.assertEqual(self.document()['applied_extraction_version'], 'legacy')
        self.assertEqual(self.pending(), 1)

    def test_historical_hash_mismatch_remains_refused(self):
        self.seed(mixed=True)
        with self.store.connect() as connection:
            connection.execute("UPDATE canonical_passages SET text_sha256=%s WHERE tenant_id=%s",
                               ('0' * 64, self.tenant))
        before = self.published_rows()
        with self.assertRaisesRegex(LogicalEvidenceError, 'legacy_tool_migration_required'):
            self.append_and_prepare()
        self.assertEqual(self.published_rows(), before)
        self.assertEqual(self.document()['applied_extraction_version'], 'legacy')
        self.assertEqual(self.pending(), 1)

    def test_current_visible_checkpoint_keeps_append_reuse(self):
        self.seed(historical=False, mixed=True)
        self.assertEqual(self.document()['append_checkpoint']['version'], 2)
        with patch.object(passage_index, '_legacy_result_intervals',
                          side_effect=AssertionError('current checkpoint unexpectedly rebuilt')):
            prepared = self.append_and_prepare(native='zz-arrival')
        try:
            self.assertIsNotNone(prepared.previous_checkpoint)
            self.assertIsNone(prepared.legacy_migration)
            self.assertEqual(self.passages._commit(prepared)['status'], 'committed')
        finally:
            prepared.close()
        self.assertEqual(self.document()['append_checkpoint']['version'], 2)
        self.assertEqual(self.document()['applied_extraction_version'], 'legacy')
        for row in self.projected():
            self.grant_and_open(row)

    def test_unaffected_source_v1_checkpoint_keeps_append_reuse(self):
        self.source = self.source.removeprefix('claude:')
        with patch.object(passage_append, 'CHECKPOINT_VERSION', 1):
            self.seed(historical=False, mixed=True)
        self.assertEqual(self.document()['append_checkpoint']['version'], 1)
        prepared = self.append_and_prepare(native='zz-arrival')
        try:
            self.assertIsNotNone(prepared.previous_checkpoint)
            self.assertIsNone(prepared.legacy_migration)
            self.assertEqual(self.passages._commit(prepared)['status'], 'committed')
        finally:
            prepared.close()
        self.assertEqual(self.document()['append_checkpoint']['version'], 2)
        for row in self.projected():
            self.grant_and_open(row)

    def test_claude_v1_visible_checkpoint_rebuilds_once_then_reuses(self):
        with patch.object(passage_append, 'CHECKPOINT_VERSION', 1):
            self.seed(historical=False, mixed=True)
        first = self.append_and_prepare(native='zz-arrival')
        try:
            self.assertIsNone(first.previous_checkpoint)
            self.assertIsNone(first.legacy_migration)
            self.assertEqual(self.passages._commit(first)['status'], 'committed')
        finally:
            first.close()
        self.assertEqual(self.document()['append_checkpoint']['version'], 2)
        second = self.append_and_prepare(native='zzz-arrival')
        try:
            self.assertIsNotNone(second.previous_checkpoint)
            self.assertIsNone(second.legacy_migration)
            self.assertEqual(self.passages._commit(second)['status'], 'committed')
        finally:
            second.close()
        self.assertEqual(self.document()['applied_extraction_version'], 'legacy')
        for row in self.projected():
            self.grant_and_open(row)


if __name__ == '__main__':
    suite = unittest.TestSuite(LosslessLegacyMigration(name)
        for name in LosslessLegacyMigration.__dict__ if name.startswith('test_'))
    raise SystemExit(not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful())
