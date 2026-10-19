"""Attempt replay keeps exact native values without retaining payload objects."""
from dataclasses import replace
import gc
import tempfile
import unittest
from unittest.mock import patch

from recall_server import passage_index
from recall_server.passage_index import _DiskSequence, _spool_tool_groups
from recall_server.tool_passage_projection import extract_tool_event_groups, extract_codex_tool_event_groups
from tests import test_tool_passage_integration as claude_fixture
from tests import test_codex_tool_passage_projection as codex_fixture
from tests.test_passage_record_spool import fixture


class AttemptSpoolTests(unittest.TestCase):
    def test_disk_and_tuple_diff_preserve_all_fields_and_refusals(self):
        from recall_server.passage_append import RetainedPassage
        from recall_server.passage_projection import PassageMessage, PassagePolicy, build_passages
        from recall_server.logical_evidence import LogicalEvidenceError
        from recall_server.passage_index import classify_passages
        values = build_passages(tenant_id='t', source_id='s', logical_document_id='ldoc_' + 'a' * 32,
            revision=2, policy=PassagePolicy(target_tokens=4, overlap_tokens=0),
            messages=(PassageMessage(record_ordinal=0, occurred_at='2026-01-01T00:00:00Z',
                roles=('user',), receipts=('recall://s/event?rev=1#item=0',), text='one two three four five six seven eight nine ten'),))
        self.assertEqual(len(values), 3)
        existing = [dict(passage_id=values[0].passage_id, ordinal=4, revision=1),
                    dict(passage_id=values[1].passage_id, ordinal=1, revision=2),
                    dict(passage_id='deleted', ordinal=3, revision=1)]
        expected = classify_passages(existing, values, revision=2)
        self.assertEqual(expected.to_insert, (values[2],))
        self.assertEqual(expected.to_delete, ('deleted',))
        self.assertEqual(expected.retained, tuple(v.passage_id for v in values[:2]))
        self.assertEqual(expected.moved, ((values[0].passage_id, 0),))
        self.assertEqual(expected.revision_stale, (values[0].passage_id,))
        with _DiskSequence() as replay:
            replay.extend(values)
            self.assertEqual(classify_passages(existing, replay, revision=2), expected)
        for invalid, code in (((values[0], values[0]), 'passage_identity_collision'),
                              ((RetainedPassage('missing', 0, ()),), 'passage_retained_prefix_missing')):
            with _DiskSequence() as replay:
                replay.extend(invalid)
                with self.assertRaisesRegex(LogicalEvidenceError, code):
                    classify_passages(existing, replay, revision=2)

    def test_interleaved_readers_slices_append_and_close(self):
        with _DiskSequence() as values:
            values.extend(range(10))
            left, right = iter(values), iter(values[2:])
            self.assertEqual((next(left), next(right), next(left)), (0, 2, 1))
            values.extend((10, 11))
            self.assertEqual(values, tuple(range(12)))
            self.assertEqual(values[::-2], (11, 9, 7, 5, 3, 1))
        self.assertTrue(values.stream.closed)
        with self.assertRaises(ValueError):
            values[0]

    def test_complete_event_partition_preserves_native_tool_groups_and_coverage(self):
        claude = claude_fixture.ToolEventGroupingTest().records
        codex = codex_fixture.CodexToolPassageProjectionTests().records
        cases = (
            (extract_tool_event_groups, [
                claude(dict(type='assistant', message=dict(role='assistant', content=[
                    dict(type='tool_use', input={'a': 'é🙂 result', 'b': 'second', 'empty': ''}),
                    dict(type='tool_use', input={'c': 'independent group'})])), segmented=True),
                claude(dict(type='user', message=dict(role='user', content=[
                    dict(type='tool_result', content='actual tool result')]))),
                claude(dict(type='assistant', message=dict(role='assistant', content=[]))),
            ]),
            (extract_codex_tool_event_groups, [
                codex(dict(type='response_item', payload=dict(type='function_call', arguments='{}')), segmented=True),
                codex(dict(type='response_item', payload=dict(type='function_call_output', output='é🙂 evidence'))),
                codex(dict(type='response_item', payload=dict(type='custom_tool_call', input=''))),
            ]),
        )
        for extractor, events in cases:
            records = []
            for group in events:
                start = len(records)
                records.extend(replace(r, ordinal=r.ordinal + start) for r in group)
            expected, coverage = extractor(records)
            with _DiskSequence() as groups:
                actual = _spool_tool_groups(iter(records), extractor, groups)
                self.assertEqual(groups, expected)
                self.assertEqual(actual, coverage)

    def test_prepared_payload_lifetime_closes_temporary_sources_before_return(self):
        projector, candidate, archive, state = fixture(count=11)
        opened, real = [], tempfile.TemporaryFile
        def tracked(*args, **kwargs):
            stream = real(*args, **kwargs)
            opened.append(stream)
            return stream
        with patch.object(projector, '_append_state', return_value=state), patch.object(
            passage_index.tempfile, 'TemporaryFile', side_effect=tracked
        ):
            prepared = projector._prepare(candidate)
        self.assertEqual(sum(not stream.closed for stream in opened), 1)
        prepared.close()
        self.assertTrue(all(stream.closed for stream in opened))
        # Direct preview callers may release a prepared value without _commit.
        with patch.object(projector, '_append_state', return_value=state), patch.object(
            passage_index.tempfile, 'TemporaryFile', side_effect=tracked
        ):
            prepared = projector._prepare(candidate)
        self.assertEqual(sum(not stream.closed for stream in opened), 1)
        del prepared
        gc.collect()
        self.assertTrue(all(stream.closed for stream in opened))


if __name__ == '__main__':
    unittest.main()
