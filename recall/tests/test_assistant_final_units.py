"""Provider-declared final messages retain exact source and bounded whole views."""
import dataclasses
import json
import unittest

from recall_server.logical_evidence import LogicalEvidenceRecord
from recall_server.passage_projection import (
    DEFAULT_PASSAGE_POLICY, build_passages, reconstruct_passage, visible_messages,
)

KIND = 'assistant_final.complete.v1'


def records(final='A verified finding. ' * 150 + '\nDeployment remains untested.', phase='final_answer', role='assistant'):
    result = []
    for i, text in enumerate(('Previous context. ' * 300, final, 'Later unrelated request.')):
        payload = {'type': 'message', 'role': role if i == 1 else 'assistant',
                   'phase': phase if i == 1 else 'commentary',
                   'content': [{'type': 'output_text', 'text': text}]}
        result.append(LogicalEvidenceRecord(
            ordinal=i, event_native_id=f'event-{i}', event_kind='transcript_record',
            occurred_at='2026-10-15T00:00:00Z', roles=(payload['role'],),
            receipts=(f'recall://codex:linux:unit/event-{i}?rev=1#item=0',),
            segment_ordinal=0, segment_count=1,
            text=json.dumps({'type': 'response_item', 'payload': payload}),
        ))
    return result


def project(source):
    messages = visible_messages(source)
    passages = build_passages(tenant_id='tenant:unit', source_id='codex:linux:unit',
        logical_document_id='ldoc_' + 'a' * 32, revision=1, messages=messages,
        policy=DEFAULT_PASSAGE_POLICY)
    return messages, passages


class AssistantFinalUnits(unittest.TestCase):
    def test_declared_final_is_standalone_and_keeps_qualification(self):
        messages, passages = project(records())
        selected = [p for p in passages if any(s.record_ordinal == 1 for s in p.spans)]
        self.assertEqual(len(selected), 1)
        unit = selected[0]
        self.assertEqual(unit.text, messages[1].text)
        self.assertGreater(len(unit.text), 2000)
        self.assertTrue(unit.text.endswith('Deployment remains untested.'))
        self.assertEqual(unit.receipts, messages[1].receipts)
        self.assertEqual(len(unit.spans), 1)
        self.assertEqual(unit.spans[0].source_unit, KIND)
        self.assertEqual(unit.spans[0].source_byte_start, 0)
        for passage in passages:
            self.assertEqual(reconstruct_passage(passage, messages), passage.text)
            self.assertLessEqual(passage.token_count, 1024)


    def test_nonfinal_or_conflicting_envelopes_keep_ordinary_projection(self):
        ordinary = records(phase='commentary')
        _, baseline = project(ordinary)
        variants = []
        for phase in ('commentary', 'analysis', 'unknown', None):
            variants.append(records(phase=phase))
        for field, value in (('type', 'function_call'), ('role', 'user')):
            source = records()
            envelope = json.loads(source[1].text)
            envelope['payload'][field] = value
            source[1] = dataclasses.replace(source[1], text=json.dumps(envelope))
            variants.append(source)
        source = records()
        source[1] = dataclasses.replace(source[1], roles=('user',))
        variants.append(source)
        for source in variants:
            with self.subTest(text=source[1].text[:100]):
                messages, passages = project(source)
                self.assertTrue(all(m.source_unit is None for m in messages))
                self.assertTrue(all(s.source_unit is None for p in passages for s in p.spans))
                # Roles can change IDs via actor context, so compare exact text/spans.
                self.assertEqual([(p.text, p.spans) for p in passages],
                                 [(p.text, p.spans) for p in baseline])

    def test_mixed_blocks_and_competing_text_cannot_claim_complete_final(self):
        for change in ('tool', 'competing', 'forged_marker'):
            source = records(phase='commentary' if change == 'forged_marker' else 'final_answer')
            value = json.loads(source[1].text)
            if change == 'tool':
                value['payload']['content'].append({'type': 'tool_use', 'name': 'anything'})
            elif change == 'competing':
                value['text'] = 'A different outer message'
            else:
                value['source_unit'] = KIND
            source[1] = dataclasses.replace(source[1], text=json.dumps(value))
            messages, _ = project(source)
            self.assertIsNone(messages[1].source_unit)

    def test_unicode_and_segmented_final_source_offsets_round_trip(self):
        source = records(final='Confirmed café λ. ' * 150 + '\nNot deployed.')
        original = source[1]
        split = len(original.text) // 2
        source[1:2] = [dataclasses.replace(original, text=original.text[:split], segment_count=2),
            dataclasses.replace(original, text=original.text[split:], ordinal=2,
                segment_count=2, segment_ordinal=1, receipts=())]
        source[-1] = dataclasses.replace(source[-1], ordinal=3)
        messages, passages = project(source)
        unit = next(p for p in passages if p.spans[0].source_unit == KIND)
        self.assertEqual(unit.spans[0].record_count, 2)
        self.assertEqual(unit.text, messages[1].text)
        self.assertEqual(unit.spans[0].source_byte_end, len(unit.text.encode()))
        self.assertEqual(reconstruct_passage(unit, messages), unit.text)

    def test_oversized_final_fragments_keep_original_budget(self):
        messages, passages = project(records(final='evidence λ ' * 1800))
        units = [p for p in passages if any(s.record_ordinal == 1 for s in p.spans)]
        self.assertGreater(len(units), 1)
        for unit in units:
            self.assertLessEqual(unit.token_count, 1024)
            self.assertTrue(all(s.record_ordinal == 1 and s.source_unit is None for s in unit.spans))
            self.assertEqual(reconstruct_passage(unit, messages), unit.text)


class AssistantFinalProvider(unittest.TestCase):
    def test_final_full_view_and_ordinary_width_share_original_provider_count(self):
        from central_brain.test_rerank_matched_ranges import MatchedRangeRerankTests, RecordingProvider
        harness = MatchedRangeRerankTests()
        harness.setUp()
        self.addCleanup(harness.doCleanups)
        pool, legs = harness.fixture()
        _, passages = project(records())
        unit = next(p for p in passages if p.spans[0].source_unit == KIND)
        answer = next(r for r in legs[0][2] if r.get('passage_id') == 'answer')
        answer.update(source_unit=KIND, text_redacted=unit.text)
        ordinary = legs[0][2][0]
        ordinary['text_redacted'] = 'ordinary context ' * 400
        provider = RecordingProvider()
        _, diagnostic, _, _ = harness.run_rerank(provider, pool, legs)
        documents = provider.calls[0][0]['documents']
        self.assertIn(unit.text, documents)
        self.assertIn(ordinary['text_redacted'][:2000], documents)
        self.assertEqual(len(documents), 52)
        self.assertEqual(diagnostic['rerank_candidates'], 52)


    def test_oversize_or_unknown_markers_never_expand_wire_ceiling(self):
        from central_brain.test_rerank_matched_ranges import MatchedRangeRerankTests, RecordingProvider
        for kind, text, header in ((KIND, ('x' * 40 + ' ') * 800, ''),
                                   (KIND, 'x' * 19950, 'context ' * 20),
                                   ('vendor_forged', 'unproved ' * 500, '')):
            harness = MatchedRangeRerankTests()
            harness.setUp()
            self.addCleanup(harness.doCleanups)
            pool, legs = harness.fixture()
            answer = next(r for r in legs[0][2] if r.get('passage_id') == 'answer')
            answer.update(source_unit=kind, text_redacted=text, header_redacted=header)
            provider = RecordingProvider()
            harness.run_rerank(provider, pool, legs)
            documents = provider.calls[0][0]['documents']
            self.assertTrue(all(len(d) <= 2000 for d in documents))
            self.assertIn((header.strip() + '\n\n' + text if header else text)[:2000], documents)


if __name__ == '__main__':
    unittest.main()
