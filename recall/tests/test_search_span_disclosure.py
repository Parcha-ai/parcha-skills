"""MCP search defers coordinates while preserving its evidence pointers."""
import copy
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from recall_server import mcp
from recall_server.passage_projection import PassageSpan, canonical_spans_json
from evals.agentic_truth import _score_validated_cases
from evals.systems_card.accuracy import candidates_from_search
from evals.systems_card.forget import result_receipts
from evals import candidate_capture
from test_agentic_truth import truth_cases
from test_candidate_capture import mounted, RECEIPT
from test_passage_metadata import MetadataStore, MetadataClient, arguments


def full_response():
    """Synthetic five-parent result with native span schema and clipped text.

    Three ranges per parent, each spanning 24 messages. Sizes are fixture
    observations, not estimates of the archived production responses.
    """
    documents = []
    for parent in range(5):
        ranges = []
        for passage in range(3):
            spans = [PassageSpan(message_index=i, record_ordinal=i, record_count=1,
                        source_byte_start=0, source_byte_end=40,
                        passage_byte_start=i*41, passage_byte_end=i*41+40,
                        origin=dict(message_id=f'synthetic-message-{i}',
                            turn_id=f'synthetic-turn-{i}', text_sha256='a'*64, role='assistant'))
                     for i in range(24)]
            ranges.append(dict(kind='dense', score=0.9-passage/10, rerank_score=0.9-passage/10,
                passage_id=f'psg_{parent*3+passage:032x}', passage_ordinal=passage,
                text=('Synthetic visible evidence. '*40)[:256], text_clipped=True,
                receipts=[f'recall://synthetic:source:0/record-{parent*24+i}?rev=1#item=0' for i in range(24)],
                spans=json.loads(canonical_spans_json(tuple(spans))),
                passage_window=['2026-07-01T00:00:00Z','2026-07-01T00:05:00Z']))
        ranges[-1]['copied_evidence']=[dict(source_id='synthetic:copy',
            logical_document_id='ldoc_'+'f'*32, native_parent_id='synthetic-copy-parent', revision=1,
            manifest_object_key='copy/manifest.json', manifest_content_sha256='c'*64,
            passage_id='psg_'+'f'*32, receipts=['recall://synthetic:copy/copy?rev=1#item=0'])]
        documents.append(dict(source_id='synthetic:source:0', logical_document_id=f'ldoc_{parent:032x}',
            revision=1, native_parent_id=f'synthetic-parent-{parent}',
            manifest_object_key=f'parent/{parent}/manifest.json', manifest_content_sha256='b'*64,
            first_occurred_at='2026-07-01T00:00:00Z',last_occurred_at='2026-07-01T00:05:00Z',
            rank=0.9-parent/10, rerank_score=0.9-parent/10,
            reasons=['dense'],matching_ranges=ranges))
    return dict(results=documents,diagnostics=dict(engine='lossless-passages-v1'))



def receipt_heavy_response(sibling_ranges):
    documents = []
    for parent in range(5):
        ranges = []
        for passage in range(1 if parent == 0 else sibling_ranges):
            ranges.append(dict(
                passage_id=f'psg_{parent:02}_{passage:04}', text='x' * 256,
                receipts=[f'recall://synthetic:source/parent-{parent}-record-{i:04}?rev=1#item=0'
                          for i in range(14 if parent == 0 else 24)],
                spans=[], spans_omitted=True))
        documents.append(dict(source_id='synthetic:source',
            logical_document_id=f'ldoc_{parent}', rerank_score=1-parent/10,
            matching_ranges=ranges))
    return dict(results=documents, diagnostics={})

def search(value, **arguments):
    store = SimpleNamespace(search=lambda *_: value)
    return mcp._call_tool(store, {}, 'recall_search', dict(query='Synthetic evidence',limit=5,**arguments))


class SearchSpanDisclosureTest(unittest.TestCase):
    def test_search_preserves_all_non_span_fields_and_both_mcp_channels(self):
        before = full_response()
        untouched = copy.deepcopy(before)
        self.assertLess(mcp._encoded_result_size(before),mcp.SEARCH_RESULT_BUDGET_BYTES)
        after = search(before, snippet_chars=256)
        expected = mcp.clip_search_snippets(copy.deepcopy(before),256)
        for document in expected['results']:
            for item in document['matching_ranges']:
                item.update(spans=[],spans_omitted=True)
        self.assertEqual(after,expected)
        self.assertEqual(before,untouched)
        self.assertEqual(result_receipts(after),result_receipts(before))
        self.assertEqual(candidates_from_search(after),candidates_from_search(before))
        case = truth_cases()[0]
        def score(value):
            row=dict(id=case['id'],candidates=candidates_from_search(value),latency_ms=0,backend_error='')
            return _score_validated_cases([case],[row],split=None)
        self.assertEqual(score(after),score(before))
        self.assertEqual(score(after)['aggregate']['boundary_recall@5'],1)
        envelope=mcp._tool_result(after)
        self.assertEqual(json.loads(envelope['content'][0]['text']),after)
        self.assertEqual(envelope['structuredContent'],after)
        self.assertFalse(envelope['isError'])
        baseline=mcp._encoded_result_size(mcp.clip_search_snippets(before,256))
        compact=mcp._encoded_result_size(after)
        self.assertLess(compact,baseline)  # Exact values recorded separately, no production SLA.

    def test_default_snippets_and_empty_legacy_results_preserve_values(self):
        value=full_response(); value['results'][0]['matching_ranges'][0]['text']='x'*1024
        result=search(value)
        self.assertEqual(result['results'][0]['matching_ranges'][0]['text'],'x'*1024)
        self.assertEqual(result['results'][0]['matching_ranges'][0]['spans'],[])
        for value in ({'results':[]},{'results':[{'receipt':'recall://synthetic/doc?rev=1','text':'legacy'}]}):
            self.assertEqual(search(value),value)

    def test_early_omission_avoids_old_emergency_receipt_loss(self):
        value=full_response()
        # Stress only presentation size, using the existing full span objects.
        for document in value['results']:
            for item in document['matching_ranges']:
                item['spans']*=8
        old=mcp.fit_search_result(mcp.clip_search_snippets(value,256))
        self.assertTrue(old['diagnostics']['ranges_trimmed'])
        self.assertTrue(old['results'][0]['matching_ranges'][0]['receipts_truncated'])
        compact=search(value,snippet_chars=256)
        self.assertLessEqual(mcp._encoded_result_size(compact),mcp.SEARCH_RESULT_BUDGET_BYTES)
        for before,after in zip(value['results'],compact['results'],strict=True):
            self.assertEqual(len(before['matching_ranges']),len(after['matching_ranges']))
            for full,hint in zip(before['matching_ranges'],after['matching_ranges'],strict=True):
                for key in ('passage_id','text','receipts','copied_evidence'):
                    self.assertEqual(full.get(key),hint.get(key))
        self.assertLess(set(result_receipts(old)),set(result_receipts(compact)))

    def test_emergency_text_reduction_keeps_full_receipts_with_exact_wire_bound(self):
        for text in ('x' * 256, ('漢🙂"\\\n' * 52)[:256]):
            with self.subTest(text=text[:5]):
                value = receipt_heavy_response(65)
                for document in value['results']:
                    for item in document['matching_ranges']:
                        item['text'] = text
                value['results'][0]['matching_ranges'][0]['copied_evidence'] = [dict(
                    source_id='synthetic:copy', logical_document_id='copy-parent',
                    passage_id='copy-passage', receipts=['recall://synthetic:copy/a?rev=1#item=0'])]
                original = copy.deepcopy(value)
                self.assertGreater(mcp._encoded_result_size(value), mcp.SEARCH_RESULT_BUDGET_BYTES)
                fitted = search(value, snippet_chars=256)
                self.assertEqual(len(fitted['results'][0]['matching_ranges'][0]['receipts']), 14)
                self.assertEqual(result_receipts(fitted), result_receipts(value))
                self.assertEqual(value, original)
                width = fitted['diagnostics']['snippet_chars']
                self.assertLess(width, 256)
                self.assertGreater(width, 0)
                self.assertNotIn('ranges_trimmed', fitted['diagnostics'])
                for before, after in zip(value['results'], fitted['results'], strict=True):
                    self.assertEqual(before['logical_document_id'], after['logical_document_id'])
                    for full, compact in zip(before['matching_ranges'], after['matching_ranges'], strict=True):
                        self.assertEqual(full['receipts'], compact['receipts'])
                        self.assertEqual(full.get('copied_evidence'), compact.get('copied_evidence'))
                        self.assertEqual(compact['text'], text[:width])
                        self.assertTrue(compact['text_clipped'])
                self.assertLessEqual(mcp._encoded_result_size(fitted), mcp.SEARCH_RESULT_BUDGET_BYTES)
                self.assertGreater(mcp._encoded_result_size(mcp.clip_search_snippets(value, width + 1)),
                                   mcp.SEARCH_RESULT_BUDGET_BYTES)
                envelope = mcp._tool_result(fitted)
                self.assertEqual(json.loads(envelope['content'][0]['text']), envelope['structuredContent'])

    def test_short_original_text_is_kept_when_clipping_flags_cost_more(self):
        value = receipt_heavy_response(65)
        for document in value['results']:
            for item in document['matching_ranges']:
                item['text'] = 'a'
        value['results'][0]['matching_ranges'][0]['text'] = 'x' * 256
        # Keep one already-clipped Unicode range as a separate flag case.
        value['results'][1]['matching_ranges'][0].update(text='🙂', text_clipped=True)
        one = mcp.clip_search_snippets(value, 1)
        slack = mcp.SEARCH_RESULT_BUDGET_BYTES - mcp._encoded_result_size(one) - 128
        value['diagnostics']['fixture_padding'] = 'p' * (slack // 2)
        self.assertGreater(mcp._encoded_result_size(value), mcp.SEARCH_RESULT_BUDGET_BYTES)
        self.assertLess(mcp._encoded_result_size(mcp.clip_search_snippets(value, 1)),
                        mcp.SEARCH_RESULT_BUDGET_BYTES)
        self.assertGreater(mcp._encoded_result_size(mcp.clip_search_snippets(value, 0)),
                           mcp.SEARCH_RESULT_BUDGET_BYTES)
        original = copy.deepcopy(value)
        fitted = search(value, snippet_chars=256)
        self.assertEqual(len(fitted['results'][0]['matching_ranges'][0]['receipts']), 14)
        self.assertEqual(result_receipts(fitted), result_receipts(value))
        self.assertEqual(value, original)
        self.assertEqual(fitted['results'][1]['matching_ranges'][1],
                         value['results'][1]['matching_ranges'][1])
        self.assertTrue(fitted['results'][1]['matching_ranges'][0]['text_clipped'])
        self.assertEqual(fitted['diagnostics']['snippet_chars'], max(
            len(item['text']) for row in fitted['results'] for item in row['matching_ranges']))
        self.assertLessEqual(mcp._encoded_result_size(fitted), mcp.SEARCH_RESULT_BUDGET_BYTES)

    def test_sibling_receipt_growth_triggers_target_receipt_loss_after_span_omission(self):
        # The target and its rank never change. Each sibling range carries
        # authorized pointers, already without source-coordinate payload.

        small, large = receipt_heavy_response(60), receipt_heavy_response(80)
        target = copy.deepcopy(small['results'][0])
        self.assertEqual(target, large['results'][0])
        self.assertLess(mcp._encoded_result_size(small), mcp.SEARCH_RESULT_BUDGET_BYTES)
        self.assertGreater(mcp._encoded_result_size(large), mcp.SEARCH_RESULT_BUDGET_BYTES)
        ordinary = search(small, snippet_chars=256)
        fitted = search(large, snippet_chars=256)
        self.assertEqual(ordinary['results'][0], target)
        self.assertEqual(fitted['results'][0]['logical_document_id'], target['logical_document_id'])
        item = fitted['results'][0]['matching_ranges'][0]
        self.assertEqual(len(item['receipts']), 8)
        self.assertEqual(item['receipts_truncated'], 6)
        self.assertEqual(item['text'], 'x' * 256)
        self.assertNotIn(target['matching_ranges'][0]['receipts'][-1], item['receipts'])
        self.assertEqual(large['results'][0], target)
        self.assertLessEqual(mcp._encoded_result_size(fitted), mcp.SEARCH_RESULT_BUDGET_BYTES)
        # Even eliminating all snippet text cannot fit these full pointers.
        # Raising the receipt prefix alone is not a complete byte-safe repair.
        empty = copy.deepcopy(large)
        for document in empty['results']:
            for item in document['matching_ranges']:
                item['text'] = ''
        self.assertGreater(mcp._encoded_result_size(empty), mcp.SEARCH_RESULT_BUDGET_BYTES)

    def test_exact_metadata_and_existing_capture_recover_source_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory); mount=root/'mount'; mount.mkdir()
            hit=mounted(mount); native=MetadataStore(hit)
            compact=search({'results':[hit]})
            self.assertTrue(all(p.get('spans_omitted') for p in compact['results'][0]['matching_ranges']))
            pages=[];cursor=None
            while True:
                page=mcp._call_tool(native.bound(),{},'recall_passage_metadata',dict(arguments(hit),cursor=cursor))
                self.assertLessEqual(mcp._encoded_result_size(page),16384)
                pages.append(page);cursor=page['next_cursor']
                if page['complete']:break
            for original,page in zip(hit['matching_ranges'],pages,strict=True):
                self.assertEqual(page['passage']['spans'],original['spans'])
                self.assertEqual(page['passage']['receipts'],original['receipts'])
            client=MetadataClient(mount,native)
            recorder=candidate_capture.CandidateCapture(root/'capture',client_factory=lambda:client,protected_families=set())
            row=recorder.capture_case('synthetic','What shipped?',compact)['candidates'][0]
            self.assertTrue(row['complete_selected_passages'])
            self.assertEqual(row['text'],'Café 🧠 shipped.\nNext action.')
            self.assertTrue(all(p['receipts']==[RECEIPT] for p in row['source_evidence']))
            self.assertEqual([call[0] for call in client.calls],['recall_passage_metadata']*2+['recall_exec']*2)
            # The unchanged native owner still refuses advanced/deleted scope.
            with self.assertRaises(ValueError):
                native.bound(authorized_sources=()).passage_metadata(**arguments(hit))
            with self.assertRaises(ValueError):
                native.bound().passage_metadata(**dict(arguments(hit),revision=hit['revision']+1))
            native.live=False
            with self.assertRaises(ValueError):
                native.bound().passage_metadata(**arguments(hit))


if __name__=='__main__':
    unittest.main()
