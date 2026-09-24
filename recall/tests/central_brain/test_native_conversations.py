"""Native conversations group authorized physical evidence without rewriting it."""
from __future__ import annotations

import copy
import hashlib
import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'server'))
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from recall_server.passage_retrieval import group_near_duplicates  # noqa: E402
from tests import test_codex_archive_collector as archive_fixture  # noqa: E402
from collector.collector import Collector  # noqa: E402
from contracts.native_conversation import native_conversation  # noqa: E402

SESSION = 'codex:019f1111-2222-7333-8444-555555555555'
FORK = 'codex:019f1111-2222-7333-8444-666666666666'


def result(source, document, text, *, session=SESSION, strand='root', record=4, rank=1.0):
    raw = text.encode()
    return {
        'source_id': source, 'logical_document_id': 'ldoc_' + document * 32,
        'native_parent_id': 'physical-' + document, 'revision': 1,
        'conversation_id': session, 'conversation_strand_id': strand,
        'first_occurred_at': '2026-09-24T00:00:00Z',
        'last_occurred_at': '2026-09-24T00:01:00Z',
        'manifest_object_key': 'objects/aa/' + document * 64,
        'manifest_content_sha256': document * 64,
        'rank': rank, 'rerank_score': rank,
        'matching_ranges': [{
            'kind': 'dense', 'passage_id': 'psg_' + document * 32,
            'passage_ordinal': 0, 'text': text, 'text_clipped': False,
            'text_sha256': hashlib.sha256(raw).hexdigest(),
            'receipts': [f'recall://{source}/{document}?rev=1#item=0'],
            'spans': [{'message_index': 0, 'record_ordinal': record, 'record_count': 1,
                       'source_byte_start': 0, 'source_byte_end': len(raw),
                       'passage_byte_start': 0, 'passage_byte_end': len(raw)}],
        }],
    }


class NativeConversationGroupingTests(unittest.TestCase):
    def test_same_session_copies_and_continuations_keep_unique_ranges(self):
        rows = [result('source:mac', 'a', 'Only the Mac knows this answer.'),
                result('source:greppy', 'b', 'Greppy has a different unique answer.', rank=.9),
                result('source:greppy', 'c', 'A later continuation is relevant too.',
                       strand='019faaaa-2222-7333-8444-555555555555', rank=.8)]
        before = copy.deepcopy(rows)
        grouped, diagnostics = group_near_duplicates(rows)
        self.assertEqual(len(grouped), 1)
        self.assertEqual(grouped[0].get('conversation_id'), SESSION)
        self.assertEqual({item['text'] for item in grouped[0]['matching_ranges']},
                         {row['matching_ranges'][0]['text'] for row in rows})
        self.assertEqual({(item.get('source_id'), item.get('logical_document_id'))
                          for item in grouped[0]['matching_ranges']},
                         {(row['source_id'], row['logical_document_id']) for row in rows})
        self.assertEqual(len(grouped[0].get('conversation_documents', [])), 3)
        self.assertEqual(grouped[0]['rank'], rows[0]['rank'])
        self.assertEqual(rows, before)
        self.assertEqual(diagnostics.get('conversation_documents_folded'), 2)

    def test_genuine_fork_is_not_folded_by_equal_text(self):
        text = 'This exact long copied prefix is shared by two real forks.'
        rows = [result('source:a', 'a', text), result('source:b', 'b', text, session=FORK)]
        grouped, _ = group_near_duplicates(rows)
        self.assertEqual(len(grouped), 2)

    def test_equal_text_at_different_native_positions_is_not_one_turn(self):
        rows = [result('source:a', 'a', 'yes', record=4),
                result('source:b', 'b', 'yes', record=8)]
        grouped, _ = group_near_duplicates(rows)
        self.assertEqual(len(grouped), 1)
        self.assertEqual(len(grouped[0]['matching_ranges']), 2)

    def test_equal_projected_positions_do_not_prove_same_native_occurrence(self):
        rows = [result('source:a', 'a', 'The copied turn has an exact known position.'),
                result('source:b', 'b', 'The copied turn has an exact known position.'),
                result('source:c', 'c', 'The copied turn has an exact known position.')]
        grouped, _ = group_near_duplicates(rows)
        self.assertEqual(len(grouped), 1)
        self.assertEqual(len(grouped[0]['matching_ranges']), 3)
        matches = grouped[0]['matching_ranges']
        self.assertEqual({item['source_id'] for item in matches}, {'source:a', 'source:b', 'source:c'})
        self.assertEqual({receipt for item in matches for receipt in item['receipts']},
                         {row['matching_ranges'][0]['receipts'][0] for row in rows})
        self.assertTrue(all('copies' not in item for item in matches))

    def test_time_clip_secondary_receipt_keeps_its_own_probe_document(self):
        from recall_server.canonical_retrieval import BoundCanonicalRetrieval
        from tests.central_brain.test_canonical_retrieval import TimeClipWindowTests
        rows = [result('source:a', 'a', 'outside time window'),
                result('source:b', 'b', 'inside time window')]
        rows[0]['matching_ranges'][0]['receipts'] = ['recall://source:a/a?rev=1#item=1']
        rows[0]['matching_ranges'][0]['passage_window'] = ['2026-08-01T00:00:00Z', '2026-09-02T00:00:00Z']
        rows[1]['matching_ranges'][0]['passage_window'] = ['2026-09-03T00:00:00Z', '2026-09-04T00:00:00Z']
        grouped, _ = group_near_duplicates(rows)
        bound = BoundCanonicalRetrieval(TimeClipWindowTests._ClipStore(),
            tenant_id='tenant:test', principal_id='principal:test', authorized_sources=('source:a','source:b'))
        clipped = bound._clip_passage_hints_to_time_window({'results': grouped},
            sources=['source:a','source:b'], since='2026-09-01T00:00:00Z', until=None)
        self.assertEqual(len(clipped['results'][0]['matching_ranges']), 1)
        probe = bound._investigation_probe(clipped)['results'][0]
        for field in ('source_id', 'logical_document_id', 'native_parent_id', 'revision'):
            self.assertEqual(probe[field], rows[1][field])
        self.assertEqual(probe['receipt'], rows[1]['matching_ranges'][0]['receipts'][0])

    def test_unknown_id_does_not_join_known_conversation_by_text(self):
        text = 'A copied looking prefix cannot establish a native session identity.'
        rows = [result('source:a', 'a', text), result('source:b', 'b', text, session=None)]
        grouped, _ = group_near_duplicates(rows)
        self.assertEqual(len(grouped), 2)

    def test_equal_range_on_distinct_strands_is_retained(self):
        rows = [result('source:a', 'a', 'yes'),
                result('source:b', 'b', 'yes', strand='agent:child-one')]
        grouped, _ = group_near_duplicates(rows)
        self.assertEqual(len(grouped), 1)
        self.assertEqual(len(grouped[0]['matching_ranges']), 2)

    def test_other_harness_and_unknown_rewritten_uuid_remain_separate(self):
        rows = [result('source:a', 'a', 'native exact same words for every session'),
                result('source:b', 'b', 'native exact same words for every session', session=SESSION.replace('codex:', 'claude:')),
                result('source:c', 'c', 'native exact same words for every session', session=FORK)]
        grouped, _ = group_near_duplicates(rows)
        self.assertEqual(len(grouped), 3)




class NativeCollectorMetadataTests(unittest.TestCase):
    setUp = archive_fixture.CodexArchiveCollectorTest.setUp
    tearDown = archive_fixture.CodexArchiveCollectorTest.tearDown
    collector = archive_fixture.CodexArchiveCollectorTest.collector

    def test_codex_root_provenance_carries_verified_native_session_on_every_event(self):
        native_id = SESSION.split(':', 1)[1]
        path = self.active / f'rollout-2026-09-24T00-00-00-{native_id}.jsonl'
        archive_fixture._rollout(path, native_id)
        collector = self.collector()
        collector.scan()
        events = [json.loads(row[0]) for row in collector.db.execute('SELECT envelope_json FROM outbox')]
        self.assertEqual(len(events), 2)
        for event in events:
            self.assertEqual(event['provenance'].get('native_conversation'),
                             {'conversation_id': SESSION, 'strand_id': 'root'})
        collector.close()

    def test_claude_subagent_does_not_claim_main_strand(self):
        native_id = SESSION.split(':', 1)[1]
        path = self.active / 'agent-child.jsonl'
        path.write_text(archive_fixture._line({'type': 'user', 'sessionId': native_id,
            'agentId': 'child-one', 'isSidechain': True,
            'message': {'role': 'user', 'content': 'synthetic task'}}))
        collector = Collector(root=self.active, harness='claude', source_id='source:claude',
                              spool_path=self.spool, endpoint=self.endpoint, token='synthetic-test-token')
        collector.scan()
        event = json.loads(collector.db.execute('SELECT envelope_json FROM outbox').fetchone()[0])
        self.assertEqual(event['provenance'].get('native_conversation'),
                         {'conversation_id': SESSION.replace('codex:', 'claude:'),
                          'strand_id': 'agent:child-one'})
        collector.close()

    def test_missing_thinned_identity_is_unknown_and_fork_uses_own_uuid(self):
        self.assertIsNone(native_conversation({'type': 'session_meta', 'payload': {'type': 'metadata'}}, harness='codex'))
        value = native_conversation({'type': 'session_meta', 'payload': {
            'id': FORK.split(':', 1)[1], 'forked_from_id': SESSION.split(':', 1)[1]}}, harness='codex')
        self.assertEqual(value.conversation_id, FORK)


class NativeProjectionIdentityTests(unittest.TestCase):
    def resolve(self, record, provenance):
        from contracts import native_conversation as owner
        return owner.projected_conversation(record, provenance)

    def test_old_header_is_enough_but_missing_thinned_header_stays_unknown(self):
        header = {'type': 'session_meta', 'payload': {'id': SESSION.split(':')[1]}}
        self.assertEqual(self.resolve(header, {'harness': 'codex'}).conversation_id, SESSION)
        self.assertIsNone(self.resolve({'type': 'session_meta', 'payload': {}}, {'harness': 'codex'}))

    def test_thinned_explicit_identity_survives_but_conflicting_header_does_not(self):
        provenance = {'harness': 'codex', 'native_conversation': {
            'conversation_id': SESSION, 'strand_id': 'root'}}
        self.assertEqual(self.resolve({'type': 'response_item'}, provenance).conversation_id, SESSION)
        with self.assertRaisesRegex(ValueError, 'native_conversation_conflict'):
            self.resolve({'type': 'session_meta', 'payload': {'id': FORK.split(':')[1]}}, provenance)
        self.assertIsNone(self.resolve({}, dict(provenance, harness='slack')))

    def test_continuation_keeps_its_verified_strand_without_inventing_base_order(self):
        segment = '019f1111-2222-7333-8444-555555555556'
        provenance = {'harness': 'codex', 'native_conversation': {
            'conversation_id': SESSION, 'strand_id': 'segment:' + segment}}
        value = self.resolve({'type': 'session_meta', 'payload': {
            'id': SESSION.split(':')[1], 'history_base': {'thread_id': FORK.split(':')[1]}}}, provenance)
        self.assertEqual(value.strand_id, 'segment:' + segment)


if __name__ == '__main__':
    unittest.main()
