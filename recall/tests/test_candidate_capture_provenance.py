"""A new span field must never self-certify native provenance."""
from __future__ import annotations

import copy
import dataclasses
import json
import tempfile
import unittest
from pathlib import Path

from evals import candidate_capture as capture
from evals.boundary_identity import native_family_id
from evals.systems_card.mcp_client import CallOutcome
from recall_server import passage_projection as projection
from recall_server.logical_evidence import LogicalEvidenceRecord
from tests.test_candidate_capture import LocalClient, digest, mounted

SOURCE = 'codex:test:host'
PARENT = 'codex-session-' + 'a' * 24
DOC = 'ldoc_' + 'a' * 32
TIME = '2026-07-12T12:28:55.939Z'


def mount_v5(root, *, conflict=False, segmented=False):
    contents = [
        {'type': 'session_meta', 'payload': {'id': 'child', 'forked_from_id': 'parent',
            'source': {'subagent': {'thread_spawn': {'parent_thread_id': 'parent'}}}}},
        {'type': 'response_item', 'payload': {'type': 'message', 'role': 'assistant', 'id': 'msg-visible',
            'content': [{'type': 'output_text', 'text': 'Café 🧠 visible.'},
                        {'type': 'reasoning', 'text': 'NEVER EMIT THIS'},
                        {'type': 'output_text', 'text': 'Later distinct status.'}]}},
    ]
    if conflict:
        contents.append({'type': 'session_meta', 'payload': {'id': 'other-child', 'forked_from_id': 'other'}})
    records = []
    for index, content in enumerate(contents):
        text = json.dumps(content, ensure_ascii=False)
        parts = [text[:80], text[80:]] if segmented and index == 1 else [text]
        for segment, part in enumerate(parts):
            records.append(LogicalEvidenceRecord(
                ordinal=len(records), event_native_id=f'event-{index}', event_kind='codex_record',
                occurred_at=TIME, roles=('assistant',) if index == 1 else (),
                receipts=(f'recall://{SOURCE}/event-{index}?rev=1#item=0',) if segment == 0 else (),
                segment_ordinal=segment, segment_count=len(parts), text=part))
    messages = projection.visible_messages(records, policy=projection.PROVENANCE_PASSAGE_POLICY)
    passages = projection.build_passages(tenant_id='tenant:test', source_id=SOURCE,
        logical_document_id=DOC, revision=1, messages=messages, policy=projection.PROVENANCE_PASSAGE_POLICY)
    # Separate parts ensure ancestry proof reads beyond the selected passage's part.
    parts_meta = []
    for index, record in enumerate(records):
        raw = record.encode(source_id=SOURCE)
        (root / f'part-{index:05}.jsonl').write_bytes(raw)
        parts_meta.append({'ordinal': index, 'content_sha256': digest(raw),
                           'first_record_ordinal': record.ordinal, 'last_record_ordinal': record.ordinal})
    manifest = {'logical_document_id': DOC, 'revision': 1, 'native_parent_sha256': digest(PARENT.encode()),
                'record_count': len(records), 'parts': parts_meta}
    raw = json.dumps(manifest).encode(); (root / 'manifest.json').write_bytes(raw)
    hit = {'source_id': SOURCE, 'logical_document_id': DOC, 'native_parent_id': PARENT, 'revision': 1,
           'manifest_content_sha256': digest(raw), 'first_occurred_at': TIME, 'last_occurred_at': TIME,
           'matching_ranges': [{'passage_id': p.passage_id, 'text': p.text,
               'text_sha256': p.text_sha256, 'policy_fingerprint': p.policy_fingerprint,
               'spans': [dataclasses.asdict(s) for s in p.spans], 'receipts': list(p.receipts)} for p in passages]}
    return hit, messages[0].text


class NativeCaptureTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name); self.mount = self.root / 'mount'; self.mount.mkdir()

    def capture(self, hit, name='capture', protected=()):
        client = LocalClient(self.mount)
        recorder = capture.CandidateCapture(self.root / name, client_factory=lambda: client,
            protected_families=set(protected), workers=1)
        row = recorder.capture_case('case', 'Status?', {'results': [hit]})['candidates'][0]
        return row, client

    def test_frozen_legacy_spec_and_remote_output_bytes_are_unchanged(self):
        # Golden bytes obtained from the parent commit's reader before changes.
        hit = mounted(self.mount, split=True)
        spec = capture.CandidateCapture._spec(hit)
        chunks = []
        for phase in ('metadata', 'text'):
            request = {**spec, 'phase': phase}
            if phase == 'text':
                request.update(cursor=0, expected_family={
                    'kind': 'claude-parent', 'native_id': '12345678-1234-1234-1234-123456789abc'})
            chunks.append(capture._page_stdout(capture._read_page(self.mount, request)))
        raw = json.dumps(spec, sort_keys=True, separators=(',', ':')) + '\n' + ''.join(chunks)
        self.assertEqual(digest(raw.encode()), '222bc1c195aa8059377c2120a3d4cbccb192196ba55a7378b84b9427dfe57f01')

    def test_legacy_claude_multiple_spans_in_one_passage_stay_legacy(self):
        hit = mounted(self.mount)
        first, second = hit['matching_ranges']
        span = copy.deepcopy(second['spans'][0])
        offset = first['spans'][0]['passage_byte_end'] + 1
        span['passage_byte_start'] += offset
        span['passage_byte_end'] += offset
        first['spans'].append(span)
        hit['matching_ranges'] = [first]
        row, _ = self.capture(hit)
        self.assertTrue(row['complete_selected_passages'], row)
        self.assertEqual(row['text'], 'Café 🧠 shipped.\nNext action.')
        self.assertEqual(len(row['source_evidence'][0]['spans']), 2)
        self.assertTrue(all('provenance' not in span for span in row['source_evidence'][0]['spans']))

    def test_v5_exact_filtered_utf8_bytes_and_segmented_native_metadata(self):
        hit, expected = mount_v5(self.mount, segmented=True)
        row, client = self.capture(hit)
        self.assertEqual(row.get('text'), expected, row)
        self.assertTrue(row['complete_selected_passages'])
        self.assertNotIn('NEVER EMIT THIS', json.dumps(row))
        self.assertEqual(row['family_ids'], [native_family_id('codex-native', PARENT)])
        span = row['source_evidence'][0]['spans'][0]
        self.assertEqual(span['provenance'], hit['matching_ranges'][0]['spans'][0]['provenance'])
        self.assertEqual(span['event_native_id'], 'event-1')
        self.assertIsNone(span['provenance']['original_occurred_at'])
        self.assertEqual(len(client.calls), 2)

    def test_native_metadata_forgery_fails_before_source_prose(self):
        hit, _ = mount_v5(self.mount)
        changes = ({'native_message_id': 'different'}, {'native_session_id': 'other-child'},
                   {'fork_parent_session_id': 'other-parent'}, {'serialized_at': '2020-01-01T00:00:00Z'},
                   {'visible_text_sha256': '0' * 64}, {'record_type': 'event_msg'},
                   {'original_occurred_at': TIME}, {'contract': 'unknown'}, {'extra': 'field'})
        for index, change in enumerate(changes):
            candidate = copy.deepcopy(hit)
            candidate['matching_ranges'][0]['spans'][0]['provenance'].update(change)
            row, _ = self.capture(candidate, f'bad-{index}')
            self.assertFalse(row['complete_selected_passages'], change)
            self.assertNotIn('text', row)

    def test_later_unselected_header_conflict_cannot_be_hidden(self):
        hit, expected = mount_v5(self.mount, conflict=True)
        row, _ = self.capture(hit)
        self.assertEqual(row.get('text'), expected, row)
        self.assertIsNone(row['source_evidence'][0]['spans'][0]['provenance']['native_session_id'])
        forged = copy.deepcopy(hit)
        forged['matching_ranges'][0]['spans'][0]['provenance'].update(
            native_session_id='child', fork_parent_session_id='parent')
        row, _ = self.capture(forged, 'forged')
        self.assertFalse(row['complete_selected_passages'])
        self.assertNotIn('text', row)

    def test_missing_policy_metadata_recovers_exact_v5_spans(self):
        hit, expected = mount_v5(self.mount)
        original = copy.deepcopy(hit['matching_ranges'][0])
        hit['matching_ranges'][0].pop('policy_fingerprint')
        hit['matching_ranges'][0].pop('text_sha256')
        stored = {key: original[key] for key in ('passage_id', 'policy_fingerprint', 'text_sha256', 'spans', 'receipts')}
        stored['ordinal'] = 0
        metadata_sha = digest(json.dumps(stored, sort_keys=True, ensure_ascii=False, separators=(',', ':'), allow_nan=False).encode())

        class RecoveryClient(LocalClient):
            def call_tool(self, name, args, **kwargs):
                if name != 'recall_passage_metadata':
                    return super().call_tool(name, args, **kwargs)
                self.calls.append((name, args, kwargs))
                response = {**args, 'contract': 'recall.passage-metadata.v1', 'selection_sha256': 'a' * 64,
                    'passage': {**stored, 'metadata_sha256': metadata_sha,
                        'total_spans': len(stored['spans']), 'total_receipts': len(stored['receipts']),
                        'span_offset': 0, 'receipt_offset': 0}, 'next_cursor': None, 'complete': True}
                return CallOutcome(name, True, 1, result=response)

        client = RecoveryClient(self.mount)
        recorder = capture.CandidateCapture(self.root / 'recovery', client_factory=lambda: client, protected_families=set())
        row = recorder.capture_case('case', 'Status?', {'results': [hit]})['candidates'][0]
        self.assertEqual(row.get('text'), expected, row)
        self.assertEqual([name for name, _, _ in client.calls], ['recall_passage_metadata', 'recall_exec', 'recall_exec'])

    def test_policy_binding_and_mixed_span_versions_fail_before_reads(self):
        hit, _ = mount_v5(self.mount)
        for index, change in enumerate(('policy', 'mixed', 'stripped')):
            candidate = copy.deepcopy(hit)
            if change == 'policy':
                candidate['matching_ranges'][0]['policy_fingerprint'] = projection.DEFAULT_PASSAGE_POLICY.fingerprint
            elif change == 'stripped':
                for span in candidate['matching_ranges'][0]['spans']:
                    span.pop('provenance')
            else:
                old = copy.deepcopy(candidate['matching_ranges'][0]['spans'][0])
                old.pop('provenance')
                candidate['matching_ranges'][0]['spans'].append(old)
            row, client = self.capture(candidate, f'mixed-{index}')
            self.assertFalse(row['complete_selected_passages'])
            self.assertEqual(len(client.calls), 0)

    def test_corrupt_unselected_ancestry_part_and_protected_family_still_fail_closed(self):
        hit, _ = mount_v5(self.mount)
        row, client = self.capture(hit, protected=[native_family_id('codex-native', PARENT)])
        self.assertEqual(row['capture_status'], 'withheld_protected')
        self.assertEqual(len(client.calls), 1)
        self.assertNotIn('text', row)
        path = self.mount / 'part-00000.jsonl'; path.write_bytes(path.read_bytes() + b' ')
        row, _ = self.capture(hit, 'corrupt')
        self.assertFalse(row['complete_selected_passages'])
        self.assertNotIn('text', row)


if __name__ == '__main__':
    unittest.main()
