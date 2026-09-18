"""Capture source bytes, never promote a partial read to complete evidence."""
from __future__ import annotations

import base64
import copy
import gzip
import hashlib
import json
import re
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from evals import candidate_capture as capture
from evals.boundary_identity import native_family_id
from evals.retrieval import EvaluationInputError
from evals.systems_card.mcp_client import CallOutcome
from evals.systems_card import runner

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'server'))
from recall_server.deep_inspection import agent_evidence_receipts

SID = '12345678-1234-1234-1234-123456789abc'
PARENT = 'claude-session-' + 'a' * 24
SOURCE = 'claude:test:host'
DOC = 'ldoc_' + 'a' * 30
RECEIPT = 'recall://' + SOURCE + '/' + 'a' * 24 + '-000001?rev=1#item=0'


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def mounted(root, text='Café 🧠 shipped.\nNext action.', *, role='assistant', split=False):
    envelope = json.dumps({'sessionId': SID, 'message': {'content': [{'type': 'text', 'text': text}]}}, ensure_ascii=False)
    pieces = [envelope[:20], envelope[20:]] if split else [envelope]
    records = []
    for i, value in enumerate(pieces):
        rec = dict(ordinal=i, segment_ordinal=i, segment_count=len(pieces), event_native_id='a' * 24 + '-000001',
                   occurred_at='2026-09-18T00:00:00Z', roles=[role], receipts=[RECEIPT])
        rec['content_fragment' if split else 'content'] = value if split else json.loads(value)
        records.append(rec)
    raw = b''.join((json.dumps(r, ensure_ascii=False) + '\n').encode() for r in records)
    (root / 'part-00000.jsonl').write_bytes(raw)
    manifest = dict(logical_document_id=DOC, revision=2, native_parent_sha256=digest(PARENT.encode()),
                    record_count=len(records), parts=[dict(ordinal=0, content_sha256=digest(raw), first_record_ordinal=0, last_record_ordinal=len(records)-1)])
    m = json.dumps(manifest).encode(); (root / 'manifest.json').write_bytes(m)
    ranges = []
    for i, value in enumerate(text.split('\n')[:2]):
        start = len('\n'.join(text.split('\n')[:i]).encode()) + (1 if i else 0)
        ranges.append(dict(passage_id='psg_' + str(i) * 30, text=value[:3], receipts=[RECEIPT], passage_window=['2026-09-18T00:00:00Z']*2,
                           spans=[dict(record_ordinal=0, record_count=len(records), source_byte_start=start,
                                       source_byte_end=start+len(value.encode()), passage_byte_start=0, passage_byte_end=len(value.encode()))]))
    return dict(source_id=SOURCE, logical_document_id=DOC, native_parent_id=PARENT, revision=2,
                manifest_content_sha256=digest(m), first_occurred_at='2026-09-18T00:00:00Z', last_occurred_at='2026-09-18T00:00:00Z', matching_ranges=ranges)


class LocalClient:
    def __init__(self, root, mutate=None):
        self.root, self.mutate, self.calls = root, mutate, []

    def call_tool(self, name, args, **kwargs):
        self.calls.append((name, args, kwargs))
        encoded = base64.b64decode(re.search(r"SPEC = '([^']+)'", args['program']).group(1))
        spec = json.loads(gzip.decompress(encoded))
        program = args['program'].replace("Path('/docs/d1')", f'Path({str(self.root)!r})')
        executed = subprocess.run(['bash', '-c', program], check=True, capture_output=True, text=True)
        self.last_stdout = executed.stdout
        result = dict(complete=True, output_truncated=False, exit_code=0,
                      opened_receipts=agent_evidence_receipts(executed.stdout), stdout=executed.stdout)
        if self.mutate:
            self.mutate(spec, result)
        return CallOutcome(name, True, 1, result=result)


class CaptureTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name); self.mount = self.root / 'mount'; self.mount.mkdir()
        self.hit = mounted(self.mount, split=True); self.client = LocalClient(self.mount)

    def new(self, **kwargs):
        return capture.CandidateCapture(self.root / 'capture', client_factory=lambda: self.client, protected_families=set(), **kwargs)

    def run_case(self, recorder, hits=None):
        return recorder.capture_case('case-test', 'What shipped?', {'results': hits or [self.hit]})

    def test_exact_unicode_spans_and_one_newline_join(self):
        recorder = self.new(); result = self.run_case(recorder); row = result['candidates'][0]
        self.assertEqual(row['text'], 'Café 🧠 shipped.\nNext action.')
        self.assertEqual(row['text_sha256'], digest(row['text'].encode()))
        self.assertTrue(row['complete_selected_passages']); self.assertFalse(row['complete_document'])
        self.assertEqual(row['family_ids'], [native_family_id('claude-parent', SID)])
        for p in row['source_evidence']:
            s, e = p['combined_char_start'], p['combined_char_end']
            self.assertEqual(row['text'][s:e].encode(), row['text'].encode()[p['combined_byte_start']:p['combined_byte_end']])
            self.assertEqual(p['receipts'], [RECEIPT])
        self.assertEqual(len(self.client.calls), 2)
        summary = recorder.finish(); self.assertEqual(summary['complete_candidates'], 1)
        self.assertNotIn('What shipped', json.dumps(summary)); self.assertNotIn('Café', json.dumps(summary))
        for p in (self.root / 'capture').rglob('*'):
            if p.is_file(): self.assertEqual(p.stat().st_mode & 0o777, 0o600)

    def test_protected_family_never_dispatches_or_retains_source_prose(self):
        recorder = capture.CandidateCapture(self.root / 'capture', client_factory=lambda: self.client,
            protected_families={native_family_id('claude-parent', SID)})
        row = self.run_case(recorder)['candidates'][0]
        self.assertEqual(row['capture_status'], 'withheld_protected')
        self.assertNotIn('text', row); self.assertEqual(len(self.client.calls), 1)
        self.assertTrue((self.root / 'capture' / 'case-000-candidate-00.json').exists())
        recorder.finish()
        self.assertNotIn('Café', ''.join(p.read_text() for p in (self.root / 'capture').glob('*.json')))

    def test_missing_metadata_and_malformed_identity_keep_all_slots(self):
        hits=[]
        for field in ['spans', 'receipts']:
            h=copy.deepcopy(self.hit);h['matching_ranges'][0][field]=[];hits.append(h)
        h=copy.deepcopy(self.hit);h['native_parent_id']='broken';hits.append(h)
        result=self.run_case(self.new(),hits)
        self.assertEqual([r['candidate_index'] for r in result['candidates']], [0,1,2])
        self.assertTrue(all(not r['complete_selected_passages'] for r in result['candidates']))
        self.assertEqual(self.client.calls, [])

    def test_advanced_manifest_and_corrupt_part_are_not_current_text(self):
        for target in ['manifest.json','part-00000.jsonl']:
            with self.subTest(target=target):
                old=(self.mount/target).read_bytes();(self.mount/target).write_bytes(old+b' ')
                recorder=capture.CandidateCapture(self.root/target,client_factory=lambda:self.client,protected_families=set())
                row=self.run_case(recorder)['candidates'][0]
                self.assertFalse(row['complete_selected_passages']);self.assertNotIn('text',row)
                (self.mount/target).write_bytes(old)

    def test_wrong_source_receipt_and_reasoning_are_unavailable(self):
        for mode in ['foreign_receipt','reasoning']:
            with self.subTest(mode=mode):
                self.hit=mounted(self.mount)
                if mode=='foreign_receipt':self.hit['matching_ranges'][0]['receipts']=['recall://other/event?rev=1#item=0']
                else:self.hit=mounted(self.mount,role='analysis')
                recorder=capture.CandidateCapture(self.root/mode,client_factory=lambda:self.client,protected_families=set())
                row=self.run_case(recorder)['candidates'][0]
                self.assertFalse(row['complete_selected_passages']);self.assertNotIn('text',row)

    def test_timeout_truncation_and_unopened_receipts_preserve_failure(self):
        for mode in ['truncated','receipt','cursor']:
            def mutate(spec,result):
                if spec['phase']=='metadata':return
                if mode=='truncated':result['output_truncated']=True
                elif mode=='receipt':result['opened_receipts']=[]
                else:
                    rows=[json.loads(line) for line in result['stdout'].splitlines()]
                    rows[-1]['cursor']=100;result['stdout']='\n'.join(json.dumps(row) for row in rows)
            client=LocalClient(self.mount,mutate)
            recorder=capture.CandidateCapture(self.root/mode,client_factory=lambda:client,protected_families=set())
            row=self.run_case(recorder)['candidates'][0]
            self.assertFalse(row['complete_selected_passages']);self.assertNotIn('text',row)
            self.assertEqual(len(client.calls),2)

    def test_budget_exhaustion_and_search_error_are_retained(self):
        recorder=self.new(max_calls=1)
        row=self.run_case(recorder)['candidates'][0]
        self.assertEqual(row['error'],'capture_call_budget');self.assertEqual(len(self.client.calls),1)
        failure=recorder.capture_case('failed','Other question',None,search_error='backend_error')
        self.assertEqual(failure['search_status'],'unavailable')
        self.assertEqual(recorder.finish()['cases'],2)

    def test_private_output_preflight_and_incomplete_run(self):
        self.root.joinpath('public').mkdir(mode=0o755)
        with self.assertRaises(EvaluationInputError):
            capture.CandidateCapture(self.root/'public'/'new',client_factory=lambda:self.client,protected_families=set())
        recorder=self.new();self.run_case(recorder)
        self.assertFalse((self.root/'capture'/'manifest.json').exists())
        with self.assertRaises(EvaluationInputError):self.new()

    def test_frozen_prefix_mismatch_is_not_self_certified_complete(self):
        self.hit['matching_ranges'][0]['text'] = 'A different original search prefix'
        recorder = self.new()
        row = self.run_case(recorder)['candidates'][0]
        self.assertEqual(row['error'], 'capture_search_prefix_mismatch')
        self.assertFalse(row['complete_selected_passages'])
        self.assertNotIn('text', row)

    def test_oversized_program_does_not_count_an_unattempted_call(self):
        recorder = self.new()
        with mock.patch.object(capture.inspect, 'getsource', return_value='x' * 17000):
            row = self.run_case(recorder)['candidates'][0]
        self.assertEqual(row['error'], 'capture_program_bound')
        self.assertEqual(recorder.finish()['source_calls'], 0)
        self.assertEqual(self.client.calls, [])

    def test_final_page_bound_includes_null_cursor_encoding(self):
        recorder = self.new()
        spec = recorder._spec(self.hit)
        spec.update(phase='text', cursor=0, expected_family={'kind': 'claude-parent', 'native_id': SID})

        def boundary_stdout(page):
            if len(page['fragments']) == 2:
                return 'x' * (11003 if page['next_cursor'] is None else 11000)
            return 'x' * 100

        with mock.patch.object(capture, '_page_stdout', side_effect=boundary_stdout):
            page = capture._read_page(self.mount, spec)
        self.assertEqual(len(page['fragments']), 1)
        self.assertEqual(page['next_cursor'], 1)

    def test_source_times_are_required_before_capture(self):
        del self.hit['first_occurred_at']
        row = self.run_case(self.new())['candidates'][0]
        self.assertFalse(row['complete_selected_passages'])
        self.assertEqual(self.client.calls, [])

    def test_cli_capture_requires_private_expansion_before_profile(self):
        args=runner.parser().parse_args(['run','--output-dir',str(self.root/'out'),'--capture-candidate-evidence'])
        with mock.patch.object(runner,'load_profile') as profile:
            with self.assertRaises(EvaluationInputError):runner.run_card(args)
            profile.assert_not_called()

    def test_timeout_exception_and_malformed_payload_leave_other_slots(self):
        for failure in ['timeout', 'malformed']:
            class Broken(LocalClient):
                def call_tool(self, name, args, **kwargs):
                    if failure == 'timeout':
                        raise TimeoutError('private transport detail')
                    return CallOutcome(name, True, 1, result=['unexpected shape'])
            client = Broken(self.mount)
            recorder = capture.CandidateCapture(self.root / failure, client_factory=lambda: client, protected_families=set())
            result = self.run_case(recorder, [self.hit] * 47)
            self.assertEqual(len(result['candidates']), 47)
            self.assertTrue(all(not r['complete_selected_passages'] for r in result['candidates']))
            summary = recorder.finish()
            self.assertEqual(summary['unavailable_candidates'], 47)
            self.assertNotIn('private transport detail', ''.join(p.read_text() for p in (self.root / failure).glob('*.json')))

    def test_multiple_pages_are_exact_and_remote_program_executes(self):
        text = 'é🧠 ' * 8000
        self.hit = mounted(self.mount, text=text)
        recorder = self.new()
        row = self.run_case(recorder)['candidates'][0]
        self.assertEqual(row['text'], text)
        self.assertGreater(len(self.client.calls), 2)
        selected = [json.loads(line) for line in self.client.last_stdout.splitlines()]
        self.assertEqual(agent_evidence_receipts(self.client.last_stdout), [RECEIPT])
        for fragment in selected[:-1]:
            self.assertEqual(fragment['ordinal'], 0)
            self.assertEqual(fragment['event_native_id'], 'a' * 24 + '-000001')
        self.assertTrue(selected[-1]['capture_meta'])
        self.assertLessEqual(len(self.client.last_stdout.encode()), 11000)
        # Execute the exact generated program, changing only the mounted path.
        args = self.client.calls[0][1]
        program = args['program'].replace("Path('/docs/d1')", f'Path({str(self.mount)!r})')
        out = subprocess.run(['bash', '-c', program], check=True, capture_output=True, text=True)
        self.assertEqual(json.loads(out.stdout)['family'], {'kind': 'claude-parent', 'native_id': SID})

    def test_repetitive_specs_compress_without_raising_program_budget(self):
        self.hit['matching_ranges'][0]['spans'] *= 128
        recorder = self.new(max_calls=1)
        row = self.run_case(recorder)['candidates'][0]
        self.assertNotEqual(row['error'], 'capture_program_bound')
        self.assertEqual(len(self.client.calls), 1)
        self.assertLessEqual(len(self.client.calls[0][1]['program'].encode()), 16000)

    def test_conflicting_selected_event_families_fail_before_prose(self):
        second = '98765432-1234-1234-1234-123456789abc'
        self.hit = mounted(self.mount)
        rec = json.loads((self.mount / 'part-00000.jsonl').read_text())
        rec.update(ordinal=1, event_native_id='a' * 24 + '-000002')
        rec['content']['sessionId'] = second
        part = (self.mount / 'part-00000.jsonl').read_bytes() + (json.dumps(rec) + '\n').encode()
        (self.mount / 'part-00000.jsonl').write_bytes(part)
        m = json.loads((self.mount / 'manifest.json').read_text());m['parts'][0].update(content_sha256=digest(part),last_record_ordinal=1)
        raw=json.dumps(m).encode();(self.mount/'manifest.json').write_bytes(raw)
        self.hit['manifest_content_sha256']=digest(raw);self.hit['matching_ranges'][1]['spans'][0]['record_ordinal']=1
        row=self.run_case(self.new())['candidates'][0]
        self.assertEqual(row['error'],'native_identity_ambiguous')
        self.assertEqual(len(self.client.calls),1)


class CaptureProbeParityTest(unittest.TestCase):
    def test_opt_in_keeps_search_metrics_and_disabled_artifacts_exact(self):
        from evals.systems_card.accuracy import TruthBoundaryProbe
        from tests.test_systems_card_expansion import CardExpansionTest
        fixture=CardExpansionTest();fixture.setUp();self.addCleanup(fixture.temp.cleanup)
        outputs=[];calls=[];times=[]
        class SlowCapture:
            def capture_case(self, *args, **kwargs):
                clock[0] += 10
            def finish(self):
                return {'cases':43}
        for mode in ['default','disabled','enabled']:
            brain=fixture.brain();context=fixture.context(brain)
            directory=fixture.root/mode;directory.mkdir(mode=0o700);context.private_dir=str(directory)
            if mode=='disabled':context.options['capture_candidate_evidence']=False
            if mode=='enabled':context.options['_candidate_capture']=SlowCapture()
            clock=[0.0]
            handler=brain.tools['recall_search']
            def search(args):
                clock[0] += .125
                return handler(args)
            brain.tools['recall_search']=search
            with mock.patch('time.monotonic',side_effect=lambda:clock[0]),mock.patch('time.strftime',return_value='fixed'):
                result=TruthBoundaryProbe().run(context)
            outputs.append((result.metrics,result.as_dict()['gates'],next(directory.glob('*.jsonl')).read_bytes()))
            calls.append(brain.calls);times.append(clock[0])
        self.assertEqual(outputs[0],outputs[1]);self.assertEqual(outputs[0],outputs[2])
        self.assertEqual(calls[0],calls[1]);self.assertEqual(calls[0],calls[2])
        self.assertGreater(times[2],times[0]+400)
