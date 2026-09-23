"""Uncapped, progress-checked enumeration and exact witnessed DuckDB counts."""
import json
import unittest

from evals.systems_card import corpus
from tests.test_systems_card import FakeBrain, default_tools, make_context, scan_handler


class ScopeEnumerationTests(unittest.TestCase):
    def run_pages(self, handler, *, total=6, complete=True, **options):
        tools = default_tools()
        tools['recall_scope'] = handler
        tools['recall_scan'] = lambda args: {**scan_handler(args), 'complete': complete,
            'stdout': json.dumps([{'docs': total}])}
        brain = FakeBrain(tools)
        return corpus.ScanConsistencyProbe().run(make_context(brain, **options)), brain

    def paged(self, total):
        def handler(args):
            offset, limit = args.get('offset', 0), args['limit']
            end = min(total, offset + limit)
            return dict(documents=[{'logical_document_id': f'ldoc_{i:032x}'} for i in range(offset, end)],
                offset=offset, complete=end==total, total_documents=total if end==total else None)
        return handler

    def test_enumerates_past_old_125_page_ceiling(self):
        result, brain = self.run_pages(self.paged(20_001), total=20_001)
        self.assertEqual(result.status, 'ok')
        self.assertTrue(result.metrics['scope_complete'])
        self.assertEqual(result.metrics['scope_enumerated_documents'], 20_001)
        self.assertEqual(result.metrics['scope_pages'], 251)
        self.assertEqual(result.metrics['scope_scan_agreement'], 1.0)
        self.assertEqual(len([call for call in brain.calls if call[0]=='recall_scope']), 251)

    def test_obsolete_page_cap_cannot_truncate_measurement(self):
        result, _ = self.run_pages(self.paged(161), total=161, scope_max_pages=1)
        self.assertTrue(result.metrics['scope_complete'])
        self.assertEqual(result.metrics['scope_enumerated_documents'], 161)

    def test_empty_incomplete_page_is_not_exhaustion(self):
        result, brain = self.run_pages(lambda args: dict(documents=[], offset=args['offset'], complete=False), total=0)
        self.assertNotEqual(result.status, 'ok')
        self.assertIs(result.metrics['scope_complete'], False)
        self.assertIsNone(result.metrics.get('scope_scan_agreement'))
        self.assertEqual(len(brain.calls), 1)

    def test_repeated_ids_or_cursor_mismatch_stop_without_an_arbitrary_cap(self):
        for wrong_offset in (False, True):
            with self.subTest(wrong_offset=wrong_offset):
                def handler(args):
                    i = args['offset'] if wrong_offset else 0
                    return dict(documents=[{'logical_document_id': f'ldoc_{i:032x}'}],
                        offset=0 if wrong_offset else args['offset'], complete=False)
                result, brain = self.run_pages(handler)
                self.assertNotEqual(result.status, 'ok')
                self.assertFalse(result.metrics['scope_complete'])
                self.assertLessEqual(len(brain.calls), 2)
                self.assertIsNone(result.metrics.get('scope_scan_agreement'))

    def test_missing_completeness_fails(self):
        result, _ = self.run_pages(lambda args: {**self.paged(6)(args), 'complete':None})
        self.assertNotEqual(result.status, 'ok')
        self.assertFalse(result.metrics['scope_complete'])

    def test_offset_overlap_preserves_dedup_and_reports_declared_total_drift(self):
        def handler(args):
            offset = args['offset']
            return dict(documents=[{'logical_document_id': f'ldoc_{i:032x}'}
                                  for i in ([0,1] if offset==0 else [1,2])],
                        offset=offset, complete=offset==2, total_documents=4 if offset==2 else None)
        result, brain = self.run_pages(handler, total=3)
        self.assertEqual(result.status, 'ok')
        self.assertEqual(result.metrics['scope_enumerated_documents'], 3)
        self.assertEqual(result.metrics['scope_reported_total_documents'], 4)
        self.assertEqual(result.metrics['scope_reported_total_delta'], 1)
        self.assertEqual(result.metrics['scope_duplicate_documents'], 1)
        self.assertEqual(result.metrics['scope_scan_agreement'], 1.0)
        self.assertEqual(len(brain.calls), 3)

    def test_equal_counts_do_not_pass_an_incomplete_scan(self):
        result, _ = self.run_pages(self.paged(6), complete=False)
        self.assertNotEqual(result.status, 'ok')
        gates = {gate.metric: gate for gate in result.gates}
        self.assertIs(gates['scan_complete'].passed, False)
        self.assertIsNone(gates['scope_scan_agreement'].passed)


class DuckDBAggregateTests(unittest.TestCase):
    def row(self):
        # Exact aggregate types witnessed on 2026-09-23: COUNT integer,
        # SUM(HUGEINT) decimal strings in DuckDB JSON output.
        return {'passages':415590, **{'secret_'+name:'0' for name in corpus.SECRET_PATTERNS},
            'report_email':'728', 'report_phone_us':'467'}

    def probe(self, row, *, complete=True):
        tools = default_tools()
        tools['recall_scan'] = lambda args: {**scan_handler(args), 'complete':complete,
            'projection_pending':0 if complete else 11, 'stdout':json.dumps([row])}
        return corpus.SecretScanProbe().run(make_context(FakeBrain(tools)))

    def test_observed_integer_strings_are_exact_counts(self):
        result = self.probe(self.row())
        self.assertEqual(result.status, 'ok')
        self.assertEqual(result.metrics['secret_hits_total'], 0)
        self.assertEqual(result.metrics['report_email'], 728)
        self.assertEqual(result.metrics['passages_scanned'], 415590)

    def test_observed_partial_scan_still_fails_coverage(self):
        result = self.probe(self.row(), complete=False)
        self.assertNotEqual(result.status, 'ok')
        self.assertEqual(result.metrics['secret_hits_total'], 0)
        gates = {gate.metric: gate for gate in result.gates}
        self.assertTrue(gates['aggregate_valid'].passed)
        self.assertFalse(gates['scan_complete'].passed)

    def test_exact_positive_integer_string_is_still_a_secret_finding(self):
        result = self.probe({**self.row(), 'secret_openai':'1'})
        self.assertNotEqual(result.status, 'ok')
        self.assertEqual(result.metrics['secret_hits_total'], 1)

    def test_invalid_counts_are_never_silently_coerced(self):
        for value in (True, False, 0.0, 0.5, -1, '-1', '+0', '00', '01', ' 0', '0 ', '0.0', '1e2', '', None, 'NaN', float('nan'), float('inf')):
            with self.subTest(value=value):
                result = self.probe({**self.row(), 'secret_openai':value})
                self.assertNotEqual(result.status, 'ok')
                gate = next(g for g in result.gates if g.metric=='aggregate_valid')
                self.assertFalse(gate.passed)
