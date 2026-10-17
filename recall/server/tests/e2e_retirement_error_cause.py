#!/usr/bin/env python3
"""Unexpected retirement errors retain safe diagnosis without changing outcomes."""
import sys
from pathlib import Path
from unittest.mock import patch
import unittest

SERVER = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(SERVER.parent), str(SERVER)]
import e2e_retirement_capture_timing as fixtures  # noqa: E402
from recall_server.logical_evidence import LogicalEvidenceError  # noqa: E402


class ErrorCause(unittest.TestCase):
    setUp = fixtures.CaptureTiming.setUp
    run_owner = fixtures.CaptureTiming.run_owner

    def test_logical_decode_error_keeps_safe_code_and_unknown(self):
        with patch.object(self.archive, 'read_raw', side_effect=LogicalEvidenceError('logical_evidence_record_invalid')):
            result = self.run_owner()
        self.assertTrue(result['commit_outcome_unknown'])
        self.assertEqual(result['cleared_documents'], 0)
        self.assertEqual(result['error_class'], 'LogicalEvidenceError')
        self.assertEqual(result['cause_error_code'], 'logical_evidence_record_invalid')
        self.assertEqual(result['phase'], 'maintenance')
        self.assertEqual(result['errors'], {'parent_retirement_unavailable': 1})

    def test_passage_decoder_code_keeps_original_unknown_and_ledger_outcome(self):
        with patch.object(self.archive, 'read_raw', side_effect=LogicalEvidenceError('passage_logical_record_not_canonical')):
            result = self.run_owner()
        self.assertTrue(result['commit_outcome_unknown'])
        self.assertEqual(result['cleared_documents'], 0)
        self.assertEqual(result['error_class'], 'LogicalEvidenceError')
        self.assertEqual(result['cause_error_code'], 'passage_logical_record_not_canonical')
        self.assertEqual(result['errors'], {'parent_retirement_unavailable': 1})
        self.assertEqual(result['phase'], 'maintenance')

    def test_arbitrary_exception_text_is_not_emitted(self):
        private = 'private source body and postgres://credential@example'
        with patch.object(self.archive, 'read_raw', side_effect=RuntimeError(private)):
            result = self.run_owner()
        self.assertTrue(result['commit_outcome_unknown'])
        self.assertEqual(result['cleared_documents'], 0)
        self.assertEqual(result['error_class'], 'RuntimeError')
        self.assertIsNone(result['cause_error_code'])
        self.assertNotIn(private, str(result))

    def test_non_identifier_logical_error_detail_is_not_emitted(self):
        private = 'logical_evidence_record_invalid\nprivate source payload'
        with patch.object(self.archive, 'read_raw', side_effect=LogicalEvidenceError(private)):
            result = self.run_owner()
        self.assertTrue(result['commit_outcome_unknown'])
        self.assertEqual(result['error_class'], 'LogicalEvidenceError')
        self.assertIsNone(result['cause_error_code'])
        self.assertNotIn(private, str(result))


def diagnostic_case(code, expected):
    def test(self):
        with patch.object(self.archive, 'read_raw', side_effect=LogicalEvidenceError(code)):
            result = self.run_owner()
        self.assertEqual(result['cause_error_code'], expected)
        self.assertTrue(result['commit_outcome_unknown'])
        self.assertEqual(result['cleared_documents'], 0)
        self.assertEqual(result['errors'], {'parent_retirement_unavailable': 1})
        if expected is None:
            self.assertNotIn(code, str(result))
    return test


from recall_server import chunk_retirement, passage_projection  # noqa: E402
import ast  # noqa: E402
import inspect  # noqa: E402


class FiniteDiagnosticCodes(unittest.TestCase):
    def test_exact_codes_are_existing_decoder_constants(self):
        tree = ast.parse(inspect.getsource(passage_projection))
        actual = {node.value for node in ast.walk(tree) if isinstance(node, ast.Constant)
                  and isinstance(node.value, str) and node.value.startswith('passage_logical_record_')}
        self.assertEqual(chunk_retirement._PASSAGE_CAUSE_CODES, actual)


for ordinal, code in enumerate(sorted(chunk_retirement._PASSAGE_CAUSE_CODES)):
    setattr(ErrorCause, f'test_allowed_decoder_code_{ordinal}', diagnostic_case(code, code))
for ordinal, code in enumerate((
    'passage_logical_record_unknown', 'passage_logical_record_customer_123',
    'passage_logical_record_invalid https://example.invalid/private',
    'passage_logical_record_invalid\nsource body',
)):
    setattr(ErrorCause, f'test_rejected_decoder_detail_{ordinal}', diagnostic_case(code, None))


if __name__ == '__main__':
    unittest.main()
