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


if __name__ == '__main__':
    unittest.main()
