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



class DeadlinePhase(unittest.TestCase):
    setUp = fixtures.CaptureTiming.setUp
    run_owner = fixtures.CaptureTiming.run_owner

    def cancel_phase(self, query_fragment, expected_phase):
        import time
        original = self.store._execute_bounded
        cancelled = []

        def execute(connection, sql, values, deadline):
            if query_fragment in sql:
                cancelled.append(True)
                return original(connection, 'SELECT pg_sleep(%s)', (.1,), time.monotonic() + .02)
            return original(connection, sql, values, deadline)

        with patch.object(self.store, '_execute_bounded', execute):
            result = self.run_owner()
        self.assertEqual(cancelled, [True])
        self.assertEqual(result['errors'], {'parent_retirement_deadline_exceeded': 1})
        self.assertFalse(result['commit_outcome_unknown'])
        self.assertEqual(result['cleared_documents'], 0)
        self.assertEqual(result['failed_phase_counts'], {expected_phase: 1})
        self.assertGreaterEqual(result['failed_phase_ms'][expected_phase], 10)
        self.assertLessEqual(result['failed_phase_ms'][expected_phase], result['parent_wall_ms'])
        self.assertNotIn('pg_sleep', str(result))
        with self.store.connect() as c:
            state = c.execute("SELECT last_record_ordinal FROM canonical_chunk_retirement_progress WHERE tenant_id=%s", (self.tenant,)).fetchone()
            self.assertEqual(state['last_record_ordinal'], -1)
            self.assertEqual(c.execute("SELECT count(*) AS n FROM canonical_chunks WHERE tenant_id=%s AND text_redacted<>''", (self.tenant,)).fetchone()['n'], 3)

    def test_actual_pool_checkout_timeout_retains_unknown_and_elapsed(self):
        import os
        from psycopg_pool import ConnectionPool
        from recall_server import retirement_runner as runner
        original = self.store.connect
        claimed = False

        def before_parent(_candidate):
            nonlocal claimed
            claimed = True

        with ConnectionPool(os.environ['RECALL_DATABASE_URL'], min_size=1, max_size=1) as pool:
            with pool.connection():
                def connect(*, timeout=None):
                    if claimed:
                        return pool.connection(timeout=.02)
                    return original(timeout=timeout)
                with patch.object(self.store, 'connect', connect):
                    result = runner.run_retirement(self.store, self.archive, scope=self.scope,
                        apply=True, before_parent=before_parent)
        self.assertTrue(result['commit_outcome_unknown'])
        self.assertEqual(result['error_class'], 'PoolTimeout')
        self.assertEqual(result['cleared_documents'], 0)
        self.assertEqual(result['failed_phase_counts'], {'parent_connection': 1})
        self.assertGreaterEqual(result['failed_phase_ms']['parent_connection'], 10)
        self.assertFalse(self.archive.reads)

    def test_failed_capture_sql_has_attempt_phase_and_elapsed(self):
        self.cancel_phase('SELECT * FROM canonical_evidence_document_parts', 'capture_sql')

    def test_failed_batch_sql_has_attempt_phase_and_elapsed(self):
        self.cancel_phase('SELECT document.tenant_id,document.source_id,document.document_id,document.native_id,', 'batch_sql')

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
