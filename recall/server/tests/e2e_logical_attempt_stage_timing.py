#!/usr/bin/env python3
"""Attempt stage wall times remain observable on actual owner failures."""
import unittest
from unittest.mock import patch

import psycopg

import e2e_logical_append_capability as fixtures
from e2e_logical_append_observability import AppendObservability
from recall_server.logical_evidence_projection import LOG, _LogicalWorkSuperseded


FIELDS = ('input_capture_wall_ms', 'archive_prepare_wall_ms', 'commit_wall_ms')


class AttemptStageTiming(unittest.TestCase):
    setUp = fixtures.AppendCapability.setUp
    event = fixtures.AppendCapability.event
    ingest = fixtures.AppendCapability.ingest
    publish = fixtures.AppendCapability.publish
    completion = AppendObservability.completion

    def stages(self, log, present):
        for name, expected in zip(FIELDS, present, strict=True):
            self.assertIn(name, log)
            if expected:
                self.assertGreaterEqual(int(log[name]), 0)
                self.assertLessEqual(int(log[name]), int(log['elapsed_ms']))
            else:
                self.assertEqual(log[name], 'None')

    def test_full_then_append_records_each_entered_stage(self):
        self.ingest('old', 1)
        result, log = self.completion(self.publish)
        self.assertEqual(result['documents'], 1)
        self.stages(log, (True, True, True))
        self.assertEqual(log['input_sql_rows'], '1')
        self.ingest('new', 2)
        self.logical.append_enabled = True
        result, log = self.completion(self.publish)
        self.assertEqual(result['documents'], 1)
        self.assertEqual(log['append_used'], 'True')
        self.assertEqual(log['input_sql_rows'], '1')
        self.stages(log, (True, True, True))

    def test_actual_fetch_timeout_keeps_capture_time_and_unentered_stages_null(self):
        self.ingest('old', 1)
        original = psycopg.ServerCursor.execute
        cancellations = []
        def slow(cursor, query, params=None, **kwargs):
            if cursor.name == 'logical_evidence_batch_stream':
                cursor.connection.execute("SET LOCAL statement_timeout='20ms'")
                query, params = 'SELECT pg_sleep(1)', None
                cancellations.append(True)
            return original(cursor, query, params, **kwargs)
        with patch.object(psycopg.ServerCursor, 'execute', slow):
            result, log = self.completion(self.publish)
        self.assertEqual(cancellations, [True])
        self.assertEqual(result['failed'], 1)
        self.assertEqual(log['status'], 'failed')
        self.assertEqual(log['input_sql_rows'], 'None')
        self.stages(log, (True, False, False))
        self.assertGreaterEqual(int(log['input_capture_wall_ms']), 15)

    def test_archive_failure_keeps_entered_time_without_commit(self):
        self.ingest('old', 1)
        with patch.object(self.logical.projection, 'put_records', side_effect=ConnectionError('private failure text')):
            result, log = self.completion(self.publish)
        self.assertEqual(result['failed'], 1)
        self.assertEqual(log['status'], 'failed')
        self.stages(log, (True, True, False))
        self.assertNotIn('private', str(log))

    def test_dispatch_refusal_leaves_all_stages_null(self):
        self.ingest('old', 1)
        with patch.object(self.logical, '_candidate_at_dispatch', side_effect=_LogicalWorkSuperseded):
            result, log = self.completion(self.publish)
        self.assertEqual(result['source_races'], 1)
        self.assertEqual(log['status'], 'stale')
        self.stages(log, (False, False, False))

    def test_real_commit_lost_ack_remains_error_with_commit_attempt_time(self):
        self.ingest('old', 1)
        original = self.logical._commit_upload
        def lost_ack(*args, **kwargs):
            original(*args, **kwargs)
            raise ConnectionError('private lost acknowledgment')
        with patch.object(self.logical, '_commit_upload', lost_ack), self.assertLogs(LOG.name, level='INFO') as captured:
            with self.assertRaises(ConnectionError):
                self.publish()
        lines = [line for line in captured.output if 'logical owner completed ' in line]
        self.assertEqual(len(lines), 1)
        log = dict(item.split('=', 1) for item in lines[0].split() if '=' in item)
        self.assertEqual(log['status'], 'error')
        self.assertEqual(log['input_sql_rows'], 'None')
        self.stages(log, (True, True, True))
        self.assertNotIn('private', lines[0])


if __name__ == '__main__':
    unittest.main()
