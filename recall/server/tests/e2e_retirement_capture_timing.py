#!/usr/bin/env python3
"""Capture timing is attempt telemetry, independent of acknowledged mutations."""
from pathlib import Path
import os
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

SERVER = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(SERVER.parent), str(SERVER)]
from e2e_archive_reprojection import fixture  # noqa: E402
from e2e_logical_source_integrity import TrackedStore  # noqa: E402
from recall_server import retirement_runner as runner  # noqa: E402


class CaptureTiming(unittest.TestCase):
    def setUp(self):
        self.store = TrackedStore(os.environ['RECALL_DATABASE_URL'])
        self.addCleanup(self.store.close)
        self.store.migrate()
        root = tempfile.TemporaryDirectory()
        self.addCleanup(root.cleanup)
        self.tenant, self.source, self.archive, *_ = fixture(self.store, Path(root.name), count=3)
        self.scope = runner.RetirementScope(self.tenant, 'principal:reprojection', (self.source,))
        with self.store.connect() as c:
            c.execute("INSERT INTO canonical_source_grants VALUES(%s,%s,%s,'owner',now())",
                      (self.tenant, self.scope.principal_id, self.source))
        plan = runner.plan_cohort(self.store, scope=self.scope)
        self.assertEqual(runner.enroll_cohort(self.store, scope=self.scope, reviewed_plan=plan)['inserted_parents'], 1)
        with self.store.connect() as c:
            c.execute("UPDATE canonical_chunk_retirement_progress SET updated_at=now()-interval '2 minutes' WHERE tenant_id=%s", (self.tenant,))
        self.env = patch.dict(os.environ, RECALL_CHUNK_BODY_READS='archive')
        self.env.start()
        self.addCleanup(self.env.stop)

    def run_owner(self):
        return runner.run_retirement(self.store, self.archive, scope=self.scope, apply=True)

    def test_actual_capture_time_reaches_runner_without_changing_clear(self):
        result = self.run_owner()
        self.assertEqual(result['cleared_documents'], 3)
        self.assertFalse(result['commit_outcome_unknown'])
        self.assertGreater(result['metadata_capture_ms'], 0)
        self.assertLessEqual(result['metadata_capture_ms'], result['parent_wall_ms'])
        self.assertGreater(result['committed_sql_ms'], 0)
        self.assertTrue(self.archive.reads)

    def test_real_capture_cancellation_preserves_time_without_clear_or_archive(self):
        original = self.store._execute_bounded
        cancellations = []

        def execute(connection, sql, values, deadline):
            if 'SELECT * FROM canonical_evidence_document_parts' in sql:
                cancellations.append(True)
                return original(connection, 'SELECT pg_sleep(%s)', (.1,), time.monotonic() + .02)
            return original(connection, sql, values, deadline)

        with patch.object(self.store, '_execute_bounded', execute):
            result = self.run_owner()
        self.assertEqual(len(cancellations), 1)
        self.assertGreaterEqual(result['metadata_capture_ms'], 10)
        self.assertEqual(result['cleared_documents'], 0)
        self.assertEqual(result['committed_sql_ms'], 0)
        self.assertEqual(result['committed_hash_ms'], 0)
        self.assertFalse(result['commit_outcome_unknown'])
        self.assertFalse(self.archive.reads)
        self.assertEqual(result['failed_parents'], 1)
        with self.store.connect() as c:
            self.assertEqual(c.execute("SELECT count(*) AS n FROM canonical_chunks WHERE tenant_id=%s AND text_redacted<>''", (self.tenant,)).fetchone()['n'], 3)

    def test_disabled_before_capture_reports_zero_attempt_time(self):
        from recall_server.chunk_retirement import set_parent_retirement_enabled
        def disable(_candidate):
            set_parent_retirement_enabled(self.store, tenant_id=self.tenant, source_id=self.source,
                                  native_parent_id='session', enabled=False)
        result = runner.run_retirement(self.store, self.archive, scope=self.scope, apply=True,
                                       before_parent=disable)
        self.assertEqual(result['metadata_capture_ms'], 0)
        self.assertEqual(result['cleared_documents'], 0)
        self.assertEqual(result['committed_sql_ms'], 0)
        self.assertFalse(self.archive.reads)


if __name__ == '__main__':
    unittest.main()
