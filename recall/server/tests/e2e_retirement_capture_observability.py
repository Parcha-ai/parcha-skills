#!/usr/bin/env python3
"""Capture measurements reach actual owner results without changing proof outcomes."""

import os
from pathlib import Path
import sys
import unittest
from unittest.mock import patch
import psycopg

SERVER = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(SERVER.parent), str(SERVER)]
from e2e_retirement_capture_timing import CaptureTiming  # noqa: E402
from recall_server.retirement_process import RetirementStore  # noqa: E402


class CaptureObservability(unittest.TestCase):
    setUp = CaptureTiming.setUp
    run_owner = CaptureTiming.run_owner

    def activate_worker(self):
        with self.store.connect() as connection:
            connection.execute(
                (SERVER / "schema/080_singleton_receipt_profile.sql").read_text()
            )
        with patch.dict(
            os.environ,
            {
                "RECALL_VERIFIED_SINGLETON_RECEIPTS": "1",
                "RECALL_SEARCH_PLANE": "turbopuffer",
            },
        ):
            worker = RetirementStore(self.store.dsn)
        self.addCleanup(worker.close)
        self.store = worker

    def test_real_delayed_stream_separates_membership_and_spool(self):
        self.activate_worker()
        original = psycopg.Cursor.stream

        def delayed(cursor, query, *args, **kwargs):
            if "SELECT selected.*,items.chunks FROM (" in query:
                query = query.replace(
                    "SELECT selected.*,items.chunks FROM (",
                    "SELECT selected.*,items.chunks FROM (SELECT pg_sleep(0.04)) pause CROSS JOIN (",
                )
            yield from original(cursor, query, *args, **kwargs)

        with patch.object(psycopg.Cursor, "stream", delayed):
            result = self.run_owner()
        self.assertFalse(result["commit_outcome_unknown"])
        self.assertEqual(result["cleared_documents"], 3)
        self.assertEqual(result["metadata_rows"], 3)
        self.assertEqual(result["metadata_fetches"], 1)  # One streamed acquisition.
        self.assertEqual(result["metadata_membership_calls"], 1)
        self.assertGreaterEqual(result["metadata_fetch_ms"], 30)
        self.assertGreater(result["metadata_membership_ms"], 0)
        self.assertGreater(result["metadata_spool_ms"], 0)
        self.assertLessEqual(
            sum(
                result[k]
                for k in (
                    "metadata_fetch_ms",
                    "metadata_membership_ms",
                    "metadata_spool_ms",
                )
            ),
            result["metadata_capture_ms"],
        )

    def test_actual_fetch_cancellation_records_attempt_without_reclassifying_unknown(
        self,
    ):
        self.activate_worker()
        original = psycopg.Cursor.stream

        def cancelled(cursor, query, *args, **kwargs):
            if "SELECT selected.*,items.chunks FROM (" in query:
                cursor.connection.execute("SET LOCAL statement_timeout='20ms'")
                query = query.replace(
                    "SELECT selected.*,items.chunks FROM (",
                    "SELECT selected.*,items.chunks FROM (SELECT pg_sleep(0.1)) pause CROSS JOIN (",
                )
            yield from original(cursor, query, *args, **kwargs)

        with patch.object(psycopg.Cursor, "stream", cancelled):
            result = self.run_owner()
        self.assertTrue(result["commit_outcome_unknown"])
        self.assertEqual(result["error_class"], "QueryCanceled")
        self.assertEqual(result["cleared_documents"], 0)
        self.assertEqual(result["metadata_rows"], 0)
        self.assertEqual(result["metadata_fetches"], 1)
        self.assertGreaterEqual(result["metadata_fetch_ms"], 15)
        self.assertEqual(result["metadata_membership_calls"], 0)
        self.assertEqual(result["metadata_spool_ms"], 0)
        self.assertFalse(self.archive.reads)

    def test_real_profile_contradiction_discards_staged_rows_without_clear(self):
        self.activate_worker()
        with self.store.connect() as connection:
            connection.execute(
                "UPDATE canonical_documents SET chunk_receipt_profile=1 WHERE tenant_id=%s",
                (self.tenant,),
            )
        result = self.run_owner()
        self.assertTrue(result["commit_outcome_unknown"])
        self.assertEqual(result["error_class"], "ReceiptMembershipError")
        self.assertEqual(result["cleared_documents"], 0)
        self.assertEqual(result["metadata_rows"], 3)
        self.assertEqual(result["metadata_fetches"], 1)
        self.assertEqual(result["metadata_membership_calls"], 1)
        self.assertGreater(result["metadata_membership_ms"], 0)
        self.assertGreater(result["metadata_spool_ms"], 0)
        self.assertFalse(self.archive.reads)


if __name__ == "__main__":
    unittest.main()
