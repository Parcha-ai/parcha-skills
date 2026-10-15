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

    def test_real_delayed_fetch_separates_membership_and_spool(self):
        self.activate_worker()
        original = psycopg.ServerCursor.execute

        def delayed(cursor, query, *args, **kwargs):
            if cursor.name == "retirement_metadata":
                query = query.replace(
                    "SELECT selected.*,items.chunks FROM (",
                    "SELECT selected.*,items.chunks FROM (SELECT pg_sleep(0.04)) pause CROSS JOIN (",
                )
            return original(cursor, query, *args, **kwargs)

        with patch.object(psycopg.ServerCursor, "execute", delayed):
            result = self.run_owner()
        self.assertFalse(result["commit_outcome_unknown"])
        self.assertEqual(result["cleared_documents"], 3)
        self.assertEqual(result["metadata_rows"], 3)
        self.assertEqual(result["metadata_fetches"], 2)  # One data batch plus EOF.
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
        original = psycopg.Cursor.execute

        def cancelled(cursor, query, params=None, **kwargs):
            rendered = (
                query.as_string(cursor.connection)
                if hasattr(query, "as_string")
                else str(query)
            )
            if "FETCH FORWARD" in rendered and "retirement_metadata" in rendered:
                query = (
                    "SELECT set_config('statement_timeout','20ms',true); SELECT pg_sleep(0.1); "
                    + rendered[rendered.index("FETCH FORWARD") :]
                )
            return original(cursor, query, params, **kwargs)

        with patch.object(psycopg.Cursor, "execute", cancelled):
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

    def test_real_profile_contradiction_keeps_received_rows_but_no_spool_or_clear(self):
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
        self.assertEqual(result["metadata_spool_ms"], 0)
        self.assertFalse(self.archive.reads)


if __name__ == "__main__":
    unittest.main()
