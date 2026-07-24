from __future__ import annotations

import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path

SERVER = Path(__file__).resolve().parents[2] / "server"
sys.path.insert(0, str(SERVER))

from recall_server.canonical_retrieval import BoundCanonicalRetrieval  # noqa: E402
from recall_server.mcp import CANONICAL_SHOW_TOOL  # noqa: E402


class CanonicalInvestigatorContractTest(unittest.TestCase):
    def test_relative_windows_are_utc_and_source_time_based(self) -> None:
        now = datetime(2026, 7, 24, 12, 30, tzinfo=timezone.utc)
        cases = {
            "what happened today?": (
                "2026-07-24T00:00:00+00:00",
                "question:today",
            ),
            "what happened yesterday?": (
                "2026-07-23T00:00:00+00:00",
                "question:yesterday",
            ),
            "what changed in the past 2 days?": (
                "2026-07-22T12:30:00+00:00",
                "question:last-2-days",
            ),
        }
        for question, (expected_since, expected_reason) in cases.items():
            with self.subTest(question=question):
                since, until, reason = (
                    BoundCanonicalRetrieval._question_time_window(
                        question,
                        now=now,
                    )
                )
                self.assertEqual(since, expected_since)
                self.assertEqual(reason, expected_reason)
                self.assertIsNotNone(until)

    def test_canonical_show_does_not_advertise_rejected_arguments(self) -> None:
        self.assertEqual(
            set(CANONICAL_SHOW_TOOL["inputSchema"]["properties"]),
            {"target"},
        )


if __name__ == "__main__":
    unittest.main()
