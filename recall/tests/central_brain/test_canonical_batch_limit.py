from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest import mock


SERVER = Path(__file__).resolve().parents[2] / "server"
sys.path.insert(0, str(SERVER))

from recall_server.canonical import (  # noqa: E402
    MAX_CANONICAL_BATCH_EVENTS,
    CanonicalLifecycleError,
    CanonicalPlane,
)


class CanonicalBatchLimitTest(unittest.TestCase):
    @staticmethod
    def events(count: int) -> list[dict[str, str]]:
        return [
            {
                "kind": "message",
                "native_id": f"record-{index}",
                "source_id": "codex:linux:test",
            }
            for index in range(count)
        ]

    def test_accepts_the_safe_thousand_event_cap(self) -> None:
        plane = CanonicalPlane(None, None)  # type: ignore[arg-type]
        expected = {"status": "committed", "receipts": []}
        with mock.patch.object(
            plane,
            "_ingest_live_batch",
            return_value=expected,
        ) as ingest:
            result = plane.ingest_batch(
                tenant_id="tenant:test",
                principal_id="principal:test",
                events=self.events(MAX_CANONICAL_BATCH_EVENTS),
            )
        self.assertEqual(result, expected)
        ingest.assert_called_once()

    def test_rejects_above_the_safe_event_cap_before_storage(self) -> None:
        plane = CanonicalPlane(None, None)  # type: ignore[arg-type]
        with self.assertRaisesRegex(
            CanonicalLifecycleError,
            "canonical_batch_invalid",
        ):
            plane.ingest_batch(
                tenant_id="tenant:test",
                principal_id="principal:test",
                events=self.events(MAX_CANONICAL_BATCH_EVENTS + 1),
            )


if __name__ == "__main__":
    unittest.main()
