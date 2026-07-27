from __future__ import annotations

import sys
from pathlib import Path
from unittest import TestCase


ROOT = Path(__file__).resolve().parents[2]
SERVER = ROOT / "server"
for candidate in (str(ROOT), str(SERVER)):
    if candidate not in sys.path:
        sys.path.insert(0, candidate)

from recall_server.evidence_worker import run_logical_evidence_worker  # noqa: E402


class FakeLogicalProjector:
    def __init__(self, results: list[dict[str, int | str]]) -> None:
        self.results = list(results)
        self.seed_calls: list[str | None] = []
        self.project_calls: list[dict[str, int | str | None]] = []

    def seed_backfill(self, *, tenant_id: str | None) -> int:
        self.seed_calls.append(tenant_id)
        return 7

    def project_pending(
        self,
        *,
        tenant_id: str | None,
        batch_size: int,
        max_batches: int,
        upload_concurrency: int,
    ) -> dict[str, int | str]:
        self.project_calls.append(
            {
                "tenant_id": tenant_id,
                "batch_size": batch_size,
                "max_batches": max_batches,
                "upload_concurrency": upload_concurrency,
            }
        )
        return self.results.pop(0)


def result(*, documents: int, records: int, pruned: int = 0) -> dict[str, int | str]:
    return {
        "status": "complete",
        "documents": documents,
        "records": records,
        "receipts": records,
        "objects": documents * 2,
        "bytes_uploaded": records * 100,
        "batches": int(documents > 0 or pruned > 0),
        "old_objects_deleted": 0,
        "cleanup_completed": 0,
        "cleanup_failures": 0,
        "cleanup_pending": 0,
        "source_races": 0,
        "pruned": pruned,
    }


class LogicalEvidenceWorkerTests(TestCase):
    def test_once_drains_without_full_corpus_seed_scan(self) -> None:
        projector = FakeLogicalProjector([result(documents=3, records=42)])

        actual = run_logical_evidence_worker(
            projector,  # type: ignore[arg-type]
            tenant_id="tenant:company:synthetic",
            batch_size=25,
            max_batches_per_cycle=100,
            upload_concurrency=2,
            interval_seconds=5,
            once=True,
        )

        self.assertEqual(actual["documents"], 3)
        self.assertEqual(actual["records"], 42)
        self.assertEqual(projector.seed_calls, [])
        self.assertEqual(
            projector.project_calls,
            [
                {
                    "tenant_id": "tenant:company:synthetic",
                    "batch_size": 25,
                    "max_batches": 100,
                    "upload_concurrency": 2,
                }
            ],
        )

    def test_invalid_worker_budgets_fail_before_projection(self) -> None:
        invalid = (
            {"batch_size": 0},
            {"batch_size": 2001},
            {"max_batches_per_cycle": 0},
            {"upload_concurrency": 0},
            {"upload_concurrency": 33},
            {"interval_seconds": 0.0},
        )
        for override in invalid:
            projector = FakeLogicalProjector([])
            values = {
                "tenant_id": "tenant:company:synthetic",
                "batch_size": 25,
                "max_batches_per_cycle": 10,
                "upload_concurrency": 2,
                "interval_seconds": 5,
                "once": True,
                **override,
            }
            with self.subTest(override=override), self.assertRaises(ValueError):
                run_logical_evidence_worker(
                    projector,  # type: ignore[arg-type]
                    **values,
                )
            self.assertEqual(projector.seed_calls, [])
            self.assertEqual(projector.project_calls, [])
