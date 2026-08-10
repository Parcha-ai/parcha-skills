from __future__ import annotations

import unittest
import sys
from datetime import date, datetime, timezone
from pathlib import Path

SERVER = Path(__file__).resolve().parents[2] / "server"
sys.path.insert(0, str(SERVER))

from recall_server.archil_benchmark import (  # noqa: E402
    VARIANTS,
    _cohorts,
    _program,
    _summarize,
)


class _Result:
    def __init__(self, rows):
        self.rows = rows

    def fetchall(self):
        return self.rows


class _Connection:
    def __init__(self, rows):
        self.rows = rows

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return None

    def execute(self, *_):
        return _Result(self.rows)


class _Store:
    def __init__(self, rows):
        self.rows = rows

    def connect(self):
        return _Connection(self.rows)


class ArchilBenchmarkTests(unittest.TestCase):
    def setUp(self):
        first = datetime(2026, 8, 1, tzinfo=timezone.utc)
        self.rows = [
            {
                "source_id": "source:private:never-rendered",
                "bucket_start": date(2026, 8, 1),
                "dataset": dataset,
                "object_key": f"objects/{index:02x}/" + f"{index:064x}",
                "content_sha256": f"{index + 10:064x}",
                "row_count": 7 if dataset == "records" else 1,
                "size_bytes": index + 100,
                "first_occurred_at": first,
                "last_occurred_at": first,
            }
            for index, dataset in enumerate(
                ("actors", "documents", "records"),
                start=1,
            )
        ]

    def test_catalog_becomes_one_content_free_complete_cohort(self):
        cohorts = _cohorts(_Store(self.rows), "tenant:company:synthetic")
        self.assertEqual(len(cohorts), 1)
        self.assertEqual(cohorts[0].rows, 7)
        self.assertEqual(len(cohorts[0].objects), 3)
        self.assertEqual(
            set(cohorts[0].aliases.values()),
            {
                "s1/2026-08/actors.parquet",
                "s1/2026-08/documents.parquet",
                "s1/2026-08/records.parquet",
            },
        )

    def test_programs_emit_no_catalog_identity_or_result_content(self):
        cohort = _cohorts(
            _Store(self.rows),
            "tenant:company:synthetic",
        )[0]
        for variant in VARIANTS:
            with self.subTest(variant=variant):
                program = _program(variant, cohort)
                self.assertNotIn("source:private", program)
                self.assertNotIn("objects/", program)
                if variant != "stage_only":
                    self.assertIn(">/dev/null", program)

    def test_summary_contains_only_aggregate_measurements(self):
        samples = []
        for variant in VARIANTS:
            samples.append({
                "variant": variant,
                "size_band": "small",
                "ok": True,
                "rows": 7,
                "bytes": 303,
                "clientWallMs": 15,
                "timing": {
                    "totalMs": 14,
                    "queueMs": 3,
                    "executeMs": 11,
                    "phases": {"programMs": 1},
                },
            })
        summary = _summarize(samples)
        self.assertEqual(set(summary), set(VARIANTS))
        rendered = str(summary)
        self.assertNotIn("source:private", rendered)
        self.assertNotIn("objects/", rendered)
        self.assertEqual(summary["stage_only"]["failure_rate"], 0)


if __name__ == "__main__":
    unittest.main()
