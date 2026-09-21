"""H3-a: search projection outbox, tombstones and shard catalog."""

from __future__ import annotations

import inspect
import unittest
from contextlib import contextmanager
from datetime import date, datetime, timezone
from pathlib import Path

from recall_server import SCHEMA_VERSION
from recall_server.logical_evidence_projection import (
    CanonicalLogicalEvidenceProjector,
)
from recall_server.passage_index import CanonicalPassageProjector
from recall_server.projection_worker import run_projection_worker
from recall_server.search_outbox import (
    SEARCH_OUTBOX_REASONS,
    enqueue_search_outbox,
    outbox_months,
    record_passage_deletions,
    search_outbox_pending,
    seed_search_outbox,
    write_search_tombstones,
)

SCHEMA = Path(__file__).resolve().parents[2] / "server" / "schema"


def _at(year: int, month: int, day: int) -> datetime:
    return datetime(year, month, day, 12, tzinfo=timezone.utc)


class _Result:
    def __init__(self, rowcount: int = 0, rows: list | None = None):
        self.rowcount = rowcount
        self.rows = rows or []

    def fetchall(self):
        return list(self.rows)

    def fetchone(self):
        return self.rows[0] if self.rows else None


class _Connection:
    """Records every statement; answers with a scripted rowcount."""

    def __init__(self, rowcount: int = 1, rows: list | None = None):
        self.statements: list[tuple[str, object]] = []
        self.rowcount = rowcount
        self.rows = rows

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    @contextmanager
    def transaction(self):
        yield self

    def execute(self, sql, params=None):
        self.statements.append((" ".join(sql.split()), params))
        return _Result(self.rowcount, self.rows)


class OutboxMonthsTest(unittest.TestCase):
    def test_span_across_two_months_yields_both(self) -> None:
        self.assertEqual(
            outbox_months([(_at(2026, 7, 30), _at(2026, 8, 2))]),
            [date(2026, 7, 1), date(2026, 8, 1)],
        )

    def test_single_month_and_duplicates_collapse(self) -> None:
        self.assertEqual(
            outbox_months([
                (_at(2026, 7, 1), _at(2026, 7, 31)),
                (_at(2026, 7, 4), _at(2026, 7, 5)),
            ]),
            [date(2026, 7, 1)],
        )

    def test_year_rollover_and_plain_dates(self) -> None:
        self.assertEqual(
            outbox_months([(date(2025, 11, 15), _at(2026, 2, 1))]),
            [
                date(2025, 11, 1),
                date(2025, 12, 1),
                date(2026, 1, 1),
                date(2026, 2, 1),
            ],
        )

    def test_iso_strings_are_read_in_utc(self) -> None:
        # LosslessPassage carries ISO strings; a +02:00 stamp just after
        # midnight on the 1st is still the previous month in UTC.
        self.assertEqual(
            outbox_months([("2026-08-01T01:30:00+02:00", "2026-08-01T01:30:00Z")]),
            [date(2026, 7, 1), date(2026, 8, 1)],
        )
        with self.assertRaises(ValueError):
            outbox_months([("2026-08-01T01:30:00", "2026-08-01T02:00:00")])

    def test_inverted_span_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            outbox_months([(_at(2026, 8, 1), _at(2026, 7, 1))])

    def test_empty_spans(self) -> None:
        self.assertEqual(outbox_months([]), [])


class EnqueueTest(unittest.TestCase):
    def test_upsert_bumps_generation_and_keeps_backfill_sticky(self) -> None:
        connection = _Connection(rowcount=2)
        touched = enqueue_search_outbox(
            connection,
            tenant_id="tenant:t",
            source_id="source:s",
            months=[date(2026, 8, 1), date(2026, 7, 1), _at(2026, 8, 9)],
            reason="logical-update",
        )
        self.assertEqual(touched, 2)
        (sql, params), = connection.statements
        self.assertIn("INSERT INTO search_projection_outbox(", sql)
        self.assertIn("ON CONFLICT(tenant_id,source_id,month)", sql)
        self.assertIn("generation=search_projection_outbox.generation+1", sql)
        self.assertIn("WHEN search_projection_outbox.reason='backfill' THEN 'backfill'", sql)
        self.assertIn("ELSE excluded.reason", sql)
        self.assertNotIn("first_queued_at=", sql.split("DO UPDATE SET", 1)[1])
        self.assertEqual(
            params,
            ("tenant:t", "source:s", "logical-update", [date(2026, 7, 1), date(2026, 8, 1)]),
        )

    def test_empty_months_write_nothing(self) -> None:
        connection = _Connection()
        self.assertEqual(
            enqueue_search_outbox(
                connection, tenant_id="t", source_id="s", months=[], reason="forget",
            ),
            0,
        )
        self.assertEqual(connection.statements, [])

    def test_unknown_reason_is_rejected(self) -> None:
        self.assertEqual(
            SEARCH_OUTBOX_REASONS,
            {"backfill", "logical-update", "forget", "header-change"},
        )
        with self.assertRaises(ValueError):
            enqueue_search_outbox(
                _Connection(), tenant_id="t", source_id="s",
                months=[date(2026, 7, 1)], reason="compaction",
            )


class TombstoneTest(unittest.TestCase):
    def test_tombstones_are_keyed_by_first_month_and_never_rewritten(self) -> None:
        connection = _Connection(rowcount=2)
        written = write_search_tombstones(
            connection,
            tenant_id="tenant:t",
            source_id="source:s",
            passages=[
                {"passage_id": "psg_" + "a" * 32, "first_occurred_at": _at(2026, 7, 31)},
                {"passage_id": "psg_" + "b" * 32, "first_occurred_at": _at(2026, 8, 1)},
            ],
        )
        self.assertEqual(written, 2)
        (sql, params), = connection.statements
        self.assertIn("INSERT INTO search_projection_tombstones(", sql)
        self.assertIn("ON CONFLICT(tenant_id,source_id,passage_id) DO NOTHING", sql)
        self.assertEqual(params[2], ["psg_" + "a" * 32, "psg_" + "b" * 32])
        self.assertEqual(params[3], [date(2026, 7, 1), date(2026, 8, 1)])

    def test_record_deletions_tombstones_then_enqueues_every_spanned_month(self) -> None:
        connection = _Connection(rowcount=1)
        counters = record_passage_deletions(
            connection,
            tenant_id="tenant:t",
            source_id="source:s",
            passages=[
                {
                    "passage_id": "psg_" + "a" * 32,
                    "first_occurred_at": _at(2026, 7, 31),
                    "last_occurred_at": _at(2026, 8, 1),
                },
            ],
            reason="forget",
        )
        self.assertEqual(counters, {"tombstones": 1, "queued": 1})
        self.assertEqual(len(connection.statements), 2)
        self.assertIn("search_projection_tombstones", connection.statements[0][0])
        self.assertIn("search_projection_outbox", connection.statements[1][0])
        self.assertEqual(
            connection.statements[1][1],
            ("tenant:t", "source:s", "forget", [date(2026, 7, 1), date(2026, 8, 1)]),
        )

    def test_no_passages_write_nothing(self) -> None:
        connection = _Connection()
        self.assertEqual(
            record_passage_deletions(
                connection, tenant_id="t", source_id="s", passages=[], reason="forget",
            ),
            {"tombstones": 0, "queued": 0},
        )
        self.assertEqual(connection.statements, [])


class SeedTest(unittest.TestCase):
    def test_seed_reads_parquet_shards_and_is_idempotent(self) -> None:
        connection = _Connection(rowcount=3)
        self.assertEqual(
            seed_search_outbox(connection, tenant_id="tenant:t", source_id=None), 3,
        )
        (sql, params), = connection.statements
        self.assertIn("SELECT DISTINCT tenant_id,source_id,bucket_start FROM canonical_parquet_scan_shards", sql)
        self.assertIn("1,'backfill'", sql)
        # Only a month waiting for an incremental reason is promoted; a
        # backfill row is left alone so a second seed touches zero rows.
        self.assertIn("WHERE search_projection_outbox.reason<>'backfill'", sql)
        self.assertNotIn("canonical_passages", sql)
        self.assertNotIn("canonical_evidence_documents", sql)
        self.assertEqual(params, ("tenant:t", None, None))

    def test_pending_count_is_tenant_scoped(self) -> None:
        connection = _Connection(rows=[{"count": 4}])
        self.assertEqual(search_outbox_pending(connection, tenant_id="tenant:t"), 4)
        (sql, params), = connection.statements
        self.assertIn("count(*)", sql)
        self.assertIn("FROM search_projection_outbox", sql)
        self.assertEqual(params, ("tenant:t", "tenant:t"))


class WritersTest(unittest.TestCase):
    def test_passage_commit_tombstones_deletes_and_enqueues_changed_months(self) -> None:
        source = inspect.getsource(CanonicalPassageProjector._commit)

        parquet = source.index("INSERT INTO canonical_parquet_scan_queue(")
        tombstones = source.index("write_search_tombstones(")
        outbox = source.index("enqueue_search_outbox(")
        self.assertLess(parquet, tombstones)
        self.assertLess(tombstones, outbox)
        self.assertIn('reason="logical-update"', source)
        # Months come from the passages this commit inserted or deleted (and
        # retained rows whose header was filled), never the whole document.
        self.assertIn("for passage in diff.to_insert", source[outbox:])
        self.assertIn("for row in (*deleted_rows, *retained_unheaded)", source)
        # The commit queues those months itself, so the header fill inside
        # it must not double-enqueue as header-change.
        self.assertIn("enqueue=False", source)

    def test_header_fill_enqueues_only_rows_whose_header_changed(self) -> None:
        source = inspect.getsource(CanonicalPassageProjector._write_headers)

        self.assertIn("AND passage.header_redacted IS NULL", source)
        self.assertIn("RETURNING passage.first_occurred_at,passage.last_occurred_at", source)
        self.assertIn("if enqueue and changed:", source)
        self.assertIn('reason="header-change"', source)
        self.assertIn("return len(changed)", source)

    def test_forget_tombstones_passages_before_their_documents_cascade(self) -> None:
        source = inspect.getsource(CanonicalLogicalEvidenceProjector.delete_native_ids)

        select = source.index("FROM canonical_passages passage")
        record = source.index("record_passage_deletions(")
        delete = source.index("DELETE FROM canonical_evidence_documents")
        self.assertLess(select, record)
        self.assertLess(record, delete)
        self.assertIn('reason="forget"', source)
        self.assertIn("document.native_parent_id=ANY(%s)", source[select:record])


class MigrationTest(unittest.TestCase):
    def test_schema_066_creates_outbox_tombstones_and_shards(self) -> None:
        self.assertEqual(SCHEMA_VERSION, 70)
        sql = (SCHEMA / "066_search_projection_outbox.sql").read_text()
        self.assertIn("CREATE TABLE IF NOT EXISTS search_projection_outbox", sql)
        self.assertIn("CREATE TABLE IF NOT EXISTS search_projection_tombstones", sql)
        self.assertIn("CREATE TABLE IF NOT EXISTS search_projection_shards", sql)
        self.assertIn(
            "reason IN ('backfill','logical-update','forget','header-change')", sql,
        )
        self.assertIn("PRIMARY KEY (tenant_id, source_id, month)", sql)
        self.assertIn("PRIMARY KEY (tenant_id, source_id, passage_id)", sql)
        self.assertIn("dataset_uri text NOT NULL", sql)
        self.assertIn("INSERT INTO schema_migrations(version) VALUES (66)", sql)
        # Re-applied on every `cli migrate`: nothing here rewrites rows.
        self.assertNotIn("INSERT INTO search_projection_outbox", sql)


class _Store:
    def __init__(self, count: int):
        self.count = count

    def connect(self):
        return _Connection(rows=[{"count": self.count}])


class _Projector:
    def __init__(self, store=None):
        if store is not None:
            self.store = store

    def project_pending(self, **_kwargs):
        return {
            "status": "complete", "documents": 0, "repaired": 0, "records": 0,
            "batches": 1, "cleanup_failures": 0, "pruned": 0, "pending": 0,
            "passages": 0, "stale": 0, "shards": 0, "rows": 0, "contended": 0,
        }

    def embed_pending(self, **_kwargs):
        return {"status": "complete", "processed": 0}


class WorkerCycleTest(unittest.TestCase):
    def test_cycle_log_reports_search_outbox_pending(self) -> None:
        result = run_projection_worker(
            _Projector(),  # type: ignore[arg-type]
            _Projector(_Store(5)),  # type: ignore[arg-type]
            _Projector(),  # type: ignore[arg-type]
            tenant_id="tenant:t",
            logical_batch_size=1,
            passage_batch_size=1,
            embedding_batch_size=1,
            max_batches_per_cycle=1,
            upload_concurrency=1,
            passage_concurrency=1,
            interval_seconds=1,
            once=True,
            sleep=lambda _seconds: None,
        )
        self.assertEqual(result["search_outbox_pending"], 5)
        self.assertEqual(result["status"], "complete")

    def test_projector_without_a_store_reports_zero(self) -> None:
        result = run_projection_worker(
            _Projector(),  # type: ignore[arg-type]
            _Projector(),  # type: ignore[arg-type]
            None,
            tenant_id="tenant:t",
            logical_batch_size=1,
            passage_batch_size=1,
            embedding_batch_size=1,
            max_batches_per_cycle=1,
            upload_concurrency=1,
            passage_concurrency=1,
            interval_seconds=1,
            once=True,
            sleep=lambda _seconds: None,
        )
        self.assertEqual(result["search_outbox_pending"], 0)


if __name__ == "__main__":
    unittest.main()
