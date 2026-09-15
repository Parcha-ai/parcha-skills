"""H3-b: the turbopuffer search plane writer over an in-memory outbox catalog."""

from __future__ import annotations

import logging
import sys
import unittest
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "server"))

from recall_server.projection_worker import run_projection_worker  # noqa: E402
from recall_server.turbopuffer_plane import (  # noqa: E402
    EMBED_TEXT_ATTRIBUTE,
    TEXT_ATTRIBUTE,
    TurbopufferSettings,
    namespace_schema,
)
from recall_server.turbopuffer_projection import (  # noqa: E402
    INCREMENTAL_REASONS,
    RATE_LIMIT_BACKOFF_CAP_SECONDS,
    TurbopufferProjector,
    byte_bounded_batches,
    drain_search_outbox,
    is_rate_limit,
)

from .fake_turbopuffer import FakeNamespace, FakeTurbopuffer, NotFoundError  # noqa: E402


def _upserts(write: dict) -> list[str]:
    return [str(row["id"]) for row in (write.get("upsert_rows") or ())]


def _deletes(write: dict) -> list[str]:
    return [str(identifier) for identifier in (write.get("deletes") or ())]


class RateLimitError(Exception):
    """Named like ``turbopuffer.RateLimitError``; the writer matches by class name."""

TENANT = "tenant:company:test"
SOURCE = "source:codex:test"
JULY, AUGUST = date(2026, 7, 1), date(2026, 8, 1)
SETTINGS = TurbopufferSettings(api_key="synthetic-key", write_batch_rows=2)
FP = "f" * 64


def _at(month: int, day: int, hour: int = 12) -> datetime:
    return datetime(2026, month, day, hour, tzinfo=timezone.utc)


def _passage(index: int, month: int, day: int, *, created_at: datetime | None = None) -> dict[str, Any]:
    occurred = _at(month, day)
    return {
        "passage_id": f"psg_{index:032x}",
        "source_id": SOURCE,
        "logical_document_id": "doc-1",
        "policy_fingerprint": FP,
        "ordinal": index,
        "first_occurred_at": occurred,
        "last_occurred_at": occurred + timedelta(minutes=5),
        "roles": ["user"],
        "receipts": [f"rcpt-{index}"],
        "spans": [{"receipt": f"rcpt-{index}", "start": 0, "end": 10}],
        "text_redacted": f"passage {index} about the tenant boundary",
        "text_sha256": "a" * 64,
        "header_redacted": "Codex session, July 2026",
        "native_parent_id": "session-1",
        "revision": 3,
        "manifest_object_key": "evidence/doc-1/manifest.json",
        "manifest_content_sha256": "b" * 64,
        "doc_first_occurred_at": _at(7, 1),
        "doc_last_occurred_at": _at(8, 31),
        "actors": [["author", "actor:alice"], ["participant", "actor:bob"]],
        "created_at": created_at or occurred,
        "live": True,
    }


class _Result:
    def __init__(self, rows: list[dict] | None = None, rowcount: int = 0):
        self.rows = rows or []
        self.rowcount = rowcount

    def fetchall(self):
        return list(self.rows)

    def fetchone(self):
        return self.rows[0] if self.rows else None


class _Catalog:
    """The outbox tables the projector reads and writes, interpreted from SQL."""

    def __init__(self) -> None:
        self.outbox: dict[tuple[str, date], dict[str, Any]] = {}
        self.shards: dict[tuple[str, date], dict[str, Any]] = {}
        self.tombstones: list[dict[str, Any]] = []
        self.passages: list[dict[str, Any]] = []
        self.watermark = _at(9, 1)
        self.statements: list[tuple[str, Any]] = []
        self.commits = 0

    def enqueue(self, month: date, *, generation: int = 1, reason: str = "backfill", queued_at: datetime | None = None) -> None:
        self.outbox[(SOURCE, month)] = {
            "generation": generation, "reason": reason,
            "queued_at": queued_at or _at(9, 1, 1),
        }

    def execute(self, sql: str, params: Any = None) -> _Result:
        folded = " ".join(sql.split())
        self.statements.append((folded, params))
        if "FROM search_projection_outbox" in folded and "count(*)" in folded:
            return _Result([{"count": len(self.outbox)}])
        if "FROM search_projection_outbox" in folded and folded.startswith("SELECT"):
            claims = sorted(
                (
                    {"tenant_id": TENANT, "source_id": source, "month": month, **row}
                    for (source, month), row in self.outbox.items()
                ),
                key=lambda row: (row["queued_at"], row["source_id"], row["month"]),
            )
            return _Result(claims[: params[1]])
        if folded.startswith("DELETE FROM search_projection_outbox"):
            _tenant, source, month, generation = params
            row = self.outbox.get((source, month))
            if row and row["generation"] == generation:
                del self.outbox[(source, month)]
                return _Result(rowcount=1)
            return _Result(rowcount=0)
        if "FROM search_projection_shards" in folded:
            shard = self.shards.get((params[1], params[2]))
            return _Result([shard] if shard else [])
        if folded.startswith("INSERT INTO search_projection_shards"):
            _tenant, source, month, generation, uri, rows, built_at = params
            self.shards[(source, month)] = {
                "generation": generation, "dataset_uri": uri,
                "row_count": rows, "built_at": built_at,
            }
            return _Result(rowcount=1)
        if "pg_stat_activity" in folded:
            return _Result([{"watermark": self.watermark}])
        if "FROM search_projection_tombstones" in folded and folded.startswith("SELECT"):
            _tenant, source, month = params
            return _Result([
                {"passage_id": row["passage_id"]}
                for row in self.tombstones
                if row["source_id"] == source and row["month"] == month
            ])
        if folded.startswith("DELETE FROM search_projection_tombstones"):
            _tenant, source, ids = params
            before = len(self.tombstones)
            self.tombstones = [
                row for row in self.tombstones
                if not (row["source_id"] == source and row["passage_id"] in ids)
            ]
            return _Result(rowcount=before - len(self.tombstones))
        if "FROM canonical_passages passage" in folded:
            _tenant, source, start, end, since, _since, cursor_time, cursor_id, limit = params
            page = sorted(
                (
                    row for row in self.passages
                    if row["source_id"] == source and row["live"]
                    and start <= row["first_occurred_at"] < end
                    and (since is None or row["created_at"] > since)
                    and (row["first_occurred_at"], row["passage_id"]) > (cursor_time, cursor_id)
                ),
                key=lambda row: (row["first_occurred_at"], row["passage_id"]),
            )
            return _Result([dict(row) for row in page[:limit]])
        raise AssertionError(f"unexpected statement: {folded[:80]}")


class _Connection:
    def __init__(self, catalog: _Catalog):
        self.catalog = catalog

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    @contextmanager
    def transaction(self):
        yield self

    def execute(self, sql, params=None):
        return self.catalog.execute(sql, params)

    def commit(self):
        self.catalog.commits += 1


class _Store:
    def __init__(self, catalog: _Catalog):
        self.catalog = catalog

    def connect(self):
        return _Connection(self.catalog)


def _projector(catalog: _Catalog, client: FakeTurbopuffer | None = None, **kwargs) -> tuple[TurbopufferProjector, FakeTurbopuffer]:
    client = client or FakeTurbopuffer()
    kwargs.setdefault("sleep", lambda _seconds: None)
    return TurbopufferProjector(_Store(catalog), SETTINGS, client=client, **kwargs), client


class RowShapeAndBatchingTest(unittest.TestCase):
    def test_backfill_upserts_every_live_passage_in_write_batches(self) -> None:
        catalog = _Catalog()
        catalog.passages = [_passage(1, 7, 3), _passage(2, 7, 9), _passage(3, 7, 20), _passage(4, 8, 2)]
        catalog.passages[2]["live"] = False  # a receipt lost its chunk: not live
        catalog.enqueue(JULY)
        projector, client = _projector(catalog)

        result = projector.drain(tenant_id=TENANT, max_months=4)

        self.assertEqual(result, {
            "status": "complete", "months": 1, "rows": 2, "deleted": 0,
            "failed": 0, "requeued": 0, "rate_limited": 0, "pending": 0,
        })
        namespace = client.namespace(SETTINGS.namespace(TENANT))
        self.assertEqual(sorted(namespace.rows), [_passage(1, 7, 3)["passage_id"], _passage(2, 7, 9)["passage_id"]])
        row = namespace.rows[_passage(1, 7, 3)["passage_id"]]
        self.assertEqual(row["month"], "2026-07")
        self.assertEqual(row["actor_ids"], ["actor:alice", "actor:bob"])
        self.assertEqual(row["actor_keys"], ["author:actor:alice", "participant:actor:bob"])
        self.assertEqual(row["revision"], 3)
        self.assertEqual(row["native_parent_id"], "session-1")
        self.assertEqual(row["header"], "Codex session, July 2026")
        self.assertEqual(row[TEXT_ATTRIBUTE], "passage 1 about the tenant boundary")
        self.assertTrue(row[EMBED_TEXT_ATTRIBUTE].startswith("Codex session, July 2026\n\npassage 1"))
        self.assertEqual(row["first_occurred_at"], "2026-07-03T12:00:00+00:00")
        # Every write declares the schema and the metric; batches follow
        # write_batch_rows (2), so two rows are one write.
        self.assertEqual(len(namespace.writes), 1)
        self.assertEqual(namespace.writes[0]["schema"], namespace_schema(SETTINGS))
        self.assertEqual(namespace.writes[0]["distance_metric"], "cosine_distance")
        self.assertEqual(catalog.outbox, {})
        shard = catalog.shards[(SOURCE, JULY)]
        self.assertEqual(shard["generation"], 1)
        self.assertEqual(shard["row_count"], 2)
        self.assertEqual(shard["built_at"], catalog.watermark)
        self.assertEqual(shard["dataset_uri"], f"turbopuffer://{SETTINGS.region}/{SETTINGS.namespace(TENANT)}")

    def test_pages_of_batch_rows_become_one_write_each(self) -> None:
        catalog = _Catalog()
        catalog.passages = [_passage(index, 7, 1 + index) for index in range(1, 6)]
        catalog.enqueue(JULY)
        projector, client = _projector(catalog)

        result = projector.drain(tenant_id=TENANT, max_months=1)

        namespace = client.namespace(SETTINGS.namespace(TENANT))
        self.assertEqual(result["rows"], 5)
        self.assertEqual([len(_upserts(write)) for write in namespace.writes], [2, 2, 1])
        self.assertEqual(len(namespace.rows), 5)

    def test_months_are_claimed_oldest_first_and_bounded(self) -> None:
        catalog = _Catalog()
        catalog.passages = [_passage(1, 7, 3), _passage(2, 8, 3)]
        catalog.enqueue(AUGUST, queued_at=_at(9, 1, 1))
        catalog.enqueue(JULY, queued_at=_at(9, 1, 2))
        projector, client = _projector(catalog)

        first = projector.drain(tenant_id=TENANT, max_months=1)

        self.assertEqual((first["months"], first["pending"], first["status"]), (1, 1, "pending"))
        self.assertEqual(list(catalog.outbox), [(SOURCE, JULY)])
        second = projector.drain(tenant_id=TENANT, max_months=1)
        self.assertEqual((second["months"], second["pending"], second["status"]), (1, 0, "complete"))
        self.assertEqual(len(client.namespace(SETTINGS.namespace(TENANT)).rows), 2)


class TombstoneTest(unittest.TestCase):
    def test_tombstones_become_deletes_and_are_removed(self) -> None:
        catalog = _Catalog()
        catalog.passages = [_passage(1, 7, 3)]
        catalog.tombstones = [
            {"source_id": SOURCE, "passage_id": "psg_" + "d" * 32, "month": JULY},
            {"source_id": SOURCE, "passage_id": "psg_" + "e" * 32, "month": JULY},
            {"source_id": SOURCE, "passage_id": "psg_" + "0" * 32, "month": AUGUST},
        ]
        catalog.enqueue(JULY, reason="forget")
        projector, client = _projector(catalog)
        namespace = client.namespace(SETTINGS.namespace(TENANT))
        namespace.write(upsert_rows=[{"id": "psg_" + "d" * 32, "text": "old"}])

        result = projector.drain(tenant_id=TENANT, max_months=1)

        self.assertEqual((result["deleted"], result["rows"]), (2, 1))
        self.assertNotIn("psg_" + "d" * 32, namespace.rows)
        # Deletes go before upserts, then the July tombstones are gone and
        # August's waits for its own month.
        self.assertEqual(_deletes(namespace.writes[1]), ["psg_" + "d" * 32, "psg_" + "e" * 32])
        self.assertEqual(_upserts(namespace.writes[2]), [_passage(1, 7, 3)["passage_id"]])
        self.assertEqual([row["passage_id"] for row in catalog.tombstones], ["psg_" + "0" * 32])

    def test_delete_only_write_to_an_unknown_namespace_is_not_a_failure(self) -> None:
        catalog = _Catalog()
        catalog.tombstones = [{"source_id": SOURCE, "passage_id": "psg_" + "d" * 32, "month": JULY}]
        catalog.enqueue(JULY, reason="forget")
        # The fake raises NotFoundError itself for a delete-only write to a
        # namespace that never received an upsert, as the service does.
        projector, client = _projector(catalog)

        result = projector.drain(tenant_id=TENANT, max_months=1)

        self.assertIsInstance(NotFoundError("x"), Exception)
        self.assertEqual(client.namespace(SETTINGS.namespace(TENANT)).writes, [])
        self.assertEqual((result["failed"], result["deleted"], result["months"]), (0, 1, 1))
        self.assertEqual(catalog.tombstones, [])
        self.assertEqual(catalog.outbox, {})


class GenerationTest(unittest.TestCase):
    def test_generation_moved_during_the_write_keeps_the_outbox_row(self) -> None:
        catalog = _Catalog()
        catalog.passages = [_passage(1, 7, 3)]
        catalog.enqueue(JULY, generation=4, reason="logical-update")
        client = FakeTurbopuffer()

        class RequeuingNamespace(FakeNamespace):
            def write(self, **kwargs):
                catalog.outbox[(SOURCE, JULY)]["generation"] = 5
                return super().write(**kwargs)

        client.namespaces[SETTINGS.namespace(TENANT)] = RequeuingNamespace(SETTINGS.namespace(TENANT))
        projector, _ = _projector(catalog, client)

        result = projector.drain(tenant_id=TENANT, max_months=1)

        self.assertEqual((result["months"], result["requeued"], result["pending"]), (1, 1, 1))
        self.assertEqual(result["status"], "pending")
        self.assertEqual(catalog.outbox[(SOURCE, JULY)]["generation"], 5)
        # The shard row still records what was written so the next pass is incremental.
        self.assertEqual(catalog.shards[(SOURCE, JULY)]["generation"], 4)
        self.assertEqual(catalog.shards[(SOURCE, JULY)]["built_at"], catalog.watermark)


class IncrementalTest(unittest.TestCase):
    def test_incremental_reason_sends_passages_created_after_the_watermark(self) -> None:
        catalog = _Catalog()
        catalog.passages = [
            _passage(1, 7, 3, created_at=_at(8, 1)),
            _passage(2, 7, 4, created_at=_at(8, 20)),
        ]
        catalog.shards[(SOURCE, JULY)] = {
            "generation": 1, "dataset_uri": "turbopuffer://x/y",
            "row_count": 1, "built_at": _at(8, 10),
        }
        for reason in sorted(INCREMENTAL_REASONS):
            with self.subTest(reason=reason):
                catalog.enqueue(JULY, generation=2, reason=reason)
                projector, client = _projector(catalog)
                result = projector.drain(tenant_id=TENANT, max_months=1)
                namespace = client.namespace(SETTINGS.namespace(TENANT))
                self.assertEqual(result["rows"], 1)
                self.assertEqual(list(namespace.rows), [_passage(2, 7, 4)["passage_id"]])
                self.assertEqual(catalog.shards[(SOURCE, JULY)]["built_at"], catalog.watermark)
                catalog.shards[(SOURCE, JULY)]["built_at"] = _at(8, 10)

    def test_backfill_ignores_the_watermark(self) -> None:
        catalog = _Catalog()
        catalog.passages = [
            _passage(1, 7, 3, created_at=_at(8, 1)),
            _passage(2, 7, 4, created_at=_at(8, 20)),
        ]
        catalog.shards[(SOURCE, JULY)] = {
            "generation": 1, "dataset_uri": "turbopuffer://x/y",
            "row_count": 1, "built_at": _at(8, 10),
        }
        catalog.enqueue(JULY, generation=2, reason="backfill")
        projector, client = _projector(catalog)

        result = projector.drain(tenant_id=TENANT, max_months=1)

        self.assertEqual(result["rows"], 2)
        self.assertEqual(len(client.namespace(SETTINGS.namespace(TENANT)).rows), 2)
        watermark_reads = [
            params for sql, params in catalog.statements if "FROM canonical_passages passage" in sql
        ]
        self.assertTrue(all(params[4] is None for params in watermark_reads))

    def test_incremental_without_a_shard_row_sends_everything(self) -> None:
        catalog = _Catalog()
        catalog.passages = [_passage(1, 7, 3, created_at=_at(8, 1))]
        catalog.enqueue(JULY, reason="logical-update")
        projector, _client = _projector(catalog)
        self.assertEqual(projector.drain(tenant_id=TENANT, max_months=1)["rows"], 1)


def _client_rows(client: FakeTurbopuffer) -> dict:
    return client.namespace(SETTINGS.namespace(TENANT)).rows


class RateLimitTest(unittest.TestCase):
    def test_rate_limited_batch_backs_off_and_is_retried(self) -> None:
        catalog = _Catalog()
        catalog.passages = [_passage(1, 7, 3), _passage(2, 7, 4), _passage(3, 7, 5)]
        catalog.enqueue(JULY)
        client = FakeTurbopuffer()
        sleeps: list[float] = []
        remaining = {"failures": 3}

        class ThrottledNamespace(FakeNamespace):
            def write(self, **kwargs):
                if remaining["failures"]:
                    remaining["failures"] -= 1
                    raise RateLimitError("429")
                return super().write(**kwargs)

        client.namespaces[SETTINGS.namespace(TENANT)] = ThrottledNamespace(SETTINGS.namespace(TENANT))
        projector, _ = _projector(catalog, client, sleep=sleeps.append)

        result = projector.drain(tenant_id=TENANT, max_months=1)

        self.assertEqual((result["failed"], result["months"], result["rows"]), (0, 1, 3))
        self.assertEqual(result["rate_limited"], 3)
        self.assertEqual(sleeps, [1.0, 2.0, 4.0])
        self.assertEqual(len(_client_rows(client)), 3)
        self.assertEqual(catalog.outbox, {})
        # Only the throttled batch was retried: two batches, three attempts on the first.
        self.assertEqual([len(_upserts(write)) for write in client.namespace(SETTINGS.namespace(TENANT)).writes], [2, 1])

    def test_backoff_caps_and_an_exhausted_budget_fails_the_month(self) -> None:
        catalog = _Catalog()
        catalog.passages = [_passage(1, 7, 3)]
        catalog.enqueue(JULY)
        client = FakeTurbopuffer()
        client.namespace(SETTINGS.namespace(TENANT)).fail_writes = RateLimitError("429")
        sleeps: list[float] = []
        projector, _ = _projector(catalog, client, sleep=sleeps.append, rate_limit_budget_seconds=100.0)

        with self.assertLogs("recall_server.turbopuffer_projection", level=logging.WARNING) as logs:
            result = projector.drain(tenant_id=TENANT, max_months=1)

        self.assertEqual((result["failed"], result["months"]), (1, 0))
        # 1+2+4+8+16+32 = 63 spent; 64 does not fit in the remaining 37.
        self.assertEqual(sleeps, [1.0, 2.0, 4.0, 8.0, 16.0, 32.0])
        self.assertEqual(result["rate_limited"], 0)  # the month failed: its attempts are not summed
        self.assertEqual(list(catalog.outbox), [(SOURCE, JULY)])
        self.assertTrue(any("rate limit budget exhausted" in line for line in logs.output))
        self.assertTrue(all("429" not in line or "type=RateLimitError" in line for line in logs.output))
        self.assertLessEqual(max(sleeps), RATE_LIMIT_BACKOFF_CAP_SECONDS)

    def test_is_rate_limit_matches_the_sdk_class_by_name(self) -> None:
        import turbopuffer

        self.assertTrue(is_rate_limit(RateLimitError("x")))
        self.assertTrue(issubclass(turbopuffer.RateLimitError, Exception))
        self.assertTrue(any("RateLimit" in klass.__name__ for klass in turbopuffer.RateLimitError.__mro__))
        self.assertFalse(is_rate_limit(ValueError("x")))


class ByteClampTest(unittest.TestCase):
    def test_batches_are_bounded_by_rows_and_bytes(self) -> None:
        rows = [{"id": f"psg_{index:032x}", "text": "x" * 1000} for index in range(6)]
        by_rows = byte_bounded_batches(rows, max_rows=4, max_bytes=10**9)
        self.assertEqual([len(batch) for batch in by_rows], [4, 2])
        by_bytes = byte_bounded_batches(rows, max_rows=100, max_bytes=2500)
        self.assertEqual([len(batch) for batch in by_bytes], [2, 2, 2])
        oversized = byte_bounded_batches(rows[:2], max_rows=100, max_bytes=100)
        self.assertEqual([len(batch) for batch in oversized], [1, 1])
        self.assertEqual(byte_bounded_batches([], max_rows=1, max_bytes=1024), [])

    def test_projector_splits_a_page_by_bytes(self) -> None:
        catalog = _Catalog()
        catalog.passages = [_passage(1, 7, 3), _passage(2, 7, 4)]
        catalog.enqueue(JULY)
        projector, client = _projector(catalog, max_batch_bytes=1024)

        result = projector.drain(tenant_id=TENANT, max_months=1)

        self.assertEqual(result["rows"], 2)
        self.assertEqual([len(_upserts(write)) for write in client.namespace(SETTINGS.namespace(TENANT)).writes], [1, 1])


class FailureTest(unittest.TestCase):
    def test_failing_write_leaves_the_row_and_counts_the_month(self) -> None:
        catalog = _Catalog()
        catalog.passages = [_passage(1, 7, 3), _passage(2, 8, 3)]
        catalog.enqueue(JULY, queued_at=_at(9, 1, 1))
        catalog.enqueue(AUGUST, queued_at=_at(9, 1, 2))
        projector, client = _projector(catalog)
        namespace = client.namespace(SETTINGS.namespace(TENANT))

        class SyntheticOutage(Exception):
            pass

        namespace.fail_writes = SyntheticOutage("500 boom: secret-text-must-not-leak")
        with self.assertLogs("recall_server.turbopuffer_projection", level=logging.WARNING) as logs:
            result = projector.drain(tenant_id=TENANT, max_months=4)

        # The fake fails every write, so both months fail and both stay queued.
        self.assertEqual(result["failed"], 2)
        self.assertEqual(result["months"], 0)
        self.assertEqual(result["status"], "pending")
        self.assertEqual(sorted(catalog.outbox), [(SOURCE, JULY), (SOURCE, AUGUST)])
        self.assertEqual(catalog.shards, {})
        self.assertEqual(len(logs.output), 2)
        self.assertIn("type=SyntheticOutage", logs.output[0])
        self.assertNotIn("secret-text", logs.output[0])
        self.assertNotIn("synthetic-key", logs.output[0])
        # The failed months are retried on the next drain.
        namespace.fail_writes = None
        retry = projector.drain(tenant_id=TENANT, max_months=4)
        self.assertEqual((retry["failed"], retry["months"], retry["pending"]), (0, 2, 0))
        self.assertEqual(len(_client_rows(client)), 2)

    def test_deadline_stops_claiming_further_months(self) -> None:
        catalog = _Catalog()
        catalog.passages = [_passage(1, 7, 3), _passage(2, 8, 3)]
        catalog.enqueue(JULY, queued_at=_at(9, 1, 1))
        catalog.enqueue(AUGUST, queued_at=_at(9, 1, 2))
        ticks = iter([0.0, 10.0])
        projector, _client = _projector(catalog, clock=lambda: next(ticks))

        result = projector.drain(tenant_id=TENANT, max_months=4, deadline_at=5.0)

        self.assertEqual((result["months"], result["pending"]), (1, 1))

    def test_invalid_budgets_are_rejected(self) -> None:
        catalog = _Catalog()
        with self.assertRaises(ValueError):
            TurbopufferProjector(_Store(catalog), SETTINGS, client=FakeTurbopuffer(), batch_rows=0)
        with self.assertRaises(ValueError):
            TurbopufferProjector(_Store(catalog), SETTINGS, client=FakeTurbopuffer(), max_batch_bytes=1)
        projector, _client = _projector(catalog)
        with self.assertRaises(ValueError):
            projector.drain(tenant_id=TENANT, max_months=0)

    def test_drain_helper_uses_the_injected_client(self) -> None:
        catalog = _Catalog()
        catalog.passages = [_passage(1, 7, 3)]
        catalog.enqueue(JULY)
        client = FakeTurbopuffer()
        result = drain_search_outbox(
            _Store(catalog), SETTINGS, tenant_id=TENANT, max_months=2, client=client, batch_rows=10,
        )
        self.assertEqual(result["rows"], 1)
        self.assertEqual(len(client.namespace(SETTINGS.namespace(TENANT)).rows), 1)


class _Projector:
    def project_pending(self, **_kwargs):
        return {
            "status": "complete", "documents": 0, "repaired": 0, "records": 0,
            "batches": 1, "cleanup_failures": 0, "pruned": 0, "pending": 0,
            "passages": 0, "stale": 0, "shards": 0, "rows": 0, "contended": 0,
        }

    def embed_pending(self, **_kwargs):
        return {"status": "complete", "processed": 0}


class WorkerPhaseTest(unittest.TestCase):
    def _run(self, search_plane):
        return run_projection_worker(
            _Projector(),  # type: ignore[arg-type]
            _Projector(),  # type: ignore[arg-type]
            _Projector(),  # type: ignore[arg-type]
            tenant_id=TENANT,
            logical_batch_size=1,
            passage_batch_size=1,
            embedding_batch_size=1,
            max_batches_per_cycle=1,
            upload_concurrency=1,
            passage_concurrency=1,
            interval_seconds=1,
            once=True,
            sleep=lambda _seconds: None,
            search_plane=search_plane,
        )

    def test_phase_is_skipped_without_settings(self) -> None:
        result = self._run(None)
        self.assertEqual(result["status"], "complete")
        for key in ("search_plane_months", "search_plane_rows", "search_plane_deleted", "search_plane_failed", "search_plane_rate_limited", "search_plane_elapsed_ms"):
            self.assertEqual(result[key], 0)

    def test_phase_counts_reach_the_cycle_result(self) -> None:
        calls: list[int] = []

        def drain():
            calls.append(1)
            return {"status": "pending", "months": 2, "rows": 9, "deleted": 3, "failed": 1, "rate_limited": 4, "pending": 5}

        with self.assertLogs("recall_server.projection_worker", level=logging.INFO) as logs:
            result = self._run(drain)
        self.assertEqual(calls, [1])
        self.assertEqual(result["search_plane_months"], 2)
        self.assertEqual(result["search_plane_rows"], 9)
        self.assertEqual(result["search_plane_deleted"], 3)
        self.assertEqual(result["search_plane_failed"], 1)
        self.assertEqual(result["search_plane_rate_limited"], 4)
        self.assertEqual(result["status"], "pending")
        line = next(entry for entry in logs.output if "projection cycle" in entry)
        self.assertIn("search_plane_months=2 search_plane_rows=9 search_plane_deleted=3 search_plane_failed=1 search_plane_rate_limited=4", line)
        self.assertIn("search_plane_elapsed_ms=", line)


if __name__ == "__main__":
    unittest.main()


class PacingAndTransientTests(unittest.TestCase):
    def test_token_pacer_sleeps_when_the_window_is_full(self) -> None:
        from recall_server.turbopuffer_projection import TokenPacer, estimated_tokens

        now = [100.0]
        slept: list[float] = []

        def sleep(seconds: float) -> None:
            slept.append(seconds)
            now[0] += seconds

        pacer = TokenPacer(1000, clock=lambda: now[0], sleep=sleep)
        pacer.wait_for(600)
        pacer.wait_for(300)
        self.assertEqual(slept, [])
        pacer.wait_for(300)  # 1200 > 1000: wait until the first entry leaves the window
        self.assertEqual(len(slept), 1)
        self.assertGreaterEqual(now[0], 160.0)
        self.assertEqual(estimated_tokens([{"embed_text": "a" * 400}, {"embed_text": ""}]), 102)
        TokenPacer(0).wait_for(10**9)  # disabled

    def test_transient_errors_are_retried_and_others_raise(self) -> None:
        from recall_server.turbopuffer_projection import is_transient

        class InternalServerError(Exception):
            status_code = 502

        class APIConnectionError(Exception):
            pass

        class RateLimitError(Exception):
            pass

        class BadRequestError(Exception):
            status_code = 400

        self.assertTrue(is_transient(InternalServerError()))
        self.assertTrue(is_transient(APIConnectionError()))
        self.assertTrue(is_transient(RateLimitError()))
        self.assertFalse(is_transient(BadRequestError()))
        self.assertFalse(is_transient(ValueError("x")))

