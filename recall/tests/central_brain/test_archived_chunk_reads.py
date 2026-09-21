"""Exact public reader contracts when chunk bodies live in the archive."""
from __future__ import annotations

import copy
import hashlib
import sys
import unittest
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

SERVER = Path(__file__).resolve().parents[2] / "server"
sys.path.insert(0, str(SERVER))

from recall_server.archive import ArchiveCorruption  # noqa: E402
from recall_server.canonical_retrieval import BoundCanonicalRetrieval  # noqa: E402
from recall_server.db import SearchDeadlineExceeded  # noqa: E402

TENANT = "tenant:archived:test"
SOURCE = "source:archived:test"
PARENT = "session:archived:test"
START = datetime(2026, 9, 20, 10, tzinfo=timezone.utc)


def receipt(native: str, ordinal: int) -> str:
    return f"recall://{SOURCE}/{native}?rev=3#item={ordinal}"


def event(native: str, minute: int, texts: list[str]) -> dict:
    return {
        "source_id": SOURCE,
        "document_id": f"doc:{native}",
        "native_id": native,
        "native_parent_id": PARENT,
        "revision": 3,
        "event_id": f"evt:{native}",
        "kind": "transcript_record",
        "occurred_at": START + timedelta(minutes=minute),
        "observed_at": START + timedelta(minutes=minute, seconds=1),
        "created_at": START + timedelta(minutes=minute, seconds=2),
        "canonical_redacted": {
            "content": {"message": {"role": "assistant", "text": "original envelope β"}},
            "provenance": {"cwd": "/synthetic/project"},
        },
        "chunks": [
            {"ordinal": ordinal, "text": text, "receipt": receipt(native, ordinal)}
            for ordinal, text in enumerate(texts)
        ],
    }


class Rows:
    def __init__(self, rows):
        self.rows = copy.deepcopy(rows)

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def fetchall(self):
        return self.rows


class ReaderStore:
    """Database supplies live metadata; its fallback text is deliberately different."""
    search_deadline_ms = 1000

    def __init__(self):
        self.events = [
            event("before-a", -2, ["earliest α", "", "not in neighbor window"]),
            event("before-b", -1, ["previous", "previous second", "hidden third"]),
            event("anchor", 0, ["outside left", "left β", "🧠" * 5000, "right\nline", "outside right"]),
            event("after", 1, ["following", "following second", "hidden third"]),
        ]
        self.anchor = self.events[2]
        self.target = receipt("anchor", 2)
        self.old_target = f"recall://{SOURCE}/old-anchor?rev=1#item=0"
        self.calls = []
        self.body_queries = []
        self.live = True

    @contextmanager
    def connect(self):
        yield self

    def _execute_bounded(self, connection, sql, values, deadline_at):
        return self.execute(sql, values)

    def execute(self, sql, values):
        normalized = " ".join(sql.split())
        self.calls.append((normalized, values))
        if "FROM receipt_redirects" in normalized:
            return Rows([{"new_receipt": self.target}] if values[1] == self.old_target else [])
        if "AS anchor_ordinal" in normalized:
            if not self.live or SOURCE not in values[1] or values[2] != self.target:
                return Rows([])
            return Rows([{**self.anchor, "anchor_ordinal": 2}])
        if "FROM canonical_events" in normalized and "GROUP BY" in normalized:
            if tuple(values[:3]) != (TENANT, SOURCE, PARENT):
                return Rows([])
            previous = "< (%s,%s)" in normalized
            selected = list(reversed(self.events[:2])) if previous else self.events[3:]
            result = []
            for row in selected[:values[-1]]:
                row = copy.deepcopy(row)
                row["chunks"] = row["chunks"][:2]
                for chunk in row["chunks"]:
                    chunk["text"] = None if "NULL AS text" in normalized or "'text',NULL" in normalized else "PG fallback " + chunk["text"]
                result.append(row)
            return Rows(result)
        if "FROM canonical_chunks" in normalized:
            if "text_redacted" in normalized:
                self.body_queries.append((normalized, values))
            document_ids = {v for v in values if isinstance(v, str) and v.startswith("doc:")}
            lists = [v for v in values if isinstance(v, (list, tuple))]
            wanted_receipts = {r for vs in lists for r in vs if isinstance(r, str) and r.startswith("recall://")}
            if not document_ids and not wanted_receipts:
                raise AssertionError(f"Unscoped body query: {normalized}")
            result = []
            for row in self.events:
                if document_ids and row["document_id"] not in document_ids:
                    continue
                chunks = row["chunks"]
                if "abs(ordinal" in normalized:
                    chunks = sorted(sorted(chunks, key=lambda c: (abs(c["ordinal"] - 2), c["ordinal"]))[:3], key=lambda c: c["ordinal"])
                for chunk in chunks:
                    if wanted_receipts and chunk["receipt"] not in wanted_receipts:
                        continue
                    text = None if "NULL AS text" in normalized or "'text',NULL" in normalized else "PG fallback " + chunk["text"]
                    item = {**chunk, "text": text}
                    if "text_redacted" in normalized and " AS text" not in normalized:
                        item["text_redacted"] = text
                        item["text_sha256"] = hashlib.sha256(text.encode()).hexdigest()
                    if "source_id" in normalized.split("FROM")[0]:
                        item.update(source_id=SOURCE, document_id=row["document_id"])
                    result.append(item)
            return Rows(result)
        raise AssertionError(f"Unexpected SQL: {normalized}")

    def archived(self):
        return {
            (SOURCE, row["document_id"]): [
                {"ordinal": c["ordinal"], "receipt": c["receipt"], "text_redacted": c["text"]}
                for c in row["chunks"]
            ]
            for row in self.events
        }


def context_event(row, chunks):
    return {
        "source_id": SOURCE,
        "native_id": row["native_id"],
        "native_parent_id": PARENT,
        "revision": 3,
        "kind": "transcript_record",
        "occurred_at": row["occurred_at"].isoformat(),
        "observed_at": row["observed_at"].isoformat(),
        "ingested_at": row["created_at"].isoformat(),
        "time_basis": "occurred_at",
        "chunks": [
            {**c, "text": c["text"][:4096], "text_clipped": len(c["text"]) > 4096}
            for c in chunks
        ],
    }


class ArchivedChunkReadTests(unittest.TestCase):
    def setUp(self):
        self.store = ReaderStore()
        self.archive = object()
        self.retrieval = BoundCanonicalRetrieval(
            self.store,
            tenant_id=TENANT,
            principal_id="principal:archived:test",
            authorized_sources=(SOURCE,),
            chunk_body_archive=self.archive,
        )

    def reader(self, **kwargs):
        return mock.patch("recall_server.chunk_bodies.read_archived_chunks", **kwargs)

    def test_show_prefers_archive_and_preserves_exact_event_chunks_and_redirect(self):
        with self.reader(return_value=self.store.archived()) as read:
            actual = self.retrieval.show(self.store.old_target)
        row = self.store.anchor
        self.assertEqual(actual, {
            "event": {
                "source_id": SOURCE,
                "native_id": "anchor",
                "revision": 3,
                "kind": "transcript_record",
                "occurred_at": row["occurred_at"].isoformat(),
                "observed_at": row["observed_at"].isoformat(),
                "canonical_redacted": row["canonical_redacted"],
            },
            "chunks": row["chunks"],
        })
        read.assert_called_once()
        self.assertFalse(self.store.body_queries)

    def test_context_preserves_neighbor_order_anchor_window_and_clipping(self):
        with self.reader(return_value=self.store.archived()):
            actual = self.retrieval.session_context(self.store.old_target, before=2, after=1)
        self.assertEqual(actual, {
            "session": {"source_id": SOURCE, "native_parent_id": PARENT, "time_basis": "occurred_at"},
            "events": [
                context_event(row, row["chunks"][1:4] if row["native_id"] == "anchor" else row["chunks"][:2])
                for row in self.store.events
            ],
            "anchor_receipt": self.store.target,
            "bounds": {"before": 2, "after": 1},
        })
        self.assertFalse(self.store.body_queries)

    def test_context_zero_bounds_returns_only_anchor(self):
        with self.reader(return_value=self.store.archived()):
            actual = self.retrieval.session_context(self.store.target, before=0, after=0)
        self.assertEqual(actual["events"], [context_event(self.store.anchor, self.store.anchor["chunks"][1:4])])

    def test_missing_archive_catalog_falls_back_to_live_postgres_body(self):
        with self.reader(return_value={}):
            actual = self.retrieval.show(self.store.target)
        expected = [{**c, "text": "PG fallback " + c["text"]} for c in self.store.anchor["chunks"]]
        self.assertEqual(actual["chunks"], expected)
        self.assertTrue(self.store.body_queries)

    def test_corrupt_archive_never_falls_back_to_postgres(self):
        with self.reader(side_effect=ArchiveCorruption("synthetic corrupt body")):
            with self.assertRaises(ArchiveCorruption):
                self.retrieval.show(self.store.target)
        self.assertFalse(self.store.body_queries)

    def test_unavailable_archive_never_falls_back_to_postgres(self):
        with self.reader(side_effect=ValueError("archived_chunk_body_unavailable")):
            with self.assertRaisesRegex(ValueError, "archived_chunk_body_unavailable"):
                self.retrieval.session_context(self.store.target, before=0, after=0)
        self.assertFalse(self.store.body_queries)

    def test_present_catalog_missing_requested_chunk_never_falls_back(self):
        archived = self.store.archived()
        archived[(SOURCE, self.store.anchor["document_id"])].pop(2)
        with self.reader(return_value=archived):
            with self.assertRaisesRegex(ValueError, "archived_chunk_body_unavailable"):
                self.retrieval.show(self.store.target)
        self.assertFalse(self.store.body_queries)

    def test_archive_receipt_mismatch_never_substitutes_another_record(self):
        archived = self.store.archived()
        archived[(SOURCE, self.store.anchor["document_id"])][2]["receipt"] = receipt("other-event", 2)
        with self.reader(return_value=archived):
            with self.assertRaisesRegex(ValueError, "archived_chunk_body_unavailable"):
                self.retrieval.show(self.store.target)
        self.assertFalse(self.store.body_queries)

    def _metadata_query(self, query_marker, rows):
        execute = self.store.execute

        def metadata_or_default(sql, values):
            if query_marker in sql:
                self.assertIn("NULL::text AS text_redacted", sql)
                return Rows(rows)
            return execute(sql, values)

        return mock.patch.object(self.store, "execute", side_effect=metadata_or_default)

    def test_related_hydrates_text_without_leaking_internal_locator_fields(self):
        row = self.store.anchor
        metadata = {
            **row, "ordinal": 2, "receipt": self.store.target,
            "text_redacted": None, "path": "/synthetic/project", "branch": "main",
        }
        with self._metadata_query(" AS path", [metadata]), self.reader(return_value=self.store.archived()):
            actual = self.retrieval.related(cwd="/synthetic/project", branch="main", limit=1)
        self.assertEqual(actual, {
            "results": [{
                "source_id": SOURCE, "native_id": "anchor", "native_parent_id": PARENT,
                "revision": 3, "occurred_at": row["occurred_at"].isoformat(),
                "observed_at": row["observed_at"].isoformat(),
                "ingested_at": row["created_at"].isoformat(), "time_basis": "occurred_at",
                "text": "🧠" * 4096, "text_clipped": True, "receipt": self.store.target,
                "rank": round(1 / 61, 8), "path": "/synthetic/project", "branch": "main",
            }],
            "diagnostics": {"engine": "canonical-v2", "fast": False},
        })
        self.assertFalse(self.store.body_queries)

    def test_time_clip_uses_only_verified_receipts_and_archive_text(self):
        wanted = receipt("anchor", 1)
        outside = receipt("before-a", 0)
        metadata = [{
            "source_id": SOURCE, "document_id": self.store.anchor["document_id"],
            "ordinal": 1, "receipt": wanted, "text_redacted": None, "occurred_at": START,
        }]
        response = {
            "results": [{
                "source_id": SOURCE, "logical_document_id": "ldoc_" + "a" * 32,
                "matching_ranges": [{
                    "text": "out of window body must disappear", "receipts": [outside, wanted],
                    "spans": [{"record_ordinal": 0}], "kind": "dense", "score": 0.8,
                }],
            }], "diagnostics": {},
        }
        with self._metadata_query("event.occurred_at>=", metadata), self.reader(return_value=self.store.archived()):
            actual = self.retrieval._clip_passage_hints_to_time_window(
                response, sources=[SOURCE], since=START.isoformat(), until=(START + timedelta(minutes=1)).isoformat(),
            )
        self.assertEqual(actual["results"], [{
            "source_id": SOURCE, "logical_document_id": "ldoc_" + "a" * 32,
            "matching_ranges": [{
                "text": "left β", "receipts": [wanted], "kind": "dense", "score": 0.8,
                "text_clipped": False, "time_clipped": True,
            }],
        }])
        self.assertEqual(actual["diagnostics"]["time_clipped_receipts"], 1)
        self.assertFalse(self.store.body_queries)

    def test_time_clip_archive_deadline_keeps_pointer_without_unverified_prose(self):
        row = self.store.anchor
        response = {"results": [{
            "source_id": SOURCE, "logical_document_id": "ldoc_" + "a" * 32,
            "matching_ranges": [{"text": "unverified", "receipts": [self.store.target]}],
        }], "diagnostics": {}}
        metadata = [{
            "source_id": SOURCE, "document_id": row["document_id"], "ordinal": 2,
            "receipt": self.store.target, "text_redacted": None, "occurred_at": START,
        }]
        with self._metadata_query("event.occurred_at>=", metadata), self.reader(side_effect=SearchDeadlineExceeded()):
            actual = self.retrieval._clip_passage_hints_to_time_window(
                response, sources=[SOURCE], since=START.isoformat(), until=None,
            )
        self.assertEqual(actual["results"], [{
            "source_id": SOURCE, "logical_document_id": "ldoc_" + "a" * 32, "matching_ranges": [],
        }])
        self.assertEqual(actual["diagnostics"]["time_clip_status"], "deadline-exceeded")
        self.assertFalse(self.store.body_queries)

    def test_nonlive_or_unauthorized_receipt_never_opens_archive(self):
        with self.reader() as read:
            self.store.live = False
            self.assertIsNone(self.retrieval.show(self.store.target))
            self.assertIsNone(self.retrieval.session_context(self.store.target))
            self.store.live = True
            self.retrieval.authorized_sources = ()
            self.assertIsNone(self.retrieval.show(self.store.target))
            self.assertIsNone(self.retrieval.session_context(self.store.target))
        read.assert_not_called()


if __name__ == "__main__":
    unittest.main()
