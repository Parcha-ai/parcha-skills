"""Parent ranking scopes the plane first and returns only current PG receipts."""
from __future__ import annotations

import copy
import sys
import time
import unittest
from contextlib import contextmanager
from pathlib import Path
from unittest import mock

SERVER = Path(__file__).resolve().parents[2] / "server"
sys.path.insert(0, str(SERVER))
from recall_server.parent_scoped_retrieval import parent_scoped_receipts  # noqa: E402
from recall_server.turbopuffer_plane import TurbopufferSettings  # noqa: E402
from tests.central_brain.fake_turbopuffer import FakeTurbopuffer  # noqa: E402

TENANT, SOURCE, PARENT = "tenant:parent:test", "source:parent:test", "session:wanted"
LDOC = "ldoc_" + "1" * 32
PASSAGE = "psg_" + "2" * 32
POLICY = "policy:active"
RECEIPT = f"recall://{SOURCE}/wanted?rev=1#item=0"


class Rows:
    def __init__(self, rows):
        self.rows = copy.deepcopy(rows)

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def fetchall(self):
        return self.rows


class Store:
    def __init__(self):
        self.turbopuffer = TurbopufferSettings(api_key="synthetic")
        self.turbopuffer_client = FakeTurbopuffer()
        self.calls = []
        self.document = {"logical_document_id": LDOC, "revision": 3, "manifest_content_sha256": "a" * 64}
        self.live = [{"passage_id": PASSAGE, "text_sha256": "b" * 64, "revision": 3,
                      "manifest_content_sha256": "a" * 64, "receipt": RECEIPT}]
        self.query_error = None

    @contextmanager
    def connect(self):
        yield self

    def _execute_bounded(self, connection, sql, values, deadline):
        self.calls.append((sql, values, deadline))
        if self.query_error:
            raise self.query_error
        if "FROM canonical_evidence_documents" in sql:
            assert values == (TENANT, SOURCE, PARENT)
            return Rows([] if self.document is None else [self.document])
        if "FROM canonical_passages passage" in sql:
            assert values[:4] == (TENANT, SOURCE, LDOC, POLICY)
            assert values[5:7] == (PARENT, PARENT)
            return Rows(self.live)
        raise AssertionError("Unexpected query")


class ParentScopedRetrievalTests(unittest.TestCase):
    def setUp(self):
        self.store = Store()
        self.ns = self.store.turbopuffer_client.namespace(self.store.turbopuffer.namespace(TENANT))
        self.row = {
            "id": PASSAGE, "source_id": SOURCE, "logical_document_id": LDOC,
            "native_parent_id": PARENT, "policy_fingerprint": POLICY,
            "revision": 3, "manifest_content_sha256": "a" * 64, "text_sha256": "b" * 64,
            "text": "migration", "first_occurred_at": "2026-09-20T00:00:00+00:00",
            "last_occurred_at": "2026-09-21T00:00:00+00:00",
        }
        self.ns.write(upsert_rows=[self.row])

    def run_lookup(self, **kwargs):
        return parent_scoped_receipts(self.store, **{
            "tenant_id": TENANT, "source_id": SOURCE, "parent_id": PARENT,
            "terms": ["migration", "lock", "deployment"], "policy_fingerprint": POLICY,
            "since": None, "until": None, "limit": 1, "deadline_at": time.monotonic() + 2,
            **kwargs,
        })

    def test_parent_is_filtered_before_ranking_and_retries_disabled(self):
        distractors = [{**self.row, "id": f"psg_{i + 100:032x}", "logical_document_id": "ldoc_" + "f" * 32,
                        "native_parent_id": "other-parent", "text": "migration lock deployment"}
                       for i in range(150)]
        self.ns.write(upsert_rows=distractors)
        self.assertEqual(self.run_lookup(), (RECEIPT,))
        request = self.ns.queries[0]
        self.assertEqual(request["rank_by"], ("text", "BM25", "migration lock deployment"))
        self.assertIn(("source_id", "In", [SOURCE]), request["filters"][1])
        self.assertIn(("logical_document_id", "Eq", LDOC), request["filters"][1])
        self.assertIn(("policy_fingerprint", "Eq", POLICY), request["filters"][1])
        self.assertNotIn("text", request["include_attributes"])
        self.assertNotIn("receipts", request["include_attributes"])
        self.assertEqual(self.store.turbopuffer_client.options, {"max_retries": 0})
        self.assertEqual(list(self.store.turbopuffer_client.namespaces), [self.store.turbopuffer.namespace(TENANT)])
        self.assertTrue(0 < request["timeout"] <= 2)
        self.assertFalse(any("search_vector" in sql or "text_redacted" in sql for sql, _, _ in self.store.calls))

    def test_time_scope_applies_to_plane_overlap_and_exact_live_event(self):
        since, until = "2026-09-20T12:00:00+00:00", "2026-09-20T18:00:00+00:00"
        self.assertEqual(self.run_lookup(since=since, until=until), (RECEIPT,))
        filters = self.ns.queries[0]["filters"][1]
        self.assertIn(("last_occurred_at", "Gte", since), filters)
        self.assertIn(("first_occurred_at", "Lte", until), filters)
        sql, values, _ = self.store.calls[-1]
        self.assertEqual(values[-5:-1], (since, since, until, until))
        self.assertIn("event.occurred_at>=%s", sql)
        self.assertIn("event.occurred_at<=%s", sql)
        for guard in ("document.is_current", "document.deleted_at IS NULL", "chunk.deleted_at IS NULL", "later.is_tombstone"):
            self.assertIn(guard, sql)

    def test_unknown_parent_never_queries_plane(self):
        self.store.document = None
        self.assertEqual(self.run_lookup(), ())
        self.assertEqual(self.ns.queries, [])

    def test_untrusted_plane_scope_revision_and_manifest_are_rejected(self):
        for key, changed in [
            ("source_id", "other-source"), ("logical_document_id", "other-document"),
            ("native_parent_id", "other-parent"), ("policy_fingerprint", "old-policy"),
            ("revision", 2), ("manifest_content_sha256", "c" * 64),
        ]:
            with self.subTest(key=key), mock.patch(
                "recall_server.parent_scoped_retrieval.TurbopufferHintRetrieval._query",
                return_value=([{**self.row, key: changed}], "ok"),
            ):
                self.assertEqual(self.run_lookup(), ())

    def test_current_pg_pins_and_liveness_can_reject_remote_hit(self):
        for key, changed in [("text_sha256", "c" * 64), ("revision", 4), ("manifest_content_sha256", "c" * 64)]:
            saved = copy.deepcopy(self.store.live)
            with self.subTest(key=key):
                self.store.live[0][key] = changed
                self.assertEqual(self.run_lookup(), ())
            self.store.live = saved
        self.store.live = []
        self.assertEqual(self.run_lookup(), ())

    def test_pg_receipts_are_authoritative_and_deduplicated(self):
        self.ns.write(upsert_rows=[{**self.row, "receipts": ["recall://forged/body"]}])
        self.store.live += [copy.deepcopy(self.store.live[0]), {**self.store.live[0], "receipt": RECEIPT + "next"}]
        self.assertEqual(self.run_lookup(limit=2), (RECEIPT, RECEIPT + "next"))

    def test_deadline_and_provider_or_database_failure_return_no_receipts(self):
        self.assertEqual(self.run_lookup(deadline_at=0), ())
        self.assertEqual(self.store.calls, [])
        self.ns.fail_queries = TimeoutError("synthetic deadline")
        self.assertEqual(self.run_lookup(), ())
        self.assertEqual(len(self.store.calls), 1)
        self.ns.fail_queries = None
        self.store.query_error = RuntimeError("synthetic private database detail")
        with self.assertLogs("recall_server.parent_scoped_retrieval", level="WARNING") as logs:
            self.assertEqual(self.run_lookup(), ())
        self.assertIn("error_type=RuntimeError", logs.output[0])
        self.assertNotIn("synthetic private database detail", logs.output[0])

    def test_bound_route_uses_turbopuffer_and_denies_other_sources_before_io(self):
        from recall_server.canonical_retrieval import BoundCanonicalRetrieval
        self.store.search_plane = "turbopuffer"
        self.store.search_deadline_ms = 1000
        bound = BoundCanonicalRetrieval(self.store, tenant_id=TENANT,
            principal_id="principal:test", authorized_sources=(SOURCE,))
        with mock.patch("recall_server.parent_scoped_retrieval.parent_scoped_receipts", return_value=(RECEIPT,)) as lookup:
            self.assertEqual(bound._parent_scoped_receipts(
                source_id=SOURCE, parent_id=PARENT, terms=["migration"], filters=None, limit=1,
            ), (RECEIPT,))
            self.assertEqual(lookup.call_args.kwargs["policy_fingerprint"], bound.passage_policy.fingerprint)
            lookup.reset_mock()
            self.assertEqual(bound._parent_scoped_receipts(
                source_id="unauthorized", parent_id=PARENT, terms=["migration"], filters=None, limit=1,
            ), ())
            lookup.assert_not_called()
        self.assertEqual(self.store.calls, [])

    def test_late_success_is_discarded_before_receipt_verification(self):
        def late_query(**kwargs):
            clock.return_value = 101.0
            return [self.row], "ok"
        with mock.patch("recall_server.parent_scoped_retrieval.time.monotonic", return_value=99.0) as clock:
            with mock.patch("recall_server.parent_scoped_retrieval.TurbopufferHintRetrieval._query", side_effect=late_query):
                self.assertEqual(self.run_lookup(deadline_at=100.0), ())
        self.assertEqual(len(self.store.calls), 1)


if __name__ == "__main__":
    unittest.main()
