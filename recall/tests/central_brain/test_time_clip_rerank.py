"""Explicit event windows constrain the evidence scored, before top-K."""

import copy
import time
import unittest
from functools import partial
from unittest import mock

from recall_server.canonical_retrieval import BoundCanonicalRetrieval
from recall_server.passage_retrieval import PassageHintRetrieval
from recall_server.db import SearchDeadlineExceeded
from tests.central_brain.test_canonical_retrieval import RerankWiringTests, Rows
from tests.central_brain.test_passage_fusion import candidate


class TimeClipRerankTests(unittest.TestCase):
    class Provider:
        fingerprint = "synthetic-time-window"
        max_candidates = 30
        max_doc_chars = 20000

        def __init__(self):
            self.calls = []

        def rerank(self, query, documents, **kwargs):
            self.calls.append((query, list(documents), kwargs))
            return [(index, 1.0 if "FUTURE" in text else
                     0.8 if "ANSWER" in text else 0.1)
                    for index, text in enumerate(documents)]

    class Store(RerankWiringTests._Store):
        def __init__(self):
            super().__init__()
            self.eligible = {}
            self.clip_lookups = []

        def _execute_bounded(self, connection, sql, values, deadline_at):
            if "FROM canonical_chunks chunk" in sql:
                self.clip_lookups.append((values, deadline_at))
                return Rows([{"receipt": receipt, "text_redacted": self.eligible[receipt],
                              "occurred_at": "2026-08-01T00:00:00Z"}
                             for receipt in values[2] if receipt in self.eligible])
            return super()._execute_bounded(connection, sql, values, deadline_at)

    def setUp(self):
        self.store = self.Store()
        self.store.rerank_runtime = self.Provider()
        self.bound = BoundCanonicalRetrieval(
            self.store, tenant_id="tenant:test", principal_id="principal:test",
            authorized_sources=("source:test",),
        )
        self.mixed = candidate("mixed", "dense", 0.9)
        self.mixed.update(
            text_redacted="Old investigation.\n\nFUTURE answer from September.",
            header_redacted="FUTURE title", source_unit="message",
            passage_first_occurred_at="2026-08-01T00:00:00Z",
            passage_last_occurred_at="2026-09-01T00:00:00Z",
        )
        self.old_receipt = self.mixed["receipts"][0]
        self.future_receipt = "recall://source:test/future?rev=1#item=0"
        self.mixed["receipts"].append(self.future_receipt)
        self.inside = candidate("inside", "dense", 0.8)
        self.inside.update(text_redacted="ANSWER available in August.",
                           passage_first_occurred_at="2026-08-02T00:00:00Z",
                           passage_last_occurred_at="2026-08-02T00:00:00Z")
        self.store.eligible[self.old_receipt] = "Old investigation."

    def search(self, filters=None):
        rows = copy.deepcopy([self.mixed, self.inside])
        with mock.patch.object(PassageHintRetrieval, "_dense_candidates",
                               return_value=(rows, "ok", "synthetic", 2)), \
             mock.patch.object(PassageHintRetrieval, "_lexical_candidates", return_value=([], "ok")), \
             mock.patch.object(PassageHintRetrieval, "_sparse_candidates", return_value=([], "ok")):
            return self.bound.passage_hints("What was the deployment answer?", filters=filters, limit=1)

    def test_until_clips_before_rerank_and_top_k(self):
        unfiltered = self.search()
        self.assertEqual(unfiltered["results"][0]["logical_document_id"], self.mixed["logical_document_id"])
        self.assertEqual(self.store.clip_lookups, [])

        filtered = self.search({"until": "2026-08-31T23:59:59Z"})
        self.assertEqual(filtered["results"][0]["logical_document_id"], self.inside["logical_document_id"])
        query, texts, _ = self.store.rerank_runtime.calls[-1]
        self.assertEqual(query, "What was the deployment answer?")
        self.assertFalse(any("FUTURE" in text for text in texts), texts)
        self.assertEqual(len(self.store.clip_lookups), 1)
        self.assertEqual(set(self.store.clip_lookups[0][0][2]), {self.old_receipt, self.future_receipt})

    def test_since_uses_the_same_exact_window_without_rewriting_query(self):
        self.store.eligible = {self.future_receipt: "ANSWER from September."}
        self.inside["passage_first_occurred_at"] = "2026-09-02T00:00:00Z"
        self.inside["passage_last_occurred_at"] = "2026-09-02T00:00:00Z"
        response = self.search({"since": "2026-09-01T00:00:00Z"})
        texts = self.store.rerank_runtime.calls[-1][1]
        self.assertFalse(any("Old investigation" in text or "FUTURE" in text for text in texts))
        self.assertEqual(self.store.clip_lookups[0][0][3:7],
                         ("2026-09-01T00:00:00Z", "2026-09-01T00:00:00Z", None, None))
        self.assertEqual(response["diagnostics"]["time_clip_status"], "ok")

    def test_missing_or_deleted_straddling_evidence_never_reaches_model(self):
        self.store.eligible.clear()
        response = self.search({"until": "2026-08-31T23:59:59Z"})
        self.assertEqual(response["results"][0]["logical_document_id"], self.inside["logical_document_id"])
        self.assertEqual(len(self.store.rerank_runtime.calls[-1][1]), 1)
        self.assertNotIn("FUTURE", repr(response))

    def test_clip_timeout_preserves_inside_evidence_and_never_scores_unverified_prose(self):
        execute = self.store._execute_bounded

        def timeout(connection, sql, values, deadline):
            if "FROM canonical_chunks chunk" in sql:
                raise SearchDeadlineExceeded()
            return execute(connection, sql, values, deadline)

        with mock.patch.object(self.store, "_execute_bounded", side_effect=timeout):
            response = self.search({"until": "2026-08-31T23:59:59Z"})
        self.assertEqual(response["diagnostics"]["time_clip_status"], "deadline-exceeded")
        self.assertEqual(response["results"][0]["logical_document_id"], self.inside["logical_document_id"])
        self.assertEqual(len(self.store.rerank_runtime.calls[-1][1]), 1)
        self.assertNotIn("FUTURE", repr(response))

    def test_pool_receipts_are_deduplicated_in_one_bounded_clip(self):
        from recall_server.passage_retrieval import collapse_document_candidates

        rows = []
        for index in range(90):
            row = copy.deepcopy(self.mixed)
            parent = index // 3
            receipts = [f"recall://source:test/{parent}-{suffix}?rev=1#item=0"
                        for suffix in ("old", "future")]
            self.store.eligible[receipts[0]] = "Old investigation."
            row.update(logical_document_id=f"ldoc_{parent:032x}", passage_id=f"psg_{index:032x}",
                       receipts=receipts)
            rows.append(row)
        legs = (("dense", 1.0, rows),)
        results = collapse_document_candidates(legs, limit=30)
        deadline = time.monotonic() + 5
        retrieval = PassageHintRetrieval(
            self.store, tenant_id="tenant:test", sources=["source:test"], policy_fingerprint="fp",
            time_filter=partial(self.bound._clip_passage_hints_to_time_window,
                sources=["source:test"], since=None, until="2026-08-31T23:59:59Z", deadline_at=deadline),
        )
        retrieval._hydrate_ranges(results, legs, deadline_at=deadline)
        self.assertEqual(len(results), 30)
        self.assertEqual(len(self.store.clip_lookups), 1)
        values, actual_deadline = self.store.clip_lookups[0]
        self.assertEqual(len(values[2]), 60)
        self.assertEqual(len(set(values[2])), 60)
        self.assertEqual(actual_deadline, deadline)
        self.assertTrue(all(row["text_redacted"] == "Old investigation." for row in rows))

    def test_vendor_authority_precedes_clip_and_clipped_body_is_not_fetched_again(self):
        from recall_server.turbopuffer_plane import passage_row
        from recall_server.turbopuffer_retrieval import arm_row
        from recall_server.passage_retrieval import collapse_document_candidates
        from tests.central_brain.fake_turbopuffer import FakeTurbopuffer
        from tests.central_brain.test_turbopuffer_retrieval import ArmTests, SETTINGS, TENANT, catalog_passage

        for live, eligible_text in ((True, "Old investigation."), (True, ""), (False, "Old investigation.")):
            with self.subTest(live=live, eligible_text=eligible_text):
                client = FakeTurbopuffer()
                retrieval, store = ArmTests()._retrieval(client)
                sample = catalog_passage(1, "Old investigation. FUTURE answer.",
                                         first="2026-08-01T00:00:00Z")
                sample["last_occurred_at"] = "2026-09-01T00:00:00Z"
                vendor = passage_row(sample)
                ns = client.namespace(SETTINGS.namespace(TENANT))
                ns.write(upsert_rows=[vendor])
                execute, stages = store._execute_bounded, []

                def exact(connection, sql, values, deadline):
                    if "cardinality(passage.receipts)>0" in sql:
                        stages.append("authority")
                        return execute(connection, sql, values, deadline) if live else Rows([])
                    if "FROM canonical_chunks chunk" in sql:
                        stages.append("clip")
                        return Rows([{"receipt": sample["receipts"][0],
                                      "text_redacted": eligible_text, "occurred_at": sample["first_occurred_at"]}])
                    return execute(connection, sql, values, deadline)

                store._execute_bounded = exact
                bound = BoundCanonicalRetrieval(store, tenant_id=TENANT, principal_id="principal:test",
                                                authorized_sources=(sample["source_id"],))
                deadline = time.monotonic() + 5
                retrieval.time_filter = partial(bound._clip_passage_hints_to_time_window,
                    sources=[sample["source_id"]], since=None, until="2026-08-31T23:59:59Z", deadline_at=deadline)
                row = arm_row(vendor, 1.0)
                row["text_redacted"] = ""
                legs = (("dense", 1.0, [row]),)
                results = collapse_document_candidates(legs, limit=5)
                diagnostics = retrieval._hydrate_ranges(results, legs, deadline_at=deadline)
                self.assertEqual(stages, ["authority", "clip"] if live else ["authority"])
                self.assertEqual(ns.queries, [])
                self.assertEqual(diagnostics["hydrated_passages"], 0)
                if live:
                    self.assertEqual(legs[0][2][0]["text_redacted"], eligible_text)
                    self.assertEqual(results[0]["matching_ranges"][0]["text"], eligible_text)
                else:
                    self.assertEqual(results, [])
                    self.assertEqual(legs[0][2], [])



if __name__ == "__main__":
    unittest.main()
