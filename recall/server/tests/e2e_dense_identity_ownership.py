#!/usr/bin/env python3
"""A refused identical-text alias must not hide another native authorized source."""
import json
import os
import time
import unittest
from unittest.mock import patch

import e2e_passage_origin as fixture
from recall_server.canonical_retrieval import BoundCanonicalRetrieval


class DenseIdentityOwnership(unittest.TestCase):
    setUp = fixture.PassageOrigin.setUp
    ingest = fixture.PassageOrigin.ingest
    project = fixture.PassageOrigin.project
    projected = fixture.PassageOrigin.projected
    grant_and_open = fixture.PassageOrigin.grant_and_open

    def publish_aliases(self):
        self.store.search_plane = "turbopuffer"
        self.ingest("first-event", parent="first")
        self.ingest("survivor-event", parent="survivor", second=1)
        self.project()
        self.writer.drain(tenant_id=self.tenant, max_months=10)
        rows = self.projected()
        self.assertEqual(len(rows), 2)
        self.first = next(r for r in rows if "/first-event?" in r["receipts"][0])
        self.survivor = next(r for r in rows if "/survivor-event?" in r["receipts"][0])
        self.assertEqual(self.first["text_sha256"], self.survivor["text_sha256"])
        self.assertNotEqual(self.first["logical_document_id"], self.survivor["logical_document_id"])
        self.grant_and_open(self.survivor)

    def dense_search(self, *, deadline_at=None):
        original_query = self.retrieval._query

        def transport(**kwargs):
            rank = kwargs["rank_by"]
            if rank[1] == "BM25":
                # A semantic match need not also be in a finite lexical pool.
                return [], "ok"
            rows, status = original_query(**kwargs)
            if rank[1] == "ANN":
                rows.sort(key=lambda r: r["id"] != self.first["passage_id"])
                for index, row in enumerate(rows):
                    row["$dist"] = .1 + index * .0001
            return rows, status

        with patch.object(self.retrieval, "_query", side_effect=transport):
            return self.retrieval.search(
                "Sapphire deployment", lexical_query="Sapphire deployment",
                since=None, until=None, limit=10,
                deadline_at=deadline_at,
            )

    def prove_survivor(self, observed):
        self.assertEqual(observed["diagnostics"]["authority_status"], "ok")
        self.assertEqual(observed["diagnostics"]["authority_rejected"], 1)
        namespace = self.client.namespace(self.settings.namespace(self.tenant))
        self.assertIn(self.first["passage_id"], namespace.rows)
        # Same native path succeeds when the asynchronous vendor deletion has
        # caught up. This control changes only the stale vendor candidate.
        namespace.rows.pop(self.first["passage_id"])
        control = self.dense_search()
        self.assertEqual(len(control["results"]), 1)
        result = control["results"][0]
        item = result["matching_ranges"][0]
        self.assertEqual(item["passage_id"], self.survivor["passage_id"])
        self.assertEqual(item["receipts"], self.survivor["receipts"])
        pointer = {key: ({**result, **item})[key] for key in (
            "source_id", "logical_document_id", "revision", "manifest_content_sha256", "passage_id")}
        opened = BoundCanonicalRetrieval(self.store, tenant_id=self.tenant,
            principal_id=self.principal, authorized_sources=(self.source,)).show(pointer)
        self.assertTrue(opened["complete"])
        self.assertEqual(opened["opened_receipts"], self.survivor["receipts"])
        print(json.dumps({"rejected_alias": 1, "before_vendor_cleanup_results": len(observed["results"]),
            "after_vendor_cleanup_results": len(control["results"]), "native_public_open_complete": True}))
        self.assertEqual(len(observed["results"]), 1,
            "early text dedupe discarded the independently authorized identical passage")
        observed_item = observed["results"][0]["matching_ranges"][0]
        self.assertEqual(observed_item["passage_id"], self.survivor["passage_id"])
        self.assertEqual(observed_item["receipts"], self.survivor["receipts"])

    def test_forgotten_first_alias_does_not_hide_current_survivor(self):
        self.publish_aliases()
        receipt = self.first["receipts"][0]
        request = dict(contract="recall.forget-request.v1", schema_version=1,
            tenant_id=self.tenant, principal_id=self.principal, source_id=self.source,
            target_receipt=receipt, mode="explicit_forget", reason="owner_requested",
            requested_at="2026-10-01T13:00:00Z", idempotency_key="forget-dense-alias")
        self.assertEqual(self.plane.forget(request)["status"], "deleted")
        observed = self.dense_search()
        self.assertNotIn(receipt, json.dumps(observed["results"]))
        self.prove_survivor(observed)

    def test_stale_first_alias_does_not_hide_current_survivor(self):
        self.publish_aliases()
        self.ingest("first-event", parent="first", text="Different replacement body.", second=2)
        self.logical.project_pending(tenant_id=self.tenant, batch_size=2,
            max_batches=1, upload_concurrency=1)
        self.prove_survivor(self.dense_search())

    def test_dense_only_authentic_copies_fold_after_own_authority(self):
        self.publish_aliases()
        observed = self.dense_search()
        self.assertEqual(observed["diagnostics"]["authority_rejected"], 0)
        self.assertEqual(observed["diagnostics"]["copied_ranges_folded"], 1)
        self.assertEqual(len(observed["results"]), 1)
        item = observed["results"][0]["matching_ranges"][0]
        receipts = set(item["receipts"]) | set(item["copied_evidence"][0]["receipts"])
        self.assertEqual(receipts, set(self.first["receipts"] + self.survivor["receipts"]))

    def test_879_copy_vendor_backlog_keeps_limits_and_refuses_unowned_aliases(self):
        self.copy_budget(current_count=2)

    def test_879_copy_pool_can_spend_full_budget_on_current_identical_copies(self):
        self.copy_budget(current_count=45)

    def copy_budget(self, *, current_count):
        from e2e_turbopuffer_mixed_ranges import RecordingProvider, RerankRuntime
        from recall_server.passage_retrieval import DENSE_NEAREST_LIMIT

        self.publish_aliases()
        for index in range(current_count - 2):
            self.ingest(f"current-copy-{index}", parent=f"current-copy-{index}", second=index+2)
        if current_count > 2:
            self.logical.project_pending(tenant_id=self.tenant, batch_size=100,
                max_batches=1, upload_concurrency=1)
            self.passages.project_pending(tenant_id=self.tenant, batch_size=100,
                max_batches=1, concurrency=2)
            self.writer.drain(tenant_id=self.tenant, max_months=10)
        self.assertEqual(len(self.projected()), current_count)
        namespace = self.client.namespace(self.settings.namespace(self.tenant))
        template = dict(namespace.rows[self.first["passage_id"]])
        # Current copies precede the asynchronous vendor-only aliases in this
        # deterministic ANN fixture. Every identity needs its own membership.
        for index in range(879 - current_count):
            key = "psg_" + "f" * 28 + f"{index:04x}"
            namespace.rows[key] = dict(template, id=key, logical_document_id=f"ldoc_{index:032x}")
        self.assertEqual(len(namespace.rows), 879)
        provider = RecordingProvider()
        self.store.rerank_runtime = RerankRuntime(protocol="voyage", key_env="RERANK_TEST_KEY",
            max_candidates=45, max_doc_chars=20000, timeout_seconds=2.5, transport=provider)
        requested = []
        original_authorize = self.retrieval._authorize_ranges

        def authorize(results, legs, **kwargs):
            requested.append((len(results), sum(len(r["matching_ranges"]) for r in results)))
            return original_authorize(results, legs, **kwargs)

        with patch.dict(os.environ, {"RERANK_TEST_KEY": "synthetic-only"}), patch.object(
            self.retrieval, "_authorize_ranges", side_effect=authorize):
            result = self.dense_search(deadline_at=time.monotonic() + 5)
        ann = [q for q in namespace.queries if q["rank_by"][1] == "ANN"]
        self.assertEqual(len(ann), 1)
        self.assertEqual(ann[0]["limit"], DENSE_NEAREST_LIMIT)
        self.assertEqual(requested, [(45, 45)])
        self.assertEqual(result["diagnostics"]["authority_rejected"], 45 - current_count)
        self.assertEqual(result["diagnostics"]["copied_ranges_folded"], current_count - 1)
        self.assertEqual(len(provider.calls), 1, result["diagnostics"])
        self.assertEqual(len(provider.calls[0][0]["documents"]), current_count)
        self.assertEqual(len(result["results"]), 1)
        item = result["results"][0]["matching_ranges"][0]
        receipts = set(item["receipts"])
        for copy in item["copied_evidence"]:
            receipts.update(copy["receipts"])
        self.assertEqual(len(receipts), current_count)
        print(json.dumps({"vendor_copies":879,"raw_ann_limit":DENSE_NEAREST_LIMIT,
            "selected_parents":45,"selected_passages":45,"refused_unowned_aliases":45-current_count,
            "authorized_provider_inputs":current_count,"final_copy_groups":1}))


if __name__ == "__main__":
    unittest.main(verbosity=2)
