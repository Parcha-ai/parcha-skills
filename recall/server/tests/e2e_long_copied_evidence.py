#!/usr/bin/env python3
"""Clipped presentation must not discard authenticated copied-message identity."""
import json
import unittest

import e2e_passage_origin as fixture
from recall_server.db import MAX_SEARCH_RESULT_TEXT_CHARS


class LongCopiedEvidence(unittest.TestCase):
    setUp = fixture.PassageOrigin.setUp
    ingest = fixture.PassageOrigin.ingest
    project = fixture.PassageOrigin.project
    projected = fixture.PassageOrigin.projected
    grant_and_open = fixture.PassageOrigin.grant_and_open
    search = fixture.PassageOrigin.search

    def publish(self, *, other_suffix="completed"):
        self.store.search_plane = "turbopuffer"
        common = "Sapphire deployment " + "deployment-observation " * 300
        self.assertGreater(len(common), MAX_SEARCH_RESULT_TEXT_CHARS)
        self.ingest("original-event", text=common + "completed")
        self.ingest("copied-event", parent="fork", text=common + other_suffix, second=1)
        self.project()
        self.writer.drain(tenant_id=self.tenant, max_months=10)
        rows = self.projected()
        self.assertEqual(len(rows), 2)
        self.grant_and_open(rows[0])
        return self.client.namespace(self.settings.namespace(self.tenant))

    def test_long_copy_folds_after_full_body_authentication_and_forget_still_wins(self):
        self.publish()
        result = self.search()
        self.assertEqual(result["diagnostics"]["authority_status"], "ok")
        self.assertEqual(result["diagnostics"]["hydrate_status"], "ok")
        self.assertEqual(result["diagnostics"]["copied_ranges_folded"], 1)
        self.assertEqual(len(result["results"]), 1)
        item = result["results"][0]["matching_ranges"][0]
        self.assertTrue(item["text_clipped"])
        self.assertEqual(len(item["text"]), MAX_SEARCH_RESULT_TEXT_CHARS)
        self.assertEqual(len(item["copied_evidence"]), 1)
        receipts = set(item["receipts"]) | set(item["copied_evidence"][0]["receipts"])
        self.assertEqual(len(receipts), 2)
        self.assertNotIn("_origin_", json.dumps(result))
        forgotten = next(r for r in receipts if "/original-event?" in r)
        request = dict(contract="recall.forget-request.v1", schema_version=1,
            tenant_id=self.tenant, principal_id=self.principal, source_id=self.source,
            target_receipt=forgotten, mode="explicit_forget", reason="owner_requested",
            requested_at="2026-10-01T13:00:00Z", idempotency_key="forget-long-copy")
        self.assertEqual(self.plane.forget(request)["status"], "deleted")
        after = self.search()
        self.assertNotIn(forgotten, json.dumps(after))
        self.assertEqual(after["diagnostics"]["copied_ranges_folded"], 0)
        self.assertEqual(len(after["results"]), 1)

    def test_equal_display_prefix_does_not_fold_different_complete_text(self):
        self.publish(other_suffix="failed")
        result = self.search()
        self.assertEqual(result["diagnostics"]["hydrate_status"], "ok")
        self.assertEqual(result["diagnostics"]["copied_ranges_folded"], 0)
        self.assertEqual(len(result["results"]), 2)
        texts = [r["matching_ranges"][0]["text"] for r in result["results"]]
        self.assertEqual(texts[0], texts[1])

    def test_vendor_suffix_corruption_cannot_mint_body_authentication(self):
        namespace = self.publish()
        row = next(iter(namespace.rows.values()))
        row["text"] = row["text"][:-1] + "!"
        row["_origin_verified_text_sha256"] = row["text_sha256"]
        result = self.search()
        self.assertEqual(result["diagnostics"]["hydrate_status"], "partial")
        self.assertEqual(result["diagnostics"]["copied_ranges_folded"], 0)
        self.assertNotIn("_origin_verified_text_sha256", json.dumps(result))


if __name__ == "__main__":
    unittest.main(verbosity=2)
