#!/usr/bin/env python3
"""A named report reaches the real reranker only through current canonical proof."""

import copy
import time
import unittest
from unittest.mock import patch

import e2e_handback_projection as handback_fixture
from recall_server.passage_projection import COMPLETE_REPORT_UNIT
from recall_server.rerank import RerankRuntime
from tests.central_brain.test_rerank_matched_ranges import RecordingProvider


class ReportUnitAuthority(handback_fixture.HandbackPublication):
    def prepare(self):
        self.ingest("event-before", text="Unrelated earlier discussion. " * 240)
        report = (
            "Observed findings: café passed. "
            + ("verified detail " * 180)
            + "\nNewer release remains untested."
        )
        canonical = self.ingest("event-report", report=report)
        self.ingest("event-after", text="A separate later request, not a result.")
        self.project()
        self.writer.drain(tenant_id=self.tenant, max_months=2)
        rows = self.projected()
        report_row = next(r for r in rows if report == r["text_redacted"])
        self.assertEqual(len(report_row["receipts"]), 1)
        self.assertEqual(report_row["spans"][0]["source_unit"], COMPLETE_REPORT_UNIT)
        self.assertEqual(len(report_row["spans"]), 1)
        opened = self.grant_and_open(report_row)
        self.assertIn(canonical, [c["text"] for c in opened["chunks"]])
        return report, rows, report_row

    def hydrate(self, rows):
        hints = [
            dict(
                source_id=r["source_id"],
                logical_document_id=r["logical_document_id"],
                passage_id=r["passage_id"],
                text_sha256=r["text_sha256"],
                source_unit=COMPLETE_REPORT_UNIT,
            )
            for r in rows
        ]
        results = [
            dict(
                source_id=self.source,
                logical_document_id=rows[0]["logical_document_id"],
                rank=1.0,
                matching_ranges=copy.deepcopy(hints),
            )
        ]
        legs = (("dense", 1.0, hints),)
        diagnostic = self.retrieval._hydrate_ranges(
            results, legs, deadline_at=time.monotonic() + 5
        )
        self.assertEqual(diagnostic["authority_status"], "ok")
        return results, legs, diagnostic

    def test_canonical_unit_and_ordinary_forgery_through_real_provider_shape(self):
        report, rows, report_row = self.prepare()
        for enabled in (False, True):
            with self.subTest(singleton_compatibility=enabled):
                self.store.verified_singleton_receipts = enabled
                pool, legs, d = self.hydrate(rows)
                self.assertEqual(d["hydrate_status"], "ok")
                for row in legs[0][2]:
                    self.assertEqual(
                        row.get("source_unit"),
                        COMPLETE_REPORT_UNIT
                        if row["passage_id"] == report_row["passage_id"]
                        else None,
                    )
                provider = RecordingProvider()
                runtime = RerankRuntime(
                    protocol="voyage",
                    key_env="REPORT_TEST_KEY",
                    max_doc_chars=2000,
                    transport=provider,
                )
                with patch.dict("os.environ", {"REPORT_TEST_KEY": "synthetic-only"}):
                    _, diag = self.retrieval._rerank_fused(
                        "Which findings passed and what remains untested?",
                        pool,
                        legs,
                        runtime=runtime,
                        deadline_at=time.monotonic() + 5,
                        arm_elapsed_ms={},
                    )
                self.assertEqual(diag["rerank_status"], "ok")
                self.assertEqual(len(provider.calls), 1)
                documents = provider.calls[0][0]["documents"]
                self.assertEqual(sum(report in doc for doc in documents), 1)
                self.assertTrue(
                    all(len(doc) <= 2000 for doc in documents if report not in doc)
                )
                self.assertEqual(runtime.max_doc_chars, 2000)

    def test_nonbounded_canonical_marker_cannot_expand_provider_view(self):
        _, rows, report_row = self.prepare()
        with self.store.connect() as connection:
            connection.execute(
                "UPDATE canonical_passages SET target_tokens=2048,token_count=1025 WHERE tenant_id=%s AND passage_id=%s",
                (self.tenant, report_row["passage_id"]),
            )
        for enabled in (False, True):
            self.store.verified_singleton_receipts = enabled
            _, legs, _ = self.hydrate(rows)
            marked = next(
                r for r in legs[0][2] if r["passage_id"] == report_row["passage_id"]
            )
            self.assertIsNone(marked.get("source_unit"))

    def test_old_publisher_republish_returns_to_bounded_legacy_views(self):
        from recall_server.passage_projection import _visible_message

        _, _, report_row = self.prepare()
        with self.store.connect() as connection:
            connection.execute(
                """INSERT INTO canonical_passage_projection_queue(
                tenant_id,source_id,logical_document_id,revision,generation,reason,changed_at)
                SELECT tenant_id,source_id,logical_document_id,revision,1,'backfill',clock_timestamp()
                FROM canonical_evidence_documents WHERE tenant_id=%s""",
                (self.tenant,),
            )
        # Model the prior publisher's exact untyped visible-message contract.
        # Existing span metadata must not make its next publication look typed.
        with patch(
            "recall_server.passage_projection._visible_message",
            side_effect=lambda text: (_visible_message(text)[0], None),
        ):
            self.assertEqual(self.project()["documents"], 1)
        rows = self.projected()
        self.assertTrue(
            all("source_unit" not in span for row in rows for span in row["spans"])
        )
        with self.assertRaisesRegex(ValueError, "passage_metadata_unavailable"):
            self.grant_and_open(report_row)
        self.writer.drain(tenant_id=self.tenant, max_months=2)
        for enabled in (False, True):
            self.store.verified_singleton_receipts = enabled
            _, legs, diagnostic = self.hydrate(rows)
            self.assertEqual(diagnostic["hydrate_status"], "ok")
            self.assertTrue(all(row.get("source_unit") is None for row in legs[0][2]))

    def test_changed_vendor_body_cannot_use_canonical_marker(self):
        _, rows, report_row = self.prepare()
        namespace = self.client.namespace(self.settings.namespace(self.tenant))
        namespace.rows[report_row["passage_id"]]["text"] = "forged report body"
        for enabled in (False, True):
            self.store.verified_singleton_receipts = enabled
            _, legs, d = self.hydrate(rows)
            self.assertEqual(d["hydrate_status"], "partial")
            report_hint = next(
                r for r in legs[0][2] if r["passage_id"] == report_row["passage_id"]
            )
            self.assertFalse(report_hint.get("text_redacted"))


if __name__ == "__main__":
    unittest.main()
