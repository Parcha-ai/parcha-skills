#!/usr/bin/env python3
"""Completed legacy parents must discover new extraction without losing old receipts."""

import unittest
from unittest.mock import patch

from e2e_handback_projection import HandbackPublication
from e2e_legacy_tool_migration import LegacyToolMigration
from recall_server import passage_append, passage_index, passage_projection


class CompletedLegacyExtraction(LegacyToolMigration):
    def completed_historical_parent(self):
        self.ingest_blocks(
            "historical-tool",
            [
                {
                    "type": "tool_result",
                    "content": "Previously published deployment outcome.",
                }
            ],
        )
        HandbackPublication.ingest(
            self,
            "complete-report",
            report="The completed audit found three obsolete tests and retained the regression guard.",
        )
        native_visible = passage_projection._visible_message

        # Same predecessor visibility seam as the original handback fixture,
        # retaining the historical tool-result domain used by legacy migration.
        def old_visibility(text):
            return (
                (None, None)
                if "SubagentHandback" in text
                else native_visible(text, include_tool_results=True)
            )

        with (
            patch.object(
                passage_index, "_document_extraction_version", return_value="legacy"
            ),
            patch.object(passage_append, "CHECKPOINT_VERSION", 1),
            patch.object(
                passage_projection, "_visible_message", side_effect=old_visibility
            ),
        ):
            self.assertEqual(self.project()["documents"], 1)
        old = self.projected()
        self.assertTrue(old)
        self.assertFalse(any("completed audit" in p["text_redacted"] for p in old))
        self.assertEqual(self.pending(), 0)
        self.assertEqual(self.document()["requested_extraction_version"], "legacy")
        return old

    def test_finished_parent_is_discovered_without_new_source_arrival(self):
        old = self.completed_historical_parent()
        self.assertEqual(
            self.passages.seed_backfill(tenant_id=self.tenant),
            1,
            "finished matching legacy projection was skipped despite newly supported completion evidence",
        )
        self.assertEqual(self.project()["documents"], 1)
        current = self.projected()
        self.assertTrue(any("completed audit" in p["text_redacted"] for p in current))
        for previous in old:
            self.assertTrue(
                any(
                    previous["text_redacted"] == p["text_redacted"]
                    and previous["receipts"] == p["receipts"]
                    for p in current
                )
            )
        self.assertEqual(self.passages.seed_backfill(tenant_id=self.tenant), 0)

    def test_existing_explicit_migration_preserves_old_and_new_evidence(self):
        old = self.completed_historical_parent()
        document = self.document()
        candidate_args = dict(
            source_id=self.source,
            logical_document_id=document["logical_document_id"],
            revision=document["revision"],
            source_document_sha256=document["source_document_sha256"],
            extraction_version="tool_groups_v1",
            principal_id=self.principal,
            tenant_id=self.tenant,
        )
        self.assertEqual(
            self.passages.request_extraction_version(**candidate_args)["status"],
            "queued",
        )
        self.assertEqual(self.project()["documents"], 1)
        current = self.projected()
        self.assertTrue(any("completed audit" in p["text_redacted"] for p in current))
        for previous in old:
            self.assertTrue(
                any(
                    previous["text_redacted"] == p["text_redacted"]
                    and previous["receipts"] == p["receipts"]
                    for p in current
                )
            )
        self.writer.drain(tenant_id=self.tenant, max_months=2)
        self.assertEqual(
            set(self.client.namespace(self.settings.namespace(self.tenant)).rows),
            {p["passage_id"] for p in current},
        )
        self.assertEqual(
            self.passages.request_extraction_version(**candidate_args)["status"],
            "current",
        )

    def test_empty_legacy_epoch_is_acknowledged_once_without_fake_append_proof(self):
        self.ingest_blocks(
            "only-tool-input",
            [{"type": "tool_use", "name": "Read", "input": {"path": "/tmp/fixture"}}],
        )
        with (
            patch.object(
                passage_index, "_document_extraction_version", return_value="legacy"
            ),
            patch.object(passage_append, "make_checkpoint", return_value=None),
        ):
            self.assertEqual(self.project()["documents"], 1)
        self.assertEqual(self.document()["passage_count"], 0)
        self.assertEqual(self.passages.seed_backfill(tenant_id=self.tenant), 1)
        self.assertEqual(self.project()["documents"], 1)
        document = self.document()
        checkpoint = document["append_checkpoint"]
        self.assertEqual(document["passage_count"], 0)
        self.assertEqual(document["applied_extraction_version"], "legacy")
        self.assertEqual(checkpoint["version"], passage_append.CHECKPOINT_VERSION)
        self.assertTrue(checkpoint["full_rebuild_only"])
        self.assertEqual(
            checkpoint["source_document_sha256"], document["source_document_sha256"]
        )
        self.assertFalse(
            passage_append.proves_append(checkpoint, document, None, None, None)
        )
        self.assertEqual(self.passages.seed_backfill(tenant_id=self.tenant), 0)
        self.assertEqual(self.project()["documents"], 0)

    def test_typed_tool_only_parent_refreshes_epoch_without_changing_rows(self):
        self.ingest_blocks(
            "only-result",
            [{"type": "tool_result", "content": "Complete captured tool output."}],
        )
        with patch.object(passage_append, "make_checkpoint", return_value=None):
            self.assertEqual(self.project()["documents"], 1)
        old = self.published_rows()
        self.assertTrue(old)
        self.assertEqual(
            self.document()["applied_extraction_version"], "tool_groups_v1"
        )
        self.assertEqual(self.passages.seed_backfill(tenant_id=self.tenant), 1)
        self.assertEqual(self.project()["documents"], 1)
        self.assertEqual(self.published_rows(), old)
        checkpoint = self.document()["append_checkpoint"]
        self.assertTrue(checkpoint["full_rebuild_only"])
        self.assertEqual(
            checkpoint["tool_groups"]["extraction_version"], "tool_groups_v1"
        )
        self.assertEqual(self.passages.seed_backfill(tenant_id=self.tenant), 0)

    def test_future_epoch_discovers_current_legacy_once(self):
        self.ingest_blocks(
            "plain-message", [{"type": "text", "text": "Visible stable statement."}]
        )
        with patch.object(
            passage_index, "_document_extraction_version", return_value="legacy"
        ):
            self.assertEqual(self.project()["documents"], 1)
        old = self.published_rows()
        self.assertEqual(self.passages.seed_backfill(tenant_id=self.tenant), 0)
        with patch.object(
            passage_append, "CHECKPOINT_VERSION", passage_append.CHECKPOINT_VERSION + 1
        ):
            self.assertEqual(self.passages.seed_backfill(tenant_id=self.tenant), 1)
            self.assertEqual(self.project()["documents"], 1)
            self.assertEqual(self.published_rows(), old)
            self.assertEqual(self.passages.seed_backfill(tenant_id=self.tenant), 0)

    def test_empty_epoch_is_not_acknowledged_after_source_change(self):
        self.ingest_blocks(
            "only-tool-input",
            [{"type": "tool_use", "name": "Read", "input": {"path": "/tmp/fixture"}}],
        )
        with (
            patch.object(
                passage_index, "_document_extraction_version", return_value="legacy"
            ),
            patch.object(passage_append, "make_checkpoint", return_value=None),
        ):
            self.assertEqual(self.project()["documents"], 1)
        self.assertEqual(self.passages.seed_backfill(tenant_id=self.tenant), 1)
        prepared = self.passages._prepare(
            self.passages._pending(tenant_id=self.tenant, limit=1)[0], incremental=True
        )
        try:
            self.ingest_blocks(
                "later-input",
                [{"type": "text", "text": "Later authoritative source state."}],
            )
            self.logical.project_pending(
                tenant_id=self.tenant, batch_size=2, max_batches=1, upload_concurrency=1
            )
            self.assertEqual(self.passages._commit(prepared)["status"], "stale")
            self.assertIsNone(self.document()["append_checkpoint"])
            self.assertEqual(self.pending(), 1)
        finally:
            prepared.close()

    def test_claude_v2_before_handback_dedup_is_not_current(self):
        HandbackPublication.ingest(
            self,
            "complete-report",
            report="Completed native report with one semantic owner.",
        )
        with (
            patch.object(passage_append, "CHECKPOINT_VERSION", 2),
            patch.object(
                passage_index, "_handback_owned_by_visible_message", return_value=False
            ),
        ):
            self.assertEqual(self.project()["documents"], 1)
        before = self.projected()
        self.assertEqual(len(before), 2)
        self.assertEqual(before[0]["text_sha256"], before[1]["text_sha256"])
        self.assertEqual(
            self.document()["applied_extraction_version"], "tool_groups_v1"
        )
        self.assertEqual(self.passages.seed_backfill(tenant_id=self.tenant), 1)
        self.assertEqual(self.project()["documents"], 1)
        current = self.projected()
        self.assertEqual(len(current), 1)
        self.assertEqual(current[0]["text_sha256"], before[0]["text_sha256"])
        self.assertEqual(current[0]["receipts"], before[0]["receipts"])
        self.assertEqual(
            self.document()["append_checkpoint"]["version"],
            passage_append.CHECKPOINT_VERSION,
        )
        self.assertEqual(self.passages.seed_backfill(tenant_id=self.tenant), 0)

    def test_non_claude_v2_keeps_existing_append_proof(self):
        self.source = self.source.removeprefix("claude:")
        with patch.object(passage_append, "CHECKPOINT_VERSION", 2):
            self.seed(historical=False, mixed=True)
        self.assertEqual(self.document()["append_checkpoint"]["version"], 2)
        self.assertEqual(self.passages.seed_backfill(tenant_id=self.tenant), 0)
        prepared = self.append_and_prepare(native="zz-arrival")
        try:
            self.assertIsNotNone(prepared.previous_checkpoint)
            self.assertEqual(self.passages._commit(prepared)["status"], "committed")
        finally:
            prepared.close()

    def test_non_claude_v1_keeps_existing_compatibility(self):
        self.source = self.source.removeprefix("claude:")
        self.ingest_blocks(
            "plain-message", [{"type": "text", "text": "Visible stable statement."}]
        )
        with patch.object(passage_append, "CHECKPOINT_VERSION", 1):
            self.assertEqual(self.project()["documents"], 1)
        self.assertEqual(self.document()["append_checkpoint"]["version"], 1)
        self.assertEqual(self.passages.seed_backfill(tenant_id=self.tenant), 0)


if __name__ == "__main__":
    suite = unittest.TestSuite(
        CompletedLegacyExtraction(name)
        for name in CompletedLegacyExtraction.__dict__
        if name.startswith("test_")
    )
    raise SystemExit(
        not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful()
    )
