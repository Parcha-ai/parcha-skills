#!/usr/bin/env python3
"""Historical Claude envelope compatibility through original publication owners."""

import hashlib
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from e2e_legacy_tool_migration import LegacyToolMigration
from contracts.source_units import resolve_claude_tool_source_unit
from recall_server import passage_append, passage_index, passage_projection
from recall_server.logical_evidence import LogicalEvidenceError
from recall_server.canonical_retrieval import BoundCanonicalRetrieval
from recall_server.projectors import canonical_json


class ClaudeLegacyEnvelope(LegacyToolMigration):
    parent = "claude-session-" + "a" * 24
    session = "12345678-1234-1234-1234-123456789abc"

    def ingest_blocks(self, native, blocks):
        return self.ingest_envelope(
            native, {"type": "user", "message": {"role": "user", "content": blocks}}
        )

    def ingest_envelope(self, native, value):
        value = dict(value, sessionId=self.session)
        payload = canonical_json(value)
        occurred = "2026-10-13T12:00:00Z"
        artifact = self.gateway.put_raw(
            tenant_id=self.tenant,
            source_id=self.source,
            native_id=native,
            payload=payload,
            media_type="application/json",
            created_at=occurred,
        )
        envelope = dict(
            schema_version=1,
            source_id=self.source,
            native_id=native,
            native_parent_id=self.parent,
            kind="transcript_record",
            occurred_at=occurred,
            observed_at=occurred,
            principal_id=self.principal,
            visibility="private",
            content_type="application/json",
            content=value,
            provenance=dict(
                connector_id="synthetic.typed",
                connector_schema_version=1,
                artifact_ref=artifact,
            ),
            content_sha256=hashlib.sha256(payload).hexdigest(),
        )
        acknowledgement = self.plane.ingest_document(
            tenant_id=self.tenant,
            principal_id=self.principal,
            connector_id="synthetic.typed",
            artifact_ref=artifact,
            envelope=envelope,
            text_redacted=payload.decode(),
        )
        if native == "legacy-result":
            self.legacy_receipt = acknowledgement["receipt"]
        return payload.decode()

    def seed_historical_shape(self, outer_type="api-request-blob"):
        text = "Previously published café result 🙂."
        value = {
            "message": {
                "role": "user",
                "content": [{"type": "tool_result", "content": text}],
            }
        }
        if outer_type != "missing":
            value["type"] = outer_type
        raw = self.ingest_envelope("legacy-result", value)
        self.ingest_blocks(
            "standard-result",
            [{"type": "tool_result", "content": "Other original result."}],
        )
        native = passage_projection._visible_message
        with (
            patch.object(
                passage_index, "_document_extraction_version", return_value="legacy"
            ),
            patch.object(passage_append, "CHECKPOINT_VERSION", 1),
            patch.object(
                passage_projection,
                "_visible_message",
                side_effect=lambda text: native(text, include_tool_results=True),
            ),
        ):
            self.project()
        before = self.published_rows()
        self.assertIn(text, "\n".join(row["text_redacted"] for row in self.projected()))
        self.assertIn(
            raw,
            [
                chunk["text"]
                for chunk in self.grant_and_open(self.projected()[0])["chunks"]
            ],
        )
        return raw, text, before

    def test_request_blob_keeps_exact_historical_result_and_native_reader(self):
        raw, text, before = self.seed_historical_shape()
        prepared = self.append_and_prepare(native="zz-arrival")
        try:
            self.assertIsNotNone(prepared.legacy_migration)
            self.assertEqual(self.passages._commit(prepared)["status"], "committed")
        finally:
            prepared.close()
        rows = [row for row in self.projected() if row["text_redacted"] == text]
        self.assertEqual(len(rows), 1)
        opened = self.grant_and_open(rows[0])
        self.assertIn(raw, [chunk["text"] for chunk in opened["chunks"]])
        spans = rows[0]["spans"]
        self.assertEqual(len(spans), 1)
        self.assertEqual(
            resolve_claude_tool_source_unit(raw, spans[0]["source_unit"]),
            (text, "tool_result"),
        )
        self.assertEqual(rows[0]["receipts"], [self.legacy_receipt])
        self.assertIn(
            self.legacy_receipt, {r for row in before for r in row["receipts"]}
        )
        self.assertEqual(
            self.document()["applied_extraction_version"], "tool_groups_v1"
        )
        self.assertEqual(self.pending(), 0)
        self.assertIn("not verified fact or human speech", rows[0]["header_redacted"])
        self.capture_published(rows[0])

    def capture_published(self, row):
        # Use the original mounted reader on the real, verified native archive.
        from evals.candidate_capture import CandidateCapture

        sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "tests"))
        from test_candidate_capture import LocalClient

        bound = BoundCanonicalRetrieval(
            self.store,
            tenant_id=self.tenant,
            principal_id=self.principal,
            authorized_sources=(self.source,),
        )
        locator = {
            key: row[key]
            for key in (
                "source_id",
                "logical_document_id",
                "revision",
                "manifest_content_sha256",
                "passage_id",
            )
        }
        with self.store.connect() as connection:
            document = connection.execute(
                """SELECT * FROM canonical_evidence_documents
                WHERE tenant_id=%s AND source_id=%s AND logical_document_id=%s""",
                (self.tenant, self.source, row["logical_document_id"]),
            ).fetchone()
            manifest, parts = self.logical._old_references(
                connection,
                SimpleNamespace(
                    tenant_id=self.tenant,
                    source_id=self.source,
                    native_parent_id=self.parent,
                ),
            )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            mount = root / "mount"
            mount.mkdir()
            (mount / "manifest.json").write_bytes(self.archive.read_raw(manifest))
            for index, reference in enumerate(parts):
                (mount / f"part-{index:05d}.jsonl").write_bytes(
                    self.passages.logical_projection.read_part(
                        reference, tenant_id=self.tenant, source_id=self.source
                    )
                )
            before = {p.name: p.read_bytes() for p in mount.iterdir()}
            pins = {key: locator[key] for key in locator if key != "passage_id"}
            hit = dict(
                pins,
                native_parent_id=self.parent,
                first_occurred_at=document["first_occurred_at"].isoformat(),
                last_occurred_at=document["last_occurred_at"].isoformat(),
                matching_ranges=[
                    dict(
                        passage_id=row["passage_id"],
                        text=row["text_redacted"],
                        receipts=row["receipts"],
                        spans=row["spans"],
                    )
                ],
            )

            class NativeClient(LocalClient):
                def call_tool(inner, name, args, **kwargs):
                    opened = bound.show(locator)
                    assert set(row["receipts"]) <= set(opened["opened_receipts"])
                    return super().call_tool(name, args, **kwargs)

            client = NativeClient(mount)
            recorder = CandidateCapture(
                root / "capture",
                client_factory=lambda: client,
                protected_families=set(),
            )
            captured = recorder.capture_case(
                "replay", "Read exact captured text", {"results": [hit]}
            )["candidates"][0]
            self.assertTrue(captured["complete_selected_passages"], captured)
            self.assertEqual(captured["text"], row["text_redacted"])
            self.assertEqual(
                captured["source_evidence"][0]["receipts"], row["receipts"]
            )
            self.assertEqual(
                captured["source_evidence"][0]["spans"][0]["source_origin"],
                "tool_result",
            )
            self.assertEqual(before, {p.name: p.read_bytes() for p in mount.iterdir()})

    def test_conflicting_outer_type_retains_refusal_and_original_history(self):
        raw, text, before = self.seed_historical_shape("assistant")
        with self.assertRaisesRegex(
            LogicalEvidenceError, "legacy_tool_migration_required"
        ):
            self.append_and_prepare(native="zz-arrival")
        self.assertEqual(self.published_rows(), before)
        original = next(row for row in self.projected() if text in row["text_redacted"])
        with self.assertRaisesRegex(ValueError, "passage_metadata_unavailable"):
            self.grant_and_open(original)
        history = BoundCanonicalRetrieval(
            self.store,
            tenant_id=self.tenant,
            principal_id=self.principal,
            authorized_sources=(self.source,),
        ).show(self.legacy_receipt)
        self.assertIn(raw, [chunk["text"] for chunk in history["chunks"]])
        self.assertEqual(self.document()["applied_extraction_version"], "legacy")
        self.assertGreater(self.pending(), 0)


if __name__ == "__main__":
    suite = unittest.TestSuite(
        ClaudeLegacyEnvelope(name)
        for name in ClaudeLegacyEnvelope.__dict__
        if name.startswith("test_")
    )
    raise SystemExit(
        not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful()
    )
