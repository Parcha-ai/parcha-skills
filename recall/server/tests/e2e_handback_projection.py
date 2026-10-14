#!/usr/bin/env python3
"""Named report projection preserves canonical source records and manifest repair."""

from __future__ import annotations

import hashlib
import unittest
from unittest.mock import patch

import e2e_retained_passage_receipts as retained_fixture
from recall_server.canonical_retrieval import BoundCanonicalRetrieval
from recall_server.passage_projection import visible_message_text
from recall_server.projectors import canonical_json


class HandbackPublication(unittest.TestCase):
    setUp = retained_fixture.RetainedPassageReceipts.setUp
    project = retained_fixture.RetainedPassageReceipts.project
    rows = retained_fixture.RetainedPassageReceipts.rows

    def ingest(self, native, *, report=None, text=None):
        content = (
            [{"type": "text", "text": text}]
            if text
            else [
                {
                    "type": "tool_use",
                    "id": "report-one",
                    "name": "SubagentHandback",
                    "input": {"message": report},
                }
            ]
        )
        value = {
            "type": "assistant",
            "message": {"role": "assistant", "content": content},
        }
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
            native_parent_id="session-one",
            kind="transcript_record",
            occurred_at=occurred,
            observed_at=occurred,
            principal_id=self.principal,
            visibility="private",
            content_type="application/json",
            content=value,
            provenance=dict(
                connector_id="synthetic.handback",
                connector_schema_version=1,
                artifact_ref=artifact,
            ),
            content_sha256=hashlib.sha256(payload).hexdigest(),
        )
        self.plane.ingest_document(
            tenant_id=self.tenant,
            principal_id=self.principal,
            connector_id="synthetic.handback",
            artifact_ref=artifact,
            envelope=envelope,
            text_redacted=payload.decode(),
        )
        return payload.decode()

    def projected(self):
        with self.store.connect() as connection:
            return connection.execute(
                """SELECT passage.*,evidence.manifest_content_sha256
                FROM canonical_passages passage JOIN canonical_evidence_documents evidence
                  USING(tenant_id,source_id,logical_document_id)
                WHERE passage.tenant_id=%s ORDER BY passage.ordinal""",
                (self.tenant,),
            ).fetchall()

    def grant_and_open(self, row):
        import uuid

        with self.store.connect() as connection:
            organization = "org:" + uuid.uuid4().hex
            connection.execute(
                "INSERT INTO brain_organizations VALUES (%s,'company','Handback test',now())",
                (organization,),
            )
            connection.execute(
                "INSERT INTO brain_spaces(tenant_id,organization_id,brain_kind,slug) VALUES (%s,%s,'company',%s) ON CONFLICT DO NOTHING",
                (self.tenant, organization, organization.replace(":", "-")),
            )
            connection.execute(
                "INSERT INTO brain_access_grants(tenant_id,principal_id,permission) VALUES (%s,%s,'read') ON CONFLICT DO NOTHING",
                (self.tenant, self.principal),
            )
            connection.execute(
                "INSERT INTO canonical_source_grants(tenant_id,principal_id,source_id,permission) VALUES (%s,%s,%s,'read') ON CONFLICT DO NOTHING",
                (self.tenant, self.principal, self.source),
            )
        target = {
            key: row[key]
            for key in (
                "source_id",
                "logical_document_id",
                "revision",
                "manifest_content_sha256",
                "passage_id",
            )
        }
        return BoundCanonicalRetrieval(
            self.store,
            tenant_id=self.tenant,
            principal_id=self.principal,
            authorized_sources=(self.source,),
        ).show(target)

    def test_same_manifest_explicit_requeue_adds_report_without_rewriting_source(self):
        before_text = self.ingest(
            "event-before", text="Earlier visible context remains unchanged."
        )
        report = "Verified café: two checks passed. Deployment remains untested."
        report_text = self.ingest("event-report", report=report)
        after_text = self.ingest(
            "event-after", text="Later visible context remains unchanged."
        )

        def old_visibility(text):
            return None if "SubagentHandback" in text else visible_message_text(text)

        with patch(
            "recall_server.passage_projection.visible_message_text",
            side_effect=old_visibility,
        ):
            self.project()
        old_rows = self.projected()
        self.assertTrue(old_rows)
        old_open = self.grant_and_open(old_rows[0])
        self.assertTrue(old_open["complete"])
        self.assertNotIn(report_text, [chunk["text"] for chunk in old_open["chunks"]])
        self.assertEqual(
            self.passages.seed_backfill(tenant_id=self.tenant),
            0,
            "new extraction code alone does not repair already-current manifests",
        )
        self.assertEqual(self.project()["documents"], 0)
        self.assertEqual(self.grant_and_open(old_rows[0])["chunks"], old_open["chunks"])
        # Test fixture explicitly admits this exact unchanged manifest to the
        # existing publisher. A production repair needs separate scoped admission.
        with self.store.connect() as connection:
            connection.execute(
                """INSERT INTO canonical_passage_projection_queue(
                tenant_id,source_id,logical_document_id,revision,generation,reason,changed_at)
                SELECT tenant_id,source_id,logical_document_id,revision,1,'backfill',clock_timestamp()
                FROM canonical_evidence_documents WHERE tenant_id=%s""",
                (self.tenant,),
            )
        self.assertEqual(self.project()["documents"], 1)
        new_rows = self.projected()
        self.assertEqual(new_rows[0]["revision"], old_rows[0]["revision"])
        self.assertEqual(
            new_rows[0]["manifest_content_sha256"],
            old_rows[0]["manifest_content_sha256"],
        )
        self.assertNotIn(
            old_rows[0]["passage_id"], {row["passage_id"] for row in new_rows}
        )
        with self.assertRaisesRegex(ValueError, "passage_metadata_unavailable"):
            self.grant_and_open(old_rows[0])
        report_rows = [
            row
            for row in new_rows
            if any("event-report" in receipt for receipt in row["receipts"])
        ]
        self.assertTrue(
            report_rows, "completed named report was omitted by publication"
        )
        self.assertTrue(any(report in row["text_redacted"] for row in report_rows))
        opened = self.grant_and_open(report_rows[0])
        self.assertTrue(opened["complete"])
        texts = {chunk["native_id"]: chunk["text"] for chunk in opened["chunks"]}
        self.assertEqual(texts["event-report"], report_text)
        self.assertIn(report_text, texts.values())
        for native, text in (
            ("event-before", before_text),
            ("event-report", report_text),
            ("event-after", after_text),
        ):
            with self.store.connect() as connection:
                row = connection.execute(
                    "SELECT content_sha256 FROM canonical_events WHERE tenant_id=%s AND native_id=%s",
                    (self.tenant, native),
                ).fetchone()
            self.assertEqual(
                row["content_sha256"], hashlib.sha256(text.encode()).hexdigest()
            )
        self.assertEqual(self.project()["documents"], 0)


if __name__ == "__main__":
    unittest.main()
