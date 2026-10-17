#!/usr/bin/env python3
"""Extracted typed documents remain searchable without conversational roles."""
import hashlib
import unittest

import e2e_passage_origin as fixture
from recall_server.projectors import canonical_json
from recall_server.canonical_retrieval import BoundCanonicalRetrieval
from recall_server.canonical import CanonicalLifecycleError
from recall_server.passage_projection import _typed_provider_text


class TypedDocumentVisibility(unittest.TestCase):
    setUp = fixture.PassageOrigin.setUp
    project = fixture.PassageOrigin.project
    projected = fixture.PassageOrigin.projected
    grant_and_open = fixture.PassageOrigin.grant_and_open

    def ingest_document(self, native, *, kind="document.v1", event_kind="connector_record",
                        text="Sapphire attachment decision: deployment approved.", owner=True, second=0):
        content = dict(kind=kind, document_id=native, mime_type="application/pdf",
                       name="decision.pdf", surface="slack", content_fidelity="complete", text=text)
        if owner:
            content["owner_ids"] = ["slack:T123:U123"]
        payload = canonical_json(content)
        occurred = f"2026-10-01T12:00:{second:02}Z"
        artifact = self.gateway.put_raw(tenant_id=self.tenant, source_id=self.source,
            native_id=native, payload=payload, media_type="application/json", created_at=occurred)
        envelope = dict(schema_version=1, source_id=self.source, native_id=native,
            native_parent_id=native, kind=event_kind, occurred_at=occurred, observed_at=occurred,
            principal_id=self.principal, visibility="private", content_type="application/json",
            content=content, provenance=dict(connector_id="synthetic.document", connector_schema_version=2,
                artifact_ref=artifact), content_sha256=hashlib.sha256(payload).hexdigest())
        self.plane.ingest_document(tenant_id=self.tenant, principal_id=self.principal,
            connector_id="synthetic.document", artifact_ref=artifact, envelope=envelope,
            text_redacted=payload.decode())
        return payload.decode()

    def search(self):
        return self.retrieval.search("Sapphire attachment decision", lexical_query="Sapphire attachment decision",
                                     since=None, until=None, limit=10)

    def test_roleless_document_text_is_published_searchable_and_raw_history_unchanged(self):
        self.store.search_plane = "turbopuffer"
        raw = self.ingest_document("attachment")
        self.project()
        rows = self.projected()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["text_redacted"], "Sapphire attachment decision: deployment approved.")
        opened = self.grant_and_open(rows[0])
        self.assertEqual([r["text"] for r in opened["chunks"]], [raw])
        self.writer.drain(tenant_id=self.tenant, max_months=10)
        result = self.search()
        self.assertEqual(result["diagnostics"]["authority_status"], "ok")
        self.assertEqual(result["results"][0]["matching_ranges"][0]["receipts"], rows[0]["receipts"])
        with self.store.connect() as connection:
            authors = connection.execute("SELECT count(*) AS n FROM canonical_passage_actors WHERE tenant_id=%s AND relation='author'", (self.tenant,)).fetchone()["n"]
        self.assertEqual(authors, 0)

    def test_only_supported_typed_provider_content_is_visible(self):
        self.ingest_document("owner-unknown", owner=False)
        with self.assertRaises(CanonicalLifecycleError):
            self.ingest_document("untyped", kind="generic.v1", second=1)
        with self.assertRaises(CanonicalLifecycleError):
            self.ingest_document("wrong-envelope", event_kind="transcript_record", second=2)
        self.assertFalse(_typed_provider_text("transcript_record", '{"kind":"document.v1","text":"visible"}'))
        self.assertFalse(_typed_provider_text("connector_record", '{"kind":[],"text":"visible"}'))
        self.ingest_document("empty", text="   ", second=3)
        self.project();rows = self.projected()
        self.assertEqual(len(rows), 1)
        self.assertIn("/owner-unknown?", rows[0]["receipts"][0])

    def test_forget_refuses_stale_vendor_document_and_preserves_sibling(self):
        self.store.search_plane = "turbopuffer"
        self.ingest_document("forgotten");self.ingest_document("survivor", second=1)
        self.project();rows = self.projected();self.assertEqual(len(rows), 2)
        self.grant_and_open(rows[0]);self.writer.drain(tenant_id=self.tenant, max_months=10)
        receipt = next(r for row in rows for r in row["receipts"] if "/forgotten?" in r)
        request = dict(contract="recall.forget-request.v1", schema_version=1, tenant_id=self.tenant,
            principal_id=self.principal, source_id=self.source, target_receipt=receipt,
            mode="explicit_forget", reason="owner_requested", requested_at="2026-10-01T13:00:00Z",
            idempotency_key="forget-typed-document")
        self.assertEqual(self.plane.forget(request)["status"], "deleted")
        result = self.search();self.assertEqual(len(result["results"]), 1)
        returned = [r for row in result["results"] for item in row["matching_ranges"] for r in item["receipts"]]
        self.assertNotIn(receipt, returned);self.assertTrue(any("/survivor?" in r for r in returned))

    def test_current_source_grant_revocation_still_refuses_public_reader(self):
        self.ingest_document("private");self.project();row = self.projected()[0];self.grant_and_open(row)
        with self.store.connect() as connection:
            connection.execute("DELETE FROM canonical_source_grants WHERE tenant_id=%s", (self.tenant,))
        sources = tuple(self.store.authorized_canonical_source_ids(tenant_id=self.tenant, principal_id=self.principal))
        self.assertEqual(sources, ())
        reader = BoundCanonicalRetrieval(self.store, tenant_id=self.tenant, principal_id=self.principal,
                                        authorized_sources=sources)
        self.assertIsNone(reader.show(row["receipts"][0]))


if __name__ == "__main__":
    unittest.main(verbosity=2)
