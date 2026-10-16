#!/usr/bin/env python3
"""Native authority and grouped archive hydration precede time-filtered ranking."""

import unittest
import hashlib
from unittest.mock import patch

from e2e_singleton_search_authority import SingletonSearchTests
from recall_server.canonical_retrieval import BoundCanonicalRetrieval
from recall_server.db import SearchDeadlineExceeded
from recall_server.logical_evidence import LogicalEvidenceProjectionStore
from recall_server.passage_index import CanonicalPassageProjector
from recall_server.passage_projection import DEFAULT_PASSAGE_POLICY
from recall_server.projectors import canonical_json
from recall_server.turbopuffer_plane import TurbopufferSettings, passage_row
from recall_server.turbopuffer_retrieval import TurbopufferHintRetrieval
from tests.central_brain.fake_turbopuffer import FakeTurbopuffer
from tests.central_brain.test_time_clip_rerank import TimeClipRerankTests


class TimeClipBeforeRerank(SingletonSearchTests):
    def prepare(self):
        self.prepare_search(mixed=True)
        # The vendor is a coarse overlap index. Broaden its timestamps so both
        # genuine October 14 events require exact canonical event validation.
        for row in self.namespace.rows.values():
            row["first_occurred_at"] = "2026-10-13T00:00:00+00:00"
            row["last_occurred_at"] = "2026-10-15T00:00:00+00:00"
        self.bound = BoundCanonicalRetrieval(
            self.store, tenant_id=self.tenant, principal_id=self.principal,
            authorized_sources=(self.source,), chunk_body_archive=self.archive,
        )
        self.provider = TimeClipRerankTests.Provider()

    def filtered_search(self, *, until="2026-10-14T23:59:59Z"):
        with patch.object(self.store, "search_plane", "turbopuffer"), \
             patch.object(self.store, "turbopuffer", self.retrieval.settings), \
             patch.object(self.store, "turbopuffer_client", self.client), \
             patch.object(self.store, "rerank_runtime", self.provider), \
             patch.object(self.store, "search_deadline_ms", 20000), \
             patch.object(self.store, "verified_singleton_receipts", True):
            return self.bound.passage_hints("exact deployment phrase", filters={"until": until}, limit=5)

    def test_native_clip_reads_each_shared_part_once_and_skips_vendor_body_fetch(self):
        self.prepare()
        read, reads = self.archive.read_raw, []

        def counted(reference):
            payload = read(reference)
            reads.append((reference["object_key"], len(payload)))
            return payload

        with patch.object(self.archive, "read_raw", side_effect=counted):
            response = self.filtered_search()
        self.assertEqual(response["diagnostics"]["authority_status"], "ok")
        self.assertEqual(response["diagnostics"]["time_clip_status"], "ok")
        self.assertEqual(response["diagnostics"]["time_clipped_receipts"], 2)
        self.assertEqual(len(reads), 1, reads)
        self.assertGreater(reads[0][1], 0)
        with self.store.connect() as connection:
            part = connection.execute(
                "SELECT size_bytes FROM canonical_evidence_document_parts WHERE tenant_id=%s", (self.tenant,),
            ).fetchall()
        self.assertEqual(sum(size for _, size in reads), sum(row["size_bytes"] for row in part))
        self.assertFalse(any(query["rank_by"] == ("id", "asc") for query in self.namespace.queries))
        self.assertEqual(len(self.provider.calls), 1)
        sent = "\n".join(self.provider.calls[0][1])
        self.assertIn("Synthetic exact deployment phrase", sent)
        self.assertIn("Another exact deployment phrase", sent)
        receipts = {receipt for row in response["results"] for item in row["matching_ranges"]
                    for receipt in item["receipts"]}
        self.assertEqual(receipts, {self.receipt, self.physical_receipt})

    def test_native_clip_deadline_does_not_score_or_fetch_full_vendor_body(self):
        self.prepare()
        with patch.object(self.archive, "read_raw", side_effect=SearchDeadlineExceeded()):
            response = self.filtered_search()
        self.assertEqual(response["diagnostics"]["time_clip_status"], "deadline-exceeded")
        self.assertEqual(self.provider.calls, [])
        self.assertTrue(all(not row["matching_ranges"] for row in response["results"]))
        self.assertFalse(any(query["rank_by"] == ("id", "asc") for query in self.namespace.queries))

    def test_native_forget_during_clip_never_reaches_reranker(self):
        self.prepare()
        read, deleted = self.archive.read_raw, []

        def forget(reference):
            payload = read(reference)
            if not deleted:
                deleted.append(True)
                self.ingest("turn/search", "", tombstone=True)
            return payload

        with patch.object(self.archive, "read_raw", side_effect=forget):
            response = self.filtered_search()
        self.assertEqual(deleted, [True])
        self.assertEqual(response["diagnostics"]["time_clip_status"], "evidence-unavailable")
        self.assertEqual(self.provider.calls, [])
        self.assertTrue(all(not row["matching_ranges"] for row in response["results"]))

    def test_native_empty_event_window_is_removed_before_scoring(self):
        self.prepare()
        with patch.object(self.archive, "read_raw", wraps=self.archive.read_raw) as read:
            response = self.filtered_search(until="2026-10-13T23:59:59Z")
        self.assertEqual(response["results"], [])
        self.assertEqual(self.provider.calls, [])
        read.assert_not_called()

    def test_native_101_event_pool_keeps_formerly_working_final_five(self):
        # Six admitted parents contain 101 genuine events; any final five have
        # at most100. The old post-top-K clip works, so moving it must not turn
        # the same search into an archive count-limit failure.
        for index in range(101):
            native = f"turn/many-{index}"
            parent = f"session-{index // 20}"
            text = f"exact deployment phrase {parent} event{index}"
            content = {"text": text, "role": "user"}
            payload = canonical_json(content)
            ref = self.gateway.put_raw(tenant_id=self.tenant, source_id=self.source, native_id=native,
                payload=payload, media_type="application/json", created_at="2026-10-14T00:00:00Z")
            envelope = dict(schema_version=1, source_id=self.source, native_id=native,
                native_parent_id=parent, kind="connector_record", occurred_at="2026-10-14T00:00:00Z",
                observed_at="2026-10-14T00:00:00Z", principal_id=self.principal, visibility="private",
                content_type="application/json", content=content,
                provenance=dict(connector_id="synthetic.membership", artifact_ref=ref),
                content_sha256=hashlib.sha256(payload).hexdigest())
            self.plane.ingest_document(tenant_id=self.tenant, principal_id=self.principal,
                connector_id="synthetic.membership", artifact_ref=ref, envelope=envelope, text_redacted=text)
        self.projector.project_pending(tenant_id=self.tenant, batch_size=10, max_batches=1, upload_concurrency=1)
        CanonicalPassageProjector(self.store, LogicalEvidenceProjectionStore(self.archive),
            policy=DEFAULT_PASSAGE_POLICY, bound_tenant_id=self.tenant).project_pending(
                tenant_id=self.tenant, batch_size=10, max_batches=1)
        with self.store.connect() as connection:
            rows = connection.execute("""SELECT passage.*,evidence.native_parent_id,evidence.manifest_object_key,
                evidence.manifest_content_sha256,evidence.first_occurred_at AS doc_first_occurred_at,
                evidence.last_occurred_at AS doc_last_occurred_at,'[]'::jsonb AS actors
                FROM canonical_passages passage JOIN canonical_evidence_documents evidence
                USING(tenant_id,source_id,logical_document_id) WHERE passage.tenant_id=%s""",
                (self.tenant,)).fetchall()
        self.assertEqual(len({row["logical_document_id"] for row in rows}), 6)
        settings = TurbopufferSettings(api_key="synthetic-local-only")
        self.client = FakeTurbopuffer()
        self.namespace = self.client.namespace(settings.namespace(self.tenant))
        self.namespace.write(upsert_rows=[passage_row(row) | {
            "first_occurred_at": "2026-10-13T00:00:00+00:00",
            "last_occurred_at": "2026-10-15T00:00:00+00:00"} for row in rows])
        self.retrieval = TurbopufferHintRetrieval(self.store, tenant_id=self.tenant, sources=[self.source],
            policy_fingerprint=DEFAULT_PASSAGE_POLICY.fingerprint, settings=settings, client=self.client)
        self.bound = BoundCanonicalRetrieval(self.store, tenant_id=self.tenant, principal_id=self.principal,
                                            authorized_sources=(self.source,), chunk_body_archive=self.archive)
        self.provider = TimeClipRerankTests.Provider()
        with patch.object(self.store, "search_deadline_ms", 20000):
            old_top_five = self.retrieval.search("exact deployment phrase", lexical_query="deployment phrase",
                since=None, until="2026-10-14T23:59:59Z", limit=5)
            old_clipped = self.bound._clip_passage_hints_to_time_window(old_top_five,
                sources=[self.source], since=None, until="2026-10-14T23:59:59Z")
        self.assertEqual(old_clipped["diagnostics"]["time_clip_status"], "ok")
        self.assertEqual(len(old_clipped["results"]), 5)
        read, reads = self.archive.read_raw, []

        def counted(reference):
            payload = read(reference)
            reads.append((reference["object_key"], len(payload)))
            return payload

        self.namespace.queries.clear()
        with patch.object(self.archive, "read_raw", side_effect=counted):
            response = self.filtered_search()
        self.assertEqual(response["diagnostics"]["time_clip_status"], "ok")
        self.assertEqual(response["diagnostics"]["time_clipped_receipts"], 101)
        self.assertEqual(len(response["results"]), 5)
        self.assertTrue(all(row["matching_ranges"] for row in response["results"]))
        self.assertEqual(len(reads), 6)
        self.assertEqual(len({key for key, _ in reads}), 6)
        self.assertEqual(len(self.provider.calls), 1)
        self.assertEqual(len(self.provider.calls[0][1]), 6)
        self.assertFalse(any(query["rank_by"] == ("id", "asc") for query in self.namespace.queries))


if __name__ == "__main__":
    suite = unittest.TestSuite(TimeClipBeforeRerank(name) for name in TimeClipBeforeRerank.__dict__
                               if name.startswith("test_native_"))
    raise SystemExit(not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful())
