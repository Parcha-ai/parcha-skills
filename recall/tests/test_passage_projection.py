from __future__ import annotations

import inspect
import unittest
from datetime import datetime, timezone
from pathlib import Path

from recall_server import SCHEMA_VERSION
from recall_server.logical_evidence import LogicalEvidenceRecord
from recall_server.logical_evidence_projection import (
    CanonicalLogicalEvidenceProjector,
)
from recall_server.passage_index import (
    CanonicalPassageProjector,
    PassageCandidate,
)
from recall_server.passage_projection import (
    MAX_PASSAGE_TOKEN_BYTES,
    PASSAGE_CONTRACT,
    PassageMessage,
    PassagePolicy,
    build_passages,
    decode_logical_record,
    reconstruct_passage,
    visible_messages,
)
from recall_server.passage_retrieval import (
    PassageHintRetrieval,
    collapse_document_candidates,
)
from recall_server.passage_worker import run_passage_worker


class PassageProjectionTests(unittest.TestCase):
    def test_schema_is_one_document_linked_passage_path(self) -> None:
        migration = (
            Path(__file__).resolve().parents[1]
            / "server"
            / "schema"
            / "041_lossless_passage_index.sql"
        )
        rendered = " ".join(migration.read_text().split()).casefold()

        self.assertEqual(SCHEMA_VERSION, 41)
        self.assertIn(
            "create table if not exists canonical_passage_documents",
            rendered,
        )
        self.assertIn("create table if not exists canonical_passages", rendered)
        self.assertIn(
            "references canonical_passage_documents( "
            "tenant_id,source_id,logical_document_id,revision, "
            "policy_fingerprint ) "
            "on delete cascade",
            rendered,
        )
        self.assertIn("spans jsonb not null", rendered)
        self.assertIn("receipts text[] not null", rendered)
        self.assertIn("search_vector tsvector generated always", rendered)
        self.assertIn(
            "create table if not exists canonical_passage_embeddings",
            rendered,
        )
        self.assertIn(
            "create table if not exists canonical_passage_projection_queue",
            rendered,
        )
        self.assertNotIn("summary", rendered)
        self.assertNotIn("synthetic_question", rendered)

    def test_builds_lossless_overlapping_passages_without_crossing_documents(
        self,
    ) -> None:
        messages = (
            PassageMessage(
                record_ordinal=3,
                occurred_at="2026-07-27T00:00:00Z",
                roles=("user",),
                receipts=("recall://source:test/user?rev=1#item=0",),
                text="alpha beta gamma delta",
            ),
            PassageMessage(
                record_ordinal=4,
                occurred_at="2026-07-27T00:00:01Z",
                roles=("assistant",),
                receipts=("recall://source:test/assistant?rev=1#item=0",),
                text="epsilon zeta eta theta",
            ),
        )
        passages = build_passages(
            tenant_id="tenant:company:test",
            source_id="source:test",
            logical_document_id="ldoc_0123456789abcdef0123456789abcdef",
            revision=7,
            messages=messages,
            policy=PassagePolicy(target_tokens=5, overlap_tokens=2),
        )

        self.assertGreater(len(passages), 1)
        self.assertTrue(all(passage.revision == 7 for passage in passages))
        self.assertTrue(all(passage.spans for passage in passages))
        self.assertEqual(
            [passage.ordinal for passage in passages],
            list(range(len(passages))),
        )
        for passage in passages:
            self.assertEqual(reconstruct_passage(passage, messages), passage.text)
            self.assertEqual(
                passage.receipts,
                tuple(dict.fromkeys(
                    receipt
                    for span in passage.spans
                    for receipt in messages[
                        span.message_index
                    ].receipts
                )),
            )

        covered: dict[int, set[int]] = {0: set(), 1: set()}
        for passage in passages:
            for span in passage.spans:
                covered[span.message_index].update(
                    range(span.source_byte_start, span.source_byte_end)
                )
        for index, message in enumerate(messages):
            self.assertEqual(covered[index], set(range(len(message.text.encode()))))

    def test_passage_time_bounds_handle_source_order_clock_regression(
        self,
    ) -> None:
        messages = (
            PassageMessage(
                record_ordinal=0,
                occurred_at="2026-07-27T00:00:01Z",
                roles=("user",),
                receipts=("recall://source:test/user?rev=1#item=0",),
                text="first source record",
            ),
            PassageMessage(
                record_ordinal=1,
                occurred_at="2026-07-27T00:00:00Z",
                roles=("assistant",),
                receipts=("recall://source:test/assistant?rev=1#item=0",),
                text="second source record",
            ),
        )

        passage = build_passages(
            tenant_id="tenant:company:test",
            source_id="source:test",
            logical_document_id="ldoc_0123456789abcdef0123456789abcdef",
            revision=1,
            messages=messages,
            policy=PassagePolicy(target_tokens=8, overlap_tokens=1),
        )[0]

        self.assertEqual(
            passage.first_occurred_at,
            "2026-07-27T00:00:00Z",
        )
        self.assertEqual(
            passage.last_occurred_at,
            "2026-07-27T00:00:01Z",
        )

    def test_long_unicode_message_splits_on_utf8_boundaries_and_reconstructs(
        self,
    ) -> None:
        message = PassageMessage(
            record_ordinal=9,
            occurred_at="2026-07-27T00:00:00Z",
            roles=("assistant",),
            receipts=("recall://source:test/long?rev=1#item=0",),
            text=("muñequitos 🧠 " * 30).strip(),
        )

        passages = build_passages(
            tenant_id="tenant:company:test",
            source_id="source:test",
            logical_document_id="ldoc_fedcba9876543210fedcba9876543210",
            revision=1,
            messages=(message,),
            policy=PassagePolicy(target_tokens=8, overlap_tokens=3),
        )

        self.assertGreater(len(passages), 1)
        for passage in passages:
            self.assertEqual(reconstruct_passage(passage, (message,)), passage.text)
            passage.text.encode().decode()
            for span in passage.spans:
                encoded = message.text.encode()
                encoded[span.source_byte_start:span.source_byte_end].decode()

    def test_minified_payload_is_split_into_bounded_utf8_tokens(self) -> None:
        class CountingText(str):
            encode_calls = 0

            def encode(self, *args, **kwargs):
                type(self).encode_calls += 1
                return super().encode(*args, **kwargs)

        message = PassageMessage(
            record_ordinal=0,
            occurred_at="2026-07-27T00:00:00Z",
            roles=("assistant",),
            receipts=("recall://source:test/minified?rev=1#item=0",),
            text=CountingText("ñ" * 600_000),
        )
        policy = PassagePolicy(target_tokens=1024, overlap_tokens=128)

        passages = build_passages(
            tenant_id="tenant:company:test",
            source_id="source:test",
            logical_document_id="ldoc_0123456789abcdef0123456789abcdef",
            revision=1,
            messages=(message,),
            policy=policy,
        )

        self.assertEqual(PASSAGE_CONTRACT.rsplit(".", 1)[-1], "v2")
        self.assertGreater(len(passages), 1)
        self.assertTrue(all(
            len(passage.text.encode())
            <= policy.target_tokens * MAX_PASSAGE_TOKEN_BYTES
            for passage in passages
        ))
        covered = {
            byte
            for passage in passages
            for span in passage.spans
            for byte in range(
                span.source_byte_start,
                span.source_byte_end,
            )
        }
        self.assertEqual(covered, set(range(len(message.text.encode()))))
        self.assertEqual(CountingText.encode_calls, 2)

    def test_decodes_and_combines_segmented_visible_messages_only(self) -> None:
        records = (
            LogicalEvidenceRecord(
                ordinal=0,
                event_native_id="native:long",
                event_kind="message",
                occurred_at="2026-07-27T00:00:00Z",
                roles=("assistant",),
                receipts=("recall://source:test/long?rev=1#item=0",),
                segment_ordinal=0,
                segment_count=2,
                text="first ",
            ),
            LogicalEvidenceRecord(
                ordinal=1,
                event_native_id="native:long",
                event_kind="message",
                occurred_at="2026-07-27T00:00:00Z",
                roles=("assistant",),
                receipts=(),
                segment_ordinal=1,
                segment_count=2,
                text="second",
            ),
            LogicalEvidenceRecord(
                ordinal=2,
                event_native_id="native:tool",
                event_kind="tool",
                occurred_at="2026-07-27T00:00:01Z",
                roles=("tool",),
                receipts=("recall://source:test/tool?rev=1#item=0",),
                segment_ordinal=0,
                segment_count=1,
                text="sparse only",
            ),
        )
        decoded = tuple(
            decode_logical_record(
                record.encode(source_id="source:test"),
                source_id="source:test",
            )
            for record in records
        )

        self.assertEqual(decoded, records)
        self.assertEqual(
            visible_messages(decoded),
            (
                PassageMessage(
                    record_ordinal=0,
                    record_count=2,
                    occurred_at="2026-07-27T00:00:00Z",
                    roles=("assistant",),
                    receipts=("recall://source:test/long?rev=1#item=0",),
                    text="first second",
                ),
            ),
        )

    def test_projector_reads_complete_logical_parts_and_excludes_tool_dense_text(
        self,
    ) -> None:
        records = (
            LogicalEvidenceRecord(
                ordinal=0,
                event_native_id="native:user",
                event_kind="message",
                occurred_at="2026-07-27T00:00:00Z",
                roles=("user",),
                receipts=("recall://source:test/user?rev=1#item=0",),
                segment_ordinal=0,
                segment_count=1,
                text="why did the gateway change?",
            ),
            LogicalEvidenceRecord(
                ordinal=1,
                event_native_id="native:tool",
                event_kind="tool",
                occurred_at="2026-07-27T00:00:01Z",
                roles=("tool",),
                receipts=("recall://source:test/tool?rev=1#item=0",),
                segment_ordinal=0,
                segment_count=1,
                text="large tool result remains sparse",
            ),
            LogicalEvidenceRecord(
                ordinal=2,
                event_native_id="native:assistant",
                event_kind="message",
                occurred_at="2026-07-27T00:00:02Z",
                roles=("assistant",),
                receipts=("recall://source:test/assistant?rev=1#item=0",),
                segment_ordinal=0,
                segment_count=1,
                text="we changed it to preserve tenant boundaries",
            ),
        )
        payload = b"".join(
            record.encode(source_id="source:test")
            for record in records
        )
        part = {
            "contract": "recall.artifact-ref.v1",
            "schema_version": 1,
            "tenant_id": "tenant:company:test",
            "source_id": "source:test",
            "artifact_id": "art_0123456789abcdef0123456789abcdef",
            "storage_backend": "s3",
            "object_key": "objects/01/" + "a" * 64,
            "content_sha256": "b" * 64,
            "size_bytes": len(payload),
            "media_type": (
                "application/vnd.recall.logical-document-part+jsonl"
            ),
            "encryption": "sse-s3",
            "version_id": "version-1",
            "created_at": "2026-07-27T00:00:00Z",
        }
        manifest_reference = {
            **part,
            "artifact_id": "art_fedcba9876543210fedcba9876543210",
            "object_key": "objects/fe/" + "c" * 64,
            "content_sha256": "d" * 64,
            "media_type": (
                "application/vnd.recall.logical-document-manifest+json"
            ),
        }

        class StreamingPayload(bytes):
            def splitlines(self, *args, **kwargs):
                raise AssertionError("projection must not materialize all lines")

        class Projection:
            @staticmethod
            def read_manifest(_reference, *, tenant_id, source_id):
                self.assertEqual(tenant_id, "tenant:company:test")
                self.assertEqual(source_id, "source:test")
                return {
                    "logical_document_id": (
                        "ldoc_0123456789abcdef0123456789abcdef"
                    ),
                    "revision": 4,
                    "document_content_sha256": "e" * 64,
                    "parts": [
                        {
                            "ordinal": 0,
                            **{
                                field: part[field]
                                for field in (
                                    "artifact_id",
                                    "object_key",
                                    "content_sha256",
                                    "size_bytes",
                                    "media_type",
                                    "version_id",
                                )
                            },
                        }
                    ],
                }

            @staticmethod
            def read_part(_reference, *, tenant_id, source_id):
                self.assertEqual(tenant_id, "tenant:company:test")
                self.assertEqual(source_id, "source:test")
                return StreamingPayload(payload)

        projector = CanonicalPassageProjector(
            object(),
            Projection(),  # type: ignore[arg-type]
            policy=PassagePolicy(target_tokens=8, overlap_tokens=2),
        )
        prepared = projector._prepare(
            PassageCandidate(
                tenant_id="tenant:company:test",
                source_id="source:test",
                logical_document_id=(
                    "ldoc_0123456789abcdef0123456789abcdef"
                ),
                revision=4,
                generation=1,
                changed_at=datetime.now(timezone.utc),
                source_document_sha256="e" * 64,
                manifest_reference=manifest_reference,
                part_references=(part,),
            )
        )

        self.assertEqual(prepared.dense_message_count, 2)
        self.assertNotIn(
            "large tool result",
            " ".join(passage.text for passage in prepared.passages),
        )
        self.assertTrue(all(
            passage.logical_document_id
            == "ldoc_0123456789abcdef0123456789abcdef"
            for passage in prepared.passages
        ))

    def test_logical_commit_enqueues_only_the_replaced_document(self) -> None:
        source = inspect.getsource(CanonicalLogicalEvidenceProjector._commit)

        self.assertIn(
            "INSERT INTO canonical_passage_projection_queue",
            source,
        )
        self.assertIn("'logical-update'", source)
        self.assertNotIn("canonical_passage_embeddings", source)

    def test_embedding_path_uses_passages_and_never_a_completion_model(
        self,
    ) -> None:
        source = inspect.getsource(CanonicalPassageProjector.embed_pending)

        self.assertIn("FROM canonical_passages passage", source)
        self.assertIn("canonical_passage_embeddings", source)
        self.assertIn("runtime.embed_passages(", source)
        self.assertNotIn("chat", source.casefold())
        self.assertNotIn("completion", source.casefold())
        self.assertNotIn("canonical_chunk_embeddings", source)

    def test_projection_warms_only_bounded_database_headroom(self) -> None:
        source = inspect.getsource(CanonicalPassageProjector.project_pending)

        self.assertIn(
            "prepare_pool(min(PASSAGE_POOL_WARM_SIZE, concurrency))",
            source,
        )
        self.assertNotIn("prepare_pool(concurrency)", source)
        self.assertIn("self.store.pool_max_size - 1", source)
        self.assertIn("PASSAGE_COMMIT_WORKERS", source)

    def test_dense_temporal_candidates_are_oversampled_before_filtering(
        self,
    ) -> None:
        source = inspect.getsource(PassageHintRetrieval.search)
        nearest = source.split("WITH nearest AS MATERIALIZED", 1)[1]
        nearest = nearest.split("LIMIT %s", 1)[0]

        self.assertIn(
            "dense_oversample = 50 if temporal_scope else 5",
            source,
        )
        self.assertNotIn("canonical_passages", nearest)
        self.assertIn("passage.last_occurred_at>=%s", source)
        self.assertIn("passage.first_occurred_at<=%s", source)

    def test_backfill_is_idempotent_and_avoids_large_head_of_line_blocking(
        self,
    ) -> None:
        seed = inspect.getsource(CanonicalPassageProjector.seed_backfill)
        pending = inspect.getsource(CanonicalPassageProjector._pending)

        self.assertIn("ON CONFLICT(tenant_id,source_id,logical_document_id)", seed)
        self.assertIn("DO NOTHING", seed)
        self.assertNotIn("generation+1", seed)
        self.assertIn("sum(size_part.size_bytes)", pending)

    def test_worker_projects_then_embeds_in_one_bounded_cycle(self) -> None:
        class Projector:
            def __init__(self) -> None:
                self.calls = []

            def project_pending(self, **values):
                self.calls.append(("project", values))
                return {
                    "status": "complete",
                    "documents": 3,
                    "passages": 9,
                    "stale": 0,
                }

            def embed_pending(self, **values):
                self.calls.append(("embed", values))
                return {
                    "status": "complete",
                    "processed": 9,
                    "batches": 1,
                }

        projector = Projector()
        result = run_passage_worker(
            projector,  # type: ignore[arg-type]
            tenant_id="tenant:company:test",
            projection_batch_size=100,
            embedding_batch_size=500,
            max_batches_per_cycle=4,
            concurrency=8,
            interval_seconds=5,
            once=True,
        )

        self.assertEqual(
            result,
            {
                "status": "complete",
                "documents": 3,
                "passages": 9,
                "embedded": 9,
                "stale": 0,
            },
        )
        self.assertEqual(
            [name for name, _values in projector.calls],
            ["project", "embed"],
        )

    def test_hybrid_candidates_collapse_to_documents_and_keep_best_ranges(
        self,
    ) -> None:
        base = {
            "source_id": "source:test",
            "logical_document_id": (
                "ldoc_0123456789abcdef0123456789abcdef"
            ),
            "revision": 2,
            "native_parent_id": "session:test",
            "first_occurred_at": "2026-07-27T00:00:00Z",
            "last_occurred_at": "2026-07-27T00:10:00Z",
            "manifest_object_key": "objects/01/" + "a" * 64,
            "manifest_content_sha256": "b" * 64,
        }
        dense = {
            **base,
            "passage_id": "psg_" + "1" * 32,
            "passage_ordinal": 3,
            "spans": [{"record_ordinal": 7}],
            "receipts": ["recall://source:test/a?rev=1#item=0"],
            "text_redacted": "gateway tenant boundary",
            "score": 0.91,
        }
        lexical = {
            **base,
            "passage_id": "psg_" + "2" * 32,
            "passage_ordinal": 5,
            "spans": [{"record_ordinal": 11}],
            "receipts": ["recall://source:test/b?rev=1#item=0"],
            "text_redacted": "explicit source grant",
            "score": 0.72,
        }
        sparse = {
            **base,
            "receipt": "recall://source:test/tool?rev=1#item=0",
            "text_redacted": "rare exact identifier",
            "score": 0.65,
        }

        results = collapse_document_candidates(
            (
                ("dense", 0.55, [dense]),
                ("passage-lexical", 0.30, [lexical]),
                ("sparse-exact", 0.15, [sparse]),
            ),
            limit=20,
        )

        self.assertEqual(len(results), 1)
        self.assertEqual(
            results[0]["reasons"],
            ["dense", "passage-lexical", "sparse-exact"],
        )
        self.assertEqual(len(results[0]["matching_ranges"]), 3)
        self.assertNotIn("_score", results[0])
        self.assertNotIn("_ranges", results[0])

    def test_rejects_hidden_roles_and_invalid_policy(self) -> None:
        hidden = PassageMessage(
            record_ordinal=0,
            occurred_at="2026-07-27T00:00:00Z",
            roles=("thinking",),
            receipts=("recall://source:test/hidden?rev=1#item=0",),
            text="not eligible",
        )

        with self.assertRaisesRegex(ValueError, "visible user/assistant"):
            build_passages(
                tenant_id="tenant:company:test",
                source_id="source:test",
                logical_document_id="ldoc_0123456789abcdef0123456789abcdef",
                revision=1,
                messages=(hidden,),
                policy=PassagePolicy(target_tokens=8, overlap_tokens=2),
            )
        with self.assertRaisesRegex(ValueError, "overlap"):
            PassagePolicy(target_tokens=8, overlap_tokens=8)

    def test_identity_is_stable_and_policy_versioned(self) -> None:
        messages = (
            PassageMessage(
                record_ordinal=0,
                occurred_at="2026-07-27T00:00:00Z",
                roles=("user",),
                receipts=("recall://source:test/one?rev=1#item=0",),
                text="what changed in the gateway?",
            ),
        )
        first = build_passages(
            tenant_id="tenant:company:test",
            source_id="source:test",
            logical_document_id="ldoc_0123456789abcdef0123456789abcdef",
            revision=1,
            messages=messages,
            policy=PassagePolicy(target_tokens=8, overlap_tokens=2),
        )
        second = build_passages(
            tenant_id="tenant:company:test",
            source_id="source:test",
            logical_document_id="ldoc_0123456789abcdef0123456789abcdef",
            revision=1,
            messages=messages,
            policy=PassagePolicy(target_tokens=8, overlap_tokens=2),
        )
        changed = build_passages(
            tenant_id="tenant:company:test",
            source_id="source:test",
            logical_document_id="ldoc_0123456789abcdef0123456789abcdef",
            revision=1,
            messages=messages,
            policy=PassagePolicy(target_tokens=16, overlap_tokens=2),
        )

        self.assertEqual(first, second)
        self.assertNotEqual(first[0].passage_id, changed[0].passage_id)


if __name__ == "__main__":
    unittest.main()
