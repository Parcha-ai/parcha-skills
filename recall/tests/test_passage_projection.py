from __future__ import annotations

import inspect
import threading
import time
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import orjson

from recall_server import SCHEMA_VERSION
from recall_server.actor_attribution import ActorLink
from recall_server.canonical_retrieval import (
    BoundCanonicalRetrieval,
    CanonicalRetrieval,
)
from recall_server.managed_worker import run_managed_worker
from recall_server.logical_evidence import LogicalEvidenceRecord
from recall_server.logical_evidence_projection import (
    CanonicalLogicalEvidenceProjector,
    _explicit_roles,
)
from recall_server.passage_index import (
    CanonicalPassageProjector,
    PassageCandidate,
)
from recall_server.passage_projection import (
    DEFAULT_PASSAGE_POLICY,
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
    fuse_document_rankings,
)
from recall_server.passage_worker import run_passage_worker


class PassageProjectionTests(unittest.TestCase):
    def test_projection_and_retrieval_share_one_authoritative_policy(self) -> None:
        canonical = CanonicalRetrieval(object())  # type: ignore[arg-type]
        bound = BoundCanonicalRetrieval(
            object(),  # type: ignore[arg-type]
            tenant_id="tenant:company:test",
            principal_id="principal:owner",
            authorized_sources=("codex:test",),
        )

        self.assertIs(canonical.passage_policy, DEFAULT_PASSAGE_POLICY)
        self.assertIs(bound.passage_policy, DEFAULT_PASSAGE_POLICY)
        self.assertEqual(DEFAULT_PASSAGE_POLICY.target_tokens, 1024)
        self.assertEqual(DEFAULT_PASSAGE_POLICY.overlap_tokens, 128)
        self.assertIn(
            "policy=DEFAULT_PASSAGE_POLICY",
            inspect.getsource(run_managed_worker),
        )

    def test_codex_message_types_normalize_to_visible_roles(self) -> None:
        self.assertEqual(_explicit_roles(["agent_message"]), ("assistant",))
        self.assertEqual(_explicit_roles(["user_message"]), ("user",))
        self.assertEqual(
            _explicit_roles(["agent_message", "tool"]),
            ("assistant", "tool"),
        )

    def test_schema_is_one_document_linked_passage_path(self) -> None:
        migration = (
            Path(__file__).resolve().parents[1]
            / "server"
            / "schema"
            / "041_lossless_passage_index.sql"
        )
        rendered = " ".join(migration.read_text().split()).casefold()

        self.assertEqual(SCHEMA_VERSION, 52)
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

        representations = (
            Path(__file__).resolve().parents[1]
            / "server"
            / "schema"
            / "042_passage_retrieval_representations.sql"
        )
        represented = " ".join(
            representations.read_text().split()
        ).casefold()
        self.assertIn(
            "create table if not exists canonical_passage_contexts",
            represented,
        )
        self.assertIn(
            "create table if not exists "
            "canonical_passage_embedding_representations",
            represented,
        )
        self.assertIn(
            "primary key( tenant_id, source_id, passage_id, "
            "representation_fingerprint )",
            represented,
        )
        self.assertIn(
            "references canonical_passages(tenant_id, source_id, passage_id) "
            "on delete cascade",
            represented,
        )
        self.assertNotIn("summary", represented)

        native = (
            Path(__file__).resolve().parents[1]
            / "server"
            / "schema"
            / "043_native_passage_representations.sql"
        )
        native_sql = " ".join(native.read_text().split()).casefold()
        self.assertIn("embedding_1536 halfvec(1536)", native_sql)
        self.assertIn("embedding_3072 halfvec(3072)", native_sql)
        self.assertIn(
            "check (dimensions in (512, 1536, 3072))",
            native_sql,
        )
        self.assertIn(
            "drop constraint if exists "
            "canonical_passage_repr_dimensions_check",
            native_sql,
        )
        self.assertIn(
            "drop constraint if exists "
            "canonical_passage_repr_vector_dimensions_check",
            native_sql,
        )

        attribution = (
            Path(__file__).resolve().parents[1]
            / "server"
            / "schema"
            / "045_actor_attribution.sql"
        )
        actor_sql = " ".join(attribution.read_text().split()).casefold()
        for table in (
            "brain_actors",
            "brain_actor_external_identities",
            "canonical_source_actor_bindings",
            "canonical_event_actors",
            "canonical_evidence_document_actors",
            "canonical_passage_actors",
        ):
            self.assertIn(f"create table if not exists {table}", actor_sql)
        self.assertIn(
            "a principal answers \"who may access this brain?\"",
            actor_sql,
        )
        self.assertIn("subject_hmac_sha256 char(64) not null", actor_sql)
        passage_actor_table = actor_sql.split(
            "create table if not exists canonical_passage_actors",
            1,
        )[1].split("create index", 1)[0]
        self.assertNotIn(
            "references brain_actors(tenant_id, actor_id) on delete cascade",
            passage_actor_table,
        )

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

        self.assertEqual(
            PASSAGE_CONTRACT,
            "recall.lossless-message-passage.v4:actor-aware",
        )
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
        contributor = ActorLink(
            "actor_0123456789abcdef0123456789abcdef",
            "contributor",
        )
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
                actor_links=(contributor,),
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
                actor_links=(contributor,),
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
                    actor_links=(contributor,),
                ),
            ),
        )

    def test_visible_messages_strip_harness_envelopes_without_summarizing(self):
        records = (
            LogicalEvidenceRecord(
                ordinal=0,
                event_native_id="native:claude",
                event_kind="assistant",
                occurred_at="2026-07-27T00:00:00Z",
                roles=("assistant",),
                receipts=("recall://source:test/claude?rev=1#item=0",),
                segment_ordinal=0,
                segment_count=1,
                text=orjson.dumps({
                    "cwd": "/private/noise",
                    "message": {
                        "content": [
                            {"type": "text", "text": "exact visible answer"},
                            {
                                "type": "tool_use",
                                "name": "Read",
                                "input": {"path": "/private/noise"},
                            },
                        ],
                        "model": "noise-model",
                        "role": "assistant",
                    },
                    "sessionId": "noise-session",
                }).decode(),
            ),
            LogicalEvidenceRecord(
                ordinal=1,
                event_native_id="native:codex",
                event_kind="event_msg",
                occurred_at="2026-07-27T00:00:01Z",
                roles=("user",),
                receipts=("recall://source:test/codex?rev=1#item=0",),
                segment_ordinal=0,
                segment_count=1,
                text=orjson.dumps({
                    "payload": {
                        "message": "exact user request",
                        "phase": "commentary",
                        "type": "user_message",
                    },
                    "timestamp": "noise-timestamp",
                    "type": "event_msg",
                }).decode(),
            ),
            LogicalEvidenceRecord(
                ordinal=2,
                event_native_id="native:tool-only",
                event_kind="assistant",
                occurred_at="2026-07-27T00:00:02Z",
                roles=("assistant",),
                receipts=("recall://source:test/tool-only?rev=1#item=0",),
                segment_ordinal=0,
                segment_count=1,
                text=orjson.dumps({
                    "message": {
                        "content": [{
                            "type": "tool_use",
                            "name": "Read",
                            "input": {"path": "/private/noise"},
                        }],
                        "role": "assistant",
                    },
                }).decode(),
            ),
        )

        messages = visible_messages(records)

        self.assertEqual(
            [message.text for message in messages],
            ["exact visible answer", "exact user request"],
        )
        self.assertNotIn("noise", "\n".join(
            message.text for message in messages
        ))

    def test_trusted_archive_decode_validates_without_reserializing(self) -> None:
        record = LogicalEvidenceRecord(
            ordinal=0,
            event_native_id="native:trusted",
            event_kind="message",
            occurred_at="2026-07-27T00:00:00Z",
            roles=("user",),
            receipts=("recall://source:test/trusted?rev=1#item=0",),
            segment_ordinal=0,
            segment_count=1,
            text="trusted archive content",
        )
        encoded = record.encode(source_id="source:test")

        with mock.patch.object(
            LogicalEvidenceRecord,
            "encode",
            side_effect=AssertionError("unexpected reserialization"),
        ) as serializer:
            decoded = decode_logical_record(
                encoded,
                source_id="source:test",
                verify_canonical=False,
            )

        self.assertEqual(decoded, record)
        serializer.assert_not_called()

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

    def test_revision_projection_reuses_unchanged_content_embeddings(
        self,
    ) -> None:
        source = inspect.getsource(CanonicalPassageProjector._commit)

        reusable = source.index(
            "recall_reusable_passage_embeddings"
        )
        delete = source.index(
            "DELETE FROM canonical_passage_documents"
        )
        restore = source.rindex(
            "INSERT INTO canonical_passage_embeddings"
        )
        self.assertLess(reusable, delete)
        self.assertLess(delete, restore)
        self.assertIn("cached.content_sha256=", source)
        self.assertIn("passage.text_sha256", source)
        self.assertIn("ON COMMIT DROP", source)

    def test_projection_bulk_loads_passages_in_one_copy_stream(self) -> None:
        source = inspect.getsource(CanonicalPassageProjector._commit)

        self.assertIn("cursor.copy(", source)
        self.assertIn("COPY canonical_passages(", source)
        self.assertIn("copy.write_row(", source)
        self.assertNotIn("cursor.executemany(", source)

    def test_dense_temporal_scope_is_materialized_before_vector_ranking(
        self,
    ) -> None:
        source = inspect.getsource(PassageHintRetrieval._dense_candidates)
        eligible = source.split(
            "WITH eligible AS MATERIALIZED", 1
        )[1].split("), nearest AS MATERIALIZED", 1)[0]

        self.assertIn(
            "dense_oversample = 50 if temporal_scope else 5",
            source,
        )
        self.assertIn("canonical_passages", eligible)
        self.assertIn("embedding.source_id=ANY(%s)", eligible)
        self.assertIn("passage.last_occurred_at>=%s", eligible)
        self.assertIn("passage.first_occurred_at<=%s", eligible)
        self.assertIn("SELECT DISTINCT ON (", source)
        self.assertIn("passage.logical_document_id", source)

    def test_hybrid_search_arms_run_concurrently_under_one_deadline(self) -> None:
        source = inspect.getsource(PassageHintRetrieval.search)

        self.assertIn("ThreadPoolExecutor(max_workers=3", source)
        self.assertIn("self._lexical_candidates", source)
        self.assertIn("self._sparse_candidates", source)
        self.assertIn("self._dense_candidates", source)
        self.assertIn("deadline_at", source)

    def test_slow_dense_arm_cannot_erase_fast_lexical_evidence(self) -> None:
        retrieval = object.__new__(PassageHintRetrieval)
        retrieval.store = SimpleNamespace(search_deadline_ms=500)
        retrieval.actor_ids = None
        retrieval.actor_relations = None
        retrieval.policy_fingerprint = "a" * 64
        barrier = threading.Barrier(3)
        candidate = {
            "source_id": "source:test",
            "logical_document_id": "ldoc_" + "1" * 32,
            "revision": 1,
            "native_parent_id": "session:test",
            "first_occurred_at": "2026-08-03T00:00:00Z",
            "last_occurred_at": "2026-08-09T00:00:00Z",
            "manifest_object_key": "objects/01/" + "b" * 64,
            "manifest_content_sha256": "c" * 64,
            "passage_id": "psg_" + "2" * 32,
            "passage_ordinal": 0,
            "spans": [],
            "receipts": ["recall://source:test/item?rev=1"],
            "text_redacted": "Synthetic employee work evidence",
            "score": 0.9,
        }

        def lexical(*_args, **_kwargs):
            barrier.wait(timeout=0.2)
            return [candidate], "ok"

        def sparse(*_args, **_kwargs):
            barrier.wait(timeout=0.2)
            return [], "ok"

        def dense(*_args, **_kwargs):
            barrier.wait(timeout=0.2)
            time.sleep(0.05)
            return [], "deadline-exceeded", "ann-oversampled", 100_000

        retrieval._lexical_candidates = mock.Mock(side_effect=lexical)
        retrieval._sparse_candidates = mock.Mock(side_effect=sparse)
        retrieval._dense_candidates = mock.Mock(side_effect=dense)

        result = retrieval.search(
            "What did everyone work on last week?",
            lexical_query="everyone work",
            since="2026-08-03T00:00:00Z",
            until="2026-08-10T00:00:00Z",
            limit=20,
        )

        self.assertEqual(len(result["results"]), 1)
        self.assertEqual(
            result["results"][0]["logical_document_id"],
            candidate["logical_document_id"],
        )
        self.assertTrue(result["diagnostics"]["deadline_exceeded"])
        self.assertTrue(result["diagnostics"]["partial_results_preserved"])

    def test_dense_strategy_is_cardinality_aware_not_time_filter_automatic(
        self,
    ) -> None:
        source = inspect.getsource(PassageHintRetrieval._dense_candidates)

        self.assertIn("_dense_scope_passage_count", source)
        self.assertIn("MAX_EXACT_DENSE_SCOPE_PASSAGES", source)
        self.assertNotIn("or temporal_scope\n", source)

    def test_dense_person_scope_is_materialized_before_vector_ranking(
        self,
    ) -> None:
        source = inspect.getsource(PassageHintRetrieval._dense_candidates)
        eligible = source.split(
            "WITH eligible AS MATERIALIZED", 1
        )[1].split("), nearest AS MATERIALIZED", 1)[0]
        actor_nearest = source.split(
            "), nearest AS MATERIALIZED", 1
        )[1].split("), ranked_documents AS MATERIALIZED", 1)[0]

        self.assertIn("canonical_passage_actors", eligible)
        self.assertIn("actor.actor_id=ANY(%s)", eligible)
        self.assertIn("actor.relation=ANY(%s)", eligible)
        self.assertIn("FROM eligible", actor_nearest)
        self.assertIn("ORDER BY eligible.embedding <=> %s::halfvec", actor_nearest)

    def test_backfill_is_idempotent_fast_first_and_starvation_bounded(
        self,
    ) -> None:
        seed = inspect.getsource(CanonicalPassageProjector.seed_backfill)
        pending = inspect.getsource(CanonicalPassageProjector._pending)

        self.assertIn("ON CONFLICT(tenant_id,source_id,logical_document_id)", seed)
        self.assertIn("DO NOTHING", seed)
        self.assertNotIn("generation+1", seed)
        self.assertIn("sum(size_part.size_bytes)", pending)
        self.assertIn("interval '5 minutes'", pending)

    def test_embedding_backfill_shards_by_stable_passage_identity(self) -> None:
        source = inspect.getsource(CanonicalPassageProjector.embed_pending)

        self.assertIn("1 <= shard_count <= 64", source)
        self.assertIn("0 <= shard_index < shard_count", source)
        self.assertIn("hashtextextended(passage.passage_id,0)", source)
        self.assertIn("shard:{shard_index}:{shard_count}", source)

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

    def test_hybrid_ranges_preserve_passage_pointer_when_sparse_scores_crowd(
        self,
    ) -> None:
        base = {
            "source_id": "source:test",
            "logical_document_id": (
                "ldoc_0123456789abcdef0123456789abcdef"
            ),
            "revision": 1,
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
            "text_redacted": "semantic pointer",
            "score": 0.40,
        }
        sparse = [
            {
                **base,
                "receipt": f"recall://source:test/exact-{index}?rev=1#item=0",
                "text_redacted": f"exact pointer {index}",
                "score": 1.0 - index / 100,
            }
            for index in range(4)
        ]

        result = collapse_document_candidates(
            (
                ("dense", 0.55, [dense]),
                ("sparse-exact", 0.15, sparse),
            ),
            limit=20,
        )[0]

        self.assertIn(
            "dense",
            {item["kind"] for item in result["matching_ranges"]},
        )
        self.assertTrue(any(
            item.get("spans") == [{"record_ordinal": 7}]
            for item in result["matching_ranges"]
        ))

    def test_query_rankings_fuse_at_stable_document_boundaries(self) -> None:
        def candidate(document_id: str, score: float, kind: str) -> dict:
            return {
                "source_id": "source:test",
                "logical_document_id": document_id,
                "revision": 1,
                "native_parent_id": document_id,
                "first_occurred_at": "2026-07-27T00:00:00Z",
                "last_occurred_at": "2026-07-27T00:10:00Z",
                "manifest_object_key": "objects/01/" + "a" * 64,
                "manifest_content_sha256": "b" * 64,
                "rank": score,
                "reasons": [kind],
                "matching_ranges": [{
                    "kind": kind,
                    "score": score,
                    "text": document_id,
                    "text_clipped": False,
                    "receipts": [],
                }],
            }

        repeated = "ldoc_" + "1" * 32
        first_only = "ldoc_" + "2" * 32
        second_only = "ldoc_" + "3" * 32
        results = fuse_document_rankings(
            (
                [
                    candidate(first_only, 0.9, "dense"),
                    candidate(repeated, 0.8, "dense"),
                ],
                [
                    candidate(second_only, 0.9, "passage-lexical"),
                    candidate(repeated, 0.8, "passage-lexical"),
                ],
            ),
            limit=3,
        )

        self.assertEqual(results[0]["logical_document_id"], repeated)
        self.assertEqual(len({
            result["logical_document_id"] for result in results
        }), 3)
        self.assertEqual(
            results[0]["reasons"],
            ["dense", "passage-lexical"],
        )

    def test_bundle_search_is_concurrent_and_order_equivalent(self) -> None:
        def candidate(document: int, kind: str) -> dict:
            return {
                "source_id": "source:test",
                "logical_document_id": f"ldoc_{document:032x}",
                "revision": 1,
                "native_parent_id": f"session:{document}",
                "first_occurred_at": "2026-07-27T00:00:00Z",
                "last_occurred_at": "2026-07-27T00:10:00Z",
                "manifest_object_key": "objects/01/" + "a" * 64,
                "manifest_content_sha256": "b" * 64,
                "rank": 0.9,
                "reasons": [kind],
                "matching_ranges": [{
                    "kind": kind,
                    "score": 0.9,
                    "text": f"candidate {document}",
                    "text_clipped": False,
                    "receipts": [],
                }],
            }

        responses = {
            f"query-{index}": {
                "results": [
                    candidate(99, "dense"),
                    candidate(index, "dense"),
                ],
                "arms": {
                    "dense": [candidate(index, "dense")],
                    "passage-lexical": [
                        candidate(index + 10, "passage-lexical")
                    ],
                    "sparse-exact": [
                        candidate(index + 20, "sparse-exact")
                    ],
                },
                "diagnostics": {
                    "dense_status": "ok",
                    "passage_lexical_status": "ok",
                    "sparse_status": "ok",
                },
            }
            for index in range(4)
        }
        active = maximum = 0
        lock = threading.Lock()

        def delayed_search(query, **_kwargs):
            nonlocal active, maximum
            with lock:
                active += 1
                maximum = max(maximum, active)
            time.sleep(0.15)
            with lock:
                active -= 1
            return responses[query]

        retrieval = object.__new__(PassageHintRetrieval)
        with mock.patch.object(retrieval, "search", side_effect=delayed_search):
            started = time.monotonic()
            actual = retrieval.search_bundle(
                tuple(responses),
                lexical_queries=tuple(f"lexical-{index}" for index in range(4)),
                since=None,
                until=None,
                limit=20,
            )
            elapsed = time.monotonic() - started

        expected = {
            "results": fuse_document_rankings(
                tuple(response["results"] for response in responses.values()),
                limit=20,
            ),
            "arms": {
                arm: fuse_document_rankings(
                    tuple(response["arms"][arm] for response in responses.values()),
                    limit=20,
                )
                for arm in ("dense", "passage-lexical", "sparse-exact")
            },
            "diagnostics": {
                "engine": "lossless-passages-v1-bundle",
                "query_count": 4,
                "dense_status": "ok",
                "passage_lexical_status": "ok",
                "sparse_status": "ok",
            },
        }
        self.assertEqual(actual, expected)
        self.assertEqual(maximum, 4)
        sequential_seconds = 4 * 0.15
        self.assertLess(elapsed, sequential_seconds * 0.55)

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
