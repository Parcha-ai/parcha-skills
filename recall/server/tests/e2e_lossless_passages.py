#!/usr/bin/env python3
"""PostgreSQL E2E for logical-document-linked lossless passages."""

from __future__ import annotations

import json
import os
import sys
import tempfile
import uuid
from pathlib import Path


RECALL = Path(__file__).resolve().parents[2]
SERVER = RECALL / "server"
sys.path.insert(0, str(RECALL))
sys.path.insert(0, str(SERVER))

from e2e_logical_evidence_projection import (  # noqa: E402
    insert_record,
    insert_source,
)
from recall_server.archive import FilesystemArchiveStore  # noqa: E402
from recall_server.db import BrainStore  # noqa: E402
from recall_server.logical_evidence import (  # noqa: E402
    LogicalEvidenceProjectionStore,
)
from recall_server.logical_evidence_projection import (  # noqa: E402
    CanonicalLogicalEvidenceProjector,
    mark_logical_evidence_dirty,
)
from recall_server.passage_index import CanonicalPassageProjector  # noqa: E402
from recall_server.passage_projection import PassagePolicy  # noqa: E402
from recall_server.passage_representations import (  # noqa: E402
    CanonicalPassageRepresentationIndex,
    PassageContextPolicy,
    PassageRepresentation,
)
from recall_server.canonical_retrieval import (  # noqa: E402
    BoundCanonicalRetrieval,
)


class SyntheticEmbeddingRuntime:
    dimensions = 512
    fingerprint = "synthetic-lossless-passage-runtime"
    passage_fingerprint = "synthetic-lossless-passage-runtime"
    model = "synthetic-lossless-passage-model"

    def __init__(self) -> None:
        self.document_calls = 0

    def embed_documents(self, values: list[str]) -> list[list[float]]:
        self.document_calls += 1
        return [[float(index % 2)] * 512 for index, _value in enumerate(values)]

    def embed_passages(self, values: list[str]) -> list[list[float]]:
        return self.embed_documents(values)

    @staticmethod
    def embed_query(_value: str) -> list[float]:
        return [0.0] * 512


def main() -> None:
    runtime = SyntheticEmbeddingRuntime()
    store = BrainStore(
        os.environ["RECALL_DATABASE_URL"],
        semantic_runtime=runtime,  # type: ignore[arg-type]
    )
    store.migrate()
    nonce = uuid.uuid4().hex
    tenant = f"tenant:passage-e2e:{nonce}"
    principal = f"principal:passage-e2e:{nonce}"
    source = f"codex:passage-e2e:{nonce}"
    parent = f"session-passage-e2e-{nonce}"
    actor = f"actor_{nonce}"
    actor_alias = f"alice-e2e-{nonce}"
    with store.connect() as connection:
        insert_source(connection, tenant, principal, source)
        connection.execute(
            """INSERT INTO brain_actors(
                   tenant_id,actor_id,actor_kind,display_name
               ) VALUES (%s,%s,'human','Alice Example')""",
            (tenant, actor),
        )
        connection.execute(
            """INSERT INTO brain_actor_aliases(tenant_id,actor_id,alias)
               VALUES (%s,%s,%s)""",
            (tenant, actor, actor_alias),
        )
        connection.execute(
            """INSERT INTO canonical_source_actor_bindings(
                   tenant_id,source_id,actor_id,relation
               ) VALUES (%s,%s,%s,'contributor')""",
            (tenant, source, actor),
        )
        insert_record(
            connection,
            tenant=tenant,
            source=source,
            parent=parent,
            native=f"{parent}:user",
            text="why did the gateway preserve tenant boundaries",
            role="user",
            byte_start=0,
        )
        insert_record(
            connection,
            tenant=tenant,
            source=source,
            parent=parent,
            native=f"{parent}:tool",
            text="synthetic sparse-only tool marker",
            role="tool",
            byte_start=10,
        )
        insert_record(
            connection,
            tenant=tenant,
            source=source,
            parent=parent,
            native=f"{parent}:assistant",
            text="the gateway now intersects every explicit source grant",
            role="assistant",
            byte_start=20,
        )

    with tempfile.TemporaryDirectory(prefix="recall-passage-e2e-") as value:
        archive = FilesystemArchiveStore(
            Path(value) / "archive",
            namespace_key=b"p" * 32,
        )
        logical_store = LogicalEvidenceProjectionStore(archive)
        logical = CanonicalLogicalEvidenceProjector(
            store,
            logical_store,
            bound_tenant_id=tenant,
            raw_archive=archive,
        )
        assert logical.seed_backfill(tenant_id=tenant) == 1
        logical_result = logical.project_pending(
            tenant_id=tenant,
            batch_size=10,
            max_batches=1,
            upload_concurrency=1,
        )
        assert logical_result["documents"] == 1

        passages = CanonicalPassageProjector(
            store,
            logical_store,
            policy=PassagePolicy(target_tokens=4, overlap_tokens=1),
            bound_tenant_id=tenant,
        )
        passage_result = passages.project_pending(
            tenant_id=tenant,
            batch_size=10,
            max_batches=1,
            concurrency=2,
        )
        assert passage_result["documents"] == 1
        assert passage_result["passages"] >= 2
        embedding_result = passages.embed_pending(
            tenant_id=tenant,
            batch_size=100,
            max_batches=2,
        )
        assert embedding_result["status"] == "complete"
        assert embedding_result["processed"] == passage_result["passages"]
        assert runtime.document_calls == 1
        with store.connect() as connection:
            connection.execute(
                """INSERT INTO canonical_passage_projection_queue(
                       tenant_id,source_id,logical_document_id,revision,
                       generation,reason,changed_at
                   )
                   SELECT tenant_id,source_id,logical_document_id,revision,
                          1,'backfill',clock_timestamp()
                     FROM canonical_evidence_documents
                    WHERE tenant_id=%s AND source_id=%s
                      AND native_parent_id=%s
                   ON CONFLICT(tenant_id,source_id,logical_document_id)
                   DO UPDATE SET
                       revision=excluded.revision,
                       generation=
                           canonical_passage_projection_queue.generation+1,
                       reason='backfill',
                       changed_at=clock_timestamp()""",
                (tenant, source, parent),
            )
        reprojected = passages.project_pending(
            tenant_id=tenant,
            batch_size=10,
            max_batches=1,
            concurrency=2,
        )
        # H1-T3: a same-revision re-projection is a no-op on the passage
        # rows: every id is retained, nothing is written or deleted.
        assert reprojected["documents"] == 1, reprojected
        assert reprojected["passages"] == 0, reprojected
        assert reprojected["inserted"] == 0, reprojected
        assert reprojected["deleted"] == 0, reprojected
        assert reprojected["retained"] == passage_result["passages"], reprojected
        assert runtime.document_calls == 1

        def passage_rows(connection) -> dict[str, dict]:
            return {
                row["passage_id"]: dict(row)
                for row in connection.execute(
                    """SELECT passage.passage_id,passage.ordinal,
                              passage.revision,passage.text_sha256,
                              passage.xmin::text AS passage_xmin,
                              embedding.embedded_at,
                              embedding.xmin::text AS embedding_xmin
                         FROM canonical_passages passage
                         LEFT JOIN canonical_passage_embeddings embedding
                           USING(tenant_id,source_id,passage_id)
                        WHERE passage.tenant_id=%s AND passage.source_id=%s
                        ORDER BY passage.ordinal""",
                    (tenant, source),
                ).fetchall()
            }

        def requeue(connection) -> None:
            connection.execute(
                """INSERT INTO canonical_passage_projection_queue(
                       tenant_id,source_id,logical_document_id,revision,
                       generation,reason,changed_at
                   )
                   SELECT tenant_id,source_id,logical_document_id,revision,
                          1,'backfill',clock_timestamp()
                     FROM canonical_evidence_documents
                    WHERE tenant_id=%s AND source_id=%s
                      AND native_parent_id=%s
                   ON CONFLICT(tenant_id,source_id,logical_document_id)
                   DO UPDATE SET
                       revision=excluded.revision,
                       generation=
                           canonical_passage_projection_queue.generation+1,
                       reason='backfill',
                       changed_at=clock_timestamp()""",
                (tenant, source, parent),
            )

        with store.connect() as connection:
            before_rotation = passage_rows(connection)
            pointer_before = connection.execute(
                """SELECT created_at,xmin::text AS xmin,passage_count
                     FROM canonical_passage_documents
                    WHERE tenant_id=%s AND source_id=%s""",
                (tenant, source),
            ).fetchone()
        previous_passages = len(before_rotation)
        assert previous_passages == passage_result["passages"]
        assert all(row["embedded_at"] is not None for row in before_rotation.values())

        # Ordinal collision: rotate every stored ordinal by one under the
        # (document, policy, ordinal) unique index, then re-project. Every
        # retained row must move back to its real ordinal in one transaction
        # without a transient duplicate, and no row is deleted or inserted.
        with store.connect() as connection:
            with connection.transaction():
                connection.execute(
                    """UPDATE canonical_passages
                          SET ordinal=ordinal+1000000
                        WHERE tenant_id=%s AND source_id=%s""",
                    (tenant, source),
                )
                connection.execute(
                    """UPDATE canonical_passages
                          SET ordinal=(ordinal-1000000+1)%%%s
                        WHERE tenant_id=%s AND source_id=%s""",
                    (previous_passages, tenant, source),
                )
                requeue(connection)
        rotated = passages.project_pending(
            tenant_id=tenant,
            batch_size=10,
            max_batches=1,
            concurrency=2,
        )
        assert rotated["documents"] == 1, rotated
        assert (rotated["inserted"], rotated["deleted"], rotated["retained"]) == (
            0, 0, previous_passages,
        ), rotated
        with store.connect() as connection:
            after_rotation = passage_rows(connection)
        assert after_rotation.keys() == before_rotation.keys()
        for passage_id, row in before_rotation.items():
            moved = after_rotation[passage_id]
            assert moved["ordinal"] == row["ordinal"], (row, moved)
            assert moved["embedded_at"] == row["embedded_at"]
            assert moved["embedding_xmin"] == row["embedding_xmin"]
        assert runtime.document_calls == 1

        # Append: a fourth record joins the session, the logical document
        # advances to revision 2 in place, and the passage projector keeps
        # every full prefix window (same id, same embedding row), replaces at
        # most the final short window, and inserts only the new windows.
        # (Synthetic records carry no byte offsets, so the logical document
        # orders them by native id: the suffix must sort after ":user".)
        appended_native = f"{parent}:z-assistant-follow-up"
        with store.connect() as connection:
            with connection.transaction():
                insert_record(
                    connection,
                    tenant=tenant,
                    source=source,
                    parent=parent,
                    native=appended_native,
                    text="tenant grants are intersected before any passage scan",
                    role="assistant",
                    byte_start=30,
                )
                mark_logical_evidence_dirty(
                    connection,
                    tenant_id=tenant,
                    source_id=source,
                    native_ids=[appended_native],
                    reason="ingest",
                )
        appended_logical = logical.project_pending(
            tenant_id=tenant,
            batch_size=10,
            max_batches=1,
            upload_concurrency=1,
        )
        assert appended_logical["documents"] == 1, appended_logical
        appended = passages.project_pending(
            tenant_id=tenant,
            batch_size=10,
            max_batches=1,
            concurrency=2,
        )
        assert appended["documents"] == 1, appended
        assert appended["deleted"] <= 1, appended
        assert appended["retained"] >= previous_passages - 1, appended
        assert appended["retained"] + appended["deleted"] == previous_passages, appended
        assert appended["inserted"] >= 1, appended
        assert appended["passages"] == appended["inserted"], appended
        # Projection never embeds: zero new embedding calls.
        assert runtime.document_calls == 1
        with store.connect() as connection:
            after_append = passage_rows(connection)
            pointer_after = connection.execute(
                """SELECT revision,created_at,xmin::text AS xmin,passage_count
                     FROM canonical_passage_documents
                    WHERE tenant_id=%s AND source_id=%s""",
                (tenant, source),
            ).fetchone()
        total_passages = len(after_append)
        assert total_passages == previous_passages + appended["inserted"] - appended["deleted"]
        assert pointer_after["revision"] == 2, pointer_after
        assert pointer_after["passage_count"] == total_passages, pointer_after
        assert pointer_after["created_at"] == pointer_before["created_at"]
        assert pointer_after["xmin"] != pointer_before["xmin"]  # updated in place
        assert [row["ordinal"] for row in after_append.values()] == list(range(total_passages))
        retained_ids = before_rotation.keys() & after_append.keys()
        assert len(retained_ids) == appended["retained"], (len(retained_ids), appended)
        for passage_id in retained_ids:
            old, new = before_rotation[passage_id], after_append[passage_id]
            assert new["text_sha256"] == old["text_sha256"]
            assert new["revision"] == 2
            # The embedding row of a retained passage is untouched.
            assert new["embedded_at"] == old["embedded_at"], passage_id
            assert new["embedding_xmin"] == old["embedding_xmin"], passage_id
        inserted_ids = after_append.keys() - before_rotation.keys()
        assert len(inserted_ids) == appended["inserted"]
        reused_embeddings = sum(
            1 for passage_id in inserted_ids
            if after_append[passage_id]["embedded_at"] is not None
        )
        embed_after_append = passages.embed_pending(
            tenant_id=tenant,
            batch_size=100,
            max_batches=2,
        )
        assert embed_after_append["status"] == "complete"
        # Only the inserted passages without a reusable vector are embedded.
        assert embed_after_append["processed"] == appended["inserted"] - reused_embeddings, (
            embed_after_append, appended, reused_embeddings,
        )
        assert runtime.document_calls == (2 if embed_after_append["processed"] else 1)

        # Shadow parity gate (read-only): recomputing the passages of the
        # current logical document reproduces every stored id and the same
        # receipt multiset.
        with store.connect() as connection:
            before_shadow = passage_rows(connection)
        shadow = passages.shadow_diff(tenant_id=tenant, source_id=source, limit=50)
        assert shadow["read_only"] is True
        assert shadow["receipt_parity"] is True, shadow
        assert shadow["totals"]["documents"] == 1, shadow
        assert shadow["totals"]["documents_stale"] == 0, shadow
        assert shadow["totals"]["passages_existing"] == total_passages, shadow
        assert shadow["totals"]["passages_recomputed"] == total_passages, shadow
        assert shadow["totals"]["ids_shared"] == total_passages, shadow
        with store.connect() as connection:
            assert passage_rows(connection) == before_shadow  # nothing written
        actor_representation = PassageRepresentation(
            "actor-context-e2e",
            runtime,
            PassageContextPolicy(),
        )
        represented = CanonicalPassageRepresentationIndex(
            store,
            passage_policy_fingerprint=passages.policy.fingerprint,
            representation=actor_representation,
            bound_tenant_id=tenant,
        ).embed_pending(
            tenant_id=tenant,
            batch_size=100,
            max_batches=2,
        )
        assert represented["status"] == "complete"
        assert represented["processed"] == total_passages
        bound = BoundCanonicalRetrieval(
            store,
            tenant_id=tenant,
            principal_id=principal,
            authorized_sources=(source,),
            passage_policy=PassagePolicy(
                target_tokens=4,
                overlap_tokens=1,
            ),
        )
        hints = bound.passage_hints(
            "why did the gateway preserve tenant boundaries?",
            limit=5,
        )
        assert len(hints["results"]) == 1
        assert hints["results"][0]["source_id"] == source
        assert hints["results"][0]["logical_document_id"].startswith("ldoc_")
        assert hints["results"][0]["matching_ranges"]
        actor_hints = bound.passage_hints(
            "What did Alice work on?",
            filters={"person": actor_alias},
            limit=5,
        )
        assert len(actor_hints["results"]) == 1
        authored_hints = bound.passage_hints(
            "What did Alice write?",
            filters={
                "person": actor_alias,
                "person_relation": "author",
            },
            limit=5,
        )
        assert authored_hints["results"] == []
        denied = BoundCanonicalRetrieval(
            store,
            tenant_id=tenant,
            principal_id="principal:denied",
            authorized_sources=(),
            passage_policy=PassagePolicy(
                target_tokens=4,
                overlap_tokens=1,
            ),
        ).passage_hints("gateway tenant boundaries")
        assert denied["results"] == []

        # Liveness is enforced after ranking, not inside the candidate scans.
        # A passage whose chunk was forgotten must still vanish from every arm.
        with store.connect() as connection:
            connection.execute(
                """UPDATE canonical_chunks SET deleted_at=now()
                    WHERE tenant_id=%s AND source_id=%s AND deleted_at IS NULL""",
                (tenant, source),
            )
            connection.commit()
        try:
            hidden = bound.passage_hints(
                "why did the gateway preserve tenant boundaries?",
                limit=5,
            )
            assert hidden["results"] == [], hidden["diagnostics"]
            assert hidden["diagnostics"]["dense_status"] == "ok"
            assert hidden["diagnostics"]["passage_lexical_status"] == "ok"
            assert set(hidden["diagnostics"]["arm_elapsed_ms"]) == {
                "dense", "passage_lexical", "sparse_exact",
            }
        finally:
            with store.connect() as connection:
                connection.execute(
                    """UPDATE canonical_chunks SET deleted_at=NULL
                        WHERE tenant_id=%s AND source_id=%s""",
                    (tenant, source),
                )
                connection.commit()
        restored = bound.passage_hints(
            "why did the gateway preserve tenant boundaries?",
            limit=5,
        )
        assert len(restored["results"]) == 1

    with store.connect() as connection:
        counts = connection.execute(
            """SELECT
                   (SELECT count(*) FROM canonical_passage_documents
                     WHERE tenant_id=%s) AS documents,
                   (SELECT count(*) FROM canonical_passages
                     WHERE tenant_id=%s) AS passages,
                   (SELECT count(*) FROM canonical_passage_embeddings
                     WHERE tenant_id=%s) AS embeddings,
                   (SELECT count(*) FROM canonical_passage_projection_queue
                     WHERE tenant_id=%s) AS queued,
                   (SELECT count(*)
                      FROM canonical_evidence_document_actors
                     WHERE tenant_id=%s AND actor_id=%s
                       AND relation='contributor') AS document_actor_links,
                   (SELECT count(*) FROM canonical_passage_actors
                     WHERE tenant_id=%s AND actor_id=%s
                       AND relation='contributor') AS passage_actor_links,
                   (SELECT count(*) FROM canonical_passage_contexts context
                     WHERE context.tenant_id=%s
                       AND context.context_text_redacted LIKE
                           '%%people: Alice Example [contributor]%%')
                       AS actor_contexts,
                   (SELECT count(*) FROM canonical_passages
                     WHERE tenant_id=%s
                       AND text_redacted LIKE '%%sparse-only%%')
                       AS dense_tool_hits,
                   (SELECT count(*) FROM canonical_chunks
                     WHERE tenant_id=%s
                       AND text_redacted LIKE '%%sparse-only%%')
                       AS sparse_tool_hits""",
            (
                tenant,
                tenant,
                tenant,
                tenant,
                tenant,
                actor,
                tenant,
                actor,
                tenant,
                tenant,
                tenant,
            ),
        ).fetchone()
    assert counts["documents"] == 1
    assert counts["passages"] == counts["embeddings"]
    assert counts["queued"] == 0
    assert counts["document_actor_links"] == 1
    assert counts["passage_actor_links"] == counts["passages"]
    assert counts["actor_contexts"] == counts["passages"]
    assert counts["dense_tool_hits"] == 0
    assert counts["sparse_tool_hits"] == 1
    print(
        json.dumps(
            {
                "status": "pass",
                "logical_documents": 1,
                "passage_documents": counts["documents"],
                "passages": counts["passages"],
                "embeddings": counts["embeddings"],
                "reused_embeddings": counts["embeddings"],
                "same_revision_reproject": {
                    key: reprojected[key]
                    for key in ("inserted", "deleted", "retained")
                },
                "ordinal_rotation_reproject": {
                    key: rotated[key]
                    for key in ("inserted", "deleted", "retained")
                },
                "append_reproject": {
                    key: appended[key]
                    for key in ("inserted", "deleted", "retained")
                },
                "append_embedding_calls": embed_after_append["processed"],
                "append_reused_embeddings": reused_embeddings,
                "shadow_diff_totals": shadow["totals"],
                "dense_tool_hits": counts["dense_tool_hits"],
                "sparse_tool_hits": counts["sparse_tool_hits"],
                "completion_model_calls": 0,
                "authorized_hint_documents": len(hints["results"]),
                "unauthorized_hint_documents": len(denied["results"]),
                "actor_hint_documents": len(actor_hints["results"]),
                "wrong_relation_documents": len(authored_hints["results"]),
                "actor_contexts": counts["actor_contexts"],
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
