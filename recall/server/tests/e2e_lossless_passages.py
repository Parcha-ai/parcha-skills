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
        assert reprojected["passages"] == passage_result["passages"]
        assert runtime.document_calls == 1
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
        assert represented["processed"] == passage_result["passages"]
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
