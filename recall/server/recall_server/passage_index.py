"""Projection of complete logical documents into lossless retrieval passages."""

from __future__ import annotations

import json
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from datetime import datetime
from io import BytesIO
from typing import Any, Iterator

from .logical_evidence import (
    LogicalEvidenceError,
    LogicalEvidenceProjectionStore,
)
from .passage_projection import (
    LosslessPassage,
    PassagePolicy,
    build_passages,
    decode_logical_record,
    visible_messages,
)

MAX_PASSAGE_PROJECTION_BATCH = 1_000
MAX_PASSAGE_EMBEDDING_BATCH = 5_000
PASSAGE_POOL_WARM_SIZE = 4
PASSAGE_COMMIT_WORKERS = 8


@dataclass(frozen=True)
class PassageCandidate:
    tenant_id: str
    source_id: str
    logical_document_id: str
    revision: int
    generation: int
    changed_at: datetime
    source_document_sha256: str
    manifest_reference: dict[str, Any]
    part_references: tuple[dict[str, Any], ...]


@dataclass(frozen=True)
class PreparedPassageDocument:
    candidate: PassageCandidate
    dense_message_count: int
    dense_message_bytes: int
    passages: tuple[LosslessPassage, ...]


class CanonicalPassageProjector:
    """Build one disposable pointer index from authoritative logical documents."""

    def __init__(
        self,
        store: Any,
        logical_projection: LogicalEvidenceProjectionStore,
        *,
        policy: PassagePolicy,
        bound_tenant_id: str | None = None,
    ) -> None:
        if bound_tenant_id is not None and (
            not isinstance(bound_tenant_id, str)
            or not bound_tenant_id
            or len(bound_tenant_id) > 256
        ):
            raise ValueError("passage projector tenant is invalid")
        if not isinstance(policy, PassagePolicy):
            raise ValueError("passage projector policy is invalid")
        self.store = store
        self.logical_projection = logical_projection
        self.policy = policy
        self.bound_tenant_id = bound_tenant_id

    def _tenant(self, tenant_id: str | None) -> str | None:
        if self.bound_tenant_id is None:
            return tenant_id
        if tenant_id is not None and tenant_id != self.bound_tenant_id:
            raise PermissionError("passage projector tenant is not authorized")
        return self.bound_tenant_id

    @staticmethod
    def _reference(
        row: dict[str, Any],
        *,
        prefix: str,
    ) -> dict[str, Any]:
        created_at = row[prefix + "created_at"]
        if isinstance(created_at, datetime):
            created_at = created_at.isoformat()
        return {
            "contract": "recall.artifact-ref.v1",
            "schema_version": 1,
            "tenant_id": row["tenant_id"],
            "source_id": row["source_id"],
            "artifact_id": row[prefix + "artifact_id"],
            "storage_backend": row[prefix + "storage_backend"],
            "object_key": row[prefix + "object_key"],
            "content_sha256": row[prefix + "content_sha256"],
            "size_bytes": row[prefix + "size_bytes"],
            "media_type": row[prefix + "media_type"],
            "encryption": row[prefix + "encryption"],
            "version_id": row[prefix + "version_id"],
            "created_at": created_at,
        }

    def seed_backfill(self, *, tenant_id: str | None = None) -> int:
        tenant_id = self._tenant(tenant_id)
        with self.store.connect() as connection:
            result = connection.execute(
                """INSERT INTO canonical_passage_projection_queue(
                       tenant_id,source_id,logical_document_id,revision,
                       generation,reason,changed_at
                   )
                   SELECT evidence.tenant_id,evidence.source_id,
                          evidence.logical_document_id,evidence.revision,
                          1,'backfill',clock_timestamp()
                     FROM canonical_evidence_documents evidence
                     LEFT JOIN canonical_passage_documents projected
                       ON projected.tenant_id=evidence.tenant_id
                      AND projected.source_id=evidence.source_id
                      AND projected.logical_document_id
                          =evidence.logical_document_id
                      AND projected.revision=evidence.revision
                      AND projected.policy_fingerprint=%s
                   WHERE (%s::text IS NULL OR evidence.tenant_id=%s)
                      AND projected.logical_document_id IS NULL
                   ON CONFLICT(tenant_id,source_id,logical_document_id)
                   DO NOTHING""",
                (self.policy.fingerprint, tenant_id, tenant_id),
            )
        return max(0, result.rowcount)

    def _pending(
        self,
        *,
        tenant_id: str | None,
        limit: int,
    ) -> tuple[PassageCandidate, ...]:
        with self.store.connect() as connection:
            rows = connection.execute(
                """SELECT queue.tenant_id,queue.source_id,
                          queue.logical_document_id,queue.revision,
                          queue.generation,queue.changed_at,
                          evidence.document_content_sha256,
                          evidence.manifest_artifact_id,
                          evidence.manifest_storage_backend,
                          evidence.manifest_object_key,
                          evidence.manifest_content_sha256,
                          evidence.manifest_size_bytes,
                          evidence.manifest_media_type,
                          evidence.manifest_encryption,
                          evidence.manifest_version_id,
                          evidence.created_at AS manifest_created_at,
                          part.part_ordinal,
                          part.artifact_id AS part_artifact_id,
                          part.storage_backend AS part_storage_backend,
                          part.object_key AS part_object_key,
                          part.content_sha256 AS part_content_sha256,
                          part.size_bytes AS part_size_bytes,
                          part.media_type AS part_media_type,
                          part.encryption AS part_encryption,
                          part.version_id AS part_version_id,
                          part.created_at AS part_created_at
                     FROM (
                           SELECT *
                             FROM canonical_passage_projection_queue
                                  candidate_queue
                            WHERE (%s::text IS NULL
                                   OR candidate_queue.tenant_id=%s)
                            ORDER BY (
                                candidate_queue.changed_at <
                                clock_timestamp()-interval '5 minutes'
                            ) DESC,(
                                SELECT coalesce(sum(size_part.size_bytes),0)
                                  FROM canonical_evidence_document_parts
                                       size_part
                                 WHERE size_part.tenant_id=
                                           candidate_queue.tenant_id
                                   AND size_part.source_id=
                                           candidate_queue.source_id
                                   AND size_part.logical_document_id=
                                           candidate_queue.logical_document_id
                                   AND size_part.revision=
                                           candidate_queue.revision
                            ),candidate_queue.changed_at,
                              candidate_queue.tenant_id,
                              candidate_queue.source_id,
                              candidate_queue.logical_document_id
                            LIMIT %s
                     ) queue
                     JOIN canonical_evidence_documents evidence
                       ON evidence.tenant_id=queue.tenant_id
                      AND evidence.source_id=queue.source_id
                      AND evidence.logical_document_id
                          =queue.logical_document_id
                      AND evidence.revision=queue.revision
                     JOIN canonical_evidence_document_parts part
                       ON part.tenant_id=evidence.tenant_id
                      AND part.source_id=evidence.source_id
                      AND part.logical_document_id
                          =evidence.logical_document_id
                      AND part.revision=evidence.revision
                    ORDER BY queue.changed_at,queue.tenant_id,
                             queue.source_id,queue.logical_document_id,
                             part.part_ordinal""",
                (tenant_id, tenant_id, limit),
            ).fetchall()
        grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
        for row in rows:
            key = (
                row["tenant_id"],
                row["source_id"],
                row["logical_document_id"],
            )
            grouped.setdefault(key, []).append(row)
        candidates = []
        for values in grouped.values():
            first = values[0]
            candidates.append(
                PassageCandidate(
                    tenant_id=first["tenant_id"],
                    source_id=first["source_id"],
                    logical_document_id=first["logical_document_id"],
                    revision=int(first["revision"]),
                    generation=int(first["generation"]),
                    changed_at=first["changed_at"],
                    source_document_sha256=first[
                        "document_content_sha256"
                    ],
                    manifest_reference=self._reference(
                        first,
                        prefix="manifest_",
                    ),
                    part_references=tuple(
                        self._reference(row, prefix="part_")
                        for row in values
                    ),
                )
            )
        return tuple(candidates)

    def _requeue_missing(self, candidate: PassageCandidate) -> int:
        """Rebuild a current logical document whose immutable part disappeared."""

        with self.store.connect() as connection:
            result = connection.execute(
                """INSERT INTO canonical_evidence_document_queue(
                       tenant_id,source_id,native_parent_id,generation,
                       reason,changed_at
                   )
                   SELECT evidence.tenant_id,evidence.source_id,
                          evidence.native_parent_id,1,'backfill',
                          clock_timestamp()
                     FROM canonical_evidence_documents evidence
                    WHERE evidence.tenant_id=%s
                      AND evidence.source_id=%s
                      AND evidence.logical_document_id=%s
                      AND evidence.revision=%s
                   ON CONFLICT(tenant_id,source_id,native_parent_id)
                   DO UPDATE SET
                       generation=
                           canonical_evidence_document_queue.generation+1,
                       reason='backfill',
                       changed_at=clock_timestamp()""",
                (
                    candidate.tenant_id,
                    candidate.source_id,
                    candidate.logical_document_id,
                    candidate.revision,
                ),
            )
        return max(0, result.rowcount)

    def _prepare(self, candidate: PassageCandidate) -> PreparedPassageDocument:
        manifest = self.logical_projection.read_manifest(
            candidate.manifest_reference,
            tenant_id=candidate.tenant_id,
            source_id=candidate.source_id,
        )
        if (
            manifest.get("logical_document_id")
            != candidate.logical_document_id
            or manifest.get("revision") != candidate.revision
            or manifest.get("document_content_sha256")
            != candidate.source_document_sha256
            or len(manifest.get("parts") or ())
            != len(candidate.part_references)
        ):
            raise LogicalEvidenceError("passage_manifest_catalog_mismatch")
        def records() -> Iterator[Any]:
            for ordinal, reference in enumerate(candidate.part_references):
                manifest_part = manifest["parts"][ordinal]
                if (
                    manifest_part.get("ordinal") != ordinal
                    or any(
                        manifest_part.get(field) != reference[field]
                        for field in (
                            "artifact_id",
                            "object_key",
                            "content_sha256",
                            "size_bytes",
                            "media_type",
                            "version_id",
                        )
                    )
                ):
                    raise LogicalEvidenceError(
                        "passage_manifest_part_mismatch"
                    )
                payload = self.logical_projection.read_part(
                    reference,
                    tenant_id=candidate.tenant_id,
                    source_id=candidate.source_id,
                )
                if not payload.endswith(b"\n"):
                    raise LogicalEvidenceError(
                        "passage_logical_part_invalid"
                    )
                for line in BytesIO(payload):
                    # read_part verifies the immutable object hash. Validate
                    # each record contract without serializing trusted bytes
                    # back into canonical JSON a second time.
                    yield decode_logical_record(
                        line,
                        source_id=candidate.source_id,
                        verify_canonical=False,
                    )

        messages = visible_messages(records())
        passages = (
            build_passages(
                tenant_id=candidate.tenant_id,
                source_id=candidate.source_id,
                logical_document_id=candidate.logical_document_id,
                revision=candidate.revision,
                messages=messages,
                policy=self.policy,
            )
            if messages
            else ()
        )
        return PreparedPassageDocument(
            candidate=candidate,
            dense_message_count=len(messages),
            dense_message_bytes=sum(len(message.text.encode()) for message in messages),
            passages=passages,
        )

    def _commit(self, prepared: PreparedPassageDocument) -> str:
        candidate = prepared.candidate
        with self.store.connect() as connection:
            with connection.transaction():
                connection.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                    ("lossless-passages\x1f" + candidate.logical_document_id,),
                )
                queued = connection.execute(
                    """SELECT revision,generation,changed_at
                         FROM canonical_passage_projection_queue
                        WHERE tenant_id=%s AND source_id=%s
                          AND logical_document_id=%s
                        FOR UPDATE""",
                    (
                        candidate.tenant_id,
                        candidate.source_id,
                        candidate.logical_document_id,
                    ),
                ).fetchone()
                current = connection.execute(
                    """SELECT revision,document_content_sha256
                         FROM canonical_evidence_documents
                        WHERE tenant_id=%s AND source_id=%s
                          AND logical_document_id=%s""",
                    (
                        candidate.tenant_id,
                        candidate.source_id,
                        candidate.logical_document_id,
                    ),
                ).fetchone()
                if (
                    queued is None
                    or current is None
                    or int(queued["revision"]) != candidate.revision
                    or int(queued["generation"]) != candidate.generation
                    or queued["changed_at"] != candidate.changed_at
                    or int(current["revision"]) != candidate.revision
                    or current["document_content_sha256"]
                    != candidate.source_document_sha256
                ):
                    return "stale"
                connection.execute(
                    """CREATE TEMP TABLE
                           recall_reusable_passage_embeddings
                           ON COMMIT DROP AS
                       SELECT embedding.model,embedding.dimensions,
                              embedding.content_sha256,
                              embedding.runtime_fingerprint,
                              embedding.embedding,embedding.embedded_at
                         FROM canonical_passage_embeddings embedding
                         JOIN canonical_passages passage
                           USING(tenant_id,source_id,passage_id)
                        WHERE passage.tenant_id=%s
                          AND passage.source_id=%s
                          AND passage.logical_document_id=%s""",
                    (
                        candidate.tenant_id,
                        candidate.source_id,
                        candidate.logical_document_id,
                    ),
                )
                connection.execute(
                    """DELETE FROM canonical_passage_documents
                        WHERE tenant_id=%s AND source_id=%s
                          AND logical_document_id=%s""",
                    (
                        candidate.tenant_id,
                        candidate.source_id,
                        candidate.logical_document_id,
                    ),
                )
                connection.execute(
                    """INSERT INTO canonical_passage_documents(
                           tenant_id,source_id,logical_document_id,revision,
                           policy_fingerprint,target_tokens,overlap_tokens,
                           source_document_sha256,dense_message_count,
                           dense_message_bytes,passage_count
                       ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    (
                        candidate.tenant_id,
                        candidate.source_id,
                        candidate.logical_document_id,
                        candidate.revision,
                        self.policy.fingerprint,
                        self.policy.target_tokens,
                        self.policy.overlap_tokens,
                        candidate.source_document_sha256,
                        prepared.dense_message_count,
                        prepared.dense_message_bytes,
                        len(prepared.passages),
                    ),
                )
                if prepared.passages:
                    with connection.cursor() as cursor:
                        with cursor.copy(
                            """COPY canonical_passages(
                                   tenant_id,source_id,logical_document_id,
                                   revision,passage_id,ordinal,
                                   policy_fingerprint,target_tokens,
                                   overlap_tokens,token_count,
                                   first_occurred_at,last_occurred_at,
                                   roles,receipts,spans,text_redacted,
                                   text_sha256
                               ) FROM STDIN"""
                        ) as copy:
                            for passage in prepared.passages:
                                copy.write_row((
                                    passage.tenant_id,
                                    passage.source_id,
                                    passage.logical_document_id,
                                    passage.revision,
                                    passage.passage_id,
                                    passage.ordinal,
                                    passage.policy_fingerprint,
                                    self.policy.target_tokens,
                                    self.policy.overlap_tokens,
                                    passage.token_count,
                                    passage.first_occurred_at,
                                    passage.last_occurred_at,
                                    list(passage.roles),
                                    list(passage.receipts),
                                    json.dumps([
                                        asdict(span)
                                        for span in passage.spans
                                    ]),
                                    passage.text,
                                    passage.text_sha256,
                                ))
                        with cursor.copy(
                            """COPY canonical_passage_actors(
                                   tenant_id,source_id,passage_id,
                                   actor_id,relation
                               ) FROM STDIN"""
                        ) as copy:
                            for passage in prepared.passages:
                                for link in passage.actor_links:
                                    copy.write_row((
                                        passage.tenant_id,
                                        passage.source_id,
                                        passage.passage_id,
                                        link.actor_id,
                                        link.relation,
                                    ))
                    connection.execute(
                        """INSERT INTO canonical_passage_embeddings(
                               tenant_id,source_id,passage_id,model,
                               dimensions,content_sha256,
                               runtime_fingerprint,embedding,embedded_at
                           )
                           SELECT passage.tenant_id,passage.source_id,
                                  passage.passage_id,reusable.model,
                                  reusable.dimensions,
                                  reusable.content_sha256,
                                  reusable.runtime_fingerprint,
                                  reusable.embedding,reusable.embedded_at
                             FROM canonical_passages passage
                             JOIN LATERAL (
                                   SELECT cached.model,cached.dimensions,
                                          cached.content_sha256,
                                          cached.runtime_fingerprint,
                                          cached.embedding,cached.embedded_at
                                     FROM
                                          recall_reusable_passage_embeddings
                                          cached
                                    WHERE cached.content_sha256=
                                          passage.text_sha256
                                    ORDER BY cached.runtime_fingerprint
                                    LIMIT 1
                             ) reusable ON true
                            WHERE passage.tenant_id=%s
                              AND passage.source_id=%s
                              AND passage.logical_document_id=%s
                              AND passage.revision=%s
                              AND passage.policy_fingerprint=%s
                           ON CONFLICT(tenant_id,source_id,passage_id)
                           DO NOTHING""",
                        (
                            candidate.tenant_id,
                            candidate.source_id,
                            candidate.logical_document_id,
                            candidate.revision,
                            self.policy.fingerprint,
                        ),
                    )
                deleted = connection.execute(
                    """DELETE FROM canonical_passage_projection_queue
                        WHERE tenant_id=%s AND source_id=%s
                          AND logical_document_id=%s AND generation=%s""",
                    (
                        candidate.tenant_id,
                        candidate.source_id,
                        candidate.logical_document_id,
                        candidate.generation,
                    ),
                )
                if deleted.rowcount != 1:
                    raise LogicalEvidenceError("passage_queue_conflict")
                connection.execute(
                    """INSERT INTO canonical_parquet_scan_queue(
                           tenant_id,source_id,bucket_start,
                           generation,reason,changed_at
                       )
                       SELECT document.tenant_id,document.source_id,
                              month.value::date,1,'logical-update',clock_timestamp()
                         FROM canonical_evidence_documents document
                         CROSS JOIN LATERAL generate_series(
                             date_trunc('month',document.first_occurred_at),
                             date_trunc('month',document.last_occurred_at),
                             interval '1 month'
                         ) month(value)
                        WHERE document.tenant_id=%s
                          AND document.source_id=%s
                          AND document.logical_document_id=%s
                       ON CONFLICT(tenant_id,source_id,bucket_start)
                       DO UPDATE SET
                           generation=canonical_parquet_scan_queue.generation+1,
                           reason='logical-update',changed_at=clock_timestamp()""",
                    (
                        candidate.tenant_id,
                        candidate.source_id,
                        candidate.logical_document_id,
                    ),
                )
        return "committed"

    def project_pending(
        self,
        *,
        tenant_id: str | None = None,
        batch_size: int = 25,
        max_batches: int = 10,
        concurrency: int = 2,
    ) -> dict[str, int | str]:
        tenant_id = self._tenant(tenant_id)
        if (
            isinstance(batch_size, bool)
            or not isinstance(batch_size, int)
            or not 1 <= batch_size <= MAX_PASSAGE_PROJECTION_BATCH
            or isinstance(max_batches, bool)
            or not isinstance(max_batches, int)
            or not 1 <= max_batches <= 100
            or isinstance(concurrency, bool)
            or not isinstance(concurrency, int)
            or not 1 <= concurrency <= 32
        ):
            raise ValueError("passage projection budget is invalid")
        prepare_pool = getattr(self.store, "prepare_pool", None)
        if callable(prepare_pool):
            prepare_pool(min(PASSAGE_POOL_WARM_SIZE, concurrency))
        started = time.monotonic()
        documents = passages = stale = requeued = batches = 0
        while batches < max_batches:
            candidates = self._pending(
                tenant_id=tenant_id,
                limit=batch_size,
            )
            if not candidates:
                break
            with ThreadPoolExecutor(
                max_workers=min(concurrency, len(candidates)),
                thread_name_prefix="recall-passage-projector",
            ) as executor:
                futures = [
                    (candidate, executor.submit(self._prepare, candidate))
                    for candidate in candidates
                ]
                prepared_documents = []
                requeued_in_batch = 0
                for candidate, future in futures:
                    try:
                        prepared_documents.append(future.result())
                    except LogicalEvidenceError as error:
                        if str(error) != "logical_evidence_not_found":
                            raise
                        requeued_in_batch += self._requeue_missing(candidate)
            statuses: list[str] = []
            if prepared_documents:
                with ThreadPoolExecutor(
                    max_workers=min(
                        concurrency,
                        len(prepared_documents),
                        max(1, self.store.pool_max_size - 1),
                        PASSAGE_COMMIT_WORKERS,
                    ),
                    thread_name_prefix="recall-passage-commit",
                ) as executor:
                    statuses = list(
                        executor.map(self._commit, prepared_documents)
                    )
            for prepared, status in zip(
                prepared_documents,
                statuses,
                strict=True,
            ):
                if status == "stale":
                    stale += 1
                    continue
                documents += 1
                passages += len(prepared.passages)
            requeued += requeued_in_batch
            batches += 1
            if requeued_in_batch:
                # Yield to the logical projector in the unified worker. The
                # passage queue remains authoritative and retries next cycle.
                break
        with self.store.connect() as connection:
            pending = connection.execute(
                """SELECT count(*) AS count
                     FROM canonical_passage_projection_queue
                    WHERE (%s::text IS NULL OR tenant_id=%s)""",
                (tenant_id, tenant_id),
            ).fetchone()["count"]
        elapsed_seconds = max(0.001, time.monotonic() - started)
        return {
            "status": "complete" if int(pending) == 0 else "pending",
            "documents": documents,
            "passages": passages,
            "stale": stale,
            "requeued": requeued,
            "batches": batches,
            "pending": int(pending),
            "elapsed_seconds": round(elapsed_seconds, 3),
            "documents_per_second": round(
                documents / elapsed_seconds,
                3,
            ),
            "passages_per_second": round(
                passages / elapsed_seconds,
                3,
            ),
        }

    def embed_pending(
        self,
        *,
        tenant_id: str | None = None,
        batch_size: int = 100,
        max_batches: int = 10,
        shard_count: int = 1,
        shard_index: int = 0,
    ) -> dict[str, int | str]:
        """Embed only lossless passages missing the selected runtime fingerprint."""

        tenant_id = self._tenant(tenant_id)
        runtime = self.store.semantic_runtime
        if runtime is None:
            return {"status": "disabled", "processed": 0, "batches": 0}
        if runtime.dimensions != 512:
            raise ValueError("passage embeddings require 512 dimensions")
        if (
            isinstance(batch_size, bool)
            or not isinstance(batch_size, int)
            or not 1 <= batch_size <= MAX_PASSAGE_EMBEDDING_BATCH
            or isinstance(max_batches, bool)
            or not isinstance(max_batches, int)
            or not 1 <= max_batches <= 100
            or isinstance(shard_count, bool)
            or not isinstance(shard_count, int)
            or not 1 <= shard_count <= 64
            or isinstance(shard_index, bool)
            or not isinstance(shard_index, int)
            or not 0 <= shard_index < shard_count
        ):
            raise ValueError("passage embedding budget is invalid")
        processed = batches = 0
        tenant_scope = tenant_id or ""
        lock_name = f"recall:lossless-passage-embeddings:{tenant_scope}"
        if shard_count > 1:
            lock_name += f":shard:{shard_index}:{shard_count}"
        with self.store.connect() as connection:
            locked = connection.execute(
                "SELECT pg_try_advisory_lock(hashtextextended(%s,0)) AS value",
                (lock_name,),
            ).fetchone()["value"]
            connection.commit()
            if not locked:
                return {"status": "busy", "processed": 0, "batches": 0}
            try:
                while batches < max_batches:
                    rows = connection.execute(
                        """SELECT passage.tenant_id,passage.source_id,
                                  passage.passage_id,passage.text_redacted,
                                  passage.text_sha256
                             FROM canonical_passages passage
                             JOIN canonical_passage_documents document
                               USING(
                                   tenant_id,source_id,logical_document_id,
                                   revision,policy_fingerprint
                               )
                             LEFT JOIN canonical_passage_embeddings embedding
                               ON embedding.tenant_id=passage.tenant_id
                              AND embedding.source_id=passage.source_id
                              AND embedding.passage_id=passage.passage_id
                              AND embedding.runtime_fingerprint=%s
                              AND embedding.content_sha256=passage.text_sha256
                            WHERE document.policy_fingerprint=%s
                              AND (%s::text='' OR passage.tenant_id=%s)
                              AND (
                                  (
                                      hashtextextended(passage.passage_id,0)
                                      & 9223372036854775807
                                  ) %% %s
                              )=%s
                              AND embedding.passage_id IS NULL
                            ORDER BY passage.tenant_id,passage.source_id,
                                     passage.passage_id
                            LIMIT %s""",
                        (
                            runtime.passage_fingerprint,
                            self.policy.fingerprint,
                            tenant_scope,
                            tenant_scope,
                            shard_count,
                            shard_index,
                            batch_size,
                        ),
                    ).fetchall()
                    connection.commit()
                    if not rows:
                        break
                    vectors = runtime.embed_passages(
                        [row["text_redacted"] for row in rows]
                    )
                    with connection.transaction():
                        with connection.cursor() as cursor:
                            cursor.executemany(
                                """INSERT INTO canonical_passage_embeddings(
                                       tenant_id,source_id,passage_id,model,
                                       dimensions,content_sha256,
                                       runtime_fingerprint,embedding
                                   ) VALUES (%s,%s,%s,%s,512,%s,%s,%s::halfvec)
                                   ON CONFLICT(
                                       tenant_id,source_id,passage_id
                                   )
                                   DO UPDATE SET
                                       model=excluded.model,
                                       dimensions=excluded.dimensions,
                                       content_sha256=excluded.content_sha256,
                                       runtime_fingerprint=
                                           excluded.runtime_fingerprint,
                                       embedding=excluded.embedding,
                                       embedded_at=now()""",
                                [
                                    (
                                        row["tenant_id"],
                                        row["source_id"],
                                        row["passage_id"],
                                        runtime.model,
                                        row["text_sha256"],
                                        runtime.passage_fingerprint,
                                        vector,
                                    )
                                    for row, vector in zip(
                                        rows,
                                        vectors,
                                        strict=True,
                                    )
                                ],
                            )
                    processed += len(rows)
                    batches += 1
                pending = connection.execute(
                    """SELECT EXISTS(
                           SELECT 1
                             FROM canonical_passages passage
                             JOIN canonical_passage_documents document
                               USING(
                                   tenant_id,source_id,logical_document_id,
                                   revision,policy_fingerprint
                               )
                             LEFT JOIN canonical_passage_embeddings embedding
                               ON embedding.tenant_id=passage.tenant_id
                              AND embedding.source_id=passage.source_id
                              AND embedding.passage_id=passage.passage_id
                              AND embedding.runtime_fingerprint=%s
                              AND embedding.content_sha256=passage.text_sha256
                            WHERE document.policy_fingerprint=%s
                              AND (%s::text='' OR passage.tenant_id=%s)
                              AND (
                                  (
                                      hashtextextended(passage.passage_id,0)
                                      & 9223372036854775807
                                  ) %% %s
                              )=%s
                              AND embedding.passage_id IS NULL
                       ) AS value""",
                    (
                        runtime.passage_fingerprint,
                        self.policy.fingerprint,
                        tenant_scope,
                        tenant_scope,
                        shard_count,
                        shard_index,
                    ),
                ).fetchone()["value"]
                connection.commit()
                return {
                    "status": "pending" if pending else "complete",
                    "processed": processed,
                    "batches": batches,
                }
            finally:
                connection.execute(
                    "SELECT pg_advisory_unlock(hashtextextended(%s,0))",
                    (lock_name,),
                )
                connection.commit()
