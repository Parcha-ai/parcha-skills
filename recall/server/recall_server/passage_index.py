"""Projection of complete logical documents into lossless retrieval passages."""

from __future__ import annotations

import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from io import BytesIO
from typing import Any, Iterable, Iterator

from .logical_evidence import (
    LogicalEvidenceError,
    LogicalEvidenceProjectionStore,
)
from .passage_projection import (
    LosslessPassage,
    PassagePolicy,
    build_passages,
    canonical_spans_json,
    decode_logical_record,
    visible_messages,
)

MAX_PASSAGE_PROJECTION_BATCH = 1_000
MAX_PASSAGE_EMBEDDING_BATCH = 5_000
PASSAGE_POOL_WARM_SIZE = 4
PASSAGE_COMMIT_WORKERS = 8
# Retained passages whose ordinal moved are parked above every real ordinal
# before they take their final position, so the (document, policy, ordinal)
# unique index never sees a transient duplicate. ``ordinal`` is a CHECK >= 0
# integer; no document has 2**30 passages.
PASSAGE_ORDINAL_PARK_OFFSET = 1 << 30


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


@dataclass(frozen=True)
class PassageDiff:
    """Differential plan for one document's passage rows.

    ``to_insert`` are new passages (ids absent from the table), ``to_delete``
    are existing ids the new build no longer produces (shifted or edited
    windows, or a superseded policy), ``retained`` are ids present on both
    sides. ``moved`` is the subset of retained ids whose ordinal changed, with
    their new ordinal; ``revision_stale`` are retained ids whose stored
    revision differs from the candidate revision.
    """

    to_insert: tuple[LosslessPassage, ...]
    to_delete: tuple[str, ...]
    retained: tuple[str, ...]
    moved: tuple[tuple[str, int], ...]
    revision_stale: tuple[str, ...]

    @property
    def counters(self) -> dict[str, int]:
        return {
            "inserted": len(self.to_insert),
            "deleted": len(self.to_delete),
            "retained": len(self.retained),
        }


def classify_passages(
    existing: Iterable[dict[str, Any]],
    passages: tuple[LosslessPassage, ...],
    *,
    revision: int,
) -> PassageDiff:
    """Split the new passage set against the stored rows of one document.

    ``existing`` rows carry ``passage_id``, ``ordinal`` and ``revision`` (any
    policy fingerprint: rows of a superseded policy have different ids and are
    deleted). Identity is the stable passage id, so a retained row has the
    same text and spans by construction and its ``text_redacted`` is never
    rewritten.
    """

    stored: dict[str, dict[str, Any]] = {}
    for row in existing:
        passage_id = row["passage_id"]
        if passage_id in stored:
            raise LogicalEvidenceError("passage_rows_duplicate")
        stored[passage_id] = row
    new_ids = {passage.passage_id for passage in passages}
    if len(new_ids) != len(passages):
        raise LogicalEvidenceError("passage_identity_collision")
    to_insert = tuple(
        passage for passage in passages if passage.passage_id not in stored
    )
    retained = tuple(
        passage.passage_id for passage in passages if passage.passage_id in stored
    )
    moved = tuple(
        (passage.passage_id, passage.ordinal)
        for passage in passages
        if passage.passage_id in stored
        and int(stored[passage.passage_id]["ordinal"]) != passage.ordinal
    )
    revision_stale = tuple(
        passage_id
        for passage_id in retained
        if int(stored[passage_id]["revision"]) != revision
    )
    to_delete = tuple(
        passage_id for passage_id in stored if passage_id not in new_ids
    )
    return PassageDiff(
        to_insert=to_insert,
        to_delete=to_delete,
        retained=retained,
        moved=moved,
        revision_stale=revision_stale,
    )


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

    def _prepare(
        self,
        candidate: PassageCandidate,
        *,
        policy: PassagePolicy | None = None,
    ) -> PreparedPassageDocument:
        policy = self.policy if policy is None else policy
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
                policy=policy,
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

    def _commit(self, prepared: PreparedPassageDocument) -> dict[str, Any]:
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
                    return {"status": "stale"}
                existing = connection.execute(
                    """SELECT passage_id,ordinal,revision
                         FROM canonical_passages
                        WHERE tenant_id=%s AND source_id=%s
                          AND logical_document_id=%s
                        FOR UPDATE""",
                    (
                        candidate.tenant_id,
                        candidate.source_id,
                        candidate.logical_document_id,
                    ),
                ).fetchall()
                diff = classify_passages(
                    existing,
                    prepared.passages,
                    revision=candidate.revision,
                )
                # Write order (the (document, policy, ordinal) unique index
                # is not deferrable):
                #   1. capture reusable embeddings for the texts about to be
                #      inserted, before the rows that hold them are deleted;
                #   2. DELETE to_delete (frees their ordinals; cascades their
                #      actors and embeddings only);
                #   3. UPSERT the pointer row in place;
                #   4. park moved retained rows above every real ordinal,
                #      then set their final ordinal + revision (two phases,
                #      so a moved row never lands on an ordinal another
                #      retained row still holds);
                #   5. bump revision on the other retained rows (HOT update:
                #      no indexed column changes, text_redacted untouched);
                #   6. COPY to_insert passages + actors into the freed
                #      ordinals;
                #   7. re-attach embeddings for to_insert by content hash.
                if diff.to_insert:
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
                              AND passage.logical_document_id=%s
                              AND passage.text_sha256=ANY(%s::text[])""",
                        (
                            candidate.tenant_id,
                            candidate.source_id,
                            candidate.logical_document_id,
                            sorted({
                                passage.text_sha256
                                for passage in diff.to_insert
                            }),
                        ),
                    )
                if diff.to_delete:
                    connection.execute(
                        """DELETE FROM canonical_passages
                            WHERE tenant_id=%s AND source_id=%s
                              AND logical_document_id=%s
                              AND passage_id=ANY(%s::text[])""",
                        (
                            candidate.tenant_id,
                            candidate.source_id,
                            candidate.logical_document_id,
                            list(diff.to_delete),
                        ),
                    )
                connection.execute(
                    """INSERT INTO canonical_passage_documents(
                           tenant_id,source_id,logical_document_id,revision,
                           policy_fingerprint,target_tokens,overlap_tokens,
                           source_document_sha256,dense_message_count,
                           dense_message_bytes,passage_count
                       ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                       ON CONFLICT(tenant_id,source_id,logical_document_id)
                       DO UPDATE SET
                           revision=excluded.revision,
                           policy_fingerprint=excluded.policy_fingerprint,
                           target_tokens=excluded.target_tokens,
                           overlap_tokens=excluded.overlap_tokens,
                           source_document_sha256=
                               excluded.source_document_sha256,
                           dense_message_count=excluded.dense_message_count,
                           dense_message_bytes=excluded.dense_message_bytes,
                           passage_count=excluded.passage_count""",
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
                if diff.moved:
                    moved_ids = [passage_id for passage_id, _ in diff.moved]
                    connection.execute(
                        """UPDATE canonical_passages
                              SET ordinal=ordinal+%s
                            WHERE tenant_id=%s AND source_id=%s
                              AND passage_id=ANY(%s::text[])""",
                        (
                            PASSAGE_ORDINAL_PARK_OFFSET,
                            candidate.tenant_id,
                            candidate.source_id,
                            moved_ids,
                        ),
                    )
                    connection.execute(
                        """UPDATE canonical_passages passage
                              SET ordinal=moved.ordinal,revision=%s
                             FROM unnest(%s::text[],%s::int[])
                                  AS moved(passage_id,ordinal)
                            WHERE passage.tenant_id=%s
                              AND passage.source_id=%s
                              AND passage.passage_id=moved.passage_id""",
                        (
                            candidate.revision,
                            moved_ids,
                            [ordinal for _, ordinal in diff.moved],
                            candidate.tenant_id,
                            candidate.source_id,
                        ),
                    )
                moved_ids = {passage_id for passage_id, _ in diff.moved}
                revision_stale = [
                    passage_id
                    for passage_id in diff.revision_stale
                    if passage_id not in moved_ids
                ]
                if revision_stale:
                    connection.execute(
                        """UPDATE canonical_passages
                              SET revision=%s
                            WHERE tenant_id=%s AND source_id=%s
                              AND passage_id=ANY(%s::text[])""",
                        (
                            candidate.revision,
                            candidate.tenant_id,
                            candidate.source_id,
                            revision_stale,
                        ),
                    )
                if diff.to_insert:
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
                            for passage in diff.to_insert:
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
                                    canonical_spans_json(passage.spans),
                                    passage.text,
                                    passage.text_sha256,
                                ))
                        with cursor.copy(
                            """COPY canonical_passage_actors(
                                   tenant_id,source_id,passage_id,
                                   actor_id,relation
                               ) FROM STDIN"""
                        ) as copy:
                            for passage in diff.to_insert:
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
                              AND passage.passage_id=ANY(%s::text[])
                           ON CONFLICT(tenant_id,source_id,passage_id)
                           DO NOTHING""",
                        (
                            candidate.tenant_id,
                            candidate.source_id,
                            [passage.passage_id for passage in diff.to_insert],
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
        return {"status": "committed", **diff.counters}

    def shadow_diff(
        self,
        *,
        tenant_id: str,
        source_id: str,
        limit: int = 50,
    ) -> dict[str, Any]:
        """Read-only parity gate: recompute passages and compare with the rows.

        For up to ``limit`` passage documents of one source whose projection
        is current (pointer revision and source hash match the evidence
        catalog), rebuild the passage set from the archived logical document
        with the stored policy and the current id function. Reports, per
        document and in total, how many stored rows exist, how many were
        recomputed, how many ids are shared, and whether the multiset of
        receipts covered is identical. Nothing is written. The output carries
        ids and counts only, never passage text.
        """

        tenant_id = self._tenant(tenant_id)
        if (
            not isinstance(tenant_id, str)
            or not tenant_id
            or not isinstance(source_id, str)
            or not source_id
            or isinstance(limit, bool)
            or not isinstance(limit, int)
            or not 1 <= limit <= MAX_PASSAGE_PROJECTION_BATCH
        ):
            raise ValueError("passage shadow diff scope is invalid")
        with self.store.connect() as connection:
            rows = connection.execute(
                """SELECT projected.tenant_id,projected.source_id,
                          projected.logical_document_id,projected.revision,
                          projected.policy_fingerprint,projected.target_tokens,
                          projected.overlap_tokens,
                          projected.source_document_sha256,
                          projected.passage_count,
                          evidence.revision AS evidence_revision,
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
                             FROM canonical_passage_documents sampled
                            WHERE sampled.tenant_id=%s AND sampled.source_id=%s
                            ORDER BY sampled.created_at DESC,
                                     sampled.logical_document_id
                            LIMIT %s
                     ) projected
                     JOIN canonical_evidence_documents evidence
                       ON evidence.tenant_id=projected.tenant_id
                      AND evidence.source_id=projected.source_id
                      AND evidence.logical_document_id
                          =projected.logical_document_id
                     JOIN canonical_evidence_document_parts part
                       ON part.tenant_id=evidence.tenant_id
                      AND part.source_id=evidence.source_id
                      AND part.logical_document_id
                          =evidence.logical_document_id
                      AND part.revision=evidence.revision
                    ORDER BY projected.logical_document_id,part.part_ordinal""",
                (tenant_id, source_id, limit),
            ).fetchall()
            grouped: dict[str, list[dict[str, Any]]] = {}
            for row in rows:
                grouped.setdefault(row["logical_document_id"], []).append(row)
            existing_rows = connection.execute(
                """SELECT logical_document_id,passage_id,receipts
                     FROM canonical_passages
                    WHERE tenant_id=%s AND source_id=%s
                      AND logical_document_id=ANY(%s::text[])""",
                (tenant_id, source_id, sorted(grouped)),
            ).fetchall()
        existing: dict[str, dict[str, tuple[str, ...]]] = {}
        for row in existing_rows:
            existing.setdefault(row["logical_document_id"], {})[
                row["passage_id"]
            ] = tuple(row["receipts"])
        documents = []
        totals = {
            "documents": 0,
            "documents_stale": 0,
            "documents_policy_mismatch": 0,
            "passages_existing": 0,
            "passages_recomputed": 0,
            "ids_shared": 0,
            "receipt_set_equal": 0,
        }
        for logical_document_id in sorted(grouped):
            values = grouped[logical_document_id]
            first = values[0]
            stored = existing.get(logical_document_id, {})
            report: dict[str, Any] = {
                "logical_document_id": logical_document_id,
                "revision": int(first["revision"]),
                "passages_existing": len(stored),
            }
            if (
                int(first["evidence_revision"]) != int(first["revision"])
                or first["document_content_sha256"]
                != first["source_document_sha256"]
            ):
                # The stored projection lags the evidence catalog; the queue
                # will catch it up. Comparing would measure the append, not
                # the id function.
                report["status"] = "stale"
                totals["documents_stale"] += 1
                documents.append(report)
                continue
            policy = PassagePolicy(
                target_tokens=int(first["target_tokens"]),
                overlap_tokens=int(first["overlap_tokens"]),
            )
            if policy.fingerprint != str(first["policy_fingerprint"]).strip():
                report["status"] = "policy_mismatch"
                totals["documents_policy_mismatch"] += 1
                documents.append(report)
                continue
            candidate = PassageCandidate(
                tenant_id=first["tenant_id"],
                source_id=first["source_id"],
                logical_document_id=logical_document_id,
                revision=int(first["revision"]),
                generation=0,
                changed_at=first["manifest_created_at"],
                source_document_sha256=first["document_content_sha256"],
                manifest_reference=self._reference(first, prefix="manifest_"),
                part_references=tuple(
                    self._reference(row, prefix="part_") for row in values
                ),
            )
            prepared = self._prepare(candidate, policy=policy)
            recomputed = {
                passage.passage_id: passage.receipts
                for passage in prepared.passages
            }
            shared = stored.keys() & recomputed.keys()
            existing_receipts = Counter(
                receipt
                for receipts in stored.values()
                for receipt in receipts
            )
            recomputed_receipts = Counter(
                receipt
                for receipts in recomputed.values()
                for receipt in receipts
            )
            receipt_set_equal = existing_receipts == recomputed_receipts
            report.update({
                "status": "compared",
                "passages_recomputed": len(recomputed),
                "ids_shared": len(shared),
                "receipt_set_equal": receipt_set_equal,
            })
            totals["documents"] += 1
            totals["passages_existing"] += len(stored)
            totals["passages_recomputed"] += len(recomputed)
            totals["ids_shared"] += len(shared)
            totals["receipt_set_equal"] += int(receipt_set_equal)
            documents.append(report)
        return {
            "status": "ok",
            "tenant_id": tenant_id,
            "source_id": source_id,
            "limit": limit,
            "read_only": True,
            "totals": totals,
            "receipt_parity": (
                totals["receipt_set_equal"] == totals["documents"]
            ),
            "documents": documents,
        }

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
        documents = passages = stale = requeued = unavailable = batches = 0
        deleted = retained = 0
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
                unavailable_in_batch = 0
                for candidate, future in futures:
                    try:
                        prepared_documents.append(future.result())
                    except LogicalEvidenceError as error:
                        if str(error) == "logical_evidence_not_found":
                            requeued_in_batch += self._requeue_missing(candidate)
                        elif str(error) == "logical_evidence_unavailable":
                            unavailable_in_batch += 1
                        else:
                            raise
            statuses: list[dict[str, Any]] = []
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
                if status["status"] == "stale":
                    stale += 1
                    continue
                documents += 1
                # ``passages`` counts rows written (inserted), which is what
                # the churn probe and the worker's idle check consume.
                passages += int(status["inserted"])
                deleted += int(status["deleted"])
                retained += int(status["retained"])
            requeued += requeued_in_batch
            unavailable += unavailable_in_batch
            batches += 1
            if requeued_in_batch or unavailable_in_batch:
                # Yield to the logical projector in the unified worker. The
                # passage queue remains authoritative and retries next cycle.
                # A transient archive failure must never become a destructive
                # logical-document rebuild.
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
            "inserted": passages,
            "deleted": deleted,
            "retained": retained,
            "stale": stale,
            "requeued": requeued,
            "unavailable": unavailable,
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
                                   policy_fingerprint
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
                                   policy_fingerprint
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
