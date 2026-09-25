"""Projection of complete logical documents into lossless retrieval passages."""

from __future__ import annotations

import time
import json
import math
import threading
from collections import Counter
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass
from datetime import datetime
from io import BytesIO
from typing import Any, Callable, Iterable, Iterator

import psycopg

from .logical_evidence import (
    LogicalEvidenceError,
    LogicalEvidenceProjectionStore,
)
from .passage_projection import (
    MAX_PASSAGE_HEADER_BYTES,
    LOGICAL_DOCUMENT_ID_RE,
    PASSAGE_EMBEDDING_SEPARATOR,
    LosslessPassage,
    PassagePolicy,
    build_passages,
    canonical_spans_json,
    decode_logical_record,
    passage_embed_sha256,
    passage_embedding_input,
    render_passage_header,
    visible_messages,
)
from .passage_representations import ActorContext, DocumentContext
from .search_outbox import (
    enqueue_search_outbox,
    outbox_months,
    write_search_tombstones,
)

PROJECTION_PROGRESS_INTERVAL_SECONDS = 5.0
MAX_PASSAGE_PROJECTION_BATCH = 1_000
MAX_PASSAGE_EMBEDDING_BATCH = 5_000
MAX_PASSAGE_HEADER_BACKFILL_BATCH = 5_000
# Content-free token estimate for the embed plan: UTF-8 bytes per token.
PASSAGE_PLAN_BYTES_PER_TOKEN = 4
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


DOCUMENT_CONTEXT_SQL = """
    SELECT profile.family AS source_family,
           coalesce(aliases.values,ARRAY[]::text[]) AS source_aliases,
           session.harness,session.metadata,
           coalesce(attributed.actors,'[]'::jsonb) AS actors
      FROM canonical_evidence_documents evidence
      LEFT JOIN sessions session
        ON session.source_id=evidence.source_id
       AND session.native_id=evidence.native_parent_id
      LEFT JOIN source_profiles profile
        ON profile.source_id=evidence.source_id
      LEFT JOIN LATERAL (
          SELECT array_agg(alias ORDER BY alias) AS values
            FROM source_aliases
           WHERE source_id=evidence.source_id
      ) aliases ON true
      LEFT JOIN LATERAL (
          SELECT jsonb_agg(
                     jsonb_build_object(
                         'actor_id',person.actor_id,
                         'display_name',person.display_name,
                         'relations',person.relations,
                         'aliases',person.aliases
                     )
                     ORDER BY lower(person.display_name),person.actor_id
                 ) AS actors
            FROM (
                  SELECT actor.actor_id,actor.display_name,
                         array_agg(DISTINCT link.relation
                                   ORDER BY link.relation) AS relations,
                         coalesce(
                             array_agg(DISTINCT alias.alias
                                       ORDER BY alias.alias)
                             FILTER (WHERE alias.searchable),
                             ARRAY[]::text[]
                         ) AS aliases
                    FROM canonical_evidence_document_actors link
                    JOIN brain_actors actor
                      ON actor.tenant_id=link.tenant_id
                     AND actor.actor_id=link.actor_id
                     AND actor.active
                    LEFT JOIN brain_actor_aliases alias
                      ON alias.tenant_id=actor.tenant_id
                     AND alias.actor_id=actor.actor_id
                   WHERE link.tenant_id=evidence.tenant_id
                     AND link.source_id=evidence.source_id
                     AND link.logical_document_id=evidence.logical_document_id
                     AND link.revision=evidence.revision
                   GROUP BY actor.actor_id,actor.display_name
            ) person
      ) attributed ON true
     WHERE evidence.tenant_id=%s AND evidence.source_id=%s
       AND evidence.logical_document_id=%s
"""


def document_context_from_row(row: dict[str, Any] | None) -> DocumentContext:
    """Header inputs for one logical document from its catalog row."""

    if row is None:
        return DocumentContext()
    session_metadata = row.get("metadata") or {}
    if not isinstance(session_metadata, dict):
        session_metadata = {}
    return DocumentContext(
        source_family=row.get("source_family"),
        source_aliases=tuple(row.get("source_aliases") or ()),
        harness=row.get("harness") or session_metadata.get("harness"),
        workspace=session_metadata.get("cwd"),
        branch=session_metadata.get("branch"),
        actors=tuple(
            ActorContext(
                actor_id=value["actor_id"],
                display_name=value["display_name"],
                relations=tuple(value.get("relations") or ()),
                aliases=tuple(value.get("aliases") or ()),
            )
            for value in row.get("actors") or ()
        ),
    )


def document_context(
    connection: Any,
    *,
    tenant_id: str,
    source_id: str,
    logical_document_id: str,
) -> DocumentContext:
    row = connection.execute(
        DOCUMENT_CONTEXT_SQL,
        (tenant_id, source_id, logical_document_id),
    ).fetchone()
    return document_context_from_row(row)


def postgres_vector_plane(store: Any) -> bool:
    """True unless ``store`` reads the turbopuffer search plane (H3-e').

    On the turbopuffer plane no process reads or writes
    ``canonical_passage_embeddings`` or the embedding ledger: turbopuffer
    embeds natively, and migration 067 drops the tables. Stores without the
    attribute (unit-test fakes) are the postgres plane.
    """

    return getattr(store, "search_plane", "postgres") != "turbopuffer"


NOT_APPLICABLE_COVERAGE: dict[str, Any] = {
    "total": 0,
    "covered": 0,
    "coverage": 1.0,
    "status": "not-applicable",
    "plane": "turbopuffer",
}


def passage_contract_coverage(
    connection: Any,
    *,
    fingerprint: str,
    tenant_id: str | None = None,
    search_plane: str = "postgres",
) -> dict[str, Any]:
    """Share of live passages whose vector carries ``fingerprint`` (v2 key).

    A passage is covered when its embedding row has the given runtime
    fingerprint and its content hash equals the passage's ``embed_sha256``.
    An empty table counts as fully covered: nothing would be lost by reading
    the new contract. On the turbopuffer plane the question does not arise
    and the embeddings table is never read.
    """

    if search_plane == "turbopuffer":
        return dict(NOT_APPLICABLE_COVERAGE)
    row = connection.execute(
        """SELECT count(*) AS total,
                  count(*) FILTER (
                      WHERE embedding.runtime_fingerprint=%s
                        AND embedding.content_sha256=passage.embed_sha256
                  ) AS covered
             FROM canonical_passages passage
             JOIN canonical_passage_documents document
               USING(
                   tenant_id,source_id,logical_document_id,
                   policy_fingerprint
               )
             LEFT JOIN canonical_passage_embeddings embedding
               USING(tenant_id,source_id,passage_id)
            WHERE (%s::text IS NULL OR passage.tenant_id=%s)""",
        (fingerprint, tenant_id, tenant_id),
    ).fetchone()
    total = int(row["total"])
    covered = int(row["covered"])
    return {
        "total": total,
        "covered": covered,
        "coverage": 1.0 if total == 0 else covered / total,
    }


def passage_embed_plan(
    connection: Any,
    *,
    tenant_id: str,
    runtime: Any,
    price_per_mtoken: float = 0.0,
    search_plane: str = "postgres",
) -> dict[str, Any]:
    """Content-free, read-only report of the v2 re-embed for one tenant.

    Counts live passages, headers present/missing, vectors already under the
    v2 and v1 fingerprints, the rows a v2 pass would embed, and a byte-based
    token estimate (headers still missing are budgeted at the header cap).
    Nothing is written and no passage text leaves the database. On the
    turbopuffer plane nothing is pending and nothing is read.
    """

    if (
        not isinstance(tenant_id, str)
        or not tenant_id
        or isinstance(price_per_mtoken, bool)
        or not isinstance(price_per_mtoken, (int, float))
        or not 0 <= price_per_mtoken <= 1_000
    ):
        raise ValueError("passage embed plan scope is invalid")
    if search_plane == "turbopuffer":
        return {
            "status": "not-applicable",
            "read_only": True,
            "tenant_id": tenant_id,
            "plane": "turbopuffer",
            "contract": "v2",
            "runtime_configured": runtime is not None,
            "needs_embedding": 0,
            "estimated_tokens": 0,
            "price_per_mtoken": float(price_per_mtoken),
            "estimated_cost": 0.0,
        }
    fingerprint_v2 = getattr(runtime, "passage_fingerprint_v2", None)
    fingerprint_v1 = getattr(runtime, "passage_fingerprint_v1", None)
    if runtime is not None and (fingerprint_v2 is None or fingerprint_v1 is None):
        fingerprint_v2 = fingerprint_v1 = getattr(
            runtime, "passage_fingerprint", None
        )
    row = connection.execute(
        """SELECT count(*) AS passages,
                  count(passage.header_redacted) AS headers_present,
                  count(*) FILTER (
                      WHERE %s::text IS NOT NULL
                        AND embedding.runtime_fingerprint=%s
                        AND embedding.content_sha256=passage.embed_sha256
                  ) AS embedded_v2,
                  count(*) FILTER (
                      WHERE %s::text IS NOT NULL
                        AND embedding.runtime_fingerprint=%s
                        AND embedding.content_sha256=passage.text_sha256
                  ) AS embedded_v1,
                  coalesce(sum(
                      octet_length(passage.text_redacted)
                      +coalesce(octet_length(passage.header_redacted),%s)
                      +2
                  ) FILTER (
                      WHERE %s::text IS NULL
                         OR embedding.runtime_fingerprint IS DISTINCT FROM %s
                         OR embedding.content_sha256
                            IS DISTINCT FROM passage.embed_sha256
                  ),0) AS pending_bytes
             FROM canonical_passages passage
             JOIN canonical_passage_documents document
               USING(
                   tenant_id,source_id,logical_document_id,
                   policy_fingerprint
               )
             LEFT JOIN canonical_passage_embeddings embedding
               USING(tenant_id,source_id,passage_id)
            WHERE passage.tenant_id=%s""",
        (
            fingerprint_v2,
            fingerprint_v2,
            fingerprint_v1,
            fingerprint_v1,
            MAX_PASSAGE_HEADER_BYTES,
            fingerprint_v2,
            fingerprint_v2,
            tenant_id,
        ),
    ).fetchone()
    passages = int(row["passages"])
    embedded_v2 = int(row["embedded_v2"])
    pending_bytes = int(row["pending_bytes"])
    estimated_tokens = -(-pending_bytes // PASSAGE_PLAN_BYTES_PER_TOKEN)
    return {
        "status": "ok",
        "read_only": True,
        "tenant_id": tenant_id,
        "contract": "v2",
        "runtime_configured": runtime is not None,
        "passages": passages,
        "headers_present": int(row["headers_present"]),
        "headers_missing": passages - int(row["headers_present"]),
        "embedded_v2": embedded_v2,
        "embedded_v1": int(row["embedded_v1"]),
        "needs_embedding": passages - embedded_v2,
        "coverage_v2": 1.0 if passages == 0 else round(embedded_v2 / passages, 6),
        "estimated_tokens": estimated_tokens,
        "bytes_per_token": PASSAGE_PLAN_BYTES_PER_TOKEN,
        "price_per_mtoken": float(price_per_mtoken),
        "estimated_cost": round(
            estimated_tokens / 1_000_000 * float(price_per_mtoken),
            4,
        ),
    }


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
        self._prefer_notification_admission = False
        # Separate history/recent turns; fixed-size process-local state. Advance
        # on admission, so an unavailable source cannot retain the first turn.
        self._ordinary_source_cursor: dict[bool, tuple[str, str] | None] = {
            False: None, True: None,
        }
        runtime = getattr(store, "semantic_runtime", None)
        bind = getattr(runtime, "bind_passage_coverage_probe", None)
        if callable(bind) and not getattr(
            runtime, "has_passage_coverage_probe", True
        ):
            bind(self._coverage_probe)

    @property
    def search_plane(self) -> str:
        return "postgres" if postgres_vector_plane(self.store) else "turbopuffer"

    def _coverage_probe(self) -> float | None:
        runtime = getattr(self.store, "semantic_runtime", None)
        fingerprint = getattr(runtime, "passage_fingerprint_v2", None)
        if not isinstance(fingerprint, str) or not fingerprint:
            return None
        if not postgres_vector_plane(self.store):
            return NOT_APPLICABLE_COVERAGE["coverage"]
        with self.store.connect() as connection:
            return passage_contract_coverage(
                connection,
                fingerprint=fingerprint,
            )["coverage"]

    def contract_coverage(
        self,
        *,
        tenant_id: str | None = None,
    ) -> dict[str, Any]:
        """Read-only v2 coverage for the rollout flip (see ``semantic``)."""

        tenant_id = self._tenant(tenant_id)
        runtime = self.store.semantic_runtime
        fingerprint = getattr(runtime, "passage_fingerprint_v2", None)
        if not isinstance(fingerprint, str) or not fingerprint:
            fingerprint = getattr(runtime, "passage_fingerprint", "")
        if not postgres_vector_plane(self.store):
            return dict(NOT_APPLICABLE_COVERAGE)
        with self.store.connect() as connection:
            return passage_contract_coverage(
                connection,
                fingerprint=fingerprint,
                tenant_id=tenant_id,
            )

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
        recent = self._prefer_notification_admission
        cursor = self._ordinary_source_cursor[recent]
        after_tenant, after_source = cursor if cursor is not None else (None, None)
        with self.store.connect() as connection:
            rows = connection.execute(
                """WITH eligible AS MATERIALIZED (
                       SELECT candidate_queue.*,
                              CASE WHEN %s AND notification_queued_at IS NOT NULL
                                   THEN 0 ELSE 1 END AS admission_priority,
                              CASE WHEN %s::text IS NULL OR (tenant_id,source_id)>
                                        (%s::text,%s::text) THEN 0 ELSE 1 END AS source_wrap
                         FROM canonical_passage_projection_queue candidate_queue
                        WHERE (%s::text IS NULL OR tenant_id=%s)
                   ), ranked AS (
                       SELECT eligible.*,
                              row_number() OVER (
                                  PARTITION BY tenant_id,source_id,admission_priority
                                  ORDER BY CASE WHEN %s THEN changed_at END DESC,
                                           changed_at,logical_document_id
                              ) AS source_position,
                              count(*) OVER (
                                  PARTITION BY tenant_id,source_id,admission_priority
                              ) AS source_backlog
                         FROM eligible
                   ), admitted AS MATERIALIZED (
                       SELECT * FROM ranked
                        ORDER BY admission_priority,
                                 CASE WHEN admission_priority=0
                                      THEN notification_queued_at END,
                                 CASE WHEN source_position=1 THEN 0 ELSE 1 END,
                                 CASE WHEN source_position=1 THEN source_wrap END,
                                 CASE WHEN source_position=1 THEN tenant_id END,
                                 CASE WHEN source_position=1 THEN source_id END,
                                 CASE WHEN source_position>1
                                      THEN source_position::numeric/source_backlog END,
                                 changed_at,tenant_id,source_id,logical_document_id
                        LIMIT %s
                   ), queue AS (
                       SELECT admitted.*,
                              dense_rank() OVER (
                                  ORDER BY source_wrap,tenant_id,source_id
                              ) AS source_turn
                         FROM admitted
                   )
                   SELECT queue.tenant_id,queue.source_id,
                          queue.logical_document_id,queue.revision,
                          queue.generation,queue.changed_at,
                          queue.admission_priority,queue.source_turn,
                          CASE WHEN queue.admission_priority=0
                               THEN queue.notification_queued_at END AS notification_priority,
                          (queue.changed_at<clock_timestamp()-interval '5 minutes') AS aged_priority,
                          coalesce(sum(part.size_bytes) OVER (
                              PARTITION BY queue.tenant_id,queue.source_id,queue.logical_document_id
                          ),0) AS estimated_bytes,
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
                     FROM queue
                     LEFT JOIN canonical_evidence_documents evidence
                       ON evidence.tenant_id=queue.tenant_id
                      AND evidence.source_id=queue.source_id
                      AND evidence.logical_document_id=queue.logical_document_id
                      AND evidence.revision=queue.revision
                     LEFT JOIN canonical_evidence_document_parts part
                       ON part.tenant_id=evidence.tenant_id
                      AND part.source_id=evidence.source_id
                      AND part.logical_document_id=evidence.logical_document_id
                      AND part.revision=evidence.revision
                    ORDER BY notification_priority ASC NULLS LAST,
                             aged_priority DESC,estimated_bytes,
                             queue.changed_at,queue.tenant_id,
                             queue.source_id,queue.logical_document_id,
                             part.part_ordinal""",
                (recent, after_tenant, after_tenant, after_source,
                 tenant_id, tenant_id, recent, limit),
            ).fetchall()
        # Hydration can fail for an admitted key. Keep its source turn moving,
        # but retain that queue row for repair; missing metadata is never ACKed.
        ordinary = [row for row in rows if row["admission_priority"] == 1]
        if ordinary:
            last = max(ordinary, key=lambda row: row["source_turn"])
            self._ordinary_source_cursor[recent] = (last["tenant_id"], last["source_id"])
        grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
        for row in rows:
            if row["manifest_artifact_id"] is None or row["part_ordinal"] is None:
                continue
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
                       reason=CASE WHEN canonical_evidence_document_queue.reason='forget'
                                   THEN 'forget' ELSE 'backfill' END,
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
                    """SELECT passage_id,ordinal,revision,
                              first_occurred_at,last_occurred_at,
                              header_redacted IS NULL AS header_missing
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
                # H2-a: one catalog read per document renders the contextual
                # header of every inserted passage and of retained rows that
                # predate headers. The header is embedding input only.
                retained_unheaded = [
                    row
                    for row in existing
                    if row["passage_id"] in set(diff.retained)
                    and bool(row.get("header_missing"))
                ]
                context = (
                    document_context(
                        connection,
                        tenant_id=candidate.tenant_id,
                        source_id=candidate.source_id,
                        logical_document_id=candidate.logical_document_id,
                    )
                    if diff.to_insert or retained_unheaded
                    else None
                )
                headers = {
                    passage.passage_id: render_passage_header(
                        context,
                        first_occurred_at=passage.first_occurred_at,
                        last_occurred_at=passage.last_occurred_at,
                    )
                    for passage in diff.to_insert
                }
                embed_hashes = {
                    passage.passage_id: passage_embed_sha256(
                        headers[passage.passage_id],
                        passage.text,
                    )
                    for passage in diff.to_insert
                }
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
                # Steps 1 and 7 exist only on the postgres plane (H3-e'):
                # turbopuffer embeds natively and migration 067 drops the
                # embeddings table, so the turbopuffer plane never touches it.
                vector_plane = postgres_vector_plane(self.store)
                if diff.to_insert and vector_plane:
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
                              AND (
                                  passage.embed_sha256=ANY(%s::text[])
                                  OR passage.text_sha256=ANY(%s::text[])
                              )""",
                        (
                            candidate.tenant_id,
                            candidate.source_id,
                            candidate.logical_document_id,
                            sorted(set(embed_hashes.values())),
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
                if retained_unheaded:
                    self._write_headers(
                        connection,
                        tenant_id=candidate.tenant_id,
                        source_id=candidate.source_id,
                        rows=retained_unheaded,
                        contexts={
                            row["passage_id"]: context
                            for row in retained_unheaded
                        },
                        enqueue=False,
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
                                   text_sha256,header_redacted,embed_sha256
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
                                    headers[passage.passage_id],
                                    embed_hashes[passage.passage_id],
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
                if diff.to_insert and vector_plane:
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
                                          passage.embed_sha256
                                       OR cached.content_sha256=
                                          passage.text_sha256
                                    ORDER BY (
                                        cached.content_sha256=
                                        passage.embed_sha256
                                    ) DESC,cached.runtime_fingerprint
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
                # H3-a: the search outbox sees only what this commit changed.
                # Deleted rows become tombstones; every month an inserted or
                # deleted passage touched is queued for the Lance writer.
                deleted_ids = set(diff.to_delete)
                deleted_rows = [
                    row for row in existing if row["passage_id"] in deleted_ids
                ]
                if deleted_rows:
                    write_search_tombstones(
                        connection,
                        tenant_id=candidate.tenant_id,
                        source_id=candidate.source_id,
                        passages=deleted_rows,
                    )
                enqueue_search_outbox(
                    connection,
                    tenant_id=candidate.tenant_id,
                    source_id=candidate.source_id,
                    months=outbox_months(
                        [
                            (passage.first_occurred_at, passage.last_occurred_at)
                            for passage in diff.to_insert
                        ]
                        + [
                            (row["first_occurred_at"], row["last_occurred_at"])
                            for row in (*deleted_rows, *retained_unheaded)
                        ]
                    ),
                    reason="logical-update",
                )
        return {"status": "committed", **diff.counters}

    @staticmethod
    def _write_headers(
        connection: Any,
        *,
        tenant_id: str,
        source_id: str,
        rows: list[dict[str, Any]],
        contexts: dict[str, DocumentContext],
        enqueue: bool = True,
    ) -> int:
        """Fill header_redacted/embed_sha256 on rows that still lack them.

        Only NULL headers are written: an existing header is never rewritten
        here, so a retained passage keeps its embedding reuse key. With
        ``enqueue`` (H3-a) the months of the rows whose header actually
        changed are queued in the search outbox as ``header-change``; the
        differential commit passes ``False`` because it queues those months
        as ``logical-update`` itself.
        """

        if not rows:
            return 0
        ids: list[str] = []
        rendered: list[str] = []
        for row in rows:
            ids.append(row["passage_id"])
            rendered.append(render_passage_header(
                contexts[row["passage_id"]],
                first_occurred_at=row["first_occurred_at"],
                last_occurred_at=row["last_occurred_at"],
            ))
        # embed_sha256 = sha256(header || "\n\n" || text_redacted), hashed in
        # the database so passage text never round-trips for a header fill.
        # Byte-identical to ``passage_embed_sha256``.
        result = connection.execute(
            """UPDATE canonical_passages passage
                  SET header_redacted=headed.header_redacted,
                      embed_sha256=encode(sha256(convert_to(
                          headed.header_redacted||%s||passage.text_redacted,
                          'UTF8'
                      )),'hex')
                 FROM unnest(%s::text[],%s::text[])
                      AS headed(passage_id,header_redacted)
                WHERE passage.tenant_id=%s AND passage.source_id=%s
                  AND passage.passage_id=headed.passage_id
                  AND passage.header_redacted IS NULL
            RETURNING passage.first_occurred_at,passage.last_occurred_at""",
            (PASSAGE_EMBEDDING_SEPARATOR, ids, rendered, tenant_id, source_id),
        )
        changed = result.fetchall()
        if enqueue and changed:
            enqueue_search_outbox(
                connection,
                tenant_id=tenant_id,
                source_id=source_id,
                months=outbox_months(
                    (row["first_occurred_at"], row["last_occurred_at"])
                    for row in changed
                ),
                reason="header-change",
            )
        return len(changed)

    def backfill_headers(
        self,
        *,
        tenant_id: str | None = None,
        batch_size: int = 500,
        max_batches: int = 10,
    ) -> dict[str, int | str]:
        """Render headers for passages projected before schema 064.

        Reads catalog rows only (never the archive): the header is a pure
        function of the document's catalog context and the passage's own
        times. Runs ahead of ``embed_pending`` under contract v2 so every
        passage has an ``embed_sha256`` before it is embedded.
        """

        tenant_id = self._tenant(tenant_id)
        if (
            isinstance(batch_size, bool)
            or not isinstance(batch_size, int)
            or not 1 <= batch_size <= MAX_PASSAGE_HEADER_BACKFILL_BATCH
            or isinstance(max_batches, bool)
            or not isinstance(max_batches, int)
            or not 1 <= max_batches <= 100
        ):
            raise ValueError("passage header backfill budget is invalid")
        updated = batches = 0
        tenant_scope = tenant_id or ""
        with self.store.connect() as connection:
            while batches < max_batches:
                rows = connection.execute(
                    """SELECT passage.tenant_id,passage.source_id,
                              passage.logical_document_id,passage.passage_id,
                              passage.first_occurred_at,
                              passage.last_occurred_at
                         FROM canonical_passages passage
                        WHERE passage.header_redacted IS NULL
                          AND (%s::text='' OR passage.tenant_id=%s)
                        ORDER BY passage.tenant_id,passage.source_id,
                                 passage.logical_document_id,passage.ordinal
                        LIMIT %s""",
                    (tenant_scope, tenant_scope, batch_size),
                ).fetchall()
                if not rows:
                    break
                grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
                for row in rows:
                    grouped.setdefault(
                        (
                            row["tenant_id"],
                            row["source_id"],
                            row["logical_document_id"],
                        ),
                        [],
                    ).append(row)
                written = 0
                with connection.transaction():
                    for (scope_tenant, scope_source, document_id), values in (
                        grouped.items()
                    ):
                        context = document_context(
                            connection,
                            tenant_id=scope_tenant,
                            source_id=scope_source,
                            logical_document_id=document_id,
                        )
                        written += self._write_headers(
                            connection,
                            tenant_id=scope_tenant,
                            source_id=scope_source,
                            rows=values,
                            contexts={
                                row["passage_id"]: context for row in values
                            },
                        )
                updated += written
                batches += 1
                if written == 0:
                    # A concurrent writer filled these rows; do not spin.
                    break
            remaining = connection.execute(
                """SELECT EXISTS(
                       SELECT 1 FROM canonical_passages
                        WHERE header_redacted IS NULL
                          AND (%s::text='' OR tenant_id=%s)
                   ) AS value""",
                (tenant_scope, tenant_scope),
            ).fetchone()["value"]
            connection.commit()
        return {
            "status": "pending" if remaining else "complete",
            "updated": updated,
            "batches": batches,
        }

    def shadow_diff(
        self,
        *,
        tenant_id: str,
        source_id: str,
        limit: int = 50,
        _empty_only: bool = False,
        _after: str | None = None,
        _concurrency: int = 1,
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
            or type(_concurrency) is not int or not 1 <= _concurrency <= 32
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
                              AND (NOT %s OR (
                                  sampled.passage_count=0
                                  AND sampled.policy_fingerprint=%s
                                  AND NOT EXISTS (
                                      SELECT 1 FROM canonical_passage_projection_queue queued
                                       WHERE queued.tenant_id=sampled.tenant_id
                                         AND queued.source_id=sampled.source_id
                                         AND queued.logical_document_id=sampled.logical_document_id
                                  )
                              ))
                              AND (%s::text IS NULL OR sampled.logical_document_id>%s)
                            ORDER BY CASE WHEN %s THEN sampled.logical_document_id END,
                                     sampled.created_at DESC,
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
                (tenant_id, source_id, _empty_only, self.policy.fingerprint,
                 _after, _after, _empty_only, limit),
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
        def compare_document(logical_document_id: str) -> dict[str, Any]:
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
                return report
            policy = PassagePolicy(
                target_tokens=int(first["target_tokens"]),
                overlap_tokens=int(first["overlap_tokens"]),
            )
            if policy.fingerprint != str(first["policy_fingerprint"]).strip():
                report["status"] = "policy_mismatch"
                return report
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
            if _empty_only:
                embedding_bytes = sum(
                    len(passage.text.encode()) + MAX_PASSAGE_HEADER_BYTES
                    + len(PASSAGE_EMBEDDING_SEPARATOR.encode())
                    for passage in prepared.passages
                )
                report.update({
                    "source_document_sha256": candidate.source_document_sha256,
                    "policy_fingerprint": policy.fingerprint,
                    "archive_bytes": candidate.manifest_reference["size_bytes"]
                    + sum(ref["size_bytes"] for ref in candidate.part_references),
                    "embedding_bytes_estimate": embedding_bytes,
                    "embedding_tokens_estimate": math.ceil(
                        embedding_bytes / PASSAGE_PLAN_BYTES_PER_TOKEN
                    ),
                })
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
            return report

        # Each task releases its prepared bodies before returning metadata.
        # Catalog leases are already closed; no queue write occurs until every
        # comparison succeeds. map preserves document-ID report order.
        document_ids = sorted(grouped)
        if _concurrency == 1 or len(document_ids) < 2:
            documents = [compare_document(doc) for doc in document_ids]
        else:
            with ThreadPoolExecutor(
                max_workers=min(_concurrency, len(document_ids)),
                thread_name_prefix="recall-passage-shadow",
            ) as executor:
                documents = list(executor.map(compare_document, document_ids))
        totals = {
            "documents": 0,
            "documents_stale": 0,
            "documents_policy_mismatch": 0,
            "passages_existing": 0,
            "passages_recomputed": 0,
            "ids_shared": 0,
            "receipt_set_equal": 0,
        }
        for report in documents:
            if report["status"] == "stale":
                totals["documents_stale"] += 1
            elif report["status"] == "policy_mismatch":
                totals["documents_policy_mismatch"] += 1
            else:
                totals["documents"] += 1
                for key in ("passages_existing", "passages_recomputed", "ids_shared", "receipt_set_equal"):
                    totals[key] += int(report[key])
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

    def repair_empty(
        self,
        *,
        tenant_id: str,
        source_id: str,
        limit: int = 25,
        after: str | None = None,
        apply: bool = False,
        price_per_mtoken: float | None = None,
        concurrency: int = 1,
    ) -> dict[str, Any]:
        """Plan one source's empty-projection repair; optionally queue that batch.

        Reads archived evidence through the existing shadow projector. Never
        embeds or executes projection work. The existing worker owns commits.
        The cursor allows finite batches without a permanent corpus ceiling.
        """
        tenant_id = self._tenant(tenant_id)
        if (
            not isinstance(tenant_id, str) or not tenant_id
            or not isinstance(source_id, str) or not source_id
            or type(limit) is not int or not 1 <= limit <= MAX_PASSAGE_PROJECTION_BATCH
            or type(apply) is not bool
            or type(concurrency) is not int or not 1 <= concurrency <= 32
            or (after is not None and (
                not isinstance(after, str) or LOGICAL_DOCUMENT_ID_RE.fullmatch(after) is None
            ))
            or (price_per_mtoken is not None and (
                type(price_per_mtoken) not in (int, float)
                or not math.isfinite(price_per_mtoken) or price_per_mtoken < 0
            ))
        ):
            raise ValueError("empty passage repair scope is invalid")
        report = self.shadow_diff(
            tenant_id=tenant_id, source_id=source_id, limit=limit,
            _empty_only=True, _after=after, _concurrency=concurrency,
        )
        documents = report["documents"]
        eligible = [doc for doc in documents
                    if doc["status"] == "compared" and doc["passages_existing"] == 0
                    and doc["passages_recomputed"] > 0]
        queued = 0
        if apply and eligible:
            with self.store.connect() as connection:
                with connection.transaction():
                    result = connection.execute(
                        """INSERT INTO canonical_passage_projection_queue(
                               tenant_id,source_id,logical_document_id,revision,
                               generation,reason,changed_at
                           )
                           SELECT evidence.tenant_id,evidence.source_id,
                                  evidence.logical_document_id,evidence.revision,
                                  1,'backfill',clock_timestamp()
                             FROM canonical_evidence_documents evidence
                             JOIN canonical_passage_documents projected
                               USING(tenant_id,source_id,logical_document_id)
                             JOIN jsonb_to_recordset(%s::jsonb) selected(
                                 logical_document_id text,revision integer,
                                 source_document_sha256 text,policy_fingerprint text
                             ) ON selected.logical_document_id=evidence.logical_document_id
                            WHERE evidence.tenant_id=%s AND evidence.source_id=%s
                              AND evidence.revision=selected.revision
                              AND evidence.document_content_sha256=selected.source_document_sha256
                              AND projected.revision=evidence.revision
                              AND projected.source_document_sha256=evidence.document_content_sha256
                              AND projected.policy_fingerprint=selected.policy_fingerprint
                              AND projected.passage_count=0
                           ON CONFLICT(tenant_id,source_id,logical_document_id)
                           DO NOTHING""",
                        (json.dumps(eligible), tenant_id, source_id),
                    )
                    queued = max(0, result.rowcount)
        tokens = sum(doc.get("embedding_tokens_estimate", 0) for doc in eligible)
        return {
            "status": "queued" if apply else "planned",
            "read_only": not apply, "tenant_id": tenant_id, "source_id": source_id,
            "limit": limit, "after": after,
            "next_after": max((doc["logical_document_id"] for doc in documents), default=None),
            "documents_examined": len(documents), "eligible_documents": len(eligible),
            "queued": queued,
            "archive_bytes": sum(doc.get("archive_bytes", 0) for doc in documents),
            "embedding_bytes_estimate": sum(doc.get("embedding_bytes_estimate", 0) for doc in eligible),
            "embedding_tokens_estimate": tokens,
            "price_per_mtoken": price_per_mtoken,
            "embedding_cost_usd_estimate": (
                None if price_per_mtoken is None else tokens / 1_000_000 * price_per_mtoken
            ),
            "estimate_basis": "UTF-8 bytes / 4; maximum context header per new passage; advisory batch estimate only",
            "documents": documents,
        }

    def project_pending(
        self,
        *,
        tenant_id: str | None = None,
        batch_size: int = 25,
        max_batches: int = 10,
        concurrency: int = 2,
        on_progress: Callable[[], None] | None = None,
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
        phase_started = time.monotonic()
        prepare_pool = getattr(self.store, "prepare_pool", None)
        if callable(prepare_pool):
            prepare_pool(min(PASSAGE_POOL_WARM_SIZE, concurrency))
        warmup_seconds = time.monotonic() - phase_started
        pending_seconds = prepare_seconds = commit_seconds = 0.0
        started = time.monotonic()
        documents = passages = stale = requeued = unavailable = batches = 0
        deleted = retained = 0
        while batches < max_batches:
            phase_started = time.monotonic()
            candidates = self._pending(
                tenant_id=tenant_id,
                limit=batch_size,
            )
            pending_seconds += time.monotonic() - phase_started
            if not candidates:
                break
            # Persist across worker callbacks; empty rounds spend no history turn.
            self._prefer_notification_admission = not self._prefer_notification_admission
            stopping = threading.Event()
            commit_slots = threading.BoundedSemaphore(min(
                concurrency, len(candidates),
                max(1, self.store.pool_max_size - 1), PASSAGE_COMMIT_WORKERS,
            ))

            def project_document(candidate):
                # The owner releases the prepared body after its own commit;
                # futures retain scalar results, never a batch of passage text.
                preparation_started = time.monotonic()
                try:
                    prepared = self._prepare(candidate)
                except LogicalEvidenceError as error:
                    if str(error) not in {"logical_evidence_not_found", "logical_evidence_unavailable"}:
                        raise
                    return {"status": str(error)}, time.monotonic() - preparation_started, 0.0
                preparation_elapsed = time.monotonic() - preparation_started
                commit_started = time.monotonic()
                with commit_slots:
                    if stopping.is_set():
                        return {"status": "cancelled"}, preparation_elapsed, 0.0
                    try:
                        status = self._commit(prepared)
                    except psycopg.errors.LockNotAvailable:
                        # _commit has rolled back and released its locks. Keep
                        # the unchanged queue for retry without aborting siblings.
                        status = {"status": "stale"}
                return status, preparation_elapsed, time.monotonic() - commit_started

            requeued_in_batch = unavailable_in_batch = 0
            with ThreadPoolExecutor(
                max_workers=min(concurrency, len(candidates)),
                thread_name_prefix="recall-passage-projector",
            ) as executor:
                futures = {executor.submit(project_document, candidate): candidate for candidate in candidates}
                next_progress = time.monotonic() + PROJECTION_PROGRESS_INTERVAL_SECONDS
                try:
                    while futures:
                        completed, _ = wait(
                            futures,
                            timeout=max(0.0, next_progress - time.monotonic())
                            if on_progress is not None else None,
                            return_when=FIRST_COMPLETED,
                        )
                        publish = False
                        for future in completed:
                            candidate = futures.pop(future)
                            status, preparation_elapsed, commit_elapsed = future.result()
                            prepare_seconds += preparation_elapsed
                            commit_seconds += commit_elapsed
                            if status["status"] == "logical_evidence_not_found":
                                # Keep logical requeue on the coordinator's
                                # reserved pool connection, outside commit owners.
                                requeued_in_batch += self._requeue_missing(candidate)
                                continue
                            if status["status"] == "logical_evidence_unavailable":
                                unavailable_in_batch += 1
                                continue
                            if status["status"] == "stale":
                                stale += 1
                                continue
                            documents += 1
                            passages += int(status["inserted"])
                            deleted += int(status["deleted"])
                            retained += int(status["retained"])
                            publish = True
                        # Search publication has one coordinator; preparation
                        # and commits continue in the bounded executor meanwhile.
                        if on_progress is not None and (publish or time.monotonic() >= next_progress):
                            on_progress()
                            next_progress = time.monotonic() + PROJECTION_PROGRESS_INTERVAL_SECONDS
                except BaseException:
                    stopping.set()
                    for future in futures:
                        future.cancel()
                    raise
            requeued += requeued_in_batch
            unavailable += unavailable_in_batch
            batches += 1
            if requeued_in_batch or unavailable_in_batch:
                # Yield to the logical projector in the unified worker. The
                # passage queue remains authoritative and retries next cycle.
                # A transient archive failure must never become a destructive
                # logical-document rebuild.
                break
        phase_started = time.monotonic()
        with self.store.connect() as connection:
            pending = connection.execute(
                """SELECT count(*) AS count
                     FROM canonical_passage_projection_queue
                    WHERE (%s::text IS NULL OR tenant_id=%s)""",
                (tenant_id, tenant_id),
            ).fetchone()["count"]
        count_seconds = time.monotonic() - phase_started
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
            # Prepare/commit are accumulated owner durations and can overlap;
            # commit includes cap/connection waits. Other phases are coordinator
            # wall time. No per-document identifiers or passage text are retained.
            "warmup_ms": max(0, round(warmup_seconds * 1000)),
            "pending_ms": max(0, round(pending_seconds * 1000)),
            "prepare_ms": max(0, round(prepare_seconds * 1000)),
            "commit_ms": max(0, round(commit_seconds * 1000)),
            "count_ms": max(0, round(count_seconds * 1000)),
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
        max_passages: int | None = None,
    ) -> dict[str, int | str]:
        """Embed only lossless passages missing the selected runtime fingerprint.

        ``max_passages`` (H5-3) is the caller's remaining daily budget: the call
        never sends more passages than that to the provider, shrinking the last
        batch as needed.
        """

        tenant_id = self._tenant(tenant_id)
        if not postgres_vector_plane(self.store):
            # H3-e': turbopuffer embeds ``embed_text`` natively on write; no
            # passage is pending here and the embeddings table is not read.
            return {
                "status": "not-applicable",
                "processed": 0,
                "batches": 0,
                "plane": "turbopuffer",
            }
        runtime = self.store.semantic_runtime
        if runtime is None:
            return {"status": "disabled", "processed": 0, "batches": 0}
        if runtime.dimensions != 512:
            raise ValueError("passage embeddings require 512 dimensions")
        if max_passages is not None and (
            isinstance(max_passages, bool)
            or not isinstance(max_passages, int)
            or max_passages < 1
        ):
            raise ValueError("passage embedding budget is invalid")
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
        # H2-a: the worker writes the contract the runtime declares (v2 =
        # header + text keyed by embed_sha256; v1 = text keyed by
        # text_sha256). Runtimes without the v2 surface (synthetic test
        # runtimes) behave as v2 under their single fingerprint.
        write_contract = getattr(runtime, "passage_write_contract", "v2")
        write_fingerprint = getattr(
            runtime,
            "passage_write_fingerprint",
            runtime.passage_fingerprint,
        )
        headed = write_contract == "v2"
        processed = batches = 0
        tenant_scope = tenant_id or ""
        headers_backfilled = 0
        if headed:
            # Rows projected before schema 064 get their header from the
            # catalog first, so the v2 key exists for every candidate.
            headers_backfilled = int(self.backfill_headers(
                tenant_id=tenant_id,
                batch_size=min(MAX_PASSAGE_HEADER_BACKFILL_BATCH, batch_size * 5),
                max_batches=max_batches,
            )["updated"])
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
                    # H5-3 budget: the last batch shrinks to what the daily cap
                    # still allows; when nothing is allowed the loop stops.
                    batch_limit = (
                        batch_size
                        if max_passages is None
                        else min(batch_size, max_passages - processed)
                    )
                    if batch_limit <= 0:
                        break
                    rows = connection.execute(
                        f"""SELECT passage.tenant_id,passage.source_id,
                                  passage.passage_id,passage.text_redacted,
                                  passage.text_sha256,passage.header_redacted,
                                  passage.embed_sha256
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
                              AND embedding.content_sha256=passage.{
                                  "embed_sha256" if headed else "text_sha256"
                              }
                            WHERE document.policy_fingerprint=%s
                              AND (%s::text='' OR passage.tenant_id=%s)
                              AND (
                                  (
                                      hashtextextended(passage.passage_id,0)
                                      & 9223372036854775807
                                  ) %% %s
                              )=%s
                              AND embedding.passage_id IS NULL
                              {
                                  "AND passage.header_redacted IS NOT NULL"
                                  if headed else ""
                              }
                            ORDER BY passage.tenant_id,passage.source_id,
                                     passage.passage_id
                            LIMIT %s""",
                        (
                            write_fingerprint,
                            self.policy.fingerprint,
                            tenant_scope,
                            tenant_scope,
                            shard_count,
                            shard_index,
                            batch_limit,
                        ),
                    ).fetchall()
                    connection.commit()
                    if not rows:
                        break
                    vectors = runtime.embed_passages(
                        [
                            passage_embedding_input(
                                row["header_redacted"] if headed else None,
                                row["text_redacted"],
                            )
                            for row in rows
                        ]
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
                                        row["embed_sha256"]
                                        if headed
                                        else row["text_sha256"],
                                        write_fingerprint,
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
                    f"""SELECT EXISTS(
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
                              AND embedding.content_sha256=passage.{
                                  "embed_sha256" if headed else "text_sha256"
                              }
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
                        write_fingerprint,
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
                    "contract": write_contract,
                    "headers_backfilled": headers_backfilled,
                }
            finally:
                connection.execute(
                    "SELECT pg_advisory_unlock(hashtextextended(%s,0))",
                    (lock_name,),
                )
                connection.commit()
