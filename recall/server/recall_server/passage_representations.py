"""Side-by-side retrieval representations over exact lossless passages."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Any, Iterable

from psycopg import sql


CONTEXT_CONTRACT = "recall.passage-context.v2:project-basename-only"
REPRESENTATION_CONTRACT = "recall.passage-representation.v1"
REPRESENTATION_TEXT_CONTRACT = (
    "recall.passage-embedding-excerpt.v1:head-tail-7000-utf8-bytes"
)
FINGERPRINT_RE = re.compile(r"[0-9a-f]{64}\Z")
MAX_CONTEXT_FIELD_CHARS = 256
EMBEDDING_EXCERPT_MARKER = "\n[...embedding excerpt clipped...]\n"
VECTOR_COLUMNS = {
    512: "embedding",
    1536: "embedding_1536",
    3072: "embedding_3072",
}


def _fingerprint(*values: object) -> str:
    return hashlib.sha256(
        "\0".join(str(value) for value in values).encode()
    ).hexdigest()


def _bounded(value: object | None) -> str | None:
    if value is None:
        return None
    rendered = str(value).strip()
    return rendered[:MAX_CONTEXT_FIELD_CHARS] if rendered else None


def embedding_excerpt(value: str, *, max_bytes: int = 7_000) -> str:
    """Bound provider input while preserving both ends of the exact passage."""

    encoded = value.encode()
    if len(encoded) <= max_bytes:
        return value
    marker = EMBEDDING_EXCERPT_MARKER.encode()
    if max_bytes <= len(marker) + 2:
        raise ValueError("embedding excerpt budget is too small")
    remaining = max_bytes - len(marker)
    head_bytes = (remaining + 1) // 2
    tail_bytes = remaining - head_bytes
    head = encoded[:head_bytes].decode(errors="ignore")
    tail = encoded[-tail_bytes:].decode(errors="ignore")
    excerpt = head + EMBEDDING_EXCERPT_MARKER + tail
    if len(excerpt.encode()) > max_bytes:
        raise AssertionError("embedding excerpt exceeded its byte budget")
    return excerpt


def workspace_label(value: object | None) -> str | None:
    """Keep only the bounded project basename, never its parent path."""

    if value is None:
        return None
    rendered = str(value).strip()
    if not rendered:
        return None
    parts = tuple(
        part
        for part in PurePosixPath(rendered.replace("\\", "/")).parts
        if part not in {"", "/"}
    )
    return _bounded(parts[-1]) if parts else None


@dataclass(frozen=True)
class PassageContextPolicy:
    opening_passages: int = 2
    neighbor_passages: int = 2
    max_tokens: int = 7_168
    max_embedding_bytes: int = 7_000
    contract: str = CONTEXT_CONTRACT

    def __post_init__(self) -> None:
        if (
            self.contract != CONTEXT_CONTRACT
            or type(self.opening_passages) is not int
            or not 0 <= self.opening_passages <= 8
            or type(self.neighbor_passages) is not int
            or not 0 <= self.neighbor_passages <= 8
            or type(self.max_tokens) is not int
            or not 512 <= self.max_tokens <= 8_000
            or type(self.max_embedding_bytes) is not int
            or not 4_096 <= self.max_embedding_bytes <= 8_000
        ):
            raise ValueError("invalid passage context policy")

    @property
    def fingerprint(self) -> str:
        return _fingerprint(
            self.contract,
            self.opening_passages,
            self.neighbor_passages,
            self.max_tokens,
            self.max_embedding_bytes,
        )


@dataclass(frozen=True)
class ContextPassage:
    passage_id: str
    ordinal: int
    text: str
    token_count: int


@dataclass(frozen=True)
class DocumentContext:
    source_family: str | None = None
    source_aliases: tuple[str, ...] = ()
    harness: str | None = None
    workspace: str | None = None
    branch: str | None = None
    first_occurred_at: str | None = None
    last_occurred_at: str | None = None


@dataclass(frozen=True)
class ContextualPassage:
    passage_id: str
    context_text: str
    context_sha256: str


@dataclass(frozen=True)
class PassageRepresentation:
    name: str
    runtime: Any
    context_policy: PassageContextPolicy | None = None

    def __post_init__(self) -> None:
        if (
            not isinstance(self.name, str)
            or not re.fullmatch(r"[a-z0-9][a-z0-9-]{1,63}", self.name)
            or getattr(self.runtime, "dimensions", None) not in VECTOR_COLUMNS
        ):
            raise ValueError("invalid passage representation")

    @property
    def context_fingerprint(self) -> str | None:
        return (
            self.context_policy.fingerprint
            if self.context_policy is not None
            else None
        )

    @property
    def fingerprint(self) -> str:
        return _fingerprint(
            REPRESENTATION_CONTRACT,
            self.name,
            self.runtime.passage_fingerprint,
            self.context_fingerprint or "exact-passage",
            REPRESENTATION_TEXT_CONTRACT,
        )


def _metadata_lines(metadata: DocumentContext) -> list[str]:
    aliases = tuple(
        sorted({
            value
            for raw in metadata.source_aliases
            if (value := _bounded(raw)) is not None
        })[:4]
    )
    values = (
        ("source family", _bounded(metadata.source_family)),
        ("source aliases", ", ".join(aliases) if aliases else None),
        ("harness", _bounded(metadata.harness)),
        ("workspace", workspace_label(metadata.workspace)),
        ("branch", _bounded(metadata.branch)),
        ("document start", _bounded(metadata.first_occurred_at)),
        ("document end", _bounded(metadata.last_occurred_at)),
    )
    return [
        f"{label}: {value}"
        for label, value in values
        if value is not None
    ]


def contextualize_passage(
    target: ContextPassage,
    support: Iterable[ContextPassage],
    *,
    metadata: DocumentContext,
    policy: PassageContextPolicy,
) -> ContextualPassage:
    """Render bounded exact anchors around one target passage."""

    candidates = {
        passage.passage_id: passage
        for passage in support
        if passage.passage_id != target.passage_id
        and (
            passage.ordinal < policy.opening_passages
            or abs(passage.ordinal - target.ordinal)
            <= policy.neighbor_passages
        )
    }
    ordered = sorted(
        candidates.values(),
        key=lambda passage: (
            0 if passage.ordinal < policy.opening_passages else 1,
            (
                passage.ordinal
                if passage.ordinal < policy.opening_passages
                else abs(passage.ordinal - target.ordinal)
            ),
            passage.ordinal,
            passage.passage_id,
        ),
    )
    metadata_lines = _metadata_lines(metadata)
    target_prefix = [
        "[document context]",
        *metadata_lines,
        f"[target passage {target.ordinal}]",
    ]
    target_budget = max(
        512,
        policy.max_embedding_bytes
        - len("\n".join(target_prefix).encode())
        - 1,
    )
    target_text = embedding_excerpt(
        target.text,
        max_bytes=target_budget,
    )
    base_sections = [*target_prefix, target_text]
    remaining = max(0, policy.max_tokens - target.token_count)
    remaining_bytes = max(
        0,
        policy.max_embedding_bytes
        - len("\n".join(base_sections).encode()),
    )
    selected: list[ContextPassage] = []
    for passage in ordered:
        byte_cost = (
            len(f"[support passage {passage.ordinal}]".encode())
            + 1
            + len(passage.text.encode())
            + 1
        )
        if (
            passage.token_count > remaining
            or byte_cost > remaining_bytes
        ):
            continue
        selected.append(passage)
        remaining -= passage.token_count
        remaining_bytes -= byte_cost

    sections = ["[document context]", *metadata_lines]
    for passage in sorted(selected, key=lambda value: value.ordinal):
        sections.extend((
            f"[support passage {passage.ordinal}]",
            passage.text,
        ))
    sections.extend((
        f"[target passage {target.ordinal}]",
        target_text,
    ))
    text = "\n".join(sections)
    if len(text.encode()) > policy.max_embedding_bytes:
        raise AssertionError("context exceeded its embedding byte budget")
    return ContextualPassage(
        passage_id=target.passage_id,
        context_text=text,
        context_sha256=hashlib.sha256(text.encode()).hexdigest(),
    )


class CanonicalPassageRepresentationIndex:
    """Backfill and roll back one fingerprinted 512-dimensional retrieval arm."""

    def __init__(
        self,
        store: Any,
        *,
        passage_policy_fingerprint: str,
        representation: PassageRepresentation,
        bound_tenant_id: str | None = None,
    ) -> None:
        if (
            not FINGERPRINT_RE.fullmatch(passage_policy_fingerprint)
            or (
                bound_tenant_id is not None
                and (
                    not isinstance(bound_tenant_id, str)
                    or not bound_tenant_id
                    or len(bound_tenant_id) > 256
                )
            )
        ):
            raise ValueError("invalid passage representation index")
        self.store = store
        self.passage_policy_fingerprint = passage_policy_fingerprint
        self.representation = representation
        self.bound_tenant_id = bound_tenant_id

    def _tenant(self, tenant_id: str | None) -> str | None:
        if self.bound_tenant_id is None:
            return tenant_id
        if tenant_id is not None and tenant_id != self.bound_tenant_id:
            raise PermissionError("passage representation tenant is unauthorized")
        return self.bound_tenant_id

    def _plain_rows(
        self,
        *,
        tenant_id: str | None,
        limit: int,
        shard_count: int = 1,
        shard_index: int = 0,
    ) -> list[dict[str, Any]]:
        with self.store.connect() as connection:
            return connection.execute(
                """SELECT passage.tenant_id,passage.source_id,
                          passage.passage_id,passage.text_redacted,
                          passage.text_sha256
                     FROM canonical_passages passage
                     JOIN canonical_passage_documents document
                       USING(
                           tenant_id,source_id,logical_document_id,
                           revision,policy_fingerprint
                       )
                     LEFT JOIN canonical_passage_embedding_representations
                               represented
                       ON represented.tenant_id=passage.tenant_id
                      AND represented.source_id=passage.source_id
                      AND represented.passage_id=passage.passage_id
                      AND represented.representation_fingerprint=%s
                    WHERE document.policy_fingerprint=%s
                      AND (%s::text IS NULL OR passage.tenant_id=%s)
                      AND (
                          (
                              hashtextextended(passage.passage_id,0)
                              & 9223372036854775807
                          ) %% %s
                      )=%s
                      AND represented.passage_id IS NULL
                    ORDER BY passage.tenant_id,passage.source_id,
                             passage.passage_id
                    LIMIT %s""",
                (
                    self.representation.fingerprint,
                    self.passage_policy_fingerprint,
                    tenant_id,
                    tenant_id,
                    shard_count,
                    shard_index,
                    limit,
                ),
            ).fetchall()

    def _context_rows(
        self,
        *,
        tenant_id: str | None,
        limit: int,
        shard_count: int = 1,
        shard_index: int = 0,
    ) -> list[dict[str, Any]]:
        policy = self.representation.context_policy
        assert policy is not None
        with self.store.connect() as connection:
            return connection.execute(
                """WITH targets AS MATERIALIZED (
                       SELECT passage.tenant_id,passage.source_id,
                              passage.logical_document_id,passage.revision,
                              passage.passage_id,passage.ordinal,
                              passage.text_redacted,passage.token_count
                         FROM canonical_passages passage
                         JOIN canonical_passage_documents document
                           USING(
                               tenant_id,source_id,logical_document_id,
                               revision,policy_fingerprint
                           )
                         LEFT JOIN
                              canonical_passage_embedding_representations
                              represented
                           ON represented.tenant_id=passage.tenant_id
                          AND represented.source_id=passage.source_id
                          AND represented.passage_id=passage.passage_id
                          AND represented.representation_fingerprint=%s
                        WHERE document.policy_fingerprint=%s
                          AND (%s::text IS NULL OR passage.tenant_id=%s)
                          AND (
                              (
                                  hashtextextended(passage.passage_id,0)
                                  & 9223372036854775807
                              ) %% %s
                          )=%s
                          AND represented.passage_id IS NULL
                        ORDER BY passage.tenant_id,passage.source_id,
                                 passage.logical_document_id,
                                 passage.revision,passage.ordinal
                        LIMIT %s
                   )
                   SELECT target.tenant_id,target.source_id,
                          target.logical_document_id,target.revision,
                          target.passage_id AS target_passage_id,
                          target.ordinal AS target_ordinal,
                          target.text_redacted AS target_text,
                          target.token_count AS target_token_count,
                          support.passage_id AS support_passage_id,
                          support.ordinal AS support_ordinal,
                          support.text_redacted AS support_text,
                          support.token_count AS support_token_count,
                          profile.family AS source_family,
                          coalesce(
                              aliases.values,
                              ARRAY[]::text[]
                          ) AS source_aliases,
                          session.harness,session.metadata,
                          evidence.first_occurred_at,
                          evidence.last_occurred_at
                     FROM targets target
                     JOIN canonical_passages support
                       ON support.tenant_id=target.tenant_id
                      AND support.source_id=target.source_id
                      AND support.logical_document_id=
                          target.logical_document_id
                      AND support.revision=target.revision
                      AND support.policy_fingerprint=%s
                      AND (
                          support.ordinal<%s
                          OR support.ordinal BETWEEN
                              greatest(
                                  0,
                                  target.ordinal-%s
                              )
                              AND target.ordinal+%s
                      )
                     JOIN canonical_evidence_documents evidence
                       ON evidence.tenant_id=target.tenant_id
                      AND evidence.source_id=target.source_id
                      AND evidence.logical_document_id=
                          target.logical_document_id
                      AND evidence.revision=target.revision
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
                    ORDER BY target.tenant_id,target.source_id,
                             target.logical_document_id,target.revision,
                             target.ordinal,support.ordinal,
                             support.passage_id""",
                (
                    self.representation.fingerprint,
                    self.passage_policy_fingerprint,
                    tenant_id,
                    tenant_id,
                    shard_count,
                    shard_index,
                    limit,
                    self.passage_policy_fingerprint,
                    policy.opening_passages,
                    policy.neighbor_passages,
                    policy.neighbor_passages,
                ),
            ).fetchall()

    @staticmethod
    def _prepared_contexts(
        rows: list[dict[str, Any]],
        *,
        policy: PassageContextPolicy,
    ) -> list[tuple[dict[str, Any], ContextualPassage]]:
        grouped: dict[
            tuple[str, str, str],
            list[dict[str, Any]],
        ] = {}
        for row in rows:
            grouped.setdefault(
                (
                    row["tenant_id"],
                    row["source_id"],
                    row["target_passage_id"],
                ),
                [],
            ).append(row)
        prepared = []
        for values in grouped.values():
            first = values[0]
            session_metadata = first.get("metadata") or {}
            if not isinstance(session_metadata, dict):
                session_metadata = {}
            target = ContextPassage(
                passage_id=first["target_passage_id"],
                ordinal=int(first["target_ordinal"]),
                text=first["target_text"],
                token_count=int(first["target_token_count"]),
            )
            support = tuple(
                ContextPassage(
                    passage_id=row["support_passage_id"],
                    ordinal=int(row["support_ordinal"]),
                    text=row["support_text"],
                    token_count=int(row["support_token_count"]),
                )
                for row in values
            )
            context = contextualize_passage(
                target,
                support,
                metadata=DocumentContext(
                    source_family=first.get("source_family"),
                    source_aliases=tuple(first.get("source_aliases") or ()),
                    harness=first.get("harness")
                    or session_metadata.get("harness"),
                    workspace=session_metadata.get("cwd"),
                    branch=session_metadata.get("branch"),
                    first_occurred_at=str(first["first_occurred_at"]),
                    last_occurred_at=str(first["last_occurred_at"]),
                ),
                policy=policy,
            )
            prepared.append((first, context))
        return prepared

    def _store_vectors(
        self,
        rows: list[dict[str, Any]],
        texts: list[str],
        hashes: list[str],
        vectors: list[list[float]],
    ) -> None:
        context_fingerprint = self.representation.context_fingerprint
        dimensions = self.representation.runtime.dimensions
        vector_column = VECTOR_COLUMNS[dimensions]
        parent_table = (
            "canonical_passage_contexts"
            if context_fingerprint is not None
            else "canonical_passages"
        )
        parent_context = (
            " AND current.context_fingerprint=%s"
            if context_fingerprint is not None
            else ""
        )
        insert = sql.SQL(
            """INSERT INTO
                   canonical_passage_embedding_representations(
                       tenant_id,source_id,passage_id,
                       representation_fingerprint,model,dimensions,
                       content_sha256,context_fingerprint,{vector_column}
                   )
                 SELECT %s,%s,%s,%s,%s,%s,%s,%s,%s::halfvec
                   FROM {parent_table} current
                  WHERE current.tenant_id=%s
                    AND current.source_id=%s
                    AND current.passage_id=%s
                    {parent_context}
               ON CONFLICT(
                   tenant_id,source_id,passage_id,
                   representation_fingerprint
               ) DO UPDATE SET
                   model=excluded.model,
                   dimensions=excluded.dimensions,
                   content_sha256=excluded.content_sha256,
                   context_fingerprint=excluded.context_fingerprint,
                   {vector_column}=excluded.{vector_column},
                   embedded_at=now()"""
        ).format(
            vector_column=sql.Identifier(vector_column),
            parent_table=sql.Identifier(parent_table),
            parent_context=sql.SQL(parent_context),
        )
        with self.store.connect() as connection:
            with connection.transaction():
                with connection.cursor() as cursor:
                    if context_fingerprint is not None:
                        cursor.executemany(
                        """INSERT INTO canonical_passage_contexts(
                               tenant_id,source_id,passage_id,
                               context_fingerprint,context_text_redacted,
                               context_sha256
                           )
                         SELECT %s,%s,%s,%s,%s,%s
                           FROM canonical_passages current
                          WHERE current.tenant_id=%s
                            AND current.source_id=%s
                            AND current.passage_id=%s
                           ON CONFLICT(
                               tenant_id,source_id,passage_id,
                               context_fingerprint
                           ) DO UPDATE SET
                               context_text_redacted=
                                   excluded.context_text_redacted,
                               context_sha256=excluded.context_sha256,
                               created_at=now()""",
                            [
                                (
                                    row["tenant_id"],
                                    row["source_id"],
                                    (
                                        row.get("target_passage_id")
                                        or row["passage_id"]
                                    ),
                                    context_fingerprint,
                                    text,
                                    digest,
                                    row["tenant_id"],
                                    row["source_id"],
                                    (
                                        row.get("target_passage_id")
                                        or row["passage_id"]
                                    ),
                                )
                                for row, text, digest in zip(
                                    rows, texts, hashes, strict=True
                                )
                            ],
                        )
                    cursor.executemany(
                        insert,
                        [
                            (
                                row["tenant_id"],
                                row["source_id"],
                                (
                                    row.get("target_passage_id")
                                    or row["passage_id"]
                                ),
                                self.representation.fingerprint,
                                self.representation.runtime.model,
                                dimensions,
                                digest,
                                context_fingerprint,
                                vector,
                                row["tenant_id"],
                                row["source_id"],
                                (
                                    row.get("target_passage_id")
                                    or row["passage_id"]
                                ),
                                *(
                                    (context_fingerprint,)
                                    if context_fingerprint is not None
                                    else ()
                                ),
                            )
                            for row, digest, vector in zip(
                                rows, hashes, vectors, strict=True
                            )
                        ],
                    )

    def embed_pending(
        self,
        *,
        tenant_id: str | None = None,
        batch_size: int = 128,
        max_batches: int = 10,
        shard_count: int = 1,
        shard_index: int = 0,
    ) -> dict[str, int | str]:
        tenant_id = self._tenant(tenant_id)
        if (
            type(batch_size) is not int
            or not 1 <= batch_size <= 2_048
            or type(max_batches) is not int
            or not 1 <= max_batches <= 100_000
            or type(shard_count) is not int
            or not 1 <= shard_count <= 64
            or type(shard_index) is not int
            or not 0 <= shard_index < shard_count
        ):
            raise ValueError("invalid representation backfill budget")
        processed = batches = 0
        for _ in range(max_batches):
            policy = self.representation.context_policy
            if policy is None:
                rows = self._plain_rows(
                    tenant_id=tenant_id,
                    limit=batch_size,
                    shard_count=shard_count,
                    shard_index=shard_index,
                )
                texts = [
                    embedding_excerpt(row["text_redacted"])
                    for row in rows
                ]
                hashes = [
                    hashlib.sha256(text.encode()).hexdigest()
                    for text in texts
                ]
            else:
                raw_rows = self._context_rows(
                    tenant_id=tenant_id,
                    limit=batch_size,
                    shard_count=shard_count,
                    shard_index=shard_index,
                )
                prepared = self._prepared_contexts(raw_rows, policy=policy)
                rows = [row for row, _context in prepared]
                texts = [
                    context.context_text
                    for _row, context in prepared
                ]
                hashes = [
                    context.context_sha256
                    for _row, context in prepared
                ]
            if not rows:
                break
            vectors = self.representation.runtime.embed_passages(texts)
            self._store_vectors(rows, texts, hashes, vectors)
            processed += len(rows)
            batches += 1
        coverage = self.coverage(tenant_id=tenant_id)
        return {
            "status": (
                "complete"
                if coverage["missing"] == 0
                else "pending"
            ),
            "processed": processed,
            "batches": batches,
            **coverage,
        }

    def coverage(self, *, tenant_id: str | None = None) -> dict[str, int]:
        tenant_id = self._tenant(tenant_id)
        with self.store.connect() as connection:
            row = connection.execute(
                """SELECT count(*) AS eligible,
                          count(represented.passage_id) AS represented
                     FROM canonical_passages passage
                     JOIN canonical_passage_documents document
                       USING(
                           tenant_id,source_id,logical_document_id,
                           revision,policy_fingerprint
                       )
                     LEFT JOIN canonical_passage_embedding_representations
                               represented
                       ON represented.tenant_id=passage.tenant_id
                      AND represented.source_id=passage.source_id
                      AND represented.passage_id=passage.passage_id
                      AND represented.representation_fingerprint=%s
                    WHERE document.policy_fingerprint=%s
                      AND (%s::text IS NULL OR passage.tenant_id=%s)""",
                (
                    self.representation.fingerprint,
                    self.passage_policy_fingerprint,
                    tenant_id,
                    tenant_id,
                ),
            ).fetchone()
        eligible = int(row["eligible"])
        represented = int(row["represented"])
        return {
            "eligible": eligible,
            "represented": represented,
            "missing": eligible - represented,
        }

    @property
    def index_name(self) -> str:
        return (
            "canonical_passage_rep_"
            + self.representation.fingerprint[:16]
            + "_hnsw"
        )

    def ensure_hnsw_index(self) -> str:
        vector_column = VECTOR_COLUMNS[
            self.representation.runtime.dimensions
        ]
        statement = sql.SQL(
            """CREATE INDEX IF NOT EXISTS {index}
                 ON canonical_passage_embedding_representations
              USING hnsw ({vector_column} halfvec_cosine_ops)
               WITH (m=16,ef_construction=64)
              WHERE representation_fingerprint={fingerprint}
                AND {vector_column} IS NOT NULL"""
        ).format(
            index=sql.Identifier(self.index_name),
            fingerprint=sql.Literal(self.representation.fingerprint),
            vector_column=sql.Identifier(vector_column),
        )
        with self.store.connect() as connection:
            connection.execute(statement)
        return self.index_name

    def rollback(self, *, tenant_id: str | None = None) -> dict[str, int]:
        tenant_id = self._tenant(tenant_id)
        with self.store.connect() as connection:
            with connection.transaction():
                deleted = connection.execute(
                    """DELETE FROM
                           canonical_passage_embedding_representations
                       WHERE representation_fingerprint=%s
                         AND (%s::text IS NULL OR tenant_id=%s)""",
                    (
                        self.representation.fingerprint,
                        tenant_id,
                        tenant_id,
                    ),
                ).rowcount
                contexts = 0
                context_fingerprint = (
                    self.representation.context_fingerprint
                )
                if context_fingerprint is not None:
                    contexts = connection.execute(
                        """DELETE FROM canonical_passage_contexts context
                            WHERE context.context_fingerprint=%s
                              AND (%s::text IS NULL OR context.tenant_id=%s)
                              AND NOT EXISTS (
                                  SELECT 1
                                    FROM canonical_passage_embedding_representations
                                         represented
                                   WHERE represented.tenant_id=
                                         context.tenant_id
                                     AND represented.source_id=
                                         context.source_id
                                     AND represented.passage_id=
                                         context.passage_id
                                     AND represented.context_fingerprint=
                                         context.context_fingerprint
                              )""",
                        (
                            context_fingerprint,
                            tenant_id,
                            tenant_id,
                        ),
                    ).rowcount
        return {
            "representations": max(0, deleted),
            "contexts": max(0, contexts),
        }


def representation_receipt(
    representation: PassageRepresentation,
) -> str:
    return json.dumps(
        {
            "context_fingerprint": representation.context_fingerprint,
            "dimensions": representation.runtime.dimensions,
            "model": representation.runtime.model,
            "name": representation.name,
            "representation_fingerprint": representation.fingerprint,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
