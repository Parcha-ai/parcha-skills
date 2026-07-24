from __future__ import annotations

import hashlib
import json
import re
import time
import urllib.error
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import urlsplit

from .authorization import decide
from .canonical import CanonicalPlane
from .db import BrainStore, SearchDeadlineExceeded, bounded_search_text
from .federation import SOURCE_FAMILIES
from .projectors import legacy_engine

AUTHORITY_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9:._/@+-]{1,255}\Z")
ALLOWED_FILTERS = frozenset(
    {"since", "until", "source_id", "source_family", "source_alias"}
)
MAX_CANONICAL_EMBEDDING_BATCH = 5000


def _timestamp(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


class CanonicalRetrieval:
    """Tenant-keyed hybrid retrieval over only the canonical v2 projection."""

    def __init__(self, store: BrainStore, archive: Any = None):
        self.store = store
        self.archive = archive

    def bind(self, principal: dict[str, Any]) -> BoundCanonicalRetrieval:
        tenant_id = principal.get("tenant_id")
        principal_id = principal.get("principal_id")
        audience = principal.get("audience")
        if (
            principal.get("credential_kind") != "mcp"
            or audience != "recall-mcp"
            or not isinstance(tenant_id, str)
            or not AUTHORITY_RE.fullmatch(tenant_id)
            or not isinstance(principal_id, str)
            or not AUTHORITY_RE.fullmatch(principal_id)
        ):
            raise PermissionError("canonical MCP authority required")
        if not decide(principal, "mcp.ping", tenant_id=tenant_id).allowed:
            raise PermissionError("canonical MCP authority required")
        sources = tuple(principal.get("authorized_sources") or ())
        if any(
            not isinstance(source, str) or not AUTHORITY_RE.fullmatch(source)
            for source in sources
        ):
            raise PermissionError("canonical MCP source grants invalid")
        return BoundCanonicalRetrieval(
            self.store,
            tenant_id=tenant_id,
            principal_id=principal_id,
            authorized_sources=sources,
            archive=self.archive,
        )

    def embed_pending(
        self,
        *,
        tenant_id: str | None = None,
        batch_size: int = 100,
        max_batches: int = 10,
    ) -> dict[str, int | str]:
        runtime = self.store.semantic_runtime
        if runtime is None:
            return {"status": "disabled", "processed": 0, "batches": 0}
        if runtime.dimensions != 512:
            raise ValueError("canonical embeddings require 512 dimensions")
        if (
            not 1 <= batch_size <= MAX_CANONICAL_EMBEDDING_BATCH
            or not 1 <= max_batches <= 100
        ):
            raise ValueError("invalid canonical embedding batch")
        processed = batches = 0
        for _ in range(max_batches):
            with self.store.connect() as connection:
                rows = connection.execute(
                    """SELECT chunk.tenant_id,chunk.source_id,chunk.chunk_id,
                              chunk.text_redacted,chunk.text_sha256
                       FROM canonical_chunks chunk
                       JOIN canonical_documents document
                         USING(tenant_id,source_id,document_id)
                       LEFT JOIN canonical_chunk_embeddings embedding
                         ON embedding.tenant_id=chunk.tenant_id
                        AND embedding.source_id=chunk.source_id
                        AND embedding.chunk_id=chunk.chunk_id
                        AND embedding.runtime_fingerprint=%s
                       WHERE chunk.deleted_at IS NULL
                         AND document.is_current
                         AND document.deleted_at IS NULL
                         AND embedding.chunk_id IS NULL
                         AND (%s::text IS NULL OR chunk.tenant_id=%s)
                       ORDER BY chunk.tenant_id,chunk.source_id,chunk.chunk_id
                       LIMIT %s""",
                    (runtime.fingerprint, tenant_id, tenant_id, batch_size),
                ).fetchall()
            if not rows:
                break
            vectors = runtime.embed_documents(
                [row["text_redacted"] for row in rows]
            )
            with self.store.connect() as connection:
                with connection.transaction():
                    with connection.cursor() as cursor:
                        cursor.executemany(
                            """INSERT INTO canonical_chunk_embeddings(
                                   tenant_id,source_id,chunk_id,model,dimensions,
                                   content_sha256,runtime_fingerprint,embedding
                               ) VALUES (%s,%s,%s,%s,512,%s,%s,%s::halfvec)
                               ON CONFLICT(tenant_id,source_id,chunk_id)
                               DO UPDATE SET
                                 model=excluded.model,
                                 dimensions=excluded.dimensions,
                                 content_sha256=excluded.content_sha256,
                                 runtime_fingerprint=excluded.runtime_fingerprint,
                                 embedding=excluded.embedding,
                                 embedded_at=now()""",
                            [
                                (
                                    row["tenant_id"],
                                    row["source_id"],
                                    row["chunk_id"],
                                    runtime.model,
                                    row["text_sha256"],
                                    runtime.fingerprint,
                                    vector,
                                )
                                for row, vector in zip(rows, vectors, strict=True)
                            ],
                        )
            processed += len(rows)
            batches += 1
        return {"status": "complete", "processed": processed, "batches": batches}


class BoundCanonicalRetrieval:
    """A canonical retrieval view whose tenant and grants cannot be overridden."""

    def __init__(
        self,
        store: BrainStore,
        *,
        tenant_id: str,
        principal_id: str,
        authorized_sources: tuple[str, ...],
        archive: Any = None,
    ):
        self.store = store
        self.tenant_id = tenant_id
        self.principal_id = principal_id
        self.authorized_sources = authorized_sources
        self.archive = archive

    @staticmethod
    def _filters(
        filters: dict[str, Any],
    ) -> tuple[str | None, str | None, str | None, str | None, str | None]:
        if not isinstance(filters, dict) or set(filters) - ALLOWED_FILTERS:
            raise ValueError("unsupported canonical search filter")
        source_id = filters.get("source_id")
        if source_id is not None and (
            not isinstance(source_id, str) or not AUTHORITY_RE.fullmatch(source_id)
        ):
            raise ValueError("invalid source_id filter")
        source_family = filters.get("source_family")
        if source_family is not None and source_family not in SOURCE_FAMILIES:
            raise ValueError("unsupported source_family filter")
        source_alias = filters.get("source_alias")
        if source_alias is not None and (
            not isinstance(source_alias, str)
            or not re.fullmatch(r"[a-z0-9][a-z0-9._-]{1,63}", source_alias)
        ):
            raise ValueError("invalid source_alias filter")
        values: list[str | None] = []
        for name in ("since", "until"):
            value = filters.get(name)
            if value is not None:
                if not isinstance(value, str):
                    raise ValueError("invalid temporal filter")
                parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
                if parsed.tzinfo is None:
                    raise ValueError("invalid temporal filter")
            values.append(value)
        return source_id, source_family, source_alias, values[0], values[1]

    def _sources(
        self,
        *,
        source_id: str | None,
        source_family: str | None,
        source_alias: str | None,
    ) -> list[str]:
        """Resolve convenience routes only within the bound canonical grants."""
        sources = set(self.authorized_sources)
        if source_id is not None:
            sources &= {source_id}
        if not sources or (source_family is None and source_alias is None):
            return sorted(sources)
        with self.store.connect() as connection:
            if source_family is not None:
                rows = connection.execute(
                    """SELECT source_id FROM source_profiles
                       WHERE family=%s AND source_id=ANY(%s)
                       ORDER BY source_id""",
                    (source_family, sorted(sources)),
                ).fetchall()
                sources &= {row["source_id"] for row in rows}
            if source_alias is not None and sources:
                row = connection.execute(
                    """SELECT source_id FROM source_aliases
                       WHERE alias=%s AND source_id=ANY(%s)""",
                    (source_alias, sorted(sources)),
                ).fetchone()
                sources &= {row["source_id"]} if row else set()
        return sorted(sources)

    @staticmethod
    def _row(row: dict[str, Any], score: float) -> dict[str, Any]:
        text, clipped = bounded_search_text(row["text_redacted"])
        return {
            "source_id": row["source_id"],
            "native_id": row["native_id"],
            "native_parent_id": row.get("native_parent_id"),
            "revision": row["revision"],
            "occurred_at": _timestamp(row["occurred_at"]),
            "observed_at": _timestamp(row["observed_at"]),
            "ingested_at": _timestamp(row["created_at"]),
            "time_basis": "occurred_at",
            "text": text,
            "text_clipped": clipped,
            "receipt": row["receipt"],
            "rank": round(score, 8),
        }

    def search(
        self,
        query: str,
        filters: dict[str, Any] | None = None,
        limit: int = 10,
        _authorized_source: Any = None,
    ) -> dict[str, Any]:
        if not isinstance(query, str) or not query.strip() or len(query) > 8192:
            raise ValueError("invalid canonical search query")
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 20:
            raise ValueError("invalid canonical search limit")
        source_id, source_family, source_alias, since, until = self._filters(
            filters or {}
        )
        sources = self._sources(
            source_id=source_id,
            source_family=source_family,
            source_alias=source_alias,
        )
        if not sources:
            return {
                "results": [],
                "diagnostics": {
                    "engine": "canonical-v2",
                    "lexical_candidates": 0,
                    "semantic_candidates": 0,
                },
            }
        informative = legacy_engine().informative_terms(query)[:16]
        if not informative:
            return {
                "results": [],
                "diagnostics": {
                    "engine": "canonical-v2",
                    "lexical_candidates": 0,
                    "semantic_candidates": 0,
                    "lexical_mode": "no-informative-terms",
                },
            }
        candidate_limit = min(100, max(20, limit * 5))
        lexical_deadline_at = (
            time.monotonic() + self.store.search_deadline_ms / 1000
        )

        def lexical_rows(
            connection: Any,
            search_query: str,
            *,
            minimum_matches: int,
        ) -> list[dict[str, Any]]:
            rows = self.store._execute_bounded(
                connection,
                """SELECT chunk.source_id,document.native_id,document.revision,
                          event.native_parent_id,event.occurred_at,event.observed_at,
                          event.created_at,chunk.text_redacted,chunk.receipt,
                          ts_rank_cd(
                            chunk.search_vector,
                            websearch_to_tsquery('simple',%s),
                            32
                          ) AS score,
                          (SELECT count(*)
                             FROM unnest(%s::text[]) AS query_term(value)
                            WHERE chunk.search_vector @@
                                  plainto_tsquery('simple',query_term.value)
                          ) AS matched_term_count
                   FROM canonical_chunks chunk
                   JOIN canonical_documents document
                     USING(tenant_id,source_id,document_id)
                   JOIN canonical_events event
                     USING(tenant_id,source_id,event_id)
                   WHERE chunk.tenant_id=%s
                     AND chunk.source_id=ANY(%s)
                     AND chunk.deleted_at IS NULL
                     AND document.is_current
                     AND document.deleted_at IS NULL
                     AND chunk.search_vector @@
                         websearch_to_tsquery('simple',%s)
                     AND (%s::timestamptz IS NULL OR event.occurred_at>=%s)
                     AND (%s::timestamptz IS NULL OR event.occurred_at<=%s)
                   ORDER BY matched_term_count DESC,score DESC,
                            event.occurred_at DESC,chunk.chunk_id
                   LIMIT %s""",
                (
                    search_query,
                    informative,
                    self.tenant_id,
                    sources,
                    search_query,
                    since,
                    since,
                    until,
                    until,
                    candidate_limit,
                ),
                lexical_deadline_at,
            ).fetchall()
            return [
                row
                for row in rows
                if int(row["matched_term_count"]) >= minimum_matches
            ]

        strict_query = " ".join(informative)
        try:
            with self.store.connect() as connection:
                lexical = lexical_rows(
                    connection,
                    strict_query,
                    minimum_matches=len(informative),
                )
                lexical_mode = "strict"
                if not lexical and len(informative) > 1:
                    relaxed_query = " OR ".join(
                        f'"{term}"' for term in informative
                    )
                    lexical = lexical_rows(
                        connection,
                        relaxed_query,
                        minimum_matches=2 if len(informative) >= 3 else 1,
                    )
                    lexical_mode = "relaxed" if lexical else "relaxed-empty"
        except SearchDeadlineExceeded:
            lexical = []
            lexical_mode = "deadline-exceeded"
        semantic: list[dict[str, Any]] = []
        runtime = self.store.semantic_runtime
        semantic_status = "disabled" if runtime is None else "ok"
        if runtime is not None:
            try:
                bounded_embed = getattr(runtime, "embed_query_bounded", None)
                vector = (
                    bounded_embed(query)
                    if bounded_embed is not None
                    else runtime.embed_query(query)
                )
            except (json.JSONDecodeError, TimeoutError, urllib.error.URLError):
                semantic_status = "unavailable"
            else:
                with self.store.connect() as connection:
                    semantic = connection.execute(
                        """SELECT chunk.source_id,document.native_id,document.revision,
                              event.native_parent_id,event.occurred_at,event.observed_at,
                              event.created_at,chunk.text_redacted,chunk.receipt,
                              1-(embedding.embedding <=> %s::halfvec) AS score
                       FROM canonical_chunk_embeddings embedding
                       JOIN canonical_chunks chunk
                         USING(tenant_id,source_id,chunk_id)
                       JOIN canonical_documents document
                         USING(tenant_id,source_id,document_id)
                       JOIN canonical_events event
                         USING(tenant_id,source_id,event_id)
                       WHERE chunk.tenant_id=%s
                         AND chunk.source_id=ANY(%s)
                         AND embedding.runtime_fingerprint=%s
                         AND chunk.deleted_at IS NULL
                         AND document.is_current
                         AND document.deleted_at IS NULL
                         AND (%s::timestamptz IS NULL OR event.occurred_at>=%s)
                         AND (%s::timestamptz IS NULL OR event.occurred_at<=%s)
                       ORDER BY embedding.embedding <=> %s::halfvec,
                                event.occurred_at DESC,chunk.chunk_id
                       LIMIT %s""",
                        (
                            vector,
                            self.tenant_id,
                            sources,
                            runtime.fingerprint,
                            since,
                            since,
                            until,
                            until,
                            vector,
                            candidate_limit,
                        ),
                    ).fetchall()
        combined: dict[str, tuple[dict[str, Any], float]] = {}
        for weight, rows in ((0.6, lexical), (0.4, semantic)):
            for rank, row in enumerate(rows, start=1):
                score = weight / (60 + rank)
                prior = combined.get(row["receipt"])
                combined[row["receipt"]] = (
                    row,
                    score + (prior[1] if prior else 0.0),
                )
        ranked = sorted(
            combined.values(),
            key=lambda value: (value[1], value[0]["occurred_at"], value[0]["receipt"]),
            reverse=True,
        )[:limit]
        return {
            "results": [self._row(row, score) for row, score in ranked],
            "diagnostics": {
                "engine": "canonical-v2",
                "lexical_candidates": len(lexical),
                "semantic_candidates": len(semantic),
                "semantic_status": semantic_status,
                "lexical_mode": lexical_mode,
            },
        }

    def _receipt_event(
        self,
        connection: Any,
        target: str,
    ) -> dict[str, Any] | None:
        redirect = connection.execute(
            """SELECT new_receipt FROM receipt_redirects
               WHERE tenant_id=%s AND old_receipt=%s""",
            (self.tenant_id, target),
        ).fetchone()
        if redirect:
            target = redirect["new_receipt"]
        row = connection.execute(
            """SELECT chunk.source_id,chunk.document_id,chunk.ordinal AS anchor_ordinal,
                      document.native_id,
                      document.revision,event.event_id,event.native_parent_id,
                      event.kind,event.occurred_at,event.observed_at,event.created_at,
                      event.canonical_redacted
               FROM canonical_chunks chunk
               JOIN canonical_documents document
                 USING(tenant_id,source_id,document_id)
               JOIN canonical_events event
                 USING(tenant_id,source_id,event_id)
               WHERE chunk.tenant_id=%s
                 AND chunk.source_id=ANY(%s)
                 AND chunk.receipt=%s
                 AND chunk.deleted_at IS NULL
                 AND document.is_current
                 AND document.deleted_at IS NULL
                 AND NOT EXISTS (
                   SELECT 1 FROM canonical_events later
                   WHERE later.tenant_id=document.tenant_id
                     AND later.source_id=document.source_id
                     AND later.native_id=document.native_id
                     AND later.revision>document.revision
                     AND later.is_tombstone
                 )""",
            (self.tenant_id, list(self.authorized_sources), target),
        ).fetchone()
        if row is not None:
            row["resolved_receipt"] = target
        return row

    @staticmethod
    def _context_event(row: dict[str, Any]) -> dict[str, Any]:
        chunks = []
        for chunk in row["chunks"]:
            text, clipped = bounded_search_text(chunk["text"])
            chunks.append({
                "ordinal": chunk["ordinal"],
                "text": text,
                "text_clipped": clipped,
                "receipt": chunk["receipt"],
            })
        return {
            "source_id": row["source_id"],
            "native_id": row["native_id"],
            "native_parent_id": row["native_parent_id"],
            "revision": row["revision"],
            "kind": row["kind"],
            "occurred_at": _timestamp(row["occurred_at"]),
            "observed_at": _timestamp(row["observed_at"]),
            "ingested_at": _timestamp(row["created_at"]),
            "time_basis": "occurred_at",
            "chunks": chunks,
        }

    def session_context(
        self,
        target: str,
        *,
        before: int = 4,
        after: int = 4,
        authorized_source: Any = None,
    ) -> dict[str, Any] | None:
        """Expand one receipt inside its source session without crossing grants."""
        if (
            not isinstance(target, str)
            or not target.startswith("recall://")
            or isinstance(before, bool)
            or not isinstance(before, int)
            or not 0 <= before <= 20
            or isinstance(after, bool)
            or not isinstance(after, int)
            or not 0 <= after <= 20
        ):
            raise ValueError("invalid canonical session context request")
        if not self.authorized_sources:
            return None
        with self.store.connect() as connection:
            anchor = self._receipt_event(connection, target)
            if anchor is None:
                return None
            parent = anchor["native_parent_id"] or anchor["native_id"]

            def neighbors(direction: str, limit: int) -> list[dict[str, Any]]:
                if limit == 0:
                    return []
                comparator = "<" if direction == "before" else ">"
                ordering = "DESC" if direction == "before" else "ASC"
                return connection.execute(
                    f"""SELECT event.source_id,event.native_id,
                               event.native_parent_id,document.revision,event.kind,
                               event.occurred_at,event.observed_at,event.created_at,
                               jsonb_agg(
                                 jsonb_build_object(
                                   'ordinal',chunk.ordinal,
                                   'text',chunk.text_redacted,
                                   'receipt',chunk.receipt
                                 ) ORDER BY chunk.ordinal
                               ) AS chunks
                        FROM canonical_events event
                        JOIN canonical_documents document
                          USING(tenant_id,source_id,event_id)
                        JOIN LATERAL (
                          SELECT bounded.ordinal,bounded.text_redacted,bounded.receipt
                          FROM canonical_chunks bounded
                          WHERE bounded.tenant_id=document.tenant_id
                            AND bounded.source_id=document.source_id
                            AND bounded.document_id=document.document_id
                            AND bounded.deleted_at IS NULL
                          ORDER BY bounded.ordinal
                          LIMIT 2
                        ) chunk ON true
                        WHERE event.tenant_id=%s
                          AND event.source_id=%s
                          AND COALESCE(event.native_parent_id,event.native_id)=%s
                          AND (event.occurred_at,event.native_id)
                              {comparator} (%s,%s)
                          AND document.is_current
                          AND document.deleted_at IS NULL
                        GROUP BY event.source_id,event.native_id,
                                 event.native_parent_id,document.revision,event.kind,
                                 event.occurred_at,event.observed_at,event.created_at
                        ORDER BY event.occurred_at {ordering},
                                 event.native_id {ordering}
                        LIMIT %s""",
                    (
                        self.tenant_id,
                        anchor["source_id"],
                        parent,
                        anchor["occurred_at"],
                        anchor["native_id"],
                        limit,
                    ),
                ).fetchall()

            previous = list(reversed(neighbors("before", before)))
            following = neighbors("after", after)
            anchor_chunks = connection.execute(
                """SELECT ordinal,text,receipt
                   FROM (
                     SELECT ordinal,text_redacted AS text,receipt
                     FROM canonical_chunks
                     WHERE tenant_id=%s AND source_id=%s AND document_id=%s
                       AND deleted_at IS NULL
                     ORDER BY abs(ordinal-%s),ordinal
                     LIMIT 3
                   ) bounded
                   ORDER BY ordinal""",
                (
                    self.tenant_id,
                    anchor["source_id"],
                    anchor["document_id"],
                    anchor["anchor_ordinal"],
                ),
            ).fetchall()
            anchor["chunks"] = anchor_chunks
        return {
            "session": {
                "source_id": anchor["source_id"],
                "native_parent_id": parent,
                "time_basis": "occurred_at",
            },
            "events": [
                self._context_event(row)
                for row in previous + [anchor] + following
            ],
            "anchor_receipt": anchor["resolved_receipt"],
            "bounds": {"before": before, "after": after},
        }

    @staticmethod
    def _question_time_window(
        question: str,
        *,
        now: datetime | None = None,
    ) -> tuple[str | None, str | None, str]:
        """Interpret only common relative windows; never use ingestion time."""
        current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
        lowered = question.casefold()
        if re.search(r"\btoday\b", lowered):
            since = current.replace(hour=0, minute=0, second=0, microsecond=0)
            return since.isoformat(), current.isoformat(), "question:today"
        if re.search(r"\byesterday\b", lowered):
            until = current.replace(hour=0, minute=0, second=0, microsecond=0)
            return (
                (until - timedelta(days=1)).isoformat(),
                until.isoformat(),
                "question:yesterday",
            )
        match = re.search(
            r"\b(?:past|last)\s+(\d{1,3})\s+(hour|day|week)s?\b",
            lowered,
        )
        if match:
            amount = min(int(match.group(1)), 365)
            unit = match.group(2)
            delta = {
                "hour": timedelta(hours=amount),
                "day": timedelta(days=amount),
                "week": timedelta(weeks=amount),
            }[unit]
            return (
                (current - delta).isoformat(),
                current.isoformat(),
                f"question:last-{amount}-{unit}s",
            )
        return None, None, "unbounded"

    def investigate(
        self,
        question: str,
        *,
        filters: dict[str, Any] | None = None,
        depth: str = "normal",
        authorized_source: Any = None,
    ) -> dict[str, Any]:
        """Return an answer-ready, receipt-backed multi-session evidence packet."""
        if not isinstance(question, str) or not question.strip() or len(question) > 8192:
            raise ValueError("invalid canonical investigation question")
        budgets = {
            "quick": {"families": 1, "sessions": 2, "context": 2},
            "normal": {"families": 4, "sessions": 4, "context": 4},
            "deep": {"families": 8, "sessions": 6, "context": 6},
        }
        if depth not in budgets:
            raise ValueError("invalid canonical investigation depth")
        budget = budgets[depth]
        effective_filters = dict(filters or {})
        self._filters(effective_filters)
        time_reason = "explicit"
        if "since" not in effective_filters and "until" not in effective_filters:
            since, until, time_reason = self._question_time_window(question)
            if since is not None:
                effective_filters["since"] = since
                effective_filters["until"] = until

        probes: list[dict[str, Any]] = []
        first = self.search(question, effective_filters, 20)
        probes.append(first)
        if not any(
            name in effective_filters
            for name in ("source_id", "source_family", "source_alias")
        ):
            with self.store.connect() as connection:
                rows = connection.execute(
                    """SELECT family,count(*) AS source_count
                       FROM source_profiles
                       WHERE source_id=ANY(%s)
                       GROUP BY family
                       ORDER BY source_count DESC,family
                       LIMIT %s""",
                    (list(self.authorized_sources), budget["families"]),
                ).fetchall()
            for row in rows:
                family_filters = {
                    **effective_filters,
                    "source_family": row["family"],
                }
                probes.append(self.search(question, family_filters, 8))

        combined: dict[str, dict[str, Any]] = {}
        for probe_index, probe in enumerate(probes):
            for rank, result in enumerate(probe["results"], start=1):
                receipt = result["receipt"]
                prior = combined.get(receipt)
                aggregate = 1.0 / (30 + rank) + 1.0 / (60 + probe_index)
                if prior is None:
                    combined[receipt] = {**result, "_score": aggregate}
                else:
                    prior["_score"] += aggregate
        ranked = sorted(
            combined.values(),
            key=lambda item: (
                item["_score"],
                item["occurred_at"],
                item["receipt"],
            ),
            reverse=True,
        )
        selected: list[dict[str, Any]] = []
        seen_sessions: set[tuple[str, str]] = set()
        for result in ranked:
            session_id = result.get("native_parent_id") or result["native_id"]
            session_key = (result["source_id"], session_id)
            if session_key in seen_sessions:
                continue
            seen_sessions.add(session_key)
            selected.append(result)
            if len(selected) >= budget["sessions"]:
                break

        investigations = []
        since_bound = (
            datetime.fromisoformat(effective_filters["since"].replace("Z", "+00:00"))
            if effective_filters.get("since")
            else None
        )
        until_bound = (
            datetime.fromisoformat(effective_filters["until"].replace("Z", "+00:00"))
            if effective_filters.get("until")
            else None
        )
        for result in selected:
            context = self.session_context(
                result["receipt"],
                before=budget["context"],
                after=budget["context"],
            )
            if context is not None:
                context["events"] = [
                    event
                    for event in context["events"]
                    if (
                        since_bound is None
                        or datetime.fromisoformat(
                            event["occurred_at"].replace("Z", "+00:00")
                        ) >= since_bound
                    )
                    and (
                        until_bound is None
                        or datetime.fromisoformat(
                            event["occurred_at"].replace("Z", "+00:00")
                        ) <= until_bound
                    )
                ]
                investigations.append({
                    "match": {
                        key: value
                        for key, value in result.items()
                        if key != "_score"
                    },
                    "context": context,
                })

        source_ids = sorted({
            item["match"]["source_id"] for item in investigations
        })
        source_families: list[str] = []
        if source_ids:
            with self.store.connect() as connection:
                rows = connection.execute(
                    """SELECT DISTINCT family FROM source_profiles
                       WHERE source_id=ANY(%s) ORDER BY family""",
                    (source_ids,),
                ).fetchall()
            source_families = [row["family"] for row in rows]
        occurred = [
            event["occurred_at"]
            for item in investigations
            for event in item["context"]["events"]
        ]
        uncertainty = []
        if not investigations:
            uncertainty.append("No authorized evidence matched the question.")
        if len(source_ids) < 2:
            uncertainty.append(
                "Evidence is concentrated in fewer than two authorized sources."
            )
        if time_reason == "unbounded":
            uncertainty.append(
                "The question had no explicit or recognized relative time window."
            )
        return {
            "question_interpretation": {
                "time_basis": "occurred_at",
                "time_window": {
                    "since": effective_filters.get("since"),
                    "until": effective_filters.get("until"),
                    "reason": time_reason,
                },
                "depth": depth,
            },
            "investigations": investigations,
            "coverage": {
                "sources": source_ids,
                "source_families": source_families,
                "sessions": len(investigations),
                "earliest_occurred_at": min(occurred) if occurred else None,
                "latest_occurred_at": max(occurred) if occurred else None,
            },
            "uncertainty": uncertainty,
            "diagnostics": {
                "engine": "canonical-investigator-v1",
                "search_probes": len(probes),
                "unique_candidates": len(combined),
                "expanded_sessions": len(investigations),
                "bounds": budget,
            },
        }

    def show(
        self,
        target: str,
        *,
        around: str | None = None,
        tail: int = 0,
        prompts: bool = False,
        authorized_source: Any = None,
    ) -> dict[str, Any] | None:
        if (
            not isinstance(target, str)
            or not target.startswith("recall://")
            or around is not None
            or tail not in {0}
            or prompts
        ):
            raise ValueError("unsupported canonical show request")
        if not self.authorized_sources:
            return None
        with self.store.connect() as connection:
            row = self._receipt_event(connection, target)
            if row is None:
                return None
            chunks = connection.execute(
                """SELECT ordinal,text_redacted AS text,receipt
                   FROM canonical_chunks
                   WHERE tenant_id=%s AND source_id=%s AND document_id=%s
                     AND deleted_at IS NULL
                   ORDER BY ordinal""",
                (self.tenant_id, row["source_id"], row["document_id"]),
            ).fetchall()
        return {
            "event": {
                "source_id": row["source_id"],
                "native_id": row["native_id"],
                "revision": row["revision"],
                "kind": row["kind"],
                "occurred_at": _timestamp(row["occurred_at"]),
                "observed_at": _timestamp(row["observed_at"]),
                "canonical_redacted": row["canonical_redacted"],
            },
            "chunks": chunks,
        }

    def related(
        self,
        *,
        cwd: str | None = None,
        branch: str | None = None,
        limit: int = 10,
        mains_only: bool = False,
        fast: bool = False,
        authorized_source: Any = None,
    ) -> dict[str, Any]:
        if (
            (cwd is not None and (not isinstance(cwd, str) or len(cwd) > 4096))
            or (branch is not None and (not isinstance(branch, str) or len(branch) > 512))
            or isinstance(limit, bool)
            or not isinstance(limit, int)
            or not 1 <= limit <= 20
            or mains_only
        ):
            raise ValueError("unsupported canonical related request")
        if not self.authorized_sources:
            return {"results": [], "diagnostics": {"engine": "canonical-v2"}}
        with self.store.connect() as connection:
            rows = connection.execute(
                """SELECT chunk.source_id,document.native_id,document.revision,
                          event.native_parent_id,event.occurred_at,event.observed_at,
                          event.created_at,chunk.text_redacted,chunk.receipt,
                          event.canonical_redacted #>> '{provenance,cwd}' AS path,
                          event.canonical_redacted #>> '{provenance,branch}' AS branch
                   FROM canonical_chunks chunk
                   JOIN canonical_documents document
                     USING(tenant_id,source_id,document_id)
                   JOIN canonical_events event
                     USING(tenant_id,source_id,event_id)
                   WHERE chunk.tenant_id=%s
                     AND chunk.source_id=ANY(%s)
                     AND chunk.deleted_at IS NULL
                     AND document.is_current
                     AND document.deleted_at IS NULL
                     AND (%s::text IS NULL OR
                          event.canonical_redacted #>> '{provenance,cwd}'=%s)
                     AND (%s::text IS NULL OR
                          event.canonical_redacted #>> '{provenance,branch}'=%s)
                   ORDER BY event.occurred_at DESC,chunk.chunk_id
                   LIMIT %s""",
                (
                    self.tenant_id,
                    list(self.authorized_sources),
                    cwd,
                    cwd,
                    branch,
                    branch,
                    limit,
                ),
            ).fetchall()
        return {
            "results": [
                {
                    **self._row(row, 1.0 / (60 + rank)),
                    "path": row["path"],
                    "branch": row["branch"],
                }
                for rank, row in enumerate(rows, start=1)
            ],
            "diagnostics": {"engine": "canonical-v2", "fast": bool(fast)},
        }

    def forget(self, receipt: str) -> dict[str, Any]:
        if self.archive is None or not isinstance(receipt, str):
            raise ValueError("canonical forget unavailable")
        parsed = urlsplit(receipt)
        source_id = parsed.netloc
        if (
            parsed.scheme != "recall"
            or source_id not in self.authorized_sources
            or not AUTHORITY_RE.fullmatch(source_id)
        ):
            raise ValueError("canonical forget receipt not found")
        with self.store.connect() as connection:
            owner = connection.execute(
                """SELECT 1 FROM canonical_source_grants
                   WHERE tenant_id=%s AND principal_id=%s AND source_id=%s
                     AND permission='owner'""",
                (self.tenant_id, self.principal_id, source_id),
            ).fetchone()
        if not owner:
            raise ValueError("canonical forget receipt not found")
        now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        return CanonicalPlane(self.store, self.archive).forget(
            {
                "contract": "recall.forget-request.v1",
                "schema_version": 1,
                "tenant_id": self.tenant_id,
                "principal_id": self.principal_id,
                "source_id": source_id,
                "target_receipt": receipt,
                "mode": "explicit_forget",
                "reason": "owner_requested",
                "requested_at": now,
                "idempotency_key": "mcp-forget-v1-"
                + hashlib.sha256(
                    "\x1f".join(
                        (self.tenant_id, self.principal_id, receipt)
                    ).encode()
                ).hexdigest(),
            }
        )
