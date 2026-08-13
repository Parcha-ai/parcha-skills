from __future__ import annotations

import hashlib
import json
import logging
import re
import shlex
import time
import urllib.error
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import urlsplit

from .actor_attribution import ACTOR_RELATIONS
from .authorization import decide
from .canonical import CanonicalPlane
from .db import BrainStore, SearchDeadlineExceeded, bounded_search_text
from .deep_inspection import (
    AgentExecObject,
    DeepInspectionBudget,
    DeepInspectionError,
    EvidenceTarget,
    agent_evidence_receipts,
)
from .federation import SOURCE_FAMILIES
from .projectors import legacy_engine
from .passage_projection import DEFAULT_PASSAGE_POLICY, PassagePolicy
from .passage_retrieval import PassageHintRetrieval

AUTHORITY_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9:._/@+-]{1,255}\Z")
UUID_RE = re.compile(
    r"(?<![0-9a-f])"
    r"[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}"
    r"(?![0-9a-f])",
    re.IGNORECASE,
)
ALLOWED_FILTERS = frozenset(
    {
        "since",
        "until",
        "source_id",
        "source_family",
        "source_alias",
        "source_connector",
    }
)
MAX_CANONICAL_EMBEDDING_BATCH = 5000
MAX_AGENTIC_MAPS = 5
MAX_AGENTIC_MAP_FINDINGS = 40
MAX_AGENTIC_MAP_FINDING_BYTES = 64_000
MAX_AGENT_EXEC_MAP_SHARDS = 8
MAX_AGENT_EXEC_MAP_SHARD_STDOUT_BYTES = 20_000
MAX_AGENT_EXEC_MAP_SHARD_STDERR_BYTES = 2_000
MAX_PARQUET_SCAN_OUTPUT_BYTES = 16 * 1024
MONTH_TERMS = frozenset({
    "january",
    "february",
    "march",
    "april",
    "may",
    "june",
    "july",
    "august",
    "september",
    "october",
    "november",
    "december",
})
QUERY_SCAFFOLD_TERMS = frozenset({
    *MONTH_TERMS,
    "across",
    "actual",
    "actually",
    "blocker",
    "blockers",
    "between",
    "claude",
    "codex",
    "coding",
    "decision",
    "decisions",
    "distinguish",
    "evidence",
    "families",
    "family",
    "happened",
    "implementation",
    "proposed",
    "session",
    "sessions",
    "source",
    "sources",
    "steps",
    "synthesize",
    "through",
    "unresolved",
    "verification",
    "verify",
})
LOG = logging.getLogger(__name__)


def _informative_query_terms(query: str) -> list[str]:
    """Prefer an explicit UUID as an exact corpus route over surrounding prose."""

    identifier = UUID_RE.search(query)
    if identifier is not None:
        return [identifier.group(0).lower()]
    terms = legacy_engine().informative_terms(query)
    has_month = any(term in MONTH_TERMS for term in terms)
    focused = [
        term
        for term in terms
        if term not in QUERY_SCAFFOLD_TERMS
        and not (
            has_month
            and term.isdigit()
            and (
                1 <= int(term) <= 31
                or 1900 <= int(term) <= 2100
            )
        )
    ]
    return (focused or terms)[:16]


def _timestamp(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


class CanonicalRetrieval:
    """Tenant-keyed hybrid retrieval over only the canonical v2 projection."""

    def __init__(
        self,
        store: BrainStore,
        archive: Any = None,
        *,
        evidence_projector: Any = None,
        deep_inspector: Any = None,
        passage_policy: PassagePolicy | None = None,
    ):
        self.store = store
        self.archive = archive
        self.evidence_projector = evidence_projector
        self.deep_inspector = deep_inspector
        self.passage_policy = passage_policy or DEFAULT_PASSAGE_POLICY

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
            evidence_projector=self.evidence_projector,
            deep_inspector=self.deep_inspector,
            passage_policy=self.passage_policy,
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
        tenant_scope = tenant_id or ""
        global_lock = "recall:canonical-embeddings"
        tenant_lock = f"{global_lock}:{tenant_scope}"
        with self.store.connect() as connection:
            shared_global = tenant_id is not None
            global_locked = connection.execute(
                (
                    "SELECT pg_try_advisory_lock_shared("
                    "hashtextextended(%s,0)) AS value"
                    if shared_global
                    else "SELECT pg_try_advisory_lock("
                    "hashtextextended(%s,0)) AS value"
                ),
                (global_lock,),
            ).fetchone()["value"]
            tenant_locked = False
            if global_locked and shared_global:
                tenant_locked = connection.execute(
                    "SELECT pg_try_advisory_lock(hashtextextended(%s,0)) AS value",
                    (tenant_lock,),
                ).fetchone()["value"]
            connection.commit()
            if not global_locked or (shared_global and not tenant_locked):
                if global_locked:
                    connection.execute(
                        "SELECT pg_advisory_unlock_shared(hashtextextended(%s,0))",
                        (global_lock,),
                    )
                    connection.commit()
                return {"status": "busy", "processed": 0, "batches": 0}
            try:
                seed = connection.execute(
                    """SELECT tenant_id,source_id,chunk_id
                       FROM canonical_chunk_embeddings
                       WHERE runtime_fingerprint=%s
                         AND (%s::text='' OR tenant_id=%s)
                       ORDER BY tenant_id DESC,source_id DESC,chunk_id DESC
                       LIMIT 1""",
                    (runtime.fingerprint, tenant_scope, tenant_scope),
                ).fetchone()
                connection.commit()
                connection.execute(
                    """INSERT INTO canonical_embedding_projection_watermarks(
                           runtime_fingerprint,tenant_scope,last_tenant_id,
                           last_source_id,last_chunk_id
                       ) VALUES (%s,%s,%s,%s,%s)
                       ON CONFLICT(runtime_fingerprint,tenant_scope) DO NOTHING""",
                    (
                        runtime.fingerprint,
                        tenant_scope,
                        seed["tenant_id"] if seed else "",
                        seed["source_id"] if seed else "",
                        seed["chunk_id"] if seed else "",
                    ),
                )
                connection.commit()
                wrapped = False
                while batches < max_batches:
                    watermark = connection.execute(
                        """SELECT last_tenant_id,last_source_id,last_chunk_id
                           FROM canonical_embedding_projection_watermarks
                           WHERE runtime_fingerprint=%s AND tenant_scope=%s""",
                        (runtime.fingerprint, tenant_scope),
                    ).fetchone()
                    connection.commit()
                    select_started = time.monotonic()
                    scope_clause = (
                        "AND tenant_id=%s" if tenant_id is not None else ""
                    )
                    scan_values: list[Any] = []
                    if tenant_id is not None:
                        scan_values.append(tenant_id)
                    scan_values.extend(
                        (
                            watermark["last_tenant_id"],
                            watermark["last_source_id"],
                            watermark["last_chunk_id"],
                            batch_size,
                            runtime.fingerprint,
                        )
                    )
                    window = connection.execute(
                        f"""WITH scan_window AS MATERIALIZED (
                                SELECT tenant_id,source_id,document_id,chunk_id,
                                       text_redacted,text_sha256
                                FROM canonical_chunks
                                WHERE deleted_at IS NULL
                                  {scope_clause}
                                  AND (tenant_id,source_id,chunk_id)
                                      > (%s,%s,%s)
                                ORDER BY tenant_id,source_id,chunk_id
                                LIMIT %s
                            )
                            SELECT chunk.tenant_id,chunk.source_id,chunk.chunk_id,
                                   chunk.text_redacted,chunk.text_sha256,
                                   COALESCE(
                                       document.is_current
                                       AND document.deleted_at IS NULL
                                       AND embedding.chunk_id IS NULL,
                                       false
                                   ) AS eligible
                            FROM scan_window chunk
                            LEFT JOIN canonical_documents document
                              USING(tenant_id,source_id,document_id)
                            LEFT JOIN canonical_chunk_embeddings embedding
                              ON embedding.tenant_id=chunk.tenant_id
                             AND embedding.source_id=chunk.source_id
                             AND embedding.chunk_id=chunk.chunk_id
                             AND embedding.runtime_fingerprint=%s
                            ORDER BY chunk.tenant_id,chunk.source_id,chunk.chunk_id
                         """,
                        tuple(scan_values),
                    ).fetchall()
                    connection.commit()
                    if not window:
                        cursor_is_set = any(
                            watermark[key]
                            for key in (
                                "last_tenant_id",
                                "last_source_id",
                                "last_chunk_id",
                            )
                        )
                        if cursor_is_set and not wrapped:
                            connection.execute(
                                """UPDATE canonical_embedding_projection_watermarks
                                   SET last_tenant_id='',last_source_id='',
                                       last_chunk_id='',updated_at=now()
                                   WHERE runtime_fingerprint=%s
                                     AND tenant_scope=%s""",
                                (runtime.fingerprint, tenant_scope),
                            )
                            connection.commit()
                            wrapped = True
                            continue
                        break
                    window_end = window[-1]
                    rows = [row for row in window if row["eligible"]]
                    selected_seconds = time.monotonic() - select_started
                    if not rows:
                        with connection.transaction():
                            connection.execute(
                                """UPDATE canonical_embedding_projection_watermarks
                                   SET last_tenant_id=%s,last_source_id=%s,
                                       last_chunk_id=%s,updated_at=now()
                                   WHERE runtime_fingerprint=%s
                                     AND tenant_scope=%s""",
                                (
                                    window_end["tenant_id"],
                                    window_end["source_id"],
                                    window_end["chunk_id"],
                                    runtime.fingerprint,
                                    tenant_scope,
                                ),
                            )
                        batches += 1
                        LOG.info(
                            "canonical embedding batch scanned=%s eligible=0 "
                            "select_ms=%s embed_ms=0 persist_ms=0",
                            len(window),
                            round(selected_seconds * 1000),
                        )
                        continue
                    embedding_started = time.monotonic()
                    vectors = runtime.embed_documents(
                        [row["text_redacted"] for row in rows]
                    )
                    embedded_seconds = time.monotonic() - embedding_started
                    persistence_started = time.monotonic()
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
                                    for row, vector in zip(
                                        rows, vectors, strict=True
                                    )
                                ],
                            )
                            cursor.execute(
                                """UPDATE canonical_embedding_projection_watermarks
                                   SET last_tenant_id=%s,last_source_id=%s,
                                       last_chunk_id=%s,updated_at=now()
                                   WHERE runtime_fingerprint=%s
                                     AND tenant_scope=%s""",
                                (
                                    window_end["tenant_id"],
                                    window_end["source_id"],
                                    window_end["chunk_id"],
                                    runtime.fingerprint,
                                    tenant_scope,
                                ),
                            )
                    persisted_seconds = time.monotonic() - persistence_started
                    processed += len(rows)
                    batches += 1
                    LOG.info(
                        "canonical embedding batch scanned=%s eligible=%s "
                        "select_ms=%s embed_ms=%s persist_ms=%s",
                        len(window),
                        len(rows),
                        round(selected_seconds * 1000),
                        round(embedded_seconds * 1000),
                        round(persisted_seconds * 1000),
                    )
                return {
                    "status": "complete",
                    "processed": processed,
                    "batches": batches,
                }
            finally:
                if tenant_locked:
                    connection.execute(
                        "SELECT pg_advisory_unlock(hashtextextended(%s,0))",
                        (tenant_lock,),
                    )
                connection.execute(
                    (
                        "SELECT pg_advisory_unlock_shared("
                        "hashtextextended(%s,0))"
                        if shared_global
                        else "SELECT pg_advisory_unlock("
                        "hashtextextended(%s,0))"
                    ),
                    (global_lock,),
                )
                connection.commit()


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
        evidence_projector: Any = None,
        deep_inspector: Any = None,
        passage_policy: PassagePolicy | None = None,
    ):
        self.store = store
        self.tenant_id = tenant_id
        self.principal_id = principal_id
        self.authorized_sources = authorized_sources
        self.archive = archive
        self.evidence_projector = evidence_projector
        self.deep_inspector = deep_inspector
        self.passage_policy = passage_policy or DEFAULT_PASSAGE_POLICY

    @staticmethod
    def _filters(
        filters: dict[str, Any],
    ) -> tuple[
        str | None,
        str | None,
        str | None,
        str | None,
        str | None,
        str | None,
    ]:
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
        source_connector = filters.get("source_connector")
        if source_connector is not None and (
            not isinstance(source_connector, str)
            or not re.fullmatch(
                r"[a-z0-9][a-z0-9._-]{1,63}",
                source_connector,
            )
        ):
            raise ValueError("invalid source_connector filter")
        values: list[str | None] = []
        for name in ("since", "until"):
            value = filters.get(name)
            if value is not None:
                if not isinstance(value, str):
                    raise ValueError("invalid temporal filter")
                if re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
                    value += "T00:00:00Z"
                parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
                if parsed.tzinfo is None:
                    raise ValueError("invalid temporal filter")
            values.append(value)
        return (
            source_id,
            source_family,
            source_alias,
            source_connector,
            values[0],
            values[1],
        )

    @staticmethod
    def _routing_filters(filters: dict[str, Any]) -> dict[str, Any]:
        """Keep actor scope for passages, not source/time route parsing."""

        return {
            key: value
            for key, value in filters.items()
            if key not in {"person", "person_relation"}
        }

    def _actor_sources(
        self,
        person: str,
        relation: str | None,
        sources: list[str],
        deadline_at: float,
    ) -> list[str]:
        """Find actor-linked sources so investigation can cover each tool."""

        if not sources:
            return []
        try:
            with self.store.connect() as connection:
                rows = self.store._execute_bounded(
                    connection,
                    """SELECT DISTINCT linked.source_id
                         FROM canonical_passage_actors linked
                         JOIN brain_actors actor
                           ON actor.tenant_id=linked.tenant_id
                          AND actor.actor_id=linked.actor_id
                         LEFT JOIN brain_actor_aliases alias
                           ON alias.tenant_id=actor.tenant_id
                          AND alias.actor_id=actor.actor_id
                          AND alias.searchable
                        WHERE linked.tenant_id=%s
                          AND linked.source_id=ANY(%s)
                          AND actor.active
                          AND (
                              lower(actor.display_name)=lower(%s)
                              OR lower(alias.alias)=lower(%s)
                          )
                          AND (%s::text IS NULL OR linked.relation=%s)
                        ORDER BY linked.source_id
                        LIMIT 64""",
                    (
                        self.tenant_id,
                        sources,
                        person.strip(),
                        person.strip(),
                        relation,
                        relation,
                    ),
                    deadline_at,
                ).fetchall()
        except SearchDeadlineExceeded:
            return []
        return [row["source_id"] for row in rows]

    def _sources_exclusively_bound_to_actors(
        self,
        sources: list[str],
        actor_ids: tuple[str, ...],
        deadline_at: float,
    ) -> bool:
        """Whether every selected source belongs only to the target actor."""

        if not sources or not actor_ids:
            return False
        try:
            with self.store.connect() as connection:
                rows = self.store._execute_bounded(
                    connection,
                    """SELECT binding.source_id
                         FROM canonical_source_actor_bindings binding
                        WHERE binding.tenant_id=%s
                          AND binding.source_id=ANY(%s)
                        GROUP BY binding.source_id
                       HAVING count(DISTINCT binding.actor_id)=1
                          AND bool_and(binding.actor_id=ANY(%s))
                        ORDER BY binding.source_id""",
                    (self.tenant_id, sources, list(actor_ids)),
                    deadline_at,
                ).fetchall()
        except SearchDeadlineExceeded:
            return False
        return {row["source_id"] for row in rows} == set(sources)

    def _sources(
        self,
        *,
        source_id: str | None,
        source_family: str | None,
        source_alias: str | None,
        source_connector: str | None,
    ) -> list[str]:
        """Resolve convenience routes only within the bound canonical grants."""
        sources = set(self.authorized_sources)
        if source_id is not None:
            sources &= {source_id}
        if source_connector is not None:
            sources = {
                source
                for source in sources
                if source.partition(":")[0] == source_connector
            }
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

    def list_people(self) -> dict[str, Any]:
        """List active people explicitly bound to the caller's sources."""

        started_at = time.monotonic()
        if not self.authorized_sources:
            return {
                "people": [],
                "complete": True,
                "diagnostics": {
                    "engine": "canonical-people-v1",
                    "elapsed_ms": 0.0,
                },
            }
        deadline_at = started_at + self.store.search_deadline_ms / 1000
        try:
            with self.store.connect() as connection:
                rows = self.store._execute_bounded(
                    connection,
                    """SELECT actor.actor_id,actor.display_name,
                              binding.source_id,binding.relation,
                              profile.family
                         FROM canonical_source_actor_bindings binding
                         JOIN brain_actors actor
                           ON actor.tenant_id=binding.tenant_id
                          AND actor.actor_id=binding.actor_id
                         LEFT JOIN source_profiles profile
                           ON profile.source_id=binding.source_id
                        WHERE binding.tenant_id=%s
                          AND binding.source_id=ANY(%s)
                          AND actor.active
                          AND actor.actor_kind='human'
                        ORDER BY lower(actor.display_name),actor.actor_id,
                                 binding.source_id,binding.relation
                        LIMIT 1024""",
                    (self.tenant_id, list(self.authorized_sources)),
                    deadline_at,
                ).fetchall()
        except SearchDeadlineExceeded:
            return {
                "people": [],
                "complete": False,
                "diagnostics": {
                    "engine": "canonical-people-v1",
                    "status": "deadline-exceeded",
                    "elapsed_ms": round(
                        (time.monotonic() - started_at) * 1000,
                        3,
                    ),
                },
            }
        people: dict[str, dict[str, Any]] = {}
        for row in rows:
            person = people.setdefault(
                row["actor_id"],
                {
                    "actor_id": row["actor_id"],
                    "display_name": row["display_name"],
                    "sources": [],
                },
            )
            source = {
                "source_id": row["source_id"],
                "relation": row["relation"],
            }
            if row.get("family") is not None:
                source["family"] = row["family"]
            person["sources"].append(source)
        return {
            "people": list(people.values()),
            "complete": len(rows) < 1024,
            "diagnostics": {
                "engine": "canonical-people-v1",
                "status": "ok",
                "elapsed_ms": round(
                    (time.monotonic() - started_at) * 1000,
                    3,
                ),
            },
        }

    @staticmethod
    def _row(row: dict[str, Any], score: float) -> dict[str, Any]:
        text, clipped = bounded_search_text(row["text_redacted"])
        result = {
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
        logical_document_id = row.get("logical_document_id")
        if isinstance(logical_document_id, str):
            result["logical_document_id"] = logical_document_id
        return result

    def search(
        self,
        query: str,
        filters: dict[str, Any] | None = None,
        limit: int = 10,
        _authorized_source: Any = None,
    ) -> dict[str, Any]:
        """Search the full-document passage index used by MCP investigation."""

        return self.passage_hints(query, filters, limit)

    def _legacy_chunk_search_for_eval(
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
        effective_filters = dict(filters or {})
        (
            source_id,
            source_family,
            source_alias,
            source_connector,
            since,
            until,
        ) = self._filters(effective_filters)
        sources = self._sources(
            source_id=source_id,
            source_family=source_family,
            source_alias=source_alias,
            source_connector=source_connector,
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
        informative = _informative_query_terms(query)
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
        lexical_candidate_limit = min(2_000, max(200, candidate_limit * 20))
        started_at = time.monotonic()
        deadline_at = started_at + self.store.search_deadline_ms / 1000

        def lexical_rows(
            connection: Any,
            search_query: str,
        ) -> list[dict[str, Any]]:
            return self.store._execute_bounded(
                connection,
                """WITH candidates AS MATERIALIZED (
                     SELECT chunk.tenant_id,chunk.source_id,chunk.document_id,
                            chunk.chunk_id,chunk.text_redacted,chunk.receipt,
                            chunk.search_vector,
                            ts_rank_cd(
                              chunk.search_vector,
                              plainto_tsquery('simple',%s),
                              32
                            ) AS score
                     FROM canonical_chunks chunk
                     WHERE chunk.tenant_id=%s
                       AND chunk.source_id=ANY(%s)
                       AND chunk.deleted_at IS NULL
                       AND chunk.search_vector @@
                           plainto_tsquery('simple',%s)
                     ORDER BY score DESC,chunk.chunk_id
                     LIMIT %s
                   )
                   SELECT candidate.source_id,document.native_id,document.revision,
                          event.native_parent_id,event.occurred_at,event.observed_at,
                          event.created_at,candidate.text_redacted,candidate.receipt,
                          candidate.score,evidence.logical_document_id
                   FROM candidates candidate
                   JOIN canonical_documents document
                     USING(tenant_id,source_id,document_id)
                   JOIN canonical_events event
                     USING(tenant_id,source_id,event_id)
                   LEFT JOIN canonical_evidence_documents evidence
                     ON evidence.tenant_id=event.tenant_id
                    AND evidence.source_id=event.source_id
                    AND evidence.native_parent_id=COALESCE(
                        event.native_parent_id,event.native_id
                    )
                   WHERE document.is_current
                     AND document.deleted_at IS NULL
                     AND (%s::timestamptz IS NULL OR event.occurred_at>=%s)
                     AND (%s::timestamptz IS NULL OR event.occurred_at<=%s)
                   ORDER BY score DESC,event.occurred_at DESC,
                            candidate.chunk_id
                   LIMIT %s""",
                (
                    search_query,
                    self.tenant_id,
                    sources,
                    search_query,
                    lexical_candidate_limit,
                    since,
                    since,
                    until,
                    until,
                    candidate_limit,
                ),
                deadline_at,
            ).fetchall()

        strict_query = " ".join(informative)
        runtime = self.store.semantic_runtime

        def run_lexical() -> tuple[list[dict[str, Any]], str, dict[str, Any]]:
            leg_started = time.monotonic()
            try:
                with self.store.connect() as connection:
                    rows = lexical_rows(connection, strict_query)
                status = "strict" if rows else "strict-empty"
            except SearchDeadlineExceeded:
                rows = []
                status = "deadline-exceeded"
            return rows, status, {
                "leg": "lexical",
                "elapsed_ms": round((time.monotonic() - leg_started) * 1000, 3),
                "status": status,
                "candidates": len(rows),
            }

        def run_semantic() -> tuple[
            list[dict[str, Any]], str, int, dict[str, Any]
        ]:
            leg_started = time.monotonic()
            embedding_ms = 0.0
            database_ms = 0.0
            semantic: list[dict[str, Any]] = []
            semantic_status = "ok"
            semantic_probe_count = 0

            def semantic_probe(probe_query: str) -> list[dict[str, Any]]:
                nonlocal embedding_ms, database_ms
                embedding_started = time.monotonic()
                bounded_embed = getattr(runtime, "embed_query_bounded", None)
                vector = (
                    bounded_embed(probe_query)
                    if bounded_embed is not None
                    else runtime.embed_query(probe_query)
                )
                embedding_ms += (time.monotonic() - embedding_started) * 1000
                semantic_candidate_limit = min(
                    5_000,
                    max(1_000, candidate_limit * 50),
                )
                database_started = time.monotonic()
                try:
                    with self.store.connect() as connection:
                        rows = self.store._execute_bounded(
                            connection,
                            """WITH candidates AS MATERIALIZED (
                             SELECT embedding.tenant_id,embedding.source_id,
                                    embedding.chunk_id,
                                    embedding.embedding <=> %s::halfvec AS distance
                             FROM canonical_chunk_embeddings embedding
                             WHERE embedding.tenant_id=%s
                               AND embedding.source_id=ANY(%s)
                               AND embedding.runtime_fingerprint=%s
                             ORDER BY embedding.embedding <=> %s::halfvec
                             LIMIT %s
                           )
                           SELECT chunk.source_id,document.native_id,
                                  document.revision,
                                  event.native_parent_id,event.occurred_at,
                                  event.observed_at,event.created_at,
                                  chunk.text_redacted,chunk.receipt,
                                  1-candidate.distance AS score,
                                  evidence.logical_document_id
                           FROM candidates candidate
                           JOIN canonical_chunks chunk
                             USING(tenant_id,source_id,chunk_id)
                           JOIN canonical_documents document
                             USING(tenant_id,source_id,document_id)
                           JOIN canonical_events event
                             USING(tenant_id,source_id,event_id)
                           LEFT JOIN canonical_evidence_documents evidence
                             ON evidence.tenant_id=event.tenant_id
                            AND evidence.source_id=event.source_id
                            AND evidence.native_parent_id=COALESCE(
                                event.native_parent_id,event.native_id
                            )
                           WHERE chunk.deleted_at IS NULL
                             AND document.is_current
                             AND document.deleted_at IS NULL
                             AND (%s::timestamptz IS NULL
                                  OR event.occurred_at>=%s)
                             AND (%s::timestamptz IS NULL
                                  OR event.occurred_at<=%s)
                           ORDER BY candidate.distance,
                                    event.occurred_at DESC,chunk.chunk_id
                           LIMIT %s""",
                            (
                                vector,
                                self.tenant_id,
                                sources,
                                runtime.fingerprint,
                                vector,
                                semantic_candidate_limit,
                                since,
                                since,
                                until,
                                until,
                                candidate_limit,
                            ),
                            deadline_at,
                        ).fetchall()
                finally:
                    database_ms += (
                        time.monotonic() - database_started
                    ) * 1000
                return rows

            probe_queries = [query]
            if informative:
                domain_probe = max(informative, key=len)
                if domain_probe.casefold() != query.strip().casefold():
                    probe_queries.append(domain_probe)
            seen_semantic_receipts: set[str] = set()
            for probe_query in probe_queries:
                try:
                    rows = semantic_probe(probe_query)
                except (
                    json.JSONDecodeError,
                    TimeoutError,
                    urllib.error.URLError,
                ):
                    if not semantic:
                        semantic_status = "unavailable"
                    break
                except SearchDeadlineExceeded:
                    if not semantic:
                        semantic_status = "deadline-exceeded"
                    break
                semantic_probe_count += 1
                for row in rows:
                    if row["receipt"] not in seen_semantic_receipts:
                        seen_semantic_receipts.add(row["receipt"])
                        semantic.append(row)
                if len(semantic) >= candidate_limit:
                    break
            return semantic, semantic_status, semantic_probe_count, {
                "leg": "semantic",
                "elapsed_ms": round((time.monotonic() - leg_started) * 1000, 3),
                "embedding_ms": round(embedding_ms, 3),
                "database_ms": round(database_ms, 3),
                "status": semantic_status,
                "candidates": len(semantic),
                "probes": semantic_probe_count,
            }

        with ThreadPoolExecutor(
            max_workers=2 if runtime is not None else 1,
            thread_name_prefix="recall-hybrid-search",
        ) as executor:
            lexical_future = executor.submit(run_lexical)
            semantic_future = (
                executor.submit(run_semantic) if runtime is not None else None
            )
            lexical, lexical_mode, lexical_timing = lexical_future.result()
            if semantic_future is None:
                semantic = []
                semantic_status = "disabled"
                semantic_probe_count = 0
                semantic_timing = {
                    "leg": "semantic",
                    "elapsed_ms": 0.0,
                    "embedding_ms": 0.0,
                    "database_ms": 0.0,
                    "status": "disabled",
                    "candidates": 0,
                    "probes": 0,
                }
            else:
                (
                    semantic,
                    semantic_status,
                    semantic_probe_count,
                    semantic_timing,
                ) = semantic_future.result()

        fusion_started = time.monotonic()
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
        fusion_ms = round((time.monotonic() - fusion_started) * 1000, 3)
        elapsed_ms = round((time.monotonic() - started_at) * 1000, 3)
        deadline_exceeded = any(
            item["status"] == "deadline-exceeded"
            for item in (lexical_timing, semantic_timing)
        )
        return {
            "results": [self._row(row, score) for row, score in ranked],
            "diagnostics": {
                "engine": "canonical-v2",
                "lexical_candidates": len(lexical),
                "semantic_candidates": len(semantic),
                "semantic_status": semantic_status,
                "semantic_probes": semantic_probe_count,
                "lexical_mode": lexical_mode,
                "elapsed_ms": elapsed_ms,
                "deadline_ms": self.store.search_deadline_ms,
                "deadline_exceeded": deadline_exceeded,
                "fusion_ms": fusion_ms,
                "legs": [lexical_timing, semantic_timing],
            },
        }

    def scope_documents(
        self,
        *,
        filters: dict[str, Any] | None = None,
        limit: int = 40,
        offset: int = 0,
    ) -> dict[str, Any]:
        """Enumerate exact authorized document boundaries without prose."""

        if (
            isinstance(limit, bool)
            or not isinstance(limit, int)
            or not 1 <= limit <= 80
            or isinstance(offset, bool)
            or not isinstance(offset, int)
            or not 0 <= offset <= 10_000
        ):
            raise ValueError("invalid canonical scope page")
        started_at = time.monotonic()
        deadline_at = started_at + self.store.search_deadline_ms / 1000
        effective_filters = dict(filters or {})
        person = effective_filters.pop("person", None)
        relation = effective_filters.pop("person_relation", None)
        if person is not None and (
            not isinstance(person, str)
            or not person.strip()
            or len(person) > 256
        ):
            raise ValueError("invalid person filter")
        if relation is not None and relation not in ACTOR_RELATIONS:
            raise ValueError("invalid person relation filter")
        if relation is not None and person is None:
            raise ValueError("person relation requires a person filter")
        (
            source_id,
            source_family,
            source_alias,
            source_connector,
            since,
            until,
        ) = self._filters(effective_filters)
        sources = self._sources(
            source_id=source_id,
            source_family=source_family,
            source_alias=source_alias,
            source_connector=source_connector,
        )
        if not sources:
            return {
                "documents": [],
                "total_documents": 0,
                "offset": offset,
                "complete": True,
                "diagnostics": {
                    "engine": "canonical-scope-v1",
                    "status": "ok",
                    "elapsed_ms": round(
                        (time.monotonic() - started_at) * 1000,
                        3,
                    ),
                },
            }

        actor_ids: list[str] | None = None
        if person is not None:
            try:
                with self.store.connect() as connection:
                    actor_rows = self.store._execute_bounded(
                        connection,
                        """SELECT DISTINCT actor.actor_id
                             FROM brain_actors actor
                             LEFT JOIN brain_actor_aliases alias
                               ON alias.tenant_id=actor.tenant_id
                              AND alias.actor_id=actor.actor_id
                              AND alias.searchable
                            WHERE actor.tenant_id=%s
                              AND actor.active
                              AND (
                                  lower(actor.display_name)=lower(%s)
                                  OR lower(alias.alias)=lower(%s)
                              )
                            ORDER BY actor.actor_id
                            LIMIT 64""",
                        (self.tenant_id, person.strip(), person.strip()),
                        deadline_at,
                    ).fetchall()
            except SearchDeadlineExceeded:
                actor_rows = []
                return {
                    "documents": [],
                    "total_documents": None,
                    "offset": offset,
                    "complete": False,
                    "diagnostics": {
                        "engine": "canonical-scope-v1",
                        "status": "deadline-exceeded",
                        "elapsed_ms": round(
                            (time.monotonic() - started_at) * 1000,
                            3,
                        ),
                    },
                }
            actor_ids = [row["actor_id"] for row in actor_rows]
            if not actor_ids:
                return {
                    "documents": [],
                    "total_documents": 0,
                    "offset": offset,
                    "complete": True,
                    "diagnostics": {
                        "engine": "canonical-scope-v1",
                        "status": "ok",
                        "elapsed_ms": round(
                            (time.monotonic() - started_at) * 1000,
                            3,
                        ),
                    },
                }

        try:
            with self.store.connect() as connection:
                rows = self.store._execute_bounded(
                    connection,
                    """SELECT document.source_id,
                              document.logical_document_id,document.revision,
                              document.first_occurred_at,
                              document.last_occurred_at,
                              document.record_count,document.part_count
                         FROM canonical_evidence_documents document
                        WHERE document.tenant_id=%s
                          AND document.source_id=ANY(%s)
                          AND (%s::timestamptz IS NULL
                               OR document.last_occurred_at>=%s)
                          AND (%s::timestamptz IS NULL
                               OR document.first_occurred_at<=%s)
                          AND (
                              %s::text[] IS NULL
                              OR EXISTS (
                                  SELECT 1
                                    FROM canonical_evidence_document_actors actor
                                   WHERE actor.tenant_id=document.tenant_id
                                     AND actor.source_id=document.source_id
                                     AND actor.logical_document_id=
                                         document.logical_document_id
                                     AND actor.revision=document.revision
                                     AND actor.actor_id=ANY(%s)
                                     AND (
                                         %s::text IS NULL
                                         OR actor.relation=%s
                                     )
                              )
                          )
                        ORDER BY document.last_occurred_at DESC,
                                 document.source_id,
                                 document.logical_document_id
                        OFFSET %s LIMIT %s""",
                    (
                        self.tenant_id,
                        sources,
                        since,
                        since,
                        until,
                        until,
                        actor_ids,
                        actor_ids,
                        relation,
                        relation,
                        offset,
                        limit + 1,
                    ),
                    deadline_at,
                ).fetchall()
        except SearchDeadlineExceeded:
            return {
                "documents": [],
                "total_documents": None,
                "offset": offset,
                "complete": False,
                "diagnostics": {
                    "engine": "canonical-scope-v1",
                    "status": "deadline-exceeded",
                    "elapsed_ms": round(
                        (time.monotonic() - started_at) * 1000,
                        3,
                    ),
                },
            }
        complete = len(rows) <= limit
        rows = rows[:limit]
        documents = [
            {
                "source_id": row["source_id"],
                "logical_document_id": row["logical_document_id"],
                "revision": int(row["revision"]),
                "first_occurred_at": _timestamp(row["first_occurred_at"]),
                "last_occurred_at": _timestamp(row["last_occurred_at"]),
                "record_count": int(row["record_count"]),
                "part_count": int(row["part_count"]),
            }
            for row in rows
        ]
        return {
            "documents": documents,
            "total_documents": offset + len(documents) if complete else None,
            "offset": offset,
            "complete": complete,
            "diagnostics": {
                "engine": "canonical-scope-v1",
                "status": "ok",
                "elapsed_ms": round(
                    (time.monotonic() - started_at) * 1000,
                    3,
                ),
            },
        }

    @staticmethod
    def _empty_parquet_scan(
        *,
        sources_available: int,
        pending: int = 0,
    ) -> dict[str, Any]:
        return {
            "provider": "parquet",
            "stdout": "",
            "stderr": "",
            "exit_code": 0,
            "complete": pending == 0,
            "stopped_reason": "projection_pending" if pending else "completed",
            "output_truncated": False,
            "timing": None,
            "opened_receipts": [],
            "datasets_available": 0,
            "sources_available": sources_available,
            "buckets_available": 0,
            "projection_pending": pending,
        }

    def _parquet_person_sources(
        self,
        sources: list[str],
        *,
        person: str,
        relation: str | None,
        deadline_at: float,
    ) -> list[str]:
        try:
            with self.store.connect() as connection:
                rows = self.store._execute_bounded(
                    connection,
                    """SELECT DISTINCT linked.source_id
                         FROM canonical_evidence_document_actors linked
                         JOIN brain_actors actor
                           ON actor.tenant_id=linked.tenant_id
                          AND actor.actor_id=linked.actor_id
                         LEFT JOIN brain_actor_aliases alias
                           ON alias.tenant_id=actor.tenant_id
                          AND alias.actor_id=actor.actor_id
                          AND alias.searchable
                        WHERE linked.tenant_id=%s
                          AND linked.source_id=ANY(%s)
                          AND actor.active
                          AND (
                              lower(actor.display_name)=lower(%s)
                              OR lower(alias.alias)=lower(%s)
                          )
                          AND (%s::text IS NULL OR linked.relation=%s)
                        ORDER BY linked.source_id
                        LIMIT 256""",
                    (
                        self.tenant_id,
                        sources,
                        person,
                        person,
                        relation,
                        relation,
                    ),
                    deadline_at,
                ).fetchall()
        except SearchDeadlineExceeded:
            raise DeepInspectionError("parquet_scan_scope_deadline") from None
        return [row["source_id"] for row in rows]

    def _parquet_shards(
        self,
        sources: list[str],
        *,
        since: datetime | None,
        until: datetime | None,
    ) -> tuple[list[dict[str, Any]], int]:
        values = (self.tenant_id, sources, since, since, until, until)
        with self.store.connect() as connection:
            rows = connection.execute(
                """SELECT source_id,bucket_start,dataset,shard_index,
                          object_key,content_sha256
                     FROM canonical_parquet_scan_shards
                    WHERE tenant_id=%s AND source_id=ANY(%s)
                      AND (
                          %s::timestamptz IS NULL OR bucket_start >=
                          date_trunc('month',%s::timestamptz)::date
                      )
                      AND (
                          %s::timestamptz IS NULL OR bucket_start <=
                          date_trunc('month',%s::timestamptz)::date
                      )
                    ORDER BY source_id,bucket_start,dataset,shard_index""",
                values,
            ).fetchall()
            pending = connection.execute(
                """SELECT count(*) AS count
                     FROM canonical_parquet_scan_queue
                    WHERE tenant_id=%s AND source_id=ANY(%s)
                      AND (
                          %s::timestamptz IS NULL OR bucket_start >=
                          date_trunc('month',%s::timestamptz)::date
                      )
                      AND (
                          %s::timestamptz IS NULL OR bucket_start <=
                          date_trunc('month',%s::timestamptz)::date
                      )""",
                values,
            ).fetchone()["count"]
        return rows, int(pending)

    def _verify_parquet_receipts(
        self,
        receipts: list[str],
        *,
        sources: list[str],
        since: datetime | None,
        until: datetime | None,
        person: str | None,
        relation: str | None,
    ) -> None:
        if not receipts:
            return
        with self.store.connect() as connection:
            rows = connection.execute(
                """SELECT DISTINCT chunk.receipt
                     FROM canonical_chunks chunk
                     JOIN canonical_documents canonical
                       USING(tenant_id,source_id,document_id)
                     JOIN canonical_events event
                       USING(tenant_id,source_id,event_id)
                     JOIN canonical_evidence_documents evidence
                       ON evidence.tenant_id=event.tenant_id
                      AND evidence.source_id=event.source_id
                      AND evidence.native_parent_id=coalesce(
                          event.native_parent_id,event.native_id
                      )
                    WHERE chunk.tenant_id=%s
                      AND chunk.source_id=ANY(%s)
                      AND chunk.receipt=ANY(%s)
                      AND (%s::timestamptz IS NULL OR event.occurred_at>=%s)
                      AND (%s::timestamptz IS NULL OR event.occurred_at<=%s)
                      AND (
                          %s::text IS NULL
                          OR EXISTS (
                              SELECT 1
                                FROM (
                                      SELECT attributed.actor_id,
                                             attributed.relation
                                        FROM canonical_event_actors attributed
                                       WHERE attributed.tenant_id=event.tenant_id
                                         AND attributed.source_id=event.source_id
                                         AND attributed.event_id=event.event_id
                                      UNION
                                      SELECT binding.actor_id,binding.relation
                                        FROM canonical_source_actor_bindings binding
                                       WHERE binding.tenant_id=event.tenant_id
                                         AND binding.source_id=event.source_id
                                ) link
                                JOIN brain_actors actor
                                  ON actor.tenant_id=event.tenant_id
                                 AND actor.actor_id=link.actor_id
                                LEFT JOIN brain_actor_aliases alias
                                  ON alias.tenant_id=actor.tenant_id
                                 AND alias.actor_id=actor.actor_id
                                 AND alias.searchable
                               WHERE (
                                     lower(actor.display_name)=lower(%s)
                                     OR lower(alias.alias)=lower(%s)
                                 )
                                 AND (
                                     %s::text IS NULL
                                     OR link.relation=%s
                                 )
                          )
                      )
                      AND chunk.deleted_at IS NULL
                      AND canonical.is_current
                      AND canonical.deleted_at IS NULL""",
                (
                    self.tenant_id,
                    sources,
                    receipts,
                    since,
                    since,
                    until,
                    until,
                    person,
                    person,
                    person,
                    relation,
                    relation,
                ),
            ).fetchall()
        if set(receipts) != {row["receipt"] for row in rows}:
            raise DeepInspectionError("deep_inspector_receipt_scope_violation")

    def execute_parquet_scan(
        self,
        program: str,
        *,
        filters: dict[str, Any] | None,
        timeout_seconds: int,
    ) -> dict[str, Any]:
        """Run caller-authored DuckDB over authorized source/month projections."""

        if (
            self.deep_inspector is None
            or not callable(getattr(self.deep_inspector, "execute_scan", None))
        ):
            raise DeepInspectionError("parquet_scan_not_configured")
        if (
            not isinstance(program, str)
            or not program.strip()
            or len(program.encode()) > 16_000
            or isinstance(timeout_seconds, bool)
            or not isinstance(timeout_seconds, int)
            or not 1 <= timeout_seconds <= 240
        ):
            raise DeepInspectionError("parquet_scan_request_invalid")
        effective = dict(filters or {})
        person = effective.pop("person", None)
        relation = effective.pop("person_relation", None)
        if person is not None and (
            not isinstance(person, str)
            or not person.strip()
            or len(person) > 256
        ):
            raise DeepInspectionError("parquet_scan_request_invalid")
        if relation is not None and (
            relation not in ACTOR_RELATIONS or person is None
        ):
            raise DeepInspectionError("parquet_scan_request_invalid")
        (
            source_id,
            source_family,
            source_alias,
            source_connector,
            since,
            until,
        ) = self._filters(effective)
        sources = self._sources(
            source_id=source_id,
            source_family=source_family,
            source_alias=source_alias,
            source_connector=source_connector,
        )
        deadline_at = time.monotonic() + self.store.search_deadline_ms / 1000
        if person is not None:
            sources = self._parquet_person_sources(
                sources,
                person=person.strip(),
                relation=relation,
                deadline_at=deadline_at,
            )
        if not sources:
            return self._empty_parquet_scan(sources_available=0)
        rows, pending = self._parquet_shards(
            sources,
            since=since,
            until=until,
        )
        referenced_datasets = {
            dataset
            for dataset in ("documents", "passages", "records", "actors")
            if re.search(rf"(?<![A-Za-z0-9_-]){dataset}-part-", program)
        }
        if referenced_datasets:
            rows = [row for row in rows if row["dataset"] in referenced_datasets]
        if not rows:
            return self._empty_parquet_scan(
                sources_available=len(sources),
                pending=pending,
            )
        if len(rows) > 511:
            raise DeepInspectionError("parquet_scan_scope_too_large")
        source_aliases = {
            value: f"s{index}"
            for index, value in enumerate(
                sorted({row["source_id"] for row in rows}),
                start=1,
            )
        }
        objects = tuple(
            AgentExecObject(
                object_key=row["object_key"],
                content_sha256=row["content_sha256"],
            )
            for row in rows
        )
        dataset_aliases = {
            row["object_key"]: (
                f"{source_aliases[row['source_id']]}/"
                f"{row['bucket_start'].isoformat()[:7]}/"
                f"{row['dataset']}-part-{int(row['shard_index']):05d}.parquet"
            )
            for row in rows
        }
        result = self.deep_inspector.execute_scan(
            tenant_id=self.tenant_id,
            program=program,
            objects=objects,
            dataset_aliases=dataset_aliases,
            timeout_seconds=timeout_seconds,
        )
        stdout = result.get("stdout")
        if not isinstance(stdout, str):
            raise DeepInspectionError("deep_inspector_result_invalid_execution")
        stdout_bytes = stdout.encode()
        if len(stdout_bytes) > MAX_PARQUET_SCAN_OUTPUT_BYTES:
            stdout = stdout_bytes[:MAX_PARQUET_SCAN_OUTPUT_BYTES].decode(
                errors="ignore"
            )
            result = {
                **result,
                "stdout": stdout,
                "complete": False,
                "stopped_reason": "output_limit",
                "output_truncated": True,
            }
        mentioned = agent_evidence_receipts(stdout)
        self._verify_parquet_receipts(
            mentioned,
            sources=sources,
            since=since,
            until=until,
            person=person,
            relation=relation,
        )
        buckets = {
            (row["source_id"], row["bucket_start"])
            for row in rows
        }
        return {
            **result,
            "opened_receipts": mentioned,
            "datasets_available": len(rows),
            "sources_available": len(source_aliases),
            "buckets_available": len(buckets),
            "projection_pending": pending,
            "complete": bool(result.get("complete")) and pending == 0,
        }

    def passage_hints(
        self,
        query: str,
        filters: dict[str, Any] | None = None,
        limit: int = 10,
    ) -> dict[str, Any]:
        """Return document/range hints without answering the natural question."""

        started_at = time.monotonic()
        deadline_at = started_at + self.store.search_deadline_ms / 1000

        if not isinstance(query, str) or not query.strip() or len(query) > 8192:
            raise ValueError("invalid passage hint query")
        if (
            isinstance(limit, bool)
            or not isinstance(limit, int)
            or not 1 <= limit <= 20
        ):
            raise ValueError("invalid passage hint limit")
        effective_filters = dict(filters or {})
        person = effective_filters.pop("person", None)
        relation = effective_filters.pop("person_relation", None)
        if person is not None and (
            not isinstance(person, str)
            or not person.strip()
            or len(person) > 256
        ):
            raise ValueError("invalid person filter")
        if relation is not None and relation not in ACTOR_RELATIONS:
            raise ValueError("invalid person relation filter")
        if relation is not None and person is None:
            raise ValueError("person relation requires a person filter")
        (
            source_id,
            source_family,
            source_alias,
            source_connector,
            since,
            until,
        ) = self._filters(effective_filters)
        sources = self._sources(
            source_id=source_id,
            source_family=source_family,
            source_alias=source_alias,
            source_connector=source_connector,
        )
        if not sources:
            return {
                "results": [],
                "diagnostics": {
                    "engine": "lossless-passages-v1",
                    "reason": "no-authorized-sources",
                },
            }
        informative = _informative_query_terms(query)
        lexical_query = " ".join(informative) if informative else query
        actor_ids: tuple[str, ...] | None = None
        if person is not None:
            sources = self._actor_sources(
                person,
                relation,
                sources,
                deadline_at,
            )
            if not sources:
                return {
                    "results": [],
                    "diagnostics": {
                        "engine": "lossless-passages-v1",
                        "reason": "no-person-sources",
                    },
                }
            with self.store.connect() as connection:
                rows = connection.execute(
                    """SELECT DISTINCT actor.actor_id
                         FROM brain_actors actor
                         LEFT JOIN brain_actor_aliases alias
                           ON alias.tenant_id=actor.tenant_id
                          AND alias.actor_id=actor.actor_id
                          AND alias.searchable
                        WHERE actor.tenant_id=%s
                          AND actor.active
                          AND (
                              lower(actor.display_name)=lower(%s)
                              OR lower(alias.alias)=lower(%s)
                          )
                        ORDER BY actor.actor_id""",
                    (self.tenant_id, person.strip(), person.strip()),
                ).fetchall()
            actor_ids = tuple(row["actor_id"] for row in rows)
        passage_actor_ids = actor_ids
        if (
            relation is None
            and actor_ids is not None
            and self._sources_exclusively_bound_to_actors(
                sources,
                actor_ids,
                deadline_at,
            )
        ):
            passage_actor_ids = None
        response = PassageHintRetrieval(
            self.store,
            tenant_id=self.tenant_id,
            sources=sources,
            policy_fingerprint=self.passage_policy.fingerprint,
            actor_ids=passage_actor_ids,
            actor_relations=(relation,) if relation is not None else None,
            actor_scope=person is not None,
        ).search(
            query,
            lexical_query=lexical_query,
            since=since,
            until=until,
            limit=limit,
            deadline_at=deadline_at,
        )
        if since is None and until is None:
            return response
        return self._clip_passage_hints_to_time_window(
            response,
            sources=sources,
            since=since,
            until=until,
            deadline_at=deadline_at,
        )

    def _clip_passage_hints_to_time_window(
        self,
        response: dict[str, Any],
        *,
        sources: list[str],
        since: str | None,
        until: str | None,
        deadline_at: float | None = None,
    ) -> dict[str, Any]:
        """Keep hint text and receipts inside the requested event window."""

        receipts = list(dict.fromkeys(
            receipt
            for document in response.get("results", ())
            for matching_range in document.get("matching_ranges", ())
            for receipt in matching_range.get("receipts", ())
        ))
        diagnostics = dict(response.get("diagnostics", {}))
        if not receipts:
            diagnostics["time_clip_status"] = "ok"
            return {**response, "diagnostics": diagnostics}
        try:
            with self.store.connect() as connection:
                rows = self.store._execute_bounded(
                    connection,
                    """SELECT chunk.receipt,chunk.text_redacted,
                              event.occurred_at
                         FROM canonical_chunks chunk
                         JOIN canonical_documents document
                           USING(tenant_id,source_id,document_id)
                         JOIN canonical_events event
                           USING(tenant_id,source_id,event_id)
                        WHERE chunk.tenant_id=%s
                          AND chunk.source_id=ANY(%s)
                          AND chunk.receipt=ANY(%s)
                          AND chunk.deleted_at IS NULL
                          AND document.is_current
                          AND document.deleted_at IS NULL
                          AND (%s::timestamptz IS NULL
                               OR event.occurred_at>=%s)
                          AND (%s::timestamptz IS NULL
                               OR event.occurred_at<=%s)
                        ORDER BY event.occurred_at,chunk.ordinal,
                                 chunk.receipt""",
                    (
                        self.tenant_id,
                        sources,
                        receipts,
                        since,
                        since,
                        until,
                        until,
                    ),
                    deadline_at
                    if deadline_at is not None
                    else time.monotonic()
                    + self.store.search_deadline_ms / 1000,
                ).fetchall()
        except SearchDeadlineExceeded:
            diagnostics["time_clip_status"] = "deadline-exceeded"
            diagnostics["time_filter_requires_exec"] = True
            # Every retrieval arm already applied the document-level time
            # overlap. Keep those authorized pointers, but remove prose and
            # receipts whose exact event timestamps could not be verified.
            pointer_results = [
                {**document, "matching_ranges": []}
                for document in response.get("results", ())
            ]
            return {
                **response,
                "results": pointer_results,
                "diagnostics": diagnostics,
            }
        eligible = {row["receipt"]: row for row in rows}
        results = []
        for document in response.get("results", ()):
            ranges = []
            for matching_range in document.get("matching_ranges", ()):
                range_rows = [
                    eligible[receipt]
                    for receipt in matching_range.get("receipts", ())
                    if receipt in eligible
                ]
                if not range_rows:
                    continue
                text, clipped = bounded_search_text("\n\n".join(
                    row["text_redacted"] for row in range_rows
                ))
                ranges.append({
                    key: value
                    for key, value in matching_range.items()
                    if key != "spans"
                } | {
                    "text": text,
                    "text_clipped": clipped,
                    "receipts": [row["receipt"] for row in range_rows],
                    "time_clipped": True,
                })
            if ranges:
                results.append({**document, "matching_ranges": ranges})
        diagnostics["time_clip_status"] = "ok"
        diagnostics["time_clipped_receipts"] = len(eligible)
        return {**response, "results": results, "diagnostics": diagnostics}

    @staticmethod
    def _investigation_probe(
        passage_probe: dict[str, Any],
    ) -> dict[str, Any]:
        """Adapt lossless document hints into receipt-backed session seeds."""

        results: list[dict[str, Any]] = []
        for document in passage_probe.get("results", ()):
            selected_range = next(
                (
                    item
                    for item in document.get("matching_ranges", ())
                    if item.get("receipts")
                ),
                None,
            )
            if selected_range is None:
                continue
            receipts = selected_range["receipts"]
            receipt = receipts[len(receipts) // 2]
            native_id = urlsplit(receipt).path.lstrip("/")
            results.append({
                "source_id": document["source_id"],
                "native_id": native_id,
                "native_parent_id": document["native_parent_id"],
                "revision": document["revision"],
                "occurred_at": document["last_occurred_at"],
                "time_basis": "occurred_at",
                "text": selected_range.get("text", ""),
                "text_clipped": bool(selected_range.get("text_clipped")),
                "receipt": receipt,
                "rank": document["rank"],
                "logical_document_id": document["logical_document_id"],
                "reasons": document.get("reasons", ()),
            })
        return {
            "results": results,
            "diagnostics": passage_probe.get("diagnostics", {}),
        }

    def execute_agent_program(
        self,
        program: str,
        *,
        logical_document_ids: tuple[str, ...],
        record_spans: dict[str, tuple[tuple[int, int], ...]],
        routing_receipts: dict[str, tuple[str, ...]],
        timeout_seconds: int,
        document_aliases: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """Execute beside only the immutable documents admitted by prior hints."""

        if (
            self.deep_inspector is None
            or not callable(getattr(self.deep_inspector, "execute", None))
        ):
            raise DeepInspectionError("deep_inspector_not_configured")
        if (
            not isinstance(logical_document_ids, tuple)
            or not 1 <= len(logical_document_ids) <= 80
            or len(logical_document_ids) != len(set(logical_document_ids))
            or any(
                not isinstance(item, str)
                or not re.fullmatch(r"ldoc_[0-9a-f]{32}", item)
                for item in logical_document_ids
            )
        ):
            raise DeepInspectionError("deep_inspector_target_invalid")
        if (
            not isinstance(routing_receipts, dict)
            or not set(routing_receipts).issubset(logical_document_ids)
            or any(
                not isinstance(document_id, str)
                or not re.fullmatch(r"ldoc_[0-9a-f]{32}", document_id)
                or not isinstance(receipts, tuple)
                or len(receipts) > 256
                or any(
                    not isinstance(receipt, str)
                    or not receipt.startswith("recall://")
                    or len(receipt) > 2048
                    or urlsplit(receipt).netloc
                    not in self.authorized_sources
                    for receipt in receipts
                )
                for document_id, receipts in routing_receipts.items()
            )
        ):
            raise DeepInspectionError("deep_inspector_target_invalid")
        if (
            not isinstance(record_spans, dict)
            or not set(record_spans).issubset(logical_document_ids)
            or any(
                not isinstance(document_id, str)
                or not re.fullmatch(r"ldoc_[0-9a-f]{32}", document_id)
                or not isinstance(spans, tuple)
                or len(spans) > 256
                or any(
                    not isinstance(span, tuple)
                    or len(span) != 2
                    or isinstance(span[0], bool)
                    or not isinstance(span[0], int)
                    or span[0] < 0
                    or isinstance(span[1], bool)
                    or not isinstance(span[1], int)
                    or not 1 <= span[1] <= 10_000
                    for span in spans
                )
                for document_id, spans in record_spans.items()
            )
        ):
            raise DeepInspectionError("deep_inspector_target_invalid")
        aliases = document_aliases or {
            document_id: f"d{ordinal}"
            for ordinal, document_id in enumerate(
                logical_document_ids,
                start=1,
            )
        }
        if (
            not isinstance(aliases, dict)
            or set(aliases) != set(logical_document_ids)
            or len(set(aliases.values())) != len(aliases)
            or any(
                not isinstance(alias, str)
                or re.fullmatch(r"d[1-9][0-9]?", alias) is None
                for alias in aliases.values()
            )
        ):
            raise DeepInspectionError("deep_inspector_target_invalid")
        with self.store.connect() as connection:
            rows = connection.execute(
                """SELECT document.logical_document_id,
                          document.manifest_object_key AS object_key,
                          document.manifest_content_sha256 AS content_sha256
                     FROM canonical_evidence_documents document
                    WHERE document.tenant_id=%s
                      AND document.source_id=ANY(%s)
                      AND document.logical_document_id=ANY(%s)
                    UNION ALL
                   SELECT part.logical_document_id,part.object_key,
                          part.content_sha256
                     FROM canonical_evidence_document_parts part
                    WHERE part.tenant_id=%s
                      AND part.source_id=ANY(%s)
                      AND part.logical_document_id=ANY(%s)
                    ORDER BY logical_document_id,object_key""",
                (
                    self.tenant_id,
                    list(self.authorized_sources),
                    list(logical_document_ids),
                    self.tenant_id,
                    list(self.authorized_sources),
                    list(logical_document_ids),
                ),
            ).fetchall()
        admitted_documents = {
            row["logical_document_id"] for row in rows
        }
        if (
            admitted_documents != set(logical_document_ids)
            or len(rows) > 512
        ):
            raise DeepInspectionError("deep_inspector_target_invalid")
        objects = tuple(
            AgentExecObject(
                object_key=row["object_key"],
                content_sha256=row["content_sha256"],
            )
            for row in rows
        )
        result = self.deep_inspector.execute(
            tenant_id=self.tenant_id,
            program=program,
            objects=objects,
            document_aliases=aliases,
            record_spans=record_spans,
            routing_receipts=routing_receipts,
            timeout_seconds=timeout_seconds,
        )
        stdout = result.get("stdout")
        if not isinstance(stdout, str):
            raise DeepInspectionError("deep_inspector_result_invalid_execution")
        # Full-document prose may itself mention recall:// URLs. Only an
        # explicit line-oriented evidence emission can become citation
        # authority; arbitrary rg context remains untrusted document text.
        mentioned = agent_evidence_receipts(stdout)
        if mentioned:
            with self.store.connect() as connection:
                verified_rows = connection.execute(
                    """SELECT DISTINCT chunk.receipt
                         FROM canonical_chunks chunk
                         JOIN canonical_documents canonical
                           USING(tenant_id,source_id,document_id)
                         JOIN canonical_events event
                           USING(tenant_id,source_id,event_id)
                         JOIN canonical_evidence_documents evidence
                           ON evidence.tenant_id=event.tenant_id
                          AND evidence.source_id=event.source_id
                          AND evidence.native_parent_id=COALESCE(
                              event.native_parent_id,event.native_id
                          )
                        WHERE chunk.tenant_id=%s
                          AND chunk.source_id=ANY(%s)
                          AND chunk.receipt=ANY(%s)
                          AND evidence.logical_document_id=ANY(%s)
                          AND chunk.deleted_at IS NULL
                          AND canonical.is_current
                          AND canonical.deleted_at IS NULL""",
                    (
                        self.tenant_id,
                        list(self.authorized_sources),
                        mentioned,
                        sorted(admitted_documents),
                    ),
                ).fetchall()
            verified = {row["receipt"] for row in verified_rows}
            if set(mentioned) != verified:
                raise DeepInspectionError(
                    "deep_inspector_receipt_scope_violation"
                )
        return {
            **result,
            "opened_receipts": mentioned,
            "documents_available": len(admitted_documents),
            "objects_available": len(objects),
        }

    def execute_agent_program_parallel(
        self,
        program: str,
        *,
        logical_document_ids: tuple[str, ...],
        document_aliases: dict[str, str],
        timeout_seconds: int,
        max_parallel: int,
        shard_size: int,
    ) -> dict[str, Any]:
        """Fan one agent-authored program across authorized Archil shards."""

        if (
            not isinstance(program, str)
            or not program.strip()
            or len(program.encode()) > 16_000
            or not isinstance(logical_document_ids, tuple)
            or not 1 <= len(logical_document_ids) <= 80
            or len(set(logical_document_ids)) != len(logical_document_ids)
            or set(document_aliases) != set(logical_document_ids)
            or len(set(document_aliases.values())) != len(document_aliases)
            or any(
                not isinstance(alias, str)
                or re.fullmatch(r"d[1-9][0-9]?", alias) is None
                or int(alias[1:]) > 80
                for alias in document_aliases.values()
            )
            or isinstance(timeout_seconds, bool)
            or not isinstance(timeout_seconds, int)
            or not 1 <= timeout_seconds <= 30
            or isinstance(max_parallel, bool)
            or not isinstance(max_parallel, int)
            or not 1 <= max_parallel <= 8
            or isinstance(shard_size, bool)
            or not isinstance(shard_size, int)
            or not 1 <= shard_size <= 20
        ):
            raise DeepInspectionError("deep_inspector_target_invalid")
        # Fail closed before launching any shard if even one requested
        # document is not present in this bound tenant/source authority.
        with self.store.connect() as connection:
            rows = connection.execute(
                """SELECT document.logical_document_id,
                          document.manifest_size_bytes
                          + COALESCE(SUM(part.size_bytes),0) AS size_bytes
                     FROM canonical_evidence_documents document
                     LEFT JOIN canonical_evidence_document_parts part
                       ON part.tenant_id=document.tenant_id
                      AND part.source_id=document.source_id
                      AND part.logical_document_id=
                          document.logical_document_id
                      AND part.revision=document.revision
                    WHERE document.tenant_id=%s
                      AND document.source_id=ANY(%s)
                      AND document.logical_document_id=ANY(%s)
                    GROUP BY document.logical_document_id,
                             document.manifest_size_bytes""",
                (
                    self.tenant_id,
                    list(self.authorized_sources),
                    list(logical_document_ids),
                ),
            ).fetchall()
        if {row["logical_document_id"] for row in rows} != set(
            logical_document_ids
        ):
            raise DeepInspectionError("deep_inspector_target_invalid")

        sizes = {
            row["logical_document_id"]: int(row["size_bytes"])
            for row in rows
        }
        # Keep the aggregate response bounded to eight shard outputs, then
        # greedily place the largest documents in the currently lightest bin.
        # This avoids one giant transcript becoming the serial tail of a wave.
        shard_count = min(
            MAX_AGENT_EXEC_MAP_SHARDS,
            (len(logical_document_ids) + shard_size - 1) // shard_size,
        )
        effective_shard_size = (
            len(logical_document_ids) + shard_count - 1
        ) // shard_count
        bins: list[tuple[list[str], int]] = [([], 0) for _ in range(shard_count)]
        order = {value: index for index, value in enumerate(logical_document_ids)}
        for document_id in sorted(
            logical_document_ids,
            key=lambda value: (-sizes[value], order[value]),
        ):
            eligible = [
                index
                for index, (documents, _total) in enumerate(bins)
                if len(documents) < effective_shard_size
            ]
            destination = min(
                eligible,
                key=lambda index: (bins[index][1], len(bins[index][0]), index),
            )
            documents, total = bins[destination]
            documents.append(document_id)
            bins[destination] = (documents, total + sizes[document_id])
        shards = tuple(
            (tuple(documents), total)
            for documents, total in bins
            if documents
        )
        started_at = time.monotonic()

        def execute(
            item: tuple[int, tuple[tuple[str, ...], int]],
        ) -> dict[str, Any]:
            shard_index, (document_ids, input_bytes) = item
            try:
                result = self.execute_agent_program(
                    program,
                    logical_document_ids=document_ids,
                    document_aliases={
                        document_id: document_aliases[document_id]
                        for document_id in document_ids
                    },
                    record_spans={document_id: () for document_id in document_ids},
                    routing_receipts={
                        document_id: () for document_id in document_ids
                    },
                    timeout_seconds=timeout_seconds,
                )
                stdout = result.get("stdout", "")
                stderr = result.get("stderr", "")
                stdout_bytes = stdout.encode()
                stderr_bytes = stderr.encode()
                output_truncated = (
                    len(stdout_bytes) > MAX_AGENT_EXEC_MAP_SHARD_STDOUT_BYTES
                    or len(stderr_bytes) > MAX_AGENT_EXEC_MAP_SHARD_STDERR_BYTES
                )
                if output_truncated:
                    stdout = stdout_bytes[
                        :MAX_AGENT_EXEC_MAP_SHARD_STDOUT_BYTES
                    ].decode(errors="ignore")
                    stderr = stderr_bytes[
                        :MAX_AGENT_EXEC_MAP_SHARD_STDERR_BYTES
                    ].decode(errors="ignore")
                    # Citation authority follows only evidence still visible to
                    # the caller after the map response is bounded.
                    visible_receipts = agent_evidence_receipts(stdout)
                    result = {
                        **result,
                        "stdout": stdout,
                        "stderr": stderr,
                        "opened_receipts": visible_receipts,
                        "complete": False,
                        "stopped_reason": "output_limit",
                        "output_truncated": True,
                    }
                return {
                    "shard": shard_index,
                    "input_bytes": input_bytes,
                    **result,
                }
            except DeepInspectionError:
                return {
                    "shard": shard_index,
                    "input_bytes": input_bytes,
                    "complete": False,
                    "stopped_reason": "provider_failure",
                    "stdout": "",
                    "stderr": "",
                    "opened_receipts": [],
                    "documents_available": len(document_ids),
                }

        with ThreadPoolExecutor(
            max_workers=min(max_parallel, len(shards)),
            thread_name_prefix="recall-archil-map",
        ) as executor:
            results = list(executor.map(execute, enumerate(shards)))
        opened_receipts = list(dict.fromkeys(
            receipt
            for result in results
            for receipt in result.get("opened_receipts", ())
        ))
        complete = all(bool(result.get("complete")) for result in results)
        return {
            "provider": next(
                (
                    result.get("provider")
                    for result in results
                    if isinstance(result.get("provider"), str)
                ),
                "archil",
            ),
            "complete": complete,
            "stopped_reason": "completed" if complete else "partial_failure",
            "opened_receipts": opened_receipts,
            "shards": results,
            "timing": {
                "elapsed_ms": round(
                    (time.monotonic() - started_at) * 1000,
                    3,
                ),
                "shard_count": len(shards),
                "max_parallel": min(max_parallel, len(shards)),
                "effective_shard_size": effective_shard_size,
                "input_bytes": sum(sizes.values()),
            },
        }

    @staticmethod
    def _project_agent_records(
        result: dict[str, Any],
        *,
        requested: set[str],
        aliases: dict[str, str],
        limit: int,
    ) -> tuple[list[dict[str, Any]], list[str]]:
        opened = set(result.get("opened_receipts", ()))
        matches: list[dict[str, Any]] = []
        visible_receipts: list[str] = []
        stdout = result.get("stdout", "")
        for line in stdout.splitlines() if isinstance(stdout, str) else ():
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            document_id = (
                record.get("logical_document_id")
                if isinstance(record, dict)
                else None
            )
            receipts = (
                record.get("receipts")
                if isinstance(record, dict)
                else None
            )
            if (
                document_id not in requested
                or document_id not in aliases
                or not isinstance(receipts, list)
                or not isinstance(record.get("content"), str)
                or not isinstance(record.get("ordinal"), int)
                or isinstance(record.get("ordinal"), bool)
                or record["ordinal"] < 0
            ):
                continue
            authoritative = [
                receipt
                for receipt in receipts
                if isinstance(receipt, str) and receipt in opened
            ]
            if not authoritative:
                continue
            for receipt in authoritative:
                if receipt not in visible_receipts:
                    visible_receipts.append(receipt)
            projected = {
                "document_alias": aliases[document_id],
                "record_ordinal": record["ordinal"],
                "event_native_id": record.get("event_native_id"),
                "occurred_at": record.get("occurred_at"),
                "content": record["content"],
                "content_start": record.get("content_start", 0),
                "content_end": record.get("content_end"),
                "content_length": record.get("content_length"),
                "content_byte_start": record.get("content_byte_start"),
                "content_byte_end": record.get("content_byte_end"),
                "content_length_bytes": record.get("content_length_bytes"),
                "content_complete": bool(
                    record.get("content_complete", False)
                ),
                "receipts": authoritative,
            }
            matches.append(projected)
            if len(matches) >= limit:
                break
        return matches, visible_receipts

    def find_documents(
        self,
        *,
        logical_document_ids: tuple[str, ...],
        document_aliases: dict[str, str],
        patterns: tuple[str, ...],
        context_chars: int,
        limit: int,
        record_spans: dict[str, tuple[tuple[int, int], ...]],
        routing_receipts: dict[str, tuple[str, ...]],
        timeout_seconds: int,
    ) -> dict[str, Any]:
        """Find agent-authored literal needles with match-centered excerpts."""

        if (
            not isinstance(logical_document_ids, tuple)
            or not 1 <= len(logical_document_ids) <= 20
            or len(logical_document_ids) != len(set(logical_document_ids))
            or not isinstance(patterns, tuple)
            or not 1 <= len(patterns) <= 5
            or any(
                not isinstance(pattern, str)
                or not pattern.strip()
                or len(pattern) > 512
                for pattern in patterns
            )
            or sum(len(pattern) for pattern in patterns) > 2_000
            or isinstance(context_chars, bool)
            or not isinstance(context_chars, int)
            or not 200 <= context_chars <= 4_000
            or isinstance(limit, bool)
            or not isinstance(limit, int)
            or not 1 <= limit <= 20
        ):
            raise DeepInspectionError("deep_inspector_find_invalid")
        command = ["recall-scan"]
        for document_id in logical_document_ids:
            command.extend(("--document", document_id))
        for pattern in patterns:
            command.extend(("--pattern", pattern))
        command.extend((
            "--fixed",
            "--broad",
            "--excerpt-chars",
            str(context_chars),
            "--limit",
            str(limit),
        ))
        result = self.execute_agent_program(
            shlex.join(command),
            logical_document_ids=logical_document_ids,
            document_aliases=document_aliases,
            record_spans={
                document_id: tuple(record_spans.get(document_id, ()))
                for document_id in logical_document_ids
            },
            routing_receipts={
                document_id: tuple(
                    routing_receipts.get(document_id, ())
                )
                for document_id in logical_document_ids
            },
            timeout_seconds=timeout_seconds,
        )
        if result.get("exit_code") not in {None, 0}:
            raise DeepInspectionError("deep_inspector_find_failed")
        matches, visible_receipts = self._project_agent_records(
            result,
            requested=set(logical_document_ids),
            aliases=document_aliases,
            limit=limit,
        )
        return {
            "provider": result.get("provider"),
            "matches": matches,
            "opened_receipts": visible_receipts,
            "complete": bool(result.get("complete")),
            "stopped_reason": result.get("stopped_reason"),
            "timing": result.get("timing"),
            "documents_available": result.get("documents_available"),
            "objects_available": result.get("objects_available"),
        }

    def open_document(
        self,
        *,
        logical_document_id: str,
        document_alias: str,
        cursor: str | None,
        record_ordinal: int | None,
        page_bytes: int,
        record_spans: dict[str, tuple[tuple[int, int], ...]],
        routing_receipts: dict[str, tuple[str, ...]],
        timeout_seconds: int,
    ) -> dict[str, Any]:
        """Open one complete document through deterministic cursor pages."""

        if (
            not isinstance(logical_document_id, str)
            or re.fullmatch(
                r"ldoc_[0-9a-f]{32}",
                logical_document_id,
            )
            is None
            or not isinstance(document_alias, str)
            or re.fullmatch(r"d[1-9][0-9]?", document_alias) is None
            or (
                cursor is not None
                and (
                    not isinstance(cursor, str)
                    or re.fullmatch(
                        r"\d{1,6}:\d{1,12}:\d{1,12}",
                        cursor,
                    )
                    is None
                )
            )
            or (
                record_ordinal is not None
                and (
                    isinstance(record_ordinal, bool)
                    or not isinstance(record_ordinal, int)
                    or record_ordinal < 0
                )
            )
            or (cursor is not None and record_ordinal is not None)
            or isinstance(page_bytes, bool)
            or not isinstance(page_bytes, int)
            or not 1_024 <= page_bytes <= 32_768
        ):
            raise DeepInspectionError("deep_inspector_open_invalid")
        command = [
            "recall-scan",
            "--document",
            logical_document_id,
            "--all",
            "--broad",
            "--cursor",
            cursor or "0:0:0",
            "--page-bytes",
            str(page_bytes),
            "--limit",
            "50",
        ]
        hinted_spans = tuple(record_spans.get(logical_document_id, ()))
        hinted_start = (
            hinted_spans[0][0]
            if cursor is None
            and record_ordinal is None
            and hinted_spans
            else None
        )
        selected_start = (
            record_ordinal
            if cursor is None and record_ordinal is not None
            else hinted_start
        )
        if selected_start is not None:
            command.extend(("--start-record", str(selected_start)))
        if record_ordinal is not None:
            command.append("--one-record")
        result = self.execute_agent_program(
            shlex.join(command),
            logical_document_ids=(logical_document_id,),
            document_aliases={logical_document_id: document_alias},
            record_spans={
                logical_document_id: tuple(
                    record_spans.get(logical_document_id, ())
                )
            },
            routing_receipts={
                logical_document_id: tuple(
                    routing_receipts.get(logical_document_id, ())
                )
            },
            timeout_seconds=timeout_seconds,
        )
        if result.get("exit_code") not in {None, 0}:
            raise DeepInspectionError("deep_inspector_open_failed")
        records, visible_receipts = self._project_agent_records(
            result,
            requested={logical_document_id},
            aliases={logical_document_id: document_alias},
            limit=50,
        )
        page = None
        stdout = result.get("stdout", "")
        for line in stdout.splitlines() if isinstance(stdout, str) else ():
            if not line.startswith("RECALL_PAGE "):
                continue
            try:
                candidate = json.loads(line.removeprefix("RECALL_PAGE "))
            except json.JSONDecodeError:
                continue
            if (
                isinstance(candidate, dict)
                and set(candidate)
                == {"complete", "emitted_bytes", "next_cursor"}
                and isinstance(candidate["complete"], bool)
                and isinstance(candidate["emitted_bytes"], int)
                and (
                    candidate["next_cursor"] is None
                    or (
                        isinstance(candidate["next_cursor"], str)
                        and re.fullmatch(
                            r"\d{1,6}:\d{1,12}:\d{1,12}",
                            candidate["next_cursor"],
                        )
                    )
                )
            ):
                page = candidate
                break
        if page is None:
            raise DeepInspectionError("deep_inspector_open_result_invalid")
        return {
            "provider": result.get("provider"),
            "document_alias": document_alias,
            "records": records,
            "opened_receipts": visible_receipts,
            "next_cursor": page["next_cursor"],
            "complete": page["complete"],
            "start_basis": (
                "record"
                if record_ordinal is not None
                else "hint"
                if hinted_start is not None
                else "beginning"
                if cursor is None
                else "cursor"
            ),
            "page_bytes": page["emitted_bytes"],
            "timing": result.get("timing"),
            "documents_available": result.get("documents_available"),
            "objects_available": result.get("objects_available"),
        }

    def inspect_documents(
        self,
        *,
        logical_document_ids: tuple[str, ...],
        query: str | None,
        scope: str,
        literal: bool,
        context: int,
        limit: int,
        record_spans: dict[str, tuple[tuple[int, int], ...]],
        routing_receipts: dict[str, tuple[str, ...]],
        timeout_seconds: int,
    ) -> dict[str, Any]:
        """Run one structured, agent-authored search over admitted documents."""

        if (
            not isinstance(logical_document_ids, tuple)
            or not 1 <= len(logical_document_ids) <= 20
            or len(logical_document_ids) != len(set(logical_document_ids))
            or any(
                not isinstance(document_id, str)
                or re.fullmatch(r"ldoc_[0-9a-f]{32}", document_id) is None
                for document_id in logical_document_ids
            )
            or (
                query is not None
                and (
                    not isinstance(query, str)
                    or not query.strip()
                    or len(query) > 4_000
                )
            )
            or scope not in {"pointers", "full_documents"}
            or not isinstance(literal, bool)
            or isinstance(context, bool)
            or not isinstance(context, int)
            or not 0 <= context <= 5
            or isinstance(limit, bool)
            or not isinstance(limit, int)
            or not 1 <= limit <= 20
        ):
            raise DeepInspectionError("deep_inspector_inspect_invalid")
        if scope == "pointers" and not any(
            record_spans.get(document_id)
            for document_id in logical_document_ids
        ):
            return {
                "provider": "canonical",
                "scope": scope,
                "matches": [],
                "opened_receipts": [],
                "complete": True,
                "stopped_reason": "no_pointer_windows",
            }
        command = ["recall-scan"]
        for document_id in logical_document_ids:
            command.extend(("--document", document_id))
        if query is None:
            command.append("--all")
        else:
            command.extend(("--pattern", query))
            if literal:
                command.append("--fixed")
        if scope == "full_documents":
            command.append("--broad")
        command.extend(("--context", str(context), "--limit", str(limit)))
        result = self.execute_agent_program(
            shlex.join(command),
            logical_document_ids=logical_document_ids,
            record_spans={
                document_id: tuple(record_spans.get(document_id, ()))
                for document_id in logical_document_ids
            },
            routing_receipts={
                document_id: tuple(
                    routing_receipts.get(document_id, ())
                )
                for document_id in logical_document_ids
            },
            timeout_seconds=timeout_seconds,
        )
        if result.get("exit_code") not in {None, 0}:
            raise DeepInspectionError("deep_inspector_inspect_failed")
        opened = set(result.get("opened_receipts", ()))
        requested = set(logical_document_ids)
        matches: list[dict[str, Any]] = []
        visible_receipts: list[str] = []
        stdout = result.get("stdout", "")
        for line in stdout.splitlines() if isinstance(stdout, str) else ():
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            document_id = (
                record.get("logical_document_id")
                if isinstance(record, dict)
                else None
            )
            receipts = (
                record.get("receipts")
                if isinstance(record, dict)
                else None
            )
            if (
                document_id not in requested
                or not isinstance(receipts, list)
                or not isinstance(record.get("content"), str)
                or not isinstance(record.get("ordinal"), int)
                or isinstance(record.get("ordinal"), bool)
                or record["ordinal"] < 0
            ):
                continue
            authoritative = [
                receipt
                for receipt in receipts
                if isinstance(receipt, str) and receipt in opened
            ]
            if not authoritative:
                continue
            for receipt in authoritative:
                if receipt not in visible_receipts:
                    visible_receipts.append(receipt)
            matches.append({
                "logical_document_id": document_id,
                "record_ordinal": record["ordinal"],
                "event_native_id": record.get("event_native_id"),
                "occurred_at": record.get("occurred_at"),
                "content": record["content"],
                "receipts": authoritative,
            })
            if len(matches) >= limit:
                break
        return {
            "provider": result.get("provider"),
            "scope": scope,
            "matches": matches,
            "opened_receipts": visible_receipts,
            "complete": bool(result.get("complete")),
            "stopped_reason": result.get("stopped_reason"),
            "timing": result.get("timing"),
            "documents_available": result.get("documents_available"),
            "objects_available": result.get("objects_available"),
        }

    def _receipt_event(
        self,
        connection: Any,
        target: str,
        *,
        deadline_at: float | None = None,
    ) -> dict[str, Any] | None:
        redirect = self.store._execute_bounded(
            connection,
            """SELECT new_receipt FROM receipt_redirects
               WHERE tenant_id=%s AND old_receipt=%s""",
            (self.tenant_id, target),
            deadline_at,
        ).fetchone()
        if redirect:
            target = redirect["new_receipt"]
        row = self.store._execute_bounded(
            connection,
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
            deadline_at,
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
        _deadline_at: float | None = None,
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
            anchor = self._receipt_event(
                connection,
                target,
                deadline_at=_deadline_at,
            )
            if anchor is None:
                return None
            parent = anchor["native_parent_id"] or anchor["native_id"]

            def neighbors(direction: str, limit: int) -> list[dict[str, Any]]:
                if limit == 0:
                    return []
                comparator = "<" if direction == "before" else ">"
                ordering = "DESC" if direction == "before" else "ASC"
                return self.store._execute_bounded(
                    connection,
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
                    _deadline_at,
                ).fetchall()

            previous = list(reversed(neighbors("before", before)))
            following = neighbors("after", after)
            anchor_chunks = self.store._execute_bounded(
                connection,
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
                _deadline_at,
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
        started_at = time.monotonic()
        budgets = {
            "quick": {
                "families": 1,
                "sessions": 2,
                "context": 2,
                "deadline_seconds": 5,
                "max_response_bytes": 900_000,
            },
            "normal": {
                "families": 4,
                "sessions": 4,
                "context": 4,
                "deadline_seconds": 15,
                "max_response_bytes": 900_000,
            },
            "deep": {
                "families": 8,
                "sessions": 6,
                "context": 6,
                "deadline_seconds": 30,
                "max_response_bytes": 900_000,
            },
        }
        if depth not in budgets:
            raise ValueError("invalid canonical investigation depth")
        budget = budgets[depth]
        deadline_at = started_at + budget["deadline_seconds"]
        effective_filters = dict(filters or {})
        (
            source_id,
            source_family,
            source_alias,
            source_connector,
            _,
            _,
        ) = self._filters(self._routing_filters(effective_filters))
        time_reason = "explicit"
        if "since" not in effective_filters and "until" not in effective_filters:
            since, until, time_reason = self._question_time_window(question)
            if since is not None:
                effective_filters["since"] = since
                effective_filters["until"] = until
        eligible_sources = self._sources(
            source_id=source_id,
            source_family=source_family,
            source_alias=source_alias,
            source_connector=source_connector,
        )

        probes: list[dict[str, Any]] = []
        first = self._investigation_probe(
            self.passage_hints(question, effective_filters, 20)
        )
        probes.append(first)
        if not any(
            name in effective_filters
            for name in (
                "source_id",
                "source_family",
                "source_alias",
                "source_connector",
            )
        ):
            try:
                with self.store.connect() as connection:
                    rows = self.store._execute_bounded(
                        connection,
                        """SELECT family,count(*) AS source_count
                           FROM source_profiles
                           WHERE source_id=ANY(%s)
                           GROUP BY family
                           ORDER BY source_count DESC,family
                           LIMIT %s""",
                        (eligible_sources, budget["families"]),
                        deadline_at,
                    ).fetchall()
            except SearchDeadlineExceeded:
                rows = []
            for row in rows:
                if time.monotonic() - started_at >= budget["deadline_seconds"]:
                    break
                family_filters = {
                    **effective_filters,
                    "source_family": row["family"],
                }
                probes.append(self._investigation_probe(
                    self.passage_hints(question, family_filters, 8)
                ))
        person = effective_filters.get("person")
        if (
            isinstance(person, str)
            and not any(
                name in effective_filters
                for name in (
                    "source_id",
                    "source_family",
                    "source_alias",
                    "source_connector",
                )
            )
        ):
            represented_sources = {
                result["source_id"]
                for probe in probes
                for result in probe["results"]
            }
            actor_sources = self._actor_sources(
                person,
                effective_filters.get("person_relation"),
                eligible_sources,
                deadline_at,
            )
            missing_sources = [
                source for source in actor_sources
                if source not in represented_sources
            ][:budget["families"]]
            for actor_source in missing_sources:
                if time.monotonic() >= deadline_at:
                    break
                source_filters = {
                    **effective_filters,
                    "source_id": actor_source,
                }
                probes.append(self._investigation_probe(
                    self.passage_hints(question, source_filters, 8)
                ))

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
            if time.monotonic() - started_at >= budget["deadline_seconds"]:
                break
            try:
                context = self.session_context(
                    result["receipt"],
                    before=budget["context"],
                    after=budget["context"],
                    _deadline_at=deadline_at,
                )
            except SearchDeadlineExceeded:
                break
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
        while (
            len(json.dumps(investigations, default=str).encode())
            > budget["max_response_bytes"] - 100_000
            and investigations
        ):
            investigations.pop()

        source_ids = sorted({
            item["match"]["source_id"] for item in investigations
        })
        source_families: list[str] = []
        if source_ids:
            try:
                with self.store.connect() as connection:
                    rows = self.store._execute_bounded(
                        connection,
                        """SELECT DISTINCT family FROM source_profiles
                           WHERE source_id=ANY(%s) ORDER BY family""",
                        (source_ids,),
                        deadline_at,
                    ).fetchall()
            except SearchDeadlineExceeded:
                rows = []
            source_families = [row["family"] for row in rows]
        accounting_status = "ok"
        try:
            with self.store.connect() as connection:
                configured_rows = self.store._execute_bounded(
                    connection,
                    """SELECT source_id
                       FROM canonical_sources
                       WHERE tenant_id=%s
                         AND source_id=ANY(%s)
                       ORDER BY source_id""",
                    (self.tenant_id, list(self.authorized_sources)),
                    deadline_at,
                ).fetchall()
        except SearchDeadlineExceeded:
            configured_rows = []
            accounting_status = "deadline-exceeded"
        configured = {row["source_id"] for row in configured_rows}
        if accounting_status != "ok":
            # Search evidence already proved these grants are live. Optional
            # accounting must never turn a healthy source into a false outage.
            configured.update(source_ids)
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
                "source_accounting": {
                    "searched": eligible_sources,
                    "stale": [],
                    "filtered": sorted(
                        set(self.authorized_sources) - set(eligible_sources)
                    ),
                    "unavailable": sorted(
                        set(self.authorized_sources) - set(configured)
                    ),
                    "freshness_status": "not_evaluated",
                    "accounting_status": accounting_status,
                    "stale_after_seconds": 7 * 24 * 60 * 60,
                },
            },
            "uncertainty": uncertainty,
            "diagnostics": {
                "engine": "canonical-investigator-v1",
                "candidate_engines": sorted({
                    probe.get("diagnostics", {}).get("engine")
                    for probe in probes
                    if probe.get("diagnostics", {}).get("engine")
                }),
                "search_probes": len(probes),
                "unique_candidates": len(combined),
                "expanded_sessions": len(investigations),
                "bounds": budget,
                "elapsed_ms": round(
                    (time.monotonic() - started_at) * 1000,
                    3,
                ),
                "deadline_reached": (
                    time.monotonic() - started_at
                    >= budget["deadline_seconds"]
                ),
            },
        }

    def _parent_scoped_receipts(
        self,
        *,
        source_id: str,
        parent_id: str,
        terms: list[str],
        filters: dict[str, Any] | None,
        limit: int,
    ) -> tuple[str, ...]:
        """Rank objective-bearing evidence inside one authorized session."""

        if (
            source_id not in self.authorized_sources
            or not isinstance(parent_id, str)
            or not parent_id
            or not isinstance(terms, list)
            or not terms
            or len(terms) > 16
            or any(not isinstance(term, str) or not term for term in terms)
            or not 1 <= limit <= 100
        ):
            return ()
        search_query = " OR ".join(f'"{term}"' for term in terms)
        _, _, _, _, since, until = self._filters(
            self._routing_filters(filters or {})
        )
        deadline_at = time.monotonic() + self.store.search_deadline_ms / 1000
        try:
            with self.store.connect() as connection:
                rows = self.store._execute_bounded(
                    connection,
                    """WITH session_documents AS MATERIALIZED (
                         SELECT document.tenant_id,document.source_id,
                                document.document_id
                         FROM canonical_events event
                         JOIN canonical_documents document
                           USING(tenant_id,source_id,event_id)
                         WHERE event.tenant_id=%s
                           AND event.source_id=%s
                           AND COALESCE(
                                 event.native_parent_id,event.native_id
                               )=%s
                           AND (%s::timestamptz IS NULL
                                OR event.occurred_at>=%s)
                           AND (%s::timestamptz IS NULL
                                OR event.occurred_at<=%s)
                           AND document.is_current
                           AND document.deleted_at IS NULL
                       )
                       SELECT chunk.receipt,
                              (SELECT count(*)
                                 FROM unnest(%s::text[]) query_term(value)
                                WHERE chunk.search_vector @@
                                      plainto_tsquery(
                                          'simple',query_term.value
                                      )
                              ) AS matched_term_count,
                              ts_rank_cd(
                                  chunk.search_vector,
                                  websearch_to_tsquery('simple',%s),
                                  32
                              ) AS score
                       FROM session_documents document
                       JOIN canonical_chunks chunk
                         USING(tenant_id,source_id,document_id)
                       WHERE chunk.deleted_at IS NULL
                         AND chunk.search_vector @@
                             websearch_to_tsquery('simple',%s)
                       ORDER BY matched_term_count DESC,score DESC,
                                chunk.chunk_id
                       LIMIT %s""",
                    (
                        self.tenant_id,
                        source_id,
                        parent_id,
                        since,
                        since,
                        until,
                        until,
                        terms,
                        search_query,
                        search_query,
                        limit,
                    ),
                    deadline_at,
                ).fetchall()
        except SearchDeadlineExceeded:
            return ()
        return tuple(dict.fromkeys(row["receipt"] for row in rows))

    def _exact_session_receipts(
        self,
        question: str,
        investigation: dict[str, Any],
        filters: dict[str, Any] | None,
        *,
        limit: int,
    ) -> tuple[str, ...]:
        """Rank evidence inside the session routed by an explicit UUID."""

        if UUID_RE.search(question) is None:
            return ()
        investigations = investigation.get("investigations")
        if not isinstance(investigations, list) or not investigations:
            return ()
        match = investigations[0].get("match", {})
        terms = legacy_engine().informative_terms(
            UUID_RE.sub(" ", question)
        )[:16]
        return self._parent_scoped_receipts(
            source_id=match.get("source_id"),
            parent_id=match.get("native_parent_id"),
            terms=terms,
            filters=filters,
            limit=limit,
        )

    def deep_search(
        self,
        question: str,
        *,
        filters: dict[str, Any] | None = None,
        depth: str = "normal",
        authorized_source: Any = None,
        _seed_receipts: tuple[str, ...] | None = None,
    ) -> dict[str, Any]:
        """Deepen authorized Recall candidates in full evidence objects."""
        budgets = {
            "quick": DeepInspectionBudget(
                max_files=6,
                max_matches=12,
                max_output_bytes=32_000,
                timeout_seconds=8,
                concurrency=4,
            ),
            "normal": DeepInspectionBudget(
                max_files=20,
                max_matches=50,
                max_output_bytes=96_000,
                timeout_seconds=20,
                concurrency=8,
            ),
            "deep": DeepInspectionBudget(
                max_files=60,
                max_matches=150,
                max_output_bytes=128_000,
                timeout_seconds=30,
                concurrency=16,
            ),
        }
        if depth not in budgets:
            raise ValueError("invalid canonical deep-search depth")
        configured_tenant = getattr(
            self.evidence_projector,
            "bound_tenant_id",
            None,
        )
        if (
            self.evidence_projector is None
            or self.deep_inspector is None
            or not callable(getattr(self.deep_inspector, "inspect", None))
            or (
                configured_tenant is not None
                and configured_tenant != self.tenant_id
            )
        ):
            return {
                "status": "unavailable",
                "question": question,
                "findings": [],
                "coverage": {
                    "candidate_files": 0,
                    "files_scanned": 0,
                    "complete": False,
                    "reason": (
                        "deep_inspector_not_configured"
                        if configured_tenant in {None, self.tenant_id}
                        else "deep_inspector_not_configured_for_brain"
                    ),
                },
                "uncertainty": [
                    "Deep inspection is not configured for this Recall deployment."
                ],
            }
        budget = budgets[depth]
        if _seed_receipts is not None:
            if (
                not isinstance(_seed_receipts, tuple)
                or not 1 <= len(_seed_receipts) <= 32
                or len(_seed_receipts) != len(set(_seed_receipts))
                or any(
                    not isinstance(receipt, str)
                    or not receipt.startswith("recall://")
                    or len(receipt) > 2048
                    for receipt in _seed_receipts
                )
            ):
                raise ValueError("invalid canonical map seed")
            (
                _,
                source_family,
                source_alias,
                source_connector,
                since,
                until,
            ) = self._filters(self._routing_filters(filters or {}))
            eligible_sources = set(self._sources(
                source_id=(filters or {}).get("source_id"),
                source_family=source_family,
                source_alias=source_alias,
                source_connector=source_connector,
            ))
            since_bound = (
                datetime.fromisoformat(since.replace("Z", "+00:00"))
                if since is not None
                else None
            )
            until_bound = (
                datetime.fromisoformat(until.replace("Z", "+00:00"))
                if until is not None
                else None
            )
            resolved = []
            seed_parents: list[tuple[str, str]] = []
            with self.store.connect() as connection:
                for receipt in _seed_receipts:
                    row = self._receipt_event(connection, receipt)
                    occurred_at = row["occurred_at"] if row is not None else None
                    if (
                        row is None
                        or row["source_id"] not in eligible_sources
                        or (
                            since_bound is not None
                            and occurred_at < since_bound
                        )
                        or (
                            until_bound is not None
                            and occurred_at > until_bound
                        )
                    ):
                        raise ValueError(
                            "canonical map seed escaped its hard scope"
                        )
                    resolved.append(row["resolved_receipt"])
                    parent_id = (
                        row["native_parent_id"] or row["native_id"]
                    )
                    parent = (row["source_id"], parent_id)
                    if parent not in seed_parents:
                        seed_parents.append(parent)
            parent_receipts: list[str] = []
            terms = _informative_query_terms(question)
            bounded_parents = seed_parents[:8]
            per_parent_limit = max(
                1,
                budget.max_files // max(1, len(bounded_parents)),
            )
            for source_id, parent_id in bounded_parents:
                parent_receipts.extend(
                    self._parent_scoped_receipts(
                        source_id=source_id,
                        parent_id=parent_id,
                        terms=terms,
                        filters=filters,
                        limit=min(per_parent_limit, 100),
                    )
                )
            receipts = tuple(dict.fromkeys((
                *parent_receipts,
                *resolved,
            )))[: budget.max_files]
            route_coverage = {
                "mode": "seeded",
                "candidates": len(receipts),
                "seed_sessions": len(seed_parents),
                "session_candidate_receipts": len(parent_receipts),
            }
            uncertainty = []
        else:
            investigation = self.investigate(
                question,
                filters=filters,
                depth=depth,
                authorized_source=authorized_source,
            )
            receipts = tuple(
                dict.fromkeys(
                    chunk["receipt"]
                    for item in investigation["investigations"]
                    for event in item["context"]["events"]
                    for chunk in event["chunks"]
                )
            )
            exact_session_receipts = self._exact_session_receipts(
                question,
                investigation,
                filters,
                limit=min(budget.max_files, 100),
            )
            receipts = tuple(
                dict.fromkeys((*exact_session_receipts, *receipts))
            )
            route_coverage = {
                **investigation["coverage"],
                "exact_session_candidate_receipts": len(
                    exact_session_receipts
                ),
            }
            uncertainty = list(investigation["uncertainty"])
        selected = self.evidence_projector.targets_for_receipts(
            tenant_id=self.tenant_id,
            source_ids=self.authorized_sources,
            receipts=receipts,
            limit=min(budget.max_files + 1, 100),
        )
        selection_complete = len(selected) <= budget.max_files
        selected_targets = selected[: budget.max_files]
        targets = tuple(
            EvidenceTarget.from_reference(
                item["reference"],
                receipts=item["receipts"],
            )
            for item in selected_targets
        )
        deep = self.deep_inspector.inspect(
            tenant_id=self.tenant_id,
            question=question,
            targets=targets,
            budget=budget,
        )
        verified = []
        with self.store.connect() as connection:
            for finding in deep["findings"]:
                row = self._receipt_event(connection, finding["receipt"])
                if (
                    row is not None
                    and row["source_id"] in self.authorized_sources
                ):
                    verified.append({
                        **finding,
                        "source_id": row["source_id"],
                        "native_id": row["native_id"],
                        "native_parent_id": row["native_parent_id"],
                        "occurred_at": _timestamp(row["occurred_at"]),
                        "time_basis": "occurred_at",
                    })
        if len(verified) != len(deep["findings"]):
            uncertainty.append(
                "One or more deep findings became unavailable during verification."
            )
        if not deep["complete"]:
            uncertainty.append("Deep inspection returned partial coverage.")
        if not selection_complete:
            uncertainty.append(
                "Candidate evidence exceeded the deep-inspection file bound."
            )
        return {
            "status": "complete",
            "question": question,
            "findings": verified,
            "coverage": {
                "candidate_receipts": len(receipts),
                "candidate_files": len(selected),
                "candidate_files_truncated": not selection_complete,
                "files_scanned": deep["files_scanned"],
                "complete": bool(deep["complete"]) and selection_complete,
                "stopped_reason": (
                    deep["stopped_reason"]
                    if selection_complete
                    else "max_files"
                ),
                "provider": deep["provider"],
                "recall": route_coverage,
            },
            "uncertainty": uncertainty,
            "diagnostics": {
                "engine": "canonical-deep-search-v1",
                "depth": depth,
                "budget": {
                    "max_files": budget.max_files,
                    "max_matches": budget.max_matches,
                    "max_output_bytes": budget.max_output_bytes,
                    "timeout_seconds": budget.timeout_seconds,
                },
                "provider_timing": deep["timing"],
            },
        }

    def map_reduce_search(
        self,
        question: str,
        *,
        maps: list[dict[str, Any]],
        depth: str = "normal",
    ) -> dict[str, Any]:
        """Run agent-authored retrieval maps concurrently for later reduction.

        The calling agent owns semantic decomposition and final synthesis. Recall
        owns the hard boundary: every seed came from prior hybrid routing and is
        rechecked against tenant, source, and time scope before Archil receives a
        bounded object list.
        """
        if (
            not isinstance(question, str)
            or not question.strip()
            or len(question) > 8192
            or not isinstance(maps, list)
            or not 1 <= len(maps) <= MAX_AGENTIC_MAPS
            or depth not in {"quick", "normal", "deep"}
        ):
            raise ValueError("invalid canonical map-reduce request")
        normalized: list[dict[str, Any]] = []
        seen_ids: set[str] = set()
        for item in maps:
            if not isinstance(item, dict) or set(item) != {
                "map_id",
                "objective",
                "query",
                "filters",
                "seed_receipts",
            }:
                raise ValueError("invalid canonical map-reduce request")
            map_id = item["map_id"]
            objective = item["objective"]
            query = item["query"]
            filters = item["filters"]
            seed_receipts = item["seed_receipts"]
            if (
                not isinstance(map_id, str)
                or not re.fullmatch(r"[a-z][a-z0-9_-]{0,31}", map_id)
                or map_id in seen_ids
                or not isinstance(objective, str)
                or not objective.strip()
                or len(objective) > 1024
                or not isinstance(query, str)
                or not query.strip()
                or len(query) > 8192
                or not isinstance(filters, dict)
                or not isinstance(seed_receipts, list)
                or not 1 <= len(seed_receipts) <= 32
                or len(seed_receipts) != len(set(seed_receipts))
                or any(
                    not isinstance(receipt, str)
                    or not receipt.startswith("recall://")
                    or len(receipt) > 2048
                    for receipt in seed_receipts
                )
            ):
                raise ValueError("invalid canonical map-reduce request")
            self._filters(filters)
            seen_ids.add(map_id)
            normalized.append({
                "map_id": map_id,
                "objective": objective,
                "query": query,
                "filters": dict(filters),
                "seed_receipts": list(seed_receipts),
            })

        started_at = time.monotonic()

        def run_map(item: dict[str, Any]) -> dict[str, Any]:
            result = self.deep_search(
                item["query"],
                filters=item["filters"],
                depth=depth,
                _seed_receipts=tuple(item["seed_receipts"]),
            )
            bounded_findings: list[dict[str, Any]] = []
            for finding in result["findings"]:
                candidate = [*bounded_findings, finding]
                if (
                    len(candidate) > MAX_AGENTIC_MAP_FINDINGS
                    or len(
                        json.dumps(
                            candidate,
                            ensure_ascii=False,
                            separators=(",", ":"),
                        ).encode()
                    )
                    > MAX_AGENTIC_MAP_FINDING_BYTES
                ):
                    break
                bounded_findings.append(finding)
            coverage = dict(result["coverage"])
            uncertainty = list(result["uncertainty"])
            if len(bounded_findings) != len(result["findings"]):
                coverage["complete"] = False
                coverage["stopped_reason"] = "map_output_bound"
                uncertainty.append(
                    "Map evidence was truncated at the agentic output bound."
                )
            coverage["evidence_found"] = bool(bounded_findings)
            if not bounded_findings:
                uncertainty.append(
                    "No evidence matched this map objective; reformulation may be required."
                )
            return {
                "map_id": item["map_id"],
                "objective": item["objective"],
                "query": item["query"],
                "filters": item["filters"],
                "status": result["status"],
                "findings": bounded_findings,
                "coverage": coverage,
                "uncertainty": uncertainty,
            }

        with ThreadPoolExecutor(
            max_workers=min(len(normalized), MAX_AGENTIC_MAPS),
            thread_name_prefix="recall-map",
        ) as executor:
            results = list(executor.map(run_map, normalized))

        findings = [
            finding
            for item in results
            for finding in item["findings"]
        ]
        unique_receipts = {
            finding["receipt"]
            for finding in findings
            if isinstance(finding, dict)
            and isinstance(finding.get("receipt"), str)
        }
        complete_maps = sum(
            bool(item["coverage"].get("complete"))
            for item in results
            if isinstance(item.get("coverage"), dict)
        )
        maps_with_evidence = sum(
            bool(item["coverage"].get("evidence_found"))
            for item in results
            if isinstance(item.get("coverage"), dict)
        )
        return {
            "contract": "recall.agentic-map-reduce.v1",
            "question": question,
            "maps": results,
            "coverage": {
                "maps": len(results),
                "complete_maps": complete_maps,
                "complete": complete_maps == len(results),
                "maps_with_evidence": maps_with_evidence,
                "evidence_found_for_every_map": maps_with_evidence == len(results),
                "unique_receipts": len(unique_receipts),
            },
            "diagnostics": {
                "engine": "canonical-agentic-map-reduce-v1",
                "elapsed_ms": round(
                    (time.monotonic() - started_at) * 1000,
                    3,
                ),
                "parallelism": min(len(normalized), MAX_AGENTIC_MAPS),
                "reducer": "agent",
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
        return CanonicalPlane(
            self.store,
            self.archive,
            self.evidence_projector,
        ).forget(
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
