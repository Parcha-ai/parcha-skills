from __future__ import annotations

import hashlib
import json
import os
import re
import secrets
import threading
import time
import uuid
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

import psycopg
from psycopg_pool import ConnectionPool
from psycopg.rows import dict_row

from . import PROJECTOR_VERSION
from .actor_attribution import (
    actor_id_for_employee_key,
    actor_id_for_principal,
    claimable_actor_for_display_name,
)
from .authorization import normalize_verified_email
from .capture import (
    CAPTURE_ORIGIN_RE,
    build_capture_event,
    build_forget_event,
    parse_capture_receipt,
)
from .federation import SOURCE_FAMILIES, SourceProfile, freshness_score, normalized_evidence
from .projectors import KIND_RE, SOURCE_ID_RE, advisory_lock_key, canonical_json, effective_session_id, event_receipt, legacy_engine, partial_lexical_probes, phrase_query_spec, preferred_phrase_probes, project, redact_text, validate_envelope
from .ranking import DEFAULT_SEARCH_DEADLINE_MS, evidence_rank_components, should_run_partial
from .semantic import SemanticRuntime

MAX_SEARCH_RESULT_TEXT_CHARS = 4096
TURN_USER_MARKER = "User request:\n"
TURN_ASSISTANT_MARKER = "\nAssistant response:\n"
TURN_CONTINUATION_MARKER = "\n\nAssistant continuation:\n"
TURN_EMBEDDING_CONTRACT = "recall.turn-embedding.v1:question-complete-response"
V2_AUTHORITY_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9:._/@+-]{1,255}\Z")


class IdempotencyConflict(Exception):
    pass


class SearchDeadlineExceeded(Exception):
    pass


def bounded_search_text(value: str) -> tuple[str, bool]:
    """Return an agent-sized search snippet; show resolves the full receipt."""
    return value[:MAX_SEARCH_RESULT_TEXT_CHARS], len(value) > MAX_SEARCH_RESULT_TEXT_CHARS


def turn_embedding_text(user_text: str, assistant_texts: Iterable[str]) -> str:
    """Represent a conversational turn with the question and its complete response."""
    responses = list(assistant_texts)
    if not user_text.strip() or not responses or any(not value.strip() for value in responses):
        raise ValueError("turn embedding requires non-empty user and assistant text")
    return (
        TURN_USER_MARKER
        + user_text
        + TURN_ASSISTANT_MARKER
        + TURN_CONTINUATION_MARKER.join(responses)
    )


def turn_runtime_fingerprint(semantic_fingerprint: str) -> str:
    return hashlib.sha256(
        f"{TURN_EMBEDDING_CONTRACT}\0{semantic_fingerprint}".encode()
    ).hexdigest()


def semantic_candidate_limit(result_limit: int) -> int:
    """Keep enough dense anchors for deduplication without broad payload reads."""
    return min(100, max(20, result_limit * 4))


def optional_rescue_deadline(
    *,
    overall_deadline_at: float,
    now: float,
    search_deadline_ms: int,
) -> float:
    """Give all optional lexical rescues one strict sub-budget."""
    budget_ms = min(800, max(100, round(search_deadline_ms * 0.3)))
    return min(overall_deadline_at, now + budget_ms / 1000)


def enough_session_anchors(rows: Iterable[dict], *, result_limit: int) -> bool:
    """Stop rescues once ranking can fill the requested session slots."""
    anchors = {
        (row["source_id"], row["session_native_id"])
        for row in rows
    }
    return len(anchors) >= max(1, result_limit - 1)


def should_run_optional_rescue(
    *,
    exact_question_count: int,
    rows: Iterable[dict],
    result_limit: int,
) -> bool:
    """Rescue only when exact adjacency and session diversity are both absent."""
    return (
        exact_question_count == 0
        and not enough_session_anchors(rows, result_limit=result_limit)
    )


def related_candidate_limit(result_limit: int) -> int:
    """Bound fast related lookups while leaving room for path filtering."""
    return min(400, max(100, result_limit * 20))


class BrainStore:
    def __init__(self, dsn: str, search_deadline_ms: int | None = None,
                 semantic_runtime: SemanticRuntime | None = None,
                 semantic_minimum_similarity: float | None = None,
                 pool_max_size: int | None = None):
        self.dsn = dsn
        self._pool: ConnectionPool | None = None
        self._pool_lock = threading.Lock()
        try:
            configured_pool_size = (
                pool_max_size
                if pool_max_size is not None
                else int(os.environ.get("RECALL_DATABASE_POOL_MAX_SIZE", "8"))
            )
        except ValueError as exc:
            raise ValueError(
                "database pool size must be between 4 and 32"
            ) from exc
        if not 4 <= configured_pool_size <= 32:
            raise ValueError("database pool size must be between 4 and 32")
        self.pool_max_size = configured_pool_size
        configured = search_deadline_ms if search_deadline_ms is not None else int(os.environ.get("RECALL_SEARCH_DEADLINE_MS", str(DEFAULT_SEARCH_DEADLINE_MS)))
        if not 10 <= configured <= 30_000:
            raise ValueError(
                "search deadline must be between 10 and 30000 milliseconds"
            )
        self.search_deadline_ms = configured
        try:
            similarity = (
                semantic_minimum_similarity
                if semantic_minimum_similarity is not None
                else float(
                    os.environ.get(
                        "RECALL_SEMANTIC_MINIMUM_SIMILARITY",
                        "0.35",
                    )
                )
            )
        except ValueError as exc:
            raise ValueError(
                "semantic minimum similarity must be between 0 and 1"
            ) from exc
        if not 0 <= similarity <= 1:
            raise ValueError(
                "semantic minimum similarity must be between 0 and 1"
            )
        self.semantic_minimum_similarity = similarity
        if semantic_runtime is not None and semantic_runtime.dimensions != 512:
            raise ValueError("BrainStore semantic runtime requires 512 dimensions")
        self.semantic_runtime = semantic_runtime

    def connect(self):
        if self._pool is None:
            with self._pool_lock:
                if self._pool is None:
                    self._pool = ConnectionPool(
                        self.dsn,
                        kwargs={"row_factory": dict_row},
                        min_size=1,
                        max_size=self.pool_max_size,
                        timeout=5,
                        max_idle=300,
                        max_lifetime=1800,
                        open=True,
                        name="recall-brain",
                    )
        return self._pool.connection()

    def prepare_pool(self, minimum_size: int, *, timeout: float = 60.0) -> None:
        """Pre-warm bounded parallel workers before their measured work starts."""

        if (
            isinstance(minimum_size, bool)
            or not isinstance(minimum_size, int)
            or not 1 <= minimum_size <= self.pool_max_size
            or not 1 <= timeout <= 300
        ):
            raise ValueError("database pool warmup budget is invalid")
        with self.connect():
            pass
        assert self._pool is not None
        self._pool.resize(minimum_size, self.pool_max_size)
        self._pool.wait(timeout=timeout)

    def close(self) -> None:
        if self._pool is not None:
            self._pool.close()

    def migrate(self) -> None:
        schema_dir = Path(__file__).resolve().parents[1] / "schema"
        with self.connect() as conn:
            for schema in sorted(schema_dir.glob("*.sql")):
                conn.execute(schema.read_text())

    def create_collector_token(
        self,
        name: str,
        source_id: str | None,
        scopes: list[str],
        *,
        tenant_id: str | None = None,
        principal_id: str | None = None,
        capture_origin: str | None = None,
        webhook_privacy_mode: str | None = None,
    ) -> dict:
        allowed = {"read", "write", "metrics", "webhook"}
        if not name or not scopes or set(scopes) - allowed:
            raise ValueError("invalid collector credential")
        if "write" in scopes and not source_id:
            raise ValueError("write credential requires a source")
        if tenant_id is not None and (
            not V2_AUTHORITY_RE.fullmatch(tenant_id)
            or "write" not in scopes
            or not isinstance(source_id, str)
            or not V2_AUTHORITY_RE.fullmatch(source_id)
            or not isinstance(principal_id, str)
            or not V2_AUTHORITY_RE.fullmatch(principal_id)
        ):
            raise ValueError("invalid canonical tenant credential")
        if capture_origin is not None and (
            "write" not in scopes
            or not source_id
            or not principal_id
            or not CAPTURE_ORIGIN_RE.fullmatch(capture_origin)
        ):
            raise ValueError("invalid capture credential")
        if (
            ("webhook" in scopes)
            != (
                bool(source_id)
                and bool(principal_id)
                and webhook_privacy_mode in {"scrub", "drop"}
            )
            or ("webhook" in scopes and set(scopes) != {"webhook"})
        ):
            raise ValueError("invalid webhook credential")
        plaintext = "rcl_" + secrets.token_urlsafe(32)
        digest = hashlib.sha256(plaintext.encode()).hexdigest()
        credential_id = uuid.uuid4()
        with self.connect() as conn:
            conn.execute(
                """INSERT INTO collector_credentials(
                       id,name,token_sha256,tenant_id,source_id,scopes,principal_id,
                       capture_origin,webhook_privacy_mode
                   ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (
                    credential_id, name, digest, tenant_id, source_id, scopes,
                    principal_id, capture_origin, webhook_privacy_mode,
                ),
            )
            conn.execute(
                "INSERT INTO audit_events(operation,status,metadata) VALUES ('credential.create','success',%s)",
                (json.dumps({
                    "credential_id": str(credential_id),
                    "name": name,
                    "tenant_id": tenant_id,
                    "source_id": source_id,
                    "principal_id": principal_id,
                    "capture_origin": capture_origin,
                    "webhook_privacy_mode": webhook_privacy_mode,
                    "scopes": scopes,
                }),),
            )
        return {
            "id": str(credential_id),
            "name": name,
            "token": plaintext,
            "tenant_id": tenant_id,
            "source_id": source_id,
            "principal_id": principal_id,
            "capture_origin": capture_origin,
            "webhook_privacy_mode": webhook_privacy_mode,
            "scopes": scopes,
        }

    def revoke_collector_token(self, name: str) -> bool:
        with self.connect() as conn:
            row = conn.execute(
                "UPDATE collector_credentials SET revoked_at=now() WHERE name=%s AND revoked_at IS NULL RETURNING id",
                (name,),
            ).fetchone()
            conn.execute(
                "INSERT INTO audit_events(operation,status,metadata) VALUES ('credential.revoke',%s,%s)",
                ("success" if row else "not_found", json.dumps({"name": name})),
            )
            return bool(row)

    def provision_brain(
        self,
        *,
        organization_id: str,
        organization_kind: str,
        display_name: str,
        tenant_id: str,
        brain_kind: str,
        slug: str,
        owner_principal_id: str,
    ) -> dict[str, str]:
        values = (organization_id, tenant_id, owner_principal_id)
        if (
            any(not isinstance(value, str) or not V2_AUTHORITY_RE.fullmatch(value)
                for value in values)
            or organization_kind not in {"personal", "company"}
            or brain_kind != organization_kind
            or not isinstance(display_name, str)
            or not 1 <= len(display_name) <= 200
            or not isinstance(slug, str)
            or not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,62}", slug)
        ):
            raise ValueError("invalid brain provisioning request")
        with self.connect() as conn:
            with conn.transaction():
                conn.execute(
                    """INSERT INTO brain_organizations(
                           organization_id,organization_kind,display_name
                       ) VALUES (%s,%s,%s)
                       ON CONFLICT(organization_id) DO UPDATE SET
                         display_name=excluded.display_name
                       WHERE brain_organizations.organization_kind=
                             excluded.organization_kind""",
                    (organization_id, organization_kind, display_name),
                )
                conn.execute(
                    """INSERT INTO brain_tenants(tenant_id)
                       VALUES (%s) ON CONFLICT DO NOTHING""",
                    (tenant_id,),
                )
                conn.execute(
                    """INSERT INTO brain_principals(tenant_id,principal_id)
                       VALUES (%s,%s) ON CONFLICT DO NOTHING""",
                    (tenant_id, owner_principal_id),
                )
                conn.execute(
                    """INSERT INTO brain_spaces(
                           tenant_id,organization_id,brain_kind,slug
                       ) VALUES (%s,%s,%s,%s)
                       ON CONFLICT(tenant_id) DO NOTHING""",
                    (tenant_id, organization_id, brain_kind, slug),
                )
                conn.execute(
                    """INSERT INTO brain_memberships(
                           organization_id,principal_id,role
                       ) VALUES (%s,%s,'owner')
                       ON CONFLICT(organization_id,principal_id)
                       DO UPDATE SET role='owner'""",
                    (organization_id, owner_principal_id),
                )
                conn.execute(
                    """INSERT INTO brain_access_grants(
                           tenant_id,principal_id,permission
                       ) VALUES (%s,%s,'owner')
                       ON CONFLICT(tenant_id,principal_id)
                       DO UPDATE SET permission='owner'""",
                    (tenant_id, owner_principal_id),
                )
                existing_actor = conn.execute(
                    """SELECT actor_id FROM brain_actor_principals
                       WHERE tenant_id=%s AND principal_id=%s""",
                    (tenant_id, owner_principal_id),
                ).fetchone()
                if existing_actor is None:
                    actor_id = actor_id_for_principal(
                        tenant_id,
                        owner_principal_id,
                    )
                    conn.execute(
                        """INSERT INTO brain_actors(
                               tenant_id,actor_id,actor_kind,display_name
                           ) VALUES (%s,%s,'human',%s)
                           ON CONFLICT DO NOTHING""",
                        (tenant_id, actor_id, owner_principal_id),
                    )
                    conn.execute(
                        """INSERT INTO brain_actor_principals(
                               tenant_id,actor_id,principal_id
                           ) VALUES (%s,%s,%s)
                           ON CONFLICT DO NOTHING""",
                        (tenant_id, actor_id, owner_principal_id),
                    )
                    conn.execute(
                        """INSERT INTO brain_actor_aliases(
                               tenant_id,actor_id,alias
                           ) VALUES (%s,%s,%s)
                           ON CONFLICT DO NOTHING""",
                        (tenant_id, actor_id, owner_principal_id),
                    )
                configured = conn.execute(
                    """SELECT organization.organization_kind,space.brain_kind,
                              space.organization_id,space.slug
                       FROM brain_spaces space
                       JOIN brain_organizations organization
                         USING(organization_id)
                       WHERE space.tenant_id=%s""",
                    (tenant_id,),
                ).fetchone()
                if (
                    configured is None
                    or configured["organization_id"] != organization_id
                    or configured["organization_kind"] != organization_kind
                    or configured["brain_kind"] != brain_kind
                    or configured["slug"] != slug
                ):
                    raise ValueError("brain provisioning conflict")
        return {
            "organization_id": organization_id,
            "organization_kind": organization_kind,
            "tenant_id": tenant_id,
            "brain_kind": brain_kind,
            "slug": slug,
            "owner_principal_id": owner_principal_id,
        }

    def grant_canonical_source(
        self,
        *,
        tenant_id: str,
        principal_id: str,
        source_id: str,
        permission: str = "read",
    ) -> None:
        if (
            any(
                not isinstance(value, str)
                or not V2_AUTHORITY_RE.fullmatch(value)
                for value in (tenant_id, principal_id, source_id)
            )
            or permission not in {"owner", "read"}
        ):
            raise ValueError("invalid canonical source grant")
        with self.connect() as conn:
            with conn.transaction():
                brain_access = conn.execute(
                    """SELECT 1 FROM brain_access_grants
                       WHERE tenant_id=%s AND principal_id=%s
                         AND permission IN ('owner','admin','read')""",
                    (tenant_id, principal_id),
                ).fetchone()
                source = conn.execute(
                    """SELECT 1 FROM canonical_sources
                       WHERE tenant_id=%s AND source_id=%s""",
                    (tenant_id, source_id),
                ).fetchone()
                if not brain_access or not source:
                    raise ValueError("canonical source grant authority missing")
                conn.execute(
                    """INSERT INTO canonical_source_grants(
                           tenant_id,principal_id,source_id,permission
                       ) VALUES (%s,%s,%s,%s)
                       ON CONFLICT(tenant_id,principal_id,source_id)
                       DO UPDATE SET permission=excluded.permission""",
                    (tenant_id, principal_id, source_id, permission),
                )

    def create_mcp_token(
        self,
        name: str,
        *,
        tenant_id: str,
        principal_id: str,
        principal_kind: str = "workload",
        scopes: list[str] | None = None,
        expires_in_days: int = 30,
    ) -> dict[str, Any]:
        scopes = list(scopes or ["read"])
        if (
            not isinstance(name, str)
            or not name
            or not isinstance(tenant_id, str)
            or not V2_AUTHORITY_RE.fullmatch(tenant_id)
            or not isinstance(principal_id, str)
            or not V2_AUTHORITY_RE.fullmatch(principal_id)
            or principal_kind not in {"human", "workload"}
            or not isinstance(expires_in_days, int)
            or isinstance(expires_in_days, bool)
            or not 1 <= expires_in_days <= 365
            or not scopes
            or len(scopes) != len(set(scopes))
            or "read" not in scopes
            or set(scopes) - {"read", "forget"}
        ):
            raise ValueError("invalid MCP credential")
        plaintext = "rcl_" + secrets.token_urlsafe(32)
        digest = hashlib.sha256(plaintext.encode()).hexdigest()
        credential_id = uuid.uuid4()
        expires_at = datetime.now(timezone.utc) + timedelta(days=expires_in_days)
        with self.connect() as conn:
            access = conn.execute(
                """SELECT 1
                   FROM brain_access_grants access
                   JOIN brain_spaces space USING(tenant_id)
                   JOIN brain_memberships membership
                     ON membership.organization_id=space.organization_id
                    AND membership.principal_id=access.principal_id
                   WHERE access.tenant_id=%s AND access.principal_id=%s
                     AND access.permission IN ('owner','admin','read')""",
                (tenant_id, principal_id),
            ).fetchone()
            if not access:
                raise ValueError("MCP brain access missing")
            conn.execute(
                """INSERT INTO mcp_credentials(
                       id,name,token_sha256,tenant_id,principal_id,
                       principal_kind,audience,scopes,expires_at
                   ) VALUES (%s,%s,%s,%s,%s,%s,'recall-mcp',%s,%s)""",
                (
                    credential_id,
                    name,
                    digest,
                    tenant_id,
                    principal_id,
                    principal_kind,
                    scopes,
                    expires_at,
                ),
            )
        return {
            "id": str(credential_id),
            "name": name,
            "token": plaintext,
            "tenant_id": tenant_id,
            "principal_id": principal_id,
            "principal_kind": principal_kind,
            "audience": "recall-mcp",
            "scopes": scopes,
            "expires_at": expires_at.isoformat(),
        }

    def revoke_mcp_token(self, name: str) -> bool:
        with self.connect() as conn:
            row = conn.execute(
                """UPDATE mcp_credentials SET revoked_at=now()
                   WHERE name=%s AND revoked_at IS NULL RETURNING id""",
                (name,),
            ).fetchone()
        return bool(row)

    def set_source_profile(self, value: SourceProfile | dict[str, Any]) -> dict[str, Any]:
        profile = value if isinstance(value, SourceProfile) else SourceProfile.from_mapping(value)
        with self.connect() as conn:
            with conn.transaction():
                if not conn.execute(
                    "SELECT 1 FROM sources WHERE id=%s", (profile.source_id,)
                ).fetchone():
                    raise ValueError("source profile source does not exist")
                conn.execute(
                    """INSERT INTO source_profiles(
                           source_id,family,quality,freshness_half_life_days
                       ) VALUES (%s,%s,%s,%s)
                       ON CONFLICT(source_id) DO UPDATE SET
                         family=excluded.family,quality=excluded.quality,
                         freshness_half_life_days=excluded.freshness_half_life_days,
                         updated_at=now()""",
                    (
                        profile.source_id, profile.family, profile.quality,
                        profile.freshness_half_life_days,
                    ),
                )
                if profile.family == "coding_history":
                    inserted = conn.execute(
                        """INSERT INTO canonical_source_actor_bindings(
                               tenant_id,source_id,actor_id,relation
                           )
                           SELECT source.tenant_id,source.source_id,
                                  actor.actor_id,'contributor'
                             FROM canonical_sources source
                             JOIN brain_actor_principals actor
                               ON actor.tenant_id=source.tenant_id
                              AND actor.principal_id=source.owner_principal_id
                            WHERE source.source_id=%s
                              AND NOT EXISTS (
                                  SELECT 1
                                    FROM canonical_source_actor_bindings existing
                                   WHERE existing.tenant_id=source.tenant_id
                                     AND existing.source_id=source.source_id
                              )
                           ON CONFLICT DO NOTHING
                           RETURNING tenant_id,source_id""",
                        (profile.source_id,),
                    ).fetchone()
                    if inserted is not None:
                        self._queue_actor_projection(
                            conn,
                            tenant_id=inserted["tenant_id"],
                            source_ids=[inserted["source_id"]],
                        )
                conn.execute(
                    """INSERT INTO audit_events(
                           operation,source_id,status,metadata
                       ) VALUES ('source.profile',%s,'success',%s)""",
                    (profile.source_id, json.dumps({
                        "family": profile.family, "quality": profile.quality,
                        "freshness_half_life_days": profile.freshness_half_life_days,
                    })),
                )
        return {"status": "configured", **profile.to_mapping()}

    @staticmethod
    def _queue_actor_projection(
        connection: Any,
        *,
        tenant_id: str,
        source_ids: list[str],
    ) -> int:
        result = connection.execute(
            """INSERT INTO canonical_evidence_document_queue(
                   tenant_id,source_id,native_parent_id,
                   generation,reason,changed_at
               )
               SELECT event.tenant_id,event.source_id,
                      coalesce(event.native_parent_id,event.native_id),
                      1,'actor-binding',clock_timestamp()
                 FROM canonical_events event
                 JOIN canonical_documents document
                   ON document.tenant_id=event.tenant_id
                  AND document.source_id=event.source_id
                  AND document.event_id=event.event_id
                  AND document.is_current
                  AND document.deleted_at IS NULL
                WHERE event.tenant_id=%s
                  AND event.source_id=ANY(%s)
                GROUP BY event.tenant_id,event.source_id,
                         coalesce(event.native_parent_id,event.native_id)
               ON CONFLICT(tenant_id,source_id,native_parent_id)
               DO UPDATE SET
                   generation=canonical_evidence_document_queue.generation+1,
                   reason='actor-binding',changed_at=clock_timestamp()""",
            (tenant_id, source_ids),
        )
        return max(0, result.rowcount)

    def bind_coding_sources_to_employee(
        self,
        *,
        tenant_id: str,
        employee_key: str,
        display_name: str,
        source_ids: list[str],
    ) -> dict[str, Any]:
        """Bind legacy coding sources to one stable employee actor."""

        normalized_name = display_name.strip() if isinstance(display_name, str) else ""
        if (
            not isinstance(tenant_id, str)
            or not V2_AUTHORITY_RE.fullmatch(tenant_id)
            or not 1 <= len(normalized_name) <= 200
            or not isinstance(source_ids, list)
            or not 1 <= len(source_ids) <= 32
            or any(
                not isinstance(value, str)
                or not V2_AUTHORITY_RE.fullmatch(value)
                for value in source_ids
            )
        ):
            raise ValueError("invalid employee source binding")
        normalized_sources = sorted(set(source_ids))
        if len(normalized_sources) != len(source_ids):
            raise ValueError("invalid employee source binding")
        provisional_actor_id = actor_id_for_employee_key(tenant_id, employee_key)
        with self.connect() as conn:
            with conn.transaction():
                sources = conn.execute(
                    """SELECT source.source_id
                         FROM canonical_sources source
                         JOIN source_profiles profile
                           ON profile.source_id=source.source_id
                        WHERE source.tenant_id=%s
                          AND source.source_id=ANY(%s)
                          AND profile.family='coding_history'
                        ORDER BY source.source_id
                        FOR SHARE OF source""",
                    (tenant_id, normalized_sources),
                ).fetchall()
                if [row["source_id"] for row in sources] != normalized_sources:
                    raise ValueError("employee binding requires coding-history sources")
                exact_name = conn.execute(
                    """SELECT actor_id
                         FROM brain_actors
                        WHERE tenant_id=%s AND actor_kind='human' AND active
                          AND lower(display_name)=lower(%s)
                        ORDER BY actor_id
                        LIMIT 2""",
                    (tenant_id, normalized_name),
                ).fetchall()
                if len(exact_name) > 1:
                    raise ValueError("employee display name is ambiguous")
                actor_id = (
                    exact_name[0]["actor_id"] if exact_name else provisional_actor_id
                )
                conn.execute(
                    """INSERT INTO brain_actors(
                           tenant_id,actor_id,actor_kind,display_name
                       ) VALUES (%s,%s,'human',%s)
                       ON CONFLICT(tenant_id,actor_id) DO NOTHING""",
                    (tenant_id, actor_id, normalized_name),
                )
                actor = conn.execute(
                    """SELECT display_name,actor_kind,active
                         FROM brain_actors
                        WHERE tenant_id=%s AND actor_id=%s""",
                    (tenant_id, actor_id),
                ).fetchone()
                if (
                    actor is None
                    or actor["actor_kind"] != "human"
                    or not actor["active"]
                    or actor["display_name"].casefold() != normalized_name.casefold()
                ):
                    raise ValueError("employee actor key conflicts")
                conflicting = conn.execute(
                    """SELECT DISTINCT binding.source_id,binding.actor_id
                         FROM canonical_source_actor_bindings binding
                        WHERE binding.tenant_id=%s
                          AND binding.source_id=ANY(%s)
                          AND binding.relation='contributor'
                          AND binding.actor_id<>%s
                        ORDER BY binding.source_id,binding.actor_id
                        LIMIT 1""",
                    (tenant_id, normalized_sources, actor_id),
                ).fetchone()
                if conflicting is not None:
                    raise ValueError("coding source already belongs to another employee")
                conn.execute(
                    """INSERT INTO brain_actor_aliases(
                           tenant_id,actor_id,alias
                       ) VALUES (%s,%s,%s) ON CONFLICT DO NOTHING""",
                    (tenant_id, actor_id, normalized_name),
                )
                inserted = conn.execute(
                    """INSERT INTO canonical_source_actor_bindings(
                           tenant_id,source_id,actor_id,relation
                       )
                       SELECT %s,source_id,%s,'contributor'
                         FROM unnest(%s::text[]) source(source_id)
                       ON CONFLICT DO NOTHING
                       RETURNING source_id""",
                    (tenant_id, actor_id, normalized_sources),
                ).fetchall()
                queued = self._queue_actor_projection(
                    conn,
                    tenant_id=tenant_id,
                    source_ids=normalized_sources,
                )
        return {
            "status": "bound",
            "actor_id": actor_id,
            "sources": len(normalized_sources),
            "new_bindings": len(inserted),
            "queued_documents": queued,
        }

    def set_source_alias(self, alias: str, source_id: str) -> dict[str, str]:
        if not isinstance(alias, str) or not re.fullmatch(r"[a-z0-9][a-z0-9._-]{1,63}", alias):
            raise ValueError("source alias is invalid")
        with self.connect() as conn:
            if not conn.execute("SELECT 1 FROM sources WHERE id=%s", (source_id,)).fetchone():
                raise ValueError("source alias source does not exist")
            conn.execute(
                """INSERT INTO source_aliases(alias,source_id) VALUES (%s,%s)
                   ON CONFLICT(alias) DO UPDATE SET source_id=excluded.source_id,updated_at=now()""",
                (alias, source_id),
            )
            conn.execute(
                """INSERT INTO audit_events(operation,source_id,status,metadata)
                   VALUES ('source.alias',%s,'success',%s)""",
                (source_id, json.dumps({"alias": alias})),
            )
        return {"status": "configured", "alias": alias, "source_id": source_id}

    def federation_scoreboard(self) -> dict[str, Any]:
        with self.connect() as conn:
            groups = conn.execute(
                """SELECT profile.family,profile.quality,count(*) AS sources,
                          coalesce(sum(events.n),0) AS source_events,
                          coalesce(sum(live.n),0) AS live_items
                   FROM source_profiles profile
                   LEFT JOIN (
                     SELECT source_id,count(*) AS n FROM source_events GROUP BY source_id
                   ) events ON events.source_id=profile.source_id
                   LEFT JOIN (
                     SELECT source_id,count(*) AS n FROM items
                     WHERE deleted_at IS NULL GROUP BY source_id
                   ) live ON live.source_id=profile.source_id
                   GROUP BY profile.family,profile.quality
                   ORDER BY profile.family,profile.quality"""
            ).fetchall()
            age_rows = conn.execute(
                """SELECT CASE
                            WHEN latest.created_at IS NULL THEN 'empty'
                            WHEN latest.created_at >= now()-interval '1 day' THEN 'fresh_1d'
                            WHEN latest.created_at >= now()-interval '30 days' THEN 'active_30d'
                            ELSE 'stale_30d'
                          END AS bucket,count(*) AS sources
                   FROM source_profiles profile
                   LEFT JOIN (
                     SELECT source_id,max(created_at) AS created_at
                     FROM source_events GROUP BY source_id
                   ) latest ON latest.source_id=profile.source_id
                   GROUP BY bucket ORDER BY bucket"""
            ).fetchall()
            totals = conn.execute(
                """SELECT count(*) FILTER (WHERE profile.source_id IS NOT NULL) AS profiled,
                          count(*) FILTER (WHERE profile.source_id IS NULL) AS unprofiled
                   FROM sources source LEFT JOIN source_profiles profile
                     ON profile.source_id=source.id"""
            ).fetchone()
        return {
            "schema_version": 1,
            "profiled_sources": int(totals["profiled"]),
            "unprofiled_sources": int(totals["unprofiled"]),
            "groups": [{
                "family": row["family"], "quality": row["quality"],
                "sources": int(row["sources"]),
                "source_events": int(row["source_events"]),
                "live_items": int(row["live_items"]),
            } for row in groups],
            "age_buckets": {row["bucket"]: int(row["sources"]) for row in age_rows},
        }

    def authenticate_bearer(self, plaintext: str, required_scope: str) -> dict | None:
        digest = hashlib.sha256(plaintext.encode()).hexdigest()
        with self.connect() as conn:
            row = conn.execute(
                """SELECT credential.id,credential.name,credential.tenant_id,
                          credential.source_id,credential.principal_id,
                          credential.capture_origin,
                          credential.webhook_privacy_mode,credential.scopes
                   FROM collector_credentials credential
                   LEFT JOIN connector_installations installation
                     ON installation.id=credential.installation_id
                   WHERE credential.token_sha256=%s
                     AND credential.revoked_at IS NULL
                     AND (
                         credential.installation_id IS NULL
                         OR installation.state='enabled'
                     )""",
                (digest,),
            ).fetchone()
            if row:
                if required_scope not in row["scopes"]:
                    return None
                if required_scope == "webhook" and set(row["scopes"]) != {"webhook"}:
                    return None
                return {**row, "credential_kind": "collector", "audience": None}
            mcp = conn.execute(
                """SELECT credential.id,credential.name,credential.tenant_id,
                          NULL::text AS source_id,
                          credential.principal_id,NULL::text AS capture_origin,
                          NULL::text AS webhook_privacy_mode,
                          credential.scopes,credential.audience,
                          credential.principal_kind,
                          CASE
                            WHEN membership.role='owner'
                             AND access.permission='owner' THEN 'owner'
                            WHEN membership.role IN ('owner','admin')
                             AND access.permission IN ('owner','admin') THEN 'admin'
                            ELSE 'member'
                          END AS role
                   FROM mcp_credentials credential
                   JOIN brain_access_grants access
                     ON access.tenant_id=credential.tenant_id
                    AND access.principal_id=credential.principal_id
                   JOIN brain_spaces space
                     ON space.tenant_id=credential.tenant_id
                   JOIN brain_memberships membership
                     ON membership.organization_id=space.organization_id
                    AND membership.principal_id=credential.principal_id
                   WHERE credential.token_sha256=%s
                     AND credential.revoked_at IS NULL
                     AND credential.expires_at>now()
                     AND credential.audience='recall-mcp'
                     AND access.permission IN ('owner','admin','read')""",
                (digest,),
            ).fetchone()
            if not mcp or required_scope not in mcp["scopes"]:
                return None
            return {**mcp, "credential_kind": "mcp"}

    def record_authorization_event(
        self,
        principal: dict[str, Any],
        *,
        action: str,
        allowed: bool,
        reason: str,
        policy_version: str,
    ) -> None:
        values = (
            principal.get("principal_kind"),
            principal.get("principal_id"),
            principal.get("tenant_id"),
            action,
            "allowed" if allowed else "denied",
            reason,
            policy_version,
        )
        if any(not isinstance(value, str) or not value for value in values):
            raise ValueError("authorization audit identity invalid")
        with self.connect() as conn:
            conn.execute(
                """INSERT INTO authorization_audit_events(
                       principal_kind,principal_id,tenant_id,action,
                       decision,reason,policy_version
                   ) VALUES (%s,%s,%s,%s,%s,%s,%s)""",
                values,
            )

    def resolve_external_identity(
        self,
        *,
        issuer: str,
        subject: str,
        scopes: list[str],
        audience: str,
        tenant_id: str | None = None,
    ) -> dict[str, Any] | None:
        if (
            not isinstance(issuer, str)
            or not issuer.startswith("https://")
            or not isinstance(subject, str)
            or not subject
            or not isinstance(audience, str)
            or not audience.startswith("https://")
            or not isinstance(scopes, list)
            or not scopes
            or "read" not in scopes
            or any(not isinstance(scope, str) or not scope for scope in scopes)
            or (
                tenant_id is not None
                and not V2_AUTHORITY_RE.fullmatch(tenant_id)
            )
        ):
            return None
        subject_sha256 = hashlib.sha256(subject.encode()).hexdigest()
        with self.connect() as conn:
            rows = conn.execute(
                """SELECT binding.tenant_id,binding.principal_id,
                          binding.principal_kind,
                          CASE
                            WHEN membership.role='owner'
                             AND access.permission='owner' THEN 'owner'
                            WHEN membership.role IN ('owner','admin')
                             AND access.permission IN ('owner','admin') THEN 'admin'
                            ELSE 'member'
                          END AS role
                   FROM external_identity_bindings binding
                   JOIN brain_access_grants access
                     ON access.tenant_id=binding.tenant_id
                    AND access.principal_id=binding.principal_id
                   JOIN brain_spaces space ON space.tenant_id=binding.tenant_id
                   JOIN brain_memberships membership
                     ON membership.organization_id=space.organization_id
                    AND membership.principal_id=binding.principal_id
                   WHERE binding.issuer=%s AND binding.subject_sha256=%s
                     AND binding.revoked_at IS NULL
                     AND (%s::text IS NULL OR binding.tenant_id=%s)
                     AND access.permission IN ('owner','admin','read')""",
                (issuer, subject_sha256, tenant_id, tenant_id),
            ).fetchall()
        if len(rows) != 1:
            return None
        row = rows[0]
        return {
            **row,
            "credential_kind": "mcp",
            "name": "external-identity",
            "source_id": None,
            "capture_origin": None,
            "webhook_privacy_mode": None,
            "audience": "recall-mcp",
            "scopes": scopes,
        }

    def accept_external_invitation(
        self,
        *,
        issuer: str,
        subject: str,
        email: str,
        email_index: str,
        scopes: list[str],
        audience: str,
        tenant_id: str | None = None,
        invitation_id: str | None = None,
        actor_email_index: str | None = None,
    ) -> dict[str, Any] | None:
        normalized_email = normalize_verified_email(email)
        if (
            normalized_email is None
            or not isinstance(email_index, str)
            or not re.fullmatch(r"[0-9a-f]{64}", email_index)
            or not isinstance(issuer, str)
            or not issuer.startswith("https://")
            or not isinstance(subject, str)
            or not subject
            or not isinstance(audience, str)
            or not audience.startswith("https://")
            or not isinstance(scopes, list)
            or "read" not in scopes
            or any(not isinstance(scope, str) or not scope for scope in scopes)
            or (
                tenant_id is not None
                and not V2_AUTHORITY_RE.fullmatch(tenant_id)
            )
            or (
                invitation_id is not None
                and not re.fullmatch(r"[0-9a-f-]{36}", invitation_id)
            )
            or (
                actor_email_index is not None
                and not re.fullmatch(r"[0-9a-f]{64}", actor_email_index)
            )
        ):
            return None
        subject_sha256 = hashlib.sha256(subject.encode()).hexdigest()
        email_sha256 = email_index
        principal_id = "principal:human:" + hashlib.sha256(
            f"{issuer}\0{subject}".encode()
        ).hexdigest()[:32]
        try:
            parsed_invitation_id = (
                uuid.UUID(invitation_id) if invitation_id is not None else None
            )
        except ValueError:
            return None
        with self.connect() as conn:
            with conn.transaction():
                invitations = conn.execute(
                    """SELECT invitation.id,invitation.tenant_id,
                              invitation.role,invitation.actor_display_name,
                              space.organization_id
                       FROM brain_invitations invitation
                       JOIN brain_spaces space USING(tenant_id)
                       WHERE invitation.email_sha256=%s
                         AND invitation.accepted_at IS NULL
                         AND invitation.revoked_at IS NULL
                         AND invitation.expires_at>now()
                         AND space.brain_kind='company'
                         AND (%s::text IS NULL OR invitation.tenant_id=%s)
                         AND (%s::uuid IS NULL OR invitation.id=%s::uuid)
                       ORDER BY invitation.created_at
                       FOR UPDATE OF invitation""",
                    (
                        email_sha256,
                        tenant_id,
                        tenant_id,
                        parsed_invitation_id,
                        parsed_invitation_id,
                    ),
                ).fetchall()
                if len(invitations) != 1:
                    return None
                invitation = invitations[0]
                accepted_invitation_id = invitation["id"]
                existing = conn.execute(
                    """SELECT principal_id FROM external_identity_bindings
                       WHERE issuer=%s AND subject_sha256=%s AND tenant_id=%s
                       FOR UPDATE""",
                    (issuer, subject_sha256, invitation["tenant_id"]),
                ).fetchone()
                if existing and existing["principal_id"] != principal_id:
                    return None
                permission = "admin" if invitation["role"] == "admin" else "read"
                conn.execute(
                    """INSERT INTO brain_principals(tenant_id,principal_id)
                       VALUES (%s,%s) ON CONFLICT DO NOTHING""",
                    (invitation["tenant_id"], principal_id),
                )
                conn.execute(
                    """INSERT INTO brain_memberships(
                           organization_id,principal_id,role
                       ) VALUES (%s,%s,%s)
                       ON CONFLICT(organization_id,principal_id)
                       DO UPDATE SET role=excluded.role""",
                    (invitation["organization_id"], principal_id, invitation["role"]),
                )
                conn.execute(
                    """INSERT INTO brain_access_grants(
                           tenant_id,principal_id,permission
                       ) VALUES (%s,%s,%s)
                       ON CONFLICT(tenant_id,principal_id)
                       DO UPDATE SET permission=excluded.permission""",
                    (invitation["tenant_id"], principal_id, permission),
                )
                conn.execute(
                    """INSERT INTO canonical_source_grants(
                           tenant_id,principal_id,source_id,permission
                       ) SELECT source.tenant_id,%s,source.source_id,'read'
                         FROM canonical_sources source
                         JOIN brain_spaces space
                           ON space.tenant_id=source.tenant_id
                         JOIN brain_memberships source_membership
                           ON source_membership.organization_id=space.organization_id
                          AND source_membership.principal_id=source.owner_principal_id
                        WHERE source.tenant_id=%s
                          AND space.organization_id=%s
                          AND space.brain_kind='company'
                       ON CONFLICT(tenant_id,principal_id,source_id)
                       DO UPDATE SET permission='read'""",
                    (
                        principal_id,
                        invitation["tenant_id"],
                        invitation["organization_id"],
                    ),
                )
                conn.execute(
                    """INSERT INTO external_identity_bindings(
                           issuer,subject_sha256,tenant_id,principal_id,
                           principal_kind,revoked_at
                       ) VALUES (%s,%s,%s,%s,'human',NULL)
                       ON CONFLICT(issuer,subject_sha256,tenant_id)
                       DO UPDATE SET principal_id=excluded.principal_id,
                                     principal_kind='human',revoked_at=NULL""",
                    (
                        issuer,
                        subject_sha256,
                        invitation["tenant_id"],
                        principal_id,
                    ),
                )
                linked_actor = conn.execute(
                    """SELECT actor_id FROM brain_actor_principals
                       WHERE tenant_id=%s AND principal_id=%s""",
                    (invitation["tenant_id"], principal_id),
                ).fetchone()
                display_name = invitation["actor_display_name"] or normalized_email.split(
                    "@", 1
                )[0]
                actor_id = (
                    linked_actor["actor_id"]
                    if linked_actor is not None
                    else claimable_actor_for_display_name(
                        conn,
                        tenant_id=invitation["tenant_id"],
                        display_name=display_name,
                    )
                    or actor_id_for_principal(invitation["tenant_id"], principal_id)
                )
                conn.execute(
                    """INSERT INTO brain_actors(
                           tenant_id,actor_id,actor_kind,display_name
                       ) VALUES (%s,%s,'human',%s)
                       ON CONFLICT(tenant_id,actor_id)
                       DO UPDATE SET display_name=excluded.display_name,
                                     active=true,updated_at=now()""",
                    (invitation["tenant_id"], actor_id, display_name),
                )
                conn.execute(
                    """INSERT INTO brain_actor_principals(
                           tenant_id,actor_id,principal_id
                       ) VALUES (%s,%s,%s)
                       ON CONFLICT(tenant_id,principal_id)
                       DO UPDATE SET actor_id=excluded.actor_id""",
                    (invitation["tenant_id"], actor_id, principal_id),
                )
                conn.execute(
                    """INSERT INTO brain_actor_aliases(
                           tenant_id,actor_id,alias
                       ) VALUES (%s,%s,%s),(%s,%s,%s)
                       ON CONFLICT DO NOTHING""",
                    (
                        invitation["tenant_id"], actor_id, display_name,
                        invitation["tenant_id"], actor_id, normalized_email,
                    ),
                )
                if actor_email_index is not None:
                    conn.execute(
                        """INSERT INTO brain_actor_external_identities(
                               tenant_id,actor_id,connector_id,namespace,
                               subject_hmac_sha256
                           ) VALUES (%s,%s,'identity','email',%s)
                           ON CONFLICT(
                               tenant_id,connector_id,namespace,
                               subject_hmac_sha256
                           ) DO UPDATE SET actor_id=excluded.actor_id""",
                        (invitation["tenant_id"], actor_id, actor_email_index),
                    )
                accepted = conn.execute(
                    """UPDATE brain_invitations
                       SET accepted_principal_id=%s,accepted_at=now()
                       WHERE id=%s AND accepted_at IS NULL AND revoked_at IS NULL
                       RETURNING id""",
                    (principal_id, accepted_invitation_id),
                ).fetchone()
                if not accepted:
                    return None
                conn.execute(
                    """INSERT INTO control_audit_events(
                           id,principal_id,operation,status,target_sha256
                       ) VALUES (%s,%s,'brain.invitation.accept','success',%s)""",
                    (
                        uuid.uuid4(),
                        principal_id,
                        hashlib.sha256(
                            str(accepted_invitation_id).encode()
                        ).hexdigest(),
                    ),
                )
        return self.resolve_external_identity(
            issuer=issuer,
            subject=subject,
            scopes=scopes,
            audience=audience,
            tenant_id=tenant_id,
        )

    def authorized_source_ids(self, principal_id: str) -> list[str]:
        if not isinstance(principal_id, str) or not principal_id:
            return []
        with self.connect() as conn:
            rows = conn.execute(
                """SELECT DISTINCT source_id
                   FROM source_grants
                   WHERE principal_id=%s AND permission IN ('owner','read')
                   ORDER BY source_id""",
                (principal_id,),
            ).fetchall()
        return [row["source_id"] for row in rows]

    def authorized_canonical_source_ids(
        self,
        tenant_id: str,
        principal_id: str,
    ) -> list[str]:
        if (
            not isinstance(tenant_id, str)
            or not V2_AUTHORITY_RE.fullmatch(tenant_id)
            or not isinstance(principal_id, str)
            or not V2_AUTHORITY_RE.fullmatch(principal_id)
        ):
            return []
        with self.connect() as conn:
            brain_access = conn.execute(
                """SELECT 1 FROM brain_access_grants
                   WHERE tenant_id=%s AND principal_id=%s
                     AND permission IN ('owner','admin','read')""",
                (tenant_id, principal_id),
            ).fetchone()
            if not brain_access:
                return []
            rows = conn.execute(
                """SELECT source_id FROM canonical_source_grants
                   WHERE tenant_id=%s AND principal_id=%s
                     AND permission IN ('owner','read')
                   ORDER BY source_id""",
                (tenant_id, principal_id),
            ).fetchall()
        return [row["source_id"] for row in rows]

    def capture(self, principal: dict, arguments: dict) -> dict:
        event, privacy = build_capture_event(arguments, principal)
        idempotency_key = (
            "mcp-capture-v1-" + hashlib.sha256(canonical_json(event)).hexdigest()
        )
        acknowledgement, replay = self.ingest(idempotency_key, [event])
        result = {
            "status": acknowledgement.get("status", "committed"),
            "native_id": event["native_id"],
            "replay": replay,
            "privacy": privacy,
        }
        if acknowledgement.get("receipts"):
            result["receipt"] = acknowledgement["receipts"][0] + "#item=0"
        return result

    def forget_capture(self, principal: dict, receipt: str) -> dict:
        source_id = principal.get("source_id")
        principal_id = principal.get("principal_id")
        if not isinstance(source_id, str) or not isinstance(principal_id, str):
            raise ValueError("capture authority is invalid")
        native_id, revision, event_part = parse_capture_receipt(receipt, source_id)
        with self.connect() as conn:
            captured = conn.execute(
                """SELECT occurred_at FROM source_events
                   WHERE source_id=%s AND native_id=%s AND revision=%s
                     AND kind='capture' AND principal_id=%s""",
                (source_id, native_id, revision, principal_id),
            ).fetchone()
        if not captured:
            raise ValueError("capture receipt not found")
        event = build_forget_event(
            source_id=source_id,
            principal_id=principal_id,
            native_id=native_id,
            deleted_receipt=event_part,
            captured_at=captured["occurred_at"],
        )
        idempotency_key = (
            "mcp-forget-v1-" + hashlib.sha256(canonical_json(event)).hexdigest()
        )
        acknowledgement, replay = self.ingest(idempotency_key, [event])
        return {
            "status": acknowledgement.get("status", "committed"),
            "native_id": native_id,
            "receipt": acknowledgement["receipts"][0],
            "replay": replay,
        }

    def readiness(self) -> dict[str, str]:
        """Prove the database is reachable without scanning runtime data."""
        with self.connect() as conn:
            row = conn.execute("SELECT 1 AS ready").fetchone()
        if not row or row["ready"] != 1:
            raise RuntimeError("database readiness probe failed")
        return {"status": "ready"}

    def operational_health(self) -> dict[str, str | int]:
        """Return client health without scanning the corpus or exact metric totals."""
        with self.connect() as conn:
            row = conn.execute(
                """SELECT GREATEST(
                       0,
                       COALESCE((
                           SELECT id FROM source_events ORDER BY id DESC LIMIT 1
                       ), 0) -
                       COALESCE((
                           SELECT last_event_id FROM projection_watermarks
                           WHERE projector='items'
                       ), 0)
                   ) AS projection_lag"""
            ).fetchone()
        if not row:
            raise RuntimeError("database operational health probe failed")
        return {"status": "ok", "projection_lag": row["projection_lag"]}

    def service_metrics(self) -> dict:
        with self.connect() as conn:
            metrics = {
                "source_events": conn.execute("SELECT count(*) AS n FROM source_events").fetchone()["n"],
                "dead_letters": conn.execute("SELECT count(*) AS n FROM dead_letters").fetchone()["n"],
                "projection_lag": conn.execute(
                    """SELECT GREATEST(0,
                       COALESCE((SELECT max(id) FROM source_events),0) -
                       COALESCE((SELECT last_event_id FROM projection_watermarks WHERE projector='items'),0)) AS n"""
                ).fetchone()["n"],
                "source_freshness_seconds": conn.execute(
                    "SELECT COALESCE(GREATEST(0, extract(epoch FROM now() - max(created_at)))::bigint, 0) AS n FROM source_events"
                ).fetchone()["n"],
            }
            if conn.execute("SELECT to_regclass('public.item_embeddings') AS value").fetchone()["value"]:
                compatibility = ""
                values: list[Any] = []
                if self.semantic_runtime is not None:
                    compatibility = """ AND embedding.model=%s
                       AND embedding.runtime_fingerprint=%s AND embedding.dimensions=%s
                       AND embedding.projector_version=item.projector_version
                       AND embedding.content_sha256=
                           encode(sha256(convert_to(item.text_redacted,'UTF8')),'hex')"""
                    values = [
                        self.semantic_runtime.model,
                        self.semantic_runtime.fingerprint,
                        self.semantic_runtime.dimensions,
                    ]
                row = conn.execute(
                    f"""SELECT count(*) AS embedded,
                               (SELECT count(*) FROM items
                                WHERE deleted_at IS NULL
                                  AND btrim(text_redacted) <> '') AS live
                        FROM item_embeddings embedding
                        JOIN items item ON item.id=embedding.item_id
                        WHERE item.deleted_at IS NULL
                          AND btrim(item.text_redacted) <> ''{compatibility}""",
                    values,
                ).fetchone()
                metrics["embedded_items"] = row["embedded"]
                metrics["embedding_lag"] = max(0, row["live"] - row["embedded"])
            else:
                metrics["embedded_items"] = 0
                metrics["embedding_lag"] = conn.execute(
                    """SELECT count(*) AS n FROM items
                    WHERE deleted_at IS NULL AND btrim(text_redacted) <> ''"""
                ).fetchone()["n"]
            return metrics

    def embed_pending(
        self, batch_size: int = 128, max_batches: int | None = None,
        source_id: str | None = None,
        surface: str | None = None,
    ) -> dict[str, Any]:
        if self.semantic_runtime is None:
            raise ValueError("semantic runtime is not configured")
        if not 1 <= batch_size <= 1000 or (max_batches is not None and max_batches < 1):
            raise ValueError("invalid embedding backfill bounds")
        if source_id is not None and not SOURCE_ID_RE.fullmatch(source_id):
            raise ValueError("invalid embedding source")
        if surface is not None and not KIND_RE.fullmatch(surface):
            raise ValueError("invalid embedding surface")
        processed = batches = 0
        with self.connect() as conn:
            global_lock = "recall:item-embeddings"
            source_scoped = source_id is not None
            use_watermark = source_id is None and surface is None
            if source_scoped:
                global_locked = conn.execute(
                    "SELECT pg_try_advisory_lock_shared(hashtextextended(%s,0)) AS value",
                    (global_lock,),
                ).fetchone()["value"]
            else:
                global_locked = conn.execute(
                    "SELECT pg_try_advisory_lock(hashtextextended(%s,0)) AS value",
                    (global_lock,),
                ).fetchone()["value"]
            if not global_locked:
                return {"status": "busy", "processed": 0, "batches": 0}
            try:
                watermark = 0
                if use_watermark:
                    conn.execute(
                        """INSERT INTO embedding_projection_watermarks(
                             runtime_fingerprint,model,dimensions,last_item_id
                           ) VALUES (%s,%s,%s,0)
                           ON CONFLICT(runtime_fingerprint) DO NOTHING""",
                        (
                            self.semantic_runtime.fingerprint,
                            self.semantic_runtime.model,
                            self.semantic_runtime.dimensions,
                        ),
                    )
                    watermark = conn.execute(
                        """SELECT last_item_id
                           FROM embedding_projection_watermarks
                           WHERE runtime_fingerprint=%s
                           FOR UPDATE""",
                        (self.semantic_runtime.fingerprint,),
                    ).fetchone()["last_item_id"]
                    stale_runtime_item = conn.execute(
                        """SELECT min(item_id) AS item_id
                           FROM item_embeddings
                           WHERE runtime_fingerprint<>%s""",
                        (self.semantic_runtime.fingerprint,),
                    ).fetchone()["item_id"]
                    if (
                        stale_runtime_item is not None
                        and stale_runtime_item <= watermark
                    ):
                        watermark = max(0, stale_runtime_item - 1)
                        conn.execute(
                            """UPDATE embedding_projection_watermarks
                               SET last_item_id=LEAST(last_item_id,%s),
                                   updated_at=now()
                               WHERE runtime_fingerprint=%s""",
                            (watermark, self.semantic_runtime.fingerprint),
                        )
                while max_batches is None or batches < max_batches:
                    if use_watermark:
                        rows = conn.execute(
                            """SELECT item.id,item.source_id,item.text_redacted,
                                      item.projector_version
                               FROM items item
                               LEFT JOIN item_embeddings embedding
                                 ON embedding.item_id=item.id
                               WHERE item.id>%s
                                 AND item.deleted_at IS NULL
                                 AND btrim(item.text_redacted) <> ''
                                 AND (
                                   embedding.item_id IS NULL OR embedding.model<>%s
                                   OR embedding.runtime_fingerprint<>%s
                                   OR embedding.dimensions<>%s
                                   OR embedding.projector_version<>item.projector_version
                                   OR embedding.content_sha256<>
                                      encode(sha256(convert_to(item.text_redacted,'UTF8')),'hex')
                                 )
                               ORDER BY item.id LIMIT %s
                               FOR UPDATE OF item SKIP LOCKED""",
                            (
                                watermark,
                                self.semantic_runtime.model,
                                self.semantic_runtime.fingerprint,
                                self.semantic_runtime.dimensions,
                                batch_size,
                            ),
                        ).fetchall()
                    else:
                        rows = conn.execute(
                            """SELECT item.id,item.source_id,item.text_redacted,
                                      item.projector_version
                               FROM items item
                               LEFT JOIN item_embeddings embedding
                                 ON embedding.item_id=item.id
                               WHERE item.deleted_at IS NULL
                                 AND btrim(item.text_redacted) <> ''
                                 AND (%s::text IS NULL OR item.source_id=%s)
                                 AND (%s::text IS NULL OR item.surface=%s)
                                 AND (
                                   embedding.item_id IS NULL OR embedding.model<>%s
                                   OR embedding.runtime_fingerprint<>%s
                                   OR embedding.dimensions<>%s
                                   OR embedding.projector_version<>item.projector_version
                                   OR embedding.content_sha256<>
                                      encode(sha256(convert_to(item.text_redacted,'UTF8')),'hex')
                                 )
                               ORDER BY item.id LIMIT %s
                               FOR UPDATE OF item SKIP LOCKED""",
                            (
                                source_id,
                                source_id,
                                surface,
                                surface,
                                self.semantic_runtime.model,
                                self.semantic_runtime.fingerprint,
                                self.semantic_runtime.dimensions,
                                batch_size,
                            ),
                        ).fetchall()
                    if not rows:
                        if use_watermark:
                            watermark = conn.execute(
                                """SELECT COALESCE(max(id),%s) AS value
                                   FROM items
                                   WHERE id>%s
                                     AND deleted_at IS NULL
                                     AND btrim(text_redacted) <> ''""",
                                (watermark, watermark),
                            ).fetchone()["value"]
                            conn.execute(
                                """UPDATE embedding_projection_watermarks
                                   SET last_item_id=GREATEST(last_item_id,%s),
                                       updated_at=now()
                                   WHERE runtime_fingerprint=%s""",
                                (watermark, self.semantic_runtime.fingerprint),
                            )
                        break
                    vectors = self.semantic_runtime.embed_documents(
                        [row["text_redacted"] for row in rows],
                    )
                    values = []
                    for row, vector in zip(rows, vectors, strict=True):
                        content_hash = hashlib.sha256(row["text_redacted"].encode()).hexdigest()
                        values.append((
                            row["id"], row["source_id"], self.semantic_runtime.model,
                            self.semantic_runtime.dimensions, row["projector_version"],
                            content_hash, self.semantic_runtime.fingerprint,
                            json.dumps(vector, separators=(",", ":")),
                        ))
                    with conn.transaction():
                        with conn.cursor() as cursor:
                            cursor.executemany(
                                """INSERT INTO item_embeddings(
                                     item_id,source_id,model,dimensions,projector_version,
                                     content_sha256,runtime_fingerprint,embedding
                                   ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s::halfvec)
                                   ON CONFLICT(item_id) DO UPDATE SET
                                     source_id=excluded.source_id,model=excluded.model,
                                     dimensions=excluded.dimensions,
                                     projector_version=excluded.projector_version,
                                     content_sha256=excluded.content_sha256,
                                     runtime_fingerprint=excluded.runtime_fingerprint,
                                     embedding=excluded.embedding,embedded_at=now()""",
                                values,
                            )
                            if use_watermark:
                                watermark = max(row["id"] for row in rows)
                                cursor.execute(
                                    """UPDATE embedding_projection_watermarks
                                       SET last_item_id=GREATEST(last_item_id,%s),
                                           updated_at=now()
                                       WHERE runtime_fingerprint=%s""",
                                    (
                                        watermark,
                                        self.semantic_runtime.fingerprint,
                                    ),
                                )
                    processed += len(rows)
                    batches += 1
            finally:
                if source_scoped:
                    conn.execute(
                        "SELECT pg_advisory_unlock_shared(hashtextextended(%s,0))",
                        (global_lock,),
                    )
                else:
                    conn.execute(
                        "SELECT pg_advisory_unlock(hashtextextended(%s,0))",
                        (global_lock,),
                    )
        return {
            "status": "complete", "processed": processed, "batches": batches,
            "source_scoped": source_id is not None,
            "surface_scoped": surface is not None,
        }

    @staticmethod
    def _dedupe_turn_embeddings_for_session(
        connection: Any,
        source_id: str,
        session_native_id: str,
    ) -> None:
        connection.execute(
            """WITH ranked AS (
                 SELECT embedding.anchor_item_id,
                        row_number() OVER (
                          PARTITION BY anchor.text_redacted
                          ORDER BY embedding.anchor_item_id DESC
                        ) AS position
                 FROM turn_embeddings embedding
                 JOIN items anchor ON anchor.id=embedding.anchor_item_id
                 WHERE embedding.source_id=%s
                   AND embedding.session_native_id=%s
               )
               DELETE FROM turn_embeddings duplicate
               USING ranked
               WHERE duplicate.anchor_item_id=ranked.anchor_item_id
                 AND ranked.position>1""",
            (source_id, session_native_id),
        )

    def embed_pending_turns(
        self,
        batch_size: int = 128,
        max_batches: int | None = None,
        source_id: str | None = None,
        dirty_only: bool = False,
    ) -> dict[str, Any]:
        """Embed user request plus its complete assistant response as one retrieval unit."""
        if self.semantic_runtime is None:
            raise ValueError("semantic runtime is not configured")
        if not 1 <= batch_size <= 1000 or (max_batches is not None and max_batches < 1):
            raise ValueError("invalid turn embedding backfill bounds")
        if source_id is not None and not SOURCE_ID_RE.fullmatch(source_id):
            raise ValueError("invalid embedding source")
        if not isinstance(dirty_only, bool):
            raise ValueError("invalid dirty-only mode")
        fingerprint = turn_runtime_fingerprint(self.semantic_runtime.fingerprint)
        processed = batches = 0
        with self.connect() as conn:
            lock_name = "recall:turn-embeddings"
            lock_function = (
                "pg_try_advisory_lock_shared"
                if dirty_only
                else "pg_try_advisory_lock"
            )
            locked = conn.execute(
                f"SELECT {lock_function}(hashtextextended(%s,0)) AS value",
                (lock_name,),
            ).fetchone()["value"]
            if not locked:
                return {"status": "busy", "processed": 0, "batches": 0}
            try:
                use_watermark = source_id is None and not dirty_only
                watermark = 0
                if use_watermark:
                    conn.execute(
                        """INSERT INTO turn_embedding_projection_watermarks(
                             runtime_fingerprint,model,dimensions,last_anchor_item_id
                           ) VALUES (%s,%s,%s,0)
                           ON CONFLICT(runtime_fingerprint) DO NOTHING""",
                        (
                            fingerprint,
                            self.semantic_runtime.model,
                            self.semantic_runtime.dimensions,
                        ),
                    )
                    watermark = conn.execute(
                        """SELECT last_anchor_item_id
                           FROM turn_embedding_projection_watermarks
                           WHERE runtime_fingerprint=%s
                           FOR UPDATE""",
                        (fingerprint,),
                    ).fetchone()["last_anchor_item_id"]
                while max_batches is None or batches < max_batches:
                    dirty = conn.execute(
                        """SELECT source_id,session_native_id,last_anchor_item_id
                           FROM turn_embedding_dirty_sessions
                           WHERE (%s::text IS NULL OR source_id=%s)
                           ORDER BY updated_at,source_id,session_native_id
                           LIMIT 1
                           FOR UPDATE SKIP LOCKED""",
                        (source_id, source_id),
                    ).fetchone()
                    if dirty is None and dirty_only:
                        break
                    scan_source_id = (
                        dirty["source_id"] if dirty is not None else source_id
                    )
                    scan_session_native_id = (
                        dirty["session_native_id"]
                        if dirty is not None
                        else None
                    )
                    scan_after = (
                        dirty["last_anchor_item_id"]
                        if dirty is not None
                        else (watermark if use_watermark else None)
                    )
                    rows = conn.execute(
                        """
                        WITH latest_anchors AS MATERIALIZED (
                          SELECT max(candidate_anchor.id) AS anchor_item_id
                          FROM items candidate_anchor
                          WHERE candidate_anchor.deleted_at IS NULL
                            AND candidate_anchor.role='user'
                            AND btrim(candidate_anchor.text_redacted)<>''
                            AND (
                              %s::text IS NULL
                              OR candidate_anchor.source_id=%s
                            )
                            AND (
                              %s::text IS NULL
                              OR candidate_anchor.session_native_id=%s
                            )
                            AND (
                              %s::bigint IS NULL
                              OR candidate_anchor.id>%s
                            )
                          GROUP BY candidate_anchor.source_id,
                                   candidate_anchor.session_native_id,
                                   candidate_anchor.text_redacted
                          ORDER BY max(candidate_anchor.id)
                        )
                        SELECT anchor.id AS anchor_item_id,anchor.source_id,
                               anchor.session_native_id,anchor.text_redacted AS user_text,
                               response.response_item_id,response.item_ids,
                               response.assistant_texts
                        FROM latest_anchors latest_anchor
                        JOIN items anchor
                          ON anchor.id=latest_anchor.anchor_item_id
                        LEFT JOIN LATERAL (
                          SELECT boundary.occurred_at,boundary.id
                          FROM items boundary
                          WHERE boundary.source_id=anchor.source_id
                            AND boundary.session_native_id=anchor.session_native_id
                            AND boundary.deleted_at IS NULL
                            AND boundary.role='user'
                            AND (boundary.occurred_at,boundary.id)>
                                (anchor.occurred_at,anchor.id)
                          ORDER BY boundary.occurred_at,boundary.id
                          LIMIT 1
                        ) next_user ON true
                        JOIN LATERAL (
                          SELECT
                            (array_agg(candidate.id ORDER BY
                               candidate.occurred_at DESC,candidate.id DESC))[1]
                              AS response_item_id,
                            array_agg(candidate.id ORDER BY
                               candidate.occurred_at,candidate.id) AS item_ids,
                            array_agg(candidate.text_redacted ORDER BY
                               candidate.occurred_at,candidate.id) AS assistant_texts
                          FROM items candidate
                          WHERE candidate.source_id=anchor.source_id
                            AND candidate.session_native_id=anchor.session_native_id
                            AND candidate.deleted_at IS NULL
                            AND candidate.role='assistant'
                            AND btrim(candidate.text_redacted)<>''
                            AND (candidate.occurred_at,candidate.id)>
                                (anchor.occurred_at,anchor.id)
                            AND (candidate.occurred_at,candidate.id)<
                                (
                                  COALESCE(
                                    next_user.occurred_at,
                                    'infinity'::timestamptz
                                  ),
                                  COALESCE(
                                    next_user.id,9223372036854775807
                                  )
                                )
                          HAVING count(*)>0
                        ) response ON true
                        LEFT JOIN turn_embeddings embedding
                          ON embedding.anchor_item_id=anchor.id
                        WHERE (
                            %s::boolean
                            OR (
                              embedding.anchor_item_id IS NULL
                              OR embedding.response_item_id<>
                                 response.response_item_id
                              OR embedding.model<>%s
                              OR embedding.runtime_fingerprint<>%s
                              OR embedding.dimensions<>%s
                              OR cardinality(response.item_ids)+1<>(
                                SELECT count(*)
                                FROM turn_embedding_items linked
                                WHERE linked.anchor_item_id=anchor.id
                              )
                              OR EXISTS (
                                SELECT 1
                                FROM turn_embedding_items linked
                                JOIN items linked_item
                                  ON linked_item.id=linked.item_id
                                WHERE linked.anchor_item_id=anchor.id
                                  AND linked_item.deleted_at IS NOT NULL
                              )
                            )
                          )
                        ORDER BY anchor.id
                        LIMIT %s
                        FOR UPDATE OF anchor SKIP LOCKED
                        """,
                        (
                            scan_source_id,
                            scan_source_id,
                            scan_session_native_id,
                            scan_session_native_id,
                            scan_after,
                            scan_after,
                            dirty is not None,
                            self.semantic_runtime.model,
                            fingerprint,
                            self.semantic_runtime.dimensions,
                            batch_size,
                        ),
                    ).fetchall()
                    if not rows:
                        if dirty is not None:
                            self._dedupe_turn_embeddings_for_session(
                                conn,
                                dirty["source_id"],
                                dirty["session_native_id"],
                            )
                            conn.execute(
                                """DELETE FROM turn_embedding_dirty_sessions
                                   WHERE source_id=%s
                                     AND session_native_id=%s""",
                                (
                                    dirty["source_id"],
                                    dirty["session_native_id"],
                                ),
                            )
                            conn.commit()
                            continue
                        if use_watermark:
                            watermark = conn.execute(
                                """SELECT COALESCE(max(id),%s) AS value
                                   FROM items
                                   WHERE id>%s AND deleted_at IS NULL
                                     AND role='user' AND btrim(text_redacted)<>''""",
                                (watermark, watermark),
                            ).fetchone()["value"]
                            conn.execute(
                                """UPDATE turn_embedding_projection_watermarks
                                   SET last_anchor_item_id=GREATEST(
                                         last_anchor_item_id,%s
                                       ),
                                       updated_at=now()
                                   WHERE runtime_fingerprint=%s""",
                                (watermark, fingerprint),
                            )
                        break
                    documents = [
                        turn_embedding_text(
                            row["user_text"], row["assistant_texts"],
                        )
                        for row in rows
                    ]
                    vectors = self.semantic_runtime.embed_documents(documents)
                    with conn.transaction():
                        with conn.cursor() as cursor:
                            cursor.executemany(
                                """DELETE FROM turn_embeddings older_embedding
                                   USING items older_anchor
                                   WHERE older_embedding.anchor_item_id=
                                         older_anchor.id
                                     AND older_embedding.source_id=%s
                                     AND older_embedding.session_native_id=%s
                                     AND older_embedding.anchor_item_id<>%s
                                     AND older_anchor.text_redacted=%s""",
                                [
                                    (
                                        row["source_id"],
                                        row["session_native_id"],
                                        row["anchor_item_id"],
                                        row["user_text"],
                                    )
                                    for row in rows
                                ],
                            )
                            cursor.executemany(
                                """INSERT INTO turn_embeddings(
                                     anchor_item_id,response_item_id,source_id,
                                     session_native_id,model,dimensions,
                                     content_sha256,runtime_fingerprint,embedding
                                   ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s::halfvec)
                                   ON CONFLICT(anchor_item_id) DO UPDATE SET
                                     response_item_id=excluded.response_item_id,
                                     source_id=excluded.source_id,
                                     session_native_id=excluded.session_native_id,
                                     model=excluded.model,
                                     dimensions=excluded.dimensions,
                                     content_sha256=excluded.content_sha256,
                                     runtime_fingerprint=excluded.runtime_fingerprint,
                                     embedding=excluded.embedding,
                                     embedded_at=now()""",
                                [
                                    (
                                        row["anchor_item_id"],
                                        row["response_item_id"],
                                        row["source_id"],
                                        row["session_native_id"],
                                        self.semantic_runtime.model,
                                        self.semantic_runtime.dimensions,
                                        hashlib.sha256(document.encode()).hexdigest(),
                                        fingerprint,
                                        json.dumps(vector, separators=(",", ":")),
                                    )
                                    for row, document, vector in zip(
                                        rows, documents, vectors, strict=True
                                    )
                                ],
                            )
                            cursor.executemany(
                                """DELETE FROM turn_embedding_items
                                   WHERE anchor_item_id=%s""",
                                [(row["anchor_item_id"],) for row in rows],
                            )
                            cursor.executemany(
                                """INSERT INTO turn_embedding_items(
                                     anchor_item_id,item_id
                                   ) VALUES (%s,%s)""",
                                [
                                    (row["anchor_item_id"], item_id)
                                    for row in rows
                                    for item_id in (
                                        row["anchor_item_id"],
                                        *row["item_ids"],
                                    )
                                ],
                            )
                            if use_watermark and dirty is None:
                                watermark = max(
                                    row["anchor_item_id"] for row in rows
                                )
                                cursor.execute(
                                    """UPDATE turn_embedding_projection_watermarks
                                       SET last_anchor_item_id=GREATEST(
                                             last_anchor_item_id,%s
                                           ),
                                           updated_at=now()
                                       WHERE runtime_fingerprint=%s""",
                                    (watermark, fingerprint),
                                )
                            if dirty is not None:
                                cursor.execute(
                                    """UPDATE turn_embedding_dirty_sessions
                                       SET last_anchor_item_id=GREATEST(
                                             last_anchor_item_id,%s
                                           ),
                                           updated_at=updated_at
                                       WHERE source_id=%s
                                         AND session_native_id=%s""",
                                    (
                                        max(
                                            row["anchor_item_id"]
                                            for row in rows
                                        ),
                                        dirty["source_id"],
                                        dirty["session_native_id"],
                                    ),
                                )
                    processed += len(rows)
                    batches += 1
            finally:
                unlock_function = (
                    "pg_advisory_unlock_shared"
                    if dirty_only
                    else "pg_advisory_unlock"
                )
                conn.execute(
                    f"SELECT {unlock_function}(hashtextextended(%s,0))",
                    (lock_name,),
                )
        return {
            "status": "complete",
            "processed": processed,
            "batches": batches,
            "source_scoped": source_id is not None,
        }

    def record_dead_letter(self, error_code: str, summary: str) -> None:
        """Record rejection metadata only; never persist the rejected payload."""
        with self.connect() as conn:
            conn.execute(
                "INSERT INTO dead_letters(error_code,error_summary) VALUES (%s,%s)",
                (error_code[:100], redact_text(summary)[:500]),
            )
            conn.execute(
                "INSERT INTO audit_events(operation,status,metadata) VALUES ('ingest','rejected',%s)",
                (json.dumps({"error_code": error_code[:100]}),),
            )

    def ingest(self, idempotency_key: str, events: list[dict]) -> tuple[dict, bool]:
        if not idempotency_key or len(idempotency_key) > 200:
            raise ValueError("invalid idempotency key")
        if not events:
            raise ValueError("empty ingest batch")
        for envelope in events:
            validate_envelope(envelope)
        request_hash = hashlib.sha256(canonical_json(events)).hexdigest()
        batch_id = uuid.uuid4()
        with self.connect() as conn:
            with conn.transaction():
                conn.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                    ("batch\x1f" + idempotency_key,),
                )
                existing = conn.execute(
                    "SELECT request_sha256, acknowledgement FROM ingest_batches WHERE idempotency_key=%s FOR UPDATE",
                    (idempotency_key,),
                ).fetchone()
                if existing:
                    if existing["request_sha256"] != request_hash:
                        raise IdempotencyConflict("idempotency key reused with different request")
                    return existing["acknowledgement"], True

                placeholder = {"batch_id": str(batch_id), "status": "building"}
                conn.execute(
                    "INSERT INTO ingest_batches(id,idempotency_key,request_sha256,status,acknowledgement) VALUES (%s,%s,%s,'committed',%s)",
                    (batch_id, idempotency_key, request_hash, json.dumps(placeholder)),
                )
                receipts: list[str] = []
                inserted = 0
                duplicate_events = 0
                pending_projections: list[tuple[int, dict, int]] = []
                source_principals: dict[str, str] = {}
                for envelope in events:
                    source_id = envelope["source_id"]
                    principal_id = envelope["principal_id"]
                    if source_id in source_principals and source_principals[source_id] != principal_id:
                        raise ValueError("source principal mismatch within batch")
                    source_principals[source_id] = principal_id
                for source_id, principal_id in source_principals.items():
                    conn.execute(
                        "INSERT INTO sources(id,principal_id) VALUES (%s,%s) ON CONFLICT(id) DO NOTHING",
                        (source_id, principal_id),
                    )
                    source = conn.execute("SELECT principal_id FROM sources WHERE id=%s", (source_id,)).fetchone()
                    if source["principal_id"] != principal_id:
                        raise ValueError("source principal mismatch")
                    conn.execute(
                        "INSERT INTO source_grants(source_id,principal_id,permission) VALUES (%s,%s,'owner') ON CONFLICT DO NOTHING",
                        (source_id, principal_id),
                    )
                identities = sorted({(envelope["source_id"], envelope["native_id"]) for envelope in events})
                lock_keys = [advisory_lock_key(source_id, native_id) for source_id, native_id in identities]
                conn.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended(value,0)) FROM unnest(%s::text[]) AS locks(value) ORDER BY value",
                    (lock_keys,),
                ).fetchall()
                wanted = ",".join(["(%s,%s)"] * len(identities))
                identity_params = [value for identity in identities for value in identity]
                existing_rows = conn.execute(
                    f"""WITH wanted(source_id,native_id) AS (VALUES {wanted})
                        SELECT event.id,event.source_id,event.native_id,event.content_sha256,event.revision
                        FROM source_events event JOIN wanted USING(source_id,native_id)""",
                    identity_params,
                ).fetchall()
                existing_by_content = {
                    (row["source_id"], row["native_id"], row["content_sha256"]): row["revision"]
                    for row in existing_rows
                }
                max_revision = {}
                for row in existing_rows:
                    identity = (row["source_id"], row["native_id"])
                    max_revision[identity] = max(max_revision.get(identity, 0), row["revision"])
                for envelope in events:
                    source_id = envelope["source_id"]
                    identity = (source_id, envelope["native_id"])
                    content_identity = (*identity, envelope["content_sha256"])
                    if content_identity in existing_by_content:
                        revision = existing_by_content[content_identity]
                        duplicate_events += 1
                    else:
                        revision = max_revision.get(identity, 0) + 1
                        row = conn.execute(
                            """INSERT INTO source_events(source_id,native_id,native_parent_id,kind,occurred_at,observed_at,
                               principal_id,visibility,content_type,content_sha256,revision,envelope,is_tombstone,batch_id)
                               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id""",
                            (source_id, envelope["native_id"], envelope.get("native_parent_id"), envelope["kind"],
                             envelope["occurred_at"], envelope["observed_at"], envelope["principal_id"],
                             envelope["visibility"], envelope["content_type"], envelope["content_sha256"], revision,
                             json.dumps(envelope), envelope["kind"] == "tombstone", batch_id),
                        ).fetchone()
                        pending_projections.append((row["id"], envelope, revision))
                        existing_by_content[content_identity] = revision
                        max_revision[identity] = revision
                        inserted += 1
                    receipts.append(event_receipt(source_id, envelope["native_id"], revision))

                self._project_batch(conn, pending_projections)
                acknowledgement = {
                    "batch_id": str(batch_id),
                    "status": "committed",
                    "inserted": inserted,
                    "duplicate_events": duplicate_events,
                    "receipts": receipts,
                }
                conn.execute("UPDATE ingest_batches SET acknowledgement=%s WHERE id=%s", (json.dumps(acknowledgement), batch_id))
                conn.execute(
                    "INSERT INTO audit_events(operation,status,metadata) VALUES ('ingest','success',%s)",
                    (json.dumps({"batch_id": str(batch_id), "event_count": len(events), "request_sha256": request_hash}),),
                )
                return acknowledgement, False

    def _project_batch(
        self,
        conn,
        events: Iterable[tuple[int, dict, int]],
    ) -> None:
        latest_event_id: int | None = None
        for event_id, envelope, revision in events:
            self._project_one(conn, event_id, envelope, revision)
            if latest_event_id is None or event_id > latest_event_id:
                latest_event_id = event_id
        if latest_event_id is not None:
            self._advance_projector(conn, latest_event_id)

    def _project_one(self, conn, event_id: int, envelope: dict, revision: int) -> None:
        session_id = effective_session_id(envelope)
        if envelope["kind"] == "tombstone":
            target = envelope.get("content", {}).get("target_native_id") or envelope["native_id"]
            conn.execute(
                "UPDATE items SET deleted_at=now() WHERE source_id=%s AND event_native_id=%s AND deleted_at IS NULL",
                (envelope["source_id"], target),
            )
            return
        items, metadata = project(envelope, revision)
        # A source/native identity is one logical record whose immutable
        # source_events rows retain its history. Only the latest revision may
        # remain in the live search projection.
        conn.execute(
            """UPDATE items
               SET deleted_at=now()
               WHERE source_id=%s
                 AND event_native_id=%s
                 AND deleted_at IS NULL""",
            (envelope["source_id"], envelope["native_id"]),
        )
        if not items and set(metadata) <= {"projector_version", "harness"}:
            return
        conn.execute(
            """INSERT INTO sessions(source_id,native_id,principal_id,harness,started_at,ended_at,metadata,projector_version)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
               ON CONFLICT(source_id,native_id) DO UPDATE SET
                 started_at=LEAST(sessions.started_at,excluded.started_at),
                 ended_at=GREATEST(sessions.ended_at,excluded.ended_at),
                 metadata=sessions.metadata || excluded.metadata,
                 projector_version=excluded.projector_version,rebuilt_at=now()""",
            (envelope["source_id"], session_id, envelope["principal_id"], metadata.get("harness"),
             envelope["occurred_at"], envelope["occurred_at"], json.dumps(metadata), PROJECTOR_VERSION),
        )
        for item in items:
            occurred = item["occurred_at"]
            if isinstance(occurred, (int, float)):
                occurred = None if occurred is None else __import__("datetime").datetime.fromtimestamp(occurred, __import__("datetime").timezone.utc)
            row = conn.execute(
                """INSERT INTO items(event_id,source_id,session_native_id,event_native_id,ordinal,occurred_at,role,surface,
                   text_redacted,receipt,projector_version) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id""",
                (event_id, envelope["source_id"], session_id, envelope["native_id"], item["ordinal"], occurred,
                 item["role"], item["surface"], item["text_redacted"], item["receipt"], PROJECTOR_VERSION),
            ).fetchone()
            conn.execute(
                "INSERT INTO chunks(item_id,ordinal,text_redacted,receipt) VALUES (%s,0,%s,%s)",
                (row["id"], item["text_redacted"], item["receipt"]),
            )
            if item.get("entities"):
                with conn.cursor() as cursor:
                    cursor.executemany(
                        "INSERT INTO entities(item_id,source_id,kind,value,normalized) VALUES (%s,%s,%s,%s,%s)",
                        [
                            (row["id"], envelope["source_id"], entity["kind"], entity["value"], entity["normalized"])
                            for entity in item["entities"]
                        ],
                    )
    def _advance_projector(self, conn, event_id: int) -> None:
        conn.execute(
            """INSERT INTO projection_watermarks(projector,version,last_event_id) VALUES ('items',%s,%s)
               ON CONFLICT(projector) DO UPDATE SET version=excluded.version,
               last_event_id=GREATEST(projection_watermarks.last_event_id,excluded.last_event_id),updated_at=now()""",
            (PROJECTOR_VERSION, event_id),
        )

    def resolve(self, receipt: str, authorized_source: str | None = None) -> dict | None:
        event_part = receipt.split("#", 1)[0]
        try:
            base, query = event_part.rsplit("?rev=", 1)
            source_native = base.removeprefix("recall://")
            source_id, native_id = source_native.split("/", 1)
            revision = int(query)
        except (ValueError, TypeError):
            raise ValueError("invalid receipt") from None
        with self.connect() as conn:
            event = conn.execute(
                """SELECT id,source_id,native_id,native_parent_id,kind,occurred_at,observed_at,principal_id,
                   visibility,content_type,content_sha256,revision,is_tombstone,
                   jsonb_strip_nulls(jsonb_build_object(
                     'uri', envelope #>> '{provenance,uri}',
                     'original_path', envelope #>> '{provenance,original_path}',
                     'archive', envelope #>> '{provenance,archive}',
                     'member', envelope #>> '{provenance,member}',
                     'harness', envelope #>> '{provenance,harness}',
                     'cwd', envelope #>> '{provenance,cwd}',
                     'branch', envelope #>> '{provenance,branch}',
                     'slot', envelope #>> '{provenance,slot}'
                   )) AS provenance
                   FROM source_events event
                   WHERE source_id=%s AND native_id=%s AND revision=%s
                     AND (%s::text IS NULL OR event.source_id=%s)
                     AND NOT EXISTS (
                       SELECT 1 FROM source_events later
                       WHERE later.source_id=event.source_id
                         AND later.native_id=event.native_id
                         AND later.revision>event.revision
                         AND later.is_tombstone
                     )""",
                (source_id, native_id, revision, authorized_source, authorized_source),
            ).fetchone()
            if not event:
                return None
            items = conn.execute(
                """SELECT ordinal,occurred_at,role,surface,text_redacted,receipt FROM items
                   WHERE event_id=%s AND deleted_at IS NULL ORDER BY ordinal""",
                (event["id"],),
            ).fetchall()
            return {"event": {key: value for key, value in event.items() if key != "id"}, "items": items}

    @staticmethod
    def _read_filters(
        filters: dict,
        authorized_source: str | list[str] | tuple[str, ...] | None = None,
        routed_source_ids: list[str] | None = None,
    ) -> tuple[str, list[Any]]:
        allowed = {"since", "until", "cwd", "branch", "harness", "source_id", "source_family", "source_alias"}
        if set(filters) - allowed:
            raise ValueError("unsupported search filter")
        clauses = ["i.deleted_at IS NULL"]
        params: list[Any] = []
        if isinstance(authorized_source, str):
            clauses.append("i.source_id = %s"); params.append(authorized_source)
        elif authorized_source is not None:
            clauses.append("i.source_id = ANY(%s)")
            params.append(list(authorized_source))
        if filters.get("source_id"):
            source_id = filters["source_id"]
            if (
                not isinstance(source_id, str)
                or not re.fullmatch(r"[A-Za-z0-9_.:@-]{3,160}", source_id)
            ):
                raise ValueError("invalid source_id filter")
            clauses.append("i.source_id = %s"); params.append(source_id)
        if filters.get("source_family"):
            family = filters["source_family"]
            if family not in SOURCE_FAMILIES:
                raise ValueError("unsupported source_family filter")
        if filters.get("source_alias"):
            alias = filters["source_alias"]
            if not isinstance(alias, str) or not re.fullmatch(r"[a-z0-9][a-z0-9._-]{1,63}", alias):
                raise ValueError("invalid source_alias filter")
        if routed_source_ids is not None:
            clauses.append("i.source_id = ANY(%s)")
            params.append(routed_source_ids)
        if filters.get("since"):
            clauses.append("i.occurred_at >= %s::timestamptz"); params.append(filters["since"])
        if filters.get("until"):
            clauses.append("i.occurred_at <= %s::timestamptz"); params.append(filters["until"])
        if filters.get("cwd"):
            clauses.append("COALESCE(s.metadata->>'cwd','') ILIKE %s"); params.append("%" + str(filters["cwd"]) + "%")
        if filters.get("branch"):
            clauses.append("COALESCE(s.metadata->>'branch','') ILIKE %s"); params.append("%" + str(filters["branch"]) + "%")
        if filters.get("harness"):
            if filters["harness"] not in {"claude", "codex"}:
                raise ValueError("unsupported harness filter")
            clauses.append("s.harness = %s"); params.append(filters["harness"])
        return " AND ".join(clauses), params

    @staticmethod
    def _resolve_routed_source_ids(conn, filters: dict) -> list[str] | None:
        routed: set[str] | None = None
        family = filters.get("source_family")
        if family is not None:
            if family not in SOURCE_FAMILIES:
                raise ValueError("unsupported source_family filter")
            family_sources = {
                row["source_id"] for row in conn.execute(
                    "SELECT source_id FROM source_profiles WHERE family=%s ORDER BY source_id",
                    (family,),
                ).fetchall()
            }
            routed = family_sources
        alias = filters.get("source_alias")
        if alias is not None:
            if not isinstance(alias, str) or not re.fullmatch(r"[a-z0-9][a-z0-9._-]{1,63}", alias):
                raise ValueError("invalid source_alias filter")
            row = conn.execute(
                "SELECT source_id FROM source_aliases WHERE alias=%s", (alias,),
            ).fetchone()
            alias_sources = {row["source_id"]} if row else set()
            routed = alias_sources if routed is None else routed & alias_sources
        return None if routed is None else sorted(routed)

    def _lexical_leg(self, conn, query: str, query_function: str, filters: dict,
                     leg: str, tier: int, *, exact: str | None = None,
                     limit: int = 400, authorized_source: str | None = None,
                     routed_source_ids: list[str] | None = None,
                     deadline_at: float | None = None) -> list[dict]:
        if query_function not in {"plainto_tsquery", "phraseto_tsquery", "websearch_to_tsquery"}:
            raise ValueError("unsupported query function")
        where, params = self._read_filters(filters, authorized_source, routed_source_ids)
        exact_sql = ""
        query_params: list[Any] = [query]
        if exact is not None:
            exact_sql = " AND strpos(lower(i.text_redacted), %s) > 0"
            query_params.append(exact.casefold())
        sql = f"""
            SELECT i.id,i.source_id,i.session_native_id,i.event_native_id,i.occurred_at,i.surface,
                   i.text_redacted,i.receipt,i.projector_version,s.started_at,s.ended_at,s.metadata,
                   se.envelope #>> '{{provenance,original_path}}' AS path,se.observed_at,
                   coalesce(sp.family,'unclassified') AS source_family,
                   coalesce(sp.quality,'unrated') AS source_quality,
                   coalesce(sp.freshness_half_life_days,180) AS freshness_half_life_days,
                   (sp.source_id IS NOT NULL) AS source_profiled,
                   ts_rank_cd(to_tsvector('simple',i.text_redacted),
                              {query_function}('simple',%s),32) AS lexical_rank
            FROM items i
            JOIN sessions s ON s.source_id=i.source_id AND s.native_id=i.session_native_id
            JOIN source_events se ON se.id=i.event_id
            LEFT JOIN source_profiles sp ON sp.source_id=i.source_id
            WHERE to_tsvector('simple',i.text_redacted) @@ {query_function}('simple',%s)
              AND {where}{exact_sql}
            ORDER BY lexical_rank DESC,i.occurred_at DESC NULLS LAST,i.id DESC
            LIMIT %s
        """
        # The same query feeds rank and match; filters remain structurally parameterized.
        values = [query, query, *params]
        if exact is not None:
            values.append(exact.casefold())
        values.append(limit)
        rows = self._execute_bounded(conn, sql, values, deadline_at).fetchall()
        return [{**dict(row), "leg": leg, "tier": tier} for row in rows]

    def _exact_question_leg(self, conn, query: str, filters: dict, *,
                            authorized_source: str | None = None, limit: int = 20,
                            routed_source_ids: list[str] | None = None,
                            deadline_at: float | None = None) -> list[dict]:
        """Find a repeated user question without ranking a broad lexical result set."""
        where, params = self._read_filters(filters, authorized_source, routed_source_ids)
        rows = self._execute_bounded(
            conn,
            f"""
            SELECT i.id,i.source_id,i.session_native_id,i.event_native_id,i.occurred_at,i.surface,
                   i.text_redacted,i.receipt,i.projector_version,s.started_at,s.ended_at,s.metadata,
                   se.envelope #>> '{{provenance,original_path}}' AS path,se.observed_at,
                   coalesce(sp.family,'unclassified') AS source_family,
                   coalesce(sp.quality,'unrated') AS source_quality,
                   coalesce(sp.freshness_half_life_days,180) AS freshness_half_life_days,
                   (sp.source_id IS NOT NULL) AS source_profiled,
                   1.0::real AS lexical_rank
            FROM items i
            JOIN sessions s ON s.source_id=i.source_id AND s.native_id=i.session_native_id
            JOIN source_events se ON se.id=i.event_id
            LEFT JOIN source_profiles sp ON sp.source_id=i.source_id
            WHERE i.role='user'
              AND i.text_redacted=%s
              AND to_tsvector('simple',i.text_redacted) @@ plainto_tsquery('simple',%s)
              AND {where}
            ORDER BY i.occurred_at DESC NULLS LAST,i.id DESC
            LIMIT %s
            """,
            [query, query, *params, limit],
            deadline_at,
        ).fetchall()
        return [{**dict(row), "leg": "exact-question", "tier": 3} for row in rows]

    @staticmethod
    def _execute_bounded(conn, sql: str, values: list[Any] | tuple[Any, ...], deadline_at: float | None):
        if deadline_at is None:
            return conn.execute(sql, values)
        remaining_ms = int((deadline_at - time.monotonic()) * 1000)
        if remaining_ms <= 0:
            raise SearchDeadlineExceeded("search deadline exceeded")
        conn.execute("SELECT set_config('statement_timeout', %s, true)", (f"{remaining_ms}ms",))
        try:
            return conn.execute(sql, values)
        except psycopg.errors.QueryCanceled as exc:
            raise SearchDeadlineExceeded("search deadline exceeded") from exc

    def _entity_leg(self, conn, values: list[str], filters: dict,
                    *, authorized_source: str | None = None, limit: int = 400,
                    routed_source_ids: list[str] | None = None,
                    deadline_at: float | None = None, tier: int = 3) -> list[dict]:
        normalized = sorted({value.casefold() for value in values if value})
        if not normalized:
            return []
        where, params = self._read_filters(filters, authorized_source, routed_source_ids)
        rows = self._execute_bounded(
            conn,
            f"""
            SELECT DISTINCT i.id,i.source_id,i.session_native_id,i.event_native_id,i.occurred_at,i.surface,
                   i.text_redacted,i.receipt,i.projector_version,s.started_at,s.ended_at,s.metadata,
                   se.envelope #>> '{{provenance,original_path}}' AS path,se.observed_at,
                   coalesce(sp.family,'unclassified') AS source_family,
                   coalesce(sp.quality,'unrated') AS source_quality,
                   coalesce(sp.freshness_half_life_days,180) AS freshness_half_life_days,
                   (sp.source_id IS NOT NULL) AS source_profiled,
                   1.0::real AS lexical_rank
            FROM entities e
            JOIN items i ON i.id=e.item_id
            JOIN sessions s ON s.source_id=i.source_id AND s.native_id=i.session_native_id
            JOIN source_events se ON se.id=i.event_id
            LEFT JOIN source_profiles sp ON sp.source_id=i.source_id
            WHERE octet_length(e.normalized) <= 512 AND e.normalized = ANY(%s) AND {where}
            ORDER BY i.id DESC
            LIMIT %s
            """,
            [normalized, *params, limit],
            deadline_at,
        ).fetchall()
        return [{**dict(row), "leg": "entity", "tier": tier} for row in rows]

    def _semantic_leg(self, conn, vector: list[float], filters: dict,
                      *, authorized_source: str | None = None, limit: int = 100,
                      routed_source_ids: list[str] | None = None,
                      deadline_at: float | None = None,
                      minimum_similarity: float = 0.35) -> list[dict]:
        if self.semantic_runtime is None:
            return []
        where, params = self._read_filters(filters, authorized_source, routed_source_ids)
        encoded = json.dumps(vector, separators=(",", ":"))
        # RRF depends on a stable position in each leg. pgvector's relaxed mode
        # explicitly permits out-of-order results, so use strict ordering and
        # a durable item-id tie break for reproducible rankings.
        conn.execute("SELECT set_config('hnsw.iterative_scan','strict_order',true)")
        rows = self._execute_bounded(
            conn,
            f"""
            SELECT i.id,i.source_id,i.session_native_id,i.event_native_id,i.occurred_at,i.surface,
                   i.text_redacted,i.receipt,i.projector_version,s.started_at,s.ended_at,s.metadata,
                   se.envelope #>> '{{provenance,original_path}}' AS path,se.observed_at,
                   coalesce(sp.family,'unclassified') AS source_family,
                   coalesce(sp.quality,'unrated') AS source_quality,
                   coalesce(sp.freshness_half_life_days,180) AS freshness_half_life_days,
                   (sp.source_id IS NOT NULL) AS source_profiled,
                   1-(embedding.embedding <=> %s::halfvec) AS lexical_rank
            FROM item_embeddings embedding
            JOIN items i ON i.id=embedding.item_id
            JOIN sessions s ON s.source_id=i.source_id AND s.native_id=i.session_native_id
            JOIN source_events se ON se.id=i.event_id
            LEFT JOIN source_profiles sp ON sp.source_id=i.source_id
            WHERE embedding.model=%s
              AND embedding.runtime_fingerprint=%s
              AND embedding.dimensions=%s
              AND embedding.projector_version=i.projector_version
              AND embedding.content_sha256=
                  encode(sha256(convert_to(i.text_redacted,'UTF8')),'hex')
              AND {where}
            ORDER BY embedding.embedding <=> %s::halfvec,i.id DESC
            LIMIT %s
            """,
            [
                encoded,
                self.semantic_runtime.model,
                self.semantic_runtime.fingerprint,
                self.semantic_runtime.dimensions,
                *params,
                encoded,
                limit,
            ],
            deadline_at,
        ).fetchall()
        return [
            {**dict(row), "leg": "semantic", "tier": 2}
            for row in rows
            if float(row["lexical_rank"]) >= minimum_similarity
        ]

    def _turn_semantic_leg(
        self,
        conn,
        vector: list[float],
        filters: dict,
        *,
        authorized_source: str | list[str] | tuple[str, ...] | None = None,
        limit: int = 100,
        routed_source_ids: list[str] | None = None,
        deadline_at: float | None = None,
        minimum_similarity: float = 0.35,
    ) -> list[dict]:
        """Retrieve a final answer using an embedding of its complete conversational turn."""
        if self.semantic_runtime is None:
            return []
        where, params = self._read_filters(
            filters, authorized_source, routed_source_ids,
        )
        encoded = json.dumps(vector, separators=(",", ":"))
        fingerprint = turn_runtime_fingerprint(self.semantic_runtime.fingerprint)
        conn.execute("SELECT set_config('hnsw.iterative_scan','strict_order',true)")
        rows = self._execute_bounded(
            conn,
            f"""
            WITH nearest AS MATERIALIZED (
              SELECT embedding.anchor_item_id,embedding.response_item_id,
                     embedding.embedding <=> %s::halfvec AS distance
              FROM turn_embeddings embedding
              JOIN items i ON i.id=embedding.response_item_id
              JOIN sessions s
                ON s.source_id=i.source_id
               AND s.native_id=i.session_native_id
              WHERE embedding.model=%s
                AND embedding.runtime_fingerprint=%s
                AND embedding.dimensions=%s
                AND {where}
              ORDER BY embedding.embedding <=> %s::halfvec,
                       embedding.anchor_item_id DESC
              LIMIT %s
            )
            SELECT i.id,i.source_id,i.session_native_id,i.event_native_id,
                   i.occurred_at,i.surface,i.text_redacted,i.receipt,
                   i.projector_version,s.started_at,s.ended_at,s.metadata,
                   se.envelope #>> '{{provenance,original_path}}' AS path,
                   se.observed_at,
                   coalesce(sp.family,'unclassified') AS source_family,
                   coalesce(sp.quality,'unrated') AS source_quality,
                   coalesce(sp.freshness_half_life_days,180)
                     AS freshness_half_life_days,
                   (sp.source_id IS NOT NULL) AS source_profiled,
                   (1-nearest.distance)::real AS lexical_rank
            FROM nearest
            JOIN items anchor ON anchor.id=nearest.anchor_item_id
            JOIN items i ON i.id=nearest.response_item_id
            JOIN sessions s
              ON s.source_id=i.source_id
             AND s.native_id=i.session_native_id
            JOIN source_events se ON se.id=i.event_id
            LEFT JOIN source_profiles sp ON sp.source_id=i.source_id
            LEFT JOIN LATERAL (
              SELECT boundary.occurred_at,boundary.id
              FROM items boundary
              WHERE boundary.source_id=anchor.source_id
                AND boundary.session_native_id=anchor.session_native_id
                AND boundary.deleted_at IS NULL
                AND boundary.role='user'
                AND (boundary.occurred_at,boundary.id)>
                    (anchor.occurred_at,anchor.id)
              ORDER BY boundary.occurred_at,boundary.id
              LIMIT 1
            ) next_user ON true
            JOIN LATERAL (
              SELECT candidate.id
              FROM items candidate
              WHERE candidate.source_id=anchor.source_id
                AND candidate.session_native_id=anchor.session_native_id
                AND candidate.deleted_at IS NULL
                AND candidate.role='assistant'
                AND (candidate.occurred_at,candidate.id)>
                    (anchor.occurred_at,anchor.id)
                AND (
                  next_user.id IS NULL
                  OR (candidate.occurred_at,candidate.id)<
                     (next_user.occurred_at,next_user.id)
                )
              ORDER BY candidate.occurred_at DESC,candidate.id DESC
              LIMIT 1
            ) current_response ON current_response.id=nearest.response_item_id
            WHERE anchor.deleted_at IS NULL
              AND NOT EXISTS (
                SELECT 1
                FROM turn_embedding_items linked
                JOIN items linked_item ON linked_item.id=linked.item_id
                WHERE linked.anchor_item_id=nearest.anchor_item_id
                  AND linked_item.deleted_at IS NOT NULL
              )
            ORDER BY nearest.distance,nearest.anchor_item_id DESC
            """,
            [
                encoded,
                self.semantic_runtime.model,
                fingerprint,
                self.semantic_runtime.dimensions,
                *params,
                encoded,
                limit,
            ],
            deadline_at,
        ).fetchall()
        return [
            {**dict(row), "leg": "turn-semantic", "tier": 2}
            for row in rows
            if float(row["lexical_rank"]) >= minimum_similarity
        ]

    def _answer_leg(self, conn, anchors: list[dict], filters: dict, *,
                    deadline_at: float | None = None) -> list[dict]:
        """Promote the final assistant item in a matched user's conversational turn."""
        bounded = sorted(
            anchors,
            key=lambda row: (
                float(row.get("fusion_score", 0.0)),
                float(row.get("lexical_rank", 0.0)),
                int(row["id"]),
            ),
            reverse=True,
        )[:20]
        if not bounded:
            return []
        rows = self._execute_bounded(
            conn,
            """
            WITH anchors AS (
              SELECT *
              FROM unnest(
                %s::bigint[],%s::double precision[],%s::integer[],%s::boolean[]
              ) WITH ORDINALITY AS value(
                anchor_id,lexical_rank,anchor_tier,exact_question,anchor_order
              )
            ), answers AS (
              SELECT value.lexical_rank,value.anchor_tier,value.exact_question,
                     value.anchor_order,response.id
              FROM anchors value
              JOIN items anchor ON anchor.id=value.anchor_id
              LEFT JOIN LATERAL (
                SELECT boundary.occurred_at,boundary.id
                FROM items boundary
                WHERE boundary.source_id=anchor.source_id
                  AND boundary.session_native_id=anchor.session_native_id
                  AND boundary.deleted_at IS NULL
                  AND boundary.role='user'
                  AND (boundary.occurred_at,boundary.id)>(anchor.occurred_at,anchor.id)
                ORDER BY boundary.occurred_at,boundary.id
                LIMIT 1
              ) next_user ON true
              JOIN LATERAL (
                SELECT candidate.id
                FROM items candidate
                WHERE anchor.role='user'
                  AND candidate.source_id=anchor.source_id
                  AND candidate.session_native_id=anchor.session_native_id
                  AND candidate.deleted_at IS NULL
                  AND candidate.role='assistant'
                  AND (candidate.occurred_at,candidate.id)>(anchor.occurred_at,anchor.id)
                  AND (%s::timestamptz IS NULL OR candidate.occurred_at >= %s::timestamptz)
                  AND (%s::timestamptz IS NULL OR candidate.occurred_at <= %s::timestamptz)
                  AND (
                    next_user.id IS NULL
                    OR (candidate.occurred_at,candidate.id)
                       <(next_user.occurred_at,next_user.id)
                  )
                ORDER BY candidate.occurred_at DESC,candidate.id DESC
                LIMIT 1
              ) response ON true
              WHERE anchor.deleted_at IS NULL
            )
            SELECT i.id,i.source_id,i.session_native_id,i.event_native_id,i.occurred_at,i.surface,
                   i.text_redacted,i.receipt,i.projector_version,s.started_at,s.ended_at,s.metadata,
                   se.envelope #>> '{provenance,original_path}' AS path,se.observed_at,
                   coalesce(sp.family,'unclassified') AS source_family,
                   coalesce(sp.quality,'unrated') AS source_quality,
                   coalesce(sp.freshness_half_life_days,180) AS freshness_half_life_days,
                   (sp.source_id IS NOT NULL) AS source_profiled,
                   answers.lexical_rank::real AS lexical_rank,
                   answers.anchor_tier,answers.exact_question
            FROM answers
            JOIN items i ON i.id=answers.id
            JOIN sessions s ON s.source_id=i.source_id AND s.native_id=i.session_native_id
            JOIN source_events se ON se.id=i.event_id
            LEFT JOIN source_profiles sp ON sp.source_id=i.source_id
            ORDER BY answers.anchor_order
            """,
            [
                [int(row["id"]) for row in bounded],
                [float(row.get("lexical_rank", 0.0)) for row in bounded],
                [int(row.get("tier", 1)) for row in bounded],
                ["exact-question" in row.get("legs", ()) for row in bounded],
                filters.get("since"), filters.get("since"),
                filters.get("until"), filters.get("until"),
            ],
            deadline_at,
        ).fetchall()
        return [
            {
                **{
                    key: value
                    for key, value in dict(row).items()
                    if key not in {"anchor_tier", "exact_question"}
                },
                "leg": "exact-answer" if row["exact_question"] else "answer",
                "tier": max(
                    4 if row["exact_question"] else 2,
                    int(row["anchor_tier"]),
                ),
            }
            for row in rows
        ]

    def search(
        self,
        query: str,
        filters: dict | None = None,
        limit: int = 10,
        authorized_source: str | list[str] | tuple[str, ...] | None = None,
    ) -> dict:
        if not isinstance(query, str) or not query.strip():
            raise ValueError("query is required")
        if len(query) > 8192:
            raise ValueError("query is too large")
        if not isinstance(limit, int) or not 1 <= limit <= 20:
            raise ValueError("limit must be between 1 and 20")
        filters = filters or {}
        if not isinstance(filters, dict):
            raise ValueError("filters must be an object")
        routing = {
            "requested_source_id": filters.get("source_id"),
            "requested_source_family": filters.get("source_family"),
            "requested_source_alias": filters.get("source_alias"),
            "authorized_source_scope": authorized_source is not None,
        }
        engine = legacy_engine()
        informative = engine.informative_terms(query)
        if not informative:
            return {
                "results": [], "abstention_reason": "no informative lexical terms",
                "diagnostics": {
                    "deadline_ms": self.search_deadline_ms, "elapsed_ms": 0.0,
                    "deadline_exceeded": False, "legs": [], "routing": routing,
                },
            }
        started = time.monotonic()
        identifiers = sorted(
            engine.identifier_terms(informative),
            key=lambda value: (
                bool(re.fullmatch(r"[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}", value)),
                bool(re.fullmatch(r"[0-9a-f]{8,}", value)), len(value),
            ),
            reverse=True,
        )
        plan = None
        semantic_vectors: list[list[float]] = []
        semantic_error = None
        planner_error = None
        planner_elapsed_ms = 0.0
        if self.semantic_runtime is not None and not identifiers:
            try:
                semantic_vectors = self.semantic_runtime.embed_queries([query])
            except Exception as exc:
                semantic_error = type(exc).__name__
        candidates: dict[int, dict] = {}
        database_started = time.monotonic()
        deadline_at = database_started + self.search_deadline_ms / 1000
        leg_timings: list[dict[str, Any]] = []
        deadline_exceeded = False
        rescue_deadline_at: float | None = None
        rescue_exhausted = False
        exact_question_count = 0
        dense_anchor_keys: list[tuple[str, str]] = []
        exact_rescue_scores: dict[tuple[str, str], tuple[int, int, float]] = {}

        def merge(rows: list[dict]) -> None:
            for position, row in enumerate(rows, 1):
                contribution = 1.0 / (60 + position)
                leg = row.pop("leg")
                if leg == "exact-answer":
                    observable_legs = {leg, "answer"}
                elif leg == "turn-semantic":
                    observable_legs = {leg, "semantic", "answer"}
                else:
                    observable_legs = {leg}
                existing = candidates.get(row["id"])
                if existing is None:
                    row["legs"] = observable_legs
                    row["leg_contributions"] = {leg: contribution}
                    row["fusion_score"] = contribution
                    candidates[row["id"]] = row
                else:
                    existing["legs"].update(observable_legs)
                    existing["leg_contributions"][leg] = max(
                        contribution,
                        existing["leg_contributions"].get(leg, 0.0),
                    )
                    existing["fusion_score"] = sum(existing["leg_contributions"].values())
                    if (row["tier"], float(row["lexical_rank"])) > (existing["tier"], float(existing["lexical_rank"])):
                        legs = existing["legs"]
                        leg_contributions = existing["leg_contributions"]
                        fusion_score = existing["fusion_score"]
                        row["legs"] = legs
                        row["leg_contributions"] = leg_contributions
                        row["fusion_score"] = fusion_score
                        candidates[row["id"]] = row

        def run_leg(name: str, operation, *, optional: bool = False) -> list[dict]:
            nonlocal deadline_exceeded, rescue_exhausted
            leg_started = time.monotonic()
            try:
                rows = operation()
                leg_timings.append({
                    "leg": name, "elapsed_ms": round((time.monotonic() - leg_started) * 1000, 3),
                    "n_results": len(rows), "timed_out": False,
                })
                return rows
            except SearchDeadlineExceeded:
                leg_timings.append({
                    "leg": name, "elapsed_ms": round((time.monotonic() - leg_started) * 1000, 3),
                    "n_results": 0, "timed_out": True,
                })
                if optional:
                    conn.rollback()
                    rescue_exhausted = True
                    return []
                deadline_exceeded = True
                raise

        try:
            with self.connect() as conn:
                routed_source_ids = self._resolve_routed_source_ids(conn, filters)
                exact_question_rows = run_leg(
                    "exact-question",
                    lambda: self._exact_question_leg(
                        conn,
                        query,
                        filters,
                        authorized_source=authorized_source,
                        routed_source_ids=routed_source_ids,
                        deadline_at=deadline_at,
                    ),
                )
                exact_question_count = len(exact_question_rows)
                merge(exact_question_rows)
                if identifiers:
                    merge(run_leg("entity", lambda: self._entity_leg(
                        conn, identifiers, filters, authorized_source=authorized_source,
                        routed_source_ids=routed_source_ids,
                        deadline_at=deadline_at, tier=3,
                    )))
                    for identifier in identifiers[:3]:
                        exact_rows = run_leg("identifier", lambda identifier=identifier: self._lexical_leg(
                            conn, identifier, "plainto_tsquery", filters, "identifier", 3,
                            exact=identifier, limit=100, authorized_source=authorized_source, deadline_at=deadline_at,
                            routed_source_ids=routed_source_ids,
                        ))
                        merge(exact_rows)
                        if exact_rows:
                            break
                else:
                    phrase_spec = phrase_query_spec(preferred_phrase_probes(engine.phrase_queries(query)))
                    # Dense retrieval is the primary non-exact natural-language
                    # leg. Run it before broad phrase scans and optional rewrites.
                    # Those rescues run lazily only when dense evidence cannot
                    # fill the requested session anchors.
                    for vector_index, semantic_vector in enumerate(semantic_vectors):
                        turn_rows = run_leg(
                            f"turn-semantic-{vector_index}",
                            lambda semantic_vector=semantic_vector:
                                self._turn_semantic_leg(
                                    conn,
                                    semantic_vector,
                                    filters,
                                    authorized_source=authorized_source,
                                    routed_source_ids=routed_source_ids,
                                    deadline_at=deadline_at,
                                    limit=semantic_candidate_limit(limit),
                                    minimum_similarity=
                                        self.semantic_minimum_similarity,
                                ),
                        )
                        merge(turn_rows)
                        semantic_rows = run_leg(f"semantic-{vector_index}", lambda semantic_vector=semantic_vector: self._semantic_leg(
                            conn, semantic_vector, filters, authorized_source=authorized_source,
                            routed_source_ids=routed_source_ids, deadline_at=deadline_at,
                            limit=semantic_candidate_limit(limit),
                            minimum_similarity=self.semantic_minimum_similarity,
                        ))
                        if vector_index == 0:
                            for semantic_row in [*turn_rows, *semantic_rows]:
                                anchor = (
                                    semantic_row["source_id"],
                                    semantic_row["session_native_id"],
                                )
                                if anchor not in dense_anchor_keys:
                                    dense_anchor_keys.append(anchor)
                                if len(dense_anchor_keys) >= max(1, limit - 1):
                                    break
                        merge(semantic_rows)
                    rescue_deadline_at = optional_rescue_deadline(
                        overall_deadline_at=deadline_at,
                        now=time.monotonic(),
                        search_deadline_ms=self.search_deadline_ms,
                    )
                    if (
                        phrase_spec and len(informative) >= 2
                        and should_run_optional_rescue(
                            exact_question_count=exact_question_count,
                            rows=candidates.values(),
                            result_limit=limit,
                        )
                    ):
                        phrase_query, phrase_function = phrase_spec
                        merge(run_leg("phrase", lambda: self._lexical_leg(
                            conn, phrase_query, phrase_function, filters, "phrase", 3,
                            limit=100, authorized_source=authorized_source,
                            deadline_at=rescue_deadline_at,
                            routed_source_ids=routed_source_ids,
                        ), optional=True))
                    if (
                        self.semantic_runtime is not None
                        and not rescue_exhausted
                        and should_run_optional_rescue(
                            exact_question_count=exact_question_count,
                            rows=candidates.values(),
                            result_limit=limit,
                        )
                        and len(informative) > 1
                        and redact_text(query) == query
                    ):
                        # Release the read snapshot before optional model I/O.
                        # A slow planner must never hold a database transaction.
                        conn.commit()
                        planner_started = time.monotonic()
                        try:
                            plan = self.semantic_runtime.plan(query)
                        except Exception as exc:
                            planner_error = type(exc).__name__
                        planner_elapsed_ms = round(
                            (time.monotonic() - planner_started) * 1000, 3,
                        )
                        # The deadline is a database-work budget. An approved
                        # optional planner wait must not consume the remaining
                        # bounded SQL time for the rescue it produces.
                        deadline_at += planner_elapsed_ms / 1000
                        rescue_deadline_at += planner_elapsed_ms / 1000
                    if (
                        plan is not None and plan.searchable and plan.phrases
                        and not rescue_exhausted
                        and should_run_optional_rescue(
                            exact_question_count=exact_question_count,
                            rows=candidates.values(),
                            result_limit=limit,
                        )
                    ):
                        # Rewrites may only add high-precision evidence. Broad OR
                        # rewrites let one generic model phrase reshuffle an
                        # otherwise stable dense baseline. Exact multiword probes
                        # rescue canonical labels without rewarding synonyms that
                        # are absent from the evidence.
                        for rewrite_index, rewrite_phrase in enumerate(plan.phrases):
                            if (
                                rescue_exhausted
                                or enough_session_anchors(
                                    candidates.values(), result_limit=limit,
                                )
                            ):
                                break
                            if len(re.findall(r"[A-Za-z0-9_./#-]+", rewrite_phrase)) < 2:
                                continue
                            rewrite_rows = run_leg(f"rewrite-{rewrite_index}", lambda rewrite_phrase=rewrite_phrase: self._lexical_leg(
                                conn, rewrite_phrase, "phraseto_tsquery", filters, "rewrite", 2,
                                exact=rewrite_phrase, limit=20, authorized_source=authorized_source,
                                deadline_at=rescue_deadline_at,
                                routed_source_ids=routed_source_ids,
                            ), optional=True)
                            for rewrite_row in rewrite_rows:
                                key = (rewrite_row["source_id"], rewrite_row["session_native_id"])
                                score = (
                                    -len(normalized_evidence(rewrite_row["text_redacted"])),
                                    -rewrite_index,
                                    float(rewrite_row["lexical_rank"]),
                                )
                                exact_rescue_scores[key] = max(score, exact_rescue_scores.get(key, score))
                            merge(rewrite_rows)
                    if (
                        phrase_spec and len(informative) == 1
                        and len(candidates) < limit and not rescue_exhausted
                        and exact_question_count == 0
                    ):
                        phrase_query, phrase_function = phrase_spec
                        merge(run_leg("phrase", lambda: self._lexical_leg(
                            conn, phrase_query, phrase_function, filters, "phrase", 3,
                            limit=100, authorized_source=authorized_source,
                            deadline_at=rescue_deadline_at,
                            routed_source_ids=routed_source_ids,
                        ), optional=True))
                    entity_values = [
                        part
                        for term in informative
                        for part in re.split(r"[./_-]+", term)
                        if re.search(r"(?:error|exception|timeout)$", part, re.I)
                    ]
                    if entity_values:
                        merge(run_leg("entity", lambda: self._entity_leg(
                            conn, entity_values, filters, authorized_source=authorized_source,
                            routed_source_ids=routed_source_ids,
                            deadline_at=deadline_at, tier=2,
                        )))
                if not identifiers and should_run_partial(candidate_count=len(candidates), result_limit=limit):
                    for probe, leg, tier in partial_lexical_probes(
                        informative,
                        has_time_filter=bool(filters.get("since") or filters.get("until")),
                    ):
                        merge(run_leg(leg, lambda probe=probe, leg=leg, tier=tier: self._lexical_leg(
                            conn, probe, "plainto_tsquery", filters, leg, tier,
                            limit=100, authorized_source=authorized_source, deadline_at=deadline_at,
                            routed_source_ids=routed_source_ids,
                        )))
                if len(candidates) < limit and not any(row["tier"] >= 3 for row in candidates.values()):
                    merge(run_leg("all", lambda: self._lexical_leg(
                        conn, " ".join(informative), "plainto_tsquery", filters, "all", 1,
                        authorized_source=authorized_source, deadline_at=deadline_at,
                        routed_source_ids=routed_source_ids,
                    )))
                if candidates:
                    merge(run_leg("answer", lambda: self._answer_leg(
                        conn, list(candidates.values()), filters, deadline_at=deadline_at,
                    )))
        except SearchDeadlineExceeded:
            pass

        now_datetime = datetime.now(timezone.utc)
        now = now_datetime.timestamp()
        families_by_evidence: dict[str, set[str]] = {}
        for row in candidates.values():
            if row["source_profiled"]:
                families_by_evidence.setdefault(
                    normalized_evidence(row["text_redacted"]), set(),
                ).add(row["source_family"])
        grouped: dict[tuple[str, str], tuple[tuple[float, ...], dict]] = {}
        for row in candidates.values():
            matched = [term for term in informative if term.casefold() in row["text_redacted"].casefold()]
            if row["tier"] == 0 and len([term for term in matched if len(term) >= 5]) < 2:
                continue
            occurred = row["occurred_at"] or row["started_at"]
            epoch = occurred.timestamp() if occurred else now
            recency_factor = freshness_score(
                occurred or datetime.fromtimestamp(epoch, timezone.utc),
                now=now_datetime,
                half_life_days=row["freshness_half_life_days"],
            )
            corroborating_families = max(
                1, len(families_by_evidence.get(normalized_evidence(row["text_redacted"]), set())),
            )
            evidence = evidence_rank_components(
                legs=row["legs"], surface=row["surface"], lexical_rank=float(row["lexical_rank"]),
                matched_count=len(matched), informative_count=len(informative),
                has_identifier=bool(identifiers), recency_factor=recency_factor,
                quality=row["source_quality"],
                corroborating_families=corroborating_families,
                fusion_score=float(row.get("fusion_score", 0.0)),
            )
            key = (row["source_id"], row["session_native_id"])
            rank_key = tuple(evidence["rank_key"])
            if key not in grouped or rank_key > grouped[key][0]:
                grouped[key] = (rank_key, {**row, "matched_terms": matched, "evidence": evidence})
        ranked_all = sorted(grouped.items(), key=lambda value: value[1][0], reverse=True)
        if dense_anchor_keys:
            # Preserve the deterministic local dense baseline and reserve one
            # result slot for the best complementary lexical/planner evidence.
            selected_keys = [key for key in dense_anchor_keys if key in grouped][:max(1, limit - 1)]
            for key in sorted(exact_rescue_scores, key=lambda value: (exact_rescue_scores[value], value), reverse=True):
                if key in grouped and key not in selected_keys:
                    selected_keys.append(key)
                    break
            for key, _value in ranked_all:
                if len(selected_keys) >= limit:
                    break
                if key not in selected_keys:
                    selected_keys.append(key)
            ranked = sorted(
                (grouped[key] for key in selected_keys),
                key=lambda value: value[0],
                reverse=True,
            )
        else:
            ranked = [value for _key, value in ranked_all[:limit]]
        results = []
        for _rank, row in ranked:
            metadata = row["metadata"] or {}
            path = row["path"] or metadata.get("original_path") or f"recall://{row['source_id']}/{row['session_native_id']}"
            cwd = metadata.get("cwd")
            text, text_truncated = bounded_search_text(row["text_redacted"])
            results.append({
                "source_id": row["source_id"], "native_id": row["event_native_id"],
                "session_native_id": row["session_native_id"], "path": path,
                "occurred_at": row["occurred_at"], "observed_at": row["observed_at"],
                "cwd": cwd, "slot": metadata.get("slot") or (re.search(r"grep\d+", cwd or "").group(0) if re.search(r"grep\d+", cwd or "") else None),
                "branch": metadata.get("branch"), "harness": metadata.get("harness"),
                "surface": row["surface"], "text": text,
                "text_truncated": text_truncated,
                "receipt": row["receipt"], "matched_terms": row["matched_terms"],
                "legs": sorted(row["legs"]), "tier": row["evidence"]["class_priority"],
                "evidence": row["evidence"],
                "source_profile": {
                    "profiled": row["source_profiled"],
                    "family": row["source_family"],
                    "quality": row["source_quality"],
                },
                "projector_version": row["projector_version"],
            })
        diagnostics = {
            "deadline_ms": self.search_deadline_ms,
            "elapsed_ms": round((time.monotonic() - started) * 1000, 3),
            "database_elapsed_ms": round(max(
                0.0,
                (time.monotonic() - database_started) * 1000 - planner_elapsed_ms,
            ), 3),
            "deadline_exceeded": deadline_exceeded,
            "legs": leg_timings,
            "routing": routing,
            "semantic": {
                "configured": self.semantic_runtime is not None,
                "planner_used": plan is not None,
                "planner_error_type": planner_error,
                "searchable": None if plan is None else plan.searchable,
                "phrase_count": 0 if plan is None else len(plan.phrases),
                "planner_elapsed_ms": planner_elapsed_ms,
                "error_type": semantic_error,
            },
        }
        return {
            "results": results,
            "abstention_reason": None if results else ("search deadline exceeded" if deadline_exceeded else "insufficient lexical evidence"),
            "diagnostics": diagnostics,
        }

    def show(
        self,
        target: str,
        *,
        around: str | None = None,
        tail: int = 0,
        prompts: bool = False,
        authorized_source: str | list[str] | tuple[str, ...] | None = None,
    ) -> dict | None:
        if not target:
            raise ValueError("target is required")
        if tail < 0 or tail > 1000:
            raise ValueError("tail must be between 0 and 1000")
        with self.connect() as conn:
            if isinstance(authorized_source, str):
                source_scope_sql = "event.source_id=%s"
                source_scope_params: list[Any] = [authorized_source]
            elif authorized_source is not None:
                source_scope_sql = "event.source_id=ANY(%s)"
                source_scope_params = [list(authorized_source)]
            else:
                source_scope_sql = "TRUE"
                source_scope_params = []
            if target.startswith("recall://"):
                event_part = target.split("#", 1)[0]
                try:
                    base, revision = event_part.rsplit("?rev=", 1)
                    source_id, native_id = base.removeprefix("recall://").split("/", 1)
                    int(revision)
                except (ValueError, TypeError):
                    raise ValueError("invalid receipt") from None
                identity = conn.execute(
                    f"""SELECT event.source_id,
                              COALESCE(item.session_native_id,event.native_parent_id,event.native_id) AS session_native_id
                       FROM source_events event LEFT JOIN items item ON item.event_id=event.id
                       WHERE event.source_id=%s AND event.native_id=%s AND event.revision=%s
                         AND {source_scope_sql}
                       ORDER BY item.id LIMIT 1""",
                    [source_id, native_id, int(revision), *source_scope_params],
                ).fetchone()
            else:
                identity = conn.execute(
                    f"""SELECT event.source_id,
                              COALESCE(item.session_native_id,event.native_parent_id,event.native_id) AS session_native_id
                       FROM source_events event LEFT JOIN items item ON item.event_id=event.id
                       WHERE event.envelope #>> '{{provenance,original_path}}'=%s
                         AND {source_scope_sql}
                       ORDER BY event.id DESC,item.id LIMIT 1""",
                    [target, *source_scope_params],
                ).fetchone()
            if not identity:
                return None
            where = "source_id=%s AND session_native_id=%s AND deleted_at IS NULL"
            values: list[Any] = [identity["source_id"], identity["session_native_id"]]
            if prompts:
                where += " AND surface='user'"
            if around:
                point = datetime.fromisoformat(around.replace("Z", "+00:00"))
                before = conn.execute(
                    f"SELECT occurred_at,surface,text_redacted AS text,receipt FROM items WHERE {where} AND occurred_at<=%s ORDER BY occurred_at DESC,id DESC LIMIT 4",
                    [*values, point],
                ).fetchall()
                after = conn.execute(
                    f"SELECT occurred_at,surface,text_redacted AS text,receipt FROM items WHERE {where} AND occurred_at>%s ORDER BY occurred_at,id LIMIT 3",
                    [*values, point],
                ).fetchall()
                rows = list(reversed(before)) + list(after)
            elif tail:
                rows = list(reversed(conn.execute(
                    f"SELECT occurred_at,surface,text_redacted AS text,receipt FROM items WHERE {where} ORDER BY occurred_at DESC NULLS LAST,id DESC LIMIT %s",
                    [*values, tail],
                ).fetchall()))
            else:
                rows = conn.execute(
                    f"SELECT occurred_at,surface,text_redacted AS text,receipt FROM items WHERE {where} ORDER BY occurred_at,id LIMIT 1000",
                    values,
                ).fetchall()
            return {"chunks": [dict(row) for row in rows], "truncated": not around and not tail and len(rows) == 1000}

    @staticmethod
    def _session_evidence_id(source_id: str, session_id: str, event_native_id: str,
                             ordinal: int, text: str) -> tuple[str, str]:
        text_sha = hashlib.sha256(text.encode()).hexdigest()
        identity = f"{source_id}\0{session_id}\0{event_native_id}\0{ordinal}\0{text_sha}"
        return "rse_" + hashlib.sha256(identity.encode()).hexdigest(), text_sha

    def session_export(self, *, target: str | None, cursor: str | None = None,
                       limit: int = 1000, authorized_source: str | None = None) -> dict | None:
        """Return one immutable, authorization-scoped page of an exact session snapshot."""
        if not 1 <= limit <= 1000:
            raise ValueError("limit must be between 1 and 1000")
        if bool(target) == bool(cursor):
            raise ValueError("provide exactly one of target or cursor")
        with self.connect() as conn:
            conn.execute("DELETE FROM session_export_cursors WHERE expires_at <= now()")
            if cursor:
                if not re.fullmatch(r"rsc_[A-Za-z0-9_-]{32,128}", cursor):
                    raise ValueError("invalid session export cursor")
                cursor_sha = hashlib.sha256(cursor.encode()).hexdigest()
                state = conn.execute(
                    """SELECT * FROM session_export_cursors
                       WHERE token_sha256=%s AND expires_at>now()
                         AND (%s::text IS NULL OR source_id=%s)""",
                    (cursor_sha, authorized_source, authorized_source),
                ).fetchone()
                if not state:
                    return None
                source_id = state["source_id"]
                session_id = state["session_native_id"]
                snapshot_max = state["snapshot_max_item_id"]
                snapshot_at = state["snapshot_at"]
                after_native = state["after_event_native_id"]
                after_ordinal = state["after_ordinal"]
                after_item_id = state["after_item_id"]
                after_sequence = state["after_sequence"]
            else:
                if target is None:
                    raise ValueError("session export target is required")
                if target.startswith("recall://"):
                    event_part = target.split("#", 1)[0]
                    try:
                        base, revision = event_part.rsplit("?rev=", 1)
                        source_id, native_id = base.removeprefix("recall://").split("/", 1)
                        int(revision)
                    except (ValueError, TypeError):
                        raise ValueError("invalid receipt") from None
                    identity = conn.execute(
                        """SELECT event.source_id,
                                  COALESCE(item.session_native_id,event.native_parent_id,event.native_id) AS session_native_id
                           FROM source_events event LEFT JOIN items item ON item.event_id=event.id
                           WHERE event.source_id=%s AND event.native_id=%s AND event.revision=%s
                             AND (%s::text IS NULL OR event.source_id=%s)
                             AND event.kind!='tombstone'
                             AND NOT EXISTS (
                               SELECT 1 FROM source_events later
                               WHERE later.source_id=event.source_id
                                 AND later.native_id=event.native_id
                                 AND later.revision>event.revision
                                 AND later.is_tombstone)""",
                        (source_id, native_id, int(revision), authorized_source, authorized_source),
                    ).fetchone()
                else:
                    identity = conn.execute(
                        """SELECT event.source_id,
                                  COALESCE(item.session_native_id,event.native_parent_id,event.native_id) AS session_native_id
                           FROM source_events event LEFT JOIN items item ON item.event_id=event.id
                           WHERE event.envelope #>> '{provenance,original_path}'=%s
                             AND (%s::text IS NULL OR event.source_id=%s)
                             AND event.kind!='tombstone'
                             AND NOT EXISTS (
                               SELECT 1 FROM source_events later
                               WHERE later.source_id=event.source_id
                                 AND later.native_id=event.native_id
                                 AND later.revision>event.revision
                                 AND later.is_tombstone)
                           ORDER BY event.id DESC,item.id LIMIT 1""",
                        (target, authorized_source, authorized_source),
                    ).fetchone()
                if not identity:
                    return None
                source_id = identity["source_id"]
                session_id = identity["session_native_id"]
                snapshot = conn.execute(
                    """SELECT COALESCE(max(id),0) AS max_id,now() AS snapshot_at FROM items
                       WHERE source_id=%s AND session_native_id=%s""",
                    (source_id, session_id),
                ).fetchone()
                snapshot_max = snapshot["max_id"]
                snapshot_at = snapshot["snapshot_at"]
                after_native, after_ordinal, after_item_id, after_sequence = "", -1, 0, 0

            session = conn.execute(
                """SELECT source_id,native_id,harness,started_at,ended_at,metadata,projector_version
                   FROM sessions WHERE source_id=%s AND native_id=%s
                     AND (%s::text IS NULL OR source_id=%s)""",
                (source_id, session_id, authorized_source, authorized_source),
            ).fetchone()
            if not session:
                return None
            rows = conn.execute(
                """SELECT i.id,i.event_native_id,i.ordinal,i.occurred_at,i.role,i.surface,
                          i.text_redacted,i.receipt,i.projector_version
                   FROM items i
                   WHERE i.source_id=%s AND i.session_native_id=%s AND i.id<=%s
                     AND (i.deleted_at IS NULL OR i.deleted_at>%s)
                     AND (i.event_native_id,i.ordinal,i.id)>(%s,%s,%s)
                     AND NOT EXISTS (
                       SELECT 1 FROM items newer
                       WHERE newer.source_id=i.source_id
                         AND newer.session_native_id=i.session_native_id
                         AND newer.event_native_id=i.event_native_id
                         AND newer.ordinal=i.ordinal
                         AND newer.id>i.id AND newer.id<=%s
                         AND (newer.deleted_at IS NULL OR newer.deleted_at>%s)
                     )
                   ORDER BY i.event_native_id,i.ordinal,i.id LIMIT %s""",
                (source_id, session_id, snapshot_max, snapshot_at,
                 after_native, after_ordinal, after_item_id, snapshot_max, snapshot_at, limit + 1),
            ).fetchall()
            has_more = len(rows) > limit
            page_rows = rows[:limit]
            entities_by_item: dict[int, list[dict[str, str]]] = {}
            if page_rows:
                entity_rows = conn.execute(
                    """SELECT item_id,kind,value FROM entities
                       WHERE item_id=ANY(%s) ORDER BY item_id,kind,value""",
                    ([row["id"] for row in page_rows],),
                ).fetchall()
                for entity in entity_rows:
                    entities_by_item.setdefault(entity["item_id"], []).append({
                        "kind": entity["kind"], "value": entity["value"],
                    })
            items = []
            for index, row in enumerate(page_rows, after_sequence):
                evidence_id, text_sha = self._session_evidence_id(
                    source_id, session_id, row["event_native_id"], row["ordinal"], row["text_redacted"],
                )
                item = {
                    "sequence": index,
                    "evidence_id": evidence_id,
                    "event_native_id": row["event_native_id"],
                    "item_ordinal": row["ordinal"],
                    "occurred_at": row["occurred_at"],
                    "role": row["role"],
                    "surface": row["surface"],
                    "text": row["text_redacted"],
                    "text_sha256": text_sha,
                    "receipt": row["receipt"],
                    "projector_version": row["projector_version"],
                }
                if entities_by_item.get(row["id"]):
                    item["entities"] = entities_by_item[row["id"]]
                if row["surface"] in {"tool_input", "tool_output"} and len(row["text_redacted"]) >= (
                    2048 if row["surface"] == "tool_input" else 4096
                ):
                    item["possibly_truncated"] = True
                items.append(item)
            next_cursor = None
            if has_more:
                last = page_rows[-1]
                next_cursor = "rsc_" + secrets.token_urlsafe(32)
                conn.execute(
                    """INSERT INTO session_export_cursors(
                         token_sha256,source_id,session_native_id,snapshot_max_item_id,snapshot_at,
                         after_event_native_id,after_ordinal,after_item_id,after_sequence,expires_at)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,now()+interval '1 hour')""",
                    (hashlib.sha256(next_cursor.encode()).hexdigest(), source_id, session_id,
                     snapshot_max, snapshot_at, last["event_native_id"], last["ordinal"], last["id"],
                     after_sequence + len(page_rows)),
                )
            metadata = session["metadata"] or {}
            current_snapshot = conn.execute(
                """SELECT COALESCE(max(id),0) AS max_id FROM items
                   WHERE source_id=%s AND session_native_id=%s""",
                (source_id, session_id),
            ).fetchone()
            source_snapshot_stable = current_snapshot["max_id"] == snapshot_max
            boundary = hashlib.sha256(f"{source_id}\0{session_id}\0{snapshot_max}".encode()).hexdigest()
            page_receipt = hashlib.sha256(
                "\n".join(item["evidence_id"] for item in items).encode()
            ).hexdigest()
            return {
                "schema_version": "recall.session-export.v1",
                "session": {
                    "source_id": source_id,
                    "native_session_id": session_id,
                    "harness": session["harness"],
                    "started_at": session["started_at"],
                    "ended_at": session["ended_at"],
                    "metadata": metadata,
                    "projector_version": session["projector_version"],
                    "privacy_policy_version": metadata.get("privacy_policy_version", "unknown"),
                    "boundary_receipt": boundary,
                    "children_included": False,
                    "source_snapshot_stable": source_snapshot_stable,
                },
                "items": items,
                "page": {
                    "count": len(items),
                    "complete": not has_more,
                    "next_cursor": next_cursor,
                    "page_receipt": page_receipt,
                    "snapshot_at": snapshot_at,
                    "snapshot_max_item_id": snapshot_max,
                },
            }

    def related(
        self,
        *,
        cwd: str | None,
        branch: str | None,
        limit: int = 10,
        mains_only: bool = False,
        fast: bool = False,
        authorized_source: str | list[str] | tuple[str, ...] | None = None,
    ) -> dict:
        if not 1 <= limit <= 20:
            raise ValueError("limit must be between 1 and 20")
        if not cwd and not branch:
            return {"results": []}
        clauses, score_parts, params = [], [], []
        if cwd:
            clauses.append("COALESCE(s.metadata->>'cwd','') ILIKE %s"); params.append("%" + cwd + "%")
            score_parts.append("(COALESCE(s.metadata->>'cwd','') ILIKE %s)::int")
        if branch:
            clauses.append("COALESCE(s.metadata->>'branch','') ILIKE %s"); params.append("%" + branch + "%")
            score_parts.append("(COALESCE(s.metadata->>'branch','') ILIKE %s)::int")
        score_params = list(params)
        if isinstance(authorized_source, str):
            source_scope_sql = "s.source_id=%s"
            source_scope_params: list[Any] = [authorized_source]
        elif authorized_source is not None:
            source_scope_sql = "s.source_id=ANY(%s)"
            source_scope_params = [list(authorized_source)]
        else:
            source_scope_sql = "TRUE"
            source_scope_params = []
        candidate_limit_sql = "LIMIT %s" if fast else ""
        candidate_limit_params = (
            [related_candidate_limit(limit)] if fast else []
        )
        with self.connect() as conn:
            rows = conn.execute(
                f"""WITH candidates AS MATERIALIZED (
                      SELECT s.source_id,s.native_id,s.metadata,s.ended_at,
                             ({' + '.join(score_parts)}) AS overlap
                      FROM sessions s
                      WHERE ({' OR '.join(clauses)})
                        AND {source_scope_sql}
                      ORDER BY overlap DESC,s.ended_at DESC NULLS LAST
                      {candidate_limit_sql}
                    )
                    SELECT candidate.source_id,candidate.native_id,
                           candidate.metadata,candidate.ended_at,
                           evidence.path,evidence.receipt,candidate.overlap
                    FROM candidates candidate
                    JOIN LATERAL (
                      SELECT i.receipt,
                             event.envelope #>> '{{provenance,original_path}}' AS path
                      FROM items i
                      JOIN source_events event ON event.id=i.event_id
                      WHERE i.source_id=candidate.source_id
                        AND i.session_native_id=candidate.native_id
                        AND i.deleted_at IS NULL
                        AND event.envelope #>> '{{provenance,original_path}}' IS NOT NULL
                      ORDER BY i.occurred_at DESC NULLS LAST,i.id DESC LIMIT 1
                    ) evidence ON true
                    {"WHERE evidence.path NOT LIKE '%%/subagents/%%'" if mains_only else ''}
                    ORDER BY candidate.overlap DESC,
                             candidate.ended_at DESC NULLS LAST
                    LIMIT %s""",
                [
                    *score_params,
                    *params,
                    *source_scope_params,
                    *candidate_limit_params,
                    limit,
                ],
            ).fetchall()
        return {"results": [{
            "source_id": row["source_id"], "session_native_id": row["native_id"],
            "path": row["path"], "cwd": (row["metadata"] or {}).get("cwd"),
            "branch": (row["metadata"] or {}).get("branch"), "overlap": row["overlap"],
            "receipt": row["receipt"],
        } for row in rows]}

    def doctor(self, authorized_source: str | None = None) -> dict:
        result = self.service_metrics()
        with self.connect() as conn:
            if authorized_source:
                result = {
                    "source_events": conn.execute("SELECT count(*) AS n FROM source_events WHERE source_id=%s", (authorized_source,)).fetchone()["n"],
                    "dead_letters": conn.execute("SELECT count(*) AS n FROM dead_letters WHERE source_id=%s", (authorized_source,)).fetchone()["n"],
                    "projection_lag": result["projection_lag"],
                    "source_freshness_seconds": conn.execute("SELECT COALESCE(GREATEST(0,extract(epoch FROM now()-max(created_at)))::bigint,0) AS n FROM source_events WHERE source_id=%s", (authorized_source,)).fetchone()["n"],
                }
            result.update({
                "sources": 1 if authorized_source else conn.execute("SELECT count(*) AS n FROM sources").fetchone()["n"],
                "sessions": conn.execute("SELECT count(*) AS n FROM sessions WHERE (%s::text IS NULL OR source_id=%s)", (authorized_source, authorized_source)).fetchone()["n"],
                "live_items": conn.execute("SELECT count(*) AS n FROM items WHERE deleted_at IS NULL AND (%s::text IS NULL OR source_id=%s)", (authorized_source, authorized_source)).fetchone()["n"],
            })
        return {"status": "ok", **result}

    def rebuild(self) -> dict:
        with self.connect() as conn:
            with conn.transaction():
                before = conn.execute("SELECT count(*) AS n FROM items WHERE deleted_at IS NULL").fetchone()["n"]
                entities_before = conn.execute("SELECT count(*) AS n FROM entities").fetchone()["n"]
                conn.execute(
                    "TRUNCATE turn_embedding_items,turn_embeddings,"
                    "turn_embedding_projection_watermarks,"
                    "turn_embedding_dirty_sessions,item_embeddings,"
                    "embedding_projection_watermarks,entities,chunks,items,"
                    "sessions,projection_watermarks RESTART IDENTITY"
                )
                rows = conn.execute("SELECT id,envelope,revision FROM source_events ORDER BY id").fetchall()
                self._project_batch(
                    conn,
                    (
                        (row["id"], row["envelope"], row["revision"])
                        for row in rows
                    ),
                )
                after = conn.execute("SELECT count(*) AS n FROM items WHERE deleted_at IS NULL").fetchone()["n"]
                entities_after = conn.execute("SELECT count(*) AS n FROM entities").fetchone()["n"]
                return {
                    "events": len(rows), "items_before": before, "items_after": after,
                    "entities_before": entities_before, "entities_after": entities_after,
                }

    def backfill_cowork_sessions(
        self, batch_size: int = 5000, max_batches: int | None = None,
    ) -> dict:
        """Repair legacy Cowork derived session identity without mutating source events."""
        if not 1 <= batch_size <= 20000:
            raise ValueError("batch size must be between 1 and 20000")
        if max_batches is not None and max_batches < 1:
            raise ValueError("max batches must be positive")
        name = "cowork-session-v1"
        batches = scanned = moved_events = moved_items = removed_sessions = 0
        with self.connect() as conn:
            conn.autocommit = True
            conn.execute("SELECT pg_advisory_lock(hashtextextended('recall:cowork-session-backfill-v1',0))")
            try:
                while max_batches is None or batches < max_batches:
                    with conn.transaction():
                        state = conn.execute(
                            """SELECT target_item_id,last_item_id,completed_at
                               FROM projection_backfills WHERE name=%s FOR UPDATE""",
                            (name,),
                        ).fetchone()
                        if state is None:
                            target = conn.execute(
                                "SELECT COALESCE(max(id),0) AS n FROM source_events"
                            ).fetchone()["n"]
                            conn.execute(
                                """INSERT INTO projection_backfills(name,target_item_id,last_item_id)
                                   VALUES (%s,%s,0)""",
                                (name, target),
                            )
                            state = {"target_item_id": target, "last_item_id": 0, "completed_at": None}
                        if state["completed_at"] is not None:
                            break
                        rows = conn.execute(
                            """SELECT id,source_id,native_id,envelope
                               FROM source_events
                               WHERE id>%s AND id<=%s
                                 AND envelope #>> '{provenance,connector_id}'='anthropic.cowork-local'
                                 AND envelope #>> '{content,session_id}' IS NOT NULL
                               ORDER BY id LIMIT %s""",
                            (state["last_item_id"], state["target_item_id"], batch_size),
                        ).fetchall()
                        for row in rows:
                            new_session = effective_session_id(row["envelope"])
                            current = conn.execute(
                                """SELECT session_native_id,count(*) AS n FROM items
                                   WHERE event_id=%s GROUP BY session_native_id""",
                                (row["id"],),
                            ).fetchall()
                            if not current or all(value["session_native_id"] == new_session for value in current):
                                continue
                            old_session = current[0]["session_native_id"]
                            session = conn.execute(
                                """SELECT principal_id,harness,started_at,ended_at,metadata,projector_version
                                   FROM sessions WHERE source_id=%s AND native_id=%s""",
                                (row["source_id"], old_session),
                            ).fetchone()
                            if session is None:
                                raise RuntimeError("cowork session projection is missing")
                            conn.execute(
                                """INSERT INTO sessions(source_id,native_id,principal_id,harness,started_at,ended_at,metadata,projector_version)
                                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                                   ON CONFLICT(source_id,native_id) DO UPDATE SET
                                     started_at=LEAST(sessions.started_at,excluded.started_at),
                                     ended_at=GREATEST(sessions.ended_at,excluded.ended_at),
                                     metadata=sessions.metadata || excluded.metadata,
                                     projector_version=GREATEST(sessions.projector_version,excluded.projector_version),
                                     rebuilt_at=now()""",
                                (
                                    row["source_id"], new_session, session["principal_id"], session["harness"],
                                    session["started_at"], session["ended_at"], json.dumps(session["metadata"]),
                                    session["projector_version"],
                                ),
                            )
                            changed = conn.execute(
                                """UPDATE items SET session_native_id=%s
                                   WHERE event_id=%s AND session_native_id<>%s""",
                                (new_session, row["id"], new_session),
                            ).rowcount
                            deleted = conn.execute(
                                """DELETE FROM sessions session WHERE source_id=%s AND native_id=%s
                                   AND NOT EXISTS (
                                     SELECT 1 FROM items item WHERE item.source_id=session.source_id
                                       AND item.session_native_id=session.native_id
                                   )""",
                                (row["source_id"], old_session),
                            ).rowcount
                            moved_events += 1
                            moved_items += changed
                            removed_sessions += deleted
                        batches += 1
                        scanned += len(rows)
                        completed = len(rows) < batch_size
                        last_event_id = state["target_item_id"] if completed else rows[-1]["id"]
                        conn.execute(
                            """UPDATE projection_backfills SET last_item_id=%s,
                               completed_at=CASE WHEN %s THEN now() ELSE NULL END,updated_at=now()
                               WHERE name=%s""",
                            (last_event_id, completed, name),
                        )
                        if completed:
                            break
                state = conn.execute(
                    """SELECT target_item_id,last_item_id,completed_at
                       FROM projection_backfills WHERE name=%s""",
                    (name,),
                ).fetchone()
            finally:
                conn.execute("SELECT pg_advisory_unlock(hashtextextended('recall:cowork-session-backfill-v1',0))")
        return {
            "batches": batches,
            "events_scanned": scanned,
            "events_moved": moved_events,
            "items_moved": moved_items,
            "sessions_removed": removed_sessions,
            "target_event_id": state["target_item_id"],
            "last_event_id": state["last_item_id"],
            "completed": state["completed_at"] is not None,
        }

    def backfill_entities(self, batch_size: int = 5000, max_batches: int | None = None) -> dict:
        """Resume an online canonical-event replay into the entity projection."""
        if not 1 <= batch_size <= 20000:
            raise ValueError("batch size must be between 1 and 20000")
        if max_batches is not None and max_batches < 1:
            raise ValueError("max batches must be positive")
        engine = legacy_engine()
        batches = scanned = inserted = 0
        with self.connect() as conn:
            conn.autocommit = True
            conn.execute("SELECT pg_advisory_lock(hashtextextended('recall:entity-backfill',0))")
            try:
                conn.execute(
                    """CREATE TEMP TABLE entity_backfill_stage (
                         item_id bigint,source_id text,kind text,value text,normalized text
                       ) ON COMMIT DELETE ROWS"""
                )
                while max_batches is None or batches < max_batches:
                    with conn.transaction():
                        state = conn.execute(
                            "SELECT target_item_id,last_item_id,completed_at FROM projection_backfills WHERE name='entities-v2' FOR UPDATE"
                        ).fetchone()
                        if state is None:
                            target = conn.execute("SELECT COALESCE(max(id),0) AS n FROM items").fetchone()["n"]
                            conn.execute(
                                "INSERT INTO projection_backfills(name,target_item_id,last_item_id) VALUES ('entities-v2',%s,0)",
                                (target,),
                            )
                            state = {"target_item_id": target, "last_item_id": 0, "completed_at": None}
                        if state["completed_at"] is not None:
                            break
                        rows = conn.execute(
                            """SELECT i.id,i.source_id,i.text_redacted FROM items i
                               WHERE i.id>%s AND i.id<=%s ORDER BY i.id LIMIT %s""",
                            (state["last_item_id"], state["target_item_id"], batch_size),
                        ).fetchall()
                        if not rows:
                            conn.execute(
                                "UPDATE projection_backfills SET completed_at=now(),updated_at=now() WHERE name='entities-v2'"
                            )
                            break
                        entity_rows = []
                        for row in rows:
                            entities = [
                                {"kind": kind, "value": value, "normalized": value.casefold()}
                                for kind, value in engine.extract_entities(row["text_redacted"])
                            ]
                            entity_rows.extend(
                                (row["id"], row["source_id"], entity["kind"], entity["value"], entity["normalized"])
                                for entity in entities
                            )
                        if entity_rows:
                            with conn.cursor() as cursor:
                                with cursor.copy(
                                    "COPY entity_backfill_stage(item_id,source_id,kind,value,normalized) FROM STDIN"
                                ) as copy:
                                    for entity_row in entity_rows:
                                        copy.write_row(entity_row)
                            conn.execute(
                                """INSERT INTO entities(item_id,source_id,kind,value,normalized)
                                   SELECT item_id,source_id,kind,value,normalized FROM entity_backfill_stage
                                   ON CONFLICT DO NOTHING"""
                            )
                        last_item_id = rows[-1]["id"]
                        completed = last_item_id >= state["target_item_id"]
                        conn.execute(
                            """UPDATE projection_backfills SET last_item_id=%s,
                               completed_at=CASE WHEN %s THEN now() ELSE NULL END,updated_at=now()
                               WHERE name='entities-v2'""",
                            (last_item_id, completed),
                        )
                        batches += 1
                        scanned += len(rows)
                        inserted += len(entity_rows)
                        if completed:
                            break
                state = conn.execute(
                    "SELECT target_item_id,last_item_id,completed_at FROM projection_backfills WHERE name='entities-v2'"
                ).fetchone()
            finally:
                try:
                    conn.execute("DROP TABLE IF EXISTS entity_backfill_stage")
                finally:
                    conn.execute("SELECT pg_advisory_unlock(hashtextextended('recall:entity-backfill',0))")
        return {
            "batches": batches, "items_scanned": scanned, "entity_rows_attempted": inserted,
            "target_item_id": state["target_item_id"], "last_item_id": state["last_item_id"],
            "completed": state["completed_at"] is not None,
        }

    def backfill_redaction(
        self,
        batch_size: int = 5000,
        max_batches: int | None = None,
        workers: int = 1,
    ) -> dict:
        """Converge derived text on the current privacy projector without rewriting evidence."""
        if not 1 <= batch_size <= 20000:
            raise ValueError("batch size must be between 1 and 20000")
        if max_batches is not None and max_batches < 1:
            raise ValueError("max batches must be positive")
        if not 1 <= workers <= 32:
            raise ValueError("workers must be between 1 and 32")
        engine = legacy_engine()
        batches = scanned = rewritten = rewritten_chunks = rebuilt_entity_items = 0
        executor = ProcessPoolExecutor(max_workers=workers) if workers > 1 else None

        def redact_batch(values: list[str]) -> list[str]:
            if executor is None:
                return [redact_text(value) for value in values]
            return list(executor.map(redact_text, values, chunksize=64))

        with self.connect() as conn:
            conn.autocommit = True
            conn.execute("SELECT pg_advisory_lock(hashtextextended('recall:redaction-backfill-v3',0))")
            try:
                while max_batches is None or batches < max_batches:
                    with conn.transaction():
                        state = conn.execute(
                            """SELECT target_item_id,last_item_id,completed_at
                               FROM projection_backfills WHERE name='redaction-v3' FOR UPDATE"""
                        ).fetchone()
                        if state is None:
                            target = conn.execute(
                                "SELECT COALESCE(max(id),0) AS n FROM items"
                            ).fetchone()["n"]
                            conn.execute(
                                """INSERT INTO projection_backfills(name,target_item_id,last_item_id)
                                   VALUES ('redaction-v3',%s,0)""",
                                (target,),
                            )
                            state = {
                                "target_item_id": target,
                                "last_item_id": 0,
                                "completed_at": None,
                            }
                        if state["completed_at"] is not None:
                            break
                        rows = conn.execute(
                            """SELECT id,source_id,text_redacted FROM items
                               WHERE id>%s AND id<=%s ORDER BY id LIMIT %s""",
                            (state["last_item_id"], state["target_item_id"], batch_size),
                        ).fetchall()
                        if not rows:
                            conn.execute(
                                """UPDATE projection_backfills SET completed_at=now(),updated_at=now()
                                   WHERE name='redaction-v3'"""
                            )
                            conn.execute(
                                "UPDATE sessions SET projector_version=%s WHERE projector_version<%s",
                                (PROJECTOR_VERSION, PROJECTOR_VERSION),
                            )
                            conn.execute(
                                """UPDATE projection_watermarks SET version=%s,updated_at=now()
                                   WHERE projector='items' AND version<%s""",
                                (PROJECTOR_VERSION, PROJECTOR_VERSION),
                            )
                            break

                        row_ids = [row["id"] for row in rows]
                        rows_by_id = {row["id"]: row for row in rows}
                        safe_items = dict(zip(
                            row_ids,
                            redact_batch([row["text_redacted"] for row in rows]),
                            strict=True,
                        ))
                        changed_items = {
                            row_id for row_id, safe in safe_items.items()
                            if safe != rows_by_id[row_id]["text_redacted"]
                        }
                        chunks = conn.execute(
                            """SELECT id,item_id,text_redacted FROM chunks
                               WHERE item_id=ANY(%s)""",
                            (row_ids,),
                        ).fetchall()
                        safe_chunks = dict(zip(
                            [chunk["id"] for chunk in chunks],
                            redact_batch([chunk["text_redacted"] for chunk in chunks]),
                            strict=True,
                        ))
                        changed_chunks = [
                            chunk for chunk in chunks
                            if safe_chunks[chunk["id"]] != chunk["text_redacted"]
                        ]
                        entity_rows = conn.execute(
                            "SELECT item_id,value FROM entities WHERE item_id=ANY(%s)",
                            (row_ids,),
                        ).fetchall()
                        safe_entity_values = redact_batch([
                            entity["value"] for entity in entity_rows
                        ])
                        unsafe_entity_items = {
                            entity["item_id"]
                            for entity, safe in zip(entity_rows, safe_entity_values, strict=True)
                            if safe != entity["value"]
                        }
                        repair_entity_items = (
                            changed_items
                            | {chunk["item_id"] for chunk in changed_chunks}
                            | unsafe_entity_items
                        )

                        for row_id in changed_items:
                            conn.execute(
                                "UPDATE items SET text_redacted=%s WHERE id=%s",
                                (safe_items[row_id], row_id),
                            )
                        for chunk in changed_chunks:
                            conn.execute(
                                "UPDATE chunks SET text_redacted=%s WHERE id=%s",
                                (safe_chunks[chunk["id"]], chunk["id"]),
                            )
                        for row_id in repair_entity_items:
                            row = rows_by_id[row_id]
                            conn.execute("DELETE FROM entities WHERE item_id=%s", (row_id,))
                            projected = [
                                (row_id, row["source_id"], kind, value, value.casefold())
                                for kind, value in engine.extract_entities(safe_items[row_id])
                            ]
                            if projected:
                                with conn.cursor() as cursor:
                                    cursor.executemany(
                                        """INSERT INTO entities(item_id,source_id,kind,value,normalized)
                                           VALUES (%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
                                        projected,
                                    )

                        conn.execute(
                            "UPDATE items SET projector_version=%s WHERE id=ANY(%s)",
                            (PROJECTOR_VERSION, row_ids),
                        )
                        last_item_id = rows[-1]["id"]
                        completed = last_item_id >= state["target_item_id"]
                        conn.execute(
                            """UPDATE projection_backfills SET last_item_id=%s,
                               completed_at=CASE WHEN %s THEN now() ELSE NULL END,updated_at=now()
                               WHERE name='redaction-v3'""",
                            (last_item_id, completed),
                        )
                        if completed:
                            conn.execute(
                                "UPDATE sessions SET projector_version=%s WHERE projector_version<%s",
                                (PROJECTOR_VERSION, PROJECTOR_VERSION),
                            )
                            conn.execute(
                                """UPDATE projection_watermarks SET version=%s,updated_at=now()
                                   WHERE projector='items' AND version<%s""",
                                (PROJECTOR_VERSION, PROJECTOR_VERSION),
                            )
                        batches += 1
                        scanned += len(rows)
                        rewritten += len(changed_items)
                        rewritten_chunks += len(changed_chunks)
                        rebuilt_entity_items += len(repair_entity_items)
                        if completed:
                            break
                state = conn.execute(
                    """SELECT target_item_id,last_item_id,completed_at
                       FROM projection_backfills WHERE name='redaction-v3'"""
                ).fetchone()
            finally:
                conn.execute(
                    "SELECT pg_advisory_unlock(hashtextextended('recall:redaction-backfill-v3',0))"
                )
                if executor is not None:
                    executor.shutdown()
        return {
            "batches": batches,
            "items_scanned": scanned,
            "items_rewritten": rewritten,
            "chunks_rewritten": rewritten_chunks,
            "entity_items_rebuilt": rebuilt_entity_items,
            "target_item_id": state["target_item_id"],
            "last_item_id": state["last_item_id"],
            "completed": state["completed_at"] is not None,
        }

    def export_raw(self) -> list[dict]:
        """Admin/offline API only; intentionally not routed by the HTTP app."""
        with self.connect() as conn:
            return [row["envelope"] for row in conn.execute("SELECT envelope FROM source_events ORDER BY id")]
