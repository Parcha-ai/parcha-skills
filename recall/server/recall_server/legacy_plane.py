"""Legacy v1 plane retirement: feature flags, 410 responses, canonical bridge.

Miguel's 2026-09-11 decision: the v1 tables (``sources``, ``source_grants``,
``source_events``, ``items``, ``chunks``, ``entities``, ``item_embeddings``,
``sessions``, ``turn_embedding*``, ``projection_watermarks``,
``projection_backfills``, ``ingest_batches``,
``embedding_projection_watermarks``) stop receiving writes now, the four
legacy read routes answer ``410 Gone``, and the tables are dropped thirty
days later. ``/v1/receipts/resolve`` stays and resolves canonical receipts.

Both flags default to ``0`` (retired). ``RECALL_LEGACY_WRITES=1`` restores the
old dual-write path for rollback; ``RECALL_LEGACY_READS=1`` restores the four
legacy read routes.
"""

from __future__ import annotations

import copy
import json
import os
from collections.abc import Mapping
from typing import Any

from .canonical import (
    CanonicalArchiveGateway,
    CanonicalLifecycleError,
    CanonicalPlane,
)
from .projectors import canonical_json, validate_envelope

LEGACY_WRITES_ENV = "RECALL_LEGACY_WRITES"
LEGACY_READS_ENV = "RECALL_LEGACY_READS"
LEGACY_INGEST_TENANT_ENV = "RECALL_LEGACY_INGEST_TENANT_ID"
DEFAULT_LEGACY_INGEST_TENANT = "tenant:personal"

LEGACY_PLANE_RETIRED_CODE = "legacy_plane_retired"

# Legacy read route -> canonical replacement (MCP tool name).
LEGACY_READ_REPLACEMENTS: dict[str, str] = {
    "/v1/search": "recall_search",
    "/v1/show": "recall_show",
    "/v1/related": "recall_related",
    "/v1/session-export": "recall_session_context",
}
LEGACY_READ_ROUTES = frozenset(LEGACY_READ_REPLACEMENTS)

# Tables the thirty-day drop removes. Listed here so the runbook, the CLI, and
# the tests share one inventory.
LEGACY_DROPPABLE_TABLES: tuple[str, ...] = (
    "chunks",
    "embedding_projection_watermarks",
    "entities",
    "ingest_batches",
    "item_embeddings",
    "items",
    "projection_backfills",
    "projection_watermarks",
    "sessions",
    "source_events",
    "source_grants",
    "sources",
    "turn_embedding_items",
    "turn_embedding_projection_watermarks",
    "turn_embeddings",
)

_TRUE = frozenset({"1", "true", "yes", "on"})
_FALSE = frozenset({"0", "false", "no", "off", ""})


class LegacyFlagError(ValueError):
    """An ``RECALL_LEGACY_*`` value is not a recognisable boolean."""


class CanonicalPlaneUnavailable(RuntimeError):
    """Legacy writes are retired and no canonical plane is configured."""


def parse_flag(
    name: str,
    *,
    default: bool = False,
    environment: Mapping[str, str] | None = None,
) -> bool:
    values = os.environ if environment is None else environment
    raw = values.get(name)
    if raw is None:
        return default
    normalized = raw.strip().casefold()
    if normalized in _TRUE:
        return True
    if normalized in _FALSE:
        return False
    raise LegacyFlagError(f"{name} must be 0 or 1")


def legacy_writes_enabled(environment: Mapping[str, str] | None = None) -> bool:
    return parse_flag(LEGACY_WRITES_ENV, default=False, environment=environment)


def legacy_reads_enabled(environment: Mapping[str, str] | None = None) -> bool:
    return parse_flag(LEGACY_READS_ENV, default=False, environment=environment)


def validate_legacy_flags(environment: Mapping[str, str] | None = None) -> None:
    """Fail fast at startup on an unparseable flag instead of per request."""
    try:
        legacy_writes_enabled(environment)
        legacy_reads_enabled(environment)
    except LegacyFlagError as error:
        raise RuntimeError(str(error)) from None


def legacy_ingest_tenant_id(
    principal: Mapping[str, Any] | None = None,
    environment: Mapping[str, str] | None = None,
) -> str:
    """Tenant that owns canonical rows written on behalf of a v1 caller.

    A credential bound to a tenant wins. Otherwise the deployment-wide
    ``RECALL_LEGACY_INGEST_TENANT_ID`` applies (default ``tenant:personal``,
    matching the collector-side ``RECALL_TENANT_ID`` default).
    """
    if principal is not None:
        bound = principal.get("tenant_id")
        if isinstance(bound, str) and bound:
            return bound
    values = os.environ if environment is None else environment
    configured = values.get(LEGACY_INGEST_TENANT_ENV, "").strip()
    return configured or DEFAULT_LEGACY_INGEST_TENANT


def legacy_retired_response(path: str) -> dict[str, str]:
    replacement = LEGACY_READ_REPLACEMENTS.get(path)
    if replacement is None:
        raise KeyError(path)
    return {
        "error": "gone",
        "code": LEGACY_PLANE_RETIRED_CODE,
        "replacement": replacement,
    }


def legacy_connector_id(
    envelope: Mapping[str, Any],
    default: str | None = None,
) -> str:
    """Connector identity for a v1 envelope that never carried one."""
    provenance = envelope.get("provenance") or {}
    connector_id = provenance.get("connector_id")
    if isinstance(connector_id, str) and connector_id:
        return connector_id
    if default:
        return default
    harness = provenance.get("harness")
    if isinstance(harness, str) and harness:
        return f"legacy.{harness}"
    return "legacy.ingest"


def _redacted_text(envelope: Mapping[str, Any]) -> str:
    if envelope.get("kind") == "tombstone":
        return ""
    return json.dumps(
        envelope.get("content"),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def _text_summary(envelope: Mapping[str, Any]) -> str:
    """Search text for one canonical document.

    Typed connector records (webhooks) expose a ``text`` field; everything else
    keeps the compact JSON body, exactly like ``CanonicalPlane.ingest_batch``.
    """
    content = envelope.get("content")
    if (
        envelope.get("kind") == "connector_record"
        and isinstance(content, dict)
        and isinstance(content.get("text"), str)
        and content["text"]
    ):
        return content["text"]
    return _redacted_text(envelope)


class LegacyIngestBridge:
    """Route v1 envelope writes onto the canonical plane.

    Every accepted envelope gets its raw bytes archived through the fenced
    archive gateway (``raw_artifacts``) and then flows through
    ``CanonicalPlane.ingest_document``. Content-addressed dedupe makes a
    replay return ``duplicate_events=1`` and ``replay=True`` with the same
    receipt, which ``/v1/receipts/resolve`` still resolves.
    """

    def __init__(
        self,
        store: Any,
        canonical_plane: CanonicalPlane | None,
        archive_store: Any | None,
        *,
        environment: Mapping[str, str] | None = None,
    ):
        self.store = store
        self.canonical_plane = canonical_plane
        self.archive_store = archive_store
        self.environment = environment

    @property
    def canonical_available(self) -> bool:
        return self.canonical_plane is not None and self.archive_store is not None

    def ingest(
        self,
        idempotency_key: str,
        events: list[dict[str, Any]],
        *,
        principal: Mapping[str, Any] | None,
        raw_payload: bytes | None = None,
        media_type: str = "application/json",
        connector_id: str | None = None,
    ) -> tuple[dict[str, Any], bool]:
        """Commit ``events`` and return ``(acknowledgement, replay)``.

        ``raw_payload`` is the request body a webhook received; when omitted
        each envelope's canonical JSON is the archived artifact.
        ``connector_id`` names the connector for envelopes that carry none.
        """
        if legacy_writes_enabled(self.environment):
            if self.canonical_available:
                self._ingest_canonical(
                    events,
                    principal=principal,
                    raw_payload=raw_payload,
                    media_type=media_type,
                    connector_id=connector_id,
                )
            return self.store.ingest(idempotency_key, events)
        if not idempotency_key or len(idempotency_key) > 200:
            raise ValueError("invalid idempotency key")
        if not self.canonical_available:
            raise CanonicalPlaneUnavailable("canonical plane unavailable")
        acknowledgement = self._ingest_canonical(
            events,
            principal=principal,
            raw_payload=raw_payload,
            media_type=media_type,
            connector_id=connector_id,
        )
        return acknowledgement, acknowledgement["replay"]

    def _ingest_canonical(
        self,
        events: list[dict[str, Any]],
        *,
        principal: Mapping[str, Any] | None,
        raw_payload: bytes | None,
        media_type: str,
        connector_id: str | None = None,
    ) -> dict[str, Any]:
        if not isinstance(events, list) or not events:
            raise ValueError("empty ingest batch")
        tenant_id = legacy_ingest_tenant_id(principal, self.environment)
        prepared: list[tuple[str, str, dict[str, Any], dict[str, Any]]] = []
        for original in events:
            envelope = copy.deepcopy(validate_envelope(original))
            principal_id = envelope["principal_id"]
            source_id = envelope["source_id"]
            event_connector_id = legacy_connector_id(envelope, connector_id)
            payload = (
                raw_payload
                if raw_payload is not None
                else canonical_json(original)
            )
            gateway = CanonicalArchiveGateway(
                self.store,
                self.archive_store,
                tenant_id=tenant_id,
                principal_id=principal_id,
            )
            artifact = gateway.put_raw(
                tenant_id=tenant_id,
                source_id=source_id,
                native_id=envelope["native_id"],
                payload=payload,
                media_type=media_type,
                created_at=envelope["observed_at"],
            )
            provenance = dict(envelope.get("provenance") or {})
            provenance["connector_id"] = event_connector_id
            provenance["artifact_ref"] = artifact
            envelope["provenance"] = provenance
            prepared.append((principal_id, event_connector_id, artifact, envelope))
        results: list[dict[str, Any]] = []
        with self.store.connect() as connection:
            with connection.transaction():
                for principal_id, event_connector_id, artifact, envelope in prepared:
                    results.append(
                        self.canonical_plane.ingest_document(
                            tenant_id=tenant_id,
                            principal_id=principal_id,
                            connector_id=event_connector_id,
                            artifact_ref=artifact,
                            envelope=envelope,
                            text_redacted=_text_summary(envelope),
                            _connection=connection,
                        )
                    )
        return {
            "status": "committed",
            "inserted": sum(result["inserted"] for result in results),
            "duplicate_events": sum(
                result["duplicate_events"] for result in results
            ),
            "receipts": [result["receipt"] for result in results],
            "replay": all(result["replay"] for result in results),
        }


__all__ = [
    "CanonicalLifecycleError",
    "CanonicalPlaneUnavailable",
    "DEFAULT_LEGACY_INGEST_TENANT",
    "LEGACY_DROPPABLE_TABLES",
    "LEGACY_INGEST_TENANT_ENV",
    "LEGACY_PLANE_RETIRED_CODE",
    "LEGACY_READ_REPLACEMENTS",
    "LEGACY_READ_ROUTES",
    "LEGACY_READS_ENV",
    "LEGACY_WRITES_ENV",
    "LegacyFlagError",
    "LegacyIngestBridge",
    "legacy_connector_id",
    "legacy_ingest_tenant_id",
    "legacy_reads_enabled",
    "legacy_retired_response",
    "legacy_writes_enabled",
    "parse_flag",
    "validate_legacy_flags",
]
