from __future__ import annotations

import hashlib
import json
from contextlib import nullcontext
from datetime import datetime, timezone
from typing import Any, Protocol
from urllib.parse import unquote, urlsplit

from contracts.v2 import ContractError, IDENTITY_RE, validate_contract

from .db import BrainStore
from .projectors import validate_envelope

MAX_CANONICAL_TEXT_BYTES = 8_000_000
MAX_CANONICAL_CHUNK_BYTES = 24_000
MAX_LINKED_IDENTITIES = 10_000
MAX_CANONICAL_BATCH_EVENTS = 1_000


class CanonicalLifecycleError(RuntimeError):
    """Stable, content-free failure at a v2 lifecycle boundary."""

    def __init__(self, error_code: str):
        self.error_code = error_code
        super().__init__(error_code)


class ArchiveLifecycle(Protocol):
    def put_raw(
        self,
        *,
        tenant_id: str,
        source_id: str,
        native_id: str,
        payload: bytes,
        media_type: str,
        created_at: str,
    ) -> dict[str, Any]: ...

    def delete_raw(self, reference: dict[str, Any]) -> bool: ...


def _identity_sha256(tenant_id: str, source_id: str, native_id: str) -> str:
    return hashlib.sha256(
        "\x1f".join((tenant_id, source_id, native_id)).encode()
    ).hexdigest()


def _opaque(prefix: str, *values: str) -> str:
    digest = hashlib.sha256("\x1f".join(values).encode()).hexdigest()
    return f"{prefix}_{digest[:32]}"


def _linked_native_ids(
    conn: Any,
    *,
    tenant_id: str,
    source_id: str,
    native_id: str,
) -> list[str]:
    rows = conn.execute(
        """WITH RECURSIVE linked(native_id) AS (
               SELECT DISTINCT native_id
               FROM canonical_events
               WHERE tenant_id=%s AND source_id=%s AND native_parent_id=%s
               UNION
               SELECT event.native_id
               FROM canonical_events event
               JOIN linked parent ON event.native_parent_id=parent.native_id
               WHERE event.tenant_id=%s AND event.source_id=%s
           )
           SELECT native_id FROM linked ORDER BY native_id LIMIT %s""",
        (
            tenant_id,
            source_id,
            native_id,
            tenant_id,
            source_id,
            MAX_LINKED_IDENTITIES + 1,
        ),
    ).fetchall()
    if len(rows) > MAX_LINKED_IDENTITIES:
        raise CanonicalLifecycleError("canonical_lineage_limit")
    return [
        native_id,
        *sorted({row["native_id"] for row in rows if row["native_id"] != native_id}),
    ]


def canonical_text_chunks(text: str) -> list[str]:
    """Split retrieval text into lossless, embedding-safe UTF-8 chunks."""
    if not isinstance(text, str):
        raise TypeError("canonical text must be a string")
    encoded = text.encode()
    if not encoded:
        return [""]
    chunks: list[str] = []
    offset = 0
    while offset < len(encoded):
        end = min(offset + MAX_CANONICAL_CHUNK_BYTES, len(encoded))
        while end < len(encoded) and end > offset and encoded[end] & 0xC0 == 0x80:
            end -= 1
        if end == offset:
            raise ValueError("canonical chunk boundary is invalid")
        candidate = encoded[offset:end].decode()
        if end < len(encoded):
            window_start = max(0, len(candidate) - 2_048)
            split = max(
                candidate.rfind("\n", window_start),
                candidate.rfind(" ", window_start),
            )
            if split >= window_start:
                candidate = candidate[: split + 1]
                end = offset + len(candidate.encode())
        chunks.append(candidate)
        offset = end
    return chunks


def _timestamp(value: Any) -> str:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    if isinstance(value, str):
        return value
    raise CanonicalLifecycleError("canonical_state_invalid")


def _native_from_receipt(receipt: str, source_id: str) -> str:
    parsed = urlsplit(receipt)
    if parsed.scheme != "recall" or parsed.netloc != source_id:
        raise CanonicalLifecycleError("forget_target_invalid")
    native_id = unquote(parsed.path.lstrip("/"))
    if not native_id or not IDENTITY_RE.fullmatch(native_id):
        raise CanonicalLifecycleError("forget_target_invalid")
    return native_id


class CanonicalArchiveGateway:
    """Serialize archive writes with the canonical forget fence."""

    def __init__(
        self,
        store: BrainStore,
        archive: ArchiveLifecycle,
        *,
        tenant_id: str,
        principal_id: str,
    ):
        CanonicalPlane._validate_host_identity(
            tenant_id, principal_id, "source:placeholder",
        )
        self.store = store
        self.archive = archive
        self.tenant_id = tenant_id
        self.principal_id = principal_id

    def put_raw(
        self,
        *,
        tenant_id: str,
        source_id: str,
        native_id: str,
        payload: bytes,
        media_type: str,
        created_at: str,
    ) -> dict[str, Any]:
        if tenant_id != self.tenant_id:
            raise CanonicalLifecycleError("archive_authority_forbidden")
        CanonicalPlane._validate_host_identity(
            tenant_id, self.principal_id, source_id,
        )
        if not isinstance(native_id, str) or not IDENTITY_RE.fullmatch(native_id):
            raise CanonicalLifecycleError("archive_identity_invalid")
        identity_sha256 = _identity_sha256(tenant_id, source_id, native_id)
        with self.store.connect() as conn:
            with conn.transaction():
                CanonicalPlane._register_source(
                    conn,
                    tenant_id=tenant_id,
                    principal_id=self.principal_id,
                    source_id=source_id,
                )
                conn.execute(
                    """SELECT pg_advisory_xact_lock(hashtextextended(%s,0))""",
                    (f"v2\x1f{tenant_id}\x1f{source_id}\x1f{native_id}",),
                )
                forgotten = conn.execute(
                    """SELECT 1 FROM forget_tombstones
                       WHERE tenant_id=%s AND source_id=%s
                         AND target_identity_sha256=%s""",
                    (tenant_id, source_id, identity_sha256),
                ).fetchone()
                if forgotten:
                    raise CanonicalLifecycleError("archive_identity_forgotten")
                return self.archive.put_raw(
                    tenant_id=tenant_id,
                    source_id=source_id,
                    native_id=native_id,
                    payload=payload,
                    media_type=media_type,
                    created_at=created_at,
                )


class CanonicalPlane:
    """Tenant-aware v2 canonical ingest and crash-resumable authoritative forget."""

    def __init__(
        self,
        store: BrainStore,
        archive: ArchiveLifecycle,
        evidence_projector: Any = None,
    ):
        self.store = store
        self.archive = archive
        self.evidence_projector = evidence_projector

    @staticmethod
    def _validate_host_identity(
        tenant_id: str,
        principal_id: str,
        source_id: str,
    ) -> None:
        if not all(
            isinstance(value, str) and IDENTITY_RE.fullmatch(value)
            for value in (tenant_id, principal_id, source_id)
        ):
            raise CanonicalLifecycleError("canonical_authority_invalid")

    @staticmethod
    def _register_source(
        conn: Any,
        *,
        tenant_id: str,
        principal_id: str,
        source_id: str,
    ) -> None:
        conn.execute(
            "INSERT INTO brain_tenants(tenant_id) VALUES (%s) ON CONFLICT DO NOTHING",
            (tenant_id,),
        )
        conn.execute(
            """INSERT INTO brain_principals(tenant_id,principal_id)
               VALUES (%s,%s) ON CONFLICT DO NOTHING""",
            (tenant_id, principal_id),
        )
        conn.execute(
            """INSERT INTO canonical_sources(tenant_id,source_id,owner_principal_id)
               VALUES (%s,%s,%s) ON CONFLICT DO NOTHING""",
            (tenant_id, source_id, principal_id),
        )
        owner = conn.execute(
            """SELECT owner_principal_id
               FROM canonical_sources
               WHERE tenant_id=%s AND source_id=%s""",
            (tenant_id, source_id),
        ).fetchone()
        if owner is None or owner["owner_principal_id"] != principal_id:
            raise CanonicalLifecycleError("canonical_authority_forbidden")
        conn.execute(
            """INSERT INTO canonical_source_grants(
                   tenant_id,principal_id,source_id,permission
               ) VALUES (%s,%s,%s,'owner')
               ON CONFLICT(tenant_id,principal_id,source_id)
               DO UPDATE SET permission='owner'""",
            (tenant_id, principal_id, source_id),
        )
        conn.execute(
            """INSERT INTO canonical_source_grants(
                   tenant_id,principal_id,source_id,permission
               ) SELECT space.tenant_id,access.principal_id,%s,'read'
                   FROM brain_spaces space
                   JOIN brain_memberships source_membership
                     ON source_membership.organization_id=space.organization_id
                    AND source_membership.principal_id=%s
                   JOIN brain_access_grants access
                     ON access.tenant_id=space.tenant_id
                    AND access.permission IN ('owner','admin','read')
                   JOIN brain_invitations invitation
                     ON invitation.tenant_id=space.tenant_id
                    AND invitation.accepted_principal_id=access.principal_id
                    AND invitation.accepted_at IS NOT NULL
                    AND invitation.revoked_at IS NULL
                  WHERE space.tenant_id=%s AND space.brain_kind='company'
               ON CONFLICT(tenant_id,principal_id,source_id)
               DO UPDATE SET permission=CASE
                   WHEN canonical_source_grants.permission='owner' THEN 'owner'
                   ELSE 'read'
               END""",
            (source_id, principal_id, tenant_id),
        )

    def ingest_document(
        self,
        *,
        tenant_id: str,
        principal_id: str,
        connector_id: str,
        artifact_ref: dict[str, Any],
        envelope: dict[str, Any],
        text_redacted: str,
        _connection: Any | None = None,
    ) -> dict[str, Any]:
        self._validate_host_identity(tenant_id, principal_id, envelope.get("source_id"))
        if not isinstance(connector_id, str) or not IDENTITY_RE.fullmatch(connector_id):
            raise CanonicalLifecycleError("canonical_connector_invalid")
        if (
            not isinstance(text_redacted, str)
            or len(text_redacted.encode()) > MAX_CANONICAL_TEXT_BYTES
        ):
            raise CanonicalLifecycleError("canonical_text_invalid")
        try:
            artifact = validate_contract(
                artifact_ref, expected="recall.artifact-ref.v1",
            )
            event = validate_envelope(envelope)
        except (ContractError, ValueError):
            raise CanonicalLifecycleError("canonical_contract_invalid") from None
        source_id = event["source_id"]
        native_id = event["native_id"]
        is_tombstone = event["kind"] == "tombstone"
        if (
            artifact["tenant_id"] != tenant_id
            or artifact["source_id"] != source_id
            or event["principal_id"] != principal_id
            or event.get("provenance", {}).get("artifact_ref") != artifact
        ):
            raise CanonicalLifecycleError("canonical_lineage_invalid")
        raw_sha256 = artifact["content_sha256"]
        event_sha256 = event["content_sha256"]
        identity_sha256 = _identity_sha256(tenant_id, source_id, native_id)
        event_id = _opaque("evt", tenant_id, source_id, native_id, event_sha256)
        job_id = _opaque("job", tenant_id, source_id, connector_id, event_id)
        document_id = _opaque("doc", tenant_id, source_id, event_id)
        text_sha256 = hashlib.sha256(text_redacted.encode()).hexdigest()
        chunk_texts = (
            []
            if is_tombstone
            else canonical_text_chunks(text_redacted)
        )

        connection_context = (
            self.store.connect()
            if _connection is None
            else nullcontext(_connection)
        )
        with connection_context as conn:
            transaction_context = (
                conn.transaction()
                if _connection is None
                else nullcontext()
            )
            with transaction_context:
                self._register_source(
                    conn,
                    tenant_id=tenant_id,
                    principal_id=principal_id,
                    source_id=source_id,
                )
                forgotten = conn.execute(
                    """SELECT 1 FROM forget_tombstones
                       WHERE tenant_id=%s AND source_id=%s
                         AND target_identity_sha256=%s""",
                    (tenant_id, source_id, identity_sha256),
                ).fetchone()
                if forgotten:
                    raise CanonicalLifecycleError("canonical_identity_forgotten")
                conn.execute(
                    """SELECT pg_advisory_xact_lock(hashtextextended(%s,0))""",
                    (f"v2\x1f{tenant_id}\x1f{source_id}\x1f{native_id}",),
                )
                conn.execute(
                    """INSERT INTO raw_artifacts(
                           tenant_id,source_id,artifact_id,storage_backend,object_key,
                           content_sha256,size_bytes,media_type,encryption,version_id,created_at
                       ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                       ON CONFLICT(tenant_id,source_id,artifact_id) DO NOTHING""",
                    (
                        tenant_id, source_id, artifact["artifact_id"],
                        artifact["storage_backend"], artifact["object_key"],
                        raw_sha256, artifact["size_bytes"], artifact["media_type"],
                        artifact["encryption"], artifact["version_id"],
                        artifact["created_at"],
                    ),
                )
                stored_artifact = conn.execute(
                    """SELECT storage_backend,object_key,content_sha256,size_bytes,
                              media_type,encryption,version_id,state
                       FROM raw_artifacts
                       WHERE tenant_id=%s AND source_id=%s AND artifact_id=%s""",
                    (tenant_id, source_id, artifact["artifact_id"]),
                ).fetchone()
                expected_artifact = {
                    key: artifact[key]
                    for key in (
                        "storage_backend", "object_key", "content_sha256", "size_bytes",
                        "media_type", "encryption", "version_id",
                    )
                }
                if (
                    stored_artifact is None
                    or stored_artifact["state"] != "live"
                    or any(
                        stored_artifact[key] != value
                        for key, value in expected_artifact.items()
                    )
                ):
                    raise CanonicalLifecycleError("canonical_artifact_conflict")
                existing = conn.execute(
                    """SELECT revision FROM canonical_events
                       WHERE tenant_id=%s AND source_id=%s
                         AND native_id=%s AND content_sha256=%s""",
                    (tenant_id, source_id, native_id, event_sha256),
                ).fetchone()
                if existing:
                    revision = existing["revision"]
                    return {
                        "status": "committed",
                        "inserted": 0,
                        "duplicate_events": 1,
                        "revision": revision,
                        "receipt": (
                            f"recall://{source_id}/{native_id}?rev={revision}#item=0"
                        ),
                        "replay": True,
                    }
                row = conn.execute(
                    """SELECT COALESCE(max(revision),0)+1 AS revision
                       FROM canonical_events
                       WHERE tenant_id=%s AND source_id=%s AND native_id=%s""",
                    (tenant_id, source_id, native_id),
                ).fetchone()
                revision = row["revision"]
                receipt = f"recall://{source_id}/{native_id}?rev={revision}#item=0"
                conn.execute(
                    """INSERT INTO canonical_ingest_jobs(
                           tenant_id,source_id,job_id,connector_id,mode,status,
                           attempt,created_at,updated_at
                       ) VALUES (%s,%s,%s,%s,'incremental','committed',1,now(),now())
                       ON CONFLICT(tenant_id,source_id,job_id) DO NOTHING""",
                    (tenant_id, source_id, job_id, connector_id),
                )
                conn.execute(
                    """INSERT INTO canonical_events(
                           tenant_id,source_id,event_id,native_id,native_parent_id,
                           artifact_id,job_id,kind,content_sha256,revision,
                           occurred_at,observed_at,is_tombstone,canonical_redacted
                       ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    (
                        tenant_id, source_id, event_id, native_id,
                        event.get("native_parent_id"), artifact["artifact_id"], job_id,
                        event["kind"], event_sha256, revision, event["occurred_at"],
                        event["observed_at"], is_tombstone, json.dumps(event),
                    ),
                )
                affected_native_ids = (
                    _linked_native_ids(
                        conn,
                        tenant_id=tenant_id,
                        source_id=source_id,
                        native_id=native_id,
                    )
                    if is_tombstone
                    else [native_id]
                )
                conn.execute(
                    """UPDATE canonical_documents
                       SET is_current=false,
                           deleted_at=CASE WHEN %s THEN now() ELSE deleted_at END
                       WHERE tenant_id=%s AND source_id=%s AND native_id=ANY(%s)
                         AND is_current""",
                    (is_tombstone, tenant_id, source_id, affected_native_ids),
                )
                if is_tombstone:
                    conn.execute(
                        """UPDATE canonical_chunks chunk
                           SET deleted_at=now()
                           FROM canonical_documents document
                           WHERE chunk.tenant_id=document.tenant_id
                             AND chunk.source_id=document.source_id
                             AND chunk.document_id=document.document_id
                             AND document.tenant_id=%s AND document.source_id=%s
                             AND document.native_id=ANY(%s)
                             AND chunk.deleted_at IS NULL""",
                        (tenant_id, source_id, affected_native_ids),
                    )
                else:
                    conn.execute(
                        """INSERT INTO canonical_documents(
                               tenant_id,source_id,document_id,event_id,artifact_id,native_id,
                               content_sha256,revision,is_current,text_redacted,text_sha256
                           ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,true,%s,%s)""",
                        (
                            tenant_id, source_id, document_id, event_id,
                            artifact["artifact_id"], native_id, event_sha256, revision,
                            text_redacted, text_sha256,
                        ),
                    )
                    with conn.cursor() as cursor:
                        cursor.executemany(
                            """INSERT INTO canonical_chunks(
                                   tenant_id,source_id,chunk_id,document_id,ordinal,
                                   receipt,text_redacted,text_sha256
                               ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
                            [
                                (
                                    tenant_id,
                                    source_id,
                                    _opaque(
                                        "chk",
                                        tenant_id,
                                        source_id,
                                        document_id,
                                        str(ordinal),
                                    ),
                                    document_id,
                                    ordinal,
                                    (
                                        f"recall://{source_id}/{native_id}"
                                        f"?rev={revision}#item={ordinal}"
                                    ),
                                    chunk_text,
                                    hashlib.sha256(chunk_text.encode()).hexdigest(),
                                )
                                for ordinal, chunk_text in enumerate(chunk_texts)
                            ],
                        )
                conn.execute(
                    """INSERT INTO canonical_audit_events(
                           tenant_id,source_id,audit_id,operation,status,
                           subject_sha256,item_count,byte_count
                       ) VALUES (%s,%s,%s,'ingest.commit','success',%s,1,%s)""",
                    (
                        tenant_id, source_id,
                        _opaque("audit", tenant_id, source_id, event_id, "ingest"),
                        identity_sha256, artifact["size_bytes"],
                    ),
                )
        return {
            "status": "committed",
            "inserted": 1,
            "duplicate_events": 0,
            "revision": revision,
            "receipt": receipt,
            "replay": False,
        }

    def ingest_batch(
        self,
        *,
        tenant_id: str,
        principal_id: str,
        events: list[dict[str, Any]],
    ) -> dict[str, Any]:
        if (
            not isinstance(events, list)
            or not 1 <= len(events) <= MAX_CANONICAL_BATCH_EVENTS
        ):
            raise CanonicalLifecycleError("canonical_batch_invalid")
        if (
            all(event.get("kind") != "tombstone" for event in events)
            and len({event.get("native_id") for event in events}) == len(events)
            and len({event.get("source_id") for event in events}) == 1
        ):
            return self._ingest_live_batch(
                tenant_id=tenant_id,
                principal_id=principal_id,
                events=events,
            )
        results = []
        with self.store.connect() as connection:
            with connection.transaction():
                for envelope in events:
                    provenance = envelope.get("provenance", {})
                    connector_id = provenance.get("connector_id")
                    artifact_ref = provenance.get("artifact_ref")
                    text_redacted = json.dumps(
                        envelope.get("content"),
                        sort_keys=True,
                        separators=(",", ":"),
                        ensure_ascii=False,
                        allow_nan=False,
                    )
                    results.append(self.ingest_document(
                        tenant_id=tenant_id,
                        principal_id=principal_id,
                        connector_id=connector_id,
                        artifact_ref=artifact_ref,
                        envelope=envelope,
                        text_redacted=(
                            ""
                            if envelope.get("kind") == "tombstone"
                            else text_redacted
                        ),
                        _connection=connection,
                    ))
        return {
            "status": "committed",
            "inserted": sum(result["inserted"] for result in results),
            "duplicate_events": sum(
                result["duplicate_events"] for result in results
            ),
            "receipts": [result["receipt"] for result in results],
            "replay": all(result["replay"] for result in results),
        }

    def _ingest_live_batch(
        self,
        *,
        tenant_id: str,
        principal_id: str,
        events: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """Commit independent live documents with set-based remote SQL.

        Tombstones and duplicate native identities intentionally stay on the
        lineage-aware sequential path. This path removes per-event database
        round trips while preserving the same contracts, locks, revisions,
        artifact checks, receipts, and audit records.
        """
        prepared: list[dict[str, Any]] = []
        artifacts: dict[str, dict[str, Any]] = {}
        source_ids: set[str] = set()
        for envelope in events:
            provenance = envelope.get("provenance", {})
            connector_id = provenance.get("connector_id")
            artifact_ref = provenance.get("artifact_ref")
            if not isinstance(connector_id, str) or not IDENTITY_RE.fullmatch(
                connector_id
            ):
                raise CanonicalLifecycleError("canonical_connector_invalid")
            try:
                artifact = validate_contract(
                    artifact_ref,
                    expected="recall.artifact-ref.v1",
                )
                event = validate_envelope(envelope)
            except (ContractError, ValueError):
                raise CanonicalLifecycleError("canonical_contract_invalid") from None
            self._validate_host_identity(
                tenant_id,
                principal_id,
                event.get("source_id"),
            )
            source_id = event["source_id"]
            source_ids.add(source_id)
            if (
                artifact["tenant_id"] != tenant_id
                or artifact["source_id"] != source_id
                or event["principal_id"] != principal_id
                or event.get("provenance", {}).get("artifact_ref") != artifact
            ):
                raise CanonicalLifecycleError("canonical_lineage_invalid")
            text_redacted = json.dumps(
                event.get("content"),
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
                allow_nan=False,
            )
            if len(text_redacted.encode()) > MAX_CANONICAL_TEXT_BYTES:
                raise CanonicalLifecycleError("canonical_text_invalid")
            artifact_id = artifact["artifact_id"]
            prior_artifact = artifacts.get(artifact_id)
            if prior_artifact is not None and prior_artifact != artifact:
                raise CanonicalLifecycleError("canonical_artifact_conflict")
            artifacts[artifact_id] = artifact
            native_id = event["native_id"]
            event_sha256 = event["content_sha256"]
            event_id = _opaque(
                "evt",
                tenant_id,
                source_id,
                native_id,
                event_sha256,
            )
            prepared.append(
                {
                    "source_id": source_id,
                    "native_id": native_id,
                    "native_parent_id": event.get("native_parent_id"),
                    "connector_id": connector_id,
                    "artifact": artifact,
                    "event": event,
                    "event_id": event_id,
                    "event_sha256": event_sha256,
                    "identity_sha256": _identity_sha256(
                        tenant_id,
                        source_id,
                        native_id,
                    ),
                    "job_id": _opaque(
                        "job",
                        tenant_id,
                        source_id,
                        connector_id,
                        event_id,
                    ),
                    "document_id": _opaque(
                        "doc",
                        tenant_id,
                        source_id,
                        event_id,
                    ),
                    "text_redacted": text_redacted,
                    "text_sha256": hashlib.sha256(
                        text_redacted.encode()
                    ).hexdigest(),
                    "chunks": canonical_text_chunks(text_redacted),
                }
            )
        if len(source_ids) != 1:
            raise CanonicalLifecycleError("canonical_batch_invalid")
        source_id = next(iter(source_ids))
        native_ids = [item["native_id"] for item in prepared]
        lock_keys = sorted(
            f"v2\x1f{tenant_id}\x1f{source_id}\x1f{native_id}"
            for native_id in native_ids
        )

        with self.store.connect() as connection:
            with connection.transaction():
                self._register_source(
                    connection,
                    tenant_id=tenant_id,
                    principal_id=principal_id,
                    source_id=source_id,
                )
                forgotten = connection.execute(
                    """SELECT target_identity_sha256
                       FROM forget_tombstones
                       WHERE tenant_id=%s AND source_id=%s
                         AND target_identity_sha256=ANY(%s)""",
                    (
                        tenant_id,
                        source_id,
                        [item["identity_sha256"] for item in prepared],
                    ),
                ).fetchone()
                if forgotten:
                    raise CanonicalLifecycleError("canonical_identity_forgotten")
                connection.execute(
                    """SELECT pg_advisory_xact_lock(hashtextextended(value,0))
                       FROM unnest(%s::text[]) AS lock_key(value)
                       ORDER BY value""",
                    (lock_keys,),
                ).fetchall()

                artifact_rows = [
                    {
                        key: artifact[key]
                        for key in (
                            "artifact_id",
                            "storage_backend",
                            "object_key",
                            "content_sha256",
                            "size_bytes",
                            "media_type",
                            "encryption",
                            "version_id",
                            "created_at",
                        )
                    }
                    for artifact in artifacts.values()
                ]
                connection.execute(
                    """INSERT INTO raw_artifacts(
                           tenant_id,source_id,artifact_id,storage_backend,object_key,
                           content_sha256,size_bytes,media_type,encryption,version_id,
                           created_at
                       )
                       SELECT %s,%s,row.artifact_id,row.storage_backend,row.object_key,
                              row.content_sha256,row.size_bytes,row.media_type,
                              row.encryption,row.version_id,row.created_at
                       FROM jsonb_to_recordset(%s::jsonb) AS row(
                           artifact_id text,storage_backend text,object_key text,
                           content_sha256 char(64),size_bytes bigint,media_type text,
                           encryption text,version_id text,created_at timestamptz
                       )
                       ON CONFLICT(tenant_id,source_id,artifact_id) DO NOTHING""",
                    (
                        tenant_id,
                        source_id,
                        json.dumps(artifact_rows),
                    ),
                )
                stored_artifacts = connection.execute(
                    """SELECT artifact_id,storage_backend,object_key,content_sha256,
                              size_bytes,media_type,encryption,version_id,state
                       FROM raw_artifacts
                       WHERE tenant_id=%s AND source_id=%s
                         AND artifact_id=ANY(%s)""",
                    (
                        tenant_id,
                        source_id,
                        list(artifacts),
                    ),
                ).fetchall()
                stored_by_id = {
                    row["artifact_id"]: row for row in stored_artifacts
                }
                for artifact_id, artifact in artifacts.items():
                    stored = stored_by_id.get(artifact_id)
                    if (
                        stored is None
                        or stored["state"] != "live"
                        or any(
                            stored[key] != artifact[key]
                            for key in (
                                "storage_backend",
                                "object_key",
                                "content_sha256",
                                "size_bytes",
                                "media_type",
                                "encryption",
                                "version_id",
                            )
                        )
                    ):
                        raise CanonicalLifecycleError("canonical_artifact_conflict")

                prior_events = connection.execute(
                    """SELECT native_id,content_sha256,revision
                       FROM canonical_events
                       WHERE tenant_id=%s AND source_id=%s
                         AND native_id=ANY(%s)""",
                    (tenant_id, source_id, native_ids),
                ).fetchall()
                exact_prior = {
                    (row["native_id"], row["content_sha256"]): row["revision"]
                    for row in prior_events
                }
                max_revision: dict[str, int] = {}
                for row in prior_events:
                    max_revision[row["native_id"]] = max(
                        max_revision.get(row["native_id"], 0),
                        int(row["revision"]),
                    )
                live_rows: list[dict[str, Any]] = []
                results: list[dict[str, Any]] = []
                for item in prepared:
                    prior_revision = exact_prior.get(
                        (item["native_id"], item["event_sha256"])
                    )
                    if prior_revision is not None:
                        revision = int(prior_revision)
                        results.append(
                            {
                                "inserted": 0,
                                "duplicate_events": 1,
                                "revision": revision,
                                "receipt": (
                                    f"recall://{source_id}/{item['native_id']}"
                                    f"?rev={revision}#item=0"
                                ),
                                "replay": True,
                            }
                        )
                        continue
                    revision = max_revision.get(item["native_id"], 0) + 1
                    item["revision"] = revision
                    item["receipt"] = (
                        f"recall://{source_id}/{item['native_id']}"
                        f"?rev={revision}#item=0"
                    )
                    live_rows.append(item)
                    results.append(
                        {
                            "inserted": 1,
                            "duplicate_events": 0,
                            "revision": revision,
                            "receipt": item["receipt"],
                            "replay": False,
                        }
                    )

                if live_rows:
                    connection.execute(
                        """UPDATE canonical_documents
                           SET is_current=false
                           WHERE tenant_id=%s AND source_id=%s
                             AND native_id=ANY(%s) AND is_current""",
                        (
                            tenant_id,
                            source_id,
                            [item["native_id"] for item in live_rows],
                        ),
                    )
                    job_rows = [
                        {
                            "job_id": item["job_id"],
                            "connector_id": item["connector_id"],
                        }
                        for item in live_rows
                    ]
                    connection.execute(
                        """INSERT INTO canonical_ingest_jobs(
                               tenant_id,source_id,job_id,connector_id,mode,status,
                               attempt,created_at,updated_at
                           )
                           SELECT %s,%s,row.job_id,row.connector_id,
                                  'incremental','committed',1,now(),now()
                           FROM jsonb_to_recordset(%s::jsonb) AS row(
                               job_id text,connector_id text
                           )
                           ON CONFLICT(tenant_id,source_id,job_id) DO NOTHING""",
                        (
                            tenant_id,
                            source_id,
                            json.dumps(job_rows),
                        ),
                    )
                    event_rows = [
                        {
                            "event_id": item["event_id"],
                            "native_id": item["native_id"],
                            "native_parent_id": item["native_parent_id"],
                            "artifact_id": item["artifact"]["artifact_id"],
                            "job_id": item["job_id"],
                            "kind": item["event"]["kind"],
                            "content_sha256": item["event_sha256"],
                            "revision": item["revision"],
                            "occurred_at": item["event"]["occurred_at"],
                            "observed_at": item["event"]["observed_at"],
                            "canonical_redacted": item["event"],
                        }
                        for item in live_rows
                    ]
                    connection.execute(
                        """INSERT INTO canonical_events(
                               tenant_id,source_id,event_id,native_id,native_parent_id,
                               artifact_id,job_id,kind,content_sha256,revision,
                               occurred_at,observed_at,is_tombstone,canonical_redacted
                           )
                           SELECT %s,%s,row.event_id,row.native_id,
                                  row.native_parent_id,row.artifact_id,row.job_id,
                                  row.kind,row.content_sha256,row.revision,
                                  row.occurred_at,row.observed_at,false,
                                  row.canonical_redacted
                           FROM jsonb_to_recordset(%s::jsonb) AS row(
                               event_id text,native_id text,native_parent_id text,
                               artifact_id text,job_id text,kind text,
                               content_sha256 char(64),revision integer,
                               occurred_at timestamptz,observed_at timestamptz,
                               canonical_redacted jsonb
                           )""",
                        (
                            tenant_id,
                            source_id,
                            json.dumps(event_rows),
                        ),
                    )
                    document_rows = [
                        {
                            "document_id": item["document_id"],
                            "event_id": item["event_id"],
                            "artifact_id": item["artifact"]["artifact_id"],
                            "native_id": item["native_id"],
                            "content_sha256": item["event_sha256"],
                            "revision": item["revision"],
                            "text_redacted": item["text_redacted"],
                            "text_sha256": item["text_sha256"],
                        }
                        for item in live_rows
                    ]
                    connection.execute(
                        """INSERT INTO canonical_documents(
                               tenant_id,source_id,document_id,event_id,artifact_id,
                               native_id,content_sha256,revision,is_current,
                               text_redacted,text_sha256
                           )
                           SELECT %s,%s,row.document_id,row.event_id,row.artifact_id,
                                  row.native_id,row.content_sha256,row.revision,true,
                                  row.text_redacted,row.text_sha256
                           FROM jsonb_to_recordset(%s::jsonb) AS row(
                               document_id text,event_id text,artifact_id text,
                               native_id text,content_sha256 char(64),
                               revision integer,text_redacted text,
                               text_sha256 char(64)
                           )""",
                        (
                            tenant_id,
                            source_id,
                            json.dumps(document_rows),
                        ),
                    )
                    chunk_rows = [
                        {
                            "chunk_id": _opaque(
                                "chk",
                                tenant_id,
                                source_id,
                                item["document_id"],
                                str(ordinal),
                            ),
                            "document_id": item["document_id"],
                            "ordinal": ordinal,
                            "receipt": (
                                f"recall://{source_id}/{item['native_id']}"
                                f"?rev={item['revision']}#item={ordinal}"
                            ),
                            "text_redacted": chunk_text,
                            "text_sha256": hashlib.sha256(
                                chunk_text.encode()
                            ).hexdigest(),
                        }
                        for item in live_rows
                        for ordinal, chunk_text in enumerate(item["chunks"])
                    ]
                    connection.execute(
                        """INSERT INTO canonical_chunks(
                               tenant_id,source_id,chunk_id,document_id,ordinal,
                               receipt,text_redacted,text_sha256
                           )
                           SELECT %s,%s,row.chunk_id,row.document_id,row.ordinal,
                                  row.receipt,row.text_redacted,row.text_sha256
                           FROM jsonb_to_recordset(%s::jsonb) AS row(
                               chunk_id text,document_id text,ordinal integer,
                               receipt text,text_redacted text,text_sha256 char(64)
                           )""",
                        (
                            tenant_id,
                            source_id,
                            json.dumps(chunk_rows),
                        ),
                    )
                    audit_rows = [
                        {
                            "audit_id": _opaque(
                                "audit",
                                tenant_id,
                                source_id,
                                item["event_id"],
                                "ingest",
                            ),
                            "subject_sha256": item["identity_sha256"],
                            "byte_count": item["artifact"]["size_bytes"],
                        }
                        for item in live_rows
                    ]
                    connection.execute(
                        """INSERT INTO canonical_audit_events(
                               tenant_id,source_id,audit_id,operation,status,
                               subject_sha256,item_count,byte_count
                           )
                           SELECT %s,%s,row.audit_id,'ingest.commit','success',
                                  row.subject_sha256,1,row.byte_count
                           FROM jsonb_to_recordset(%s::jsonb) AS row(
                               audit_id text,subject_sha256 char(64),
                               byte_count bigint
                           )""",
                        (
                            tenant_id,
                            source_id,
                            json.dumps(audit_rows),
                        ),
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

    def _archive_references(
        self,
        conn: Any,
        *,
        tenant_id: str,
        source_id: str,
        native_ids: list[str],
    ) -> list[dict[str, Any]]:
        rows = conn.execute(
            """SELECT DISTINCT artifact.tenant_id,artifact.source_id,
                       artifact.artifact_id,artifact.storage_backend,artifact.object_key,
                       artifact.content_sha256,artifact.size_bytes,artifact.media_type,
                       artifact.encryption,artifact.version_id,artifact.created_at
               FROM canonical_events event
               JOIN raw_artifacts artifact
                 USING(tenant_id,source_id,artifact_id)
               WHERE event.tenant_id=%s AND event.source_id=%s
                 AND event.native_id=ANY(%s)
                 AND NOT EXISTS (
                     SELECT 1 FROM canonical_events retained
                     WHERE retained.tenant_id=event.tenant_id
                       AND retained.source_id=event.source_id
                       AND retained.artifact_id=event.artifact_id
                       AND NOT (retained.native_id=ANY(%s))
                 )
               ORDER BY artifact.artifact_id""",
            (tenant_id, source_id, native_ids, native_ids),
        ).fetchall()
        return [
            {
                "contract": "recall.artifact-ref.v1",
                "schema_version": 1,
                **{
                    key: row[key]
                    for key in (
                        "tenant_id", "source_id", "artifact_id", "storage_backend",
                        "object_key", "content_sha256", "size_bytes", "media_type",
                        "encryption", "version_id",
                    )
                },
                "created_at": _timestamp(row["created_at"]),
            }
            for row in rows
        ]

    def forget(self, request: dict[str, Any]) -> dict[str, Any]:
        try:
            value = validate_contract(request, expected="recall.forget-request.v1")
        except ContractError:
            raise CanonicalLifecycleError("forget_contract_invalid") from None
        tenant_id = value["tenant_id"]
        principal_id = value["principal_id"]
        source_id = value["source_id"]
        native_id = _native_from_receipt(value["target_receipt"], source_id)
        target_sha256 = _identity_sha256(tenant_id, source_id, native_id)
        reason = value["reason"]

        with self.store.connect() as conn:
            with conn.transaction():
                owner = conn.execute(
                    """SELECT owner_principal_id FROM canonical_sources
                       WHERE tenant_id=%s AND source_id=%s""",
                    (tenant_id, source_id),
                ).fetchone()
                if owner is None or owner["owner_principal_id"] != principal_id:
                    raise CanonicalLifecycleError("forget_authority_forbidden")
                conn.execute(
                    """SELECT pg_advisory_xact_lock(hashtextextended(%s,0))""",
                    (f"v2\x1f{tenant_id}\x1f{source_id}\x1f{native_id}",),
                )
                prior = conn.execute(
                    """SELECT target_identity_sha256,status
                       FROM forget_tombstones
                       WHERE tenant_id=%s AND source_id=%s AND idempotency_key=%s""",
                    (tenant_id, source_id, value["idempotency_key"]),
                ).fetchone()
                if prior and prior["target_identity_sha256"] != target_sha256:
                    raise CanonicalLifecycleError("forget_idempotency_conflict")
                tombstone = conn.execute(
                    """SELECT status FROM forget_tombstones
                       WHERE tenant_id=%s AND source_id=%s
                         AND target_identity_sha256=%s
                       FOR UPDATE""",
                    (tenant_id, source_id, target_sha256),
                ).fetchone()
                if tombstone and tombstone["status"] == "deleted":
                    return {
                        "status": "deleted",
                        "replay": True,
                        "raw_deleted": 0,
                        "projections_deleted": 0,
                        "evidence_deleted": 0,
                    }
                target = conn.execute(
                    """SELECT document.native_id
                       FROM canonical_chunks chunk
                       JOIN canonical_documents document
                         USING(tenant_id,source_id,document_id)
                       WHERE chunk.tenant_id=%s AND chunk.source_id=%s
                         AND chunk.receipt=%s""",
                    (tenant_id, source_id, value["target_receipt"]),
                ).fetchone()
                if target is None or target["native_id"] != native_id:
                    raise CanonicalLifecycleError("forget_target_not_found")
                native_ids = _linked_native_ids(
                    conn,
                    tenant_id=tenant_id,
                    source_id=source_id,
                    native_id=native_id,
                )
                identity_hashes = [
                    _identity_sha256(tenant_id, source_id, candidate)
                    for candidate in native_ids
                ]
                for candidate in native_ids[1:]:
                    conn.execute(
                        "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                        (f"v2\x1f{tenant_id}\x1f{source_id}\x1f{candidate}",),
                    )
                if tombstone is None:
                    conn.execute(
                        """INSERT INTO forget_tombstones(
                               tenant_id,source_id,target_identity_sha256,mode,reason,
                               deleted_at,status,idempotency_key
                           ) VALUES (%s,%s,%s,%s,%s,%s,'deleting',%s)""",
                        (
                            tenant_id, source_id, target_sha256, value["mode"], reason,
                            value["requested_at"], value["idempotency_key"],
                        ),
                    )
                for child_hash in identity_hashes[1:]:
                    conn.execute(
                        """INSERT INTO forget_tombstones(
                               tenant_id,source_id,target_identity_sha256,mode,reason,
                               deleted_at,status,idempotency_key
                           ) VALUES (%s,%s,%s,%s,%s,%s,'deleting',NULL)
                           ON CONFLICT(tenant_id,source_id,target_identity_sha256)
                           DO NOTHING""",
                        (
                            tenant_id, source_id, child_hash, value["mode"], reason,
                            value["requested_at"],
                        ),
                    )
                references = self._archive_references(
                    conn,
                    tenant_id=tenant_id,
                    source_id=source_id,
                    native_ids=native_ids,
                )
                projection_count = conn.execute(
                    """SELECT count(*) AS count
                       FROM canonical_chunks chunk
                       JOIN canonical_documents document
                         USING(tenant_id,source_id,document_id)
                       WHERE document.tenant_id=%s AND document.source_id=%s
                         AND document.native_id=ANY(%s) AND chunk.deleted_at IS NULL""",
                    (tenant_id, source_id, native_ids),
                ).fetchone()["count"]
                conn.execute(
                    """UPDATE canonical_chunks chunk
                       SET deleted_at=COALESCE(chunk.deleted_at,%s)
                       FROM canonical_documents document
                       WHERE chunk.tenant_id=document.tenant_id
                         AND chunk.source_id=document.source_id
                         AND chunk.document_id=document.document_id
                         AND document.tenant_id=%s AND document.source_id=%s
                         AND document.native_id=ANY(%s)""",
                    (value["requested_at"], tenant_id, source_id, native_ids),
                )
                conn.execute(
                    """UPDATE canonical_documents
                       SET is_current=false,
                           deleted_at=COALESCE(deleted_at,%s)
                       WHERE tenant_id=%s AND source_id=%s AND native_id=ANY(%s)""",
                    (value["requested_at"], tenant_id, source_id, native_ids),
                )
                conn.execute(
                    """UPDATE raw_artifacts artifact
                       SET state='deleting'
                       FROM canonical_events event
                       WHERE artifact.tenant_id=event.tenant_id
                         AND artifact.source_id=event.source_id
                         AND artifact.artifact_id=event.artifact_id
                         AND event.tenant_id=%s AND event.source_id=%s
                         AND event.native_id=ANY(%s) AND artifact.state='live'
                         AND NOT EXISTS (
                             SELECT 1 FROM canonical_events retained
                             WHERE retained.tenant_id=event.tenant_id
                               AND retained.source_id=event.source_id
                               AND retained.artifact_id=event.artifact_id
                               AND NOT (retained.native_id=ANY(%s))
                         )""",
                    (tenant_id, source_id, native_ids, native_ids),
                )

        evidence_deleted = 0
        try:
            for reference in references:
                self.archive.delete_raw(reference)
            if self.evidence_projector is not None:
                evidence_deleted = self.evidence_projector.delete_native_ids(
                    tenant_id=tenant_id,
                    source_id=source_id,
                    native_ids=native_ids,
                )
        except Exception:
            with self.store.connect() as conn:
                conn.execute(
                    """INSERT INTO canonical_audit_events(
                           tenant_id,source_id,audit_id,operation,status,
                           subject_sha256,item_count
                       ) VALUES (%s,%s,%s,'forget.archive','failed',%s,%s)
                       ON CONFLICT DO NOTHING""",
                    (
                        tenant_id, source_id,
                        _opaque(
                            "audit", tenant_id, source_id, target_sha256,
                            value["idempotency_key"], "failed",
                        ),
                        target_sha256, len(references),
                    ),
                )
            raise CanonicalLifecycleError("archive_delete_failed") from None

        with self.store.connect() as conn:
            with conn.transaction():
                conn.execute(
                    """DELETE FROM canonical_chunks chunk
                       USING canonical_documents document
                       WHERE chunk.tenant_id=document.tenant_id
                         AND chunk.source_id=document.source_id
                         AND chunk.document_id=document.document_id
                         AND document.tenant_id=%s AND document.source_id=%s
                         AND document.native_id=ANY(%s)""",
                    (tenant_id, source_id, native_ids),
                )
                conn.execute(
                    """DELETE FROM canonical_documents
                       WHERE tenant_id=%s AND source_id=%s AND native_id=ANY(%s)""",
                    (tenant_id, source_id, native_ids),
                )
                conn.execute(
                    """DELETE FROM canonical_events
                       WHERE tenant_id=%s AND source_id=%s AND native_id=ANY(%s)""",
                    (tenant_id, source_id, native_ids),
                )
                artifact_ids = [reference["artifact_id"] for reference in references]
                if artifact_ids:
                    conn.execute(
                        """DELETE FROM raw_artifacts
                           WHERE tenant_id=%s AND source_id=%s
                             AND artifact_id=ANY(%s)""",
                        (tenant_id, source_id, artifact_ids),
                    )
                conn.execute(
                    """UPDATE forget_tombstones
                       SET status='deleted',completed_at=now()
                       WHERE tenant_id=%s AND source_id=%s
                         AND target_identity_sha256=ANY(%s)""",
                    (tenant_id, source_id, identity_hashes),
                )
                conn.execute(
                    """INSERT INTO canonical_audit_events(
                           tenant_id,source_id,audit_id,operation,status,
                           subject_sha256,item_count,byte_count
                       ) VALUES (%s,%s,%s,'forget.commit','success',%s,%s,%s)""",
                    (
                        tenant_id, source_id,
                        _opaque(
                            "audit", tenant_id, source_id, target_sha256,
                            value["idempotency_key"], "success",
                        ),
                        target_sha256, projection_count,
                        sum(reference["size_bytes"] for reference in references),
                    ),
                )
        return {
            "status": "deleted",
            "replay": False,
            "raw_deleted": len(references),
            "projections_deleted": projection_count,
            "evidence_deleted": evidence_deleted,
        }
