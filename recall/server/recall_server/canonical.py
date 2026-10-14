from __future__ import annotations

import hashlib
import json
from contextlib import ExitStack, contextmanager, nullcontext
from datetime import datetime, timezone
from typing import Any, Protocol
from urllib.parse import unquote, urlsplit

from contracts.v2 import ContractError, IDENTITY_RE, validate_contract

from .actor_attribution import ActorIdentityIndex, attribute_canonical_events
from .canonical_history import (
    HistoryAuthorityError, HistoryUnavailable, prepare_history, restore_outgoing,
)
from .db import BrainStore
from .archive_ownership import admit_references
from .canonical_thinning import _compact_event_expression
from .identity_cache import REGISTRATION_CACHE, IdentityRegistrationCache
from .canonical_text import (
    MAX_CANONICAL_CHUNK_BYTES as MAX_CANONICAL_CHUNK_BYTES,
    MAX_CANONICAL_TEXT_BYTES,
    canonical_text_chunks,
)
from .logical_evidence_projection import (
    OVERSIZED_MEDIA_TYPE,
    mark_logical_evidence_dirty,
    mark_logical_notification_pending,
)
from .projectors import canonical_json, item_receipt, validate_envelope

MAX_LINKED_IDENTITIES = 10_000
MAX_CANONICAL_BATCH_EVENTS = 1_000
OVERSIZED_CONTRACT = "recall.oversized-projection.v1"
_SHA256_HEX = 64


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
    def verify_raw_bounded(self, reference: dict[str, Any], *, deadline_at: float) -> None: ...
    def delete_internal_raw_bounded(self, reference: dict[str, Any], *, deadline_at: float) -> bool: ...


def _validate_oversized_pointer(event: dict[str, Any], artifact: dict[str, Any]) -> None:
    """Refuse an oversized pointer whose declared archive facts contradict its artifact.

    The pointer is the only description of the archived full record that the
    logical projector ever sees, and nothing downstream can repair a pointer
    that never matched. The artifact reference in the same envelope is
    content-addressed and already verified by the archive gateway, so any
    disagreement between the two is a producer fault and is rejected here,
    where the collector receives the error, instead of poisoning the logical
    projection queue days later.
    """
    content = event.get("content")
    if event.get("kind") == "tombstone" or not isinstance(content, dict):
        return
    if content.get("contract") != OVERSIZED_CONTRACT:
        return
    full_size = content.get("full_size_bytes")
    digest = content.get("full_content_sha256")
    archive_size = content.get("archive_size_bytes")
    if (
        artifact.get("media_type") != OVERSIZED_MEDIA_TYPE
        or content.get("full_record_available") is not True
        or content.get("archive_encoding") != "gzip"
        or isinstance(full_size, bool)
        or not isinstance(full_size, int)
        or full_size < 1
        or not isinstance(digest, str)
        or len(digest) != _SHA256_HEX
        or any(character not in "0123456789abcdef" for character in digest)
        or (
            archive_size is not None
            and (
                isinstance(archive_size, bool)
                or not isinstance(archive_size, int)
                or archive_size != artifact.get("size_bytes")
            )
        )
    ):
        raise CanonicalLifecycleError("canonical_oversized_pointer_invalid")


def _identity_sha256(tenant_id: str, source_id: str, native_id: str) -> str:
    return hashlib.sha256(
        "\x1f".join((tenant_id, source_id, native_id)).encode()
    ).hexdigest()


def _opaque(prefix: str, *values: str) -> str:
    digest = hashlib.sha256("\x1f".join(values).encode()).hexdigest()
    return f"{prefix}_{digest[:32]}"


def _source_ordinal(event: dict[str, Any]) -> int | None:
    provenance = event.get("provenance")
    value = provenance.get("byte_start") if isinstance(provenance, dict) else None
    return value if type(value) is int and -(2**63) <= value < 2**63 else None


def _lineage_job_id(tenant_id: str, source_id: str, connector_id: str) -> str:
    # Legacy per-event jobs use an evt_* final component; the namespaces are disjoint.
    return _opaque("job", tenant_id, source_id, connector_id, "connector-lineage-v1")


def _ensure_lineage_jobs(connection, *, tenant_id: str, source_id: str, connector_ids) -> None:
    # This existing FK target records connector lineage, not execution attempts.
    # Its timestamps describe registration and never track an individual event.
    connectors = set(connector_ids)
    jobs = {_lineage_job_id(tenant_id, source_id, connector): connector
            for connector in connectors}
    if len(jobs) != len(connectors):
        raise CanonicalLifecycleError("canonical_job_conflict")
    if not jobs:
        return
    connection.execute(
        """INSERT INTO canonical_ingest_jobs(
               tenant_id,source_id,job_id,connector_id,mode,status,attempt)
           SELECT %s,%s,row.job_id,row.connector_id,'incremental','committed',0
           FROM jsonb_to_recordset(%s::jsonb) AS row(job_id text,connector_id text)
           ORDER BY row.job_id
           ON CONFLICT(tenant_id,source_id,job_id) DO NOTHING""",
        (tenant_id, source_id, json.dumps([
            {"job_id": job, "connector_id": connector} for job, connector in jobs.items()
        ])),
    )
    # A separate statement sees a concurrent creator after its INSERT commits.
    # Shared locks allow concurrent admissions and prevent metadata changing mid-write.
    rows = connection.execute(
        """SELECT job_id,connector_id,mode,status,attempt,created_at,updated_at
           FROM canonical_ingest_jobs
           WHERE tenant_id=%s AND source_id=%s AND job_id=ANY(%s)
           ORDER BY job_id FOR SHARE""",
        (tenant_id, source_id, list(jobs)),
    ).fetchall()
    if len(rows) != len(jobs) or {row["job_id"] for row in rows} != set(jobs) or any(
        row["connector_id"] != jobs[row["job_id"]]
        or row["mode"] != "incremental" or row["status"] != "committed"
        or row["attempt"] != 0 or row["created_at"] != row["updated_at"]
        for row in rows
    ):
        raise CanonicalLifecycleError("canonical_job_conflict")


def _linked_native_ids(
    conn: Any,
    *,
    tenant_id: str,
    source_id: str,
    native_id: str,
) -> list[str]:
    rows = conn.execute(
        """WITH RECURSIVE linked(native_id) AS (
               -- Children are looked up through the expression index on
               -- COALESCE(native_parent_id, native_id) (session_order_idx);
               -- a bare native_parent_id predicate has no index and scanned
               -- every event of the source (12 GB, 518 s on the largest one).
               SELECT DISTINCT native_id
               FROM canonical_events
               WHERE tenant_id=%s AND source_id=%s
                 AND COALESCE(native_parent_id,native_id)=%s
                 AND native_parent_id IS NOT NULL
               UNION
               SELECT event.native_id
               FROM canonical_events event
               JOIN linked parent
                 ON COALESCE(event.native_parent_id,event.native_id)
                    =parent.native_id
               WHERE event.tenant_id=%s AND event.source_id=%s
                 AND event.native_parent_id IS NOT NULL
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


def _linked_native_ids_batch(
    conn: Any,
    *,
    tenant_id: str,
    source_id: str,
    native_ids: list[str],
) -> list[str]:
    """Resolve tombstone descendants for many roots in one database round trip."""
    rows = conn.execute(
        """WITH RECURSIVE roots(root_native_id) AS (
               SELECT DISTINCT value FROM unnest(%s::text[]) AS item(value)
           ), linked(root_native_id,native_id) AS (
               SELECT root.root_native_id,event.native_id
               FROM roots root
               JOIN canonical_events event
                 ON event.tenant_id=%s AND event.source_id=%s
                AND COALESCE(event.native_parent_id,event.native_id)
                    =root.root_native_id
                AND event.native_parent_id IS NOT NULL
               UNION
               SELECT parent.root_native_id,event.native_id
               FROM canonical_events event
               JOIN linked parent
                 ON COALESCE(event.native_parent_id,event.native_id)
                    =parent.native_id
               WHERE event.tenant_id=%s AND event.source_id=%s
                 AND event.native_parent_id IS NOT NULL
           ), bounded AS (
               SELECT root_native_id,native_id,
                      row_number() OVER (
                          PARTITION BY root_native_id ORDER BY native_id
                      ) AS ordinal
               FROM linked
           )
           SELECT root_native_id,native_id,ordinal
           FROM bounded
           WHERE ordinal <= %s
           ORDER BY root_native_id,ordinal""",
        (
            native_ids,
            tenant_id,
            source_id,
            tenant_id,
            source_id,
            MAX_LINKED_IDENTITIES + 1,
        ),
    ).fetchall()
    if any(int(row["ordinal"]) > MAX_LINKED_IDENTITIES for row in rows):
        raise CanonicalLifecycleError("canonical_lineage_limit")
    return sorted({*native_ids, *(row["native_id"] for row in rows)})


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
            tenant_id,
            principal_id,
            "source:placeholder",
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
        require_existing_owner: bool = False,
    ) -> dict[str, Any]:
        if tenant_id != self.tenant_id:
            raise CanonicalLifecycleError("archive_authority_forbidden")
        CanonicalPlane._validate_host_identity(
            tenant_id,
            self.principal_id,
            source_id,
        )
        if not isinstance(native_id, str) or not IDENTITY_RE.fullmatch(native_id):
            raise CanonicalLifecycleError("archive_identity_invalid")
        identity_sha256 = _identity_sha256(tenant_id, source_id, native_id)
        with self.store.connect() as conn:
            with conn.transaction():
                if require_existing_owner:
                    CanonicalPlane._require_existing_source_owner(
                        conn, tenant_id=tenant_id, principal_id=self.principal_id,
                        source_id=source_id)
                else:
                    CanonicalPlane.register_source(
                        conn, tenant_id=tenant_id, principal_id=self.principal_id,
                        source_id=source_id)
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


def _slack_terminal_ack(source_id: str, native_id: str, revision: int) -> dict[str, Any]:
    # duplicate_events includes an already-terminal native identity. The subset
    # diagnostic distinguishes it from an exact content-hash replay.
    return {'status': 'committed', 'inserted': 0, 'duplicate_events': 1,
            'suppressed_deleted_events': 1, 'revision': revision,
            'receipt': f'recall://{source_id}/{native_id}?rev={revision}#item=0', 'replay': True}


def _lock_artifacts(connection, tenant_id: str, source_id: str, artifact_ids, *, wait=True) -> bool:
    keys = sorted({f"v2-artifact\x1f{tenant_id}\x1f{source_id}\x1f{artifact}" for artifact in artifact_ids})
    if not keys:
        return True
    if wait:
        connection.execute("""SELECT pg_advisory_xact_lock(hashtextextended(value,0))
            FROM unnest(%s::text[]) AS keys(value) ORDER BY value""", (keys,)).fetchall()
        return True
    rows = connection.execute("""SELECT pg_try_advisory_xact_lock(hashtextextended(value,0)) AS locked
        FROM unnest(%s::text[]) AS keys(value) ORDER BY value""", (keys,)).fetchall()
    return all(row['locked'] for row in rows)


def _artifact_fence_available(connection) -> bool:
    return connection.execute(
        "SELECT to_regclass('public.canonical_artifact_forget_tombstones') IS NOT NULL AS present"
    ).fetchone()["present"]


def _guard_artifact_admission(connection, tenant_id: str, source_id: str, artifact_ids) -> None:
    _lock_artifacts(connection, tenant_id, source_id, artifact_ids)
    if _artifact_fence_available(connection) and connection.execute(
        """SELECT 1 FROM canonical_artifact_forget_tombstones
           WHERE tenant_id=%s AND source_id=%s AND artifact_id=ANY(%s) LIMIT 1""",
        (tenant_id, source_id, list(artifact_ids)),
    ).fetchone():
        raise CanonicalLifecycleError("canonical_artifact_forgotten")


def _suppressed_ownership_available(connection) -> bool:
    return connection.execute(
        "SELECT to_regclass('public.canonical_suppressed_artifact_owners') IS NOT NULL AS present"
    ).fetchone()["present"]


def _audit_slack_suppression(connection, *, tenant_id, source_id, rows):
    if not rows:
        return
    if not _suppressed_ownership_available(connection):
        raise CanonicalLifecycleError("slack_deletion_schema_required")
    connection.execute("""INSERT INTO canonical_suppressed_artifact_owners(
        tenant_id,source_id,identity_sha256,artifact_id)
        SELECT %s,%s,row.identity_sha256,row.artifact_id
        FROM jsonb_to_recordset(%s::jsonb) AS row(identity_sha256 char(64),artifact_id text)
        ON CONFLICT DO NOTHING""", (tenant_id, source_id, json.dumps([
            {"identity_sha256": identity, "artifact_id": artifact_id}
            for event_id, identity, size, artifact_id in rows
        ])))
    connection.execute('''INSERT INTO canonical_audit_events(
        tenant_id,source_id,audit_id,operation,status,subject_sha256,item_count,byte_count)
        SELECT %s,%s,row.audit_id,'ingest.slack_suppressed_deleted','success',
               row.subject_sha256,1,row.byte_count
        FROM jsonb_to_recordset(%s::jsonb) AS row(
            audit_id text,subject_sha256 char(64),byte_count bigint)
        ON CONFLICT(tenant_id,source_id,audit_id) DO NOTHING''',
        (tenant_id, source_id, json.dumps([
            {'audit_id': _opaque('audit', tenant_id, source_id, event_id, 'slack-terminal'),
             'subject_sha256': identity, 'byte_count': size}
            for event_id, identity, size, artifact_id in rows
        ])))


def _filesystem_cleanup(event, connector_id=None):
    """Recognize the two transcript connectors, never an explicit forget."""
    if not isinstance(event, dict):
        return False
    provenance = event.get("provenance")
    return event.get("kind") == "tombstone" and (
        connector_id in ("claude.jsonl", "codex.jsonl")
        or (isinstance(provenance, dict)
            and provenance.get("connector_id") in ("claude.jsonl", "codex.jsonl"))
    )


def _reject_filesystem_deletion(event, connector_id=None):
    if _filesystem_cleanup(event, connector_id):
        raise CanonicalLifecycleError("canonical_filesystem_deletion_forbidden")


class CanonicalPlane:
    """Tenant-aware v2 canonical ingest and crash-resumable authoritative forget."""

    def __init__(
        self,
        store: BrainStore,
        archive: ArchiveLifecycle,
        evidence_projector: Any = None,
        actor_identity_index: ActorIdentityIndex | None = None,
        *,
        chunk_body_archive: Any = None,
    ):
        self.store = store
        self.archive = archive
        self.evidence_projector = evidence_projector
        self.actor_identity_index = actor_identity_index
        self.chunk_body_archive = chunk_body_archive

    @contextmanager
    def prepare_history(self, *, tenant_id, events, principal_id=None):
        candidates = []
        for envelope in events:
            try:
                event = validate_envelope(envelope)
            except ValueError:
                raise CanonicalLifecycleError("canonical_contract_invalid") from None
            owner = event["principal_id"] if principal_id is None else principal_id
            self._validate_host_identity(tenant_id, owner, event["source_id"])
            if owner != event["principal_id"]:
                raise CanonicalLifecycleError("canonical_lineage_invalid")
            if event["kind"] != "tombstone":
                candidates.append({key: event[key] for key in
                                   ("source_id", "native_id", "content_sha256", "principal_id")})
        try:
            with prepare_history(self.store, self.chunk_body_archive,
                                 tenant_id=tenant_id, candidates=candidates) as staged:
                yield staged
        except HistoryAuthorityError:
            raise CanonicalLifecycleError("canonical_authority_forbidden") from None
        except HistoryUnavailable:
            raise CanonicalLifecycleError("canonical_history_unavailable") from None

    def _restore_history(self, connection, staged, *, tenant_id, source_id, native_ids):
        try:
            restore_outgoing(connection, staged, tenant_id=tenant_id,
                             source_id=source_id, native_ids=native_ids, store=self.store)
        except HistoryUnavailable:
            raise CanonicalLifecycleError("canonical_history_unavailable") from None

    def authorize_filesystem_cleanup(self, connection, *, tenant_id, principal_id,
                                     event, connector_id=None):
        """Validate the marker and lock its existing owner grant without creating it."""
        try:
            event = validate_envelope(event)
        except ValueError:
            raise CanonicalLifecycleError('canonical_contract_invalid') from None
        self._validate_host_identity(tenant_id, principal_id, event.get('source_id'))
        if event['principal_id'] != principal_id or not _filesystem_cleanup(event, connector_id):
            raise CanonicalLifecycleError('canonical_lineage_invalid')
        self._require_existing_source_owner(connection, tenant_id=tenant_id,
            principal_id=principal_id, source_id=event['source_id'])
        return event

    @staticmethod
    def _require_existing_source_owner(connection, *, tenant_id, principal_id, source_id):
        owner = connection.execute('''SELECT source.owner_principal_id
            FROM canonical_sources source JOIN canonical_source_grants grant_row
              USING(tenant_id,source_id)
            WHERE source.tenant_id=%s AND source.source_id=%s
              AND source.owner_principal_id=%s AND grant_row.principal_id=%s
              AND grant_row.permission='owner'
            FOR SHARE OF source,grant_row''',
            (tenant_id, source_id, principal_id, principal_id)).fetchone()
        if owner is None:
            raise CanonicalLifecycleError('canonical_authority_forbidden')

    def suppress_filesystem_cleanup(self, connection, *, tenant_id, principal_id,
                                    event, connector_id=None, artifact=None):
        """Record a policy audit, not a document or deletion receipt.

        The legacy bridge uses this before upload, including after explicit forget.
        Canonical requests call it after their existing artifact/forget checks.
        The caller owns the transaction; returning here is not a COMMIT ACK.
        """
        event = self.authorize_filesystem_cleanup(connection, tenant_id=tenant_id,
            principal_id=principal_id, event=event, connector_id=connector_id)
        source_id, native_id = event['source_id'], event['native_id']
        connection.execute('SELECT pg_advisory_xact_lock(hashtextextended(%s,0))',
                           (f'v2\x1f{tenant_id}\x1f{source_id}\x1f{native_id}',))
        audit_event = {**event, 'provenance': {
            key: value for key, value in event.get('provenance', {}).items()
            if key != 'artifact_ref'}}
        encoded = canonical_json(audit_event)
        audit_id = _opaque('audit', tenant_id, source_id, hashlib.sha256(encoded).hexdigest(),
                           'filesystem-cleanup-suppressed')
        identity = _identity_sha256(tenant_id, source_id, native_id)
        if artifact is not None:
            if not _suppressed_ownership_available(connection):
                raise CanonicalLifecycleError('canonical_suppression_schema_required')
            connection.execute('''INSERT INTO canonical_suppressed_artifact_owners(
                tenant_id,source_id,identity_sha256,artifact_id) VALUES (%s,%s,%s,%s)
                ON CONFLICT DO NOTHING''', (tenant_id, source_id, identity, artifact['artifact_id']))
        inserted = connection.execute('''INSERT INTO canonical_audit_events(
            tenant_id,source_id,audit_id,operation,status,subject_sha256,item_count,byte_count)
            VALUES (%s,%s,%s,'ingest.filesystem_cleanup_suppressed','success',%s,1,%s)
            ON CONFLICT(tenant_id,source_id,audit_id) DO NOTHING RETURNING audit_id''',
            (tenant_id, source_id, audit_id, identity, len(encoded))).fetchone()
        return dict(status='committed', inserted=0, duplicate_events=int(inserted is None),
                    suppressed_filesystem_events=1, receipt_kind='policy_audit',
                    receipt=f'recall-audit://{source_id}/{audit_id}', replay=inserted is None)

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
    def register_source(
        conn: Any,
        *,
        tenant_id: str,
        principal_id: str,
        source_id: str,
        cache: IdentityRegistrationCache | None = None,
    ) -> None:
        """Register the identity tuple, or skip every statement on a cache hit.

        A hit means an earlier call on this process saw the source row already
        committed with this owner, so the ownership check and the idempotent
        upserts are known to hold. Membership mutations invalidate the tenant;
        see ``identity_cache`` for the safety argument.
        """
        registration_cache = REGISTRATION_CACHE if cache is None else cache
        key = (tenant_id, principal_id, source_id)
        if registration_cache.contains(key):
            return
        conn.execute(
            "INSERT INTO brain_tenants(tenant_id) VALUES (%s) ON CONFLICT DO NOTHING",
            (tenant_id,),
        )
        conn.execute(
            """INSERT INTO brain_principals(tenant_id,principal_id)
               VALUES (%s,%s) ON CONFLICT DO NOTHING""",
            (tenant_id, principal_id),
        )
        inserted_source = conn.execute(
            """INSERT INTO canonical_sources(tenant_id,source_id,owner_principal_id)
               VALUES (%s,%s,%s) ON CONFLICT DO NOTHING""",
            (tenant_id, source_id, principal_id),
        )
        # Only a source that already existed before this transaction is safe
        # to remember: a fresh insert may still roll back with the caller.
        source_preexisted = getattr(inserted_source, "rowcount", None) == 0
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
                   JOIN brain_memberships reader_membership
                     ON reader_membership.organization_id=space.organization_id
                    AND reader_membership.principal_id=access.principal_id
                    AND reader_membership.role IN ('owner','admin','member')
                  WHERE space.tenant_id=%s AND space.brain_kind='company'
                    AND (
                        (
                            access.permission='owner'
                            AND reader_membership.role='owner'
                        )
                        OR EXISTS (
                            SELECT 1 FROM brain_invitations invitation
                             WHERE invitation.tenant_id=space.tenant_id
                               AND invitation.accepted_principal_id=
                                   access.principal_id
                               AND invitation.accepted_at IS NOT NULL
                               AND invitation.revoked_at IS NULL
                        )
                    )
               ON CONFLICT(tenant_id,principal_id,source_id)
               DO UPDATE SET permission=CASE
                   WHEN canonical_source_grants.permission='owner' THEN 'owner'
                   ELSE 'read'
               END""",
            (source_id, principal_id, tenant_id),
        )
        if source_preexisted:
            registration_cache.remember(key)

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
        _history: Any = None,
        _notification: bool = False,
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
                artifact_ref,
                expected="recall.artifact-ref.v1",
            )
            event = validate_envelope(envelope)
        except (ContractError, ValueError):
            raise CanonicalLifecycleError("canonical_contract_invalid") from None
        source_id = event["source_id"]
        if (
            artifact["tenant_id"] != tenant_id
            or artifact["source_id"] != source_id
            or event["principal_id"] != principal_id
            or event.get("provenance", {}).get("artifact_ref") != artifact
        ):
            raise CanonicalLifecycleError("canonical_lineage_invalid")
        _validate_oversized_pointer(event, artifact)
        connection_context = (
            self.store.connect() if _connection is None else nullcontext(_connection)
        )
        with ExitStack() as stack:
            history = _history
            if _connection is None:
                history = stack.enter_context(self.prepare_history(
                    tenant_id=tenant_id, principal_id=principal_id, events=[event]))
            conn = stack.enter_context(connection_context)
            transaction_context = (
                conn.transaction() if _connection is None else nullcontext()
            )
            with transaction_context:
                if _filesystem_cleanup(event, connector_id):
                    # Do not let register_source recreate a revoked owner grant.
                    self.authorize_filesystem_cleanup(conn, tenant_id=tenant_id,
                        principal_id=principal_id, event=event, connector_id=connector_id)
                    return self._write_authorized_document(
                        conn, tenant_id=tenant_id, connector_id=connector_id,
                        artifact=artifact, event=event, text_redacted=text_redacted)
                self.register_source(
                    conn,
                    tenant_id=tenant_id,
                    principal_id=principal_id,
                    source_id=source_id,
                )
                return self._write_authorized_document(
                    conn, tenant_id=tenant_id, connector_id=connector_id,
                    artifact=artifact, event=event, text_redacted=text_redacted,
                    history=history, notification=_notification,
                )

    def _write_authorized_document(
        self, conn, *, tenant_id, connector_id, artifact, event,
        text_redacted, history=None, notification=False,
    ):
        """Write inside a caller-owned authorized transaction; never commits.

        Public ingestion validates caller attribution and registers the source.
        The explicit historical correction owner independently locks company
        admin/source authority and exact obsolete history before entering here.
        """
        source_id, native_id = event["source_id"], event["native_id"]
        is_tombstone = event["kind"] == "tombstone"
        raw_sha256 = artifact["content_sha256"]
        event_sha256 = event["content_sha256"]
        identity_sha256 = _identity_sha256(tenant_id, source_id, native_id)
        event_id = _opaque("evt", tenant_id, source_id, native_id, event_sha256)
        job_id = _lineage_job_id(tenant_id, source_id, connector_id)
        document_id = _opaque("doc", tenant_id, source_id, event_id)
        text_sha256 = hashlib.sha256(text_redacted.encode()).hexdigest()
        chunk_texts = [] if is_tombstone else canonical_text_chunks(text_redacted)
        # The live raw reference is checked below before INSERT. Keep original
        # chunks in this same transaction; logical publication still reads them.
        compact_body = not is_tombstone and artifact["storage_backend"] == "s3"

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
            raise CanonicalLifecycleError("canonical_identity_forgotten")
        admit_references(conn, self.archive, [artifact])
        conn.execute(
            """INSERT INTO raw_artifacts(
                           tenant_id,source_id,artifact_id,storage_backend,object_key,
                           content_sha256,size_bytes,media_type,encryption,version_id,created_at
                       ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                       ON CONFLICT(tenant_id,source_id,artifact_id) DO NOTHING""",
            (
                tenant_id,
                source_id,
                artifact["artifact_id"],
                artifact["storage_backend"],
                artifact["object_key"],
                raw_sha256,
                artifact["size_bytes"],
                artifact["media_type"],
                artifact["encryption"],
                artifact["version_id"],
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
                "storage_backend",
                "object_key",
                "content_sha256",
                "size_bytes",
                "media_type",
                "encryption",
                "version_id",
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
        if _filesystem_cleanup(event, connector_id):
            return self.suppress_filesystem_cleanup(
                conn, tenant_id=tenant_id, principal_id=event['principal_id'],
                event=event, connector_id=connector_id, artifact=artifact)
        existing = conn.execute(
            """SELECT revision FROM canonical_events
                       WHERE tenant_id=%s AND source_id=%s
                         AND native_id=%s AND content_sha256=%s""",
            (tenant_id, source_id, native_id, event_sha256),
        ).fetchone()
        if existing:
            revision = existing["revision"]
            inserted_actor_links = attribute_canonical_events(
                conn,
                tenant_id=tenant_id,
                source_id=source_id,
                events=((event_id, connector_id, event),),
                identity_index=self.actor_identity_index,
            )
            if inserted_actor_links:
                mark_logical_evidence_dirty(
                    conn,
                    tenant_id=tenant_id,
                    source_id=source_id,
                    native_ids=[native_id],
                    reason="ingest",
                )
            if notification:
                mark_logical_notification_pending(
                    conn, tenant_id=tenant_id, source_id=source_id, event_id=event_id,
                )
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
        if connector_id == "slack.messages" and native_id.startswith("slack:") and not is_tombstone:
            terminal = conn.execute(
                """SELECT max(revision) AS revision FROM canonical_events
                           WHERE tenant_id=%s AND source_id=%s AND native_id=%s AND is_tombstone""",
                (tenant_id, source_id, native_id),
            ).fetchone()["revision"]
            if terminal is not None:
                _audit_slack_suppression(conn, tenant_id=tenant_id, source_id=source_id,
                    rows=[(event_id, identity_sha256, artifact["size_bytes"], artifact["artifact_id"])])
                return _slack_terminal_ack(source_id, native_id, int(terminal))
        row = conn.execute(
            """SELECT COALESCE(max(revision),0)+1 AS revision
                       FROM canonical_events
                       WHERE tenant_id=%s AND source_id=%s AND native_id=%s""",
            (tenant_id, source_id, native_id),
        ).fetchone()
        revision = row["revision"]
        receipt = f"recall://{source_id}/{native_id}?rev={revision}#item=0"
        _ensure_lineage_jobs(conn, tenant_id=tenant_id, source_id=source_id,
                             connector_ids=[connector_id])
        conn.execute(
            f"""INSERT INTO canonical_events(
                           tenant_id,source_id,event_id,native_id,native_parent_id,
                           artifact_id,job_id,kind,content_sha256,revision,
                           occurred_at,observed_at,is_tombstone,canonical_redacted,
                           source_ordinal,body_location
                       ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                           (SELECT CASE WHEN %s THEN {_compact_event_expression('input')}
                                   ELSE input.canonical_redacted END
                            FROM (VALUES (%s::jsonb)) input(canonical_redacted)),%s,%s)""",
            (
                tenant_id,
                source_id,
                event_id,
                native_id,
                event.get("native_parent_id"),
                artifact["artifact_id"],
                job_id,
                event["kind"],
                event_sha256,
                revision,
                event["occurred_at"],
                event["observed_at"],
                is_tombstone,
                compact_body,
                json.dumps(event),
                _source_ordinal(event),
                "raw" if compact_body else "inline",
            ),
        )
        attribute_canonical_events(
            conn,
            tenant_id=tenant_id,
            source_id=source_id,
            events=((event_id, connector_id, event),),
            identity_index=self.actor_identity_index,
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
        if not is_tombstone:
            self._restore_history(conn, history, tenant_id=tenant_id,
                                  source_id=source_id, native_ids=affected_native_ids)
        conn.execute(
            """UPDATE canonical_documents
                       SET is_current=false,
                           text_redacted=CASE WHEN %s THEN '' ELSE text_redacted END,
                           deleted_at=CASE WHEN %s THEN now() ELSE deleted_at END
                       WHERE tenant_id=%s AND source_id=%s AND native_id=ANY(%s)
                         AND is_current""",
            (is_tombstone, is_tombstone, tenant_id, source_id, affected_native_ids),
        )
        if is_tombstone:
            conn.execute(
                """UPDATE canonical_chunks chunk
                           SET deleted_at=now(),
                               text_redacted=CASE
                                   WHEN document.deleted_at IS NOT NULL AND NOT document.is_current
                                   THEN '' ELSE chunk.text_redacted END
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
                               content_sha256,revision,is_current,text_redacted,text_sha256,body_location
                           ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,true,%s,%s,%s)""",
                (
                    tenant_id,
                    source_id,
                    document_id,
                    event_id,
                    artifact["artifact_id"],
                    native_id,
                    event_sha256,
                    revision,
                    "" if compact_body else text_redacted,
                    text_sha256,
                    "chunks" if compact_body else "inline",
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
                            item_receipt(
                                source_id,
                                native_id,
                                revision,
                                ordinal,
                            ),
                            chunk_text,
                            hashlib.sha256(chunk_text.encode()).hexdigest(),
                        )
                        for ordinal, chunk_text in enumerate(chunk_texts)
                    ],
                )
        mark_logical_evidence_dirty(
            conn,
            tenant_id=tenant_id,
            source_id=source_id,
            native_ids=affected_native_ids,
            reason="ingest",
            new_native_ids=(native_id,) if revision == 1 and not is_tombstone else (),
        )
        if notification:
            mark_logical_notification_pending(
                conn, tenant_id=tenant_id, source_id=source_id, event_id=event_id,
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
            len({event.get("kind") == "tombstone" for event in events}) == 1
            and not any(_filesystem_cleanup(event) for event in events)
            and len({event.get("native_id") for event in events}) == len(events)
            and len({event.get("source_id") for event in events}) == 1
        ):
            return self._ingest_set_batch(
                tenant_id=tenant_id,
                principal_id=principal_id,
                events=events,
            )
        results = []
        with ExitStack() as stack:
            history = stack.enter_context(self.prepare_history(
                tenant_id=tenant_id, principal_id=principal_id, events=events))
            connection = stack.enter_context(self.store.connect())
            with connection.transaction():
                # Lock cleanup authority before any ordinary member can register
                # a source, regardless of input order or registration-cache state.
                for envelope in events:
                    if _filesystem_cleanup(envelope):
                        self.authorize_filesystem_cleanup(connection, tenant_id=tenant_id,
                            principal_id=principal_id, event=envelope)
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
                    results.append(
                        self.ingest_document(
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
                            _history=history,
                        )
                    )
        return {
            "status": "committed",
            "inserted": sum(result["inserted"] for result in results),
            "duplicate_events": sum(result["duplicate_events"] for result in results),
            "receipts": [result["receipt"] for result in results],
            "replay": all(result["replay"] for result in results),
            **({"suppressed_filesystem_events": sum(result.get("suppressed_filesystem_events", 0) for result in results)}
               if any(result.get("suppressed_filesystem_events") for result in results) else {}),
            **({"suppressed_deleted_events": sum(result.get("suppressed_deleted_events", 0) for result in results)}
               if any(result.get("suppressed_deleted_events") for result in results) else {}),
        }

    def source_status(
        self,
        *,
        tenant_id: str,
        principal_id: str,
        source_id: str,
    ) -> dict[str, Any]:
        """Return content-free corpus and projection parity for one source."""
        self._validate_host_identity(tenant_id, principal_id, source_id)
        # H3-e': the passage-embedding parity columns exist only on the
        # postgres plane; on turbopuffer the table is retired and both read 0.
        if getattr(self.store, "search_plane", "postgres") == "turbopuffer":
            embedding_parity_sql = (
                "0::bigint AS passage_embeddings,"
                "0::bigint AS missing_passage_embeddings"
            )
            embedding_parity_params: tuple[str, ...] = ()
        else:
            embedding_parity_sql = """(SELECT count(*) FROM canonical_passage_embeddings
                       WHERE tenant_id=%s AND source_id=%s) AS passage_embeddings,
                     (SELECT count(*)
                        FROM canonical_passages passage
                        LEFT JOIN canonical_passage_embeddings embedding
                          ON embedding.tenant_id=passage.tenant_id
                         AND embedding.source_id=passage.source_id
                         AND embedding.passage_id=passage.passage_id
                         AND embedding.content_sha256=passage.text_sha256
                       WHERE passage.tenant_id=%s AND passage.source_id=%s
                         AND embedding.passage_id IS NULL)
                       AS missing_passage_embeddings"""
            embedding_parity_params = (tenant_id, source_id, tenant_id, source_id)
        with self.store.connect() as connection:
            owner = connection.execute(
                """SELECT owner_principal_id
                   FROM canonical_sources
                   WHERE tenant_id=%s AND source_id=%s""",
                (tenant_id, source_id),
            ).fetchone()
            if owner is None or owner["owner_principal_id"] != principal_id:
                raise CanonicalLifecycleError("canonical_authority_forbidden")
            row = connection.execute(
                """WITH latest AS (
                       SELECT DISTINCT ON (native_id)
                              native_id,native_parent_id,revision,is_tombstone
                       FROM canonical_events
                       WHERE tenant_id=%s AND source_id=%s
                       ORDER BY native_id DESC,revision DESC
                   ), expected AS (
                       SELECT native_id,native_parent_id,revision
                       FROM latest WHERE NOT is_tombstone
                   ), current_documents AS (
                       SELECT document.native_id,document.revision,
                              document.document_id,event.native_parent_id,
                              document.text_redacted,document.body_location
                       FROM canonical_documents document
                       JOIN canonical_events event
                         ON event.tenant_id=document.tenant_id
                        AND event.source_id=document.source_id
                        AND event.event_id=document.event_id
                       WHERE document.tenant_id=%s
                         AND document.source_id=%s
                         AND document.is_current
                         AND document.deleted_at IS NULL
                   ), expected_parents AS (
                       SELECT DISTINCT native_parent_id
                       FROM current_documents
                       WHERE native_parent_id IS NOT NULL
                   )
                   SELECT
                     (SELECT count(*) FROM latest) AS latest_events,
                     (SELECT count(*) FROM expected) AS live_events,
                     (SELECT count(*) FROM latest WHERE is_tombstone)
                       AS tombstoned_identities,
                     (SELECT count(*) FROM current_documents)
                       AS current_documents,
                     (SELECT count(*)
                        FROM expected item
                        LEFT JOIN current_documents document
                          ON document.native_id=item.native_id
                         AND document.revision=item.revision
                       WHERE document.native_id IS NULL)
                       AS missing_current_documents,
                     (SELECT count(*)
                        FROM current_documents document
                        LEFT JOIN expected item
                          ON item.native_id=document.native_id
                         AND item.revision=document.revision
                       WHERE item.native_id IS NULL)
                       AS stale_current_documents,
                     (SELECT count(*) FROM current_documents
                       WHERE text_redacted=''
                         AND body_location='inline') AS empty_current_documents,
                     (SELECT count(*) FROM current_documents
                       WHERE body_location='chunks') AS chunk_backed_documents,
                     (SELECT count(*) FROM expected_parents)
                       AS logical_documents_expected,
                     (SELECT count(*) FROM canonical_evidence_documents
                       WHERE tenant_id=%s AND source_id=%s)
                       AS logical_documents,
                     (SELECT count(*)
                        FROM expected_parents parent
                        LEFT JOIN canonical_evidence_documents document
                          ON document.tenant_id=%s
                         AND document.source_id=%s
                         AND document.native_parent_id=parent.native_parent_id
                       WHERE document.logical_document_id IS NULL)
                       AS missing_logical_documents,
                     (SELECT count(*)
                        FROM canonical_evidence_documents document
                        LEFT JOIN expected_parents parent
                          ON parent.native_parent_id=document.native_parent_id
                       WHERE document.tenant_id=%s AND document.source_id=%s
                         AND parent.native_parent_id IS NULL)
                       AS stale_logical_documents,
                     (SELECT count(*) FROM canonical_evidence_document_parts
                       WHERE tenant_id=%s AND source_id=%s)
                       AS logical_document_parts,
                     (SELECT count(*) FROM canonical_evidence_document_queue
                       WHERE tenant_id=%s AND source_id=%s)
                       AS logical_queue,
                     (SELECT count(*) FROM canonical_evidence_cleanup_queue
                       WHERE tenant_id=%s AND source_id=%s)
                       AS cleanup_queue,
                     (SELECT count(*) FROM canonical_passage_documents
                       WHERE tenant_id=%s AND source_id=%s)
                       AS passage_documents,
                     (SELECT count(*)
                        FROM canonical_evidence_documents document
                        LEFT JOIN canonical_passage_documents passage_document
                          ON passage_document.tenant_id=document.tenant_id
                         AND passage_document.source_id=document.source_id
                         AND passage_document.logical_document_id=
                             document.logical_document_id
                       WHERE document.tenant_id=%s AND document.source_id=%s
                         AND passage_document.logical_document_id IS NULL)
                       AS missing_passage_documents,
                     (SELECT count(*) FROM canonical_passage_projection_queue
                       WHERE tenant_id=%s AND source_id=%s)
                       AS passage_queue,
                     (SELECT count(*) FROM canonical_passages
                       WHERE tenant_id=%s AND source_id=%s) AS passages,
                     """ + embedding_parity_sql + """""",
                (
                    tenant_id,
                    source_id,
                    tenant_id,
                    source_id,
                    tenant_id,
                    source_id,
                    tenant_id,
                    source_id,
                    tenant_id,
                    source_id,
                    tenant_id,
                    source_id,
                    tenant_id,
                    source_id,
                    tenant_id,
                    source_id,
                    tenant_id,
                    source_id,
                    tenant_id,
                    source_id,
                    tenant_id,
                    source_id,
                    tenant_id,
                    source_id,
                    *embedding_parity_params,
                ),
            ).fetchone()
        return {
            "status": "ok",
            "tenant_id": tenant_id,
            "source_id": source_id,
            **{key: int(value) for key, value in row.items()},
        }

    def _ingest_set_batch(
        self,
        *,
        tenant_id: str,
        principal_id: str,
        events: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """Commit one homogeneous event batch with set-based remote SQL.

        Duplicate native identities intentionally stay on the lineage-aware
        sequential path. Homogeneous tombstone batches resolve descendants in
        one recursive query. The path preserves the same contracts, locks,
        revisions, artifact checks, receipts, and exceptional audit records.
        """
        prepared: list[dict[str, Any]] = []
        artifacts: dict[str, dict[str, Any]] = {}
        source_ids: set[str] = set()
        tombstone_modes: set[bool] = set()
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
            is_tombstone = event["kind"] == "tombstone"
            tombstone_modes.add(is_tombstone)
            if (
                artifact["tenant_id"] != tenant_id
                or artifact["source_id"] != source_id
                or event["principal_id"] != principal_id
                or event.get("provenance", {}).get("artifact_ref") != artifact
            ):
                raise CanonicalLifecycleError("canonical_lineage_invalid")
            _reject_filesystem_deletion(event, connector_id)
            _validate_oversized_pointer(event, artifact)
            text_redacted = (
                ""
                if is_tombstone
                else json.dumps(
                    event.get("content"),
                    sort_keys=True,
                    separators=(",", ":"),
                    ensure_ascii=False,
                    allow_nan=False,
                )
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
                    "job_id": _lineage_job_id(tenant_id, source_id, connector_id),
                    "document_id": _opaque(
                        "doc",
                        tenant_id,
                        source_id,
                        event_id,
                    ),
                    "text_redacted": text_redacted,
                    "text_sha256": hashlib.sha256(text_redacted.encode()).hexdigest(),
                    "chunks": (
                        [] if is_tombstone else canonical_text_chunks(text_redacted)
                    ),
                    "is_tombstone": is_tombstone,
                }
            )
        if len(source_ids) != 1 or len(tombstone_modes) != 1:
            raise CanonicalLifecycleError("canonical_batch_invalid")
        source_id = next(iter(source_ids))
        is_tombstone_batch = next(iter(tombstone_modes))
        native_ids = [item["native_id"] for item in prepared]
        lock_keys = sorted(
            f"v2\x1f{tenant_id}\x1f{source_id}\x1f{native_id}"
            for native_id in native_ids
        )

        with ExitStack() as stack:
            history = stack.enter_context(self.prepare_history(
                tenant_id=tenant_id, principal_id=principal_id, events=events))
            connection = stack.enter_context(self.store.connect())
            with connection.transaction():
                self.register_source(
                    connection,
                    tenant_id=tenant_id,
                    principal_id=principal_id,
                    source_id=source_id,
                )
                connection.execute(
                    """SELECT pg_advisory_xact_lock(hashtextextended(value,0))
                       FROM unnest(%s::text[]) AS lock_key(value)
                       ORDER BY value""",
                    (lock_keys,),
                ).fetchall()

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

                admit_references(connection, self.archive, artifacts.values())
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
                stored_by_id = {row["artifact_id"]: row for row in stored_artifacts}
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
                    """SELECT native_id,content_sha256,revision,is_tombstone
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
                terminal_revisions: dict[str, int] = {}
                for row in prior_events:
                    if row["is_tombstone"]:
                        terminal_revisions[row["native_id"]] = max(
                            terminal_revisions.get(row["native_id"], 0), int(row["revision"]))
                    max_revision[row["native_id"]] = max(
                        max_revision.get(row["native_id"], 0),
                        int(row["revision"]),
                    )
                new_rows: list[dict[str, Any]] = []
                suppressed = []
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
                                    f"recall://{source_id}/{item['native_id']}?rev={revision}#item=0"
                                ),
                                "replay": True,
                            }
                        )
                        continue
                    terminal = terminal_revisions.get(item["native_id"])
                    if (terminal is not None and item["connector_id"] == "slack.messages"
                            and item["native_id"].startswith("slack:") and not item["is_tombstone"]):
                        suppressed.append((item["event_id"], item["identity_sha256"], item["artifact"]["size_bytes"], item["artifact"]["artifact_id"]))
                        results.append(_slack_terminal_ack(source_id, item["native_id"], terminal))
                        continue
                    revision = max_revision.get(item["native_id"], 0) + 1
                    item["revision"] = revision
                    item["receipt"] = (
                        f"recall://{source_id}/{item['native_id']}?rev={revision}#item=0"
                    )
                    new_rows.append(item)
                    results.append(
                        {
                            "inserted": 1,
                            "duplicate_events": 0,
                            "revision": revision,
                            "receipt": item["receipt"],
                            "replay": False,
                        }
                    )

                _audit_slack_suppression(connection, tenant_id=tenant_id, source_id=source_id, rows=suppressed)
                if new_rows:
                    affected_native_ids = [item["native_id"] for item in new_rows]
                    if is_tombstone_batch:
                        # Avoid compilation overhead for recursive lineage lookup;
                        # the setting expires with this ingest transaction.
                        connection.execute("SET LOCAL jit = off")
                        affected_native_ids = _linked_native_ids_batch(
                            connection,
                            tenant_id=tenant_id,
                            source_id=source_id,
                            native_ids=affected_native_ids,
                        )
                    if not is_tombstone_batch:
                        self._restore_history(connection, history, tenant_id=tenant_id,
                                              source_id=source_id, native_ids=affected_native_ids)
                    connection.execute(
                        """UPDATE canonical_documents
                           SET is_current=false,
                               text_redacted=CASE WHEN %s THEN '' ELSE text_redacted END,
                               deleted_at=CASE
                                   WHEN %s THEN now() ELSE deleted_at
                               END
                           WHERE tenant_id=%s AND source_id=%s
                             AND native_id=ANY(%s) AND is_current""",
                        (
                            is_tombstone_batch,
                            is_tombstone_batch,
                            tenant_id,
                            source_id,
                            affected_native_ids,
                        ),
                    )
                    if is_tombstone_batch:
                        connection.execute(
                            """UPDATE canonical_chunks chunk
                               SET deleted_at=now(),
                                   text_redacted=CASE
                                       WHEN document.deleted_at IS NOT NULL AND NOT document.is_current
                                       THEN '' ELSE chunk.text_redacted END
                               FROM canonical_documents document
                               WHERE chunk.tenant_id=document.tenant_id
                                 AND chunk.source_id=document.source_id
                                 AND chunk.document_id=document.document_id
                                 AND document.tenant_id=%s
                                 AND document.source_id=%s
                                 AND document.native_id=ANY(%s)
                                 AND chunk.deleted_at IS NULL""",
                            (tenant_id, source_id, affected_native_ids),
                        )
                    _ensure_lineage_jobs(
                        connection, tenant_id=tenant_id, source_id=source_id,
                        connector_ids=(item["connector_id"] for item in new_rows),
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
                            "is_tombstone": item["is_tombstone"],
                            "canonical_redacted": item["event"],
                            "source_ordinal": _source_ordinal(item["event"]),
                            "body_location": ("raw" if not item["is_tombstone"]
                                and item["artifact"]["storage_backend"] == "s3" else "inline"),
                        }
                        for item in new_rows
                    ]
                    connection.execute(
                        f"""INSERT INTO canonical_events(
                               tenant_id,source_id,event_id,native_id,native_parent_id,
                               artifact_id,job_id,kind,content_sha256,revision,
                               occurred_at,observed_at,is_tombstone,canonical_redacted,
                               source_ordinal,body_location
                           )
                           SELECT %s,%s,row.event_id,row.native_id,
                                  row.native_parent_id,row.artifact_id,row.job_id,
                                  row.kind,row.content_sha256,row.revision,
                                  row.occurred_at,row.observed_at,row.is_tombstone,
                                  CASE WHEN row.body_location='raw'
                                       THEN {_compact_event_expression('row')}
                                       ELSE row.canonical_redacted END,
                                  row.source_ordinal,row.body_location
                           FROM jsonb_to_recordset(%s::jsonb) AS row(
                               event_id text,native_id text,native_parent_id text,
                               artifact_id text,job_id text,kind text,
                               content_sha256 char(64),revision integer,
                               occurred_at timestamptz,observed_at timestamptz,
                               is_tombstone boolean,
                               canonical_redacted jsonb,source_ordinal bigint,body_location text
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
                            "text_redacted": ("" if item["artifact"]["storage_backend"] == "s3"
                                              else item["text_redacted"]),
                            "text_sha256": item["text_sha256"],
                            "body_location": ("chunks" if item["artifact"]["storage_backend"] == "s3"
                                              else "inline"),
                        }
                        for item in new_rows
                        if not item["is_tombstone"]
                    ]
                    if document_rows:
                        connection.execute(
                            """INSERT INTO canonical_documents(
                               tenant_id,source_id,document_id,event_id,artifact_id,
                               native_id,content_sha256,revision,is_current,
                               text_redacted,text_sha256,body_location
                           )
                           SELECT %s,%s,row.document_id,row.event_id,row.artifact_id,
                                  row.native_id,row.content_sha256,row.revision,true,
                                  row.text_redacted,row.text_sha256,row.body_location
                           FROM jsonb_to_recordset(%s::jsonb) AS row(
                               document_id text,event_id text,artifact_id text,
                               native_id text,content_sha256 char(64),
                               revision integer,text_redacted text,
                               text_sha256 char(64),body_location text
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
                            "receipt": item_receipt(
                                source_id,
                                item["native_id"],
                                item["revision"],
                                ordinal,
                            ),
                            "text_redacted": chunk_text,
                            "text_sha256": hashlib.sha256(
                                chunk_text.encode()
                            ).hexdigest(),
                        }
                        for item in new_rows
                        if not item["is_tombstone"]
                        for ordinal, chunk_text in enumerate(item["chunks"])
                    ]
                    if chunk_rows:
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
                    mark_logical_evidence_dirty(
                        connection,
                        tenant_id=tenant_id,
                        source_id=source_id,
                        native_ids=affected_native_ids,
                        reason="ingest",
                        new_native_ids=tuple(item["native_id"] for item in new_rows
                                             if item["revision"] == 1 and not item["is_tombstone"]),
                    )
                inserted_actor_links = attribute_canonical_events(
                    connection,
                    tenant_id=tenant_id,
                    source_id=source_id,
                    events=(
                        (item["event_id"], item["connector_id"], item["event"])
                        for item, result in zip(prepared, results, strict=True)
                        if not result.get("suppressed_deleted_events")
                    ),
                    identity_index=self.actor_identity_index,
                )
                duplicate_native_ids = [
                    item["native_id"]
                    for item, result in zip(prepared, results, strict=True)
                    if result["duplicate_events"] and not result.get("suppressed_deleted_events")
                ]
                if inserted_actor_links and duplicate_native_ids:
                    mark_logical_evidence_dirty(
                        connection,
                        tenant_id=tenant_id,
                        source_id=source_id,
                        native_ids=duplicate_native_ids,
                        reason="ingest",
                    )
        return {
            "status": "committed",
            "inserted": sum(result["inserted"] for result in results),
            "duplicate_events": sum(result["duplicate_events"] for result in results),
            "receipts": [result["receipt"] for result in results],
            "replay": all(result["replay"] for result in results),
            **({"suppressed_deleted_events": sum(result.get("suppressed_deleted_events", 0) for result in results)}
               if any(result.get("suppressed_deleted_events") for result in results) else {}),
        }

    def _archive_references(
        self,
        conn: Any,
        *,
        tenant_id: str,
        source_id: str,
        native_ids: list[str],
    ) -> list[dict[str, Any]]:
        identities = [_identity_sha256(tenant_id, source_id, native) for native in native_ids]
        owners = _suppressed_ownership_available(conn)
        suppression_candidates = """UNION SELECT artifact_id
            FROM canonical_suppressed_artifact_owners
            WHERE tenant_id=%(tenant)s AND source_id=%(source)s
              AND identity_sha256=ANY(%(identities)s)""" if owners else ""
        suppression_retained = """AND NOT EXISTS (
            SELECT 1 FROM canonical_suppressed_artifact_owners retained
            WHERE retained.tenant_id=artifact.tenant_id AND retained.source_id=artifact.source_id
              AND retained.artifact_id=artifact.artifact_id
              AND NOT(retained.identity_sha256=ANY(%(identities)s))
              AND NOT EXISTS(SELECT 1 FROM forget_tombstones forgotten
                WHERE forgotten.tenant_id=retained.tenant_id AND forgotten.source_id=retained.source_id
                  AND forgotten.target_identity_sha256=retained.identity_sha256))""" if owners else ""
        candidates = conn.execute(f"""SELECT DISTINCT artifact_id FROM canonical_events
            WHERE tenant_id=%(tenant)s AND source_id=%(source)s AND native_id=ANY(%(natives)s)
            {suppression_candidates}""", {"tenant": tenant_id, "source": source_id,
                                          "natives": native_ids, "identities": identities}).fetchall()
        _lock_artifacts(conn, tenant_id, source_id, [row["artifact_id"] for row in candidates])
        rows = conn.execute(
            f"""WITH candidates AS (
                SELECT DISTINCT artifact_id FROM canonical_events
                WHERE tenant_id=%(tenant)s AND source_id=%(source)s AND native_id=ANY(%(natives)s)
                {suppression_candidates})
            SELECT artifact.tenant_id,artifact.source_id,artifact.artifact_id,
                   artifact.storage_backend,artifact.object_key,artifact.content_sha256,
                   artifact.size_bytes,artifact.media_type,artifact.encryption,
                   artifact.version_id,artifact.created_at
            FROM raw_artifacts artifact JOIN candidates USING(artifact_id)
            WHERE artifact.tenant_id=%(tenant)s AND artifact.source_id=%(source)s
              AND NOT EXISTS (
                SELECT 1 FROM canonical_events retained
                WHERE retained.tenant_id=artifact.tenant_id AND retained.source_id=artifact.source_id
                  AND retained.artifact_id=artifact.artifact_id
                  AND NOT(retained.native_id=ANY(%(natives)s))
                  AND NOT EXISTS(SELECT 1 FROM forget_tombstones forgotten
                    WHERE forgotten.tenant_id=retained.tenant_id AND forgotten.source_id=retained.source_id
                      AND forgotten.target_identity_sha256=encode(sha256(convert_to(
                        retained.tenant_id || chr(31) || retained.source_id || chr(31) || retained.native_id,
                        'UTF8')),'hex')))
              {suppression_retained}
            ORDER BY artifact.artifact_id""",
            {"tenant": tenant_id, "source": source_id, "natives": native_ids, "identities": identities},
        ).fetchall()
        return [
            {
                "contract": "recall.artifact-ref.v1",
                "schema_version": 1,
                **{
                    key: row[key]
                    for key in (
                        "tenant_id",
                        "source_id",
                        "artifact_id",
                        "storage_backend",
                        "object_key",
                        "content_sha256",
                        "size_bytes",
                        "media_type",
                        "encryption",
                        "version_id",
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
                if target is None and getattr(self.store, 'verified_singleton_receipts', False):
                    from .receipt_membership import lookup_receipt_metadata

                    target = lookup_receipt_metadata(self.store, tenant_id=tenant_id,
                        authorized_sources=(source_id,), receipts=(value['target_receipt'],),
                        allow_verified_singletons=True, current_only=False,
                        connection=conn, include_deleted=True).get(value['target_receipt'])
                if target is None and native_id.startswith("slack:"):
                    target = conn.execute(
                        """SELECT event.native_id FROM canonical_events event
                           JOIN canonical_ingest_jobs job USING(tenant_id,source_id,job_id)
                           WHERE event.tenant_id=%s AND event.source_id=%s AND event.native_id=%s
                             AND event.is_tombstone AND job.connector_id='slack.messages'
                             AND 'recall://' || event.source_id || '/' || event.native_id ||
                                 '?rev=' || event.revision || '#item=0'=%s""",
                        (tenant_id, source_id, native_id, value["target_receipt"]),
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
                            tenant_id,
                            source_id,
                            target_sha256,
                            value["mode"],
                            reason,
                            value["requested_at"],
                            value["idempotency_key"],
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
                            tenant_id,
                            source_id,
                            child_hash,
                            value["mode"],
                            reason,
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
                if getattr(self.store, 'verified_singleton_receipts', False):
                    from .receipt_membership import lookup_receipt_metadata

                    admitted = conn.execute('''SELECT native_id,revision FROM canonical_documents
                        WHERE tenant_id=%s AND source_id=%s AND native_id=ANY(%s)
                          AND chunk_receipt_profile IS NOT NULL AND deleted_at IS NULL''',
                        (tenant_id, source_id, native_ids)).fetchall()
                    receipts = tuple(item_receipt(source_id, row['native_id'], row['revision'], 0)
                                     for row in admitted)
                    projection_count += len(lookup_receipt_metadata(self.store, tenant_id=tenant_id,
                        authorized_sources=(source_id,), receipts=receipts,
                        allow_verified_singletons=True, current_only=False, connection=conn))
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
                if _artifact_fence_available(conn):
                    conn.execute("""INSERT INTO canonical_artifact_forget_tombstones(
                        tenant_id,source_id,artifact_id)
                        SELECT %s,%s,value FROM unnest(%s::text[]) AS ids(value)
                        ON CONFLICT DO NOTHING""",
                        (tenant_id, source_id, [reference["artifact_id"] for reference in references]))
                conn.execute(
                    """UPDATE raw_artifacts SET state='deleting'
                       WHERE tenant_id=%s AND source_id=%s AND artifact_id=ANY(%s) AND state='live'""",
                    (tenant_id, source_id, [reference["artifact_id"] for reference in references]),
                )
                mark_logical_evidence_dirty(
                    conn,
                    tenant_id=tenant_id,
                    source_id=source_id,
                    native_ids=native_ids,
                    reason="forget",
                )

        evidence_deleted = 0
        try:
            for reference in references:
                # The first forget transaction revoked these raw owners. Other
                # archive catalogues can still own the same immutable object.
                from .archive_ownership import delete_persisted_reference, live_references, reference_key
                with self.store.connect() as connection:
                    _lock_artifacts(connection, tenant_id, source_id, [reference["artifact_id"]])
                    if reference_key(reference) not in live_references(connection, [reference]):
                        delete_persisted_reference(self.archive, reference)
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
                        tenant_id,
                        source_id,
                        _opaque(
                            "audit",
                            tenant_id,
                            source_id,
                            target_sha256,
                            value["idempotency_key"],
                            "failed",
                        ),
                        target_sha256,
                        len(references),
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
                forgotten_events = conn.execute(
                    """DELETE FROM canonical_events
                       WHERE tenant_id=%s AND source_id=%s AND native_id=ANY(%s)
                       RETURNING COALESCE(native_parent_id,native_id) AS native_parent_id""",
                    (tenant_id, source_id, native_ids),
                ).fetchall()
                forgotten_parents = sorted({row["native_parent_id"] for row in forgotten_events})
                if forgotten_parents:
                    # Catalog removal retains operator intent across partial forget.
                    # Drop that metadata only after this owner has removed the last
                    # event and catalog; permanent forget tombstones still deny replay.
                    conn.execute(
                        """DELETE FROM canonical_chunk_retirement_progress progress
                           WHERE progress.tenant_id=%s AND progress.source_id=%s
                             AND progress.native_parent_id=ANY(%s)
                             AND progress.logical_document_id IS NULL AND NOT progress.enabled
                             AND NOT EXISTS (
                                 SELECT 1 FROM canonical_events event
                                 WHERE event.tenant_id=progress.tenant_id
                                   AND event.source_id=progress.source_id
                                   AND COALESCE(event.native_parent_id,event.native_id)=progress.native_parent_id)
                             AND NOT EXISTS (
                                 SELECT 1 FROM canonical_evidence_documents evidence
                                 WHERE evidence.tenant_id=progress.tenant_id
                                   AND evidence.source_id=progress.source_id
                                   AND evidence.native_parent_id=progress.native_parent_id)""",
                        (tenant_id, source_id, forgotten_parents),
                    )
                if _suppressed_ownership_available(conn):
                    conn.execute("""DELETE FROM canonical_suppressed_artifact_owners
                        WHERE tenant_id=%s AND source_id=%s AND identity_sha256=ANY(%s)""",
                        (tenant_id, source_id, identity_hashes))
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
                        tenant_id,
                        source_id,
                        _opaque(
                            "audit",
                            tenant_id,
                            source_id,
                            target_sha256,
                            value["idempotency_key"],
                            "success",
                        ),
                        target_sha256,
                        projection_count,
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
