"""Thin redundant canonical bodies after immutable object projection succeeds."""

from __future__ import annotations

from typing import Any

from .db import BrainStore


def _compact_event_expression(alias: str = "event") -> str:
    """Retain only routing metadata used after the full body moves to raw storage."""

    def structural(path: str) -> str:
        return f"jsonb_strip_nulls(jsonb_build_object('role',{alias}.canonical_redacted #> '{{{path},role}}','type',{alias}.canonical_redacted #> '{{{path},type}}'))"

    content_metadata = (
        "jsonb_strip_nulls(jsonb_build_object("
        f"'role',{alias}.canonical_redacted #> '{{content,role}}',"
        f"'type',{alias}.canonical_redacted #> '{{content,type}}',"
        f"'message',{structural('content,message')},"
        f"'payload',{structural('content,payload')}))"
    )
    oversized_pointer = (
        "jsonb_strip_nulls(jsonb_build_object("
        f"'contract',{alias}.canonical_redacted #> '{{content,contract}}',"
        f"'schema_version',{alias}.canonical_redacted #> '{{content,schema_version}}',"
        f"'full_record_available',{alias}.canonical_redacted #> "
        "'{content,full_record_available}',"
        f"'full_content_sha256',{alias}.canonical_redacted #> "
        "'{content,full_content_sha256}',"
        f"'full_size_bytes',{alias}.canonical_redacted #> "
        "'{content,full_size_bytes}',"
        f"'archive_encoding',{alias}.canonical_redacted #> "
        "'{content,archive_encoding}'))"
    )
    content = (
        f"CASE WHEN {alias}.canonical_redacted #>> '{{content,contract}}'="
        "'recall.oversized-projection.v1' "
        f"THEN {oversized_pointer} ELSE {content_metadata} END"
    )
    provenance = (
        "jsonb_strip_nulls(jsonb_build_object("
        f"'connector_id',{alias}.canonical_redacted #> '{{provenance,connector_id}}',"
        f"'connector_schema_version',{alias}.canonical_redacted #> "
        "'{provenance,connector_schema_version}',"
        f"'collector_version',{alias}.canonical_redacted #> "
        "'{provenance,collector_version}',"
        f"'privacy_policy_version',{alias}.canonical_redacted #> "
        "'{provenance,privacy_policy_version}',"
        f"'harness',{alias}.canonical_redacted #> '{{provenance,harness}}',"
        f"'cwd',{alias}.canonical_redacted #> '{{provenance,cwd}}',"
        f"'branch',{alias}.canonical_redacted #> '{{provenance,branch}}',"
        f"'slot',{alias}.canonical_redacted #> '{{provenance,slot}}',"
        f"'byte_start',{alias}.canonical_redacted #> '{{provenance,byte_start}}',"
        f"'byte_end',{alias}.canonical_redacted #> '{{provenance,byte_end}}'))"
    )
    return (
        "jsonb_strip_nulls(jsonb_build_object("
        f"'role',{alias}.canonical_redacted->'role',"
        f"'type',{alias}.canonical_redacted->'type',"
        f"'provenance',{provenance},"
        f"'content',{content},"
        f"'message',{structural('message')},"
        f"'payload',{structural('payload')}))"
    )


def _thinning_statement(*, bounded: bool = False, probe: bool = False) -> str:
    """One authority/update owner for standalone and bounded worker selection."""
    compact_event = _compact_event_expression()
    key_filter = document_filter = event_filter = ""
    document_relation = "canonical_documents document"
    event_relation = "canonical_events event"
    artifact_relation = "raw_artifacts artifact"
    if bounded and probe:
        # These unlocked identity lookups stay correlated to enumerated keys.
        # The final mutation below uses plain base relations so its row locks
        # remain after the original ORDER/LIMIT, rather than being pushed here.
        document_relation = """unnest(%s::text[],%s::text[])
                             AS scope(key_source,key_document)
                           CROSS JOIN LATERAL (
                             SELECT d.* FROM canonical_documents d
                              WHERE d.tenant_id=%s
                                AND d.source_id=scope.key_source
                                AND d.document_id=scope.key_document
                                AND d.body_location='inline'
                                AND d.deleted_at IS NULL
                              OFFSET 0
                           ) document"""
        event_relation = """LATERAL (
                             SELECT e.* FROM canonical_events e
                              WHERE e.tenant_id=document.tenant_id
                                AND e.source_id=document.source_id
                                AND e.event_id=document.event_id
                              OFFSET 0
                           ) event"""
        artifact_relation = """LATERAL (
                             SELECT a.* FROM raw_artifacts a
                              WHERE a.tenant_id=event.tenant_id
                                AND a.source_id=event.source_id
                                AND a.artifact_id=event.artifact_id
                              OFFSET 0
                           ) artifact"""
    if bounded and not probe:
        key_filter = """AND (document.source_id,document.document_id) IN (
                                SELECT * FROM unnest(%s::text[],%s::text[]))
                            AND document.source_id=ANY(%s::text[])
                            AND document.document_id=ANY(%s::text[])"""
        document_filter = """AND document.tenant_id=%s
                            AND document.source_id=ANY(%s::text[])
                            AND document.document_id=ANY(ARRAY(
                                SELECT document_id FROM candidates))"""
        event_filter = """AND event.tenant_id=%s
                            AND event.source_id=ANY(%s::text[])
                            AND event.event_id=ANY(ARRAY(
                                SELECT event_id FROM candidates))"""
    lock_clause = "" if probe else "FOR UPDATE OF document,event SKIP LOCKED"
    projection = ("document.source_id,document.document_id" if probe else
                  """document.tenant_id,document.source_id,
                      document.document_id,document.event_id,
                      document.text_redacted,
                      octet_length(document.text_redacted)::bigint AS document_bytes,
                      octet_length(event.canonical_redacted::text)::bigint AS event_bytes""")
    candidate_sql = f"""SELECT {projection}
                           FROM {document_relation}
                           JOIN {event_relation}
                             USING(tenant_id,source_id,event_id)
                           JOIN {artifact_relation}
                             ON artifact.tenant_id=event.tenant_id
                            AND artifact.source_id=event.source_id
                            AND artifact.artifact_id=event.artifact_id
                          WHERE document.tenant_id=%s
                            {key_filter}
                            AND document.body_location='inline'
                            AND document.deleted_at IS NULL
                            AND artifact.storage_backend='s3'
                            AND artifact.state='live'
                            AND EXISTS (
                                SELECT 1
                                  FROM canonical_evidence_documents evidence
                                 WHERE evidence.tenant_id=event.tenant_id
                                   AND evidence.source_id=event.source_id
                                   AND evidence.native_parent_id=COALESCE(
                                       event.native_parent_id,event.native_id
                                   )
                                   AND evidence.manifest_storage_backend='s3'
                            )
                            AND EXISTS (
                                SELECT 1
                                  FROM canonical_chunks chunk
                                 WHERE chunk.tenant_id=document.tenant_id
                                   AND chunk.source_id=document.source_id
                                   AND chunk.document_id=document.document_id
                                   AND chunk.deleted_at IS NULL
                            )
                            AND NOT EXISTS (
                                SELECT 1
                                  FROM canonical_evidence_document_queue queued
                                 WHERE queued.tenant_id=event.tenant_id
                                   AND queued.source_id=event.source_id
                                   AND queued.native_parent_id=COALESCE(
                                       event.native_parent_id,event.native_id
                                   )
                            )
                          ORDER BY document.source_id,document.document_id
                          LIMIT %s
                          {lock_clause}
                     """
    if probe:
        return f"""WITH candidates AS MATERIALIZED ( {candidate_sql})
                   SELECT source_id,document_id FROM candidates
                   ORDER BY source_id,document_id"""
    return f"""WITH candidates AS MATERIALIZED ( {candidate_sql}), updated_documents AS (
                         UPDATE canonical_documents document
                            SET text_redacted='',body_location='chunks'
                           FROM candidates candidate
                          WHERE document.tenant_id=candidate.tenant_id
                            AND document.source_id=candidate.source_id
                            AND document.document_id=candidate.document_id
                            {document_filter}
                      RETURNING candidate.event_id,
                                candidate.tenant_id,candidate.source_id,
                                candidate.document_bytes,candidate.event_bytes
                     ), updated_events AS (
                         UPDATE canonical_events event
                            SET canonical_redacted={compact_event},
                                body_location='raw'
                           FROM updated_documents updated
                          WHERE event.tenant_id=updated.tenant_id
                            AND event.source_id=updated.source_id
                            AND event.event_id=updated.event_id
                            {event_filter}
                      RETURNING updated.document_bytes,updated.event_bytes
                     )
                     SELECT (SELECT count(*) FROM candidates)::integer
                                AS candidates,
                            count(*)::integer AS documents,
                            count(*)::integer AS events,
                            coalesce(sum(document_bytes),0)::bigint
                                AS document_bytes,
                            coalesce(sum(event_bytes),0)::bigint AS event_bytes
                       FROM updated_events"""


def thin_canonical_bodies(
    store: BrainStore,
    *,
    tenant_id: str,
    batch_size: int = 1_000,
    max_batches: int = 1,
) -> dict[str, Any]:
    """Remove duplicate bodies only after searchable chunks and S3 authority exist.

    The canonical chunk plane remains the single database-resident text copy. The
    raw and logical evidence objects remain the immutable full-document authority.
    Each batch fails closed for a document unless it has at least one live chunk.
    We deliberately do not reread and concatenate the retained chunk corpus here:
    object storage, not a second SQL body copy, is the recovery authority.
    """

    if (
        not isinstance(tenant_id, str)
        or not tenant_id
        or isinstance(batch_size, bool)
        or not isinstance(batch_size, int)
        or not 1 <= batch_size <= 10_000
        or isinstance(max_batches, bool)
        or not isinstance(max_batches, int)
        or not 1 <= max_batches <= 10_000
    ):
        raise ValueError("canonical body thinning budget is invalid")

    documents = events = document_bytes = event_bytes = batches = candidates = 0
    for _ in range(max_batches):
        with store.connect() as connection:
            row = connection.execute(
                _thinning_statement(),
                (tenant_id, batch_size),
            ).fetchone()
        current_candidates = int(row["candidates"])
        current = int(row["documents"])
        candidates += current_candidates
        documents += current
        events += int(row["events"])
        document_bytes += int(row["document_bytes"])
        event_bytes += int(row["event_bytes"])
        batches += 1
        if current_candidates < batch_size or current < current_candidates:
            break
    # Non-document oversized records are a historical repair concern, not part
    # of this hot path. Scanning every event's JSON after each small document
    # batch made a bounded update pay for a full-table TOAST walk. The explicit
    # storage-recompact-oversized-events operation owns that one-time repair;
    # document-backed oversized records are already compacted above.
    oversized_events = 0
    refused = candidates - documents
    return {
        "status": (
            "refused"
            if refused
            else "complete"
            if candidates < batch_size * max_batches
            else "pending"
        ),
        "tenant_id": tenant_id,
        "batches": batches,
        "documents": documents,
        "events": events,
        "oversized_events": oversized_events,
        "refused": refused,
        "document_bytes_removed": document_bytes,
        "event_bytes_replaced": event_bytes,
    }


class CanonicalBodyThinner:
    """A worker-local, bounded pass over inline keys, revisited after wrap.

    A pass is not a proof of global eligibility exhaustion: skipped locks,
    ineligible parents and arrivals behind the cursor are retried next pass.
    No cursor is persisted or returned. Restarting repeats safe original checks.
    """

    WINDOW_SIZE = 1024

    def __init__(self, store: BrainStore, *, tenant_id: str):
        if not isinstance(tenant_id, str) or not tenant_id:
            raise ValueError("canonical body thinning budget is invalid")
        self._store = store
        self._tenant_id = tenant_id
        self._after: tuple[str, str] | None = None
        self._through: tuple[str, str] | None = None
        self._committed_keys: dict[tuple[str, str], None] = {}

    def thin(
        self, *, batch_size: int,
        committed_keys: tuple[tuple[str, str, str], ...] = (),
    ) -> dict[str, Any]:
        if (isinstance(batch_size, bool) or not isinstance(batch_size, int)
                or not 1 <= batch_size <= 10_000):
            raise ValueError("canonical body thinning budget is invalid")
        # Best-effort hints are bounded and never authoritative. Import before
        # opening the transaction so an unknown COMMIT retains them for recheck.
        for tenant, source, document in committed_keys:
            if tenant != self._tenant_id or any(
                not isinstance(value, str) or not 1 <= len(value) <= 255
                for value in (source, document)
            ):
                continue
            self._committed_keys[(source, document)] = None
            if len(self._committed_keys) > 256:
                del self._committed_keys[next(iter(self._committed_keys))]
        through, after = self._through, self._after
        row = dict(candidates=0, documents=0, events=0,
                   document_bytes=0, event_bytes=0)
        keys = []
        eligible = []
        attempted_hints = []
        with self._store.connect() as connection:
            # Per-statement cancellation bounds a bad plan or lock wait. This
            # is not a whole-callback deadline; context exit rolls back errors.
            connection.execute("SET LOCAL statement_timeout='2s'", ())
            if through is None:
                bound = connection.execute(
                    """SELECT source_id,document_id FROM canonical_documents
                        WHERE tenant_id=%s AND body_location='inline'
                          AND deleted_at IS NULL
                        ORDER BY source_id DESC,document_id DESC LIMIT 1""",
                    (self._tenant_id,),
                ).fetchone()
                through = ((bound["source_id"], bound["document_id"])
                           if bound else None)
            if through is not None:
                lower = ("AND (source_id,document_id)>(%s,%s)"
                         if after is not None else "")
                params = ((self._tenant_id,) + through
                          + (after if after is not None else ())
                          + (self.WINDOW_SIZE,))
                keys = connection.execute(
                    f"""SELECT source_id,document_id FROM canonical_documents
                        WHERE tenant_id=%s AND body_location='inline'
                          AND deleted_at IS NULL
                          AND (source_id,document_id)<=(%s,%s)
                          {lower}
                        ORDER BY source_id,document_id LIMIT %s""", params,
                ).fetchall()
            if keys:
                sources = [key["source_id"] for key in keys]
                documents = [key["document_id"] for key in keys]
                eligible = connection.execute(
                    _thinning_statement(bounded=True, probe=True),
                    (sources, documents, self._tenant_id, self._tenant_id,
                     batch_size),
                ).fetchall()
                if len(eligible) == batch_size:
                    after = (eligible[-1]["source_id"], eligible[-1]["document_id"])
                else:
                    after = (keys[-1]["source_id"], keys[-1]["document_id"])
            # Historical selection owns the cursor and has first claim on the
            # batch. Hints only fill unused slots in the same atomic mutation.
            if len(eligible) < batch_size and self._committed_keys:
                selected = {(key["source_id"], key["document_id"]) for key in eligible}
                attempted_hints = [key for key in self._committed_keys if key not in selected]
                if attempted_hints:
                    eligible.extend(connection.execute(
                        _thinning_statement(bounded=True, probe=True),
                        ([key[0] for key in attempted_hints],
                         [key[1] for key in attempted_hints], self._tenant_id,
                         self._tenant_id, batch_size - len(eligible)),
                    ).fetchall())
            if eligible:
                sources = [key["source_id"] for key in eligible]
                documents = [key["document_id"] for key in eligible]
                source_scope = sorted(set(sources))
                row = connection.execute(
                    _thinning_statement(bounded=True),
                    (self._tenant_id, sources, documents, source_scope,
                     documents, batch_size, self._tenant_id, source_scope,
                     self._tenant_id, source_scope),
                ).fetchone()
        # Publish progress only after successful context exit (COMMIT ACK).
        # Keep unselected hints for later batches. Ineligible hints remain
        # bounded and are evicted by new arrivals; history still revisits them.
        for key in eligible:
            self._committed_keys.pop((key["source_id"], key["document_id"]), None)
        complete = not keys or after == through
        self._after, self._through = ((None, None) if complete
                                     else (after, through))
        candidates, documents = int(row["candidates"]), int(row["documents"])
        refused = candidates - documents
        return dict(
            status=("refused" if refused else "complete"
                    if through is None else "pending"),
            tenant_id=self._tenant_id, batches=1, documents=documents,
            events=int(row["events"]), oversized_events=0, refused=refused,
            document_bytes_removed=int(row["document_bytes"]),
            event_bytes_replaced=int(row["event_bytes"]),
            scanned_keys=len(keys), pass_complete=complete,
        )
