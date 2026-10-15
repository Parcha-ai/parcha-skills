"""Optional append classification; publication still uses the full input owner."""

from __future__ import annotations


def classify_new_natives(
    connection, *, tenant_id, source_id, native_ids, new_native_ids
):
    """Annotate the queue already locked by this canonical ingest transaction.

    Unknown writers only increment generation, automatically invalidating any
    old annotation. A later append cannot revive that poisoned generation.
    """
    if not new_native_ids:
        return
    connection.execute(
        """WITH affected AS (
            SELECT COALESCE(event.native_parent_id,event.native_id) AS native_parent_id,
                   bool_and(event.native_id=ANY(%s::text[])
                            AND event.revision=1 AND NOT event.is_tombstone
                            AND event.source_ordinal IS NOT NULL) AS new_only,
                   min(event.source_ordinal) AS first_ordinal
            FROM canonical_events event
            WHERE event.tenant_id=%s AND event.source_id=%s
              AND event.native_id=ANY(%s::text[])
            GROUP BY COALESCE(event.native_parent_id,event.native_id)
        ), classified AS (
            SELECT queue.native_parent_id,document.manifest_artifact_id,
                   affected.new_only
                   AND document.append_source_ordinal IS NOT NULL
                   AND document.append_manifest_id=document.manifest_artifact_id
                   AND affected.first_ordinal>document.append_source_ordinal
                   AND queue.reason='ingest'
                   AND (queue.generation=1 OR (
                        queue.append_generation=queue.generation-1
                        AND queue.append_base_manifest_id=document.manifest_artifact_id
                   )) AS append_safe
            FROM canonical_evidence_document_queue queue
            JOIN affected USING(native_parent_id)
            LEFT JOIN canonical_evidence_documents document
              ON document.tenant_id=queue.tenant_id AND document.source_id=queue.source_id
             AND document.native_parent_id=queue.native_parent_id
            WHERE queue.tenant_id=%s AND queue.source_id=%s
        ) UPDATE canonical_evidence_document_queue queue
          SET append_generation=CASE WHEN classified.append_safe THEN queue.generation END,
              append_base_manifest_id=CASE WHEN classified.append_safe
                                           THEN classified.manifest_artifact_id END
          FROM classified WHERE queue.tenant_id=%s AND queue.source_id=%s
            AND queue.native_parent_id=classified.native_parent_id""",
        (
            list(new_native_ids),
            tenant_id,
            source_id,
            list(native_ids),
            tenant_id,
            source_id,
            tenant_id,
            source_id,
        ),
    )


def record_frontier(connection, candidate, locators, manifest_id):
    """Bind a complete successful input snapshot in its existing transaction."""
    from .logical_evidence_projection import _LocatorSpool

    frontier = (
        locators.append_source_ordinal
        if isinstance(locators, _LocatorSpool) and locators.complete_metadata
        else None
    )
    binding = manifest_id if frontier is not None else None
    connection.execute(
        """WITH changed AS MATERIALIZED (
            SELECT logical_document_id FROM canonical_evidence_documents
            WHERE tenant_id=%s AND source_id=%s AND native_parent_id=%s
              AND manifest_artifact_id=%s
              AND (append_source_ordinal,append_manifest_id) IS DISTINCT FROM (%s::bigint,%s::text)
            FOR UPDATE NOWAIT
        ) UPDATE canonical_evidence_documents document
          SET append_source_ordinal=%s,append_manifest_id=%s
          FROM changed WHERE document.tenant_id=%s AND document.source_id=%s
            AND document.logical_document_id=changed.logical_document_id""",
        (
            candidate.tenant_id,
            candidate.source_id,
            candidate.native_parent_id,
            manifest_id,
            frontier,
            binding,
            frontier,
            binding,
            candidate.tenant_id,
            candidate.source_id,
        ),
    )
