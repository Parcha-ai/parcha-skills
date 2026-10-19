
    SELECT document.tenant_id,document.source_id,document.document_id,
           document.native_id,document.revision,document.text_sha256,
           document.body_record_ordinal,document.body_record_count,
           event.native_parent_id,event.kind,event.occurred_at,
           artifact.media_type AS raw_media_type,
           ARRAY[event.canonical_redacted->>'type',
                 event.canonical_redacted #>> '{content,type}',
                 event.canonical_redacted #>> '{content,message,type}',
                 event.canonical_redacted #>> '{content,payload,type}',
                 event.canonical_redacted #>> '{message,type}',
                 event.canonical_redacted #>> '{payload,type}'] AS structural_types,
           EXISTS (
               SELECT 1 FROM canonical_evidence_document_queue queued
                WHERE queued.tenant_id=event.tenant_id AND queued.source_id=event.source_id
                  AND queued.native_parent_id=COALESCE(event.native_parent_id,event.native_id)
           ) AS pending,
           to_jsonb(evidence) AS manifest,
           COALESCE(parts.items,'[]'::jsonb) AS parts,
           COALESCE(chunks.items,'[]'::jsonb) AS chunks
      FROM canonical_documents document
      JOIN canonical_events event USING(tenant_id,source_id,event_id)
      JOIN raw_artifacts artifact
        ON artifact.tenant_id=event.tenant_id AND artifact.source_id=event.source_id
       AND artifact.artifact_id=event.artifact_id
      LEFT JOIN canonical_evidence_documents evidence
        ON evidence.tenant_id=event.tenant_id AND evidence.source_id=event.source_id
       AND evidence.native_parent_id=COALESCE(event.native_parent_id,event.native_id)
      LEFT JOIN LATERAL (
          SELECT jsonb_agg(to_jsonb(part) ORDER BY part.part_ordinal) AS items
            FROM canonical_evidence_document_parts part
           WHERE part.tenant_id=evidence.tenant_id AND part.source_id=evidence.source_id
             AND part.logical_document_id=evidence.logical_document_id
             AND part.revision=evidence.revision
      ) parts ON true
      LEFT JOIN LATERAL (
          SELECT jsonb_agg(jsonb_build_object(
                     'ordinal',chunk.ordinal,'receipt',chunk.receipt,
                     'text_sha256',chunk.text_sha256
                 ) ORDER BY chunk.ordinal) AS items
            FROM canonical_chunks chunk
           WHERE chunk.tenant_id=document.tenant_id AND chunk.source_id=document.source_id
             AND chunk.document_id=document.document_id AND chunk.deleted_at IS NULL
      ) chunks ON true
     WHERE document.tenant_id=%s AND document.source_id=ANY(%s)
       AND document.document_id=ANY(%s)
       AND (NOT %s OR document.body_record_ordinal IS NOT NULL
                   OR document.body_record_count IS NOT NULL)
       AND document.is_current AND document.deleted_at IS NULL
       AND NOT event.is_tombstone
       AND NOT EXISTS (
           SELECT 1 FROM canonical_events later
            WHERE later.tenant_id=document.tenant_id AND later.source_id=document.source_id
              AND later.native_id=document.native_id AND later.revision>document.revision
              AND later.is_tombstone
       )
     ORDER BY document.source_id,document.document_id
