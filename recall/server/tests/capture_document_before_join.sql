SELECT selected.*,items.chunks FROM (SELECT captured.*, authority.is_current AS membership_current,
                authority.deleted_at AS membership_deleted,
                authority.chunk_receipt_profile AS membership_profile,
                CASE WHEN authority.chunk_receipt_profile IS NOT NULL THEN EXISTS (
                    SELECT 1 FROM canonical_chunks remaining
                    WHERE remaining.tenant_id=authority.tenant_id AND remaining.source_id=authority.source_id
                      AND remaining.document_id=authority.document_id
                ) ELSE false END AS membership_physical FROM (SELECT document.tenant_id,document.source_id,document.document_id,
       document.native_id,document.revision,document.text_sha256,
       document.body_record_ordinal,document.body_record_count,event.kind,
       artifact.media_type AS raw_media_type,
       ARRAY[event.canonical_redacted->>'type',
             event.canonical_redacted #>> '{content,type}',
             event.canonical_redacted #>> '{content,message,type}',
             event.canonical_redacted #>> '{content,payload,type}',
             event.canonical_redacted #>> '{message,type}',
             event.canonical_redacted #>> '{payload,type}'] AS structural_types
  FROM canonical_documents document
  JOIN canonical_events event USING(tenant_id,source_id,event_id)
  JOIN raw_artifacts artifact
    ON artifact.tenant_id=event.tenant_id AND artifact.source_id=event.source_id
   AND artifact.artifact_id=event.artifact_id
 WHERE document.tenant_id=%s AND document.source_id=%s
   AND COALESCE(event.native_parent_id,event.native_id)=%s
   AND document.is_current AND document.deleted_at IS NULL AND NOT event.is_tombstone
   AND NOT EXISTS (SELECT 1 FROM canonical_events later
                    WHERE later.tenant_id=document.tenant_id AND later.source_id=document.source_id
                      AND later.native_id=document.native_id AND later.revision>document.revision
                      AND later.is_tombstone)
 ORDER BY document.document_id LIMIT %s) captured JOIN canonical_documents authority
                ON authority.tenant_id=captured.tenant_id AND authority.source_id=captured.source_id
                  AND authority.document_id=captured.document_id) selected
            CROSS JOIN LATERAL (SELECT coalesce(jsonb_agg(to_jsonb(item) ORDER BY item.ordinal),'[]'::jsonb) AS chunks
                FROM (SELECT ordinal,receipt,text_sha256,octet_length(text_redacted) AS pg_bytes,chunk_id
                      FROM canonical_chunks WHERE tenant_id=selected.tenant_id AND source_id=selected.source_id
                        AND document_id=selected.document_id AND deleted_at IS NULL ORDER BY ordinal LIMIT %s) item) items