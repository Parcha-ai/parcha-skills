-- Optional family admission access path; outside automatic schema migrations.
CREATE INDEX CONCURRENTLY IF NOT EXISTS canonical_passage_documents_family_candidate_idx
    ON public.canonical_passage_documents(tenant_id,source_id,policy_fingerprint,logical_document_id)
    INCLUDE(revision,source_document_sha256)
    WHERE requested_extraction_version=applied_extraction_version
      AND requested_extraction_version<>'codex_tool_groups_v1'
      AND (source_id LIKE 'codex:%'
           OR (source_id LIKE 'claude:%' AND requested_extraction_version='legacy'));
