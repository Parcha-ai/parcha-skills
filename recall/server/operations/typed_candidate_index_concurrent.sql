-- Optional finite-backfill access path; no automatic schema migration.
-- Remove after Codex legacy upgrades drain and ordinary admission is reliable.
-- Non-Codex legacy rows remain indexed, so an empty index is not the removal gate.
CREATE INDEX CONCURRENTLY IF NOT EXISTS canonical_passage_documents_typed_candidate_idx
    ON public.canonical_passage_documents(tenant_id, source_id, policy_fingerprint, logical_document_id)
    INCLUDE (revision, source_document_sha256)
    WHERE requested_extraction_version = applied_extraction_version
      AND requested_extraction_version <> 'codex_tool_groups_v1';
