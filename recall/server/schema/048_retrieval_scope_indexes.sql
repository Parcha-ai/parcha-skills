BEGIN;

-- Exact person/source/time scope enumeration must not depend on semantic
-- ranking. These indexes support the metadata-only MCP scope contract.
CREATE INDEX IF NOT EXISTS canonical_evidence_documents_time_scope_idx
    ON canonical_evidence_documents(
        tenant_id, source_id, last_occurred_at DESC,
        first_occurred_at, logical_document_id
    );

INSERT INTO schema_migrations(version) VALUES (48) ON CONFLICT DO NOTHING;

COMMIT;
