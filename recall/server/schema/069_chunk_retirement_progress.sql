BEGIN;

-- Scheduling and cumulative logical work only, never archived body authority.
-- Each enable is explicit; new parents are disabled until separately approved.
CREATE TABLE IF NOT EXISTS canonical_chunk_retirement_progress (
    tenant_id text NOT NULL,
    source_id text NOT NULL,
    native_parent_id text NOT NULL,
    logical_document_id text NOT NULL,
    enabled boolean NOT NULL DEFAULT false,
    scope_epoch bigint NOT NULL DEFAULT 1 CHECK (scope_epoch >= 1),
    manifest_artifact_id text,
    last_record_ordinal integer NOT NULL DEFAULT -1 CHECK (last_record_ordinal >= -1),
    status text NOT NULL DEFAULT 'disabled' CHECK (status IN ('disabled','pending','partial','complete')),
    cumulative_cleared_documents bigint NOT NULL DEFAULT 0 CHECK (cumulative_cleared_documents >= 0),
    cumulative_cleared_chunks bigint NOT NULL DEFAULT 0 CHECK (cumulative_cleared_chunks >= 0),
    cumulative_cleared_utf8_bytes bigint NOT NULL DEFAULT 0 CHECK (cumulative_cleared_utf8_bytes >= 0),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id,source_id,native_parent_id),
    FOREIGN KEY (tenant_id,source_id,logical_document_id)
        REFERENCES canonical_evidence_documents(tenant_id,source_id,logical_document_id) ON DELETE CASCADE
);
COMMENT ON COLUMN canonical_chunk_retirement_progress.cumulative_cleared_utf8_bytes IS
    'Sum of successful nonempty-to-empty UTF8 body updates. Not net retained, compressed, or physical space; restores/revisions do not subtract.';

INSERT INTO schema_migrations(version) VALUES (69) ON CONFLICT DO NOTHING;
COMMIT;
