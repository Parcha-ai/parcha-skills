BEGIN;

DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM schema_migrations WHERE version=75) THEN
        RAISE EXCEPTION 'retirement policy requires schema 075';
    END IF;
END $$;

ALTER TABLE canonical_chunk_retirement_progress
    DROP CONSTRAINT IF EXISTS canonical_chunk_retirement_progress_status_check;
ALTER TABLE canonical_chunk_retirement_progress
    ADD CONSTRAINT canonical_chunk_retirement_progress_status_check
        CHECK (status IN ('disabled','pending','partial','complete','detached','baseline'));
ALTER TABLE canonical_chunk_retirement_progress
    DROP CONSTRAINT IF EXISTS canonical_chunk_retirement_baseline_scope;
ALTER TABLE canonical_chunk_retirement_progress
    ADD CONSTRAINT canonical_chunk_retirement_baseline_scope
        CHECK (status<>'baseline' OR (NOT enabled AND logical_document_id IS NULL));

-- Approval and resumable capture metadata only. Parent intent stays in progress.
CREATE TABLE IF NOT EXISTS canonical_retirement_source_policy (
    tenant_id text NOT NULL,
    source_id text NOT NULL,
    principal_id text NOT NULL,
    policy_epoch bigint NOT NULL DEFAULT 1 CHECK (policy_epoch >= 1),
    enabled boolean NOT NULL DEFAULT false,
    allow_detached_rebind boolean NOT NULL DEFAULT false,
    capture_started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    capture_cursor text,
    eligible_after timestamptz,
    capture_complete boolean NOT NULL DEFAULT false,
    captured_parents bigint NOT NULL DEFAULT 0 CHECK (captured_parents >= 0),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id,source_id),
    FOREIGN KEY (tenant_id,source_id) REFERENCES canonical_sources(tenant_id,source_id)
        ON DELETE CASCADE,
    CHECK (NOT enabled OR (capture_complete AND eligible_after IS NOT NULL))
);
INSERT INTO schema_migrations(version) VALUES (76) ON CONFLICT DO NOTHING;
COMMIT;
