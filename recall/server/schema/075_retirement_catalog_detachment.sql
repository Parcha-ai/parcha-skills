BEGIN;

-- Keep explicit disabled/restored intent after a disposable catalog is removed.
-- A detached formerly-enabled scope needs a fresh owner-reviewed catalog bind.
ALTER TABLE canonical_chunk_retirement_progress
    ALTER COLUMN logical_document_id DROP NOT NULL;

ALTER TABLE canonical_chunk_retirement_progress
    DROP CONSTRAINT IF EXISTS canonical_chunk_retirement_progress_status_check;
ALTER TABLE canonical_chunk_retirement_progress
    ADD CONSTRAINT canonical_chunk_retirement_progress_status_check
        CHECK (status IN ('disabled','pending','partial','complete','detached'));

CREATE OR REPLACE FUNCTION recall_detach_retirement_catalog()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.logical_document_id IS NOT NULL AND NEW.logical_document_id IS NULL THEN
        NEW.status := CASE WHEN OLD.enabled THEN 'detached' ELSE 'disabled' END;
        NEW.enabled := false;
        NEW.scope_epoch := OLD.scope_epoch + 1;
        NEW.updated_at := clock_timestamp();
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS recall_retirement_catalog_detached
    ON canonical_chunk_retirement_progress;
CREATE TRIGGER recall_retirement_catalog_detached
    BEFORE UPDATE OF logical_document_id ON canonical_chunk_retirement_progress
    FOR EACH ROW EXECUTE FUNCTION recall_detach_retirement_catalog();

-- Schema069 used PostgreSQL's generated constraint name. Discover that exact
-- catalog FK rather than depending on identifier truncation across versions.
DO $$
DECLARE existing record;
BEGIN
    FOR existing IN
        SELECT conname FROM pg_constraint
        WHERE conrelid='canonical_chunk_retirement_progress'::regclass
          AND confrelid='canonical_evidence_documents'::regclass AND contype='f'
    LOOP
        EXECUTE format('ALTER TABLE canonical_chunk_retirement_progress DROP CONSTRAINT %I', existing.conname);
    END LOOP;
END;
$$;

ALTER TABLE canonical_chunk_retirement_progress
    ADD CONSTRAINT canonical_chunk_retirement_catalog_fk
    FOREIGN KEY (tenant_id,source_id,logical_document_id)
    REFERENCES canonical_evidence_documents(tenant_id,source_id,logical_document_id)
    ON DELETE SET NULL (logical_document_id);

ALTER TABLE canonical_chunk_retirement_progress
    DROP CONSTRAINT IF EXISTS canonical_chunk_retirement_bound_enabled,
    DROP CONSTRAINT IF EXISTS canonical_chunk_retirement_detached_scope;
ALTER TABLE canonical_chunk_retirement_progress
    ADD CONSTRAINT canonical_chunk_retirement_bound_enabled
        CHECK (NOT enabled OR logical_document_id IS NOT NULL),
    ADD CONSTRAINT canonical_chunk_retirement_detached_scope
        CHECK (status<>'detached' OR (NOT enabled AND logical_document_id IS NULL));

-- Nullable catalog identity must not let metadata outlive its owning source.
ALTER TABLE canonical_chunk_retirement_progress
    DROP CONSTRAINT IF EXISTS canonical_chunk_retirement_source_fk;
ALTER TABLE canonical_chunk_retirement_progress
    ADD CONSTRAINT canonical_chunk_retirement_source_fk
    FOREIGN KEY (tenant_id,source_id) REFERENCES canonical_sources(tenant_id,source_id)
    ON DELETE CASCADE;

COMMENT ON COLUMN canonical_chunk_retirement_progress.logical_document_id IS
    'NULL after catalog deletion. The retained row cannot authorize clearing; only an explicit reviewed rebind can attach a formerly-enabled scope.';

INSERT INTO schema_migrations(version) VALUES (75) ON CONFLICT DO NOTHING;
COMMIT;
