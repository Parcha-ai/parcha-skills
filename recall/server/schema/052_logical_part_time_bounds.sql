BEGIN;

ALTER TABLE canonical_evidence_document_parts
    ADD COLUMN IF NOT EXISTS first_occurred_at timestamptz,
    ADD COLUMN IF NOT EXISTS last_occurred_at timestamptz;

ALTER TABLE canonical_evidence_document_parts
    DROP CONSTRAINT IF EXISTS canonical_evidence_document_parts_time_bounds_check;

ALTER TABLE canonical_evidence_document_parts
    ADD CONSTRAINT canonical_evidence_document_parts_time_bounds_check CHECK (
        (first_occurred_at IS NULL AND last_occurred_at IS NULL)
        OR (
            first_occurred_at IS NOT NULL
            AND last_occurred_at IS NOT NULL
            AND first_occurred_at <= last_occurred_at
        )
    );

CREATE INDEX IF NOT EXISTS canonical_evidence_document_parts_time_idx
    ON canonical_evidence_document_parts(
        tenant_id,source_id,first_occurred_at,last_occurred_at
    );

INSERT INTO schema_migrations(version) VALUES (52) ON CONFLICT DO NOTHING;

COMMIT;
