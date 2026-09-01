BEGIN;

ALTER TABLE canonical_documents
    ADD COLUMN IF NOT EXISTS body_location text NOT NULL DEFAULT 'inline';

ALTER TABLE canonical_documents
    DROP CONSTRAINT IF EXISTS canonical_documents_body_location_check;

ALTER TABLE canonical_documents
    ADD CONSTRAINT canonical_documents_body_location_check
        CHECK (body_location IN ('inline','chunks'));

ALTER TABLE canonical_events
    ADD COLUMN IF NOT EXISTS body_location text NOT NULL DEFAULT 'inline';

ALTER TABLE canonical_events
    DROP CONSTRAINT IF EXISTS canonical_events_body_location_check;

ALTER TABLE canonical_events
    ADD CONSTRAINT canonical_events_body_location_check
        CHECK (body_location IN ('inline','raw'));

CREATE INDEX IF NOT EXISTS canonical_documents_inline_body_idx
    ON canonical_documents(tenant_id,source_id,document_id)
    WHERE body_location='inline' AND deleted_at IS NULL;

INSERT INTO schema_migrations(version) VALUES (56) ON CONFLICT DO NOTHING;

COMMIT;
