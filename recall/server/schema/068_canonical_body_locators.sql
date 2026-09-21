BEGIN;

-- Positions in the current parent logical document, published atomically with
-- that parent's parts. NULL preserves the transitional reader for old rows.
-- No per-parent revision is copied here: unchanged prefixes incur no UPDATE.
ALTER TABLE canonical_documents
    ADD COLUMN IF NOT EXISTS body_record_ordinal integer,
    ADD COLUMN IF NOT EXISTS body_record_count integer;

ALTER TABLE canonical_documents
    ADD CONSTRAINT canonical_documents_body_locator_check CHECK (
        (body_record_ordinal IS NULL AND body_record_count IS NULL)
        OR (body_record_ordinal IS NOT NULL AND body_record_count IS NOT NULL
            AND body_record_ordinal >= 0 AND body_record_count >= 1)
    ) NOT VALID;

INSERT INTO schema_migrations(version) VALUES (68) ON CONFLICT DO NOTHING;
COMMIT;
