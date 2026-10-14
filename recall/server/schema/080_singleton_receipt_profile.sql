BEGIN;
-- Metadata-only, nullable and without a default: existing rows remain unadmitted.
ALTER TABLE canonical_documents ADD COLUMN IF NOT EXISTS chunk_receipt_profile smallint;
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_attribute
        WHERE attrelid='canonical_documents'::regclass AND attname='chunk_receipt_profile'
          AND atttypid='smallint'::regtype AND NOT attnotnull AND NOT atthasdef
          AND NOT attisdropped
    ) THEN
        RAISE EXCEPTION 'singleton receipt profile column is incompatible';
    END IF;
END $$;
COMMENT ON COLUMN canonical_documents.chunk_receipt_profile IS
    'NULL retains physical chunk authority. Profile 1 is assigned only by explicit verified empty-singleton retirement.';
INSERT INTO schema_migrations(version) VALUES (80) ON CONFLICT DO NOTHING;
COMMIT;
