BEGIN;
-- Optional preparation state. No backfill and no change to source authority.
ALTER TABLE canonical_passage_documents
    ADD COLUMN IF NOT EXISTS append_checkpoint jsonb;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_attribute
        WHERE attrelid='canonical_passage_documents'::regclass
          AND attname='append_checkpoint' AND atttypid='jsonb'::regtype
          AND NOT attnotnull AND NOT atthasdef AND NOT attisdropped) THEN
        RAISE EXCEPTION 'passage_append_checkpoint_column_invalid';
    END IF;
END;
$$;
COMMENT ON COLUMN canonical_passage_documents.append_checkpoint IS
    'Versioned exact immutable-prefix and overlapping-tail preparation state. Missing or invalid state requires full projection; never an authority proof.';
INSERT INTO schema_migrations(version) VALUES (84) ON CONFLICT DO NOTHING;
COMMIT;
