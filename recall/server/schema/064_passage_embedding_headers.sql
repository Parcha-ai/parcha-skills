BEGIN;

-- H2-a: a deterministic contextual header per passage, rendered from catalog
-- fields only (source family, source aliases, harness, workspace basename,
-- branch, document people, passage first/last time). The header is
-- embedding input only: text_redacted, spans, receipts, text_sha256 and the
-- passage id never change. embed_sha256 = sha256(header || E'\n\n' || text)
-- is the embedding reuse key under passage-embedding contract v2; rows
-- without a header (projected before this migration, backfilled by the
-- worker) fall back to text_sha256 under contract v1.
--
-- Plain nullable columns: no rewrite of existing rows, no exclusive lock on a
-- populated table beyond the brief catalog update.

ALTER TABLE canonical_passages
    ADD COLUMN IF NOT EXISTS header_redacted text,
    ADD COLUMN IF NOT EXISTS embed_sha256 char(64);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
         WHERE conname = 'canonical_passages_embed_sha256_check'
           AND conrelid = 'canonical_passages'::regclass
    ) THEN
        ALTER TABLE canonical_passages
            ADD CONSTRAINT canonical_passages_embed_sha256_check
            CHECK (embed_sha256 IS NULL OR embed_sha256 ~ '^[0-9a-f]{64}$');
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
         WHERE conname = 'canonical_passages_header_pair_check'
           AND conrelid = 'canonical_passages'::regclass
    ) THEN
        ALTER TABLE canonical_passages
            ADD CONSTRAINT canonical_passages_header_pair_check
            CHECK ((header_redacted IS NULL) = (embed_sha256 IS NULL));
    END IF;
END $$;

INSERT INTO schema_migrations(version) VALUES (64) ON CONFLICT DO NOTHING;

COMMIT;
