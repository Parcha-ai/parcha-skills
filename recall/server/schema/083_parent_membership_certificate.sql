BEGIN;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM schema_migrations WHERE version=80)
       OR NOT EXISTS (SELECT 1 FROM schema_migrations WHERE version=82)
       OR NOT EXISTS (
           SELECT 1 FROM pg_attribute
            WHERE attrelid=to_regclass('canonical_parent_authority')
              AND attname='generation' AND NOT attisdropped
              AND atttypid='bigint'::regtype AND attnotnull
       ) THEN
        RAISE EXCEPTION 'parent_certificate_generation_schema_required';
    END IF;
END;
$$;

-- No backfill, default, index or reader trust. Existing publications remain
-- uncertified until the separate owner proves their exact membership snapshot.
ALTER TABLE canonical_passages
    ADD COLUMN IF NOT EXISTS membership_generation bigint,
    ADD COLUMN IF NOT EXISTS membership_binding_sha256 text;

DO $$
BEGIN
    IF (SELECT count(*) FROM pg_attribute
         WHERE attrelid='canonical_passages'::regclass AND NOT attisdropped
           AND NOT attnotnull AND NOT atthasdef
           AND ((attname='membership_generation' AND atttypid='bigint'::regtype)
             OR (attname='membership_binding_sha256' AND atttypid='text'::regtype))) <> 2 THEN
        RAISE EXCEPTION 'parent_certificate_columns_invalid';
    END IF;
END;
$$;

COMMENT ON COLUMN canonical_passages.membership_generation IS
    'Parent generation captured with exact native membership and ownership proof. Null or mismatched values require native verification.';
COMMENT ON COLUMN canonical_passages.membership_binding_sha256 IS
    'Versioned exact publication binding including an operator-owned reset epoch. Never an ACL or standalone authority proof.';

INSERT INTO schema_migrations(version) VALUES (83) ON CONFLICT DO NOTHING;
COMMIT;
