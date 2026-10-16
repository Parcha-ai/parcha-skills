BEGIN;
ALTER TABLE canonical_passage_projection_queue
    ADD COLUMN IF NOT EXISTS certification_checkpoint jsonb;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_attribute
        WHERE attrelid='canonical_passage_projection_queue'::regclass
          AND attname='certification_checkpoint' AND atttypid='jsonb'::regtype
          AND NOT attnotnull AND NOT atthasdef AND NOT attisdropped) THEN
        RAISE EXCEPTION 'membership_remainder_checkpoint_column_invalid';
    END IF;
END;
$$;
ALTER TABLE canonical_passage_projection_queue
    DROP CONSTRAINT canonical_passage_projection_queue_reason_check;
ALTER TABLE canonical_passage_projection_queue
    ADD CONSTRAINT canonical_passage_projection_queue_reason_check
    CHECK (reason IN ('backfill','logical-update','certification'));
COMMENT ON COLUMN canonical_passage_projection_queue.certification_checkpoint IS
    'Optional native certification continuation. Only certification reason consumes this generation/epoch-bound cursor; source publication always supersedes it.';
INSERT INTO schema_migrations(version) VALUES (85) ON CONFLICT DO NOTHING;
COMMIT;
