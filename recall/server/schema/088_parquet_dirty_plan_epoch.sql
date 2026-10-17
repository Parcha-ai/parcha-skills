BEGIN;
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM schema_migrations WHERE version=61) THEN
        RAISE EXCEPTION 'parquet_dirty_plan_requires_fragment_catalog';
    END IF;
END $$;
ALTER TABLE canonical_parquet_scan_shards ADD COLUMN IF NOT EXISTS dirty_plan_epoch text;
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_attribute
        WHERE attrelid='canonical_parquet_scan_shards'::regclass
          AND attname='dirty_plan_epoch' AND atttypid='text'::regtype
          AND NOT attnotnull AND NOT atthasdef AND NOT attisdropped) THEN
        RAISE EXCEPTION 'parquet_dirty_plan_column_invalid';
    END IF;
END $$;
COMMENT ON COLUMN canonical_parquet_scan_shards.dirty_plan_epoch IS
    'Full metadata planner enrollment for a complete native dirty-writer epoch. NULL or a different epoch requires the full planner. Rotate after older writers; schema alone enables no sparse planning.';
INSERT INTO schema_migrations(version) VALUES (88) ON CONFLICT DO NOTHING;
COMMIT;
