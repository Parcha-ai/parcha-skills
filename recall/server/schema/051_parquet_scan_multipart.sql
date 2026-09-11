BEGIN;

-- Re-key the shard catalog by (…, dataset, shard_index) exactly once. This used
-- to drop and re-add the primary key on every migrate run; since migration 061
-- hangs a foreign key off that key, the re-key is now guarded by its own
-- schema_migrations row.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM schema_migrations WHERE version=51) THEN
        ALTER TABLE canonical_parquet_scan_shards
            ADD COLUMN IF NOT EXISTS shard_index integer NOT NULL DEFAULT 0
            CHECK (shard_index >= 0 AND shard_index <= 99999);

        ALTER TABLE canonical_parquet_scan_shards
            DROP CONSTRAINT IF EXISTS canonical_parquet_scan_shards_pkey;

        ALTER TABLE canonical_parquet_scan_shards
            ADD PRIMARY KEY(
                tenant_id,source_id,bucket_start,dataset,shard_index
            );

        DROP INDEX IF EXISTS canonical_parquet_scan_shards_scope_idx;
        CREATE INDEX canonical_parquet_scan_shards_scope_idx
            ON canonical_parquet_scan_shards(
                tenant_id,source_id,bucket_start,dataset,shard_index
            );
    END IF;
END $$;

INSERT INTO schema_migrations(version) VALUES (51) ON CONFLICT DO NOTHING;

COMMIT;
