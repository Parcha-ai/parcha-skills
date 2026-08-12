BEGIN;

ALTER TABLE canonical_parquet_scan_shards
    DROP CONSTRAINT IF EXISTS canonical_parquet_scan_shards_dataset_check;

ALTER TABLE canonical_parquet_scan_shards
    ADD CONSTRAINT canonical_parquet_scan_shards_dataset_check
    CHECK (dataset IN ('documents','passages','records','actors'));

-- Schema v2 adds the time-ordered lossless-passage planning dataset. Rebuild every
-- extant source/month exactly once; immutable v1 objects remain readable until each
-- replacement commits and are then removed through the normal cleanup queue.
INSERT INTO canonical_parquet_scan_queue(
    tenant_id,source_id,bucket_start,generation,reason,changed_at
)
SELECT DISTINCT shard.tenant_id,shard.source_id,shard.bucket_start,
       1,'backfill',clock_timestamp()
  FROM canonical_parquet_scan_shards shard
 WHERE NOT EXISTS (
     SELECT 1 FROM schema_migrations WHERE version=53
 )
ON CONFLICT(tenant_id,source_id,bucket_start)
DO UPDATE SET generation=canonical_parquet_scan_queue.generation+1,
              reason='backfill',changed_at=clock_timestamp();

INSERT INTO schema_migrations(version) VALUES (53) ON CONFLICT DO NOTHING;

COMMIT;
