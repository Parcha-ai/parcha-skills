BEGIN;

-- Migration 53 originally used a volatile per-row timestamp in a DISTINCT projection. On
-- production-sized months that made otherwise-identical source/month rows unique
-- and could make one upsert affect the same queue row twice. Requeue every extant
-- source/month with a statement-stable timestamp so databases that already applied
-- 53 and fresh installs converge on the same passage projection work.
INSERT INTO canonical_parquet_scan_queue(
    tenant_id,source_id,bucket_start,generation,reason,changed_at
)
SELECT DISTINCT shard.tenant_id,shard.source_id,shard.bucket_start,
       1,'backfill',statement_timestamp()
  FROM canonical_parquet_scan_shards shard
 WHERE NOT EXISTS (
     SELECT 1 FROM schema_migrations WHERE version=54
 )
ON CONFLICT(tenant_id,source_id,bucket_start)
DO UPDATE SET generation=canonical_parquet_scan_queue.generation+1,
              reason='backfill',changed_at=statement_timestamp();

INSERT INTO schema_migrations(version) VALUES (54) ON CONFLICT DO NOTHING;

COMMIT;
