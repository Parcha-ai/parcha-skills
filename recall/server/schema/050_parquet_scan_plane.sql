BEGIN;

CREATE TABLE IF NOT EXISTS canonical_parquet_scan_queue (
    tenant_id text NOT NULL,
    source_id text NOT NULL,
    bucket_start date NOT NULL,
    generation bigint NOT NULL DEFAULT 1 CHECK (generation >= 1),
    reason text NOT NULL CHECK (reason IN ('backfill','logical-update','forget')),
    changed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(tenant_id, source_id, bucket_start),
    FOREIGN KEY(tenant_id, source_id)
        REFERENCES canonical_sources(tenant_id, source_id) ON DELETE CASCADE,
    CHECK (EXTRACT(DAY FROM bucket_start)=1)
);

CREATE INDEX IF NOT EXISTS canonical_parquet_scan_queue_work_idx
    ON canonical_parquet_scan_queue(
        tenant_id, changed_at, source_id, bucket_start
    );

CREATE TABLE IF NOT EXISTS canonical_parquet_scan_shards (
    tenant_id text NOT NULL,
    source_id text NOT NULL,
    bucket_start date NOT NULL,
    dataset text NOT NULL CHECK (dataset IN ('documents','records','actors')),
    generation_sha256 char(64) NOT NULL,
    artifact_id text NOT NULL,
    storage_backend text NOT NULL CHECK (storage_backend IN ('filesystem','s3')),
    object_key text NOT NULL,
    content_sha256 char(64) NOT NULL,
    size_bytes bigint NOT NULL CHECK (size_bytes >= 1 AND size_bytes <= 5368709120),
    media_type text NOT NULL CHECK (media_type='application/vnd.apache.parquet'),
    encryption text NOT NULL CHECK (
        encryption IN ('filesystem-owner-only','sse-s3','sse-kms','sse-c')
    ),
    version_id text NOT NULL,
    row_count bigint NOT NULL CHECK (row_count >= 0),
    first_occurred_at timestamptz,
    last_occurred_at timestamptz,
    created_at timestamptz NOT NULL,
    PRIMARY KEY(tenant_id,source_id,bucket_start,dataset),
    UNIQUE(storage_backend,object_key,version_id),
    FOREIGN KEY(tenant_id,source_id)
        REFERENCES canonical_sources(tenant_id,source_id) ON DELETE CASCADE,
    CHECK (EXTRACT(DAY FROM bucket_start)=1),
    CHECK (generation_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (artifact_id ~ '^art_[0-9a-f]{32}$'),
    CHECK (object_key ~ '^objects/[0-9a-f]{2}/[0-9a-f]{64}$'),
    CHECK (content_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (
        (first_occurred_at IS NULL AND last_occurred_at IS NULL)
        OR first_occurred_at <= last_occurred_at
    )
);

CREATE INDEX IF NOT EXISTS canonical_parquet_scan_shards_scope_idx
    ON canonical_parquet_scan_shards(
        tenant_id,source_id,bucket_start,dataset
    );

-- Seed the pre-existing logical corpus exactly once. A document already queued
-- for logical repair is intentionally skipped: its successful replacement
-- transaction will enqueue the correct source-months with current attribution.
INSERT INTO canonical_parquet_scan_queue(
    tenant_id,source_id,bucket_start,generation,reason,changed_at
)
SELECT DISTINCT document.tenant_id,document.source_id,
       month.value::date,1,'backfill',clock_timestamp()
  FROM canonical_evidence_documents document
 CROSS JOIN LATERAL generate_series(
     date_trunc('month',document.first_occurred_at),
     date_trunc('month',document.last_occurred_at),
     interval '1 month'
 ) month(value)
 WHERE NOT EXISTS (
     SELECT 1 FROM schema_migrations WHERE version=50
 )
   AND NOT EXISTS (
     SELECT 1
       FROM canonical_evidence_document_queue queued
      WHERE queued.tenant_id=document.tenant_id
        AND queued.source_id=document.source_id
        AND queued.native_parent_id=document.native_parent_id
 )
ON CONFLICT(tenant_id,source_id,bucket_start) DO NOTHING;

INSERT INTO schema_migrations(version) VALUES (50) ON CONFLICT DO NOTHING;

COMMIT;
