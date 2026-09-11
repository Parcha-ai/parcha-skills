BEGIN;

-- H1-T4: the Parquet scan plane rebuilds only the fragments (parts) whose
-- documents changed. Two catalog tables make that possible:
--
--   * dirty_documents records which logical documents a source-month rebuild
--     must consider. The sentinel logical_document_id '*' marks the whole
--     source-month dirty (reason 'backfill' or 'compaction': full rebuild).
--   * fragment_documents records which logical documents each live part holds,
--     with the projection fingerprint used to write them, so a rebuild can find
--     the parts that intersect the dirty set and detect unchanged documents.
--
-- Both are plain new tables plus inserts: no rewrite of existing rows, no
-- exclusive lock on a populated table.

CREATE TABLE IF NOT EXISTS canonical_parquet_scan_dirty_documents (
    tenant_id text NOT NULL,
    source_id text NOT NULL,
    bucket_start date NOT NULL,
    logical_document_id text NOT NULL,
    reason text NOT NULL CHECK (
        reason IN ('backfill','logical-update','forget','compaction')
    ),
    queued_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(tenant_id,source_id,bucket_start,logical_document_id),
    FOREIGN KEY(tenant_id,source_id)
        REFERENCES canonical_sources(tenant_id,source_id) ON DELETE CASCADE,
    CHECK (EXTRACT(DAY FROM bucket_start)=1)
);

CREATE TABLE IF NOT EXISTS canonical_parquet_scan_fragment_documents (
    tenant_id text NOT NULL,
    source_id text NOT NULL,
    bucket_start date NOT NULL,
    dataset text NOT NULL CHECK (dataset IN ('documents','passages','records','actors')),
    shard_index integer NOT NULL CHECK (shard_index >= 0 AND shard_index <= 99999),
    logical_document_id text NOT NULL,
    revision integer NOT NULL CHECK (revision >= 1),
    generation_sha256 char(64) NOT NULL CHECK (generation_sha256 ~ '^[0-9a-f]{64}$'),
    PRIMARY KEY(
        tenant_id,source_id,bucket_start,dataset,shard_index,logical_document_id
    ),
    FOREIGN KEY(tenant_id,source_id,bucket_start,dataset,shard_index)
        REFERENCES canonical_parquet_scan_shards(
            tenant_id,source_id,bucket_start,dataset,shard_index
        ) ON DELETE CASCADE,
    CHECK (EXTRACT(DAY FROM bucket_start)=1)
);

-- Delta rebuilds look fragments up by document.
CREATE INDEX IF NOT EXISTS canonical_parquet_scan_fragment_documents_document_idx
    ON canonical_parquet_scan_fragment_documents(
        tenant_id,source_id,bucket_start,logical_document_id
    );

INSERT INTO schema_migrations(version) VALUES (61) ON CONFLICT DO NOTHING;

COMMIT;
