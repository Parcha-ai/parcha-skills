BEGIN;

-- H3-a: the outbox that feeds the Lance-on-S3 search plane (H3-b). It is a
-- separate queue from canonical_parquet_scan_queue so the Parquet and Lance
-- cadences never couple: every passage-plane write enqueues the source-months
-- it touched here, the Lance writer drains one source-month at a time.
--
--   * search_projection_outbox: one row per dirty (tenant, source, month).
--     generation increments on every re-enqueue so a writer can detect that a
--     month changed again while it was being built. 'backfill' is sticky: a
--     month queued for a full rebuild keeps that reason until it is built.
--   * search_projection_tombstones: explicit per-passage deletions (differential
--     commit to_delete, forget). A Lance shard is append-mostly, so the writer
--     applies these as deletes instead of diffing the whole month. month is the
--     passage's first month (its primary shard); the outbox row covers every
--     month the passage spanned.
--   * search_projection_shards: the catalog the Lance writer fills, one row per
--     built source-month with the outbox generation it consumed.
--
-- Plain new tables plus indexes: no rewrite of existing rows.

CREATE TABLE IF NOT EXISTS search_projection_outbox (
    tenant_id text NOT NULL,
    source_id text NOT NULL,
    month date NOT NULL,
    generation bigint NOT NULL DEFAULT 1 CHECK (generation >= 1),
    reason text NOT NULL CHECK (
        reason IN ('backfill','logical-update','forget','header-change')
    ),
    queued_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    first_queued_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, source_id, month),
    FOREIGN KEY (tenant_id, source_id)
        REFERENCES canonical_sources(tenant_id, source_id) ON DELETE CASCADE,
    CHECK (EXTRACT(DAY FROM month)=1)
);

-- The writer drains oldest-first within a tenant.
CREATE INDEX IF NOT EXISTS search_projection_outbox_work_idx
    ON search_projection_outbox(tenant_id, queued_at, source_id, month);

CREATE TABLE IF NOT EXISTS search_projection_tombstones (
    tenant_id text NOT NULL,
    source_id text NOT NULL,
    passage_id text NOT NULL,
    month date NOT NULL,
    deleted_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, source_id, passage_id),
    FOREIGN KEY (tenant_id, source_id)
        REFERENCES canonical_sources(tenant_id, source_id) ON DELETE CASCADE,
    CHECK (passage_id ~ '^psg_[0-9a-f]{32}$'),
    CHECK (EXTRACT(DAY FROM month)=1)
);

-- A month rebuild reads the tombstones of that source-month.
CREATE INDEX IF NOT EXISTS search_projection_tombstones_month_idx
    ON search_projection_tombstones(tenant_id, source_id, month, deleted_at);

CREATE TABLE IF NOT EXISTS search_projection_shards (
    tenant_id text NOT NULL,
    source_id text NOT NULL,
    month date NOT NULL,
    generation bigint NOT NULL CHECK (generation >= 1),
    dataset_uri text NOT NULL CHECK (length(dataset_uri) BETWEEN 1 AND 2048),
    row_count bigint NOT NULL CHECK (row_count >= 0),
    built_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, source_id, month),
    FOREIGN KEY (tenant_id, source_id)
        REFERENCES canonical_sources(tenant_id, source_id) ON DELETE CASCADE,
    CHECK (EXTRACT(DAY FROM month)=1)
);

INSERT INTO schema_migrations(version) VALUES (66) ON CONFLICT DO NOTHING;

COMMIT;
