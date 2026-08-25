BEGIN;

-- One content-free, source-scoped heartbeat per installed collector. The
-- server owns reported_at and device identity; clients never send paths,
-- credentials, transcript text, or arbitrary metadata.
CREATE TABLE IF NOT EXISTS collector_health_reports (
    tenant_id text NOT NULL,
    source_id text NOT NULL,
    installation_id uuid REFERENCES connector_installations(id) ON DELETE SET NULL,
    collector_kind text NOT NULL CHECK (collector_kind IN ('claude','codex','connector')),
    collector_version integer NOT NULL CHECK (collector_version >= 1),
    status text NOT NULL CHECK (status IN ('ready','running','backfilling','degraded')),
    scan_complete boolean NOT NULL,
    pending_records bigint NOT NULL CHECK (pending_records >= 0),
    dead_records bigint NOT NULL CHECK (dead_records >= 0),
    coverage_percent double precision NOT NULL
        CHECK (coverage_percent >= 0 AND coverage_percent <= 100),
    archive_coverage_percent double precision
        CHECK (
            archive_coverage_percent IS NULL
            OR (
                archive_coverage_percent >= 0
                AND archive_coverage_percent <= 100
            )
        ),
    archive_backlog bigint CHECK (archive_backlog IS NULL OR archive_backlog >= 0),
    last_success_at timestamptz,
    last_error_code text CHECK (
        last_error_code IS NULL
        OR last_error_code ~ '^[a-z][a-z0-9_]{2,127}$'
    ),
    reported_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY(tenant_id, source_id),
    FOREIGN KEY(tenant_id, source_id)
        REFERENCES canonical_sources(tenant_id, source_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS collector_health_reports_fleet_idx
    ON collector_health_reports(tenant_id, reported_at DESC, source_id);

CREATE INDEX IF NOT EXISTS canonical_events_fleet_activity_idx
    ON canonical_events(tenant_id, source_id, created_at DESC);

CREATE INDEX IF NOT EXISTS raw_artifacts_fleet_transfer_idx
    ON raw_artifacts(tenant_id, source_id, created_at DESC);

INSERT INTO schema_migrations(version) VALUES (55) ON CONFLICT DO NOTHING;

COMMIT;
