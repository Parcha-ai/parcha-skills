BEGIN;

CREATE TABLE IF NOT EXISTS agent_runs (
    tenant_id text NOT NULL,
    run_id text NOT NULL,
    task_id text NOT NULL UNIQUE,
    request_id text NOT NULL,
    principal_id text NOT NULL,
    trace_id text NOT NULL,
    request_sha256 char(64) NOT NULL,
    source_ids text[] NOT NULL,
    status text NOT NULL
        CHECK (status IN (
            'queued', 'running', 'complete', 'partial', 'no_answer',
            'failed', 'cancelled'
        )),
    attempt integer NOT NULL DEFAULT 1 CHECK (attempt BETWEEN 1 AND 100),
    cancel_requested boolean NOT NULL DEFAULT false,
    lease_owner text,
    lease_expires_at timestamptz,
    error_code text,
    trace_events jsonb NOT NULL DEFAULT '[]'::jsonb,
    result jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    started_at timestamptz,
    completed_at timestamptz,
    PRIMARY KEY(tenant_id, run_id),
    FOREIGN KEY(tenant_id, principal_id)
        REFERENCES brain_principals(tenant_id, principal_id),
    CHECK (run_id ~ '^run_[0-9a-f]{32}$'),
    CHECK (task_id ~ '^tsk_[0-9a-f]{32}$'),
    CHECK (request_id ~ '^req_[A-Za-z0-9_-]{16,128}$'),
    CHECK (trace_id ~ '^trc_[0-9a-f]{32}$'),
    CHECK (request_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (cardinality(source_ids) BETWEEN 0 AND 256),
    CHECK (
        (status = 'running' AND lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL)
        OR (status <> 'running' AND lease_owner IS NULL AND lease_expires_at IS NULL)
    ),
    CHECK (
        (status IN ('complete', 'partial', 'no_answer') AND result IS NOT NULL)
        OR (status NOT IN ('complete', 'partial', 'no_answer') AND result IS NULL)
    ),
    CHECK (
        (status IN ('complete', 'partial', 'no_answer', 'failed', 'cancelled')
            AND completed_at IS NOT NULL)
        OR (status IN ('queued', 'running') AND completed_at IS NULL)
    ),
    CHECK (jsonb_typeof(trace_events) = 'array'),
    CHECK (pg_column_size(trace_events) <= 1048576),
    CHECK (result IS NULL OR pg_column_size(result) <= 1048576),
    CHECK (error_code IS NULL OR error_code ~ '^[a-z][a-z0-9_.-]{1,63}$')
);

CREATE INDEX IF NOT EXISTS agent_runs_principal_lookup_idx
    ON agent_runs(tenant_id, principal_id, updated_at DESC);

CREATE INDEX IF NOT EXISTS agent_runs_recovery_idx
    ON agent_runs(status, lease_expires_at, created_at)
    WHERE status IN ('queued', 'running');

INSERT INTO schema_migrations(version) VALUES (38) ON CONFLICT DO NOTHING;

COMMIT;
