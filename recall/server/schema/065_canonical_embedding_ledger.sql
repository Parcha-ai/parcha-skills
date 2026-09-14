BEGIN;

-- H5-3: a durable per-tenant, per-UTC-day count of passages the dedicated
-- embedding worker sent to the provider. The worker reads its remaining
-- budget from this table every cycle, so a daily cap survives restarts and
-- an accidental full re-embed (September 2026: ~$800) stops at the cap.
CREATE TABLE IF NOT EXISTS canonical_embedding_ledger (
    tenant_id text NOT NULL,
    day date NOT NULL,
    embedded integer NOT NULL DEFAULT 0 CHECK (embedded >= 0),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, day)
);

INSERT INTO schema_migrations(version) VALUES (65) ON CONFLICT DO NOTHING;

COMMIT;
