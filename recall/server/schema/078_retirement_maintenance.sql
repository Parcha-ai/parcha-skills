BEGIN;
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM schema_migrations WHERE version=76) THEN
        RAISE EXCEPTION 'retirement maintenance requires schema 076';
    END IF;
END $$;
ALTER TABLE canonical_retirement_source_policy
    ADD COLUMN IF NOT EXISTS maintenance_cursor text,
    ADD COLUMN IF NOT EXISTS maintenance_scheduled_at timestamptz NOT NULL DEFAULT '1970-01-01 00:00:00+00',
    ADD COLUMN IF NOT EXISTS maintenance_epoch bigint NOT NULL DEFAULT 0 CHECK (maintenance_epoch>=0),
    ADD COLUMN IF NOT EXISTS maintenance_intent jsonb;
COMMENT ON COLUMN canonical_retirement_source_policy.maintenance_intent IS
    'Last bounded worker parent/epoch observation; operational recovery metadata, never retirement authority.';
INSERT INTO schema_migrations(version) VALUES(78) ON CONFLICT DO NOTHING;
COMMIT;
