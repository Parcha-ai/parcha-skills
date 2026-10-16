BEGIN;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM schema_migrations WHERE version=78) THEN
        RAISE EXCEPTION 'audit_maintenance_requires_source_policy';
    END IF;
END;
$$;
ALTER TABLE canonical_retirement_source_policy
    ADD COLUMN IF NOT EXISTS audit_checkpoint jsonb;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_attribute
        WHERE attrelid='canonical_retirement_source_policy'::regclass
          AND attname='audit_checkpoint' AND atttypid='jsonb'::regtype
          AND NOT attnotnull AND NOT atthasdef AND NOT attisdropped) THEN
        RAISE EXCEPTION 'audit_maintenance_checkpoint_column_invalid';
    END IF;
END;
$$;
COMMENT ON COLUMN canonical_retirement_source_policy.audit_checkpoint IS
    'Explicit fixed-cutoff ordinary success diagnostic retirement. NULL disables audit work; a pending immutable receipt survives uncertain clears.';
INSERT INTO schema_migrations(version) VALUES (86) ON CONFLICT DO NOTHING;
COMMIT;
