BEGIN;
-- A compatibility floor, not a data migration: older capability readers reject
-- this version before serving. Apply only after compatible readers are live.
-- The separately enabled writer still proves each exact outgoing archive.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM schema_migrations WHERE version=80)
       OR to_regclass('canonical_evidence_objects') IS NULL THEN
        RAISE EXCEPTION 'historical singleton prerequisites are absent';
    END IF;
END $$;
INSERT INTO schema_migrations(version) VALUES (95) ON CONFLICT DO NOTHING;
COMMIT;
