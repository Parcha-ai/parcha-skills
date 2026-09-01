BEGIN;

-- Keep session ordering as a compact ingestion-time scalar. This preserves all
-- existing values while allowing canonical event JSON to be rewritten without
-- a second full-table generated-column pass.
ALTER TABLE canonical_events
    ALTER COLUMN source_ordinal DROP EXPRESSION IF EXISTS;

INSERT INTO schema_migrations(version) VALUES (57) ON CONFLICT DO NOTHING;

COMMIT;
