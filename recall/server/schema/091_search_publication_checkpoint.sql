BEGIN;
-- Acknowledged upsert-page position only. The shard watermark remains EOF-only.
ALTER TABLE search_projection_outbox ADD COLUMN IF NOT EXISTS page_checkpoint jsonb;
INSERT INTO schema_migrations(version) VALUES (91) ON CONFLICT DO NOTHING;
COMMIT;
