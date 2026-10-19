BEGIN;
-- Acknowledged upsert-page position only. The shard watermark remains EOF-only.
ALTER TABLE search_projection_outbox ADD COLUMN IF NOT EXISTS page_checkpoint jsonb;
COMMIT;
