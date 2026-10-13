BEGIN;

ALTER TABLE canonical_passage_projection_queue
    ADD COLUMN IF NOT EXISTS notification_queued_at timestamptz;

INSERT INTO schema_migrations(version) VALUES (73) ON CONFLICT DO NOTHING;

COMMIT;
