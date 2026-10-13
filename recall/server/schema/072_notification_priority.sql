BEGIN;

ALTER TABLE canonical_evidence_document_queue
    ADD COLUMN IF NOT EXISTS notification_queued_at timestamptz;

INSERT INTO schema_migrations(version) VALUES (72) ON CONFLICT DO NOTHING;

COMMIT;
