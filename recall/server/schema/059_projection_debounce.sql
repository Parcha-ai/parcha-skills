BEGIN;

-- A session that is still growing is re-projected on every collector cycle.
-- Remember when it first entered the queue so the projector can wait for a
-- quiet period (debounce) while still bounding the total wait.
ALTER TABLE canonical_evidence_document_queue
    ADD COLUMN IF NOT EXISTS first_queued_at timestamptz NOT NULL DEFAULT clock_timestamp();

INSERT INTO schema_migrations(version) VALUES (59) ON CONFLICT DO NOTHING;

COMMIT;
