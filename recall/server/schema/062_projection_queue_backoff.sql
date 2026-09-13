BEGIN;

-- A group whose projection fails (for example an oversized record whose
-- archived bytes no longer match their declared digest) must not take the
-- whole worker down with it. Track attempts per queued group, back off
-- exponentially, and quarantine after MAX_LOGICAL_ATTEMPTS so the rest of
-- the tenant keeps projecting while the poisoned group is repaired.
ALTER TABLE canonical_evidence_document_queue
    ADD COLUMN IF NOT EXISTS attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    ADD COLUMN IF NOT EXISTS next_attempt_at timestamptz,
    ADD COLUMN IF NOT EXISTS last_error_code text;

INSERT INTO schema_migrations(version) VALUES (62) ON CONFLICT DO NOTHING;

COMMIT;
