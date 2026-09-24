BEGIN;

-- Group authorized physical evidence; never replace source-scoped identities.
ALTER TABLE canonical_evidence_documents
    ADD COLUMN IF NOT EXISTS conversation_id text,
    ADD COLUMN IF NOT EXISTS conversation_strand_id text;

INSERT INTO schema_migrations(version) VALUES (71) ON CONFLICT DO NOTHING;
COMMIT;
