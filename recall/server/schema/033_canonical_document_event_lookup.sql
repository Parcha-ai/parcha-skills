BEGIN;

-- Support canonical event lifecycle operations and their foreign-key checks.
CREATE INDEX IF NOT EXISTS canonical_documents_event_lookup_idx
    ON canonical_documents(tenant_id, source_id, event_id);

INSERT INTO schema_migrations(version) VALUES (33) ON CONFLICT DO NOTHING;

COMMIT;
