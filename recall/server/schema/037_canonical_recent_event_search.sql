BEGIN;

-- Bound broad lexical queries by recent source occurrence time before joining
-- multi-million-row document and chunk projections.
CREATE INDEX IF NOT EXISTS canonical_events_recent_search_idx
    ON canonical_events(
        tenant_id,
        source_id,
        occurred_at DESC,
        event_id
    );

INSERT INTO schema_migrations(version) VALUES (37) ON CONFLICT DO NOTHING;

COMMIT;
