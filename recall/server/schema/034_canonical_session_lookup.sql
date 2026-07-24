BEGIN;

-- Session expansion is always tenant/source bounded and ordered by source time.
CREATE INDEX IF NOT EXISTS canonical_events_session_lookup_idx
    ON canonical_events(
        tenant_id,
        source_id,
        (COALESCE(native_parent_id, native_id)),
        occurred_at,
        native_id
    );

INSERT INTO schema_migrations(version) VALUES (34) ON CONFLICT DO NOTHING;

COMMIT;
