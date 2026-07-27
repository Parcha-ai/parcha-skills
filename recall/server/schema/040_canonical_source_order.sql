BEGIN;

ALTER TABLE canonical_events
    ADD COLUMN IF NOT EXISTS source_ordinal bigint
    GENERATED ALWAYS AS (
        CASE
            WHEN jsonb_typeof(
                canonical_redacted #> '{provenance,byte_start}'
            ) = 'number'
            THEN (
                canonical_redacted #>> '{provenance,byte_start}'
            )::bigint
        END
    ) STORED;

CREATE INDEX IF NOT EXISTS canonical_events_session_order_idx
    ON canonical_events(
        tenant_id,
        source_id,
        (COALESCE(native_parent_id, native_id)),
        source_ordinal,
        occurred_at,
        native_id
    );

DROP INDEX IF EXISTS canonical_events_session_lookup_idx;

INSERT INTO schema_migrations(version) VALUES (40) ON CONFLICT DO NOTHING;

COMMIT;
