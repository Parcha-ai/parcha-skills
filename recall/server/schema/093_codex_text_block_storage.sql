BEGIN;
SET LOCAL lock_timeout = '2s';

-- The retained predicate already covers every validated old row. NOT VALID
-- avoids a corpus scan under the DDL lock; new writes are still checked.
-- This migration permits a source-unit shape, not source authority or enrollment.
ALTER TABLE canonical_passages DROP CONSTRAINT canonical_passages_roles_check1;
ALTER TABLE canonical_passages ADD CONSTRAINT canonical_passages_roles_check1 CHECK (
    roles <@ ARRAY['assistant','user']::text[] OR (
        roles = ARRAY['tool']::text[] AND source_id LIKE 'codex:%'
        AND jsonb_array_length(spans) > 0
        AND jsonb_array_length(jsonb_path_query_array(spans,
            '$[*] ? (@.source_unit == "codex.tool-result.string.v1#/payload/output"
                || @.source_unit like_regex "^codex\\.tool-result\\.block-text\\.v1#/payload/output/(0|[1-9][0-9]*)/text$")'))
            = jsonb_array_length(spans)
    )
) NOT VALID;
INSERT INTO schema_migrations(version) VALUES (93) ON CONFLICT DO NOTHING;
COMMIT;
