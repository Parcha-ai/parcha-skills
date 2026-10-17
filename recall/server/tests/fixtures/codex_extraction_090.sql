-- Test-only future migration. No runtime activation or enrollment.
ALTER TABLE canonical_passages DROP CONSTRAINT canonical_passages_roles_check1;
ALTER TABLE canonical_passages ADD CONSTRAINT canonical_passages_roles_check1 CHECK (
    roles <@ ARRAY['assistant','user']::text[] OR (
        roles = ARRAY['tool']::text[] AND source_id LIKE 'codex:%'
        AND jsonb_array_length(spans) > 0
        AND jsonb_array_length(jsonb_path_query_array(spans,
            '$[*] ? (@.source_unit == "codex.tool-result.string.v1#/payload/output")'))
            = jsonb_array_length(spans)
    )
);
ALTER TABLE canonical_passage_documents
    DROP CONSTRAINT canonical_passage_documents_requested_extraction_version_check,
    DROP CONSTRAINT canonical_passage_documents_applied_extraction_version_check,
    ADD CONSTRAINT canonical_passage_documents_requested_extraction_version_check
        CHECK (requested_extraction_version IN ('legacy','tool_groups_v1','codex_tool_groups_v1')),
    ADD CONSTRAINT canonical_passage_documents_applied_extraction_version_check
        CHECK (applied_extraction_version IN ('legacy','tool_groups_v1','codex_tool_groups_v1'));
INSERT INTO schema_migrations(version) VALUES(90);
