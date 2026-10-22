-- Refuse invalid or differently defined indexes, including missing covering fields.
SELECT (CASE WHEN EXISTS (
    SELECT 1 FROM pg_index definition
    WHERE definition.indexrelid = to_regclass('public.canonical_passage_documents_typed_candidate_idx')
      AND definition.indisvalid AND definition.indisready AND definition.indislive
      AND pg_get_indexdef(definition.indexrelid) =
          'CREATE INDEX canonical_passage_documents_typed_candidate_idx ON public.canonical_passage_documents USING btree (tenant_id, source_id, policy_fingerprint, logical_document_id) INCLUDE (revision, source_document_sha256) WHERE ((requested_extraction_version = applied_extraction_version) AND (requested_extraction_version <> ''codex_tool_groups_v1''::text))'
) THEN '1' ELSE 'typed_candidate_index_unavailable' END)::integer;
