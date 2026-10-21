-- A same-named invalid or differently defined index does not satisfy this operation.
SELECT (CASE WHEN EXISTS (
    SELECT 1 FROM pg_index definition
    WHERE definition.indexrelid = to_regclass('public.canonical_passage_documents_upgrade_idx')
      AND definition.indisvalid AND definition.indisready AND definition.indislive
      AND pg_get_indexdef(definition.indexrelid) =
          'CREATE INDEX canonical_passage_documents_upgrade_idx ON public.canonical_passage_documents USING btree (tenant_id, source_id, logical_document_id) WHERE (requested_extraction_version <> applied_extraction_version)'
) THEN '1' ELSE 'typed_upgrade_capacity_index_unavailable' END)::integer;
