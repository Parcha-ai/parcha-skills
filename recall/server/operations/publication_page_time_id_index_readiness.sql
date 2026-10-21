-- Read-only acceptance after the optional concurrent build.
-- IF NOT EXISTS does not establish validity or the expected definition.
SELECT (CASE WHEN EXISTS (
    SELECT 1 FROM pg_index definition
    WHERE definition.indexrelid=to_regclass('public.canonical_passages_page_time_id_idx')
      AND definition.indisvalid AND definition.indisready AND definition.indislive
      AND pg_get_indexdef(definition.indexrelid)=
          'CREATE INDEX canonical_passages_page_time_id_idx ON public.canonical_passages USING btree (tenant_id, source_id, first_occurred_at, passage_id)'
) THEN '1' ELSE 'publication_page_time_id_index_unavailable' END)::integer;
