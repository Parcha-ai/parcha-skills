-- Optional certification turns need source order and oldest-parent order.
-- This companion changes no checkpoint, certificate, or schema version.
CREATE INDEX CONCURRENTLY IF NOT EXISTS canonical_passage_certification_source_idx
    ON canonical_passage_projection_queue(tenant_id, source_id, changed_at, logical_document_id)
    WHERE reason = 'certification';

-- IF NOT EXISTS can retain an interrupted build or a different same-name index.
SELECT (CASE WHEN EXISTS (
    SELECT 1 FROM pg_index definition
    WHERE definition.indexrelid=to_regclass('public.canonical_passage_certification_source_idx')
      AND definition.indisvalid AND definition.indisready AND definition.indislive
      AND pg_get_indexdef(definition.indexrelid)=
          'CREATE INDEX canonical_passage_certification_source_idx ON public.canonical_passage_projection_queue USING btree (tenant_id, source_id, changed_at, logical_document_id) WHERE (reason = ''certification''::text)'
) THEN '1' ELSE 'certification_source_index_unavailable' END)::integer;
