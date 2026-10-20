-- Optional publication keyset access path, outside automatic migrations.
-- No schema marker, query change, checkpoint change or old-index retirement.
CREATE INDEX CONCURRENTLY IF NOT EXISTS canonical_passages_page_time_id_idx
    ON public.canonical_passages(tenant_id,source_id,first_occurred_at,passage_id);
