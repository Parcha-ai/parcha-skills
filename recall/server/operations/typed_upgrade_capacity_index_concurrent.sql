-- Optional outstanding-upgrade access path, outside automatic migrations.
-- Admission SQL, capacity semantics, schema markers and existing indexes stay unchanged.
CREATE INDEX CONCURRENTLY IF NOT EXISTS canonical_passage_documents_upgrade_idx
    ON public.canonical_passage_documents(tenant_id, source_id, logical_document_id)
    WHERE requested_extraction_version <> applied_extraction_version;
