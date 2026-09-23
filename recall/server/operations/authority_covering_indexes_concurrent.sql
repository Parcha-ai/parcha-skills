-- Build the measured document access path first. Apply explicitly with admitted
-- disk/WAL headroom before service rollout; IF NOT EXISTS is not validation.
-- The separate cutover operation refuses invalid or incompatible indexes.
CREATE INDEX CONCURRENTLY IF NOT EXISTS canonical_documents_live_authority_idx
    ON canonical_documents(tenant_id,source_id,document_id)
    WHERE is_current AND deleted_at IS NULL;

-- Full-history uniqueness is preserved. The index and eventual constraint have
-- the same stable name, so replay does not rebuild a renamed concurrent index.
CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS canonical_chunks_receipt_authority_key
    ON canonical_chunks(tenant_id,receipt)
    INCLUDE(source_id,document_id,deleted_at);
