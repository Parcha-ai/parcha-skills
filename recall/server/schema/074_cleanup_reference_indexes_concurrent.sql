-- Exact scoped artifact probes keep cleanup's live-reference guard selective.
-- This companion sorts before the guarded marker and runs outside a transaction.
CREATE INDEX CONCURRENTLY IF NOT EXISTS canonical_evidence_documents_manifest_artifact_idx
    ON canonical_evidence_documents(tenant_id, source_id, manifest_artifact_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS canonical_evidence_document_parts_artifact_idx
    ON canonical_evidence_document_parts(tenant_id, source_id, artifact_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS canonical_parquet_scan_shards_artifact_idx
    ON canonical_parquet_scan_shards(tenant_id, source_id, artifact_id);
