-- Optional certification turns need source order and oldest-parent order.
-- This companion changes no checkpoint, certificate, or schema version.
CREATE INDEX CONCURRENTLY IF NOT EXISTS canonical_passage_certification_source_idx
    ON canonical_passage_projection_queue(tenant_id, source_id, changed_at, logical_document_id)
    WHERE reason = 'certification';
