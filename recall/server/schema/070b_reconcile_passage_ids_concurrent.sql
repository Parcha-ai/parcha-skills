CREATE INDEX CONCURRENTLY IF NOT EXISTS canonical_passages_reconcile_idx
    ON canonical_passages(tenant_id, passage_id);
