BEGIN;
-- A new generation requests another pass without discarding acknowledged pages.
CREATE TABLE search_parent_reconciliation (
    tenant_id text NOT NULL,
    source_id text NOT NULL,
    logical_document_id text NOT NULL,
    generation bigint NOT NULL DEFAULT 1 CHECK (generation > 0),
    page_checkpoint jsonb,
    attempted_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, source_id, logical_document_id),
    FOREIGN KEY (tenant_id, source_id, logical_document_id)
        REFERENCES canonical_passage_documents(tenant_id, source_id, logical_document_id)
        ON DELETE CASCADE
);
CREATE INDEX search_parent_reconciliation_work_idx
    ON search_parent_reconciliation(tenant_id, attempted_at, source_id, logical_document_id);
INSERT INTO schema_migrations(version) VALUES (94) ON CONFLICT DO NOTHING;
COMMIT;
