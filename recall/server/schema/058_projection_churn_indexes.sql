-- Projection churn gauges (systems card freshness.projection_churn) count
-- passages and passage documents created in a trailing window. The service
-- metrics query is not tenant-scoped, so the index leads with created_at.
-- This repo runs every migration inside one transaction, so CONCURRENTLY is
-- not available; the build takes a SHARE lock on each table while it runs.
CREATE INDEX IF NOT EXISTS canonical_passages_created_idx
    ON canonical_passages(created_at);

CREATE INDEX IF NOT EXISTS canonical_passage_documents_created_idx
    ON canonical_passage_documents(created_at);

INSERT INTO schema_migrations(version) VALUES (58) ON CONFLICT DO NOTHING;
