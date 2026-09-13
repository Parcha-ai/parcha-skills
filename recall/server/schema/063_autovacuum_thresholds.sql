BEGIN;

-- H1-T7: autovacuum never ran on the 12M-row tables (default scale factor
-- 0.2 = 2.4M dead rows before a vacuum, and the two global workers were
-- busy with tiny hot tables). Planner statistics went 20-30x stale and
-- dead-row ratios reached 50-76%, which is what made the projector streams
-- and cold search reads crawl on 2026-09-12. Per-table thresholds keep
-- these tables vacuumed and analyzed continuously at a bounded IO cost.
ALTER TABLE canonical_events SET (
    autovacuum_vacuum_scale_factor = 0.02,
    autovacuum_analyze_scale_factor = 0.01,
    autovacuum_vacuum_cost_limit = 2000
);
ALTER TABLE canonical_documents SET (
    autovacuum_vacuum_scale_factor = 0.02,
    autovacuum_analyze_scale_factor = 0.01,
    autovacuum_vacuum_cost_limit = 2000
);
ALTER TABLE canonical_chunks SET (
    autovacuum_vacuum_scale_factor = 0.02,
    autovacuum_analyze_scale_factor = 0.01,
    autovacuum_vacuum_cost_limit = 2000
);
ALTER TABLE canonical_passages SET (
    autovacuum_vacuum_scale_factor = 0.05,
    autovacuum_analyze_scale_factor = 0.02
);
ALTER TABLE canonical_passage_embeddings SET (
    autovacuum_vacuum_scale_factor = 0.05,
    autovacuum_analyze_scale_factor = 0.02
);
ALTER TABLE canonical_evidence_documents SET (
    autovacuum_vacuum_scale_factor = 0.05,
    autovacuum_analyze_scale_factor = 0.02
);
ALTER TABLE canonical_passage_documents SET (
    autovacuum_vacuum_scale_factor = 0.05,
    autovacuum_analyze_scale_factor = 0.02
);
ALTER TABLE canonical_evidence_document_parts SET (
    autovacuum_vacuum_scale_factor = 0.05,
    autovacuum_analyze_scale_factor = 0.02
);

INSERT INTO schema_migrations(version) VALUES (63) ON CONFLICT DO NOTHING;

COMMIT;
