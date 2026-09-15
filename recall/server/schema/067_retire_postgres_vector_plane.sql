BEGIN;

-- H3-e': retire the Postgres vector/tsvector plane once the turbopuffer
-- search plane (RECALL_SEARCH_PLANE=turbopuffer) answers every passage arm.
-- Destructive: ``migrate`` applies this file only with
-- ``--retire-postgres-plane`` from a process whose RECALL_SEARCH_PLANE is
-- turbopuffer, and a store on the postgres plane refuses to start once
-- version 67 is recorded. Every statement is guarded so a rerun is a no-op.
--
-- ``canonical_chunks.search_vector`` (migration 028) stays: ``show`` and the
-- legacy sparse/identifier paths still read it.

-- Passage vectors (migration 041): the HNSW index and the halfvec table.
DROP INDEX IF EXISTS canonical_passage_embeddings_hnsw_idx;
DROP INDEX IF EXISTS canonical_passage_embeddings_scope_idx;
DROP TABLE IF EXISTS canonical_passage_embeddings;

-- The embedding worker's daily ledger (migration 065): nothing embeds from
-- this process any more.
DROP TABLE IF EXISTS canonical_embedding_ledger;

-- The passage tsvector (migration 041): the largest remaining generated
-- column and its GIN index. BM25 lives in the turbopuffer namespace.
DROP INDEX IF EXISTS canonical_passages_search_idx;
ALTER TABLE canonical_passages DROP COLUMN IF EXISTS search_vector;

INSERT INTO schema_migrations(version) VALUES (67) ON CONFLICT DO NOTHING;

COMMIT;
