BEGIN;

CREATE TABLE IF NOT EXISTS canonical_embedding_projection_watermarks (
    runtime_fingerprint text NOT NULL,
    tenant_scope text NOT NULL,
    last_tenant_id text NOT NULL DEFAULT '',
    last_source_id text NOT NULL DEFAULT '',
    last_chunk_id text NOT NULL DEFAULT '',
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY(runtime_fingerprint, tenant_scope),
    CHECK (length(runtime_fingerprint) BETWEEN 1 AND 256),
    CHECK (length(tenant_scope) <= 256),
    CHECK (length(last_tenant_id) <= 256),
    CHECK (length(last_source_id) <= 256),
    CHECK (length(last_chunk_id) <= 512)
);

CREATE INDEX IF NOT EXISTS canonical_chunk_embeddings_runtime_cursor_idx
    ON canonical_chunk_embeddings(
        runtime_fingerprint, tenant_id, source_id, chunk_id
    );

INSERT INTO schema_migrations(version) VALUES (36) ON CONFLICT DO NOTHING;

COMMIT;
