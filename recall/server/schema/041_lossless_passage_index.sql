BEGIN;

CREATE TABLE IF NOT EXISTS canonical_passage_documents (
    tenant_id text NOT NULL,
    source_id text NOT NULL,
    logical_document_id text NOT NULL,
    revision integer NOT NULL CHECK (revision >= 1),
    policy_fingerprint char(64) NOT NULL,
    target_tokens integer NOT NULL CHECK (target_tokens BETWEEN 4 AND 8192),
    overlap_tokens integer NOT NULL
        CHECK (overlap_tokens >= 0 AND overlap_tokens < target_tokens),
    source_document_sha256 char(64) NOT NULL,
    dense_message_count integer NOT NULL CHECK (dense_message_count >= 0),
    dense_message_bytes bigint NOT NULL CHECK (dense_message_bytes >= 0),
    passage_count integer NOT NULL CHECK (passage_count >= 0),
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY(tenant_id, source_id, logical_document_id),
    UNIQUE(
        tenant_id, source_id, logical_document_id, revision,
        policy_fingerprint
    ),
    FOREIGN KEY(tenant_id, source_id, logical_document_id, revision)
        REFERENCES canonical_evidence_documents(
            tenant_id,source_id,logical_document_id,revision
        )
        ON DELETE CASCADE,
    CHECK (policy_fingerprint ~ '^[0-9a-f]{64}$'),
    CHECK (source_document_sha256 ~ '^[0-9a-f]{64}$')
);

CREATE TABLE IF NOT EXISTS canonical_passages (
    tenant_id text NOT NULL,
    source_id text NOT NULL,
    logical_document_id text NOT NULL,
    revision integer NOT NULL CHECK (revision >= 1),
    passage_id text NOT NULL,
    ordinal integer NOT NULL CHECK (ordinal >= 0),
    policy_fingerprint char(64) NOT NULL,
    target_tokens integer NOT NULL CHECK (target_tokens BETWEEN 4 AND 8192),
    overlap_tokens integer NOT NULL
        CHECK (overlap_tokens >= 0 AND overlap_tokens < target_tokens),
    token_count integer NOT NULL
        CHECK (token_count >= 1 AND token_count <= target_tokens),
    first_occurred_at timestamptz NOT NULL,
    last_occurred_at timestamptz NOT NULL,
    roles text[] NOT NULL CHECK (cardinality(roles) >= 1),
    receipts text[] NOT NULL CHECK (cardinality(receipts) >= 1),
    spans jsonb NOT NULL CHECK (jsonb_typeof(spans) = 'array'),
    text_redacted text NOT NULL,
    text_sha256 char(64) NOT NULL,
    search_vector tsvector
        GENERATED ALWAYS AS (to_tsvector('simple', text_redacted)) STORED,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY(tenant_id, source_id, passage_id),
    UNIQUE(
        tenant_id, source_id, logical_document_id, revision,
        policy_fingerprint, ordinal
    ),
    FOREIGN KEY(
        tenant_id, source_id, logical_document_id, revision,
        policy_fingerprint
    )
        REFERENCES canonical_passage_documents(
            tenant_id,source_id,logical_document_id,revision,
            policy_fingerprint
        )
        ON DELETE CASCADE,
    CHECK (passage_id ~ '^psg_[0-9a-f]{32}$'),
    CHECK (policy_fingerprint ~ '^[0-9a-f]{64}$'),
    CHECK (text_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (roles <@ ARRAY['assistant','user']::text[]),
    CHECK (first_occurred_at <= last_occurred_at)
);

CREATE INDEX IF NOT EXISTS canonical_passages_document_idx
    ON canonical_passages(
        tenant_id, source_id, logical_document_id, revision, ordinal
    );

CREATE INDEX IF NOT EXISTS canonical_passages_time_idx
    ON canonical_passages(
        tenant_id, source_id, first_occurred_at, last_occurred_at
    );

CREATE INDEX IF NOT EXISTS canonical_passages_search_idx
    ON canonical_passages USING gin(search_vector);

CREATE TABLE IF NOT EXISTS canonical_passage_embeddings (
    tenant_id text NOT NULL,
    source_id text NOT NULL,
    passage_id text NOT NULL,
    model text NOT NULL,
    dimensions smallint NOT NULL CHECK (dimensions = 512),
    content_sha256 char(64) NOT NULL,
    runtime_fingerprint text NOT NULL,
    embedding halfvec(512) NOT NULL,
    embedded_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY(tenant_id, source_id, passage_id),
    FOREIGN KEY(tenant_id, source_id, passage_id)
        REFERENCES canonical_passages(tenant_id, source_id, passage_id)
        ON DELETE CASCADE,
    CHECK (content_sha256 ~ '^[0-9a-f]{64}$')
);

CREATE INDEX IF NOT EXISTS canonical_passage_embeddings_scope_idx
    ON canonical_passage_embeddings(tenant_id, source_id, passage_id);

CREATE INDEX IF NOT EXISTS canonical_passage_embeddings_hnsw_idx
    ON canonical_passage_embeddings
    USING hnsw (embedding halfvec_cosine_ops)
    WITH (m = 16, ef_construction = 64);

CREATE TABLE IF NOT EXISTS canonical_passage_projection_queue (
    tenant_id text NOT NULL,
    source_id text NOT NULL,
    logical_document_id text NOT NULL,
    revision integer NOT NULL CHECK (revision >= 1),
    generation bigint NOT NULL DEFAULT 1 CHECK (generation >= 1),
    reason text NOT NULL CHECK (reason IN ('backfill', 'logical-update')),
    changed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(tenant_id, source_id, logical_document_id),
    FOREIGN KEY(tenant_id, source_id, logical_document_id, revision)
        REFERENCES canonical_evidence_documents(
            tenant_id,source_id,logical_document_id,revision
        )
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS canonical_passage_projection_queue_work_idx
    ON canonical_passage_projection_queue(
        tenant_id, changed_at, source_id, logical_document_id
    );

INSERT INTO schema_migrations(version) VALUES (41) ON CONFLICT DO NOTHING;

COMMIT;
