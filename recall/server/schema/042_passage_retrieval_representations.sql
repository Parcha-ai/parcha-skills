BEGIN;

CREATE TABLE IF NOT EXISTS canonical_passage_contexts (
    tenant_id text NOT NULL,
    source_id text NOT NULL,
    passage_id text NOT NULL,
    context_fingerprint char(64) NOT NULL,
    context_text_redacted text NOT NULL,
    context_sha256 char(64) NOT NULL,
    search_vector tsvector
        GENERATED ALWAYS AS (
            to_tsvector('simple', context_text_redacted)
        ) STORED,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY(
        tenant_id, source_id, passage_id, context_fingerprint
    ),
    FOREIGN KEY(tenant_id, source_id, passage_id)
        REFERENCES canonical_passages(tenant_id, source_id, passage_id)
        ON DELETE CASCADE,
    CHECK (context_fingerprint ~ '^[0-9a-f]{64}$'),
    CHECK (context_sha256 ~ '^[0-9a-f]{64}$')
);

CREATE INDEX IF NOT EXISTS canonical_passage_contexts_search_idx
    ON canonical_passage_contexts USING gin(search_vector);

CREATE TABLE IF NOT EXISTS canonical_passage_embedding_representations (
    tenant_id text NOT NULL,
    source_id text NOT NULL,
    passage_id text NOT NULL,
    representation_fingerprint char(64) NOT NULL,
    model text NOT NULL,
    dimensions smallint NOT NULL CHECK (dimensions = 512),
    content_sha256 char(64) NOT NULL,
    context_fingerprint char(64),
    embedding halfvec(512) NOT NULL,
    embedded_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY(
        tenant_id, source_id, passage_id, representation_fingerprint
    ),
    FOREIGN KEY(tenant_id, source_id, passage_id)
        REFERENCES canonical_passages(tenant_id, source_id, passage_id)
        ON DELETE CASCADE,
    FOREIGN KEY(
        tenant_id, source_id, passage_id, context_fingerprint
    )
        REFERENCES canonical_passage_contexts(
            tenant_id, source_id, passage_id, context_fingerprint
        )
        ON DELETE CASCADE,
    CHECK (representation_fingerprint ~ '^[0-9a-f]{64}$'),
    CHECK (content_sha256 ~ '^[0-9a-f]{64}$')
);

CREATE INDEX IF NOT EXISTS canonical_passage_embedding_representations_scope_idx
    ON canonical_passage_embedding_representations(
        tenant_id, source_id, representation_fingerprint, passage_id
    );

INSERT INTO schema_migrations(version) VALUES (42) ON CONFLICT DO NOTHING;

COMMIT;
