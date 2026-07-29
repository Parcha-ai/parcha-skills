BEGIN;

CREATE TABLE IF NOT EXISTS canonical_evidence_objects (
    tenant_id text NOT NULL,
    source_id text NOT NULL,
    document_id text NOT NULL,
    evidence_id text NOT NULL,
    artifact_id text NOT NULL,
    storage_backend text NOT NULL CHECK (storage_backend IN ('filesystem', 's3')),
    object_key text NOT NULL,
    content_sha256 char(64) NOT NULL,
    size_bytes bigint NOT NULL CHECK (size_bytes >= 0 AND size_bytes <= 5368709120),
    media_type text NOT NULL,
    encryption text NOT NULL CHECK (encryption IN ('filesystem-owner-only', 'sse-s3', 'sse-kms', 'sse-c')),
    version_id text NOT NULL,
    text_sha256 char(64) NOT NULL,
    revision integer NOT NULL CHECK (revision >= 1),
    receipt_count integer NOT NULL CHECK (receipt_count >= 1),
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY(tenant_id, source_id, document_id),
    UNIQUE(tenant_id, source_id, evidence_id),
    UNIQUE(storage_backend, object_key, version_id),
    FOREIGN KEY(tenant_id, source_id, document_id)
        REFERENCES canonical_documents(tenant_id, source_id, document_id),
    CHECK (evidence_id ~ '^evd_[0-9a-f]{32}$'),
    CHECK (content_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (text_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (object_key ~ '^objects/[0-9a-f]{2}/[0-9a-f]{64}$'),
    CHECK (media_type='application/vnd.recall.evidence+json')
);

CREATE INDEX IF NOT EXISTS canonical_evidence_receipt_lookup_idx
    ON canonical_evidence_objects(tenant_id, source_id, document_id);

INSERT INTO schema_migrations(version) VALUES (35) ON CONFLICT DO NOTHING;

COMMIT;
