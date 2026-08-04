BEGIN;

CREATE TABLE IF NOT EXISTS canonical_evidence_documents (
    tenant_id text NOT NULL,
    source_id text NOT NULL,
    logical_document_id text NOT NULL,
    native_parent_id text NOT NULL,
    revision integer NOT NULL CHECK (revision >= 1),
    evidence_id text NOT NULL,
    manifest_artifact_id text NOT NULL,
    manifest_storage_backend text NOT NULL
        CHECK (manifest_storage_backend IN ('filesystem', 's3')),
    manifest_object_key text NOT NULL,
    manifest_content_sha256 char(64) NOT NULL,
    manifest_size_bytes bigint NOT NULL
        CHECK (manifest_size_bytes >= 0 AND manifest_size_bytes <= 5368709120),
    manifest_media_type text NOT NULL,
    manifest_encryption text NOT NULL
        CHECK (
            manifest_encryption IN (
                'filesystem-owner-only', 'sse-s3', 'sse-kms', 'sse-c'
            )
        ),
    manifest_version_id text NOT NULL,
    document_content_sha256 char(64) NOT NULL,
    record_count integer NOT NULL CHECK (record_count >= 1),
    receipt_count integer NOT NULL CHECK (receipt_count >= 1),
    part_count integer NOT NULL CHECK (part_count >= 1),
    first_occurred_at timestamptz NOT NULL,
    last_occurred_at timestamptz NOT NULL,
    source_updated_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY(tenant_id, source_id, logical_document_id),
    UNIQUE(tenant_id, source_id, native_parent_id),
    UNIQUE(tenant_id, source_id, logical_document_id, revision),
    UNIQUE(tenant_id, source_id, evidence_id),
    UNIQUE(manifest_storage_backend, manifest_object_key, manifest_version_id),
    FOREIGN KEY(tenant_id, source_id)
        REFERENCES canonical_sources(tenant_id, source_id),
    CHECK (logical_document_id ~ '^ldoc_[0-9a-f]{32}$'),
    CHECK (evidence_id ~ '^evd_[0-9a-f]{32}$'),
    CHECK (manifest_content_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (document_content_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (manifest_object_key ~ '^objects/[0-9a-f]{2}/[0-9a-f]{64}$'),
    CHECK (
        manifest_media_type =
        'application/vnd.recall.logical-document-manifest+json'
    ),
    CHECK (first_occurred_at <= last_occurred_at)
);

CREATE INDEX IF NOT EXISTS canonical_evidence_documents_current_source_idx
    ON canonical_evidence_documents(
        tenant_id, source_id, source_updated_at, logical_document_id
    );

CREATE TABLE IF NOT EXISTS canonical_evidence_document_parts (
    tenant_id text NOT NULL,
    source_id text NOT NULL,
    logical_document_id text NOT NULL,
    revision integer NOT NULL,
    part_ordinal integer NOT NULL CHECK (part_ordinal >= 0),
    artifact_id text NOT NULL,
    storage_backend text NOT NULL CHECK (storage_backend IN ('filesystem', 's3')),
    object_key text NOT NULL,
    content_sha256 char(64) NOT NULL,
    size_bytes bigint NOT NULL
        CHECK (size_bytes >= 1 AND size_bytes <= 5368709120),
    media_type text NOT NULL,
    encryption text NOT NULL
        CHECK (
            encryption IN (
                'filesystem-owner-only', 'sse-s3', 'sse-kms', 'sse-c'
            )
        ),
    version_id text NOT NULL,
    first_record_ordinal integer NOT NULL CHECK (first_record_ordinal >= 0),
    last_record_ordinal integer NOT NULL CHECK (last_record_ordinal >= 0),
    receipt_count integer NOT NULL CHECK (receipt_count >= 0),
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY(
        tenant_id, source_id, logical_document_id, revision, part_ordinal
    ),
    UNIQUE(storage_backend, object_key, version_id),
    FOREIGN KEY(tenant_id, source_id, logical_document_id, revision)
        REFERENCES canonical_evidence_documents(
            tenant_id, source_id, logical_document_id, revision
        )
        ON DELETE CASCADE,
    CHECK (content_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (object_key ~ '^objects/[0-9a-f]{2}/[0-9a-f]{64}$'),
    CHECK (media_type='application/vnd.recall.logical-document-part+jsonl'),
    CHECK (first_record_ordinal <= last_record_ordinal)
);

CREATE TABLE IF NOT EXISTS canonical_evidence_document_queue (
    tenant_id text NOT NULL,
    source_id text NOT NULL,
    native_parent_id text NOT NULL,
    generation bigint NOT NULL DEFAULT 1 CHECK (generation >= 1),
    reason text NOT NULL CHECK (reason IN ('backfill', 'ingest', 'forget')),
    changed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(tenant_id, source_id, native_parent_id),
    FOREIGN KEY(tenant_id, source_id)
        REFERENCES canonical_sources(tenant_id, source_id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS canonical_evidence_document_queue_work_idx
    ON canonical_evidence_document_queue(
        tenant_id, changed_at, source_id, native_parent_id
    );

CREATE TABLE IF NOT EXISTS canonical_evidence_cleanup_queue (
    tenant_id text NOT NULL,
    source_id text NOT NULL,
    artifact_id text NOT NULL,
    storage_backend text NOT NULL CHECK (storage_backend IN ('filesystem', 's3')),
    object_key text NOT NULL,
    content_sha256 char(64) NOT NULL,
    size_bytes bigint NOT NULL
        CHECK (size_bytes >= 0 AND size_bytes <= 5368709120),
    media_type text NOT NULL,
    encryption text NOT NULL
        CHECK (
            encryption IN (
                'filesystem-owner-only', 'sse-s3', 'sse-kms', 'sse-c'
            )
        ),
    version_id text NOT NULL,
    created_at timestamptz NOT NULL,
    queued_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    last_attempt_at timestamptz,
    PRIMARY KEY(tenant_id, source_id, artifact_id),
    UNIQUE(storage_backend, object_key, version_id),
    FOREIGN KEY(tenant_id, source_id)
        REFERENCES canonical_sources(tenant_id, source_id)
        ON DELETE CASCADE,
    CHECK (artifact_id ~ '^art_[0-9a-f]{32}$'),
    CHECK (content_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (object_key ~ '^objects/[0-9a-f]{2}/[0-9a-f]{64}$')
);

CREATE INDEX IF NOT EXISTS canonical_evidence_cleanup_queue_work_idx
    ON canonical_evidence_cleanup_queue(queued_at, tenant_id, source_id);

INSERT INTO schema_migrations(version) VALUES (39) ON CONFLICT DO NOTHING;

COMMIT;
