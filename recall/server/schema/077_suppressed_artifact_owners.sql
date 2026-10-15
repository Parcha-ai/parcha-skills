-- Suppressed Slack revisions retain raw audit without inventing canonical events.
-- A raw capture can serve several identities; forget preserves retained owners.
CREATE TABLE IF NOT EXISTS canonical_suppressed_artifact_owners (
    tenant_id text NOT NULL,
    source_id text NOT NULL,
    identity_sha256 char(64) NOT NULL CHECK (identity_sha256 ~ '^[0-9a-f]{64}$'),
    artifact_id text NOT NULL,
    PRIMARY KEY (tenant_id, source_id, identity_sha256, artifact_id),
    FOREIGN KEY (tenant_id, source_id, artifact_id)
        REFERENCES raw_artifacts(tenant_id, source_id, artifact_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS canonical_suppressed_artifact_reverse_idx
    ON canonical_suppressed_artifact_owners(tenant_id, source_id, artifact_id, identity_sha256);
-- An artifact reference must not be admitted again after its raw bytes are
-- forgotten, even after the raw registry row and operational logs are removed.
CREATE TABLE IF NOT EXISTS canonical_artifact_forget_tombstones (
    tenant_id text NOT NULL,
    source_id text NOT NULL,
    artifact_id text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, source_id, artifact_id),
    FOREIGN KEY (tenant_id, source_id) REFERENCES canonical_sources(tenant_id, source_id)
);
INSERT INTO schema_migrations(version) VALUES (77) ON CONFLICT DO NOTHING;
