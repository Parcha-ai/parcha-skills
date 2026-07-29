BEGIN;

ALTER TABLE mcp_credentials
    ADD COLUMN IF NOT EXISTS principal_kind text NOT NULL DEFAULT 'workload'
        CHECK (principal_kind IN ('human', 'workload'));

CREATE TABLE IF NOT EXISTS external_identity_bindings (
    issuer text NOT NULL,
    subject_sha256 char(64) NOT NULL,
    tenant_id text NOT NULL,
    principal_id text NOT NULL,
    principal_kind text NOT NULL CHECK (principal_kind IN ('human', 'workload')),
    created_at timestamptz NOT NULL DEFAULT now(),
    revoked_at timestamptz,
    PRIMARY KEY(issuer, subject_sha256, tenant_id),
    CHECK (issuer ~ '^https://'),
    CHECK (subject_sha256 ~ '^[0-9a-f]{64}$'),
    FOREIGN KEY(tenant_id, principal_id)
        REFERENCES brain_principals(tenant_id, principal_id)
);

CREATE INDEX IF NOT EXISTS external_identity_bindings_active_idx
    ON external_identity_bindings(issuer, subject_sha256, tenant_id)
    WHERE revoked_at IS NULL;

CREATE TABLE IF NOT EXISTS brain_invitations (
    id uuid PRIMARY KEY,
    tenant_id text NOT NULL REFERENCES brain_spaces(tenant_id),
    email_sha256 char(64) NOT NULL CHECK (email_sha256 ~ '^[0-9a-f]{64}$'),
    encrypted_email bytea NOT NULL,
    encryption_key_id text NOT NULL,
    role text NOT NULL CHECK (role IN ('admin', 'member')),
    invited_by_principal_id text NOT NULL,
    accepted_principal_id text,
    expires_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    accepted_at timestamptz,
    revoked_at timestamptz,
    CHECK (expires_at > created_at),
    CHECK (length(encryption_key_id) BETWEEN 1 AND 128),
    CHECK (
        (accepted_at IS NULL AND accepted_principal_id IS NULL)
        OR (accepted_at IS NOT NULL AND accepted_principal_id IS NOT NULL)
    ),
    FOREIGN KEY(tenant_id, invited_by_principal_id)
        REFERENCES brain_principals(tenant_id, principal_id),
    FOREIGN KEY(tenant_id, accepted_principal_id)
        REFERENCES brain_principals(tenant_id, principal_id)
);

CREATE INDEX IF NOT EXISTS brain_invitations_pending_idx
    ON brain_invitations(tenant_id, email_sha256, expires_at)
    WHERE accepted_at IS NULL AND revoked_at IS NULL;

CREATE INDEX IF NOT EXISTS brain_invitations_member_idx
    ON brain_invitations(tenant_id, accepted_principal_id, created_at DESC);

CREATE TABLE IF NOT EXISTS authorization_audit_events (
    id bigserial PRIMARY KEY,
    policy_version text NOT NULL CHECK (policy_version = 'recall.authorization.v1'),
    principal_kind text NOT NULL CHECK (principal_kind IN ('human', 'workload')),
    principal_id text NOT NULL,
    tenant_id text NOT NULL,
    action text NOT NULL,
    decision text NOT NULL CHECK (decision IN ('allowed', 'denied')),
    reason text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    CHECK (action ~ '^mcp\.[a-z_]+$'),
    CHECK (reason ~ '^[a-z_]+$')
);

CREATE INDEX IF NOT EXISTS authorization_audit_events_lookup_idx
    ON authorization_audit_events(tenant_id, principal_id, created_at DESC);

INSERT INTO schema_migrations(version) VALUES (32) ON CONFLICT DO NOTHING;

COMMIT;
