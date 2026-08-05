BEGIN;

ALTER TABLE admin_sessions
    ADD COLUMN IF NOT EXISTS principal_id text;

UPDATE admin_sessions session
   SET principal_id=credential.principal_id
  FROM admin_credentials credential
 WHERE session.credential_id=credential.id
   AND session.principal_id IS NULL;

ALTER TABLE admin_sessions
    ALTER COLUMN principal_id SET NOT NULL,
    ALTER COLUMN credential_id DROP NOT NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
         WHERE conname='admin_sessions_credential_or_identity_check'
    ) THEN
        ALTER TABLE admin_sessions ADD CONSTRAINT
            admin_sessions_credential_or_identity_check
            CHECK (credential_id IS NOT NULL OR principal_id IS NOT NULL);
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS identity_oauth_states (
    state_sha256 char(64) PRIMARY KEY,
    encrypted_context bytea NOT NULL,
    encryption_key_id text NOT NULL,
    expires_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    consumed_at timestamptz,
    CHECK (length(encryption_key_id) BETWEEN 1 AND 128),
    CHECK (expires_at > created_at)
);

CREATE INDEX IF NOT EXISTS identity_oauth_states_active_idx
    ON identity_oauth_states(state_sha256, expires_at)
    WHERE consumed_at IS NULL;

INSERT INTO schema_migrations(version) VALUES (44) ON CONFLICT DO NOTHING;

COMMIT;
