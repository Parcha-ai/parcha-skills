BEGIN;

ALTER TABLE brain_invitations
    ADD COLUMN IF NOT EXISTS actor_display_name text;

ALTER TABLE brain_invitations
    DROP CONSTRAINT IF EXISTS brain_invitations_actor_display_name_check;

ALTER TABLE brain_invitations
    ADD CONSTRAINT brain_invitations_actor_display_name_check
    CHECK (
        actor_display_name IS NULL
        OR length(actor_display_name) BETWEEN 1 AND 200
    );

INSERT INTO schema_migrations(version) VALUES (46) ON CONFLICT DO NOTHING;

COMMIT;
