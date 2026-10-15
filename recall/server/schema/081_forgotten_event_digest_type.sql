BEGIN;
-- Match the existing digest column type so the forget check uses its full key.

CREATE OR REPLACE FUNCTION recall_v2_reject_forgotten_event()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    target_sha256 char(64);
BEGIN
    target_sha256 := encode(
        sha256(convert_to(
            NEW.tenant_id || chr(31) || NEW.source_id || chr(31) || NEW.native_id,
            'UTF8'
        )),
        'hex'
    );
    IF EXISTS (
        SELECT 1
        FROM forget_tombstones tombstone
        WHERE tombstone.tenant_id=NEW.tenant_id
          AND tombstone.source_id=NEW.source_id
          AND tombstone.target_identity_sha256=target_sha256
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE='23514',
            MESSAGE='canonical identity was forgotten';
    END IF;
    RETURN NEW;
END;
$$;

INSERT INTO schema_migrations(version) VALUES (81) ON CONFLICT DO NOTHING;

COMMIT;
