BEGIN;

ALTER TABLE raw_artifacts
    DROP CONSTRAINT IF EXISTS raw_artifacts_encryption_check,
    DROP CONSTRAINT IF EXISTS raw_artifacts_state_check,
    DROP CONSTRAINT IF EXISTS raw_artifacts_check,
    DROP CONSTRAINT IF EXISTS raw_artifacts_lifecycle_check;

ALTER TABLE raw_artifacts
    ADD CONSTRAINT raw_artifacts_encryption_check
        CHECK (encryption IN ('filesystem-owner-only', 'sse-s3', 'sse-kms', 'sse-c')),
    ADD CONSTRAINT raw_artifacts_state_check
        CHECK (state IN ('live', 'deleting', 'deleted')),
    ADD CONSTRAINT raw_artifacts_lifecycle_check
        CHECK (
            (state IN ('live', 'deleting') AND deleted_at IS NULL)
            OR (state='deleted' AND deleted_at IS NOT NULL)
        );

ALTER TABLE canonical_ingest_jobs
    DROP CONSTRAINT IF EXISTS canonical_ingest_jobs_mode_check,
    DROP CONSTRAINT IF EXISTS canonical_ingest_jobs_status_check;

ALTER TABLE canonical_ingest_jobs
    ADD CONSTRAINT canonical_ingest_jobs_mode_check
        CHECK (mode IN ('backfill', 'incremental', 'reconcile', 'forget')),
    ADD CONSTRAINT canonical_ingest_jobs_status_check
        CHECK (status IN ('queued', 'leased', 'committed', 'retryable', 'parked', 'failed'));

ALTER TABLE forget_tombstones
    DROP CONSTRAINT IF EXISTS forget_tombstones_reason_check,
    DROP CONSTRAINT IF EXISTS forget_tombstones_status_check,
    DROP CONSTRAINT IF EXISTS forget_tombstones_completion_check,
    DROP CONSTRAINT IF EXISTS forget_tombstones_idempotency_check;

ALTER TABLE forget_tombstones
    ADD COLUMN IF NOT EXISTS status text NOT NULL DEFAULT 'deleted',
    ADD COLUMN IF NOT EXISTS idempotency_key text,
    ADD COLUMN IF NOT EXISTS completed_at timestamptz;

UPDATE forget_tombstones
SET completed_at=deleted_at
WHERE status='deleted' AND completed_at IS NULL;

ALTER TABLE forget_tombstones
    ADD CONSTRAINT forget_tombstones_reason_check
        CHECK (reason IN ('upstream_deleted', 'owner_requested', 'retention_expired')),
    ADD CONSTRAINT forget_tombstones_status_check
        CHECK (status IN ('deleting', 'deleted')),
    ADD CONSTRAINT forget_tombstones_completion_check
        CHECK (
            (status='deleting' AND completed_at IS NULL)
            OR (status='deleted' AND completed_at IS NOT NULL)
        ),
    ADD CONSTRAINT forget_tombstones_idempotency_check
        CHECK (idempotency_key IS NULL OR length(idempotency_key) BETWEEN 1 AND 200);

CREATE UNIQUE INDEX IF NOT EXISTS forget_tombstones_idempotency_idx
    ON forget_tombstones(tenant_id, source_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

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

DROP TRIGGER IF EXISTS canonical_events_reject_forgotten
    ON canonical_events;
CREATE TRIGGER canonical_events_reject_forgotten
BEFORE INSERT ON canonical_events
FOR EACH ROW
EXECUTE FUNCTION recall_v2_reject_forgotten_event();

INSERT INTO schema_migrations(version) VALUES (20) ON CONFLICT DO NOTHING;

COMMIT;
