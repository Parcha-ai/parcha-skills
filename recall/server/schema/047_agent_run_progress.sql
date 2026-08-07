BEGIN;

ALTER TABLE agent_runs
    ADD COLUMN IF NOT EXISTS status_message text NOT NULL DEFAULT 'queued';

ALTER TABLE agent_runs
    DROP CONSTRAINT IF EXISTS agent_runs_status_message_check;

ALTER TABLE agent_runs
    ADD CONSTRAINT agent_runs_status_message_check CHECK (
        status_message IN (
            'queued', 'planning', 'searching', 'inspecting',
            'synthesizing', 'verifying', 'completed', 'failed', 'cancelled'
        )
    );

UPDATE agent_runs
   SET status_message = CASE
       WHEN status IN ('complete', 'partial', 'no_answer') THEN 'completed'
       WHEN status = 'failed' THEN 'failed'
       WHEN status = 'cancelled' THEN 'cancelled'
       WHEN status = 'running' THEN 'planning'
       ELSE 'queued'
   END;

INSERT INTO schema_migrations(version) VALUES (47) ON CONFLICT DO NOTHING;

COMMIT;
