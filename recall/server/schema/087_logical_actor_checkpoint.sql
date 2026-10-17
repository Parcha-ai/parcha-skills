BEGIN;
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM schema_migrations WHERE version=79) THEN
        RAISE EXCEPTION 'logical_actor_checkpoint_requires_append_capability';
    END IF;
END $$;
ALTER TABLE canonical_evidence_documents ADD COLUMN IF NOT EXISTS actor_checkpoint jsonb;
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_attribute
        WHERE attrelid='canonical_evidence_documents'::regclass
          AND attname='actor_checkpoint' AND atttypid='jsonb'::regtype
          AND NOT attnotnull AND NOT atthasdef AND NOT attisdropped) THEN
        RAISE EXCEPTION 'logical_actor_checkpoint_column_invalid';
    END IF;
END $$;
CREATE OR REPLACE FUNCTION invalidate_logical_actor_checkpoint_on_ack()
RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog, public AS $$ BEGIN
    -- Native publication owns queue before catalog. NOWAIT also keeps an old
    -- identical-body repair from waiting behind an inverted catalog holder.
    WITH locked AS MATERIALIZED (
        SELECT tenant_id,source_id,logical_document_id
        FROM canonical_evidence_documents
        WHERE tenant_id=OLD.tenant_id AND source_id=OLD.source_id
          AND native_parent_id=OLD.native_parent_id AND actor_checkpoint IS NOT NULL
        FOR UPDATE NOWAIT
    ) UPDATE canonical_evidence_documents document SET actor_checkpoint=NULL
      FROM locked WHERE document.tenant_id=locked.tenant_id
        AND document.source_id=locked.source_id
        AND document.logical_document_id=locked.logical_document_id;
    RETURN NULL;
END $$;
CREATE TRIGGER logical_actor_checkpoint_ack
AFTER DELETE ON canonical_evidence_document_queue
FOR EACH ROW EXECUTE FUNCTION invalidate_logical_actor_checkpoint_on_ack();
COMMENT ON COLUMN canonical_evidence_documents.actor_checkpoint IS
    'Bounded event-only actor completeness for supported native dirtying owners. NULL forces full actor publication; source bindings are always read fresh.';
INSERT INTO schema_migrations(version) VALUES (87) ON CONFLICT DO NOTHING;
COMMIT;
