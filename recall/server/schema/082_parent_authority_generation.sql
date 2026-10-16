BEGIN;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM schema_migrations WHERE version=80)
       OR NOT EXISTS (
           SELECT 1 FROM pg_attribute
            WHERE attrelid='canonical_documents'::regclass
              AND attname='chunk_receipt_profile' AND NOT attisdropped
              AND atttypid='smallint'::regtype AND NOT attnotnull AND NOT atthasdef
       ) THEN
        RAISE EXCEPTION 'parent_authority_profile_schema_required';
    END IF;
END;
$$;

-- Shadow mutation state only. No reader or publication certificate uses this
-- generation. It survives disposable catalogs and identity deletion/recreation.
CREATE TABLE IF NOT EXISTS canonical_parent_authority (
    tenant_id text NOT NULL,
    source_id text NOT NULL,
    native_parent_id text NOT NULL,
    generation bigint NOT NULL CHECK (generation > 0),
    PRIMARY KEY (tenant_id, source_id, native_parent_id)
);

COMMENT ON TABLE canonical_parent_authority IS
    'Shadow receipt-membership generation, not an ACL, publication certificate or complete payload version. No cascade FK: deletion/recreation must not reset a version.';

CREATE OR REPLACE FUNCTION recall_flush_parent_authority()
RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog, public AS $$
DECLARE affected record;
BEGIN
    -- The first deferred event flushes the complete transaction-local key set.
    -- Later events see an empty set. No published parent or queue lock is taken.
    FOR affected IN
        SELECT tenant_id,source_id,native_parent_id
          FROM pg_temp.recall_parent_authority_pending
         ORDER BY tenant_id,source_id,native_parent_id
    LOOP
        INSERT INTO public.canonical_parent_authority
            (tenant_id,source_id,native_parent_id,generation)
        VALUES (affected.tenant_id,affected.source_id,affected.native_parent_id,1)
        ON CONFLICT (tenant_id,source_id,native_parent_id)
        DO UPDATE SET generation=canonical_parent_authority.generation+1;
    END LOOP;
    DELETE FROM pg_temp.recall_parent_authority_pending;
    RETURN NULL;
END;
$$;

CREATE OR REPLACE FUNCTION recall_collect_parent_authority()
RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog, public AS $$
DECLARE fields text; changed text; parents text;
BEGIN
    -- Compare only membership identity/currentness, not physical body storage.
    -- EXCEPT handles nullable profiles and no-op updates without row-by-row SQL.
    CASE TG_TABLE_NAME
      WHEN 'canonical_events' THEN
        fields := 'tenant_id,source_id,event_id,native_id,native_parent_id';
        parents := 'SELECT tenant_id,source_id,coalesce(native_parent_id,native_id) AS native_parent_id FROM changed';
      WHEN 'canonical_documents' THEN
        fields := 'tenant_id,source_id,document_id,event_id,native_id,revision,is_current,deleted_at,chunk_receipt_profile';
        parents := 'SELECT changed.tenant_id,changed.source_id,coalesce(event.native_parent_id,event.native_id) AS native_parent_id
          FROM changed JOIN public.canonical_events event USING(tenant_id,source_id,event_id)';
      WHEN 'canonical_chunks' THEN
        fields := 'tenant_id,source_id,chunk_id,document_id,ordinal,receipt,deleted_at';
        -- Resolve each affected document before joining its event. Reordering
        -- the three-way join can multiply chunks by every event in a source.
        parents := 'WITH affected_documents AS MATERIALIZED (
          SELECT document.tenant_id,document.source_id,document.event_id
          FROM (SELECT DISTINCT tenant_id,source_id,document_id FROM changed) keys
          JOIN public.canonical_documents document USING(tenant_id,source_id,document_id)
        )
        SELECT DISTINCT affected_documents.tenant_id,affected_documents.source_id,
          coalesce(event.native_parent_id,event.native_id) AS native_parent_id
        FROM affected_documents JOIN public.canonical_events event USING(tenant_id,source_id,event_id)';
      ELSE
        RAISE EXCEPTION 'unexpected parent authority table';
    END CASE;
    CASE TG_OP
      WHEN 'INSERT' THEN changed := format('SELECT %s FROM authority_new',fields);
      WHEN 'DELETE' THEN changed := format('SELECT %s FROM authority_old',fields);
      WHEN 'UPDATE' THEN changed := format(
        '(SELECT %1$s FROM authority_old EXCEPT SELECT %1$s FROM authority_new)
         UNION ALL
         (SELECT %1$s FROM authority_new EXCEPT SELECT %1$s FROM authority_old)',fields);
      ELSE RAISE EXCEPTION 'unexpected parent authority operation';
    END CASE;

    IF to_regclass('pg_temp.recall_parent_authority_pending') IS NULL THEN
        CREATE TEMP TABLE recall_parent_authority_pending (
            tenant_id text NOT NULL, source_id text NOT NULL,
            native_parent_id text NOT NULL,
            PRIMARY KEY (tenant_id,source_id,native_parent_id)
        ) ON COMMIT DELETE ROWS;
        CREATE CONSTRAINT TRIGGER recall_parent_authority_flush
            AFTER INSERT ON recall_parent_authority_pending
            DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
            EXECUTE FUNCTION public.recall_flush_parent_authority();
    END IF;
    EXECUTE 'WITH changed AS (' || changed || ')
        INSERT INTO pg_temp.recall_parent_authority_pending
            (tenant_id,source_id,native_parent_id) '
        || parents || ' ON CONFLICT DO NOTHING';
    RETURN NULL;
END;
$$;

-- Transition tables collect OLD and NEW keys in batches. Event deletion also
-- carries the parent identity directly, independently of descendant lookups.
DO $$
DECLARE relation_name text;
BEGIN
    FOREACH relation_name IN ARRAY ARRAY['canonical_events','canonical_documents','canonical_chunks'] LOOP
        EXECUTE format('DROP TRIGGER IF EXISTS recall_parent_authority_insert ON %I',relation_name);
        EXECUTE format('DROP TRIGGER IF EXISTS recall_parent_authority_update ON %I',relation_name);
        EXECUTE format('DROP TRIGGER IF EXISTS recall_parent_authority_delete ON %I',relation_name);
        EXECUTE format('CREATE TRIGGER recall_parent_authority_insert AFTER INSERT ON %I
            REFERENCING NEW TABLE AS authority_new FOR EACH STATEMENT
            EXECUTE FUNCTION recall_collect_parent_authority()',relation_name);
        EXECUTE format('CREATE TRIGGER recall_parent_authority_update AFTER UPDATE ON %I
            REFERENCING OLD TABLE AS authority_old NEW TABLE AS authority_new FOR EACH STATEMENT
            EXECUTE FUNCTION recall_collect_parent_authority()',relation_name);
        EXECUTE format('CREATE TRIGGER recall_parent_authority_delete AFTER DELETE ON %I
            REFERENCING OLD TABLE AS authority_old FOR EACH STATEMENT
            EXECUTE FUNCTION recall_collect_parent_authority()',relation_name);
    END LOOP;
END;
$$;

INSERT INTO schema_migrations(version) VALUES (82) ON CONFLICT DO NOTHING;
COMMIT;
