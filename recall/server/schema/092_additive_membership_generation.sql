BEGIN;

-- Local spike: existing statement/deferred owner, no new persistent state.
CREATE OR REPLACE FUNCTION recall_collect_parent_authority()
RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog, public AS $$
DECLARE fields text; changed text; parents text; invalid_new boolean;
BEGIN
    -- All native writers supersede documents by INSERT. Receipt-defining
    -- identity is immutable; OLD malformed chunks can be repaired to it, but
    -- concurrent reidentification cannot race that repair under any snapshot.
    IF TG_TABLE_NAME='canonical_documents' AND TG_OP='UPDATE' THEN
      IF EXISTS(
        (SELECT tenant_id,source_id,document_id,native_id,revision FROM authority_old
         EXCEPT SELECT tenant_id,source_id,document_id,native_id,revision FROM authority_new)
        UNION ALL
        (SELECT tenant_id,source_id,document_id,native_id,revision FROM authority_new
         EXCEPT SELECT tenant_id,source_id,document_id,native_id,revision FROM authority_old)
    ) THEN
        RAISE EXCEPTION 'canonical_document_identity_immutable' USING ERRCODE='23514';
      END IF;
    END IF;
    -- A new physical receipt must identify its actual document and ordinal.
    -- Existing malformed rows may be deactivated, cleaned or repaired, but not
    -- newly installed or activated. Normal ingestion and archive restore already
    -- construct exactly this identity; this adds a database write invariant.
    IF TG_TABLE_NAME='canonical_chunks' AND TG_OP IN ('INSERT','UPDATE') THEN
        changed := 'SELECT inserted.* FROM authority_new inserted';
        IF TG_OP='UPDATE' THEN
            changed := changed || ' LEFT JOIN authority_old old_row USING(tenant_id,source_id,chunk_id)
              WHERE inserted.deleted_at IS NULL AND (old_row.chunk_id IS NULL OR old_row.deleted_at IS NOT NULL OR
                ROW(old_row.document_id,old_row.ordinal,old_row.receipt) IS DISTINCT FROM
                ROW(inserted.document_id,inserted.ordinal,inserted.receipt))';
        END IF;
        EXECUTE 'SELECT EXISTS(SELECT 1 FROM (' || changed || ') inserted
          LEFT JOIN public.canonical_documents document USING(tenant_id,source_id,document_id)
          WHERE document.document_id IS NULL OR inserted.receipt IS DISTINCT FROM
            ''recall://''||document.source_id||''/''||document.native_id||''?rev=''||
            document.revision::text||''#item=''||inserted.ordinal::text)' INTO invalid_new;
        IF invalid_new THEN
            RAISE EXCEPTION 'canonical_physical_receipt_identity_invalid' USING ERRCODE='23514';
        END IF;
    ELSIF TG_TABLE_NAME='canonical_documents' AND TG_OP='UPDATE' THEN
        SELECT EXISTS(SELECT 1 FROM authority_new new_row JOIN authority_old old_row
          USING(tenant_id,source_id,document_id)
          JOIN public.canonical_chunks chunk USING(tenant_id,source_id,document_id)
          WHERE new_row.is_current AND new_row.deleted_at IS NULL AND chunk.deleted_at IS NULL
            AND (NOT old_row.is_current OR old_row.deleted_at IS NOT NULL)
            AND chunk.receipt IS DISTINCT FROM
              'recall://'||new_row.source_id||'/'||new_row.native_id||'?rev='||
              new_row.revision::text||'#item='||chunk.ordinal::text) INTO invalid_new;
        IF invalid_new THEN
            RAISE EXCEPTION 'canonical_physical_receipt_identity_invalid' USING ERRCODE='23514';
        END IF;
    END IF;
    -- Unique event/document identity and currentness constraints make inserts
    -- additive. Supersession and deletion still invalidate via UPDATE/DELETE.
    IF TG_OP='INSERT' AND TG_TABLE_NAME IN ('canonical_events','canonical_documents') THEN
        RETURN NULL;
    END IF;
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

    IF TG_TABLE_NAME='canonical_chunks' THEN
        IF TG_OP='INSERT' THEN
            -- Only a tuple written by this transaction may exempt an INSERT.
            -- INSERT or UPDATE ownership blocks concurrent profile conversion;
            -- older tuples and subtransaction/full-XID mismatches invalidate.
            -- Profile documents always invalidate: physical ordinals can suppress
            -- derived membership. This is ownership, not an inserted-row test.
            changed := 'SELECT ' || fields || ' FROM authority_new inserted
              WHERE NOT EXISTS(SELECT 1 FROM public.canonical_documents document
                WHERE document.tenant_id=inserted.tenant_id
                  AND document.source_id=inserted.source_id
                  AND document.document_id=inserted.document_id
                  AND document.chunk_receipt_profile IS NULL
                  AND document.xmin::text=pg_current_xact_id()::text
                  AND document.is_current AND document.deleted_at IS NULL
                  AND inserted.deleted_at IS NULL
                  AND inserted.receipt=''recall://''||document.source_id||''/''||
                    document.native_id||''?rev=''||document.revision::text||
                    ''#item=''||inserted.ordinal::text)';
        ELSIF TG_OP='DELETE' THEN
            -- Exact physical-to-derived singleton transition, proved by SQL.
            -- The archive admission owner is unchanged; no caller exemption.
            changed := 'SELECT ' || fields || ' FROM authority_old removed
              WHERE NOT EXISTS(SELECT 1 FROM public.canonical_documents document
                WHERE document.tenant_id=removed.tenant_id
                  AND document.source_id=removed.source_id
                  AND document.document_id=removed.document_id
                  AND document.chunk_receipt_profile=1 AND document.is_current
                  AND document.deleted_at IS NULL AND removed.deleted_at IS NULL
                  AND removed.ordinal=0 AND removed.text_redacted=''''
                  AND removed.text_sha256=document.text_sha256
                  AND removed.receipt=''recall://''||document.source_id||''/''||
                    document.native_id||''?rev=''||document.revision::text||''#item=0''
                  AND NOT EXISTS(SELECT 1 FROM public.canonical_chunks remaining
                    WHERE remaining.tenant_id=document.tenant_id
                      AND remaining.source_id=document.source_id
                      AND remaining.document_id=document.document_id)
                  AND (SELECT count(*) FROM authority_old siblings
                    WHERE siblings.tenant_id=removed.tenant_id
                      AND siblings.source_id=removed.source_id
                      AND siblings.document_id=removed.document_id)=1)';
        END IF;
    ELSIF TG_TABLE_NAME='canonical_documents' AND TG_OP='UPDATE' THEN
        -- NULL->1 with an exact live singleton still has physical membership.
        -- Other identity/currentness/profile transitions keep invalidating.
        changed := 'WITH equivalent_singletons AS MATERIALIZED (
          SELECT old_row.tenant_id,old_row.source_id,old_row.document_id
          FROM authority_old old_row JOIN authority_new new_row
            USING(tenant_id,source_id,document_id)
          WHERE ROW(old_row.event_id,old_row.native_id,old_row.revision,old_row.is_current,old_row.deleted_at)
                  IS NOT DISTINCT FROM
                  ROW(new_row.event_id,new_row.native_id,new_row.revision,new_row.is_current,new_row.deleted_at)
              AND old_row.chunk_receipt_profile IS NULL AND new_row.chunk_receipt_profile=1
              AND new_row.is_current AND new_row.deleted_at IS NULL
              AND old_row.text_sha256=new_row.text_sha256
              AND (SELECT count(*) FROM public.canonical_chunks chunk
                WHERE chunk.tenant_id=new_row.tenant_id AND chunk.source_id=new_row.source_id
                  AND chunk.document_id=new_row.document_id)=1
              AND EXISTS(SELECT 1 FROM public.canonical_chunks chunk
                WHERE chunk.tenant_id=new_row.tenant_id AND chunk.source_id=new_row.source_id
                  AND chunk.document_id=new_row.document_id AND chunk.ordinal=0
                  AND chunk.deleted_at IS NULL AND chunk.text_sha256=new_row.text_sha256
                  AND chunk.receipt=''recall://''||new_row.source_id||''/''||new_row.native_id||
                    ''?rev=''||new_row.revision::text||''#item=0'')
        )
        SELECT changed.* FROM (' || changed || ') changed
          WHERE NOT EXISTS(SELECT 1 FROM equivalent_singletons equivalent
            WHERE equivalent.tenant_id=changed.tenant_id
              AND equivalent.source_id=changed.source_id
              AND equivalent.document_id=changed.document_id)';
    END IF;

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

INSERT INTO schema_migrations(version) VALUES (92) ON CONFLICT DO NOTHING;
COMMIT;
