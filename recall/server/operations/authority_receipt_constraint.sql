BEGIN;
SET LOCAL lock_timeout = '1s';
LOCK TABLE canonical_chunks IN ACCESS EXCLUSIVE MODE;

DO $$
DECLARE
    expected record;
    actual record;
    chunks_oid oid := 'canonical_chunks'::regclass;
    covering_oid oid;
    old_constraint record;
    new_constraint record;
    receipt_columns smallint[];
BEGIN
    -- Check structure, not just names: a canceled concurrent build can leave an
    -- invalid index that a later CREATE IF NOT EXISTS silently skips.
    FOR expected IN
        SELECT * FROM (VALUES
            ('canonical_chunks', 'canonical_chunks_receipt_authority_key',
             ARRAY['tenant_id','receipt','source_id','document_id','deleted_at'],
             2, true, NULL::text),
            ('canonical_documents', 'canonical_documents_live_authority_idx',
             ARRAY['tenant_id','source_id','document_id'],
             3, false, '(is_current AND (deleted_at IS NULL))')
        ) AS definitions(table_name,index_name,columns,key_count,is_unique,predicate)
    LOOP
        SELECT i.*, index_relation.relkind, method.amname,
               pg_get_expr(i.indpred,i.indrelid) AS predicate,
               ARRAY(SELECT attribute.attname::text
                       FROM unnest(i.indkey) WITH ORDINALITY AS key(attnum,n)
                       JOIN pg_attribute attribute
                         ON attribute.attrelid=i.indrelid AND attribute.attnum=key.attnum
                      ORDER BY key.n) AS columns,
               ARRAY(SELECT attribute.attcollation
                       FROM unnest(i.indkey) WITH ORDINALITY AS key(attnum,n)
                       JOIN pg_attribute attribute
                         ON attribute.attrelid=i.indrelid AND attribute.attnum=key.attnum
                      WHERE key.n <= i.indnkeyatts ORDER BY key.n) AS declared_collations,
               ARRAY(SELECT namespace.nspname || '.' || class.opcname
                       FROM unnest(i.indclass) WITH ORDINALITY AS operator(oid,n)
                       JOIN pg_opclass class ON class.oid=operator.oid
                       JOIN pg_namespace namespace ON namespace.oid=class.opcnamespace
                      ORDER BY operator.n) AS operator_classes
          INTO actual
          FROM pg_index i
          JOIN pg_class index_relation ON index_relation.oid=i.indexrelid
          JOIN pg_am method ON method.oid=index_relation.relam
         WHERE i.indexrelid=to_regclass(format('%I.%I',current_schema(),expected.index_name));
        IF NOT FOUND THEN
            RAISE EXCEPTION 'authority index absent: %', expected.index_name;
        END IF;
        IF actual.indrelid IS DISTINCT FROM
               to_regclass(format('%I.%I',current_schema(),expected.table_name))::oid
           OR actual.relkind <> 'i' OR actual.amname <> 'btree'
           OR NOT actual.indisvalid OR NOT actual.indisready OR NOT actual.indislive
           OR actual.indisunique IS DISTINCT FROM expected.is_unique
           OR actual.indisprimary OR actual.indisexclusion OR NOT actual.indimmediate
           OR actual.indnullsnotdistinct
           OR actual.indnkeyatts <> expected.key_count
           OR actual.indnatts <> cardinality(expected.columns)
           OR actual.columns IS DISTINCT FROM expected.columns
           OR actual.indexprs IS NOT NULL
           OR actual.predicate IS DISTINCT FROM expected.predicate
           OR ARRAY(SELECT value FROM unnest(actual.indcollation) AS value)
                IS DISTINCT FROM actual.declared_collations
           OR actual.operator_classes IS DISTINCT FROM
                array_fill('pg_catalog.text_ops'::text,ARRAY[expected.key_count])
           OR EXISTS(SELECT FROM unnest(actual.indoption) AS value WHERE value <> 0)
        THEN
            RAISE EXCEPTION 'authority index incompatible or invalid: %', expected.index_name;
        END IF;
        IF expected.table_name='canonical_chunks' THEN
            covering_oid := actual.indexrelid;
        END IF;
    END LOOP;

    SELECT ARRAY(SELECT attnum FROM pg_attribute
                  WHERE attrelid=chunks_oid AND attname IN ('tenant_id','receipt')
                  ORDER BY CASE attname WHEN 'tenant_id' THEN 0 ELSE 1 END)
      INTO receipt_columns;
    SELECT * INTO old_constraint FROM pg_constraint
     WHERE conrelid=chunks_oid AND conname='canonical_chunks_tenant_id_receipt_key';
    SELECT * INTO new_constraint FROM pg_constraint
     WHERE conrelid=chunks_oid AND conname='canonical_chunks_receipt_authority_key';

    IF new_constraint.oid IS NOT NULL THEN
        IF old_constraint.oid IS NOT NULL OR new_constraint.contype <> 'u'
           OR new_constraint.conindid <> covering_oid
           OR new_constraint.conkey IS DISTINCT FROM receipt_columns
           OR new_constraint.condeferrable OR new_constraint.condeferred
           OR NOT new_constraint.convalidated
        THEN
            RAISE EXCEPTION 'authority receipt constraint incompatible';
        END IF;
        -- Exact already-completed cutover; retain the same index OID on replay.
        RETURN;
    END IF;
    IF old_constraint.oid IS NULL OR old_constraint.contype <> 'u'
       OR old_constraint.conkey IS DISTINCT FROM receipt_columns
       OR old_constraint.condeferrable OR old_constraint.condeferred
       OR NOT old_constraint.convalidated
    THEN
        RAISE EXCEPTION 'original receipt uniqueness constraint absent or incompatible';
    END IF;
    IF NOT EXISTS (
        SELECT FROM pg_index old_index JOIN pg_index covering
          ON covering.indexrelid=covering_oid
         WHERE old_index.indexrelid=old_constraint.conindid
           AND old_index.indisunique AND old_index.indisvalid AND old_index.indisready
           AND old_index.indnkeyatts=2 AND old_index.indpred IS NULL
           AND old_index.indclass=covering.indclass
           AND old_index.indcollation=covering.indcollation
           AND old_index.indoption=covering.indoption
    ) THEN
        RAISE EXCEPTION 'original receipt uniqueness index incompatible';
    END IF;
    IF EXISTS (SELECT FROM pg_constraint
                WHERE contype='f' AND confrelid=chunks_oid
                  AND conindid=old_constraint.conindid) THEN
        RAISE EXCEPTION 'incoming foreign key depends on original receipt uniqueness';
    END IF;

    -- The valid standalone index already enforces the same full-history key.
    -- RESTRICT is intentional: any unanticipated dependency aborts atomically.
    ALTER TABLE canonical_chunks DROP CONSTRAINT canonical_chunks_tenant_id_receipt_key;
    ALTER TABLE canonical_chunks ADD CONSTRAINT canonical_chunks_receipt_authority_key
        UNIQUE USING INDEX canonical_chunks_receipt_authority_key;
END $$;

COMMIT;
