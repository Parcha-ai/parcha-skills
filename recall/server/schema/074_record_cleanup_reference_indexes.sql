BEGIN;

-- IF NOT EXISTS alone can retain a failed concurrent build or a wrong index
-- under the expected name. Record completion only for all three exact indexes.
DO $$
BEGIN
    IF (
        SELECT count(*)
          FROM (VALUES
              ('canonical_evidence_documents',
               'canonical_evidence_documents_manifest_artifact_idx',
               ARRAY['tenant_id','source_id','manifest_artifact_id']),
              ('canonical_evidence_document_parts',
               'canonical_evidence_document_parts_artifact_idx',
               ARRAY['tenant_id','source_id','artifact_id']),
              ('canonical_parquet_scan_shards',
               'canonical_parquet_scan_shards_artifact_idx',
               ARRAY['tenant_id','source_id','artifact_id'])
          ) expected(table_name,index_name,columns)
          JOIN pg_namespace namespace ON namespace.nspname='public'
          JOIN pg_class relation ON relation.relnamespace=namespace.oid
               AND relation.relname=expected.table_name
          JOIN pg_index definition ON definition.indrelid=relation.oid
          JOIN pg_class index_relation ON index_relation.oid=definition.indexrelid
               AND index_relation.relnamespace=namespace.oid
               AND index_relation.relname=expected.index_name
          JOIN pg_am method ON method.oid=index_relation.relam
         WHERE definition.indisvalid AND definition.indisready
           AND NOT definition.indisunique AND method.amname='btree'
           AND definition.indpred IS NULL AND definition.indexprs IS NULL
           AND definition.indnkeyatts=3 AND definition.indnatts=3
           AND (
               SELECT array_agg(attribute.attname::text ORDER BY key.ordinality)
                 FROM unnest(definition.indkey) WITH ORDINALITY key(attnum,ordinality)
                 JOIN pg_attribute attribute ON attribute.attrelid=relation.oid
                      AND attribute.attnum=key.attnum
           )=expected.columns
    ) <> 3 THEN
        RAISE EXCEPTION 'cleanup reference indexes unavailable';
    END IF;
END
$$;

INSERT INTO schema_migrations(version) VALUES (74) ON CONFLICT DO NOTHING;

COMMIT;
