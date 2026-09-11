BEGIN;

-- Hill 1 / T2: a logical document revision is an UPDATE of its catalog row,
-- not a DELETE plus INSERT. Child rows (parts, passage documents, passages,
-- actors, the passage queue) therefore reference the document by its stable
-- identity (tenant_id, source_id, logical_document_id) and never by revision.
-- Forget still deletes the catalog row and cascades through every child.
--
-- Every constraint here is dropped by shape (pg_constraint lookup on the
-- referenced/keyed columns), because the originals carry auto-generated
-- names. New foreign keys are added NOT VALID so this file holds only brief
-- metadata locks; 060b validates them and builds the replacement indexes
-- outside a transaction.

DO $$
DECLARE
    constraint_row record;
BEGIN
    -- Foreign keys that reference canonical_evidence_documents including the
    -- revision column.
    FOR constraint_row IN
        SELECT con.conname, con.conrelid::regclass AS relation
          FROM pg_constraint con
          JOIN pg_class ref ON ref.oid = con.confrelid
          JOIN pg_namespace ns ON ns.oid = ref.relnamespace
         WHERE con.contype = 'f'
           AND ns.nspname = current_schema()
           AND ref.relname = 'canonical_evidence_documents'
           AND (
               SELECT attnum FROM pg_attribute
                WHERE attrelid = con.confrelid AND attname = 'revision'
           ) = ANY (con.confkey)
    LOOP
        EXECUTE format(
            'ALTER TABLE %s DROP CONSTRAINT %I',
            constraint_row.relation, constraint_row.conname
        );
    END LOOP;

    -- The passage -> passage document foreign key that carries revision.
    FOR constraint_row IN
        SELECT con.conname, con.conrelid::regclass AS relation
          FROM pg_constraint con
          JOIN pg_class ref ON ref.oid = con.confrelid
          JOIN pg_namespace ns ON ns.oid = ref.relnamespace
         WHERE con.contype = 'f'
           AND ns.nspname = current_schema()
           AND ref.relname = 'canonical_passage_documents'
           AND (
               SELECT attnum FROM pg_attribute
                WHERE attrelid = con.confrelid AND attname = 'revision'
           ) = ANY (con.confkey)
    LOOP
        EXECUTE format(
            'ALTER TABLE %s DROP CONSTRAINT %I',
            constraint_row.relation, constraint_row.conname
        );
    END LOOP;

    -- Revision-keyed UNIQUE constraints on the passage plane. The passage
    -- document primary key (tenant_id, source_id, logical_document_id)
    -- already covers (…, policy_fingerprint); passages get a
    -- (…, policy_fingerprint, ordinal) unique index in 060b.
    FOR constraint_row IN
        SELECT con.conname, con.conrelid::regclass AS relation
          FROM pg_constraint con
          JOIN pg_class rel ON rel.oid = con.conrelid
          JOIN pg_namespace ns ON ns.oid = rel.relnamespace
         WHERE con.contype = 'u'
           AND ns.nspname = current_schema()
           AND rel.relname IN (
               'canonical_passage_documents', 'canonical_passages'
           )
           AND (
               SELECT attnum FROM pg_attribute
                WHERE attrelid = con.conrelid AND attname = 'revision'
           ) = ANY (con.conkey)
    LOOP
        EXECUTE format(
            'ALTER TABLE %s DROP CONSTRAINT %I',
            constraint_row.relation, constraint_row.conname
        );
    END LOOP;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
         WHERE conname = 'canonical_evidence_document_parts_document_fkey'
    ) THEN
        ALTER TABLE canonical_evidence_document_parts
            ADD CONSTRAINT canonical_evidence_document_parts_document_fkey
            FOREIGN KEY (tenant_id, source_id, logical_document_id)
            REFERENCES canonical_evidence_documents(
                tenant_id, source_id, logical_document_id
            ) ON DELETE CASCADE NOT VALID;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
         WHERE conname = 'canonical_passage_documents_document_fkey'
    ) THEN
        ALTER TABLE canonical_passage_documents
            ADD CONSTRAINT canonical_passage_documents_document_fkey
            FOREIGN KEY (tenant_id, source_id, logical_document_id)
            REFERENCES canonical_evidence_documents(
                tenant_id, source_id, logical_document_id
            ) ON DELETE CASCADE NOT VALID;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
         WHERE conname = 'canonical_evidence_document_actors_document_fkey'
    ) THEN
        ALTER TABLE canonical_evidence_document_actors
            ADD CONSTRAINT canonical_evidence_document_actors_document_fkey
            FOREIGN KEY (tenant_id, source_id, logical_document_id)
            REFERENCES canonical_evidence_documents(
                tenant_id, source_id, logical_document_id
            ) ON DELETE CASCADE NOT VALID;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
         WHERE conname = 'canonical_passage_projection_queue_document_fkey'
    ) THEN
        ALTER TABLE canonical_passage_projection_queue
            ADD CONSTRAINT canonical_passage_projection_queue_document_fkey
            FOREIGN KEY (tenant_id, source_id, logical_document_id)
            REFERENCES canonical_evidence_documents(
                tenant_id, source_id, logical_document_id
            ) ON DELETE CASCADE NOT VALID;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
         WHERE conname = 'canonical_passages_document_fkey'
    ) THEN
        ALTER TABLE canonical_passages
            ADD CONSTRAINT canonical_passages_document_fkey
            FOREIGN KEY (tenant_id, source_id, logical_document_id)
            REFERENCES canonical_passage_documents(
                tenant_id, source_id, logical_document_id
            ) ON DELETE CASCADE NOT VALID;
    END IF;
END $$;

INSERT INTO schema_migrations(version) VALUES (60) ON CONFLICT DO NOTHING;

COMMIT;
