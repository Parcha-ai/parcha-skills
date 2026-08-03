BEGIN;

ALTER TABLE canonical_passage_embedding_representations
    ALTER COLUMN embedding DROP NOT NULL;

ALTER TABLE canonical_passage_embedding_representations
    ADD COLUMN IF NOT EXISTS embedding_1536 halfvec(1536),
    ADD COLUMN IF NOT EXISTS embedding_3072 halfvec(3072);

ALTER TABLE canonical_passage_embedding_representations
    DROP CONSTRAINT IF EXISTS
        canonical_passage_embedding_representations_dimensions_check;

ALTER TABLE canonical_passage_embedding_representations
    ADD CONSTRAINT canonical_passage_repr_dimensions_check
        CHECK (dimensions IN (512, 1536, 3072)),
    ADD CONSTRAINT canonical_passage_repr_vector_dimensions_check
        CHECK (
            (
                dimensions=512
                AND embedding IS NOT NULL
                AND embedding_1536 IS NULL
                AND embedding_3072 IS NULL
            )
            OR (
                dimensions=1536
                AND embedding IS NULL
                AND embedding_1536 IS NOT NULL
                AND embedding_3072 IS NULL
            )
            OR (
                dimensions=3072
                AND embedding IS NULL
                AND embedding_1536 IS NULL
                AND embedding_3072 IS NOT NULL
            )
        );

INSERT INTO schema_migrations(version) VALUES (43) ON CONFLICT DO NOTHING;

COMMIT;
