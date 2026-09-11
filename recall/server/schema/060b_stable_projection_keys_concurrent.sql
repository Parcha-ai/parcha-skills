-- Companion to 060_stable_projection_keys.sql. `migrate()` runs every
-- statement of a *_concurrent.sql file in its own autocommit transaction, so
-- CREATE INDEX CONCURRENTLY is allowed and each VALIDATE CONSTRAINT holds only
-- a SHARE UPDATE EXCLUSIVE lock while it scans the table once. Every statement
-- is idempotent: validating a valid constraint is a no-op and the index
-- statements carry IF [NOT] EXISTS.

ALTER TABLE canonical_evidence_document_parts
    VALIDATE CONSTRAINT canonical_evidence_document_parts_document_fkey;

ALTER TABLE canonical_passage_documents
    VALIDATE CONSTRAINT canonical_passage_documents_document_fkey;

ALTER TABLE canonical_evidence_document_actors
    VALIDATE CONSTRAINT canonical_evidence_document_actors_document_fkey;

ALTER TABLE canonical_passage_projection_queue
    VALIDATE CONSTRAINT canonical_passage_projection_queue_document_fkey;

ALTER TABLE canonical_passages
    VALIDATE CONSTRAINT canonical_passages_document_fkey;

CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS
    canonical_passages_document_policy_ordinal_key
    ON canonical_passages(
        tenant_id, source_id, logical_document_id, policy_fingerprint, ordinal
    );

-- The revision-keyed document index is superseded by the unique index above,
-- whose prefix serves every (tenant, source, logical document) lookup.
DROP INDEX CONCURRENTLY IF EXISTS canonical_passages_document_idx;
