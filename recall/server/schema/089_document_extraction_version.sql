BEGIN;
-- Opt-in extraction on existing projected documents. Existing and newly created
-- documents stay legacy; migration itself enrolls no source or document.
ALTER TABLE canonical_passage_documents
    ADD COLUMN requested_extraction_version text NOT NULL DEFAULT 'legacy'
        CHECK (requested_extraction_version IN ('legacy','tool_groups_v1')),
    ADD COLUMN applied_extraction_version text NOT NULL DEFAULT 'legacy'
        CHECK (applied_extraction_version IN ('legacy','tool_groups_v1')),
    ADD COLUMN extraction_coverage jsonb;
COMMENT ON COLUMN canonical_passage_documents.requested_extraction_version IS
    'Durable document enrollment; requested with the existing passage queue in one transaction. Preserved across ordinary evidence revisions.';
COMMENT ON COLUMN canonical_passage_documents.applied_extraction_version IS
    'Extraction version of the last successfully committed full projection. A pending request never claims applied coverage.';
COMMENT ON COLUMN canonical_passage_documents.extraction_coverage IS
    'Counts and raw-only dispositions from that applied extraction. Raw source records remain in the existing archive regardless of extraction.';
INSERT INTO schema_migrations(version) VALUES (89) ON CONFLICT DO NOTHING;
COMMIT;
