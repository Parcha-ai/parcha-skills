BEGIN;
-- Advisory producer metadata only. The full publication reader remains in use.
ALTER TABLE canonical_evidence_documents
    ADD COLUMN IF NOT EXISTS append_source_ordinal bigint,
    ADD COLUMN IF NOT EXISTS append_manifest_id text;
ALTER TABLE canonical_evidence_document_queue
    ADD COLUMN IF NOT EXISTS append_generation bigint,
    ADD COLUMN IF NOT EXISTS append_base_manifest_id text;
COMMENT ON COLUMN canonical_evidence_documents.append_manifest_id IS
    'Manifest identity that the optional ordinal frontier describes; old publishers cannot advance this binding.';
COMMENT ON COLUMN canonical_evidence_document_queue.append_generation IS
    'Pure new-native append classification for this exact generation; mismatch or NULL requires full publication.';
INSERT INTO schema_migrations(version) VALUES (79) ON CONFLICT DO NOTHING;
COMMIT;
