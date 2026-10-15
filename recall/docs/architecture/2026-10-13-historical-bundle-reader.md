# Read exact archived revisions

`BrainStore.resolve` can now read a historical revision from its existing
`canonical_evidence_objects` entry when its SQL chunk text is absent or fails
its recorded hash. This uses the existing schema 035 `EvidenceBundle` format.
It does not publish objects, clear SQL bodies or change ingestion.

Valid inline historical text remains the fast path and makes no archive call.
Both paths recheck the requested chunks' current deletion state. The archive
path also checks the catalog again after the read and verifies the canonical
revision, document hash, source/session identity, exact chunk receipts and
ordinals. A catalog change, forgotten document or bad object refuses the read.
Object failures cannot substitute unrelated inline text. Archive body reads
hold no database connection and use the existing request deadline transport.

This is the reader compatibility prerequisite for replacing historical SQL
restoration. Writers still restore outgoing cleared revisions into SQL exactly
as before. Deploy this reader everywhere before enabling a future archive-only
historical writer; returning to an older reader after that activation would be
unsafe without first restoring its required inline history.

The writer remains a separate review. Its archive publication and cleanup must
coordinate with the existing evidence publisher, raw artifact admission and
forget. A native lock alone does not serialize the older evidence publisher,
which uploads before catalog registration. A table-local reference check is
therefore insufficient authority to delete an uncertain upload. This PR adds
no cleanup behavior and makes no claim of production storage convergence.
