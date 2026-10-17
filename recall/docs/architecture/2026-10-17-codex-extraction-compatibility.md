# Codex extraction compatibility before activation

This predecessor accepts the future schema 90 marker, adds honest typed Codex
roles to the existing passage builder, and makes the source audit require an
explicit projector capability for `codex_tool_groups_v1`. It does not ship
migration 090 or enable Codex extraction. `SCHEMA_VERSION` remains 89.

The existing meanings of `legacy` and `tool_groups_v1` stay unchanged. A later
publisher can declare `PassageCandidate.supported_extraction_versions` including
`codex_tool_groups_v1` only when it implements that version. Merely adding an
`extraction_version` field is insufficient. The current projector refuses this
new version before reading the archive; the audit also refuses to reconstruct it.

A Codex input has provider role `assistant`; an output has provider role `tool`.
Only the exact source-unit paths supported by the separate Codex reader contract
are accepted. These roles describe authorship and reported output, not execution,
correctness, human approval, or completion. The builder requires a Codex source
and receipts from that same source. Original visible-message offsets, passage
identities, raw archives, and source-opening authority are unchanged.

The disposable PostgreSQL fixture models the future migration. Tool-role rows
must have only role `tool`, a Codex source, and nonempty spans whose source unit
is always `codex.tool-result.string.v1#/payload/output`. Ordinary user/assistant
rows retain their previous constraint. The fixture also adds the new extraction
version to the existing requested/applied checks. It enrolls no documents.

Rollout remains ordered: deploy compatible readers and audit code to every role,
then deploy the separately reviewed version-aware publisher and migration through
the existing owners. Activation must account for mixed writers: an old worker
cannot be relied upon to create the new version on a first projection. Existing
per-document requests remain authoritative; no historical bulk enrollment is
implicit. Once documents use the new version, rolling back to a projector that
cannot reconstruct it is not a working serving/ingestion rollback.

The native tests cover schema 89 and the explicit future 90 on both search
planes, least-privilege startup, exact source reads, existing publication, strict
role/version constraints, and unknown-schema refusal. The independent reader
fixture retains exact bytes, receipts, revision, grant and Forget checks. The
future publisher owns extraction, queue races, new-document defaults, source
attribution in show/rerank, and vendor publication tests.
