# Codex text-leaf storage contract

Migration 093 permits the additive source unit
`codex.tool-result.block-text.v1#/payload/output/<index>/text` in tool-only
Codex passages. The index is canonical ASCII decimal: `0`, or a nonzero digit
followed by digits. The original whole-output unit remains accepted.

The existing source, role, nonempty-span and every-span checks remain in force.
This is a storage shape check. The unchanged source resolver, receipt authority,
Forget checks and publication transaction still establish what may be published
or read. The migration neither enrolls documents nor changes extraction.

## Ordered rollout

1. Deploy the reviewed text-leaf reader and optional-093 capability support to
   MCP, worker and managed roles while the database remains on schema 092.
   Verify each actual deployed revision and its original serving checks.
2. Use the existing bounded migration owner, starting from
   `schema92-additive-membership-001/refresh004/owner`, with a separately reviewed
   093 SQL/catalog binding and the accepted full fleet. Reuse its temporary
   credential cleanup, exact marker checks and uncertain-COMMIT reconciliation.
   Do not run the old 092 packet or assume a future deployment identity.
3. Apply only `093_codex_text_block_storage.sql`. The transaction has a two-second
   lock timeout. Its replacement CHECK is `NOT VALID` to avoid scanning the
   passage corpus under an exclusive DDL lock. Every existing valid row satisfies
   the unchanged old alternative; PostgreSQL enforces the new CHECK on new writes.
   A busy table aborts the transaction without replacing the old constraint or
   committing marker 093.
4. Verify the actual marker, constraint and unchanged public authority before
   releasing the separate mixed-output extractor. Reader compatibility and
   schema 093 are both prerequisites for that producer.

Old binaries reject unknown marker 093. After the migration, rollback targets
must retain optional-093 compatibility and the text-leaf reader. The producer
can be rolled back independently while those contracts remain. Reinstalling the
old role constraint after new-unit publication is not a safe automatic rollback.

## Evidence

The native test copies rows through PostgreSQL, proves old-constraint rejection,
rejects malformed indexes and mixed invalid spans, and exercises actual lock
timeout rollback followed by successful migration. Existing source-reader and
publication controls are retained. Compatibility accepts 092 and 093 with either
search plane; missing prerequisite 090, unknown versions, duplicate markers,
schema gaps and privilege failures remain refused.
