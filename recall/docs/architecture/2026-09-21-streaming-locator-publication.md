# Streaming locator publication

`publish_parent_locators.py` fills verified archive positions for one explicit tenant/source/parent. It handles parents beyond the small planner's20,000-document limit using the existing private SQLite metadata spool and shared body verifier. It never clears PostgreSQL text, creates archive objects, enrolls a parent, or enables retirement.

The caller must own the source and have its current owner grant. Retirement must be absent or disabled before proof and inside every publication transaction. A changed catalog creation time, revision, current document, chunk hash/receipt, source grant, or conflicting position stops the affected batch. Explicitly disabled progress remains disabled.

## Operator sequence

1. Choose a finite reviewed cohort and keep its retirement disabled. Run the command with exact tenant, source, parent and owner principal. Dry proof is the default. Use a new `--report-file` for a private0600 report containing scope, catalog identity and counts; stdout contains counts and a digest.
2. Run the same exact scope with `--apply`. This repeats current proof; a saved report is never accepted as write authority. Each attempt reads every existing part once and checks the final parent checksum before its first write. It then commits only matching NULL positions in bounded batches.
3. After the final locator publication, wait **at least 60 seconds** before separately enrolling/enabling body retirement. This is an operational gate for requests already using PostgreSQL fallback. The command does not enable retirement after waiting. Current service configuration and reader/writer rollout still need their independent gates.

Example, using identity values from a private reviewed cohort:

```sh
python scripts/publish_parent_locators.py \
  --tenant-id "$TASK_TENANT" --source-id "$TASK_SOURCE" \
  --native-parent-id "$TASK_PARENT" --owner-principal-id "$TASK_OWNER" \
  --report-file "$TASK_PRIVATE_REPORT"
```

Add `--apply` for publication and use a different report filename. `RECALL_DATABASE_URL` and the existing evidence archive configuration provide runtime access; the command loads no provider credentials itself. Archive reads use the bounded runtime client. Parent/document/chunk/archive/spool/batch caps and timeout are explicit CLI options; defaults reuse existing parent-proof limits. No worker-loop activation is included.

## Resume and failures

Published locators are the durable checkpoint. A partial/interrupted attempt leaves earlier committed batches valid and retains all PostgreSQL bodies. Repeat a fresh proof to visit remaining NULL positions; matching positions are counted but never overwritten. There is no durable proof cache. A new attempt reads the parent again, but batches within an attempt do not repeat archive reads or full-parent metadata snapshots.

A corrupt/missing object, late whole-parent hash mismatch, spool error, or proof budget failure changes no positions. A post-proof race or failed transaction rolls back that batch. Lost commit acknowledgment sets `commit_unknown=true`; reported `committed` counts include only earlier acknowledged batches. Do not claim rollback or blindly retry that commit: inspect/reprove current locator state. CLI returns0 for completion/dry proof,2 for bounded partial work, and1 for error. Public errors contain fixed codes and counters, not source text or identities.

Source bodies are never stored in the metadata spool. Memory is bounded by one existing archive part, one canonical event, bounded cursor/batch metadata, and SQLite's configured cache. Total cardinality, archive bytes and spool size remain capped; this is not an unlimited-parent mode. SQL/archive deadlines are cooperative with bounded inactivity and precommit checks, not hard cancellation of pool acquisition, DNS or COMMIT.

The existing small locator API and default retirement proof retain their contracts. Unsupported oversized/structural records, pending revisions and unsupported historical chunk boundaries retain their PostgreSQL copies. Body retirement remains a separate operation requiring its own fresh proof.

## Validation

A fresh PostgreSQL regression publishes20,003 documents from an81.8MB,20-part logical archive. Every part is read once, publication uses313transactions at64documents per batch, and all PostgreSQL bodies remain intact. The test also covers exact public routes, partial resume, source and catalog races, retirement enable races, native locking, rollback after UPDATE, cancellation before commit, and lost acknowledgment after a real COMMIT. Proof memory is measured in a fresh interpreter using Linux /proc/self/status VmRSS/VmHWM, separately from fixture/projector preparation. No brittle shared-host performance threshold is imposed.
