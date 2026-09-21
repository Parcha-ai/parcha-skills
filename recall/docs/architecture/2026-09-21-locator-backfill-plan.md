# Backfill exact positions from existing logical parts

Deployment alone does not populate old document positions. This operation verifies
existing immutable parts once per parent and fills only proven NULL positions.
It does not reproject, upload objects, change a parent catalog, or delete source prose.
The default remains a read-only coverage report; publication requires `--apply`.

## Bounded operation

Apply migration 068 first. Use the existing administrative environment with database
read access and evidence-object read access. Applying additionally requires database
TEMP privilege, the runtime role’s existing queue/catalog row-lock privileges, and
UPDATE access to the two locator columns. Credentials are supplied by that environment; the script does not retrieve or print them.

```sh
python server/scripts/plan_body_locators.py \
  --tenant TENANT --source SOURCE --limit 1 \
  --max-bytes 268435456 --seconds 60 \
  --output /private/new-locator-plan.json

python server/scripts/plan_body_locators.py \
  --tenant TENANT --source SOURCE --limit 1 --apply \
  --max-bytes 268435456 --seconds 60 \
  --output /private/new-locator-apply.json
```

Every apply reruns the complete archive proof in process. A saved JSON report is
advisory and cannot authorize locator writes. `--resume PRIOR_REPORT` uses only its
keyset cursor and matching tenant/source scope; apply can resume only a prior apply
report. Apply stops at the first failed parent, preserving the prior successful cursor
for a fresh retry. Earlier parents remain committed; each individual parent is atomic.
A dry run may continue through failures to report coverage gaps.

Reports use exclusive creation and mode 0600. They contain identifiers, proposed
positions, exclusion/error counts and the input proof fingerprint, never source prose.
Standard output contains aggregate counts only. Scope is parents already present in
the logical catalog; unprojected parents are not counted as covered. Neither mode
authorizes body thinning or establishes historical receipt recovery.

## Proof before locks

1. Capture parent manifest/parts, queue generation and changed-at, current document
   identities, revisions and hashes, chunk receipts/hashes, and existing positions in
   one read-only repeatable-read transaction. Historical and tombstoned records do not
   become current candidates.
2. Release the connection. Verify each part's tenant/source/logical identity, revision,
   ordinal topology, byte size and digest, and the complete parent digest/receipt count.
   Fetch each existing part once and retain only the current eligible event, at most
   8 MB. Reuse the current reader's exact receipt, full-event and every-chunk hash proof.
3. Keep compact candidate metadata only. Existing correct positions yield no UPDATE;
   inconsistent existing positions fail the parent. A fresh metadata snapshot rejects
   publication, revision, tombstone or queue changes during the archive scan.

Oversized or structural records, unsupported historical chunk layouts, bodies above
the canonical event bound, pending new documents, and proven older revisions remain
excluded. Missing/corrupt parts, arbitrary receipt or full-body hash mismatches, and
malformed streams fail the parent. Some unlocated historical chunk boundaries cannot
be distinguished from chunk-metadata mismatches by hashes alone; neither yields an
eligible locator. No partial positions escape a failed proof.

## Atomic NULL-only publication

Apply retains the in-process proof metadata; it never reconstructs authority from a
saved report. In a READ COMMITTED transaction it locks the existing queue row, the
parent catalog row, then only changed current document rows, all with NOWAIT. It
acquires no advisory lock. Candidate identities and positions stream into a TEMP table;
the UPDATE joins that table and also requires exact native identity, revision, whole
hash, current/nondeleted status and BOTH locator columns NULL. Omitted, unsupported,
historical, non-NULL and newly appended documents are never cleared or rewritten.

After acquiring those locks, the operation rereads the full metadata fence through the
same connection. It rechecks queue absence as well as queue presence/generation.
An append committed before this check rejects the parent and requires a new proof.
An append after the check may safely insert a new queue row: the parent catalog cannot
publish a replacement while locked, old proven documents cannot change while locked,
and the new unarchived document remains NULL. Locking an absent queue row is not used
as a fence. The existing archive and locked current documents supply the safety proof.

Both ingest paths lock old current documents before inserting replacement documents
and marking the queue. If ingest owns a document first, apply fails NOWAIT and releases
its queue/catalog locks so ingest completes. If apply owns it first, ingest waits until
verified old positions commit, then makes that old document historical; its new revision
starts with NULL positions. Any failure or deadline before commit rolls back all writes
for that parent, including TEMP staging. Repeating a completed parent makes no updates.

## Limits and accounting

Defaults: 10,000 current documents, 100,000 chunks, 4,096 parts, 200,000 records and
256 MiB of archive bytes per parent. Objects are capped at 64 MiB. The CLI shares byte
and time budgets across its page, permits at most 100 parents, and stops after at least
50,000 proposed documents (one bounded parent can cross the threshold). Explicit API
limits have fixed maxima. Each query and no-retry object read receives the same absolute
deadline; the existing transport's cooperative deadline limitations remain.

`archive_gets` and `archive_bytes` count attempted GETs and their catalog byte sizes,
including failed reads. They are work/cost estimates, not provider billing. Budget
failures include expected archive work when the catalog was captured. No second body
store or per-turn objects are created. Over-budget parents remain reported coverage
gaps and require an explicitly revised operation; they are never silently covered.

## Witnessed tests

Unit/CLI tests cover exact shared proof, private exclusive reports, explicit apply,
fresh proof despite saved data, retry cursors, deadlines and failed-read accounting.
`e2e_locator_backfill_plan.py` proves read-only behavior, single part reads, corruption,
append/publication races, tenant scoping and revision/oversized/historical exclusions.
`e2e_locator_backfill_apply.py` uses real concurrent PostgreSQL connections to prove
NULL-only/idempotent publication, unchanged body bytes and existing locator xmin,
append both before and after the final fence, revisions in both lock orders, immediate
busy-lock failure and whole-parent rollback after an injected post-UPDATE failure.
The archive spy rejects any object read while a database connection is held.

Historical recovery, both writer retirement paths, measured production coverage and
operational rollback remain separate prerequisites for deleting body copies.
