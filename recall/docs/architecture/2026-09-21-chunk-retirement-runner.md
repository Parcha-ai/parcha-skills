# Bounded retirement for an explicit existing cohort

`run_chunk_retirement.py` makes schema069 progress schedulable without a new service or worker restart. It reuses `retire_parent_chunks`: every attempt reads and verifies the complete parent once, then commits bounded batches. The runner never enrolls parents while running.

## Operator sequence

1. Run `enroll` with an exact tenant, its source owner principal, repeated explicit source IDs, and a new private `--plan-file`. This captures at most 100 existing catalog identities. Review the private page and its printed digest; stdout contains only counts and the digest. For another page, provide `--after-plan-file` from the preceding page and a new output file.
2. Run the same `enroll` with `--apply --plan-file PATH`. Current source ownership and owner grants must still hold. Each exact manifest and catalog creation timestamp is rechecked; an older page cannot authorize a recreated catalog. The transaction inserts absent rows only; existing enabled cursors and disabled markers are preserved.
3. Run `run` with the same exact scope for metadata-only readiness. Add `--apply` to execute bounded maintenance. Set `--max-parents`, `--max-archive-bytes`, `--max-clear-bytes`, `--timeout-seconds`, and parent document/chunk/spool/batch caps for the approved load. Archive reads must already be active, and configured writer restore-before-supersede behavior must already be verified.
4. Invoke the same bounded command again to resume. SIGINT/SIGTERM request a stop before the next batch; committed batches remain. No automatic retries or restores occur. An ambiguous body commit stops the invocation and reports the uncertainty; inspect durable parent progress before deciding the next invocation.

For example, from `recall/`, metadata-only review uses:

```sh
python scripts/run_chunk_retirement.py enroll \
  --tenant-id TENANT --principal-id OWNER --source-id SOURCE \
  --page-size 100 --plan-file /private/existing-parent-page.json
```

This command does not install a schedule or change service configuration. A deployment owner can invoke bounded `run --apply` from the existing maintenance scheduler after reviewing throughput and freshness impact.

## Boundaries and accounting

The API requires a current source owner and an `owner` source grant. Each body transaction locks and rechecks that authority after archive I/O. Claims increment the existing scope epoch and preserve cursors. A competing claim may supersede an old proof after the cooldown; its epoch fence prevents the old proof from clearing bytes. Claims do not hold database connections during archive reads. Metadata transactions have a five-second deadline; normal proof/batch limits still apply. Pool acquisition, DNS, and commit prevent a hard wall-clock guarantee.

Selection includes only surviving enabled parents needing work, excludes their pending logical queue, and orders by oldest attempt. A sixty-second default cooldown and an invocation-local bounded attempted set keep a poison parent from monopolizing the first slot. The shared API has a caller stop hook checked before each batch; the CLI uses it for process signals. This change does not invent global CPU/queue thresholds or attach maintenance to an ingestion loop.

Archive counters charge attempted catalog bytes, including failed reads, across the entire invocation. Clear counters describe acknowledged body mutations. Exceptions preserve known committed counters; an unavailable commit outcome is explicitly marked unknown. Durable cumulative UTF-8 counters describe lifetime work, not net missing bytes, compressed TOAST, allocated disk, or physical reclaimed space.

A claim failure before any attempted parent still raises the original exception. After prior work, the runner stops with `claim_refused`, retaining acknowledged counters and its existing primary error reason. When the claim phase is known, `errors` also includes one `retirement_claim_<phase>_<category>` diagnostic from closed phase and exception/SQLSTATE allowlists. The two labels describe the same failure; their sum is not a failure count. Diagnostics include no candidate identity or exception text.

Claim phases distinguish connection creation/entry, transaction entry, authorization, candidate selection, epoch update, and transaction/connection exit. An exit label identifies where the outward exception arose, including a cleanup failure replacing a body error; it does not establish whether a commit or rollback succeeded. These diagnostics neither resolve earlier ambiguous claims nor authorize retries. The existing body-commit uncertainty field keeps its original meaning.

`eligible_documents` and `excluded` come from the exact parent proof. A completed proof with all documents excluded for `unlocated` has cleared zero bytes. Metadata-only readiness reports parent counts and expressly leaves body eligibility unknown.

Restoring any exact current document writes a disabled parent marker, including when the ledger was previously absent. Enrollment preserves it. Schema068 restoration still works without the optional ledger. Schema069 deletes progress when the parent catalog is deleted: therefore automatic future-parent enrollment remains off. A recreated catalog has no ordinary-run candidate; enrolling it requires a new explicit reviewed page. This is deliberately not a permanent source-wide enrollment policy.

## Locator coverage before full-volume retirement

Existing NULL locators are retained and reported. Running this command alone cannot drain that cohort. Use [streaming locator publication](2026-09-21-streaming-locator-publication.md) for one exact parent, including parents beyond the small planner's 20,000-document limit. It reuses the same full-parent verifier and bounded metadata spool, publishes only matching NULL positions, and requires retirement to be absent or disabled on every commit.

Locator publication and body clearing remain separate. Wait at least 60 seconds after the final publication before separately enrolling/enabling retirement; preserve explicit disabled restoration markers. Each operation performs its own fresh archive proof. Parent size caps limit aggregate stored JSONL bytes, not a full-parent RAM allocation: the verifier reads one immutable part at a time. Review finite larger-parent document/archive/spool/time budgets explicitly; do not remove per-part, body or transaction bounds to process outliers. Unsupported records remain on PostgreSQL fallback.
