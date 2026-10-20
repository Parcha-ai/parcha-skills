# Retained passage publication recovery

A native passage rebuild can retain an old passage ID and its `created_at`.
An incremental month publication starts at the shard watermark, so it cannot
restore that retained ID if the vendor row is absent. This explains why an
incremental replay can miss an observed gap; it does not establish how the row
was originally lost.

Optional migration 094 adds `search_parent_reconciliation`. A successful native
passage commit with retained IDs requests this work in the same transaction.
Already-current polls do not create work. A same-source epoch rebuild does,
even when the passage IDs, text, receipts and revision are unchanged.

The primary key identifies the parent. `generation` records rebuild requests.
`page_checkpoint` stores the destination identity, the generation being scanned
and the last acknowledged ordinal. `attempted_at` orders attempts fairly and
binds the final compare-and-set to the exact claim. There is no separate unknown
write state. An unacknowledged page retains its old cursor.

Each normal publication quantum advances one month page and at most one parent
page. The parent page contains at most 128 passages and at most 8 MiB of measured
native publication metadata. The original PostgreSQL row projection measures
metadata before transfer. The same bound applies to both native upsert reads,
including the authority recheck under the existing source/month lease.
When a nominated page exceeds the byte allowance, its largest fitting prefix
advances and the remaining records stay owed. A single oversize item reports
`ReconciliationMetadataBudgetExceeded`; no cursor skips it.

The presence query compares source, parent, policy, body hash and header.
Compatible stale receipt/revision metadata does not force another embedding;
current native authority supplies those fields at readback. Missing or differing
rows pass to the existing native upsert owner, with its authority, publication
lease, row builder, token pacer and byte-bounded vendor writes. The reconciliation
queue is neither a serving certificate nor a source-completeness proof.

Rebuilds increment the generation without discarding an active cursor. At EOF,
the exact scanned generation is deleted. A newer generation instead requests
another pass, covering changes behind the cursor. Restarts resume the saved
page; unknown vendor outcomes first recheck presence. A lost acknowledgement
cannot establish exactly-once embedding. Queue and publication counters remain
separate concepts: `pending` now counts month and parent work items, while parent
logs report examined and written rows.

The extra page has a real cost. It adds bounded metadata reads and one vendor
presence query, and may add native upserts. Correct rows require no embedding.
The queue does not continuously audit every vendor row, detect arbitrary loss
without a later rebuild, or replace namespace-wide schema/model migration.

## Rollout

1. Roll out capability-compatible readers and producers while schema 093 remains
   active. New producers skip the optional queue when it is absent.
2. Apply migration 094 through the existing controlled migration owner. Verify
   the table, index, foreign key, runtime privileges and schema marker.
3. Confirm the ordinary worker has the new publisher and observe retained-row
   rebuilds through normal scheduling. Do not infer publication from queue ACKs.

Older binaries reject schema 094. A rollback must retain a compatible reader;
removing the marker or queue is not an automatic rollback procedure. No production
migration, deployment, or historical rebuild is part of this code change.
