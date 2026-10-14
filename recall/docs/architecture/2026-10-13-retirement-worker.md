# Retirement in the projection worker

This adds a bounded retirement round to the existing projection worker. The
command defaults to `--retirement off`; enabling it does not enroll sources or
parents. It processes only sources whose explicit policy inventory is sealed,
and clearing still requires each parent's existing enabled consent, a current
owner/grant, current catalog identity and the unchanged full-parent proof.

## One loop, existing authority

After the existing projection and thinning phases, the worker runs maintenance
at the next reachable boundary after 30 seconds. Each round services one source,
selected by the oldest durable maintenance timestamp. A source that has lost its
owner grant advances in scheduling without archive reads or clearing, so it
cannot starve the other sources. There is no new service or work queue.

Migration 078 adds four small operational fields to the existing source policy:
reconciliation cursor, last scheduled time, invocation epoch and current parent
intent. Parent authorization and cumulative clearing counters stay exclusively
in the existing retirement progress ledger.

A bounded catalog page retries publication enrollment missed because of a lock
or deadline. The cursor survives restart, advances past deferred parents and
wraps after the source tail. Current policy/owner checks run again at every
positive enrollment. Disabling the policy stops new enrollment and rebinds;
previous finite parent approvals remain independent. Per-parent disable/restore
stops that parent's clearing. `--retirement off` stops this worker's maintenance.

## Crash recovery

The existing runner claims a parent by incrementing its scope epoch under its
progress-row lock. Before reading any archive or clearing, a small callback saves
that exact parent, epoch, catalog identity and starting counters in the source
policy. A failed intent write stops the invocation before that parent clears.

A crash or lost clear response leaves that intent intact. The next round reads
the exact catalog and progress under shared locks. It reports observed counter
deltas separately from this round's cleared work and preserves uncertainty about
the old response. It refuses a still-locked row, invalid intent or unexplained
epoch/counter regression. Catalog replacement or revoked consent is observed
without reenabling it.

Once current state is readable, a fresh normal claim and whole-parent proof can
resume automatically. If the old clear already holds the progress lock, recovery
waits for a later round. If the new claim happens first, its epoch prevents the
old proof from committing. No operator approval is needed for ordinary worker
crash recovery. Source invocation epochs also prevent a stale worker from
replacing a newer worker's intent or cursor.

## Budgets, metrics and rollout

The default invocation uses at most 10 parents, 64 MiB of clearing and 256 MiB of
archive reads. The default deadline is 10 seconds, at most 5 while freshness
work is pending; `--retirement-seconds` configures the idle deadline. Reconciliation
gets at most one second and retains a margin to commit its cursor. Full-parent
proof and transport deadlines remain active, SDK retries remain disabled for
maintenance archive reads. The existing SQL pool has a separate five-second
connection wait, so saturation can extend wall time beyond the round deadline.
Archive transport also retains its existing cooperative deadline limitations
around DNS and a continuously streaming response. Late results are rejected;
these bounds are not a strict wall-clock interrupt. Clearing stops when its invocation byte budget
cannot fit another whole document. Limits bound a scheduling round, not stored
evidence or which parents may eventually be proved.

Cycle logs expose status, cleared documents/UTF8 bytes, archive bytes, enrolled
parents, reconciled intents, unknown outcomes and elapsed time. Durable ledger
counters are the authoritative cumulative measure. They measure logical body
removal, not physical SQL disk reclamation; restore/revision writes do not
subtract from them.

Deploy optional-078-compatible binaries before applying 078, which requires 076
(and therefore 075). Keep retirement off while running the existing nonoverlap
worker cutover. Apply the migration in a bounded window, then activate only after
checking the selected source policies and a known parent. Turning the command
back off is the scheduling rollback; do not remove schema markers or reset
parent holds. Existing approved finite jobs must not overlap activation.

Local proofs cover both clear/claim lock orders, crashes before proof and after
commit, policy-off versus finite consent, revoked owners, catalog recreation,
source/cursor fairness, CLI deadline transport and projection-loop cadence.
Production acceptance is still required: cleared bytes must exceed incoming plus
historically restored bytes over sustained windows, with ingest lag, search
latency, database headroom/replication lag and archive read costs within bounds.
Long projection phases still delay the next reachable maintenance boundary.
Historical revision hydration and physical database downsizing remain separate
work; this change makes no claim that either is complete.

## Optional singleton metadata retirement

`--retirement-metadata singleton` explicitly extends an enabled retirement round;
the default `keep` retains chunk rows even when singleton readers are enabled.
The selected mode travels through the existing supervisor, child and source
policy owner. It requires the compatible singleton readers, optional migration
080, archive body reads and the turbopuffer search plane. Deploying this code or
enabling reader capability does not select the metadata operation.

One unchanged whole-parent archive proof covers both body clearing and metadata
retirement. In each existing locked transaction, the worker clears proved bodies
and admits only canonical, current, empty singleton rows with exact receipt,
hash and timestamp identity and no legacy embedding dependencies. Pending
publication prevents metadata admission. Multi-chunk and other ineligible rows
stay physical. Profile admission and exact chunk deletion share the existing
progress commit; there is no second proof per small group or new progress table.

`retired_metadata_chunks` reports only this attempt's acknowledged committed
prefix, including when a later commit outcome is unknown. It is separate from
body-byte counters and is not a durable cumulative count or physical-space
measurement. Recovery preserves the old unknown outcome, reads existing progress
and membership, then uses the original claim/proof path for subsequent work.

An already complete body-only parent needs an explicit existing parent reset to
be reconsidered. Metadata-only reversal restores exact empty chunk rows and
preserves progress, so it does not immediately trigger re-deletion. Turning this
operation back to `keep` stops new metadata retirement; disabling compatible
readers or reverting to a legacy SQL read plane additionally requires restoring
every admitted row first. Production activation still requires a successful
isolated round trip, a measured finite real cohort and review of sustained
latency, ingestion and storage effects.

## Large-parent admission remains open

The default 256 MiB archive budget cannot prove a parent whose committed parts
exceed that size. Repeating the same round will not fix this. The worker retains
its enabled consent and SQL bodies, but does not yet choose a larger budget for
it. The five-second busy budget can also repeatedly defer a smaller slow parent.

A follow-up must schedule an aged large parent even while ingestion stays busy,
select one exact eligible parent, and derive a sufficient archive budget from
its committed part inventory. The existing proof streams bodies and uses a
bounded metadata spool; archive volume does not require the same amount of RAM.
Measure the whole-parent proof time before choosing a longer maintenance quantum.
If that time exceeds the agreed freshness budget, this synchronous loop cannot
satisfy both constraints. Keep that cohort on the existing finite-job owner
until a separate scheduling change is reviewed. Acceptance must cover every
parent-size cohort, not merely a positive aggregate drain rate.
